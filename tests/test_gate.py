import json
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from dataclasses import replace

import pytest

from cranelock_gate.client import certificate, command, may_enter
from cranelock_gate.core import Gate
from cranelock_gate.crypto import COMMAND_DOMAIN, canonical
from cranelock_gate.storage import initialize


def test_first_milestone(h):
    assert h.install()["allowed"]
    assert h.grant()["reason"] == "LANE_UNKNOWN"
    assert h.observe()["allowed"]
    assert h.grant()["allowed"]
    assert h.enter()["allowed"]
    assert h.grant()["reason"] == "LANE_BUSY"
    old = h.state
    assert h.install("crane-2")["allowed"]
    assert h.grant("crane-1", epoch=old["epoch"], revision=old["revision"])["reason"] == "STALE_EPOCH"
    assert h.grant()["reason"] == "LANE_BUSY"


@pytest.mark.parametrize("mutation,reason", [
    ("minority", "NO_QUORUM"), ("duplicates", "INVALID_CERTIFICATE"),
    ("altered", "INVALID_CERTIFICATE"), ("signature", "INVALID_CERTIFICATE"),
    ("unknown_signer", "INVALID_CERTIFICATE"), ("wrong_membership", "INVALID_CERTIFICATE"),
    ("mixed_payloads", "INVALID_CERTIFICATE")])
def test_certificates(h, mutation, reason):
    state = h.state
    cert = certificate(h.config, h.keys, state, "crane-1", 0)
    if mutation == "minority":
        cert["signatures"] = cert["signatures"][:1]
    elif mutation == "duplicates":
        cert["signatures"][1] = cert["signatures"][0].copy()
    elif mutation == "altered":
        cert["payload"]["holder"] = "crane-3"
    elif mutation == "signature":
        cert["signatures"][0]["signature"] = "AA=="
    elif mutation == "unknown_signer":
        cert["signatures"][0]["signer_id"] = "intruder"
    elif mutation == "wrong_membership":
        cert["payload"]["membership_id"] = "other"
    else:
        other = certificate(h.config, h.keys, state, "crane-3", 0)
        cert["signatures"][1] = other["signatures"][1]
    result = h.call("crane-1", "InstallAuthority", certificate=cert,
                    expected_epoch=-1, expected_revision=0)
    assert result["reason"] == reason
    assert h.state["epoch"] == -1


def test_5_member_quorum_requires_three(tmp_path):
    from conftest import Harness
    from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
    harness = Harness(tmp_path)
    harness.gate.close()
    for identity in ["crane-4", "crane-5"]:
        key = Ed25519PrivateKey.generate()
        harness.keys[identity] = key
        harness.config.members[identity] = key.public_key().public_bytes_raw().hex()
    path = tmp_path / "five.sqlite"
    initialize(path, harness.config, harness.clock())
    harness.gate = Gate(path, harness.config, harness.clock)
    try:
        assert harness.install(signers=["crane-1", "crane-2"])["reason"] == "NO_QUORUM"
        assert harness.install(signers=["crane-1", "crane-2", "crane-3"])["allowed"]
    finally:
        harness.gate.close()


def test_handoff_fences_old_holder(h):
    h.ready()
    old = h.state
    result = h.call("crane-1", "TransferHolder", epoch=old["epoch"], revision=old["revision"], destination="crane-2")
    assert result["allowed"]
    assert h.grant("crane-1", epoch=old["epoch"], revision=old["revision"])["reason"] == "STALE_REVISION"
    assert h.grant("crane-1")["reason"] == "WRONG_HOLDER"
    assert h.grant("crane-2")["allowed"]


@pytest.mark.parametrize("lane", ["RESERVED", "OCCUPIED", "UNKNOWN"])
def test_handoff_requires_clearance(h, lane):
    h.ready()
    if lane != "UNKNOWN":
        h.grant()
    if lane == "OCCUPIED":
        h.enter()
    if lane == "UNKNOWN":
        h.observe("UNKNOWN")
    state = h.state
    assert not h.call("crane-1", "TransferHolder", epoch=state["epoch"], revision=state["revision"], destination="crane-2")["allowed"]


def test_concurrent_grants_and_entries(h):
    h.ready()
    with ThreadPoolExecutor(max_workers=12) as pool:
        results = list(pool.map(lambda _: h.grant(), range(30)))
    assert sum(r["action_applied"] for r in results) == 1
    with ThreadPoolExecutor(max_workers=12) as pool:
        entries = list(pool.map(lambda _: h.enter(), range(30)))
    assert sum(may_enter(r) for r in entries) == 1
    assert h.state["lane_state"] == "OCCUPIED"


def test_expiry_never_clears_lane(h):
    h.ready()
    grant = h.grant()["grant"]
    state = h.state
    h.clock.advance(5)
    result = h.call("simulator", "ValidateEntry", epoch=state["epoch"], revision=state["revision"],
                    holder="crane-1", grant_id=grant["id"], session_id=state["session_id"])
    assert result["reason"] == "EXPIRED_GRANT"
    assert result["state"]["lane_state"] == "UNKNOWN"
    assert h.grant()["reason"] == "LANE_UNKNOWN"
    assert h.observe()["allowed"]
    assert h.grant()["allowed"]


def test_occupied_grant_expiry_is_not_exit(h):
    h.ready()
    h.grant()
    h.enter()
    h.clock.advance(6)
    assert h.state["lane_state"] == "UNKNOWN"
    assert not h.grant()["allowed"]


def test_lost_updates_and_late_exit(h):
    h.ready()
    h.grant()
    h.enter()
    old = h.state
    h.clock.advance(3)
    assert h.state["lane_state"] == "UNKNOWN"
    assert h.observe(observed_at_ms=1000000)["reason"] == "STALE_OBSERVATION"
    assert h.observe(actor="operator")["allowed"]
    assert h.grant()["allowed"]
    assert h.observe(session_id=old["session_id"])["reason"] == "STALE_SESSION"


def test_clear_observation_cannot_release_reserved_grant(h):
    h.ready()
    h.grant()
    assert h.observe()["reason"] == "OUTSTANDING_GRANT"
    assert h.state["lane_state"] == "RESERVED"
    assert h.observe(actor="operator")["allowed"]
    assert h.state["grant"] is None


def test_recovery_during_reservation_invalidates_grant(h):
    h.ready()
    h.grant()
    assert h.install("crane-2")["allowed"]
    assert h.state["grant"] is None
    assert h.state["lane_state"] == "UNKNOWN"


def test_recovery_during_occupancy_requires_exit(h):
    h.ready()
    h.grant()
    h.enter()
    assert h.install("crane-2")["allowed"]
    assert h.state["lane_state"] == "OCCUPIED"
    assert not h.grant()["allowed"]
    assert h.observe()["allowed"]
    assert h.grant()["allowed"]


@pytest.mark.parametrize("lane", ["CLEAR", "RESERVED", "OCCUPIED"])
def test_restart_preserves_authority_and_requires_clearance(h, lane):
    h.ready()
    if lane != "CLEAR":
        h.grant()
    if lane == "OCCUPIED":
        h.enter()
    old = h.state
    h.restart()
    state = h.state
    assert (state["epoch"], state["holder"], state["revision"]) == (old["epoch"], old["holder"], old["revision"])
    assert state["lane_state"] == "UNKNOWN" and state["grant"] is None
    assert state["session_id"] != old["session_id"]
    assert h.observe(session_id=old["session_id"])["reason"] == "STALE_SESSION"
    assert not h.grant()["allowed"]
    assert h.observe()["allowed"]
    assert h.grant()["allowed"]


def test_idempotent_entry_retry_never_moves_again(h):
    h.ready()
    h.grant()
    state = h.state
    payload = command(h.config, "simulator", "ValidateEntry", epoch=state["epoch"],
                      revision=state["revision"], holder=state["holder"],
                      grant_id=state["grant"]["id"], session_id=state["session_id"])
    assert may_enter(h.send(payload))
    replay = h.send(payload)
    assert replay["allowed"] and replay["replayed"] and not may_enter(replay)
    h.restart()
    assert not may_enter(h.send(payload))


def test_request_id_conflict(h):
    h.ready()
    payload = command(h.config, "crane-1", "RequestGrant", epoch=0, revision=1)
    assert h.send(payload)["allowed"]
    payload["revision"] += 1
    assert h.send(payload)["reason"] == "REQUEST_ID_CONFLICT"


@pytest.mark.parametrize("attack", ["tamper", "wrong_key", "wrong_operation", "noncanonical"])
def test_command_authentication(h, attack):
    h.ready()
    payload = command(h.config, "crane-1", "RequestGrant", epoch=0, revision=1)
    signature = h.keys["crane-1"].sign(COMMAND_DOMAIN + canonical(payload))
    operation = "RequestGrant"
    raw = canonical(payload)
    if attack == "tamper":
        payload["actor_id"] = "crane-2"
        raw = canonical(payload)
    elif attack == "wrong_key":
        signature = h.keys["simulator"].sign(COMMAND_DOMAIN + raw)
    elif attack == "wrong_operation":
        operation = "TransferHolder"
    else:
        raw = json.dumps(payload).encode()
    assert not h.gate.execute(operation, raw, signature)["allowed"]
    assert h.state["lane_state"] == "CLEAR"


@pytest.mark.parametrize("role", ["crane-1", "controller", "operator"])
def test_only_simulator_can_accept_entry(h, role):
    h.ready()
    h.grant()
    assert h.enter(actor=role)["reason"] == "UNAUTHORIZED"
    assert h.state["lane_state"] == "RESERVED"


def test_sequence_and_freshness(h):
    h.ready()
    assert h.observe(sequence=h.state["observation_sequence"])["reason"] == "STALE_OBSERVATION"
    assert h.observe(observed_at_ms=int(h.clock() * 1000) + 1000)["reason"] == "STALE_OBSERVATION"
    h.clock.advance(3)
    assert h.state["lane_state"] == "UNKNOWN"
    assert h.observe(observed_at_ms=1001500)["reason"] == "STALE_OBSERVATION"
    assert h.observe()["allowed"]


def test_clock_rollback_fails_closed(h):
    h.ready()
    h.grant()
    h.clock.advance(-1)
    assert h.grant()["reason"] == "CLOCK_ROLLBACK"
    assert h.state["lane_state"] == "UNKNOWN"
    h.clock.advance(1)
    assert h.observe()["allowed"]


def test_commit_failure_never_returns_success(h, monkeypatch):
    h.ready()
    state = h.state
    def fail():
        raise sqlite3.OperationalError("injected disk error")
    monkeypatch.setattr(h.gate, "_commit", fail)
    assert h.call("crane-1", "RequestGrant", epoch=state["epoch"], revision=state["revision"])["reason"] == "STORAGE_UNAVAILABLE"
    assert h.gate.failed
    monkeypatch.undo()
    h.restart()
    assert h.state["grant"] is None


def test_read_only_export_does_not_block_commits(h):
    for _ in range(50):
        h.gate.db.execute("INSERT INTO events(value) VALUES('{}')")
    reader = sqlite3.connect(h.path.resolve().as_uri() + "?mode=ro", uri=True)
    try:
        cursor = reader.execute("SELECT value FROM events ORDER BY event_id")
        cursor.fetchone()  # Export paused mid-scan, still holding its read snapshot.
        h.ready()
        assert not h.gate.failed
    finally:
        reader.close()


def test_failed_rollback_still_fails_closed(h, monkeypatch):
    real = h.gate.db

    class BrokenDisk:
        def execute(self, sql, *args):
            if sql == "BEGIN IMMEDIATE":
                raise sqlite3.OperationalError("disk I/O error")
            return real.execute(sql, *args)

        def rollback(self):
            raise sqlite3.OperationalError("disk I/O error")

        def __getattr__(self, name):
            return getattr(real, name)

    monkeypatch.setattr(h.gate, "db", BrokenDisk())
    assert h.gate.get_state()["reason"] == "STORAGE_UNAVAILABLE"
    assert h.gate.failed


def test_missing_corrupt_and_duplicate_gate(h, tmp_path):
    with pytest.raises(RuntimeError, match="Another gate"):
        Gate(h.path, h.config, h.clock)
    with pytest.raises(RuntimeError, match="missing"):
        Gate(tmp_path / "missing.sqlite", h.config, h.clock)
    corrupt = tmp_path / "corrupt.sqlite"
    corrupt.write_bytes(b"corrupted database")
    with pytest.raises(sqlite3.DatabaseError):
        Gate(corrupt, h.config, h.clock)
    h.gate.close()
    with pytest.raises(FileExistsError):
        initialize(h.path, h.config, h.clock())


def test_config_mismatch(h):
    h.gate.close()
    with pytest.raises(RuntimeError, match="mismatch"):
        Gate(h.path, replace(h.config, membership_id="different"), h.clock)


def test_duplicate_public_keys_cannot_count_as_distinct_members(h):
    members = dict(h.config.members)
    members["crane-2"] = members["crane-1"].upper()
    with pytest.raises(ValueError, match="distinct public key"):
        replace(h.config, members=members)


def test_controller_return_preserves_lane_and_fences(h):
    h.ready()
    h.grant()
    h.enter()
    old = h.state
    result = h.install("crane-2", epoch=old["epoch"], mode="CENTRAL")
    assert result["allowed"]
    assert h.state["mode"] == "CENTRAL"
    assert h.state["lane_state"] == "OCCUPIED"
    assert h.state["grant"] is None
    assert not h.grant()["allowed"]
    assert h.grant("crane-1", epoch=old["epoch"], revision=old["revision"])["reason"] == "STALE_REVISION"


def test_old_controller_snapshot_rejected(h):
    h.ready()
    old = h.state
    cert = certificate(h.config, h.keys, old, "crane-1", old["epoch"], "CENTRAL", ["controller"])
    assert h.install("crane-2")["allowed"]
    assert h.call("controller", "InstallAuthority", certificate=cert,
                  expected_epoch=old["epoch"], expected_revision=old["revision"])["reason"] == "STALE_EPOCH"


def test_audit_trace(h):
    h.ready()
    h.grant()
    h.enter()
    events = h.gate.events()
    entries = [event for event in events if event["operation"] == "ValidateEntry" and event["allowed"]]
    assert len(entries) == 1 and entries[0]["lane_state"] == "OCCUPIED"
    assert entries[0]["session_id"] == entries[0]["grant_id"]


@pytest.mark.parametrize("field,value", [("epoch", True), ("revision", -1), ("epoch", []), ("revision", "1")])
def test_malformed_signed_fields_do_not_poison_gate(h, field, value):
    h.ready()
    assert h.grant(**{field: value})["reason"] == "INVALID_REQUEST"
    assert not h.gate.failed
    assert h.grant()["allowed"]
