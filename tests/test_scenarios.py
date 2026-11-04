import random

import pytest

from cranelock_gate.client import command, may_enter


@pytest.mark.parametrize("seed", [7, 20, 3104])
def test_deterministic_fault_sequences_preserve_exclusive_physical_occupancy(h, seed):
    """Independent physical model; digital recovery never removes a crane."""
    rng = random.Random(seed)
    physical_holder = None
    stale_entries = []
    accepted_entries = 0
    h.ready()
    for _ in range(250):
        action = rng.randrange(9)
        state = h.state
        if action == 0:
            h.grant()
        elif action == 1 and state["grant"]:
            payload = command(h.config, "simulator", "ValidateEntry", epoch=state["epoch"],
                              revision=state["revision"], holder=state["holder"],
                              grant_id=state["grant"]["id"], session_id=state["session_id"])
            result = h.send(payload)
            stale_entries.append(payload)
            if may_enter(result):
                assert physical_holder is None, "Two cranes physically admitted"
                physical_holder = state["holder"]
                accepted_entries += 1
        elif action == 2:
            h.install(rng.choice(list(h.config.members)))
        elif action == 3:
            h.clock.advance(rng.choice([0.5, 2.5, 6]))
        elif action == 4:
            h.restart()
        elif action == 5:
            h.observe("CLEAR" if physical_holder is None else "OCCUPIED")
        elif action == 6:
            # The physical crane exits independently; only then is clearance reported.
            physical_holder = None
            h.observe(actor="operator")
        elif action == 7 and stale_entries:
            old = rng.choice(stale_entries)
            assert not may_enter(h.send(old)), "Old entry replay caused another movement"
        elif action == 8:
            h.call(state["holder"], "TransferHolder", epoch=state["epoch"], revision=state["revision"],
                   destination=rng.choice(list(h.config.members)))
        state = h.state
        if physical_holder is not None:
            assert state["lane_state"] in {"OCCUPIED", "UNKNOWN"}
    assert accepted_entries > 0


def test_missing_database_while_running_is_fail_closed(h, monkeypatch):
    h.ready()
    state = h.state
    original = type(h.path).is_file
    def missing(path):
        return False if path == h.path else original(path)
    monkeypatch.setattr(type(h.path), "is_file", missing)
    result = h.call("crane-1", "RequestGrant", epoch=state["epoch"], revision=state["revision"])
    assert not result["allowed"] and result["reason"] == "STORAGE_UNAVAILABLE"


def test_conflicting_same_epoch_authority_is_rejected(h):
    h.ready()
    assert h.install("crane-2", epoch=0)["reason"] == "AUTHORITY_CONFLICT"
    assert h.state["holder"] == "crane-1"


def test_recovery_certificate_cannot_override_a_new_handoff(h):
    from cranelock_gate.client import certificate
    h.ready()
    old = h.state
    pending = certificate(h.config, h.keys, old, "crane-3", 1)
    assert h.call("crane-1", "TransferHolder", epoch=old["epoch"], revision=old["revision"], destination="crane-2")["allowed"]
    result = h.call("crane-3", "InstallAuthority", certificate=pending,
                    expected_epoch=old["epoch"], expected_revision=old["revision"])
    assert result["reason"] == "STALE_REVISION"
    assert h.state["holder"] == "crane-2"


def test_pre_entry_clearance_cannot_clear_post_entry_occupancy(h):
    h.ready()
    h.grant()
    reserved = h.state
    # Sampled before entry, delivered later, with a timestamp inside allowed clock skew.
    pending_clearance = command(h.config, "simulator", "ReportLaneState",
        session_id=reserved["session_id"], sequence=reserved["observation_sequence"] + 1,
        observation_revision=reserved["observation_revision"],
        observed_at_ms=int(h.clock() * 1000) + 100, lane_state="CLEAR")
    assert h.enter()["allowed"]
    assert h.send(pending_clearance)["reason"] == "STALE_OBSERVATION"
    assert h.state["lane_state"] == "OCCUPIED"
    assert not h.grant()["allowed"]


def test_earlier_development_database_upgrade_preserves_authority(h):
    import sqlite3
    import json
    h.ready()
    old = h.state
    h.gate.close()
    with sqlite3.connect(h.path) as db:
        state = json.loads(db.execute("SELECT value FROM state").fetchone()[0])
        del state["observation_revision"]
        db.execute("UPDATE state SET value=?", (json.dumps(state),))
        db.execute("UPDATE metadata SET version=1")
    from cranelock_gate.core import Gate
    h.gate = Gate(h.path, h.config, h.clock)
    state = h.state
    assert (state["epoch"], state["revision"], state["holder"]) == (old["epoch"], old["revision"], old["holder"])
    assert state["lane_state"] == "UNKNOWN" and state["observation_revision"] >= 1
    assert h.gate.db.execute("SELECT version FROM metadata").fetchone()[0] == 2
