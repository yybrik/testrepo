"""End-to-end safety evidence: the gate never accepts conflicting access.

Every test here attacks the gate with commands that were valid at some earlier point
(old grants, old handoffs, old certificates, old physical evidence) across authority
changes, communication failures and gate crashes. docs/SAFETY_PROOF.md maps the
safety claims to these tests.
"""
import random
import threading
import uuid
from collections import Counter
from concurrent.futures import ThreadPoolExecutor

import pytest

from cranelock_gate.client import certificate, command, may_enter

MEMBERS = ["crane-1", "crane-2", "crane-3"]
# Rejections caused by authority/session fencing, as opposed to ordinary lane occupancy.
FENCED = {"STALE_EPOCH", "STALE_REVISION", "WRONG_HOLDER", "UNKNOWN_EPOCH", "AUTHORITY_CONFLICT",
          "INVALID_GRANT", "ALREADY_ENTERED", "STALE_SESSION", "STALE_OBSERVATION"}
AUTHORITY = ("epoch", "revision", "holder", "mode")


def install_payload(h, holder, epoch, mode="LOCAL", signers=None, state=None):
    state = state or h.state
    actor = "controller" if mode == "CENTRAL" else holder
    if mode == "CENTRAL":
        signers = ["controller"]
    cert = certificate(h.config, h.keys, state, holder, epoch, mode, signers)
    return command(h.config, actor, "InstallAuthority", certificate=cert,
                   expected_epoch=state["epoch"], expected_revision=state["revision"])


def observation(h, lane="CLEAR", actor="simulator", sequence=None, state=None):
    state = state or h.state
    fields = dict(session_id=state["session_id"], observation_revision=state["observation_revision"],
                  sequence=sequence or state["observation_sequence"] + 1,
                  observed_at_ms=int(h.clock() * 1000))
    if actor == "operator":
        return command(h.config, actor, "ClearLane", reason="Operator confirmed empty lane", **fields)
    return command(h.config, actor, "ReportLaneState", lane_state=lane, **fields)


def grant_request(h, actor, state):
    return command(h.config, actor, "RequestGrant", epoch=state["epoch"], revision=state["revision"])


def entry(h, state, grant_id):
    return command(h.config, "simulator", "ValidateEntry", epoch=state["epoch"],
                   revision=state["revision"], holder=state["holder"], grant_id=grant_id,
                   session_id=state["session_id"])


def transfer(h, state, destination):
    return command(h.config, state["holder"], "TransferHolder", epoch=state["epoch"],
                   revision=state["revision"], destination=destination)


def fresh(payload):
    """The same old command resubmitted under a new request ID, bypassing the replay cache."""
    return dict(payload, request_id=str(uuid.uuid4()))


def authority(state):
    return tuple(state[key] for key in AUTHORITY)


@pytest.mark.parametrize("change", ["local_recovery", "central_fallback", "handoff"])
def test_every_old_permission_is_rejected_after_authority_change(h, change):
    establish = install_payload(h, "crane-1", 0, "CENTRAL" if change == "central_fallback" else "LOCAL")
    assert h.send(establish)["allowed"]
    assert h.send(observation(h))["allowed"]

    # Old permissions: a used grant/entry, a never-sent handoff, and old CLEAR evidence.
    state = h.state
    old_grant_request = grant_request(h, "crane-1", state)
    grant = h.send(old_grant_request)["grant"]
    old_entry = entry(h, h.state, grant["id"])
    assert may_enter(h.send(old_entry))
    assert h.send(observation(h))["allowed"]  # crane-1 exits; lane CLEAR again.
    state = h.state
    old_transfer = transfer(h, state, "crane-3")
    # A huge sequence number means only session/phase fencing can stop this evidence.
    old_clear = observation(h, sequence=10**6, state=state)
    old_commands = [establish, old_grant_request, old_entry, old_transfer, old_clear]
    if change != "handoff":  # A handoff requires no outstanding grant.
        pending = h.send(grant_request(h, "crane-1", state))
        assert pending["allowed"]
        old_commands.append(entry(h, h.state, pending["grant"]["id"]))  # Never used.

    if change == "local_recovery":
        assert h.send(install_payload(h, "crane-2", 1))["allowed"]
    elif change == "central_fallback":
        # Fallback keeps the same holder; its CENTRAL-era grant must still die.
        assert h.send(install_payload(h, "crane-1", 1))["allowed"]
    else:
        assert h.send(transfer(h, h.state, "crane-2"))["allowed"]
    assert h.state["grant"] is None

    # Admit the new holder with fresh evidence, then attack while it is inside.
    if h.state["lane_state"] != "CLEAR":
        assert h.send(observation(h, actor="operator"))["allowed"]
    assert h.grant()["allowed"]
    assert may_enter(h.enter())
    before = h.state

    for payload in old_commands:
        for attempt in (payload, fresh(payload)):
            result = h.send(attempt)
            assert not result["action_applied"] and not may_enter(result)
            if not result["replayed"]:
                assert not result["allowed"], payload["operation"]
                assert result["reason"] in FENCED, (payload["operation"], result["reason"])
    after = h.state
    for key in AUTHORITY + ("lane_state", "session_id", "grant"):
        assert after[key] == before[key], key


def test_controller_cannot_reassert_old_authority_after_fallback(h):
    central = install_payload(h, "crane-1", 0, "CENTRAL")
    assert h.send(central)["allowed"]
    assert h.send(observation(h))["allowed"]
    assert h.grant()["allowed"]
    assert may_enter(h.enter())  # crane-1 inside throughout.
    central_snapshot = h.state
    delayed_controller = install_payload(h, "crane-2", 0, "CENTRAL", state=central_snapshot)

    assert h.send(install_payload(h, "crane-1", 1))["allowed"]  # Automatic fallback.
    fallback = h.state
    assert (fallback["mode"], fallback["epoch"], fallback["lane_state"]) == ("LOCAL", 1, "OCCUPIED")
    for payload in (delayed_controller, fresh(central)):
        result = h.send(payload)
        assert not result["allowed"] and result["reason"] == "STALE_EPOCH"
    assert authority(h.state) == authority(fallback)

    # A second LOCAL proposal prepared now must not override the controller's return.
    delayed_local = install_payload(h, "crane-3", 2)
    returned = h.send(install_payload(h, "crane-2", 1, "CENTRAL"))
    assert returned["allowed"]
    assert (returned["state"]["mode"], returned["state"]["epoch"]) == ("CENTRAL", 1)
    assert returned["state"]["lane_state"] == "OCCUPIED"  # Return cannot reset occupancy.
    result = h.send(delayed_local)
    assert not result["allowed"] and result["reason"] == "STALE_REVISION"
    assert h.state["mode"] == "CENTRAL"

    assert h.send(grant_request(h, "crane-1", fallback))["reason"] == "STALE_REVISION"
    assert h.grant()["reason"] == "LANE_BUSY"  # crane-2 waits for crane-1 to leave.
    assert h.send(observation(h))["allowed"]
    assert h.grant()["allowed"]


@pytest.mark.parametrize("crash", ["before_commit", "after_commit"])
def test_gate_crash_midway_through_transfer_applies_it_at_most_once(h, monkeypatch, crash):
    h.ready()
    state = h.state
    handoff = transfer(h, state, "crane-2")
    if crash == "before_commit":
        def disk_failure():
            raise OSError("injected crash during commit")
        monkeypatch.setattr(h.gate, "_commit", disk_failure)
        assert h.send(handoff)["reason"] == "STORAGE_UNAVAILABLE"
        monkeypatch.undo()
    else:
        h.send(handoff)  # Committed, but the reply is lost with the crashed process.
    h.restart()

    restarted = h.state
    expected = "crane-1" if crash == "before_commit" else "crane-2"
    assert restarted["holder"] == expected and restarted["grant"] is None
    assert restarted["lane_state"] == "UNKNOWN"
    for crane in MEMBERS:
        assert not h.send(grant_request(h, crane, restarted))["allowed"]

    assert h.send(observation(h, actor="operator"))["allowed"]
    retry = h.send(handoff)  # The sender retries the identical command.
    assert retry["allowed"] and retry["replayed"] == (crash == "after_commit")
    final = h.state
    assert final["holder"] == "crane-2" and final["revision"] == state["revision"] + 1
    assert h.send(grant_request(h, "crane-1", final))["reason"] == "WRONG_HOLDER"
    assert h.send(grant_request(h, "crane-1", state))["reason"] == "STALE_REVISION"


@pytest.mark.parametrize("order", ["transfer_first", "recovery_first", "concurrent"])
def test_recovery_racing_a_transfer_at_the_same_revision_has_one_winner(h, order):
    h.ready()
    state = h.state
    handoff = transfer(h, state, "crane-2")
    recovery = install_payload(h, "crane-3", state["epoch"] + 1)
    if order == "concurrent":
        barrier = threading.Barrier(2)

        def submit(payload):
            barrier.wait()
            return h.send(payload)
        with ThreadPoolExecutor(2) as pool:
            results = list(pool.map(submit, [handoff, recovery]))
    else:
        first, second = (handoff, recovery) if order == "transfer_first" else (recovery, handoff)
        results = [h.send(first), h.send(second)]
        if order == "recovery_first":
            results.reverse()

    assert sum(result["allowed"] for result in results) == 1
    final = h.state
    if results[0]["allowed"]:
        assert (final["holder"], final["epoch"]) == ("crane-2", state["epoch"])
        assert results[1]["reason"] == "STALE_REVISION"
    else:
        assert (final["holder"], final["epoch"]) == ("crane-3", state["epoch"] + 1)
        assert results[0]["reason"] == "STALE_EPOCH"
    assert final["revision"] == state["revision"] + 1
    for payload in (handoff, recovery):
        assert not h.send(fresh(payload))["allowed"]  # The loser stays fenced forever.


def is_stale(payload, state):
    """True when a command was built against authority or evidence the gate has moved past."""
    operation = payload["operation"]
    current = (state["epoch"], state["revision"])
    if operation in {"RequestGrant", "TransferHolder"}:
        return (payload["epoch"], payload["revision"]) != current
    if operation == "ValidateEntry":
        grant = state["grant"]
        return ((payload["epoch"], payload["revision"]) != current or grant is None
                or grant["entered"] or payload["grant_id"] != grant["id"])
    if operation == "InstallAuthority":
        return (payload["expected_epoch"], payload["expected_revision"]) != current
    return (payload["sequence"] <= state["observation_sequence"]
            or payload["session_id"] != state["session_id"]
            or payload["observation_revision"] != state["observation_revision"])


ACTIONS = {"grant": 3, "enter": 3, "observe": 4, "exit": 2, "operator": 1, "clock": 1,
           "central": 1, "local": 2, "transfer": 1, "crash": 1, "storage_failure": 1,
           "replay": 3, "retry": 1}


@pytest.mark.parametrize("seed", [1, 7, 20, 3104, 9001])
def test_randomised_failures_never_admit_conflicting_access(h, monkeypatch, seed):
    """Model check: a physical lane model plus every failure and stale-command attack."""
    rng = random.Random(seed)
    inside = None  # Physical truth, maintained independently of the gate.
    sent, failed = [], []  # Committed (or denied) commands; commands lost to storage failure.
    seen = Counter()

    def submit(payload):
        nonlocal inside
        before = h.state
        stale = is_stale(payload, before)
        result = h.send(payload)
        if stale and not result["replayed"]:
            assert not result["allowed"], f"Stale {payload['operation']} accepted"
            seen["stale_rejected"] += 1
        if result["replayed"]:
            assert not result["action_applied"]
        if payload["operation"] == "ValidateEntry" and may_enter(result):
            assert inside is None, "Two cranes physically admitted"
            assert result["grant"]["holder"] == before["holder"]
            assert (result["grant"]["epoch"], result["grant"]["revision"]) == (
                before["epoch"], before["revision"])
            inside = before["holder"]
            seen["admitted"] += 1
        if payload["operation"] == "InstallAuthority" and result["action_applied"]:
            new_mode = payload["certificate"]["payload"]["mode"]
            if before["mode"] == "CENTRAL" and new_mode == "LOCAL":
                seen["fallback"] += 1
            if before["mode"] == "LOCAL" and new_mode == "CENTRAL":
                seen["controller_return"] += 1
        return result

    def build(action, state):
        if action == "grant":
            return grant_request(h, state["holder"], state)
        if action == "enter" and state["grant"]:
            return entry(h, state, state["grant"]["id"])
        if action == "transfer":
            return transfer(h, state, rng.choice(MEMBERS))
        if action == "central":
            return install_payload(h, rng.choice(MEMBERS), state["epoch"], "CENTRAL", state=state)
        if action == "local":
            signers = rng.sample(MEMBERS, rng.choice([1, 2, 2, 3]))  # Minorities included.
            return install_payload(h, rng.choice(MEMBERS), state["epoch"] + 1, signers=signers, state=state)
        return None

    assert submit(install_payload(h, "crane-1", 0, "CENTRAL"))["allowed"]
    last = (h.state["epoch"], h.state["revision"])
    names, weights = list(ACTIONS), list(ACTIONS.values())
    for _ in range(400):
        state = h.state
        action = rng.choices(names, weights)[0]
        if action in {"grant", "enter", "transfer", "central", "local"}:
            payload = build(action, state)
            if payload:
                sent.append(payload)
                submit(payload)
        elif action == "observe":
            payload = observation(h, "OCCUPIED" if inside else "CLEAR", state=state)
            sent.append(payload)
            submit(payload)
        elif action == "exit" and inside:
            inside = None  # The crane leaves physically; the gate learns only from evidence.
        elif action == "operator" and inside is None:
            payload = observation(h, actor="operator", state=state)
            sent.append(payload)
            submit(payload)
        elif action == "clock":
            h.clock.advance(rng.choice([0.5, 2.5, 6]))
        elif action == "crash":
            h.restart()
            seen["crash"] += 1
        elif action == "storage_failure":
            payload = build(rng.choice(["grant", "enter", "transfer", "local"]), state)
            if payload:
                def disk_failure():
                    raise OSError("injected storage failure")
                monkeypatch.setattr(h.gate, "_commit", disk_failure)
                result = h.send(payload)
                monkeypatch.undo()
                assert not result["allowed"] and not may_enter(result)
                h.restart()
                failed.append(payload)
                seen["storage_failure"] += 1
        elif action == "replay" and sent:
            old = rng.choice(sent)
            submit(old if rng.random() < 0.5 else fresh(old))
        elif action == "retry" and failed:
            payload = failed.pop(rng.randrange(len(failed)))
            sent.append(payload)
            submit(payload)  # Retried after the gate restarted.

        state = h.state
        if inside is not None:
            assert state["lane_state"] in {"OCCUPIED", "UNKNOWN"}, "Occupied lane reported free"
        assert state["holder"] in MEMBERS
        if state["grant"]:
            assert state["grant"]["holder"] == state["holder"]
            assert (state["grant"]["epoch"], state["grant"]["revision"]) == (
                state["epoch"], state["revision"])
        current = (state["epoch"], state["revision"])
        assert current >= last, "Authority moved backwards"
        last = current

    # The run must actually exercise every hazard it claims to cover.
    for hazard in ("admitted", "stale_rejected", "fallback", "controller_return",
                   "crash", "storage_failure"):
        assert seen[hazard] > 0, (hazard, dict(seen))
