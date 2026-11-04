import asyncio
import base64
import json

import pytest

from cranelock.agent import Agent
from cranelock_gate.storage import ProcessLease
from cranelock_gate.config import Config
from cranelock_gate.crypto import CERTIFICATE_DOMAIN, verify
from cranelock_sim.failures import run_recovery_scenario, run_recovery_suite


def read_events(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


@pytest.mark.parametrize("scenario", ["before-entry", "during-occupancy"])
def test_actual_holder_detection_quorum_fencing_and_safe_progress(tmp_path, scenario):
    output = tmp_path / scenario
    progress = []
    summary = asyncio.run(run_recovery_scenario(output, scenario,
        crossing_time=0.01, clearance_delay=0.05, progress=progress.append))
    assert summary["passed"]
    assert summary["recovered_epoch"] == 1
    assert summary["certificate_signers"] == ["crane-2", "crane-3"]
    config = Config.load(output / "gate-config.json")
    certificate = json.loads((output / "recovery-certificate.json").read_text())
    assert all(verify(config.members[vote["signer_id"]], base64.b64decode(vote["signature"]),
                      certificate["payload"], CERTIFICATE_DOMAIN)
               for vote in certificate["signatures"])
    assert summary["final_lane_state"] == "CLEAR"
    assert summary["completed_replacement_crossings"] == 1
    assert summary["safety_violations"] == 0
    assert 0 <= summary["failure_detection_seconds"] < 5
    assert summary["authority_recovery_seconds"] <= summary["recovery_latency_seconds"]
    assert summary["failure_to_safe_entry_seconds"] == pytest.approx(
        summary["failure_detection_seconds"] + summary["recovery_latency_seconds"])
    assert all(line.startswith("[PASS]") for line in progress)
    events = read_events(output / "events.jsonl")
    crashed = next(event["time"] for event in events if event["event"] == "holder_stopped")
    detections = [event for event in events if event["event"] == "agent_event" and
        event["agent_event"]["event"] == "TOKEN_HOLDER_SUSPECTED"]
    assert {event["agent_event"]["agent_id"] for event in detections} == {"crane-2", "crane-3"}
    assert all(event["time"] >= crashed for event in detections)
    votes = [event["agent_event"] for event in events if event["event"] == "agent_event"
             and event["agent_event"]["event"] == "RECOVERY_VOTE_CAST"]
    # Proposer records its own durable vote directly; C signs through VoteRecovery RPC.
    assert any(event["agent_id"] == "crane-3" for event in votes)
    for member in ("crane-2", "crane-3"):
        assert "1" in json.loads((output / f"{member}.json").read_text())["vote_state"]
    old = json.loads((output / "crane-1.json").read_text())
    assert old["epoch"] == 0 and old["has_token"]  # Stale disk state is never promoted.
    gate_events = read_events(output / "gate-events.jsonl")
    assert any(event["operation"] == "RequestGrant" and event["reason"] == "STALE_EPOCH"
               for event in gate_events)
    assert any(event["operation"] == "ValidateEntry" and event["reason"] == "STALE_EPOCH"
               for event in gate_events)
    accepted_entries = [event for event in gate_events if event["operation"] == "ValidateEntry"
                        and event["reason"] == "OK" and not event["replayed"]]
    entries = [event for event in events if event["event"] == "physical_entry"]
    assert len(entries) == len(accepted_entries) == (2 if scenario == "during-occupancy" else 1)
    issued = [event for event in events if event["event"] == "grant_issued"]
    assert {event["grant_id"] for event in accepted_entries} <= {event["grant_id"] for event in issued}
    occupant = None
    for event in events:
        if event["event"] == "physical_entry":
            assert occupant is None
            occupant = event["crane"]
        elif event["event"] == "physical_exit":
            assert event["crane"] == occupant
            occupant = None
        elif event["event"] == "check_passed" and event["check"] == "occupied_lane_blocks_replacement":
            assert occupant == "crane-1"
    assert occupant is None
    if scenario == "during-occupancy":
        blocked = next(event["time"] for event in events if event["event"] == "check_passed"
                       and event["check"] == "occupied_lane_blocks_replacement")
        removal = next(event["time"] for event in events if event["event"] == "trusted_simulator_removal")
        new_entry = next(event["time"] for event in entries if event["crane"] == "crane-2")
        assert new_entry > removal > blocked
        assert summary["blocked_until_clearance_seconds"] >= 0.05
    else:
        assert summary["blocked_until_clearance_seconds"] is None
    assert not (output / "failure.json").exists()
    lease = ProcessLease(output / "gate.sqlite")
    lease.close()


def test_no_certificate_does_not_report_success_or_promote_new_holder(tmp_path, monkeypatch):
    async def no_certificate(self, gate_state, holder=None):
        return None

    monkeypatch.setattr(Agent, "propose_recovery", no_certificate)
    output = tmp_path / "failed"
    with pytest.raises(RuntimeError, match="majority failed"):
        asyncio.run(run_recovery_scenario(output, "before-entry"))
    assert not (output / "summary.json").exists()
    assert json.loads((output / "failure.json").read_text())["error"] == "RuntimeError"
    assert not any(event["event"] == "physical_entry" for event in read_events(output / "events.jsonl"))
    for member in ("crane-2", "crane-3"):
        state = json.loads((output / f"{member}.json").read_text())
        assert not state["has_token"] and state["epoch"] == 0
    lease = ProcessLease(output / "gate.sqlite")
    lease.close()


def test_suite_refuses_existing_results(tmp_path):
    marker = tmp_path / "summary.json"
    marker.write_text("keep")
    with pytest.raises(FileExistsError):
        asyncio.run(run_recovery_suite(tmp_path))
    assert marker.read_text() == "keep"


@pytest.mark.parametrize("options", [dict(scenario="typo"), dict(detection_timeout=0.05),
    dict(detection_timeout=float("inf")), dict(clearance_delay=-1), dict(crossing_time=2)])
def test_invalid_scenario_options_never_create_output(tmp_path, options):
    output = tmp_path / "invalid"
    arguments = dict(scenario="before-entry")
    arguments.update(options)
    with pytest.raises(ValueError):
        asyncio.run(run_recovery_scenario(output, **arguments))
    assert not output.exists()
