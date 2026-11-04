import asyncio
import csv
import json

import pytest

from cranelock.agent import Agent
from cranelock_gate.storage import ProcessLease
from cranelock_sim.failures import run_restart_suite


def events(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


@pytest.fixture(scope="module")
def restarted_runs(tmp_path_factory):
    output = tmp_path_factory.mktemp("restart") / "suite"
    summary = asyncio.run(run_restart_suite(output, crossing_time=0.01, clearance_delay=0.01))
    return output, summary


@pytest.mark.parametrize("scenario", ["before-entry", "during-occupancy"])
def test_stale_holder_restarts_blocked_then_rejoins_via_actual_token_transfer(restarted_runs, scenario):
    output, summary = restarted_runs
    path = output / scenario
    result = summary["scenarios"][scenario]
    assert result["passed"] and result["restarted"]
    assert result["replacement_holder"] == "crane-2" and result["final_holder"] == "crane-1"
    assert result["completed_replacement_crossings"] == result["completed_rejoined_crossings"] == 1
    assert result["recovered_epoch"] == 1 and result["final_lane_state"] == "CLEAR"
    assert result["safety_violations"] == 0
    assert 0 <= result["restart_ready_to_reconciled_seconds"] < result["restart_ready_to_safe_entry_seconds"]
    saved = json.loads((path / "crane-1.before-restart.json").read_text())
    assert saved["has_token"] and saved["epoch"] == 0 and saved["holder"] == "crane-1"
    final = json.loads((path / "crane-1.json").read_text())
    assert final["epoch"] == 1 and final["holder"] == "crane-1"
    trace = events(path / "events.jsonl")
    startup = next(event for event in trace if event["event"] == "cluster_started")
    restart = next(event for event in trace if event["event"] == "agent_restarted")
    assert restart["address"] == startup["addresses"]["crane-1"]
    for stage in ("restarted_before_reconciliation", "restarted_after_heartbeat"):
        snapshot = next(event for event in trace if event["event"] == "agent_snapshot" and event["stage"] == stage)
        state = snapshot["agents"]["crane-1"]
        assert state["mode"] == "REJOINING" and not state["has_token"] and state["epoch"] == 0
        assert snapshot["owner"] == "crane-2"
    reconciled = next(event for event in trace if event["event"] == "agent_snapshot"
                      and event["stage"] == "restarted_after_reconciliation")
    assert reconciled["agents"]["crane-1"]["mode"] == "LOCAL"
    assert not reconciled["agents"]["crane-1"]["has_token"]
    assert reconciled["agents"]["crane-1"]["epoch"] == 1
    transfers = [event for event in trace if event["event"] == "agent_event"
                 and event["agent_event"]["event"] == "TOKEN_TRANSFER_RECEIVED"
                 and event["agent_event"]["agent_id"] == "crane-1"]
    assert len(transfers) == 1 and transfers[0]["time"] > reconciled["time"]
    assert transfers[0]["agent_event"]["peer_id"] == "crane-2"
    assert transfers[0]["agent_event"]["epoch"] == 1
    audit = events(path / "gate-events.jsonl")
    assert any(event["operation"] == "ValidateEntry" and event["reason"] == "STALE_EPOCH"
               and event["timestamp"] for event in audit)
    accepted = [event for event in audit if event["operation"] == "ValidateEntry"
                and event["reason"] == "OK" and not event["replayed"]]
    assert len(accepted) == (2 if scenario == "before-entry" else 3)
    assert [(event["holder"], event["epoch"]) for event in accepted[-2:]] == [("crane-2", 1), ("crane-1", 1)]
    with (path / "requests.csv").open(newline="") as stream:
        rows = list(csv.DictReader(stream))
    rejoined = next(row for row in rows if row["request_id"] == "rejoined-holder")
    assert rejoined["status"] == "completed"
    assert float(rejoined["entered_at"]) > transfers[0]["time"]
    assert not (path / "failure.json").exists()
    lease = ProcessLease(path / "gate.sqlite")
    lease.close()


def test_failed_reconciliation_never_allows_restarted_agent_to_move(tmp_path, monkeypatch):
    reconcile = Agent.reconcile

    async def reject_rejoin(self, *args, **kwargs):
        if self.state.agent_id == "crane-1":
            raise RuntimeError("injected reconciliation failure")
        return await reconcile(self, *args, **kwargs)

    monkeypatch.setattr(Agent, "reconcile", reject_rejoin)
    output = tmp_path / "failed"
    with pytest.raises(RuntimeError, match="reconciliation failure"):
        asyncio.run(run_restart_suite(output, scenario="before-entry", crossing_time=0.01))
    path = output / "before-entry"
    assert not (path / "summary.json").exists() and not (output / "summary.json").exists()
    assert (path / "failure.json").is_file()
    assert not any(event["event"] == "physical_entry" and event["crane"] == "crane-1"
                   for event in events(path / "events.jsonl"))
    assert not json.loads((path / "crane-1.json").read_text())["has_token"]
    lease = ProcessLease(path / "gate.sqlite")
    lease.close()


def test_restart_refuses_existing_results(tmp_path):
    marker = tmp_path / "summary.json"
    marker.write_text("keep")
    with pytest.raises(FileExistsError):
        asyncio.run(run_restart_suite(tmp_path))
    assert marker.read_text() == "keep"
