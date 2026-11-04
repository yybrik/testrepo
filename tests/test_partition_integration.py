import asyncio
import json

import pytest

from cranelock_gate.storage import ProcessLease
from cranelock_sim.failures import run_partition_suite
from cranelock_sim.network import PeerNetwork


def events(path):
    return [json.loads(line) for line in path.read_text().splitlines()]


@pytest.fixture(scope="module")
def partition_runs(tmp_path_factory):
    output = tmp_path_factory.mktemp("partition") / "suite"
    summary = asyncio.run(run_partition_suite(output, crossing_time=0.01, clearance_delay=0.05))
    return output, summary


@pytest.mark.parametrize("name", ["majority-before-entry", "majority-during-occupancy", "no-majority"])
def test_agents_keep_running_while_cross_group_peer_rpcs_are_blocked(partition_runs, name):
    output, summary = partition_runs
    result = summary["scenarios"][name]
    assert result["passed"] and result["blocked_peer_rpcs"] > 0
    trace = events(output / name / "events.jsonl")
    assert not any(event["event"] == "holder_stopped" for event in trace)
    assert any(event["event"] == "check_passed" and event["check"] == "all_agents_still_running"
               for event in trace)
    split = next(event for event in trace if event["event"] == "network_partitioned")
    groups = {member: group for group, members in enumerate(split["groups"]) for member in members}
    blocked = [event for event in trace if event["event"] == "peer_rpc_blocked"]
    assert {event["method"] for event in blocked} >= {"Heartbeat", "VoteRecovery", "TokenRequest", "TokenTransfer"}
    assert all(groups[event["sender"]] != groups[event["receiver"]] for event in blocked)
    assert all(event["time"] >= split["time"] for event in blocked)
    assert not (output / name / "failure.json").exists()
    lease = ProcessLease(output / name / "gate.sqlite")
    lease.close()


@pytest.mark.parametrize("name", ["majority-before-entry", "majority-during-occupancy"])
def test_majority_recovers_and_live_isolated_holder_is_fenced(partition_runs, name):
    output, summary = partition_runs
    path = output / name
    result = summary["scenarios"][name]
    assert result["certificate_signers"] == ["crane-2", "crane-3"]
    assert result["recovered_epoch"] == 1 and result["replacement_holder"] == "crane-2"
    assert result["completed_replacement_crossings"] == 1
    assert result["final_lane_state"] == "CLEAR" and result["safety_violations"] == 0
    trace = events(path / "events.jsonl")
    snapshot = next(event for event in trace if event["event"] == "agent_snapshot"
                    and event["stage"] == "after_recovery")
    assert snapshot["authority_epoch"] == 1
    assert snapshot["owner"] == "crane-2"
    # Both remain alive claiming a token, but A's epoch-zero claim is fenced.
    assert snapshot["agents"]["crane-1"]["has_token"]
    assert snapshot["agents"]["crane-1"]["epoch"] == 0
    assert snapshot["agents"]["crane-2"]["has_token"]
    assert snapshot["agents"]["crane-2"]["epoch"] == 1
    audit = events(path / "gate-events.jsonl")
    assert any(event["operation"] == "RequestGrant" and event["reason"] == "STALE_EPOCH" for event in audit)
    assert any(event["operation"] == "ValidateEntry" and event["reason"] == "STALE_EPOCH" for event in audit)
    assert any(event["operation"] == "InstallAuthority" and event["reason"] == "NO_QUORUM" for event in audit)
    epoch_one_entries = [event for event in audit if event["operation"] == "ValidateEntry"
                         and event["reason"] == "OK" and event["epoch"] == 1]
    assert len(epoch_one_entries) == 1 and epoch_one_entries[0]["holder"] == "crane-2"
    for member in ("crane-2", "crane-3"):
        assert "1" in json.loads((path / f"{member}.json").read_text())["vote_state"]


def test_partition_recovery_preserves_occupant_until_explicit_clearance(partition_runs):
    output, _ = partition_runs
    trace = events(output / "majority-during-occupancy" / "events.jsonl")
    occupant = None
    for event in trace:
        if event["event"] == "physical_entry":
            assert occupant is None
            occupant = event["crane"]
        elif event["event"] == "physical_exit":
            assert occupant == event["crane"]
            occupant = None
        elif event["event"] == "check_passed" and event["check"] == "occupied_lane_blocks_replacement":
            assert occupant == "crane-1"
        elif event["event"] == "trusted_simulator_removal":
            assert occupant == "crane-1"
    assert occupant is None


def test_all_isolated_cannot_install_new_authority(partition_runs):
    output, summary = partition_runs
    result = summary["scenarios"]["no-majority"]
    assert result["passed"] and not result["recovered"]
    assert result["recovery_latency_seconds"] is None and result["recovered_epoch"] is None
    assert result["completed_replacement_crossings"] == 0
    assert result["final_epoch"] == 0 and result["final_holder"] == "crane-1"
    path = output / "no-majority"
    trace = events(path / "events.jsonl")
    assert not any(event["event"] == "physical_entry" for event in trace)
    rejections = [event["agent_event"] for event in trace if event["event"] == "agent_event"
                  and event["agent_event"]["event"] == "RECOVERY_NO_QUORUM"]
    assert {event["agent_id"] for event in rejections} == {"crane-2", "crane-3"}
    assert all(event["votes"] == 1 and event["required"] == 2 for event in rejections)
    audit = events(path / "gate-events.jsonl")
    assert not any(event["epoch"] > 0 for event in audit)
    assert any(event["reason"] == "NO_QUORUM" for event in audit)


@pytest.mark.parametrize("groups", [[["a"], ["a", "b", "c"]], [["a"], ["b"]],
                                   [["a", "b", "c"], []]])
def test_invalid_partition_never_changes_connectivity(groups):
    network = PeerNetwork(["a", "b", "c"], lambda *args, **kwargs: None)
    before = dict(network.groups)
    with pytest.raises(ValueError):
        network.partition(groups)
    assert network.groups == before


def test_unknown_partition_scenario_creates_no_results(tmp_path):
    output = tmp_path / "unknown"
    with pytest.raises(ValueError):
        asyncio.run(run_partition_suite(output, scenario="typo"))
    assert not output.exists()
