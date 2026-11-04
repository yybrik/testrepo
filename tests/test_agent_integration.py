import asyncio
import csv
import json
from types import SimpleNamespace

import pytest

from cranelock import protocol_pb2 as pb
from cranelock.agent import Agent
from cranelock_sim.agents import run_agent_demo, token_owner
from cranelock_sim.lane import SafetyViolation


def test_replayed_handoff_does_not_revert_holder_after_a_cycle():
    async def scenario():
        agent = Agent("observer", {"a": "127.0.0.1:1", "b": "127.0.0.1:2"},
                      "a", epoch=0, event_sink=lambda event: None)
        try:
            def announce(sender, receiver, transfer):
                return pb.HeartbeatMessage(agent_id=sender, epoch=0,
                    token_holder=receiver, has_token=False, mode=pb.LOCAL,
                    outgoing_transfer_id=transfer)
            old = announce("a", "b", "a-to-b-first")
            assert (await agent.Heartbeat(old, None)).accepted
            assert agent.state.token_holder == "b"
            await agent.Heartbeat(announce("b", "a", "b-to-a"), None)
            assert agent.state.token_holder == "a"
            await agent.Heartbeat(old, None)
            assert agent.state.token_holder == "a"
            await agent.Heartbeat(announce("a", "b", "a-to-b-second"), None)
            assert agent.state.token_holder == "b"
        finally:
            await agent.close()
    asyncio.run(scenario())


def test_received_transfer_also_fences_old_holder_announcement():
    async def scenario():
        agent = Agent("a", {"b": "127.0.0.1:1"}, "b", epoch=0,
                      event_sink=lambda event: None)
        try:
            assert (await agent.TokenTransfer(pb.TokenTransferMessage(
                sender_id="b", receiver_id="a", epoch=0, transfer_id="old"), None)).accepted
            # Model the later relinquishment to B before its next transfer arrives.
            agent.state.has_token = False
            agent.state.token_holder = "b"
            await agent.Heartbeat(pb.HeartbeatMessage(agent_id="b", epoch=0,
                token_holder="a", has_token=False, mode=pb.LOCAL,
                outgoing_transfer_id="old"), None)
            assert agent.state.token_holder == "b"
        finally:
            await agent.close()
    asyncio.run(scenario())


def test_announcement_before_transfer_does_not_block_receiver():
    async def scenario():
        agent = Agent("b", {"a": "127.0.0.1:1"}, "a", epoch=0,
                      event_sink=lambda event: None)
        try:
            await agent.Heartbeat(pb.HeartbeatMessage(agent_id="a", epoch=0,
                token_holder="b", has_token=False, mode=pb.LOCAL,
                outgoing_transfer_id="handoff"), None)
            assert not agent.state.has_token
            assert agent.state.token_holder == "a"
            result = await agent.TokenTransfer(pb.TokenTransferMessage(sender_id="a",
                receiver_id="b", epoch=0, transfer_id="handoff"), None)
            assert result.accepted
            assert agent.state.has_token
        finally:
            await agent.close()
    asyncio.run(scenario())


def test_three_agents_real_grpc_handoff_and_physical_entry(tmp_path):
    output = tmp_path / "agents"
    summary = asyncio.run(run_agent_demo(output, rounds=2, crossing_time=0.01))
    assert summary["completed"] == 6
    assert summary["completed_by_crane"] == {f"crane-{i}": 2 for i in range(1, 4)}
    assert summary["final_holder"] == "crane-3"
    assert summary["final_lane_state"] == "CLEAR"
    assert summary["safety_violations"] == 0
    events = [json.loads(line) for line in (output / "events.jsonl").read_text().splitlines()]
    entries = [event for event in events if event["event"] == "physical_entry"]
    assert [event["crane"] for event in entries] == ["crane-1", "crane-2", "crane-3"] * 2
    transfers = [event["agent_event"] for event in events
                 if event["event"] == "agent_event"
                 and event["agent_event"]["event"] == "TOKEN_TRANSFER_RECEIVED"]
    assert len(transfers) == 5
    assert len({event["transfer_id"] for event in transfers}) == 5
    assert sum(event["event"] == "nonholder_denied" for event in events) == 4
    gate_events = [json.loads(line) for line in (output / "gate-events.jsonl").read_text().splitlines()]
    accepted = [event for event in gate_events if event["operation"] == "ValidateEntry"
                and event["reason"] == "OK" and not event["replayed"]]
    issued = [event for event in events if event["event"] == "grant_issued"]
    assert len(accepted) == len(entries) == len(issued) == 6
    assert {event["grant_id"] for event in accepted} == {event["grant_id"] for event in issued}
    assert {event["request_id"] for event in entries} == {event["request_id"] for event in issued}
    occupied = None
    for event in events:
        if event["event"] == "physical_entry":
            assert occupied is None
            occupied = event["crane"]
        elif event["event"] == "physical_exit":
            assert occupied == event["crane"]
            occupied = None
        elif event["event"] == "gate_handoff":
            assert occupied is None
        elif event["event"] == "agent_snapshot":
            owners = [identity for identity, state in event["agents"].items() if state["has_token"]]
            assert len(owners) == 1
            if occupied:
                assert owners == [occupied]
    assert occupied is None
    with (output / "requests.csv").open(newline="") as stream:
        rows = list(csv.DictReader(stream))
    assert len(rows) == 6
    assert all(float(row["requested_at"]) <= float(row["entered_at"]) <
               float(row["exited_at"]) for row in rows)
    assert [event["time"] for event in events] == sorted(event["time"] for event in events)
    assert not (output / "failure.json").exists()


def test_duplicate_token_checker_rejects_two_owners():
    with pytest.raises(SafetyViolation, match="Multiple agents"):
        token_owner({"a": SimpleNamespace(has_token=True), "b": SimpleNamespace(has_token=True)})


def test_agent_demo_retains_failure_evidence(tmp_path, monkeypatch):
    async def fail_request(self):
        raise TimeoutError("injected token request timeout")

    monkeypatch.setattr(Agent, "request_token", fail_request)
    output = tmp_path / "failed"
    with pytest.raises(TimeoutError):
        asyncio.run(run_agent_demo(output, rounds=1, crossing_time=0.01))
    assert json.loads((output / "failure.json").read_text())["error"] == "TimeoutError"
    assert not (output / "summary.json").exists()
    assert (output / "events.jsonl").is_file()
    # All servers and the database lease were released even after failure.
    from cranelock_gate.storage import ProcessLease
    lease = ProcessLease(output / "gate.sqlite")
    lease.close()
