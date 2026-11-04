"""Agent integration against Max's unchanged gate over real signed gRPC."""
import asyncio
from contextlib import asynccontextmanager
import json

import grpc
import pytest

from cranelock.agent import Agent, Mode
from cranelock.gate_client import GateClient
from cranelock import protocol_pb2 as pb
from cranelock.protocol_pb2_grpc import CraneAgentStub
from cranelock_gate.server import RunningServer


@asynccontextmanager
async def cluster(h, tmp_path, count=2):
    server = RunningServer(h.gate, "127.0.0.1:0")
    agents, events = [], []
    try:
        for identity in list(h.config.members)[:count]:
            client = GateClient(grpc.insecure_channel(f"127.0.0.1:{server.port}"),
                                h.config, h.keys, identity, timeout=0.3)
            agent = Agent(identity, {p: "localhost:1" for p in h.config.members if p != identity},
                          "crane-1", epoch=0, gate=client, event_sink=events.append,
                          state_file=tmp_path / f"{identity}.json")
            agents.append(agent)
            port = await agent.start("127.0.0.1:0", auto_transfer=False)
            agent.test_port = port
        for a in agents:
            for b in agents:
                if a is not b:
                    await a.channels[b.state.agent_id].close()
                    channel = grpc.aio.insecure_channel(f"127.0.0.1:{b.test_port}")
                    a.channels[b.state.agent_id] = channel
                    a.stubs[b.state.agent_id] = CraneAgentStub(channel)
        yield agents, events
    finally:
        for agent in agents:
            await agent.close()
        server.close()


def test_gate_authorized_handoff(h, tmp_path):
    h.ready()
    async def scenario():
        async with cluster(h, tmp_path) as ([a, b], events):
            assert a.state.has_token and not b.state.has_token
            assert (await b.request_token()).accepted
            assert await a.transfer_next()
            assert h.state["holder"] == "crane-2"
            assert not a.state.has_token and b.state.has_token
            assert (a.state.current_epoch, a.state.revision) == (b.state.current_epoch, b.state.revision) == (0, 2)
            required = {"GATE_STATE_QUERIED", "GATE_STATE_RECONCILED", "GATE_TRANSFER_REQUESTED",
                        "GATE_TRANSFER_ACCEPTED", "AUTHORITY_UPDATED"}
            assert required <= {e["event"] for e in events}
            assert all("revision" in e and "monotonic" in e and "holder" in e for e in events)
    asyncio.run(scenario())


def test_gate_rejects_unknown_lane_without_granting_receiver(h, tmp_path):
    assert h.install()["allowed"]  # Deliberately no physical clearance.
    async def scenario():
        async with cluster(h, tmp_path) as ([a, b], events):
            assert (await b.request_token()).accepted
            assert not await a.transfer_next()
            assert h.state["holder"] == "crane-1"
            assert a.state.has_token and not b.state.has_token
            assert list(a.state.request_queue) == ["crane-2"]
            assert any(e["event"] == "GATE_TRANSFER_REJECTED" and e["reason"] == "LANE_UNKNOWN" for e in events)
    asyncio.run(scenario())


@pytest.mark.parametrize("epoch,revision", [(0, 1), (0, 100)])
def test_old_transfer_never_overwrites_new_authority(h, tmp_path, epoch, revision):
    h.ready()
    assert h.install("crane-2")["allowed"]  # Epoch 1 supersedes even revision 100 at epoch 0.
    async def scenario():
        async with cluster(h, tmp_path) as ([a, b], events):
            before = (b.state.current_epoch, b.state.revision, b.state.token_holder)
            reply = await b.TokenTransfer(pb.TokenTransferMessage(sender_id="crane-1",
                receiver_id="crane-2", holder="crane-2", epoch=epoch,
                revision=revision, transfer_id="old"), None)
            assert not reply.accepted
            assert (b.state.current_epoch, b.state.revision, b.state.token_holder) == before
    asyncio.run(scenario())


def test_older_same_epoch_revision_and_forged_newer_notification(h, tmp_path):
    h.ready()
    async def scenario():
        async with cluster(h, tmp_path) as ([a, b], events):
            assert (await b.request_token()).accepted
            assert await a.transfer_next()
            old = pb.TokenTransferMessage(sender_id="crane-1", receiver_id="crane-2",
                holder="crane-2", epoch=0, revision=1, transfer_id="old-revision")
            assert not (await b.TokenTransfer(old, None)).accepted
            assert b.state.revision == 2
            fake = pb.TokenTransferMessage(sender_id="crane-2", receiver_id="crane-1",
                holder="crane-1", epoch=0, revision=3, transfer_id="not-at-gate")
            assert not (await a.TokenTransfer(fake, None)).accepted
            assert not a.state.has_token
            assert a.state.revision == 2
    asyncio.run(scenario())


@pytest.mark.parametrize("authoritative_holder", ["crane-1", "crane-2"])
def test_restart_reconciles_actual_gate_not_persisted_ownership(h, tmp_path, authoritative_holder):
    h.ready()
    assert h.install(authoritative_holder)["allowed"]
    async def scenario():
        path = tmp_path / "crane-1.json"
        path.write_text(json.dumps(dict(agent_id="crane-1", epoch=0, revision=1,
                                       holder="crane-1", has_token=True, vote_state={"opaque": 7})))
        async with cluster(h, tmp_path) as ([a, b], events):
            assert a.state.current_epoch == 1 and a.state.revision == 2
            assert a.state.token_holder == authoritative_holder
            assert a.state.has_token == (authoritative_holder == "crane-1")
            assert a.state.mode == Mode.LOCAL and a.vote_state == {"opaque": 7}
            assert any(e["event"] == "AGENT_STARTED" and e["mode"] == "REJOINING" for e in events)
            saved = json.loads(path.read_text())
            assert saved["epoch"] == 1 and saved["revision"] == 2
    asyncio.run(scenario())


def test_gate_behind_persisted_revision_blocks_rejoin(h, tmp_path):
    h.ready()
    async def scenario():
        (tmp_path / "crane-1.json").write_text(json.dumps(dict(agent_id="crane-1",
            epoch=0, revision=10, holder="crane-1", has_token=True)))
        async with cluster(h, tmp_path) as ([a, b], events):
            assert a.state.revision == 10 and not a.state.has_token
            assert a.state.mode == Mode.REJOINING
            assert any(e.get("result") == "INCONSISTENT_OLDER_GATE" for e in events)
    asyncio.run(scenario())


def test_lost_committed_reply_resolves_from_getstate(h, tmp_path):
    h.ready()
    async def scenario():
        async with cluster(h, tmp_path) as ([a, b], events):
            real_transfer = a.gate.transfer
            async def lost(payload):
                result = await real_transfer(payload)
                assert result["allowed"]
                raise RuntimeError("Injected lost response after commit")
            a.gate.transfer = lost
            assert (await b.request_token()).accepted
            assert await a.transfer_next()
            assert not a.state.has_token and b.state.has_token
            assert h.state["holder"] == "crane-2"
            assert any(e["event"] == "GATE_TRANSFER_UNCERTAIN" for e in events)
            assert any(e["event"] == "GATE_TRANSFER_RESOLVED" and e["result"] == "COMMITTED" for e in events)
    asyncio.run(scenario())


def test_lost_uncommitted_reply_retries_identical_command(h, tmp_path):
    h.ready()
    async def scenario():
        async with cluster(h, tmp_path) as ([a, b], events):
            real_transfer, payloads = a.gate.transfer, []
            async def lost_once(payload):
                payloads.append(dict(payload))
                if len(payloads) == 1:
                    raise RuntimeError("Injected failure before gate commit")
                return await real_transfer(payload)
            a.gate.transfer = lost_once
            assert (await b.request_token()).accepted
            assert not await a.transfer_next()
            assert not a.state.has_token and not b.state.has_token
            assert await a.transfer_next()
            assert payloads[0] == payloads[1]
            assert b.state.has_token and not a.state.has_token
    asyncio.run(scenario())


def test_committed_but_gate_unreachable_keeps_sender_suspended(h, tmp_path):
    h.ready()
    async def scenario():
        async with cluster(h, tmp_path) as ([a, b], events):
            real_transfer, real_state = a.gate.transfer, a.gate.state
            async def unavailable():
                raise RuntimeError("Injected GetState outage")
            async def lost(payload):
                await real_transfer(payload)
                a.gate.state = unavailable
                raise RuntimeError("Injected response loss")
            a.gate.transfer = lost
            assert (await b.request_token()).accepted
            assert not await a.transfer_next()
            assert not a.state.has_token and not b.state.has_token
            assert a.state.mode == Mode.REJOINING and h.state["holder"] == "crane-2"
            a.gate.state = real_state
            assert await a.transfer_next()
            assert b.state.has_token and not a.state.has_token
    asyncio.run(scenario())


def test_concurrent_handoffs_and_fifo_through_three_members(h, tmp_path):
    h.ready()
    async def scenario():
        async with cluster(h, tmp_path, count=3) as ([a, b, c], events):
            assert (await b.request_token()).accepted
            assert (await c.request_token()).accepted
            assert list(a.state.request_queue) == ["crane-2", "crane-3"]
            results = await asyncio.gather(*(a.transfer_next() for _ in range(12)))
            assert sum(results) == 1
            assert list(b.state.request_queue) == ["crane-3"]
            assert await b.transfer_next()
            assert c.state.has_token and not a.state.has_token and not b.state.has_token
            assert h.state["holder"] == "crane-3" and h.state["revision"] == 3
            transitions = [e for e in h.gate.events() if e["operation"] == "TransferHolder"]
            assert len(transitions) == 2
    asyncio.run(scenario())


def test_same_version_holder_disagreement_uses_gate(h, tmp_path):
    h.ready()
    async def scenario():
        path = tmp_path / "crane-2.json"
        path.write_text(json.dumps(dict(agent_id="crane-2", epoch=0, revision=1,
                                       holder="crane-2", has_token=True)))
        async with cluster(h, tmp_path) as ([a, b], events):
            assert b.state.token_holder == "crane-1" and not b.state.has_token
            assert b.state.revision == 1
    asyncio.run(scenario())


def test_gate_behind_persisted_epoch_blocks_rejoin(h, tmp_path):
    h.ready()
    async def scenario():
        (tmp_path / "crane-1.json").write_text(json.dumps(dict(agent_id="crane-1",
            epoch=4, revision=1, holder="crane-1", has_token=True)))
        async with cluster(h, tmp_path) as ([a, b], events):
            assert a.state.current_epoch == 4 and not a.state.has_token
            assert a.state.mode == Mode.REJOINING
    asyncio.run(scenario())


def test_gate_wait_does_not_hold_state_lock_or_accept_more_requests(h, tmp_path):
    h.ready()
    async def scenario():
        async with cluster(h, tmp_path) as ([a, b], events):
            entered, release = asyncio.Event(), asyncio.Event()
            real_transfer = a.gate.transfer
            async def delayed(payload):
                entered.set()
                await release.wait()
                return await real_transfer(payload)
            a.gate.transfer = delayed
            assert (await b.request_token()).accepted
            task = asyncio.create_task(a.transfer_next())
            try:
                await asyncio.wait_for(entered.wait(), 2)
                state = await asyncio.wait_for(a.GetAgentState(pb.Empty(), None), 0.3)
                assert not state.has_token
                request = pb.TokenRequestMessage(agent_id="crane-3", epoch=0, revision=1)
                assert not (await a.TokenRequest(request, None)).accepted
            finally:
                release.set()
                assert await task
            assert not a.state.has_token and b.state.has_token
    asyncio.run(scenario())


def test_historical_success_never_overrides_later_gate_authority(h, tmp_path):
    h.ready()
    async def scenario():
        async with cluster(h, tmp_path) as ([a, b], events):
            real_transfer = a.gate.transfer
            async def already_moved(payload):
                assert (await real_transfer(payload))["allowed"]
                assert h.call("crane-2", "TransferHolder", epoch=0, revision=2,
                              destination="crane-3")["allowed"]
                # Same request returns historical allowed but CURRENT authority.
                replay = await real_transfer(payload)
                assert replay["allowed"] and replay["replayed"]
                return replay
            a.gate.transfer = already_moved
            assert (await b.request_token()).accepted
            assert not await a.transfer_next()
            assert not a.state.has_token and not b.state.has_token
            assert a.state.token_holder == "crane-3" and a.state.revision == 3
    asyncio.run(scenario())


def test_receiver_requires_live_gate_confirmation(h, tmp_path):
    h.ready()
    async def scenario():
        async with cluster(h, tmp_path) as ([a, b], events):
            real_state = b.gate.state
            async def unavailable():
                raise RuntimeError("Injected receiver gate outage")
            assert (await b.request_token()).accepted
            b.gate.state = unavailable
            assert not await a.transfer_next()
            assert not a.state.has_token and not b.state.has_token
            b.gate.state = real_state
            assert await b.reconcile_from_gate()
            assert await a.transfer_next()
            assert b.state.has_token
    asyncio.run(scenario())
