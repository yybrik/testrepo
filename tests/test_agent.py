import asyncio
import pytest
from cranelock.agent import Agent, Mode
from cranelock import protocol_pb2 as pb


def test_normal_grpc_transfer():
    async def scenario():
        a = Agent("yard-107", {"yard-923": "localhost:1"}, "yard-107", event_sink=lambda e: None)
        b = Agent("yard-923", {"yard-107": "localhost:1"}, "yard-107", event_sink=lambda e: None)
        try:
            pa = await a.start("localhost:0", auto_transfer=False)
            pb_port = await b.start("localhost:0", auto_transfer=False)
            import grpc
            from cranelock.protocol_pb2_grpc import CraneAgentStub
            for agent, peer, port in [(a, "yard-923", pb_port), (b, "yard-107", pa)]:
                await agent.channels[peer].close()
                agent.channels[peer] = grpc.aio.insecure_channel(f"localhost:{port}")
                agent.stubs[peer] = CraneAgentStub(agent.channels[peer])
            assert a.state.has_token and not b.state.has_token
            assert (await b.request_token()).accepted
            assert list(a.state.request_queue) == ["yard-923"]
            assert await a.transfer_next()
            assert not a.state.has_token and b.state.has_token
            assert sum(x.state.has_token for x in [a, b]) == 1
            # A heartbeat created before receipt must not undo the handoff view.
            await a.Heartbeat(pb.HeartbeatMessage(agent_id="yard-923", epoch=1,
                token_holder="yard-107", mode=pb.LOCAL, has_token=False), None)
            assert a.state.token_holder == "yard-923"
            await asyncio.sleep(0.15)
            assert not a.suspected and not b.suspected
        finally:
            await a.close()
            await b.close()
    asyncio.run(scenario())


def test_uncertain_transfer_never_restores_ownership():
    async def scenario():
        a = Agent("source", {"destination": "localhost:1"}, "source",
                  interval=0.01, timeout=0.02, event_sink=lambda e: None)
        try:
            await a.TokenRequest(pb.TokenRequestMessage(agent_id="destination", epoch=1), None)
            assert not await a.transfer_next()
            transfer_id = a.pending.transfer_id
            assert not a.state.has_token
            assert not await a.transfer_next()
            assert a.pending.transfer_id == transfer_id
            assert not a.state.has_token
        finally:
            await a.close()
    asyncio.run(scenario())


def test_fifo_duplicates_and_concurrency():
    async def scenario():
        ids = [f"crane-{i:04}" for i in range(100)]
        a = Agent("holder", {i: "localhost:1" for i in ids}, "holder", event_sink=lambda e: None)
        try:
            for i in ids[:2]:
                assert (await a.TokenRequest(pb.TokenRequestMessage(agent_id=i, epoch=1), None)).accepted
            await asyncio.gather(*(a.TokenRequest(pb.TokenRequestMessage(agent_id=i, epoch=1), None)
                                   for i in ids + ids))
            assert list(a.state.request_queue) == ids
            assert a.state.has_token
        finally:
            await a.close()
    asyncio.run(scenario())


def test_heartbeat_suspicion_and_timing():
    async def scenario():
        now = [10.0]
        events = []
        a = Agent("observer", {"holder": "localhost:1"}, "holder",
                  clock=lambda: now[0], event_sink=events.append)
        try:
            for moment in [11.0, 12.0, 13.0]:
                now[0] = moment
                await a.Heartbeat(pb.HeartbeatMessage(agent_id="holder", epoch=1), None)
                await a.detect()
                assert not a.suspected
            now[0] = 15.1
            await a.detect()
            assert a.suspected == {"holder"}
            event = next(e for e in events if e["event"] == "PEER_SUSPECTED")
            assert event["detection_latency_seconds"] == pytest.approx(2.1)
            assert event["monotonic"] == 15.1
            assert any(e["event"] == "TOKEN_HOLDER_SUSPECTED" for e in events)
            assert not a.state.has_token
            await a.detect()
            assert len([e for e in events if e["event"] == "PEER_SUSPECTED"]) == 1
        finally:
            await a.close()
    asyncio.run(scenario())


def test_stale_epoch_and_restart(tmp_path):
    async def scenario():
        path = tmp_path / "state.json"
        a = Agent("node-19", {"node-28": "localhost:1"}, "node-19", epoch=5,
                  state_file=path, event_sink=lambda e: None)
        await a.close()
        b = Agent("node-19", {"node-28": "localhost:1"}, "node-19", epoch=1,
                  state_file=path, event_sink=lambda e: None)
        try:
            assert b.state.mode == Mode.REJOINING and not b.state.has_token
            assert b.state.current_epoch == 5
            assert not (await b.TokenRequest(pb.TokenRequestMessage(agent_id="node-28", epoch=5), None)).accepted
            await b.reconcile(5, "node-28")
            assert not (await b.TokenTransfer(pb.TokenTransferMessage(sender_id="node-28",
                receiver_id="node-19", epoch=4, transfer_id="old"), None)).accepted
            assert not (await b.Heartbeat(pb.HeartbeatMessage(agent_id="node-28", epoch=4), None)).accepted
            assert b.state.current_epoch == 5 and not b.state.has_token
            with pytest.raises(ValueError):
                await b.reconcile(4, "node-19")
        finally:
            await b.close()
    asyncio.run(scenario())


def test_transfer_idempotency_and_unknown_sender():
    async def scenario():
        b = Agent("receiver", {"sender": "localhost:1"}, "sender", event_sink=lambda e: None)
        try:
            message = pb.TokenTransferMessage(sender_id="sender", receiver_id="receiver",
                                              epoch=1, transfer_id="stable-id")
            assert (await b.TokenTransfer(message, None)).accepted
            assert (await b.TokenTransfer(message, None)).accepted
            message.transfer_id = "different"
            assert not (await b.TokenTransfer(message, None)).accepted
            assert b.state.has_token
        finally:
            await b.close()
    asyncio.run(scenario())
