import asyncio

import grpc
import pytest

from cranelock.agent import Agent
from cranelock.gate_client import GateClient
from cranelock import recovery
from cranelock import protocol_pb2_grpc as rpc
from cranelock_gate.client import command
from cranelock_gate.crypto import CERTIFICATE_DOMAIN, COMMAND_DOMAIN, canonical
from cranelock_gate.server import RunningServer


async def connect(agents):
    ports = {}
    for identity, agent in agents.items():
        ports[identity] = await agent.start("localhost:0", auto_transfer=False)
    for identity, agent in agents.items():
        for peer in agent.peers:
            await agent.channels[peer].close()
            agent.channels[peer] = grpc.aio.insecure_channel(f"localhost:{ports[peer]}")
            agent.stubs[peer] = rpc.CraneAgentStub(agent.channels[peer])


@pytest.mark.parametrize("with_gate", [False, True])
def test_majority_partition_recovers_and_fences_old_holder(h, tmp_path, with_gate):
    async def scenario():
        assert h.install("crane-1")["allowed"]
        agents = {}
        events = []
        server = RunningServer(h.gate, "127.0.0.1:0") if with_gate else None
        addresses = {identity: "localhost:1" for identity in h.config.members}
        for identity in h.config.members:
            peers = {peer: address for peer, address in addresses.items() if peer != identity}
            agents[identity] = Agent(identity, peers, "crane-1", epoch=0,
                state_file=tmp_path / f"{identity}.json", event_sink=events.append,
                gate=GateClient(grpc.insecure_channel(f"127.0.0.1:{server.port}"),
                    h.config, h.keys, identity) if with_gate else None,
                revision=h.state["revision"],
                recovery_config=h.config, recovery_private_key=h.keys[identity],
                interval=0.05, timeout=0.1)
        try:
            # starts only the majority side
            ports = {}
            for identity in ("crane-2", "crane-3"):
                ports[identity] = await agents[identity].start("localhost:0", auto_transfer=False)
            for identity, peer in (("crane-2", "crane-3"), ("crane-3", "crane-2")):
                await agents[identity].channels[peer].close()
                agents[identity].channels[peer] = grpc.aio.insecure_channel(
                    f"localhost:{ports[peer]}")
                agents[identity].stubs[peer] = rpc.CraneAgentStub(agents[identity].channels[peer])
            # marks the old holder as unavailable
            agents["crane-2"].suspected.add("crane-1")
            agents["crane-3"].suspected.add("crane-1")
            certificate = await agents["crane-2"].propose_recovery(h.state, "crane-2")
            assert certificate is not None
            state = h.state
            result = h.call("crane-2", "InstallAuthority", certificate=certificate,
                expected_epoch=state["epoch"], expected_revision=state["revision"])
            assert result["allowed"] and result["state"]["epoch"] == 1
            old = command(h.config, "crane-1", "RequestGrant", epoch=0,
                          revision=state["revision"])
            denied = h.gate.execute("RequestGrant", canonical(old),
                h.keys["crane-1"].sign(COMMAND_DOMAIN + canonical(old)))
            assert denied["reason"] == "STALE_EPOCH"
            assert any(event["event"] == "RECOVERY_VOTE_CAST" for event in events)
            formed = next(event for event in events
                          if event["event"] == "RECOVERY_CERTIFICATE_FORMED")
            assert formed["holder"] == "crane-2" and formed["votes"] == h.config.majority
            if with_gate:
                for identity in ("crane-2", "crane-3"):
                    agent = agents[identity]
                    assert await agent.reconcile_from_gate()
                    assert (agent.state.current_epoch, agent.state.revision) == (1, 2)
                    assert agent.state.has_token == (identity == "crane-2")
                    assert agent.vote_state
        finally:
            await asyncio.gather(*(agent.close() for agent in agents.values()))
            if server:
                server.close()
    asyncio.run(scenario())


def test_minority_partition_cannot_form_certificate(h, tmp_path):
    async def scenario():
        assert h.install("crane-1")["allowed"]
        peers = {"crane-1": "localhost:1", "crane-3": "localhost:2"}
        minority = Agent("crane-2", peers, "crane-1", epoch=0,
            state_file=tmp_path / "minority.json", event_sink=lambda event: None,
            recovery_config=h.config, recovery_private_key=h.keys["crane-2"],
            interval=0.01, timeout=0.02)
        try:
            minority.suspected.add("crane-1")
            assert await minority.propose_recovery(h.state, "crane-2") is None
            assert h.state["epoch"] == 0 and h.state["holder"] == "crane-1"
        finally:
            await minority.close()
    asyncio.run(scenario())


def test_vote_is_persistent_and_cannot_equivocate(h, tmp_path):
    async def scenario():
        path = tmp_path / "voter.json"
        peers = {"crane-1": "localhost:1", "crane-3": "localhost:2"}
        voter = Agent("crane-2", peers, "crane-1", epoch=0, state_file=path,
            event_sink=lambda event: None, recovery_config=h.config,
            recovery_private_key=h.keys["crane-2"])
        voter.suspected.add("crane-1")
        payload = recovery.proposal(h.config, {"epoch": 0, "revision": 1}, "crane-2")
        raw = canonical(payload)
        signature = h.keys["crane-3"].sign(CERTIFICATE_DOMAIN + raw)
        voter._vote_recovery(raw, "crane-3", signature)
        assert path.exists() and voter.vote_state
        await voter.close()
        voter = Agent("crane-2", peers, "crane-1", epoch=0, state_file=path,
            event_sink=lambda event: None, recovery_config=h.config,
            recovery_private_key=h.keys["crane-2"])
        await voter.reconcile(0, "crane-1")
        voter.suspected.add("crane-1")
        conflicting = dict(payload, holder="crane-3")
        conflicting_raw = canonical(conflicting)
        conflicting_signature = h.keys["crane-3"].sign(CERTIFICATE_DOMAIN + conflicting_raw)
        import pytest
        with pytest.raises(recovery.RecoveryError, match="CONFLICTING_VOTE"):
            voter._vote_recovery(conflicting_raw, "crane-3", conflicting_signature)
        await voter.close()
    asyncio.run(scenario())
