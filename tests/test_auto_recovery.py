import asyncio
from contextlib import asynccontextmanager

import grpc
import pytest

from cranelock.agent import Agent, Mode
from cranelock.gate_client import GateClient
from cranelock.protocol_pb2_grpc import CraneAgentStub
from cranelock_gate.server import RunningServer


async def until(predicate):
    async with asyncio.timeout(4):
        while not predicate():
            await asyncio.sleep(.01)


@asynccontextmanager
async def survivors(h, tmp_path, members, mode="LOCAL", probes=None):
    assert h.install("crane-1", mode=mode)["allowed"]
    server = RunningServer(h.gate, "127.0.0.1:0")
    agents, events, ports = {}, [], {}
    try:
        for identity in members:
            gate = GateClient(grpc.insecure_channel(f"127.0.0.1:{server.port}"),
                              h.config, h.keys, identity, timeout=.3)
            agents[identity] = Agent(identity,
                {p: "127.0.0.1:1" for p in h.config.members if p != identity},
                "crane-1", epoch=0, gate=gate, recovery_config=h.config,
                recovery_private_key=h.keys[identity], state_file=tmp_path / f"{identity}.json",
                interval=.02, timeout=.2, event_sink=events.append,
                controller_probe=probes.get(identity) if probes else None)
        for identity, agent in agents.items():
            ports[identity] = await agent.start("127.0.0.1:0", auto_transfer=False)
            for task in agent.tasks:
                task.cancel()
            await asyncio.gather(*agent.tasks, return_exceptions=True)
            agent.tasks = []
        for agent in agents.values():
            for peer in members:
                if peer != agent.state.agent_id:
                    await agent.channels[peer].close()
                    channel = grpc.aio.insecure_channel(f"127.0.0.1:{ports[peer]}")
                    agent.channels[peer] = channel
                    agent.stubs[peer] = CraneAgentStub(channel)
        for agent in agents.values():
            agent.tasks = [asyncio.create_task(agent.heartbeat_loop()),
                           asyncio.create_task(agent.monitor_loop()),
                           asyncio.create_task(agent.rejoin_loop()),
                           asyncio.create_task(agent.recovery_loop())]
            if agent.controller_probe:
                agent.tasks.append(asyncio.create_task(agent.controller_watch_loop()))
        yield agents, events
    finally:
        await asyncio.gather(*(agent.close() for agent in agents.values()))
        server.close()


def test_nonholder_timeout_does_not_recover(h, tmp_path):
    async def scenario():
        async with survivors(h, tmp_path, ["crane-1", "crane-2"]) as (agents, events):
            await until(lambda: all("crane-3" in a.suspected for a in agents.values()))
            await asyncio.sleep(.1)
            assert h.state["epoch"] == 0
            assert not any(e["event"] == "RECOVERY_CERTIFICATE_FORMED" for e in events)
            assert not any(a.vote_state for a in agents.values())
    asyncio.run(scenario())


def test_holder_timeout_automatically_recovers_and_fences(h, tmp_path):
    async def scenario():
        async with survivors(h, tmp_path, ["crane-2", "crane-3"]) as (agents, events):
            await until(lambda: all(a.state.current_epoch == 1 for a in agents.values()))
            assert (h.state["epoch"], h.state["revision"], h.state["holder"]) == (1, 2, "crane-2")
            assert agents["crane-2"].state.has_token
            assert not agents["crane-3"].state.has_token
            assert all(a.state.revision == 2 and a.vote_state for a in agents.values())
            assert any(e["event"] == "TOKEN_HOLDER_SUSPECTED" for e in events)
            assert h.call("crane-1", "RequestGrant", epoch=0, revision=1)["reason"] == "STALE_EPOCH"
            await asyncio.gather(*(agents["crane-2"].recover_suspected_holder() for _ in range(10)))
            assert h.state["epoch"] == 1
            assert all(set(a.vote_state) == {"1"} for a in agents.values())
    asyncio.run(scenario())


def test_minority_retries_identical_vote_without_ownership(h, tmp_path):
    async def scenario():
        async with survivors(h, tmp_path, ["crane-3"]) as (agents, events):
            agent = agents["crane-3"]
            await until(lambda: any(e["event"] == "RECOVERY_NO_QUORUM" for e in events))
            vote = dict(agent.vote_state)
            await asyncio.gather(*(agent.recover_suspected_holder() for _ in range(8)))
            assert agent.vote_state == vote and set(vote) == {"1"}
            assert not agent.state.has_token
            assert h.state["epoch"] == 0 and h.state["holder"] == "crane-1"
    asyncio.run(scenario())


@pytest.mark.parametrize("failure", ["reject", "unavailable", "lost_reply"])
def test_install_failure_and_uncertain_reply_use_gate_authority(h, tmp_path, failure):
    async def scenario():
        async with survivors(h, tmp_path, ["crane-2", "crane-3"]) as (agents, events):
            entered, release = asyncio.Event(), asyncio.Event()
            calls = []

            for agent in agents.values():
                original = agent.gate.install_recovery

                async def install(cert, original=original):
                    calls.append(cert["payload"])
                    # Certificate formation must not grant ownership.
                    assert all(not a.state.has_token for a in agents.values())
                    entered.set()
                    await release.wait()
                    if failure == "reject":
                        return dict(allowed=False, reason="STORAGE_UNAVAILABLE")
                    if failure == "unavailable":
                        raise RuntimeError("gate unavailable")
                    await original(cert)
                    raise RuntimeError("reply lost after commit")

                agent.gate.install_recovery = install
            await asyncio.wait_for(entered.wait(), 4)
            await until(lambda: len(calls) == len(agents))
            await asyncio.gather(*(agents["crane-2"].recover_suspected_holder() for _ in range(8)))
            assert len(calls) <= len(agents)
            assert len({str(payload) for payload in calls}) == 1
            release.set()
            if failure == "lost_reply":
                await until(lambda: agents["crane-2"].state.has_token)
                assert h.state["epoch"] == 1
            else:
                await until(lambda: any(e["event"].startswith("RECOVERY_AUTHORITY_INSTALL") for e in events))
                assert all(not a.state.has_token for a in agents.values())
                assert h.state["epoch"] == 0
    asyncio.run(scenario())


def test_central_holder_timeout_does_not_start_local_recovery(h, tmp_path):
    async def scenario():
        async with survivors(h, tmp_path, ["crane-2"], mode="CENTRAL") as (agents, events):
            await until(lambda: "crane-1" in agents["crane-2"].suspected)
            await agents["crane-2"].recover_suspected_holder()
            assert agents["crane-2"].state.mode == Mode.CENTRAL
            assert not agents["crane-2"].vote_state
            assert h.state["epoch"] == 0
    asyncio.run(scenario())


def test_cached_suspected_holder_cannot_override_current_gate_holder(h, tmp_path):
    async def scenario():
        async with survivors(h, tmp_path, ["crane-2"]) as (agents, events):
            agent = agents["crane-2"]
            assert h.observe()["allowed"]
            assert h.call("crane-1", "TransferHolder", epoch=0, revision=1,
                          destination="crane-2")["allowed"]
            agent.suspected.add("crane-1")
            await agent.recover_suspected_holder()
            assert h.state["epoch"] == 0 and agent.state.revision == 2
            assert agent.state.has_token and not agent.vote_state
    asyncio.run(scenario())
