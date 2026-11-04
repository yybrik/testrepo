import asyncio

import grpc
import pytest

from cranelock.agent import Agent, Mode
from cranelock.controller import CentralController, ControllerHeartbeatClient
from cranelock_gate.server import RunningServer
from test_auto_recovery import survivors, until


def test_controller_outage_automatically_preserves_healthy_holder_and_occupancy(h, tmp_path):
    async def scenario():
        healthy = True

        async def probe():
            return healthy

        probes = {identity: probe for identity in h.config.members}
        async with survivors(h, tmp_path, list(probes), mode="CENTRAL", probes=probes) as (agents, _):
            assert h.observe()["allowed"]
            assert h.grant()["allowed"]
            assert h.enter()["allowed"]
            await asyncio.sleep(.25)
            assert all(a.state.mode == Mode.CENTRAL and not a.vote_state for a in agents.values())
            healthy = False
            await until(lambda: all(a.state.mode == Mode.LOCAL for a in agents.values()))
            assert (h.state["epoch"], h.state["revision"], h.state["holder"]) == (1, 2, "crane-1")
            assert h.state["lane_state"] == "OCCUPIED"
            assert h.grant()["reason"] == "LANE_BUSY"
            assert h.call("crane-1", "RequestGrant", epoch=0, revision=1)["reason"] == "STALE_EPOCH"
            assert [identity for identity, a in agents.items() if a.state.has_token] == ["crane-1"]
            assert all(a.state.revision == 2 and set(a.vote_state) == {"1"} for a in agents.values())
            healthy = True
            await until(lambda: not any(a.controller_suspected for a in agents.values()))
            assert all(a.state.mode == Mode.LOCAL for a in agents.values())
            assert h.state["epoch"] == 1  # Controller return belongs to Kirby.
    asyncio.run(scenario())


@pytest.mark.parametrize("isolated", [False, True])
def test_controller_suspicion_without_majority_cannot_install_local(h, tmp_path, isolated):
    async def scenario():
        async def failed():
            return False

        async def healthy():
            return True

        probes = {identity: failed if identity == "crane-2" else healthy for identity in h.config.members}
        members = ["crane-2"] if isolated else list(probes)
        async with survivors(h, tmp_path, members, mode="CENTRAL", probes=probes) as (agents, events):
            await until(lambda: any(e["event"] == "RECOVERY_NO_QUORUM" for e in events))
            assert h.state["mode"] == "CENTRAL" and h.state["epoch"] == 0
            assert not agents["crane-2"].state.has_token
            assert set(agents["crane-2"].vote_state) == {"1"}
            assert all(not a.vote_state for identity, a in agents.items() if identity != "crane-2")
    asyncio.run(scenario())


@pytest.mark.parametrize("failure", ["reject", "unavailable", "lost_reply", "stale"])
def test_fallback_waits_for_gate_and_serializes_attempts(h, tmp_path, failure):
    async def scenario():
        healthy = True

        async def probe():
            return healthy

        members = ["crane-1", "crane-2"]
        async with survivors(h, tmp_path, members, mode="CENTRAL",
                             probes={p: probe for p in members}) as (agents, events):
            # Pause after voting, before submission to the gate.
            release = asyncio.Event()
            certificates, responses = [], []
            states = {identity: a.gate.state for identity, a in agents.items()}
            installs = {identity: a.gate.install_recovery for identity, a in agents.items()}

            async def unavailable():
                raise RuntimeError("gate offline")

            for agent in agents.values():
                original = agent.gate.install_recovery

                async def install(cert, original=original, agent=agent):
                    certificates.append(cert)
                    await release.wait()
                    if failure == "reject":
                        return dict(allowed=False, reason="STORAGE_UNAVAILABLE")
                    if failure == "unavailable":
                        agent.gate.state = unavailable
                        raise RuntimeError("gate offline")
                    response = await original(cert)
                    responses.append(response)
                    if failure == "lost_reply":
                        raise RuntimeError("reply lost after commit")
                    return response

                agent.gate.install_recovery = install
            healthy = False
            await until(lambda: len(certificates) == len(agents))
            await asyncio.gather(*(agents["crane-2"].recover_suspected_holder() for _ in range(10)))
            assert len(certificates) == len(agents)
            assert certificates[0]["payload"] == certificates[1]["payload"]
            assert certificates[0]["payload"]["holder"] == "crane-1"
            assert all(a.state.mode == Mode.CENTRAL for a in agents.values())
            assert h.state["epoch"] == 0
            assert not agents["crane-2"].state.has_token
            if failure == "stale":
                assert h.install("crane-2", epoch=0, mode="CENTRAL")["allowed"]
            release.set()
            if failure == "lost_reply":
                await until(lambda: all(a.state.mode == Mode.LOCAL for a in agents.values()))
                assert h.state["epoch"] == 1
            elif failure == "unavailable":
                await until(lambda: all(a.state.mode == Mode.REJOINING for a in agents.values()))
                assert all(not a.state.has_token for a in agents.values())
                assert h.state["mode"] == "CENTRAL"
                for identity, a in agents.items():
                    a.gate.state = states[identity]
                    a.gate.install_recovery = installs[identity]
                await until(lambda: all(a.state.mode == Mode.LOCAL for a in agents.values()))
            elif failure == "stale":
                await until(lambda: all(a.state.revision == 2 for a in agents.values()))
                assert all(a.state.mode == Mode.CENTRAL for a in agents.values())
                assert responses and all(r["reason"] == "STALE_REVISION" for r in responses)
                assert h.state["epoch"] == 0 and h.state["holder"] == "crane-2"
                # Persisted votes cannot be rewritten for a different snapshot.
                await until(lambda: any(e.get("reason") == "CONFLICTING_VOTE" for e in events))
            else:
                await until(lambda: any(e["event"] == "RECOVERY_AUTHORITY_INSTALL_RESULT" for e in events))
                assert h.state["mode"] == "CENTRAL" and h.state["epoch"] == 0
    asyncio.run(scenario())


def test_restored_controller_aborts_fallback_before_submission(h, tmp_path):
    async def scenario():
        healthy = True

        async def probe():
            return healthy

        members = ["crane-1", "crane-2"]
        async with survivors(h, tmp_path, members, mode="CENTRAL",
                             probes={p: probe for p in members}) as (agents, events):
            release = asyncio.Event()
            certificates = []
            for agent in agents.values():
                original = agent.propose_recovery

                async def propose(*args, original=original):
                    cert = await original(*args)
                    if cert:
                        certificates.append(cert)
                        await release.wait()
                    return cert

                agent.propose_recovery = propose
            healthy = False
            await until(lambda: len(certificates) == len(agents))
            healthy = True
            await until(lambda: not any(a.controller_suspected for a in agents.values()))
            release.set()
            await until(lambda: not any(a.recovery_lock.locked() for a in agents.values()))
            assert h.state["mode"] == "CENTRAL" and h.state["epoch"] == 0
            assert not any(e["event"] == "RECOVERY_AUTHORITY_INSTALL_RESULT" for e in events)
            assert all(a.vote_state for a in agents.values())
    asyncio.run(scenario())


def test_controller_and_holder_failure_reuses_local_recovery_after_fallback(h, tmp_path):
    async def scenario():
        async def probe():
            return False

        members = ["crane-2", "crane-3"]
        async with survivors(h, tmp_path, members, mode="CENTRAL",
                             probes={p: probe for p in members}) as (agents, _):
            await until(lambda: all(a.state.current_epoch == 2 for a in agents.values()))
            assert h.state["holder"] == "crane-2" and h.state["mode"] == "LOCAL"
            assert agents["crane-2"].state.has_token and not agents["crane-3"].state.has_token
            assert all(set(a.vote_state) == {"1", "2"} for a in agents.values())
    asyncio.run(scenario())


def test_real_controller_heartbeat_restores_latest_gate_authority(h, tmp_path):
    async def scenario():
        gate_server = RunningServer(h.gate, "127.0.0.1:0")
        gate_channel = grpc.insecure_channel(f"127.0.0.1:{gate_server.port}")
        controller = CentralController(h.config, h.keys["controller"], gate_channel)
        controller_port = await controller.start_heartbeat_server()
        heartbeat = ControllerHeartbeatClient(
            f"127.0.0.1:{controller_port}", "crane-2", h.config.controller_id,
            timeout=.1)
        try:
            async with survivors(
                    h, tmp_path, ["crane-1", "crane-2"], mode="CENTRAL",
                    probes={identity: heartbeat.probe
                            for identity in ("crane-1", "crane-2")}) as (agents, events):
                assert await heartbeat.probe()
                await controller.stop_heartbeat_server()
                await until(lambda: all(
                    agent.state.mode == Mode.LOCAL and agent.controller_suspected
                    for agent in agents.values()))
                local = h.state
                assert (local["epoch"], local["revision"], local["holder"], local["mode"]) == (
                    1, 2, "crane-1", "LOCAL")
                assert h.observe()["allowed"]
                transfer = h.call("crane-1", "TransferHolder", epoch=1, revision=2,
                                  destination="crane-2")
                assert transfer["allowed"]
                assert (transfer["state"]["epoch"], transfer["state"]["revision"],
                        transfer["state"]["holder"]) == (1, 3, "crane-2")

                controller_port = await controller.start_heartbeat_server()
                await heartbeat.reconnect(f"127.0.0.1:{controller_port}")
                assert await heartbeat.probe()
                await until(lambda: all(
                    agent.state.mode == Mode.CENTRAL and not agent.controller_suspected
                    for agent in agents.values()))
                restored = h.state
                assert (restored["epoch"], restored["revision"], restored["holder"],
                        restored["mode"]) == (1, 4, "crane-2", "CENTRAL")
                assert all((agent.state.current_epoch, agent.state.revision,
                            agent.state.token_holder) == (1, 4, "crane-2")
                           for agent in agents.values())
                assert all(not agent.state.has_token or agent.state.agent_id == "crane-2"
                           for agent in agents.values())
                assert any(event["event"] == "CONTROLLER_RESTORED" for event in events)
        finally:
            await heartbeat.close()
            await controller.stop_heartbeat_server()
            gate_channel.close()
            gate_server.close()
    asyncio.run(scenario())


def test_repeated_controller_failures_do_not_replay_stale_authority(h, tmp_path):
    async def scenario():
        gate_server = RunningServer(h.gate, "127.0.0.1:0")
        gate_channel = grpc.insecure_channel(f"127.0.0.1:{gate_server.port}")
        controller = CentralController(h.config, h.keys["controller"], gate_channel)
        port = await controller.start_heartbeat_server()
        heartbeat = ControllerHeartbeatClient(
            f"127.0.0.1:{port}", "crane-2", h.config.controller_id, timeout=.1)
        try:
            async with survivors(
                    h, tmp_path, ["crane-1", "crane-2"], mode="CENTRAL",
                    probes={identity: heartbeat.probe
                            for identity in ("crane-1", "crane-2")}) as (agents, events):
                for expected_epoch in (1, 2):
                    await controller.stop_heartbeat_server()
                    await until(lambda: all(
                        agent.state.mode == Mode.LOCAL
                        and agent.state.current_epoch == expected_epoch
                        and agent.controller_suspected for agent in agents.values()))
                    await until(lambda: sum(
                        event["event"] == "CONTROLLER_PROBE_FAILED"
                        for event in events) >= expected_epoch * 3)
                    failed_state = h.state
                    assert (failed_state["epoch"], failed_state["mode"],
                            failed_state["holder"]) == (
                                expected_epoch, "LOCAL", "crane-1")
                    assert all(agent.state.token_holder == "crane-1"
                               for agent in agents.values())

                    port = await controller.start_heartbeat_server()
                    await heartbeat.reconnect(f"127.0.0.1:{port}")
                    assert await heartbeat.probe()
                    await until(lambda: all(
                        agent.state.mode == Mode.CENTRAL
                        and agent.state.current_epoch == expected_epoch
                        and not agent.controller_suspected
                        for agent in agents.values()))
                    restored = h.state
                    assert (restored["epoch"], restored["revision"], restored["mode"],
                            restored["holder"]) == (
                                expected_epoch, expected_epoch * 2 + 1,
                                "CENTRAL", "crane-1")
        finally:
            await heartbeat.close()
            await controller.stop_heartbeat_server()
            gate_channel.close()
            gate_server.close()
    asyncio.run(scenario())


def test_controller_return_does_not_overwrite_local_transfer(h, tmp_path):
    async def scenario():
        gate_server = RunningServer(h.gate, "127.0.0.1:0")
        gate_channel = grpc.insecure_channel(f"127.0.0.1:{gate_server.port}")
        controller = CentralController(h.config, h.keys["controller"], gate_channel)
        heartbeat_port = await controller.start_heartbeat_server()
        heartbeat = ControllerHeartbeatClient(
            f"127.0.0.1:{heartbeat_port}", "crane-2", h.config.controller_id,
            timeout=.1)
        try:
            async with survivors(
                    h, tmp_path, ["crane-1", "crane-2"], mode="CENTRAL",
                    probes={identity: heartbeat.probe
                            for identity in ("crane-1", "crane-2")}) as (agents, _):
                await controller.stop_heartbeat_server()
                await until(lambda: all(agent.state.mode == Mode.LOCAL
                                        for agent in agents.values()))
                holder = agents["crane-1"]
                async with holder.lock:
                    holder.state.request_queue.append("crane-2")
                started, release = asyncio.Event(), asyncio.Event()
                original_transfer = holder.gate.transfer

                async def delayed_transfer(payload):
                    started.set()
                    await release.wait()
                    return await original_transfer(payload)

                holder.gate.transfer = delayed_transfer
                transfer = asyncio.create_task(holder.transfer_next())
                await asyncio.wait_for(started.wait(), 2)
                heartbeat_port = await controller.start_heartbeat_server()
                await heartbeat.reconnect(f"127.0.0.1:{heartbeat_port}")
                await until(lambda: h.state["mode"] == "CENTRAL")
                assert holder.gate_attempt is not None
                assert holder.state.mode == Mode.LOCAL
                release.set()
                assert not await asyncio.wait_for(transfer, 2)
                await until(lambda: all(agent.state.mode == Mode.CENTRAL
                                        for agent in agents.values()))
                assert (h.state["epoch"], h.state["revision"], h.state["holder"]) == (
                    1, 3, "crane-1")
                assert holder.state.has_token
                assert not agents["crane-2"].state.has_token
                assert holder.gate_attempt is None
        finally:
            await heartbeat.close()
            await controller.stop_heartbeat_server()
            gate_channel.close()
            gate_server.close()
    asyncio.run(scenario())


@pytest.mark.parametrize("failure", ["exception", "timeout"])
def test_probe_failures_are_bounded_and_worker_survives(failure):
    async def scenario():
        healthy = True
        cancelled = asyncio.Event()

        async def probe():
            if healthy:
                return True
            if failure == "exception":
                raise OSError("connection lost")
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

        events = []
        agent = Agent("single", {}, "single", mode=Mode.CENTRAL,
                      interval=.01, timeout=.05, controller_probe=probe, event_sink=events.append)
        try:
            await agent.start("127.0.0.1:0", auto_transfer=False)
            await until(lambda: any(e["event"] == "CONTROLLER_HEARTBEAT_RECEIVED" for e in events))
            healthy = False
            await until(lambda: agent.controller_suspected)
            assert any(e["event"] == "CONTROLLER_PROBE_FAILED" for e in events)
            assert all(not task.done() for task in agent.tasks)
            if failure == "timeout":
                assert cancelled.is_set()
            healthy = True
            await until(lambda: not agent.controller_suspected)
            assert agent.state.mode == Mode.CENTRAL  # No gate/recovery configured.
            healthy = False
            await asyncio.sleep(.005)
        finally:
            await asyncio.wait_for(agent.close(), timeout=1)
        assert all(task.done() for task in agent.tasks)
    asyncio.run(scenario())
