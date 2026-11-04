import asyncio

import grpc
import pytest

from cranelock.agent import Agent, Mode
from cranelock.gate_client import GateClient
from cranelock.controller import (
    AgentStateMonitor,
    CentralController,
    CentralControllerFleet,
    format_snapshot,
)
from cranelock_gate.config import Config
from cranelock_gate.core import Gate
from cranelock_gate.server import RunningServer
from cranelock_gate.storage import initialize


def test_controller_bootstraps_central_authority(h):
    server = RunningServer(h.gate, "127.0.0.1:0")
    try:
        with grpc.insecure_channel(f"127.0.0.1:{server.port}") as channel:
            grpc.channel_ready_future(channel).result(timeout=3)
            controller = CentralController(h.config, h.keys["controller"], channel)

            result = controller.install_central("crane-1", request_id="controller-bootstrap")

            assert result["allowed"]
            assert result["action_applied"]
            assert result["state"]["epoch"] == 0
            assert result["state"]["mode"] == "CENTRAL"
            assert result["state"]["holder"] == "crane-1"
            assert controller.get_state()["revision"] == 1
    finally:
        server.close()


def test_controller_switches_existing_authority_to_central(h):
    assert h.install()["allowed"]
    server = RunningServer(h.gate, "127.0.0.1:0")
    try:
        with grpc.insecure_channel(f"127.0.0.1:{server.port}") as channel:
            grpc.channel_ready_future(channel).result(timeout=3)
            controller = CentralController(h.config, h.keys["controller"], channel)

            result = controller.install_central("crane-2")

            assert result["allowed"]
            assert result["state"]["epoch"] == 0
            assert result["state"]["mode"] == "CENTRAL"
            assert result["state"]["holder"] == "crane-2"
    finally:
        server.close()


@pytest.mark.parametrize("with_gate", [False, True])
def test_controller_reconciles_agents_from_gate_state(h, with_gate):
    assert h.install(mode="CENTRAL")["allowed"]
    server = RunningServer(h.gate, "127.0.0.1:0")
    try:
        with grpc.insecure_channel(f"127.0.0.1:{server.port}") as channel:
            grpc.channel_ready_future(channel).result(timeout=3)
            controller = CentralController(h.config, h.keys["controller"], channel)

            async def reconcile():
                agent = Agent(
                    "crane-2",
                    {"crane-1": "127.0.0.1:50051", "crane-3": "127.0.0.1:50053"},
                    "crane-1",
                    epoch=0,
                    mode=Mode.REJOINING,
                    gate=GateClient(grpc.insecure_channel(f"127.0.0.1:{server.port}"),
                                    h.config, h.keys, "crane-2") if with_gate else None,
                )
                try:
                    state = await controller.reconcile_agents({"crane-2": agent})
                    assert state["holder"] == "crane-1"
                    assert agent.state.current_epoch == 0
                    assert agent.state.revision == state["revision"] == 1
                    assert agent.state.token_holder == "crane-1"
                    assert agent.state.mode == Mode.CENTRAL
                    assert not agent.state.has_token
                finally:
                    await agent.close()

            asyncio.run(reconcile())
    finally:
        server.close()


def test_controller_fleet_reads_each_gate(h, tmp_path):
    second_config = Config(
        lane_id="lane-2",
        membership_id="group20-lane-2",
        members=h.config.members,
        controller_id=h.config.controller_id,
        simulator_id=h.config.simulator_id,
        operator_id=h.config.operator_id,
        service_keys=h.config.service_keys,
    )
    second_database = tmp_path / "gate-2.sqlite"
    initialize(second_database, second_config, h.clock())
    second_gate = Gate(second_database, second_config, h.clock)
    first_server = RunningServer(h.gate, "127.0.0.1:0")
    second_server = RunningServer(second_gate, "127.0.0.1:0")
    try:
        with (grpc.insecure_channel(f"127.0.0.1:{first_server.port}") as first_channel,
              grpc.insecure_channel(f"127.0.0.1:{second_server.port}") as second_channel):
            grpc.channel_ready_future(first_channel).result(timeout=3)
            grpc.channel_ready_future(second_channel).result(timeout=3)
            first = CentralController(h.config, h.keys["controller"], first_channel)
            second = CentralController(second_config, h.keys["controller"], second_channel)
            fleet = CentralControllerFleet({"lane-1": first, "lane-2": second})

            states = fleet.get_all_states()

            assert set(states) == {"lane-1", "lane-2"}
            assert states["lane-1"]["epoch"] == -1
            assert states["lane-2"]["epoch"] == -1
    finally:
        first_server.close()
        second_server.close()


def test_agent_state_monitor_reads_running_agent():
    async def start_agent():
        agent = Agent("crane-a", {}, "crane-a")
        port = await agent.start("127.0.0.1:0")
        monitor = AgentStateMonitor({"crane-a": f"127.0.0.1:{port}"}, timeout=1)
        try:
            return await asyncio.to_thread(monitor.read_states)
        finally:
            monitor.close()
            await agent.close()

    snapshot = asyncio.run(start_agent())
    result = snapshot["agents"]["crane-a"]
    assert result["status"] == "AVAILABLE"
    assert result["state"]["agent_id"] == "crane-a"
    assert result["state"]["has_token"]


@pytest.mark.parametrize("with_probe", [False, True])
def test_agent_starts_controller_watch_only_when_configured(with_probe):
    async def scenario():
        called = asyncio.Event()

        async def probe():
            called.set()
            return True

        agent = Agent("crane-a", {}, "crane-a", event_sink=lambda event: None,
                      controller_probe=probe if with_probe else None)
        try:
            await agent.start("127.0.0.1:0", auto_transfer=False)
            watchers = [task for task in agent.tasks
                        if task.get_coro().__name__ == "controller_watch_loop"]
            assert len(watchers) == int(with_probe)
            if with_probe:
                await asyncio.wait_for(called.wait(), timeout=1)
                assert not agent.controller_suspected
        finally:
            await agent.close()

    asyncio.run(scenario())


def test_format_snapshot_shows_agent_state_and_failures():
    snapshot = {
        "observed_at": 0,
        "agents": {
            "crane-a": {
                "status": "AVAILABLE",
                "state": {
                    "epoch": "1",
                    "mode": "LOCAL",
                    "has_token": False,
                    "token_holder": "crane-b",
                    "peers": [{"agent_id": "crane-b", "status": "AVAILABLE"}],
                    "request_queue": [],
                    "pending_transfer_id": "",
                },
            },
            "crane-b": {
                "status": "UNAVAILABLE",
                "error": "UNAVAILABLE",
                "details": "connection refused",
            },
        },
    }

    output = format_snapshot(snapshot, "startup")

    assert "Agent state startup -" in output
    assert "crane-a: AVAILABLE | epoch 1 | LOCAL | holds token: no | holder: crane-b" in output
    assert "peers: crane-b=AVAILABLE" in output
    assert "crane-b: UNAVAILABLE | UNAVAILABLE - connection refused" in output
