"""Run the documented gate + two-agent CLI demo and a persisted holder restart."""
import json
import os
import socket
import subprocess
import sys
import time

import grpc

from cranelock import protocol_pb2 as pb
from cranelock.protocol_pb2_grpc import CraneAgentStub
from cranelock_gate.client import Client
from cranelock_gate.config import Config


def test_gate_and_two_agent_processes_with_restart(tmp_path):
    sockets = [socket.socket() for _ in range(3)]
    for sock in sockets:
        sock.bind(("127.0.0.1", 0))
    targets = [f"127.0.0.1:{sock.getsockname()[1]}" for sock in sockets]
    for sock in sockets:
        sock.close()
    processes, streams, channels = [], [], []

    def command(*args):
        return [sys.executable, "-m", *map(str, args)]

    def run(*args):
        return subprocess.run(command(*args), check=True, capture_output=True, text=True, timeout=10)

    def start(name, args):
        stream = (tmp_path / f"{name}.log").open("w")
        streams.append(stream)
        process = subprocess.Popen(args, stdout=stream, stderr=stream,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
        processes.append(process)
        return process

    def wait_for(predicate):
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            try:
                if predicate():
                    return
            except grpc.RpcError as error:
                if error.code() not in {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED}:
                    raise
            time.sleep(0.05)
        raise AssertionError("CLI demo did not reach expected state")

    try:
        run("cranelock_gate.demo", "setup", "--directory", tmp_path)
        start("gate", command("cranelock_gate.server", "serve", "--config", tmp_path / "config.json",
            "--database", tmp_path / "gate.sqlite", "--bind", targets[0], "--metrics-port", "0"))
        gate_channel = grpc.insecure_channel(targets[0])
        channels.append(gate_channel)
        grpc.channel_ready_future(gate_channel).result(timeout=10)
        run("cranelock.gate_demo", "bootstrap", "--directory", tmp_path, "--target", targets[0])
        agent_commands = []
        for index in [1, 2]:
            other = 3 - index
            args = command("cranelock", "run", "--id", f"crane-{index}", "--listen", targets[index],
                "--peer", f"crane-{other}={targets[other]}", "--peer", "crane-3=127.0.0.1:1",
                "--holder", "crane-1", "--epoch", "0", "--state-file", tmp_path / f"agent-{index}.json",
                "--gate-address", targets[0], "--gate-config", tmp_path / "config.json",
                "--gate-keys", tmp_path / "private-keys.json", "--interval", "0.1", "--timeout", "1")
            agent_commands.append(args)
            start(f"agent-{index}", args + (["--request-after", "1"] if index == 2 else []))
            channel = grpc.insecure_channel(targets[index])
            channels.append(channel)
            grpc.channel_ready_future(channel).result(timeout=10)
        a, b = [CraneAgentStub(c) for c in channels[1:]]
        state = lambda stub: stub.GetAgentState(pb.Empty(), timeout=1)
        wait_for(lambda: state(a).has_token and not state(b).has_token)
        wait_for(lambda: list(state(a).request_queue) == ["crane-2"])
        run("cranelock.gate_demo", "clear", "--directory", tmp_path, "--target", targets[0], "--confirm-empty")
        wait_for(lambda: not state(a).has_token and state(b).has_token)
        gate = Client(gate_channel, Config.load(tmp_path / "config.json"), {}).state()
        assert (gate["epoch"], gate["revision"], gate["holder"]) == (0, 2, "crane-2")
        processes[2].terminate()
        processes[2].wait(timeout=5)
        start("agent-2-restarted", agent_commands[1])
        wait_for(lambda: state(b).has_token and state(b).revision == 2)
        restarted = state(b)
        assert restarted.mode == pb.LOCAL and restarted.token_holder == "crane-2"
    finally:
        for process in reversed(processes):
            if process.poll() is None:
                process.terminate()
            process.wait(timeout=5)
        for channel in channels:
            channel.close()
        for stream in streams:
            stream.close()
    events = [json.loads(line) for line in (tmp_path / "agent-2-restarted.log").read_text().splitlines()]
    assert any(e["event"] == "AGENT_STARTED" and e["mode"] == "REJOINING" for e in events)
    assert any(e["event"] == "GATE_STATE_RECONCILED" and e["result"] == "OK" for e in events)
