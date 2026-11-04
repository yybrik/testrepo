"""Exercise the actual CLI in two independent processes, including peer loss."""
import json
import os
import subprocess
import sys
import time

import grpc
from cranelock import protocol_pb2 as pb
from cranelock.protocol_pb2_grpc import CraneAgentStub


def test_two_process_demo(tmp_path):
    # Ask gRPC for OS-assigned ports without depending on fixed demo ports.
    import socket
    sockets = [socket.socket(), socket.socket()]
    for s in sockets:
        s.bind(("127.0.0.1", 0))
    ports = [s.getsockname()[1] for s in sockets]
    for s in sockets:
        s.close()
    processes, streams = [], []
    channels = [grpc.insecure_channel(f"127.0.0.1:{p}") for p in ports]
    try:
        for index, name in enumerate(["crane-a", "crane-b"]):
            other = 1 - index
            stream = (tmp_path / f"{name}.log").open("w")
            streams.append(stream)
            command = [sys.executable, "-m", "cranelock", "run", "--id", name,
                "--listen", f"127.0.0.1:{ports[index]}", "--peer",
                f"{'crane-b' if index == 0 else 'crane-a'}=127.0.0.1:{ports[other]}",
                "--holder", "crane-a", "--interval", "0.1", "--timeout", "0.6",
                "--state-file", str(tmp_path / f"{name}.json")]
            if index:
                command += ["--request-after", "1"]
            processes.append(subprocess.Popen(command, stdout=stream, stderr=stream,
                creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0))
        for channel in channels:
            grpc.channel_ready_future(channel).result(timeout=10)
        stubs = [CraneAgentStub(c) for c in channels]
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            states = [stub.GetAgentState(pb.Empty(), timeout=1) for stub in stubs]
            if states[1].has_token:
                break
            time.sleep(0.05)
        assert not states[0].has_token and states[1].has_token
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            state = stubs[0].GetAgentState(pb.Empty(), timeout=1)
            if state.peers[0].status == "AVAILABLE":
                break
            time.sleep(0.05)
        assert state.peers[0].status == "AVAILABLE"
        processes[1].terminate()
        processes[1].wait(timeout=5)
        deadline = time.monotonic() + 5
        while time.monotonic() < deadline:
            state = stubs[0].GetAgentState(pb.Empty(), timeout=1)
            if state.peers[0].status == "SUSPECTED_UNAVAILABLE":
                break
            time.sleep(0.05)
        assert state.peers[0].status == "SUSPECTED_UNAVAILABLE"
        assert not state.has_token
    finally:
        for process in processes:
            if process.poll() is None:
                process.terminate()
            process.wait(timeout=5)
        for channel in channels:
            channel.close()
        for stream in streams:
            stream.close()
    events = [json.loads(line) for line in (tmp_path / "crane-a.log").read_text().splitlines()]
    assert any(e["event"] == "TOKEN_HOLDER_SUSPECTED" for e in events)
