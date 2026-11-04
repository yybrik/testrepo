"""Deployment evidence: the real gate process over mutual TLS survives a hard kill."""
import json
import os
import queue
import re
import subprocess
import sys
import threading
import time

import grpc
import pytest

from cranelock_gate.client import Client, certificate, may_enter
from cranelock_gate.demo import create_demo_config, open_channel
from cranelock_gate.storage import initialize
from cranelock_gate.tls import write


class GateProcess:
    def __init__(self, directory, tls):
        self.process = subprocess.Popen([
            sys.executable, "-m", "cranelock_gate.server", "serve",
            "--config", str(directory / "config.json"), "--database", str(directory / "gate.sqlite"),
            "--bind", "127.0.0.1:0", "--metrics-port", "0",
            "--tls-cert", str(tls / "server.pem"), "--tls-key", str(tls / "server-key.pem"),
            "--tls-ca", str(tls / "ca.pem")],
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
            creationflags=subprocess.CREATE_NO_WINDOW if os.name == "nt" else 0)
        lines = queue.Queue()
        threading.Thread(target=lambda: [lines.put(line) for line in self.process.stdout],
                         daemon=True).start()
        deadline, output = time.monotonic() + 10, []
        while time.monotonic() < deadline:
            try:
                output.append(lines.get(timeout=0.1))
            except queue.Empty:
                continue
            match = re.search(r"\(port (\d+)\)", output[-1])
            if match:
                self.target = f"127.0.0.1:{match.group(1)}"
                return
        self.kill()
        raise AssertionError("Gate did not start: " + "".join(output))

    def kill(self):
        """Hard kill: no graceful shutdown, as in a node or container crash."""
        if self.process.poll() is None:
            self.process.kill()
        self.process.wait(timeout=5)
        self.process.stdout.close()


def healthcheck(target, *tls_args):
    return subprocess.run([sys.executable, "-m", "cranelock_gate.healthcheck", "--target", target,
                           *tls_args], capture_output=True, timeout=20).returncode


def test_tls_gate_process_persists_authority_and_fences_after_hard_kill(tmp_path):
    config, keys = create_demo_config(tmp_path)
    initialize(tmp_path / "gate.sqlite", config, time.time())
    tls = write(tmp_path / "tls")
    client_tls = dict(tls_ca=tls / "ca.pem", tls_cert=tls / "client.pem", tls_key=tls / "client-key.pem")
    probe_tls = ["--tls-ca", str(tls / "ca.pem"), "--tls-cert", str(tls / "probe.pem"),
                 "--tls-key", str(tls / "probe-key.pem")]

    gate = GateProcess(tmp_path, tls)
    try:
        assert healthcheck(gate.target, *probe_tls) == 0
        assert healthcheck(gate.target) == 1  # Plaintext cannot reach the TLS listener.
        anonymous = grpc.secure_channel(gate.target, grpc.ssl_channel_credentials(
            (tls / "ca.pem").read_bytes()))
        with anonymous, pytest.raises(grpc.RpcError):
            Client(anonymous, config, keys, timeout=1).state()

        with open_channel(gate.target, **client_tls) as channel:
            client = Client(channel, config, keys)
            state = client.state()
            assert client.call("crane-1", "InstallAuthority", expected_epoch=-1, expected_revision=0,
                               certificate=certificate(config, keys, state, "crane-1", 0))["allowed"]
            state = client.state()
            assert client.call("simulator", "ReportLaneState", session_id=state["session_id"],
                               observation_revision=state["observation_revision"], sequence=1,
                               observed_at_ms=int(time.time() * 1000), lane_state="CLEAR")["allowed"]
            grant = client.call("crane-1", "RequestGrant", epoch=0, revision=1)["grant"]
            state = client.state()
            old_entry = dict(epoch=0, revision=1, holder="crane-1", grant_id=grant["id"],
                             session_id=state["session_id"])
            assert may_enter(client.call("simulator", "ValidateEntry", **old_entry))
            before = client.state()
    finally:
        gate.kill()  # crane-1 is physically inside when the gate dies.

    gate = GateProcess(tmp_path, tls)
    try:
        assert healthcheck(gate.target, *probe_tls) == 0
        with open_channel(gate.target, **client_tls) as channel:
            client = Client(channel, config, keys)
            after = client.state()
            for key in ("epoch", "revision", "holder", "mode"):
                assert after[key] == before[key], key
            assert after["lane_state"] == "UNKNOWN" and after["grant"] is None
            assert after["session_id"] != before["session_id"]
            assert not client.call("simulator", "ValidateEntry", **old_entry)["allowed"]
            assert client.call("crane-1", "RequestGrant", epoch=0, revision=1)["reason"] == "LANE_UNKNOWN"

        state_cli = subprocess.run([
            sys.executable, "-m", "cranelock_gate.demo", "state", "--directory", str(tmp_path),
            "--target", gate.target, "--tls-ca", str(tls / "ca.pem"),
            "--tls-cert", str(tls / "client.pem"), "--tls-key", str(tls / "client-key.pem")],
            capture_output=True, text=True, timeout=20)
        assert state_cli.returncode == 0, state_cli.stderr
        assert json.loads(state_cli.stdout)["holder"] == "crane-1"
    finally:
        gate.kill()

    exported = subprocess.run([
        sys.executable, "-m", "cranelock_gate.server", "export-events", "--config",
        str(tmp_path / "config.json"), "--database", str(tmp_path / "gate.sqlite")],
        capture_output=True, text=True, timeout=20)
    events = [json.loads(line) for line in exported.stdout.splitlines()]
    entries = [e for e in events if e["operation"] == "ValidateEntry" and e["reason"] == "OK"]
    assert len(entries) == 1  # The pre-crash admission is durable; nothing was re-admitted.
    assert [e["operation"] for e in events].count("Restart") == 2


def test_generated_tls_material_refuses_overwrite(tmp_path):
    write(tmp_path)
    with pytest.raises(FileExistsError):
        write(tmp_path)
