"""Exercise console-signal shutdown in a real child process, not a mocked wait."""
import os
import queue
import signal
import subprocess
import sys
import threading

from cranelock_gate.demo import create_demo_config
from cranelock_gate.storage import initialize


def test_console_signal_stops_server_and_releases_database(tmp_path):
    import time
    config, _ = create_demo_config(tmp_path)
    database = tmp_path / "gate.sqlite"
    initialize(database, config, time.time())
    process = subprocess.Popen([
        sys.executable, "-m", "cranelock_gate.server", "serve",
        "--config", str(tmp_path / "config.json"), "--database", str(database),
        "--bind", "127.0.0.1:0", "--metrics-port", "0"],
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if os.name == "nt" else 0)
    lines = queue.Queue()

    def read_output():
        for line in process.stdout:
            lines.put(line)

    reader = threading.Thread(target=read_output, daemon=True)
    reader.start()
    try:
        deadline = time.monotonic() + 10
        output = []
        while time.monotonic() < deadline:
            line = lines.get(timeout=max(0.01, deadline - time.monotonic()))
            output.append(line)
            if "Gate listening" in line:
                break
        else:
            raise AssertionError("Server did not become ready: " + "".join(output))
        # Windows allows CTRL_BREAK_EVENT to be targeted to a new process group;
        # console SIGINT and SIGBREAK both follow the same short-wait shutdown path.
        process.send_signal(signal.CTRL_BREAK_EVENT if os.name == "nt" else signal.SIGINT)
        assert process.wait(timeout=8) == 0
        reader.join(timeout=2)
        while not lines.empty():
            output.append(lines.get_nowait())
        assert "Gate stopped." in "".join(output)
        from cranelock_gate.core import Gate
        gate = Gate(database, config)
        try:
            assert gate.get_state()["state"]["lane_state"] == "UNKNOWN"
        finally:
            gate.close()
    finally:
        if process.poll() is None:
            process.kill()
            process.wait(timeout=5)
        reader.join(timeout=2)
        process.stdout.close()
