"""Local development fixtures using Max's existing client and bootstrap helper.

Not a recovery component or a physical simulator. Clearance is an explicit human
assertion about an empty development lane, never inferred from protocol state.
"""
import argparse
import json
from pathlib import Path
import time

import grpc

from cranelock_gate.client import Client, certificate
from cranelock_gate.config import Config
from cranelock_gate.demo import load_keys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["bootstrap", "state", "clear"])
    parser.add_argument("--directory", required=True)
    parser.add_argument("--target", default="127.0.0.1:50060")
    parser.add_argument("--confirm-empty", action="store_true")
    args = parser.parse_args()
    if args.target.rsplit(":", 1)[0] not in {"127.0.0.1", "localhost", "[::1]"}:
        parser.error("This combined-key development helper is loopback-only")
    directory = Path(args.directory)
    config = Config.load(directory / "config.json")
    keys = load_keys(directory / "private-keys.json") if args.action != "state" else {}
    with grpc.insecure_channel(args.target) as channel:
        client = Client(channel, config, keys)
        state = client.state()
        if args.action == "bootstrap":
            if state["epoch"] != -1:
                parser.error("Bootstrap is only for a fresh uninitialized development gate")
            # Reuse the gate author's existing initial-authority fixture verbatim.
            cert = certificate(config, keys, state, "crane-1", 0)
            result = client.call("crane-1", "InstallAuthority", certificate=cert,
                                 expected_epoch=-1, expected_revision=state["revision"])
        elif args.action == "clear":
            if not args.confirm_empty:
                parser.error("Independently confirm the simulated lane is empty; pass --confirm-empty")
            result = client.call(config.operator_id, "ClearLane", session_id=state["session_id"],
                observation_revision=state["observation_revision"],
                sequence=state["observation_sequence"] + 1,
                observed_at_ms=int(time.time() * 1000), reason="Human confirms empty development lane")
        else:
            result = state
        print(json.dumps(result, indent=2))
        if args.action != "state" and not result["allowed"]:
            raise SystemExit(1)


if __name__ == "__main__":
    main()
