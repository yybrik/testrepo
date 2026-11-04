import argparse
import asyncio
import json
from pathlib import Path
import grpc
from google.protobuf.json_format import MessageToDict
from .agent import Agent, Mode
from . import protocol_pb2 as pb
from . import protocol_pb2_grpc as rpc
from .controller import ControllerHeartbeatClient


async def main():
    parser = argparse.ArgumentParser(description="CraneLock normal-operation demo")
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run")
    run.add_argument("--id", required=True)
    run.add_argument("--listen", required=True)
    run.add_argument("--peer", action="append", default=[], metavar="ID=HOST:PORT")
    run.add_argument("--holder", required=True)
    run.add_argument("--epoch", type=int, default=1)
    run.add_argument("--mode", choices=["CENTRAL", "LOCAL"], default="LOCAL")
    run.add_argument("--interval", type=float, default=0.5)
    run.add_argument("--timeout", type=float, default=2)
    run.add_argument("--state-file", required=True)
    run.add_argument("--request-after", type=float)
    run.add_argument("--gate-address")
    run.add_argument("--gate-config")
    run.add_argument("--gate-keys", help="JSON private signing keys; only this agent's key is retained")
    run.add_argument("--gate-tls-ca")
    run.add_argument("--gate-tls-cert")
    run.add_argument("--gate-tls-key")
    run.add_argument("--controller-address")
    run.add_argument("--controller-tls-ca")
    run.add_argument("--controller-tls-cert")
    run.add_argument("--controller-tls-key")
    state = commands.add_parser("state")
    state.add_argument("address")
    args = parser.parse_args()
    if args.command == "state":
        async with grpc.aio.insecure_channel(args.address) as channel:
            result = await rpc.CraneAgentStub(channel).GetAgentState(pb.Empty(), timeout=3)
            print(json.dumps(MessageToDict(result, preserving_proto_field_name=True,
                                          always_print_fields_with_no_presence=True), indent=2))
        return
    peers = dict(p.split("=", 1) for p in args.peer)
    if len(peers) != len(args.peer):
        parser.error("Duplicate peer IDs")
    gate = None
    if any([args.gate_tls_ca, args.gate_tls_cert, args.gate_tls_key]) and not args.gate_address:
        parser.error("Gate TLS options require --gate-address")
    if any([args.gate_address, args.gate_config, args.gate_keys]):
        if not all([args.gate_address, args.gate_config, args.gate_keys]):
            parser.error("Provide --gate-address, --gate-config and --gate-keys together")
        from .gate_client import GateClient
        credentials = None
        tls = [args.gate_tls_ca, args.gate_tls_cert, args.gate_tls_key]
        if any(tls):
            if not all(tls):
                parser.error("Provide all three --gate-tls-* files for mutual TLS")
            credentials = grpc.ssl_channel_credentials(
                Path(args.gate_tls_ca).read_bytes(), Path(args.gate_tls_key).read_bytes(),
                Path(args.gate_tls_cert).read_bytes())
        elif args.gate_address.rsplit(":", 1)[0] not in {"127.0.0.1", "localhost", "[::1]"}:
            parser.error("Gate connections outside loopback require mutual TLS")
        gate = GateClient.connect(args.gate_address, args.gate_config, args.gate_keys,
                                  args.id, timeout=args.timeout, credentials=credentials)
        if set(gate.client.config.members) != {args.id, *peers}:
            await gate.close()
            parser.error("Agent IDs and peers must match configured gate membership")
    if args.controller_address and not gate:
        parser.error("--controller-address requires gate configuration")
    controller_tls = [args.controller_tls_ca, args.controller_tls_cert,
                      args.controller_tls_key]
    if any(controller_tls) and (not args.controller_address or not all(controller_tls)):
        parser.error("Provide --controller-address and all --controller-tls-* files together")
    controller_probe = None
    if args.controller_address:
        controller_credentials = None
        if all(controller_tls):
            controller_credentials = grpc.ssl_channel_credentials(
                Path(args.controller_tls_ca).read_bytes(),
                Path(args.controller_tls_key).read_bytes(),
                Path(args.controller_tls_cert).read_bytes())
        elif args.controller_address.rsplit(":", 1)[0] not in {
                "127.0.0.1", "localhost", "[::1]"}:
            parser.error("Controller connections outside loopback require mutual TLS")
        controller_probe = ControllerHeartbeatClient(
            args.controller_address, args.id, gate.client.config.controller_id,
            timeout=args.timeout * .8, credentials=controller_credentials)
    agent = Agent(args.id, peers, args.holder, epoch=args.epoch, mode=Mode[args.mode],
                  interval=args.interval, timeout=args.timeout, state_file=args.state_file, gate=gate,
                  controller_probe=controller_probe.probe if controller_probe else None)
    try:
        await agent.start(args.listen)
        if args.request_after is not None:
            await asyncio.sleep(args.request_after)
            reply = await agent.request_token()
            agent.emit("TOKEN_REQUEST_RESULT", accepted=reply.accepted, reason=reply.reason)
        await asyncio.Event().wait()
    finally:
        await agent.close()
        if controller_probe:
            await controller_probe.close()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
