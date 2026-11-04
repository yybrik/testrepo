import argparse
import asyncio
import ipaddress
import time
import uuid
from collections.abc import Mapping

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
import grpc
from google.protobuf.json_format import MessageToDict

from cranelock_gate.client import Client
from cranelock_gate.crypto import CERTIFICATE_DOMAIN, canonical, encode_signature

from .agent import Agent, Mode
from . import protocol_pb2 as agent_pb
from . import protocol_pb2_grpc as agent_rpc


class ControllerError(RuntimeError):
    # Gate state cannot safely drive controller actions
    pass


class AgentStateMonitor:
    # Polls live state from configured crane-agent gRPC endpoints

    def __init__(self, agents, *, timeout=2.0):
        if not isinstance(agents, Mapping) or not agents:
            raise ValueError("agents must map agent IDs to gRPC addresses")
        if timeout <= 0:
            raise ValueError("timeout must be positive")
        if any(not isinstance(agent_id, str) or not agent_id
               or not isinstance(address, str) or not address
               for agent_id, address in agents.items()):
            raise ValueError("Agent IDs and gRPC addresses must be nonempty strings")
        self.timeout = timeout
        self.channels = {agent_id: grpc.insecure_channel(address)
                         for agent_id, address in agents.items()}
        self.stubs = {agent_id: agent_rpc.CraneAgentStub(channel)
                      for agent_id, channel in self.channels.items()}

    def read_states(self):
        results = {}
        for agent_id, stub in self.stubs.items():
            try:
                response = stub.GetAgentState(agent_pb.Empty(), timeout=self.timeout)
                state = MessageToDict(
                    response,
                    preserving_proto_field_name=True,
                    always_print_fields_with_no_presence=True,
                )
                if state["agent_id"] != agent_id:
                    results[agent_id] = {
                        "status": "IDENTITY_MISMATCH",
                        "reported_agent_id": state["agent_id"],
                    }
                else:
                    results[agent_id] = {"status": "AVAILABLE", "state": state}
            except grpc.RpcError as error:
                results[agent_id] = {
                    "status": "UNAVAILABLE",
                    "error": error.code().name,
                    "details": error.details() or "",
                }
        return {"observed_at": time.time(), "agents": results}

    def close(self):
        for channel in self.channels.values():
            channel.close()


def format_snapshot(snapshot, phase):
    # Formats a monitor snapshot as a readable per-agent status report
    observed_at = time.strftime(
        "%Y-%m-%d %H:%M:%S %z", time.localtime(snapshot["observed_at"]))
    lines = [f"Agent state {phase} - {observed_at}"]
    for agent_id, result in snapshot["agents"].items():
        status = result["status"]
        if status == "AVAILABLE":
            state = result["state"]
            token = "yes" if state.get("has_token") else "no"
            lines.append(
                f"  {agent_id}: {status} | epoch {state.get('epoch', '?')} | "
                f"{state.get('mode', '?')} | holds token: {token} | "
                f"holder: {state.get('token_holder') or '-'}")
            peers = state.get("peers", [])
            if peers:
                peer_status = ", ".join(
                    f"{peer.get('agent_id', '?')}={peer.get('status', '?')}"
                    for peer in peers)
                lines.append(f"    peers: {peer_status}")
            queue = state.get("request_queue", [])
            lines.append(f"    requests waiting: {', '.join(queue) if queue else '-'}")
            transfer = state.get("pending_transfer_id")
            if transfer:
                lines.append(f"    pending transfer: {transfer}")
        elif status == "IDENTITY_MISMATCH":
            lines.append(
                f"  {agent_id}: {status} | endpoint reported "
                f"{result.get('reported_agent_id', '?')}")
        else:
            detail = result.get("details")
            error = result.get("error", "UNKNOWN")
            lines.append(
                f"  {agent_id}: {status} | {error}"
                + (f" - {detail}" if detail else ""))
    return "\n".join(lines)


class CentralController:
    # Coordinates central authority for a configured gate.

    def __init__(self, config, private_key, channel):
        if not isinstance(private_key, Ed25519PrivateKey):
            raise TypeError("Controller signing key must be an Ed25519 private key")
        expected_key = bytes.fromhex(config.service_keys[config.controller_id])
        actual_key = private_key.public_key().public_bytes_raw()
        if actual_key != expected_key:
            raise ValueError("Controller private key does not match configured identity")
        self.config = config
        self.private_key = private_key
        self.client = Client(channel, config, {config.controller_id: private_key})
        self._heartbeat_server = None

    def get_state(self):
        # Returns the latest state reported by this gate.
        return self.client.state()

    def install_central(self, holder, *, request_id=None, max_attempts=3):
        # Installs CENTRAL authority, refreshing snapshots after stale decisions.
        if holder not in self.config.members:
            raise ValueError("Holder must be a configured member")
        if type(max_attempts) is not int or max_attempts < 1:
            raise ValueError("max_attempts must be a positive integer")
        if request_id is not None and (
                not isinstance(request_id, str) or not 1 <= len(request_id) <= 128):
            raise ValueError("request_id must contain 1 to 128 characters")

        for attempt in range(max_attempts):
            state = self.get_state()
            epoch = state.get("epoch")
            revision = state.get("revision")
            if type(epoch) is not int or epoch < -1 or type(revision) is not int or revision < 0:
                raise ControllerError("Gate returned invalid authority state")
            if epoch == -1 and revision != 0:
                raise ControllerError("Uninitialized gate has an unexpected revision")

            payload = dict(
                version=1,
                lane_id=self.config.lane_id,
                membership_id=self.config.membership_id,
                epoch=0 if epoch == -1 else epoch,
                mode="CENTRAL",
                holder=holder,
                expected_epoch=epoch,
                expected_revision=revision,
            )
            certificate = dict(
                payload=payload,
                signatures=[dict(
                    signer_id=self.config.controller_id,
                    signature=encode_signature(
                        self.private_key.sign(CERTIFICATE_DOMAIN + canonical(payload))),
                )],
            )
            result = self.client.call(
                self.config.controller_id,
                "InstallAuthority",
                request_id=request_id if attempt == 0 and request_id else str(uuid.uuid4()),
                certificate=certificate,
                expected_epoch=epoch,
                expected_revision=revision,
            )
            if result["allowed"] or result["reason"] not in {"STALE_EPOCH", "STALE_REVISION"}:
                return result
        return result

    def restore_central(self, *, max_attempts=3):
        """Restore CENTRAL on the latest gate holder without replacing newer authority."""
        if type(max_attempts) is not int or max_attempts < 1:
            raise ValueError("max_attempts must be a positive integer")
        for _ in range(max_attempts):
            state = self.get_state()
            epoch, revision, holder, mode = (
                state.get("epoch"), state.get("revision"),
                state.get("holder"), state.get("mode"))
            if (type(epoch) is not int or epoch < 0
                    or type(revision) is not int or revision < 0
                    or holder not in self.config.members
                    or mode not in {"CENTRAL", "LOCAL"}):
                raise ControllerError("Gate has no valid installed authority to restore")
            if mode == "CENTRAL":
                return state
            result = self.install_central(holder, max_attempts=1)
            if result["allowed"]:
                return result["state"]
            if result["reason"] not in {"STALE_EPOCH", "STALE_REVISION"}:
                raise ControllerError(
                    f"Gate rejected CENTRAL restoration: {result['reason']}")
        raise ControllerError("Gate authority kept changing during CENTRAL restoration")

    async def start_heartbeat_server(self, address="127.0.0.1:0", *, credentials=None):
        """Expose a heartbeat endpoint; successful heartbeats reconcile the gate to CENTRAL."""
        if self._heartbeat_server is not None:
            raise RuntimeError("Controller heartbeat server is already running")
        if not credentials:
            host = address.rsplit(":", 1)[0].strip("[]")
            try:
                loopback = ipaddress.ip_address(host).is_loopback
            except ValueError:
                loopback = host.lower() == "localhost"
            if not loopback:
                raise ValueError("Non-loopback controller listeners require TLS credentials")
        server = grpc.aio.server()
        agent_rpc.add_CraneControllerServicer_to_server(
            ControllerHeartbeatServicer(self), server)
        port = (server.add_secure_port(address, credentials) if credentials
                else server.add_insecure_port(address))
        if not port:
            raise RuntimeError("Cannot bind controller heartbeat address")
        await server.start()
        self._heartbeat_server = server
        return port

    async def stop_heartbeat_server(self):
        if self._heartbeat_server is not None:
            server, self._heartbeat_server = self._heartbeat_server, None
            await server.stop(0)

    async def reconcile_agents(self, agents):
        # Reconciles supplied agents from the gate's currently accepted authority.
        if not isinstance(agents, Mapping):
            raise TypeError("agents must be a mapping of member IDs to agent instances")
        for agent_id, agent in agents.items():
            if (agent_id not in self.config.members or not isinstance(agent, Agent)
                    or agent.state.agent_id != agent_id):
                raise ValueError("Agent mapping must use configured IDs matching each agent")

        state = await asyncio.to_thread(self.get_state)
        epoch = state.get("epoch")
        holder = state.get("holder")
        mode_name = state.get("mode")
        if (type(epoch) is not int or epoch < 0 or not isinstance(holder, str)
                or holder not in self.config.members
                or not isinstance(mode_name, str) or mode_name not in {"CENTRAL", "LOCAL"}):
            raise ControllerError("Gate has no valid installed authority to reconcile")

        mode = Mode[mode_name]
        for agent in agents.values():
            if agent.gate:
                if not await agent.reconcile_from_gate():
                    raise ControllerError("Agent could not reconcile from its gate")
            else:
                await agent.reconcile(epoch, holder, mode, revision=state["revision"])
        return state


class CentralControllerFleet:
    # Collects and dispatches controller operations across independent lane gates

    def __init__(self, controllers):
        if not isinstance(controllers, Mapping) or not controllers:
            raise ValueError("controllers must be a mapping of lane IDs to controller instances")
        for lane_id, controller in controllers.items():
            if (not isinstance(controller, CentralController)
                    or lane_id != controller.config.lane_id):
                raise ValueError("Each controller must be registered under its configured lane ID")
        self.controllers = dict(controllers)

    def get_all_states(self):
        # Reads every configured gate and propagates failures rather than omitting lanes.
        return {lane_id: controller.get_state()
                for lane_id, controller in self.controllers.items()}

    def install_central(self, lane_id, holder, **options):
        return self._controller(lane_id).install_central(holder, **options)

    async def reconcile_agents(self, lane_id, agents):
        # Reconciles agents for the selected lane.
        return await self._controller(lane_id).reconcile_agents(agents)

    def _controller(self, lane_id):
        try:
            return self.controllers[lane_id]
        except KeyError:
            raise ValueError(f"No gate is configured for lane {lane_id!r}") from None


class ControllerHeartbeatServicer(agent_rpc.CraneControllerServicer):
    def __init__(self, controller):
        self.controller = controller
        self._restore_lock = asyncio.Lock()

    async def Heartbeat(self, request, context):
        if request.agent_id not in self.controller.config.members:
            await context.abort(grpc.StatusCode.PERMISSION_DENIED, "Unknown agent")
        async with self._restore_lock:
            try:
                state = await asyncio.to_thread(self.controller.restore_central)
            except ControllerError as error:
                await context.abort(grpc.StatusCode.UNAVAILABLE, str(error))
            except grpc.RpcError as error:
                await context.abort(grpc.StatusCode.UNAVAILABLE, error.details() or str(error))
            except RuntimeError as error:
                await context.abort(grpc.StatusCode.UNAVAILABLE, str(error))
        return agent_pb.ControllerHeartbeatResponse(
            available=True,
            controller_id=self.controller.config.controller_id,
            epoch=state["epoch"],
            revision=state["revision"],
            holder=state["holder"],
            mode=state["mode"],
        )


class ControllerHeartbeatClient:
    def __init__(self, address, agent_id, controller_id, *, timeout=1.0, credentials=None):
        if not address or not agent_id or not controller_id or timeout <= 0:
            raise ValueError("Address, identities and positive timeout are required")
        self.agent_id = agent_id
        self.controller_id = controller_id
        self.timeout = timeout
        self.credentials = credentials
        self.channel = (grpc.aio.secure_channel(address, credentials) if credentials
                        else grpc.aio.insecure_channel(address))
        self.stub = agent_rpc.CraneControllerStub(self.channel)

    async def reconnect(self, address, *, credentials=None):
        if credentials is not None:
            self.credentials = credentials
        old_channel = self.channel
        self.channel = (grpc.aio.secure_channel(address, self.credentials) if self.credentials
                        else grpc.aio.insecure_channel(address))
        self.stub = agent_rpc.CraneControllerStub(self.channel)
        await old_channel.close()

    async def probe(self):
        response = await self.stub.Heartbeat(
            agent_pb.ControllerHeartbeatRequest(agent_id=self.agent_id),
            timeout=self.timeout)
        return (response.available and response.controller_id == self.controller_id
                and response.mode == "CENTRAL" and bool(response.holder))

    async def close(self):
        await self.channel.close()


def main():
    # Poll agent state and print each snapshot until interrupted
    parser = argparse.ArgumentParser(
        description="Monitor live CraneLock agent state from the central controller")
    parser.add_argument("--agent", action="append", required=True, metavar="ID=HOST:PORT")
    parser.add_argument("--interval", type=float, default=1.0)
    parser.add_argument("--timeout", type=float, default=2.0)
    args = parser.parse_args()
    if args.interval <= 0 or args.timeout <= 0:
        parser.error("--interval and --timeout must be positive")
    agents = {}
    for value in args.agent:
        if "=" not in value:
            parser.error(f"Invalid --agent {value!r}; expected ID=HOST:PORT")
        agent_id, address = value.split("=", 1)
        if not agent_id or not address or agent_id in agents:
            parser.error(f"Invalid or duplicate --agent {value!r}")
        agents[agent_id] = address

    monitor = AgentStateMonitor(agents, timeout=args.timeout)
    try:
        phase = "startup"
        while True:
            snapshot = monitor.read_states()
            print(format_snapshot(snapshot, phase), flush=True)
            phase = "update"
            time.sleep(args.interval)
    except KeyboardInterrupt:
        pass
    finally:
        monitor.close()


if __name__ == "__main__":
    main()