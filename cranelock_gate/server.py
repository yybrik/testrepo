import argparse
import json
import logging
import signal
import threading
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import grpc
from grpc_health.v1 import health, health_pb2, health_pb2_grpc
from prometheus_client import CollectorRegistry, Counter, Gauge, start_http_server

from .config import Config
from .core import Gate
from .crypto import canonical
from .generated import gate_pb2, gate_pb2_grpc
from .storage import initialize

SERVICE = "cranelock.gate.v1.TrustedAccessGate"


def response(result):
    return gate_pb2.Decision(
        allowed=result["allowed"], reason=result["reason"], replayed=result["replayed"],
        action_applied=result["action_applied"], request_id=result["request_id"],
        state_json=canonical(result["state"]), grant_json=canonical(result["grant"]))


def bind_host(address):
    """Host part of a gRPC bind address, e.g. "[::1]:50051" -> "::1"."""
    return address.rsplit(":", 1)[0].strip("[]")


class Metrics:
    def __init__(self):
        self.registry = CollectorRegistry()
        self.decisions = Counter("cranelock_gate_decisions", "Authenticated gate decisions",
                                 ["operation", "reason", "replayed"], registry=self.registry)
        self.grants = Counter("cranelock_gate_grants", "New durable grants", registry=self.registry)
        self.entries = Counter("cranelock_gate_entries", "New accepted entries", registry=self.registry)
        self.stopped = Gauge("cranelock_gate_safety_stop_seconds", "Durable UNKNOWN state duration",
                             registry=self.registry)
        self.unknown = Gauge("cranelock_gate_lane_unknown", "Lane needs clearance", registry=self.registry)
        self.available = Gauge("cranelock_gate_available", "Storage/service healthy", registry=self.registry)

    def observe(self, operation, result):
        self.decisions.labels(operation, result["reason"], str(result["replayed"]).lower()).inc()
        if result["action_applied"]:
            if operation == "RequestGrant":
                self.grants.inc()
            if operation == "ValidateEntry":
                self.entries.inc()

    def update(self, state, now, available):
        self.available.set(available)
        self.unknown.set(state.get("lane_state") == "UNKNOWN")
        total = state.get("safety_stop_total_seconds", 0)
        since = state.get("safety_stop_since")
        self.stopped.set(total + (max(0, now - since) if since is not None else 0))


class Servicer(gate_pb2_grpc.TrustedAccessGateServicer):
    def __init__(self, gate, metrics):
        self.gate, self.metrics = gate, metrics

    def _execute(self, operation, request):
        result = self.gate.execute(operation, request.payload_json, request.signature)
        self.metrics.observe(operation, result)
        return response(result)

    def InstallAuthority(self, request, context):
        return self._execute("InstallAuthority", request)

    def TransferHolder(self, request, context):
        return self._execute("TransferHolder", request)

    def RequestGrant(self, request, context):
        return self._execute("RequestGrant", request)

    def ValidateEntry(self, request, context):
        return self._execute("ValidateEntry", request)

    def ReportLaneState(self, request, context):
        return self._execute("ReportLaneState", request)

    def ClearLane(self, request, context):
        return self._execute("ClearLane", request)

    def GetState(self, request, context):
        return response(self.gate.get_state())


class RunningServer:
    def __init__(self, gate, address="127.0.0.1:50051", metrics_port=None, credentials=None,
                 metrics_address="127.0.0.1"):
        self.gate = gate
        self.metrics = Metrics()
        self.server = grpc.server(ThreadPoolExecutor(max_workers=16), options=[
            ("grpc.max_receive_message_length", 131072)])
        gate_pb2_grpc.add_TrustedAccessGateServicer_to_server(Servicer(gate, self.metrics), self.server)
        self.health = health.HealthServicer()
        health_pb2_grpc.add_HealthServicer_to_server(self.health, self.server)
        self.port = (self.server.add_secure_port(address, credentials) if credentials
                     else self.server.add_insecure_port(address))
        if not self.port:
            raise RuntimeError("Unable to bind gRPC port")
        self.done = threading.Event()
        self.http = None
        if metrics_port is not None:
            # The metrics endpoint is unauthenticated: never widen it beyond the requested host.
            self.http, self.http_thread = start_http_server(metrics_port, addr=metrics_address,
                                                            registry=self.metrics.registry)
        self.server.start()
        self._check()
        self.thread = threading.Thread(target=self._maintain, daemon=True)
        self.thread.start()

    def _check(self):
        result = self.gate.get_state()
        healthy = result["allowed"] and not self.gate.failed
        status = health_pb2.HealthCheckResponse.SERVING if healthy else health_pb2.HealthCheckResponse.NOT_SERVING
        # UNKNOWN occupancy is a safe operational state, not a reason to restart the service.
        for service in ("", SERVICE):
            self.health.set(service, status)
        self.metrics.update(result["state"], self.gate.clock(), healthy)

    def _maintain(self):
        while not self.done.wait(0.2):
            self._check()

    def close(self):
        self.done.set()
        self.thread.join(timeout=3)
        self.health.enter_graceful_shutdown()
        self.server.stop(1).wait(timeout=3)
        if self.http:
            self.http.shutdown()
            self.http.server_close()
        self.gate.close()


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="action", required=True)
    for name in ("init", "serve", "export-events"):
        command = sub.add_parser(name)
        command.add_argument("--config", required=True)
        command.add_argument("--database", required=True)
        if name == "serve":
            command.add_argument("--bind", default="127.0.0.1:50051")
            command.add_argument("--metrics-port", type=int, default=9090)
            command.add_argument("--metrics-bind", help="Metrics host; defaults to the --bind host")
            command.add_argument("--tls-cert")
            command.add_argument("--tls-key")
            command.add_argument("--tls-ca")
            command.add_argument("--allow-insecure-development", action="store_true")
    args = parser.parse_args()
    config = Config.load(args.config)
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    if args.action == "init":
        import time
        initialize(args.database, config, time.time())
        print("New experiment initialized. Lane remains UNKNOWN until trusted confirmation.")
    elif args.action == "export-events":
        # Read-only export does not instantiate Gate or invalidate running grants.
        import sqlite3
        db = sqlite3.connect(Path(args.database).resolve().as_uri() + "?mode=ro", uri=True)
        try:
            for row in db.execute("SELECT value FROM events ORDER BY event_id"):
                print(row[0])
        finally:
            db.close()
    else:
        credentials = None
        if any([args.tls_cert, args.tls_key, args.tls_ca]):
            if not all([args.tls_cert, args.tls_key, args.tls_ca]):
                parser.error("Provide --tls-cert, --tls-key and --tls-ca together for mutual TLS")
            credentials = grpc.ssl_server_credentials(
                [(Path(args.tls_key).read_bytes(), Path(args.tls_cert).read_bytes())],
                root_certificates=Path(args.tls_ca).read_bytes(), require_client_auth=True)
        elif (args.bind.rsplit(":", 1)[0] not in {"127.0.0.1", "localhost", "[::1]"}
              and not args.allow_insecure_development):
            parser.error("Network listeners require mutual TLS or explicit --allow-insecure-development")
        metrics_address = args.metrics_bind or bind_host(args.bind)
        gate = Gate(args.database, config)
        try:
            server = RunningServer(gate, args.bind, args.metrics_port, credentials, metrics_address)
        except Exception:
            gate.close()
            raise
        stop = threading.Event()
        signals = [signal.SIGINT, signal.SIGTERM]
        if hasattr(signal, "SIGBREAK"):
            signals.append(signal.SIGBREAK)
        for sig in signals:
            signal.signal(sig, lambda *_: stop.set())
        LOG = logging.getLogger("cranelock.gate")
        LOG.info("Gate listening on %s (port %s). Press Ctrl+C to stop.", args.bind, server.port)
        try:
            # On Windows, an indefinite Event.wait can prevent Python from
            # dispatching a pending console signal. Return to Python regularly.
            while not stop.wait(timeout=0.2):
                pass
        finally:
            LOG.info("Stopping gate...")
            server.close()
            LOG.info("Gate stopped.")


if __name__ == "__main__":
    main()
