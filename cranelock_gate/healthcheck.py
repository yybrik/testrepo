"""Container health check: occupancy UNKNOWN still means a healthy gate.

Without arguments it probes the plaintext development listener on loopback. For a
mutual-TLS listener, pass --tls-cert/--tls-key/--tls-ca (a client identity the gate
trusts) and, when the server certificate does not name 127.0.0.1, --server-name.
"""
import argparse
from pathlib import Path

import grpc
from grpc_health.v1 import health_pb2, health_pb2_grpc

SERVICE = "cranelock.gate.v1.TrustedAccessGate"


def check(target, credentials=None, server_name=None, timeout=2):
    if credentials:
        options = [("grpc.ssl_target_name_override", server_name)] if server_name else []
        channel = grpc.secure_channel(target, credentials, options=options)
    else:
        channel = grpc.insecure_channel(target)
    try:
        with channel:
            response = health_pb2_grpc.HealthStub(channel).Check(
                health_pb2.HealthCheckRequest(service=SERVICE), timeout=timeout)
            return response.status == health_pb2.HealthCheckResponse.SERVING
    except grpc.RpcError:
        return False


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--target", default="127.0.0.1:50051")
    parser.add_argument("--tls-cert")
    parser.add_argument("--tls-key")
    parser.add_argument("--tls-ca")
    parser.add_argument("--server-name", help="Expected name in the gate's server certificate")
    args = parser.parse_args(argv)
    credentials = None
    if any([args.tls_cert, args.tls_key, args.tls_ca]):
        if not all([args.tls_cert, args.tls_key, args.tls_ca]):
            parser.error("Provide --tls-cert, --tls-key and --tls-ca together for mutual TLS")
        credentials = grpc.ssl_channel_credentials(
            root_certificates=Path(args.tls_ca).read_bytes(),
            private_key=Path(args.tls_key).read_bytes(),
            certificate_chain=Path(args.tls_cert).read_bytes())
    raise SystemExit(0 if check(args.target, credentials, args.server_name) else 1)


if __name__ == "__main__":
    main()
