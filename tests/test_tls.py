"""Authenticate the gate's replies as well as the signed client requests."""
import datetime
import ipaddress

import grpc
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import rsa
from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

from cranelock_gate.client import Client
from cranelock_gate.server import RunningServer


def credentials():
    root_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    root_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "CraneLock test CA")])
    now = datetime.datetime.now(datetime.timezone.utc)

    def build(name, key, issuer, usage=None):
        builder = (x509.CertificateBuilder().subject_name(name).issuer_name(issuer)
                   .public_key(key.public_key()).serial_number(x509.random_serial_number())
                   .not_valid_before(now - datetime.timedelta(minutes=1))
                   .not_valid_after(now + datetime.timedelta(days=1))
                   .add_extension(x509.BasicConstraints(ca=usage is None, path_length=None), critical=True))
        if usage is not None:
            builder = builder.add_extension(x509.ExtendedKeyUsage([usage]), critical=False)
            builder = builder.add_extension(x509.SubjectAlternativeName([
                x509.DNSName("localhost"), x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]), critical=False)
        return builder.sign(root_key, hashes.SHA256()).public_bytes(serialization.Encoding.PEM)

    def key_bytes(key):
        return key.private_bytes(serialization.Encoding.PEM, serialization.PrivateFormat.PKCS8,
                                 serialization.NoEncryption())

    ca = build(root_name, root_key, root_name)
    server_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    client_key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    server_cert = build(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "localhost")]),
                        server_key, root_name, ExtendedKeyUsageOID.SERVER_AUTH)
    client_cert = build(x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "test-client")]),
                        client_key, root_name, ExtendedKeyUsageOID.CLIENT_AUTH)
    return ca, key_bytes(server_key), server_cert, key_bytes(client_key), client_cert


def test_mutual_tls_accepts_client_certificate_and_rejects_anonymous(h):
    ca, server_key, server_cert, client_key, client_cert = credentials()
    secure = grpc.ssl_server_credentials([(server_key, server_cert)], ca, require_client_auth=True)
    server = RunningServer(h.gate, "127.0.0.1:0", credentials=secure)
    target = f"127.0.0.1:{server.port}"
    try:
        with grpc.secure_channel(target, grpc.ssl_channel_credentials(ca, client_key, client_cert)) as channel:
            grpc.channel_ready_future(channel).result(timeout=3)
            client = Client(channel, h.config, h.keys)
            assert client.state()["lane_state"] == "UNKNOWN"
            h.ready()
            assert client.call("crane-1", "RequestGrant", epoch=0, revision=1)["allowed"]
        with grpc.secure_channel(target, grpc.ssl_channel_credentials(ca)) as channel:
            client = Client(channel, h.config, h.keys, timeout=0.4)
            with pytest.raises(grpc.RpcError):
                client.state()
    finally:
        server.close()


def test_health_check_authenticates_with_client_certificate(h):
    from cranelock_gate.healthcheck import check
    ca, server_key, server_cert, client_key, client_cert = credentials()
    secure = grpc.ssl_server_credentials([(server_key, server_cert)], ca, require_client_auth=True)
    server = RunningServer(h.gate, "127.0.0.1:0", credentials=secure)
    target = f"127.0.0.1:{server.port}"
    try:
        trusted = grpc.ssl_channel_credentials(ca, client_key, client_cert)
        assert check(target, trusted)
        assert check(target, trusted, server_name="localhost")
        assert not check(target, grpc.ssl_channel_credentials(ca), timeout=0.4)
        assert not check(target, timeout=0.4)  # Plaintext probes cannot reach a TLS listener.
    finally:
        server.close()
