import json
import urllib.request

import grpc
import pytest
from grpc_health.v1 import health_pb2, health_pb2_grpc
from prometheus_client import generate_latest

from cranelock_gate.client import Client, may_enter
from cranelock_gate.generated import gate_pb2
from cranelock_gate.server import RunningServer, SERVICE, bind_host


def test_grpc_real_round_trip_health_metrics_and_unavailability(h):
    server = RunningServer(h.gate, "127.0.0.1:0", metrics_port=0)
    channel = grpc.insecure_channel(f"127.0.0.1:{server.port}")
    try:
        grpc.channel_ready_future(channel).result(timeout=3)
        client = Client(channel, h.config, h.keys, timeout=0.5)
        status = health_pb2_grpc.HealthStub(channel).Check(health_pb2.HealthCheckRequest(service=SERVICE))
        assert status.status == health_pb2.HealthCheckResponse.SERVING
        h.ready()
        state = client.state()
        result = client.call("crane-1", "RequestGrant", epoch=state["epoch"], revision=state["revision"])
        assert result["allowed"]
        grant = result["grant"]
        entered = client.call("simulator", "ValidateEntry", epoch=state["epoch"], revision=state["revision"],
                              holder=state["holder"], grant_id=grant["id"], session_id=grant["id"])
        assert may_enter(entered)
        assert client.state()["lane_state"] == "OCCUPIED"
        assert server.http.server_address[0] == "127.0.0.1"  # Never all interfaces by default.
        url = f"http://127.0.0.1:{server.http.server_port}/metrics"
        with urllib.request.urlopen(url, timeout=2) as reply:
            metrics = reply.read().decode()
        assert "cranelock_gate_grants_total 1.0" in metrics
        assert "cranelock_gate_entries_total 1.0" in metrics
        # A malformed network request is denied and leaves the service healthy.
        bad = client.stub.RequestGrant(gate_pb2.SignedCommand(payload_json=b"{}"), timeout=0.5)
        assert not bad.allowed and bad.reason == "INVALID_REQUEST"
        h.gate.failed = True
        server._check()
        status = health_pb2_grpc.HealthStub(channel).Check(health_pb2.HealthCheckRequest(service=SERVICE))
        assert status.status == health_pb2.HealthCheckResponse.NOT_SERVING
    finally:
        server.close()
    try:
        with pytest.raises(grpc.RpcError):
            client.call("crane-1", "RequestGrant", epoch=0, revision=1)
    finally:
        channel.close()


def test_signed_envelope_cannot_be_sent_to_another_rpc(h):
    from cranelock_gate.client import command
    from cranelock_gate.crypto import COMMAND_DOMAIN, canonical
    server = RunningServer(h.gate, "127.0.0.1:0")
    try:
        with grpc.insecure_channel(f"127.0.0.1:{server.port}") as channel:
            grpc.channel_ready_future(channel).result(timeout=3)
            client = Client(channel, h.config, h.keys)
            h.ready()
            payload = command(h.config, "crane-1", "RequestGrant", epoch=0, revision=1)
            request = gate_pb2.SignedCommand(payload_json=canonical(payload),
                signature=h.keys["crane-1"].sign(COMMAND_DOMAIN + canonical(payload)))
            response = client.stub.TransferHolder(request, timeout=2)
            assert response.reason == "INVALID_REQUEST"
            assert client.state()["lane_state"] == "CLEAR"
    finally:
        server.close()


@pytest.mark.parametrize("address,host", [
    ("127.0.0.1:50051", "127.0.0.1"), ("0.0.0.0:50051", "0.0.0.0"),
    ("[::1]:50051", "::1"), ("localhost:50051", "localhost")])
def test_metrics_follow_grpc_bind_host(address, host):
    assert bind_host(address) == host
