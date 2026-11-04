"""Signed client shared by the standalone harness and integration tests."""
import json
import uuid

import grpc

from .crypto import COMMAND_DOMAIN, CERTIFICATE_DOMAIN, canonical, encode_signature


def command(config, actor_id, operation, request_id=None, **fields):
    return dict(version=1, lane_id=config.lane_id, membership_id=config.membership_id,
                actor_id=actor_id, operation=operation,
                request_id=request_id or str(uuid.uuid4()), **fields)


def certificate(config, keys, state, holder, epoch, mode="LOCAL", signers=None):
    payload = dict(version=1, lane_id=config.lane_id, membership_id=config.membership_id,
                   epoch=epoch, mode=mode, holder=holder,
                   expected_epoch=state["epoch"], expected_revision=state["revision"])
    signers = signers or list(config.members)[:config.majority]
    return dict(payload=payload, signatures=[
        dict(signer_id=identity, signature=encode_signature(
            keys[identity].sign(CERTIFICATE_DOMAIN + canonical(payload)))) for identity in signers])


class Client:
    def __init__(self, channel, config, keys, timeout=3):
        from .generated import gate_pb2_grpc
        self.stub = gate_pb2_grpc.TrustedAccessGateStub(channel)
        self.config, self.keys, self.timeout = config, keys, timeout

    def send(self, payload):
        from .generated import gate_pb2
        signature = self.keys[payload["actor_id"]].sign(COMMAND_DOMAIN + canonical(payload))
        response = getattr(self.stub, payload["operation"])(
            gate_pb2.SignedCommand(payload_json=canonical(payload), signature=signature),
            timeout=self.timeout)
        return dict(allowed=response.allowed, reason=response.reason, replayed=response.replayed,
                    action_applied=response.action_applied, request_id=response.request_id,
                    state=json.loads(response.state_json or b"{}"),
                    grant=json.loads(response.grant_json or b"null"))

    def call(self, actor_id, operation, **fields):
        return self.send(command(self.config, actor_id, operation, **fields))

    def state(self):
        from .generated import gate_pb2
        response = self.stub.GetState(gate_pb2.StateRequest(), timeout=self.timeout)
        if not response.allowed:
            raise RuntimeError(response.reason)
        return json.loads(response.state_json)


def may_enter(result):
    """A retry acknowledgement is historical, never a new movement instruction."""
    return bool(result["allowed"] and result["action_applied"] and not result["replayed"])
