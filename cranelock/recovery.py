# validates and signs recovery proposals
import hashlib
import json

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

from cranelock_gate.crypto import CERTIFICATE_DOMAIN, canonical, encode_signature, verify


FIELDS = {"version", "lane_id", "membership_id", "epoch", "mode", "holder",
          "expected_epoch", "expected_revision"}
MAX_INTEGER = 2**63 - 1


class RecoveryError(ValueError):
    pass


# validates the gate proposal format
def parse_proposal(raw, config):
    try:
        payload = json.loads(raw)
    except (json.JSONDecodeError, UnicodeDecodeError, TypeError) as error:
        raise RecoveryError("INVALID_PROPOSAL") from error
    if (type(payload) is not dict or set(payload) != FIELDS or canonical(payload) != raw
            or payload["version"] != 1 or type(payload["version"]) is not int
            or payload["lane_id"] != config.lane_id
            or payload["membership_id"] != config.membership_id
            or payload["mode"] != "LOCAL"
            or payload["holder"] not in config.members
            or type(payload["epoch"]) is not int or type(payload["expected_epoch"]) is not int
            or type(payload["expected_revision"]) is not int
            or not -1 <= payload["expected_epoch"] <= MAX_INTEGER
            or not 0 <= payload["epoch"] <= MAX_INTEGER
            or not 0 <= payload["expected_revision"] <= MAX_INTEGER
            or payload["epoch"] != payload["expected_epoch"] + 1):
        raise RecoveryError("INVALID_PROPOSAL")
    return payload


# creates the next epoch proposal
def proposal(config, gate_state, holder):
    if holder not in config.members:
        raise RecoveryError("INVALID_HOLDER")
    payload = dict(version=1, lane_id=config.lane_id,
                   membership_id=config.membership_id,
                   epoch=gate_state["epoch"] + 1, mode="LOCAL", holder=holder,
                   expected_epoch=gate_state["epoch"],
                   expected_revision=gate_state["revision"])
    parse_proposal(canonical(payload), config)
    return payload


# signs a recovery proposal
def sign(private_key, payload):
    return private_key.sign(CERTIFICATE_DOMAIN + canonical(payload))


# verifies a member signature
def verify_member(config, member_id, signature, payload):
    return member_id in config.members and verify(
        config.members[member_id], signature, payload, CERTIFICATE_DOMAIN)


# records one vote per target epoch
def record_vote(vote_state, payload):
    target = str(payload["epoch"])
    digest = hashlib.sha256(canonical(payload)).hexdigest()
    previous = vote_state.get(target)
    if previous is not None and previous != digest:
        raise RecoveryError("CONFLICTING_VOTE")
    vote_state[target] = digest


# builds the gate certificate
def certificate(payload, signatures):
    return dict(payload=payload, signatures=[
        dict(signer_id=member_id, signature=encode_signature(signature))
        for member_id, signature in sorted(signatures.items())])


# checks the member private key
def validate_identity(config, member_id, private_key):
    expected = bytes.fromhex(config.members[member_id])
    actual = private_key.public_key().public_bytes_raw()
    if actual != expected:
        raise ValueError("Recovery private key does not match configured member")
    Ed25519PublicKey.from_public_bytes(expected)
