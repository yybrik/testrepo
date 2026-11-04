"""One cross-language signing format, shared by clients and certificate producers."""
import base64
import json
from cryptography.exceptions import InvalidSignature

from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey

COMMAND_DOMAIN = b"cranelock.command.v1\x00"
CERTIFICATE_DOMAIN = b"cranelock.authority.v1\x00"


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=True, allow_nan=False).encode("ascii")


def encode_signature(signature):
    return base64.b64encode(signature).decode("ascii")


def verify(public_key, signature, payload, domain=COMMAND_DOMAIN):
    try:
        Ed25519PublicKey.from_public_bytes(bytes.fromhex(public_key)).verify(
            signature, domain + canonical(payload))
        return True
    except (ValueError, TypeError, InvalidSignature):
        return False
