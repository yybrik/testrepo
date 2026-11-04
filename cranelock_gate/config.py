import json
import math
from dataclasses import dataclass
from pathlib import Path
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PublicKey


@dataclass(frozen=True)
class Config:
    lane_id: str
    membership_id: str
    members: dict[str, str]
    controller_id: str
    simulator_id: str
    operator_id: str
    service_keys: dict[str, str]
    grant_ttl_seconds: float = 5.0
    observation_timeout_seconds: float = 2.0
    future_skew_seconds: float = 0.25

    def __post_init__(self):
        ids = [self.controller_id, self.simulator_id, self.operator_id]
        if (not self.lane_id or not self.membership_id or len(self.members) < 3
                or len(set(ids)) != 3 or set(ids) & set(self.members)
                or set(self.service_keys) != set(ids)):
            raise ValueError("Use >=3 fixed members and three distinct service identities")
        keys = list(self.members.values()) + list(self.service_keys.values())
        raw_keys = [bytes.fromhex(key) for key in keys]
        if len(set(raw_keys)) != len(raw_keys):
            raise ValueError("Every identity needs a distinct public key")
        for key in raw_keys:
            Ed25519PublicKey.from_public_bytes(key)
        for value in (self.grant_ttl_seconds, self.observation_timeout_seconds):
            if not math.isfinite(value) or value <= 0:
                raise ValueError("Timeouts must be finite and positive")
        if not math.isfinite(self.future_skew_seconds) or self.future_skew_seconds < 0:
            raise ValueError("Clock skew must be finite and nonnegative")

    @property
    def keys(self):
        return {**self.members, **self.service_keys}

    @property
    def majority(self):
        return len(self.members) // 2 + 1

    @classmethod
    def load(cls, path):
        return cls(**json.loads(Path(path).read_text(encoding="utf-8")))
