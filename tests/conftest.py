import pytest

from cranelock_gate.client import certificate, command
from cranelock_gate.core import Gate
from cranelock_gate.crypto import COMMAND_DOMAIN, canonical
from cranelock_gate.demo import create_demo_config
from cranelock_gate.storage import initialize


class Clock:
    def __init__(self):
        self.now = 1000.0

    def __call__(self):
        return self.now

    def advance(self, amount):
        self.now += amount


class Harness:
    def __init__(self, path):
        self.config, self.keys = create_demo_config(path)
        self.path = path / "gate.sqlite"
        self.clock = Clock()
        initialize(self.path, self.config, self.clock())
        self.gate = Gate(self.path, self.config, self.clock)

    @property
    def state(self):
        return self.gate.get_state()["state"]

    def send(self, payload):
        return self.gate.execute(payload["operation"], canonical(payload),
                                 self.keys[payload["actor_id"]].sign(COMMAND_DOMAIN + canonical(payload)))

    def call(self, actor, operation, **fields):
        return self.send(command(self.config, actor, operation, **fields))

    def install(self, holder="crane-1", epoch=None, mode="LOCAL", signers=None):
        state = self.state
        epoch = state["epoch"] + 1 if epoch is None else epoch
        actor = "controller" if mode == "CENTRAL" else holder
        cert = certificate(self.config, self.keys, state, holder, epoch, mode,
                           signers or (["controller"] if mode == "CENTRAL" else None))
        return self.call(actor, "InstallAuthority", certificate=cert,
                         expected_epoch=state["epoch"], expected_revision=state["revision"])

    def observe(self, lane="CLEAR", actor="simulator", **overrides):
        state = self.state
        fields = dict(session_id=state["session_id"], sequence=state["observation_sequence"] + 1,
                      observation_revision=state["observation_revision"],
                      observed_at_ms=int(self.clock() * 1000))
        if actor == "operator":
            fields["reason"] = "Test operator clearance"
            operation = "ClearLane"
        else:
            fields["lane_state"] = lane
            operation = "ReportLaneState"
        fields.update(overrides)
        return self.call(actor, operation, **fields)

    def ready(self):
        assert self.install()["allowed"]
        assert self.observe()["allowed"]

    def grant(self, actor=None, **overrides):
        state = self.state
        fields = dict(epoch=state["epoch"], revision=state["revision"])
        fields.update(overrides)
        return self.call(actor or state["holder"], "RequestGrant", **fields)

    def enter(self, actor="simulator", **overrides):
        state = self.state
        grant = state["grant"]
        fields = dict(epoch=state["epoch"], revision=state["revision"], holder=state["holder"],
                      grant_id=grant["id"], session_id=state["session_id"])
        fields.update(overrides)
        return self.call(actor, "ValidateEntry", **fields)

    def restart(self):
        self.gate.close()
        self.gate = Gate(self.path, self.config, self.clock)


@pytest.fixture
def h(tmp_path):
    harness = Harness(tmp_path)
    yield harness
    harness.gate.close()
