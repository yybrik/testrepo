"""Transport-independent, fail-closed gate state machine."""
import base64
import hashlib
import json
import logging
import math
import sqlite3
import threading
import time
import uuid
from pathlib import Path

from .crypto import CERTIFICATE_DOMAIN, canonical, verify
from .storage import ProcessLease, configuration_hash, connect, load_state, save_state

LOG = logging.getLogger("cranelock.gate")
COMMON = {"version", "lane_id", "membership_id", "request_id", "actor_id", "operation"}
SCHEMAS = {
    "InstallAuthority": {"certificate", "expected_epoch", "expected_revision"},
    "TransferHolder": {"epoch", "revision", "destination"},
    "RequestGrant": {"epoch", "revision"},
    "ValidateEntry": {"epoch", "revision", "holder", "grant_id", "session_id"},
    "ReportLaneState": {"session_id", "observation_revision", "sequence", "observed_at_ms", "lane_state"},
    "ClearLane": {"session_id", "observation_revision", "sequence", "observed_at_ms", "reason"},
}
CERT_FIELDS = {"version", "lane_id", "membership_id", "epoch", "mode", "holder",
               "expected_epoch", "expected_revision"}


class Reject(Exception):
    def __init__(self, reason):
        self.reason = reason


def integer(value, minimum=0):
    return type(value) is int and minimum <= value <= 2**63 - 1


def finite(value):
    return type(value) in (int, float) and math.isfinite(value)


class Gate:
    def __init__(self, database, config, clock=time.time):
        self.path = Path(database)
        if not self.path.is_file():
            raise RuntimeError("Gate database missing; explicit init is required for a NEW experiment")
        self.config, self.clock = config, clock
        self.lock = threading.RLock()
        self.failed = False
        self.closed = False
        self.lease = ProcessLease(self.path)
        self.db = None
        try:
            self.db = connect(self.path)
            if self.db.execute("PRAGMA quick_check").fetchone()[0] != "ok":
                raise RuntimeError("Database integrity check failed")
            row = self.db.execute("SELECT version, configuration_hash FROM metadata WHERE id=1").fetchone()
            if not row or row[0] not in (1, 2) or row[1] != configuration_hash(config):
                raise RuntimeError("Database schema/configuration mismatch")
            self.db.execute("BEGIN IMMEDIATE")
            state = load_state(self.db)
            if row[0] == 1:
                # Earlier development databases predate phase-bound observations.
                # Preserve all authority/cache/audit state; startup still fences grants.
                state.setdefault("observation_revision", 0)
                self.db.execute("UPDATE metadata SET version=2 WHERE id=1")
            self._validate_state(state)
            now = self.clock()
            self._set_lane(state, "UNKNOWN", now)
            state["grant"] = None
            state["session_id"] = str(uuid.uuid4())
            state["observation_revision"] += 1
            state["last_observation_at"] = None
            state["observation_not_before"] = now
            state["last_clock"] = max(now, state["last_clock"])
            save_state(self.db, state)
            self._audit("Restart", "", "RECONCILIATION_REQUIRED", False, state, now)
            self.db.commit()
        except Exception:
            if self.db:
                self.db.close()
            self.lease.close()
            raise

    def _validate_state(self, state):
        if (state["lane_state"] not in {"CLEAR", "RESERVED", "OCCUPIED", "UNKNOWN"}
                or not integer(state["epoch"], -1) or not integer(state["revision"])
                or not integer(state["observation_sequence"])
                or not integer(state["observation_revision"])
                or not finite(state["last_clock"])
                or state["mode"] not in {"CENTRAL", "LOCAL", "UNINITIALIZED"}
                or (state["epoch"] >= 0 and state["holder"] not in self.config.members)):
            raise RuntimeError("Invalid persisted state")

    def close(self):
        with self.lock:
            if not self.closed:
                self.db.close()
                self.lease.close()
                self.closed = True

    def _set_lane(self, state, lane, now):
        # Safety-stop time measures UNKNOWN blocking, not normal occupancy/reservation.
        if lane != state["lane_state"]:
            state["observation_revision"] += 1
        if lane == "UNKNOWN" and state["lane_state"] != "UNKNOWN":
            state["observation_not_before"] = now
        stopped = lane == "UNKNOWN"
        if stopped and state["safety_stop_since"] is None:
            state["safety_stop_since"] = now
        elif not stopped and state["safety_stop_since"] is not None:
            state["safety_stop_total_seconds"] += max(0, now - state["safety_stop_since"])
            state["safety_stop_since"] = None
        state["lane_state"] = lane

    def _expire(self, state, now):
        if now < state["last_clock"]:
            state["grant"] = None
            self._set_lane(state, "UNKNOWN", state["last_clock"])
            return "CLOCK_ROLLBACK"
        state["last_clock"] = now
        grant = state["grant"]
        reason = None
        if grant and not grant["entered"] and now >= grant["expires_at"]:
            state["grant"] = None
            self._set_lane(state, "UNKNOWN", now)
            reason = "EXPIRED_GRANT"
        seen = state["last_observation_at"]
        if (state["lane_state"] in {"CLEAR", "OCCUPIED"} and
                (seen is None or now - seen > self.config.observation_timeout_seconds)):
            self._set_lane(state, "UNKNOWN", now)
            reason = reason or "OBSERVATION_TIMEOUT"
        return reason

    def _audit(self, operation, request_id, reason, allowed, state, now, replayed=False):
        event = dict(timestamp=now, operation=operation, request_id=request_id,
                     reason=reason, allowed=allowed, replayed=replayed,
                     epoch=state["epoch"], revision=state["revision"], holder=state["holder"],
                     lane_state=state["lane_state"], session_id=state["session_id"],
                     observation_revision=state["observation_revision"],
                     grant_id=state["grant"]["id"] if state["grant"] else None)
        cursor = self.db.execute("INSERT INTO events(value) VALUES(?)", (canonical(event).decode(),))
        event["event_id"] = cursor.lastrowid
        self.db.execute("UPDATE events SET value=? WHERE event_id=?",
                        (canonical(event).decode(), cursor.lastrowid))
        return event

    def _commit(self):
        self.db.commit()

    def _fail(self):
        # Mark failure first: rollback itself raises if BEGIN never started a transaction.
        self.failed = True
        try:
            self.db.rollback()
        except sqlite3.Error:
            pass
        return self._unavailable()

    def _unavailable(self, reason="STORAGE_UNAVAILABLE"):
        return dict(allowed=False, reason=reason, replayed=False, action_applied=False,
                    request_id="", state={}, grant=None)

    def get_state(self):
        with self.lock:
            if not self.path.is_file():
                self.failed = True
            if self.failed or self.closed:
                return self._unavailable()
            try:
                self.db.execute("BEGIN IMMEDIATE")
                state = load_state(self.db)
                now = self.clock()
                reason = self._expire(state, now)
                save_state(self.db, state)
                event = None
                if reason:
                    event = self._audit("Maintenance", "", reason, False, state, now)
                self._commit()
                if event:
                    LOG.info(canonical(event).decode())
                return dict(allowed=True, reason="STATE", replayed=False,
                            action_applied=False, request_id="", state=state, grant=None)
            except (sqlite3.Error, OSError, ValueError, KeyError, TypeError):
                return self._fail()

    def execute(self, operation, payload_bytes, signature):
        if operation not in SCHEMAS:
            return self._unavailable("INVALID_REQUEST")
        try:
            if len(payload_bytes) > 65536:
                raise ValueError()
            command = json.loads(payload_bytes)
            if (type(command) is not dict or set(command) != COMMON | SCHEMAS[operation]
                    or canonical(command) != payload_bytes or command["operation"] != operation
                    or command["version"] != 1 or type(command["version"]) is not int
                    or command["lane_id"] != self.config.lane_id
                    or command["membership_id"] != self.config.membership_id
                    or not isinstance(command["actor_id"], str)
                    or not isinstance(command["request_id"], str)
                    or not 1 <= len(command["request_id"]) <= 128):
                raise ValueError()
            actor = command["actor_id"]
            if actor not in self.config.keys or not verify(self.config.keys[actor], signature, command):
                return self._unavailable("INVALID_SIGNATURE")
        except (ValueError, TypeError, OverflowError, RecursionError):
            return self._unavailable("INVALID_REQUEST")

        request_id = command["request_id"]
        fingerprint = hashlib.sha256(payload_bytes).hexdigest()
        with self.lock:
            if not self.path.is_file():
                self.failed = True
            if self.failed or self.closed:
                return self._unavailable()
            try:
                self.db.execute("BEGIN IMMEDIATE")
                state = load_state(self.db)
                now = self.clock()
                expiry_reason = self._expire(state, now)
                if expiry_reason:
                    self._audit("Maintenance", "", expiry_reason, False, state, now)
                previous = self.db.execute("SELECT fingerprint,result FROM requests WHERE request_id=?",
                                           (request_id,)).fetchone()
                if previous and previous[0] == fingerprint:
                    result = json.loads(previous[1])
                    result.update(replayed=True, action_applied=False, state=state)
                elif previous:
                    result = dict(allowed=False, reason="REQUEST_ID_CONFLICT", replayed=False,
                                  action_applied=False, request_id=request_id, state=state, grant=None)
                else:
                    try:
                        if expiry_reason == "CLOCK_ROLLBACK":
                            raise Reject("CLOCK_ROLLBACK")
                        try:
                            grant = self._apply(operation, command, state, now, expiry_reason)
                        except (TypeError, ValueError, KeyError, OverflowError):
                            raise Reject("INVALID_REQUEST") from None
                        result = dict(allowed=True, reason="OK", replayed=False, action_applied=True,
                                      request_id=request_id, state=state, grant=grant)
                    except Reject as rejection:
                        result = dict(allowed=False, reason=rejection.reason, replayed=False,
                                      action_applied=False, request_id=request_id, state=state, grant=None)
                    self.db.execute("INSERT INTO requests VALUES(?,?,?)",
                                    (request_id, fingerprint, canonical(result).decode()))
                save_state(self.db, state)
                event = self._audit(operation, request_id, result["reason"], result["allowed"],
                                    state, now, result["replayed"])
                self._commit()
            except (sqlite3.Error, OSError, ValueError, KeyError, TypeError, OverflowError):
                return self._fail()
            LOG.info(canonical(event).decode())
            return result

    def _authority_check(self, command, state, holder=None):
        if not integer(command["epoch"]) or not integer(command["revision"]):
            raise Reject("INVALID_REQUEST")
        if command["epoch"] < state["epoch"]:
            raise Reject("STALE_EPOCH")
        if command["epoch"] != state["epoch"]:
            raise Reject("UNKNOWN_EPOCH")
        if command["revision"] != state["revision"]:
            raise Reject("STALE_REVISION")
        if (holder or command["actor_id"]) != state["holder"]:
            raise Reject("WRONG_HOLDER")

    def _clear_required(self, state):
        if state["lane_state"] != "CLEAR":
            raise Reject("LANE_UNKNOWN" if state["lane_state"] == "UNKNOWN" else "LANE_BUSY")
        if state["grant"] is not None:
            raise Reject("LANE_BUSY")

    def _apply(self, operation, command, state, now, expiry_reason):
        actor = command["actor_id"]
        if operation == "InstallAuthority":
            self._install(command, state, now)
        elif operation == "TransferHolder":
            self._authority_check(command, state)
            destination = command["destination"]
            if not isinstance(destination, str) or destination not in self.config.members or destination == actor:
                raise Reject("INVALID_HOLDER")
            self._clear_required(state)
            state["holder"] = destination
            state["revision"] += 1
        elif operation == "RequestGrant":
            self._authority_check(command, state)
            self._clear_required(state)
            session = str(uuid.uuid4())
            grant = dict(id=session, holder=actor, epoch=state["epoch"], revision=state["revision"],
                         issued_at=now, expires_at=now + self.config.grant_ttl_seconds, entered=False)
            state["grant"] = grant
            state["session_id"] = session
            state["observation_not_before"] = now
            self._set_lane(state, "RESERVED", now)
            return grant.copy()
        elif operation == "ValidateEntry":
            if actor != self.config.simulator_id:
                raise Reject("UNAUTHORIZED")
            self._authority_check(command, state, command["holder"])
            if not isinstance(command["grant_id"], str) or not isinstance(command["session_id"], str):
                raise Reject("INVALID_REQUEST")
            grant = state["grant"]
            if not grant:
                raise Reject("EXPIRED_GRANT" if expiry_reason == "EXPIRED_GRANT" else "INVALID_GRANT")
            if command["grant_id"] != grant["id"] or command["session_id"] != state["session_id"]:
                raise Reject("INVALID_GRANT")
            if grant["entered"]:
                raise Reject("ALREADY_ENTERED")
            if now >= grant["expires_at"]:
                raise Reject("EXPIRED_GRANT")
            if state["lane_state"] != "RESERVED":
                raise Reject("LANE_UNKNOWN")
            grant["entered"] = True
            state["last_observation_at"] = now
            self._set_lane(state, "OCCUPIED", now)
            return grant.copy()
        elif operation in {"ReportLaneState", "ClearLane"}:
            self._observe(operation, command, state, now)

    def _install(self, command, state, now):
        cert = command["certificate"]
        if type(cert) is not dict or set(cert) != {"payload", "signatures"}:
            raise Reject("INVALID_CERTIFICATE")
        payload, signatures = cert["payload"], cert["signatures"]
        if (type(payload) is not dict or set(payload) != CERT_FIELDS
                or payload["version"] != 1 or type(payload["version"]) is not int
                or payload["lane_id"] != self.config.lane_id
                or payload["membership_id"] != self.config.membership_id
                or not integer(payload["epoch"])
                or not integer(payload["expected_epoch"], -1)
                or not integer(payload["expected_revision"])
                or not isinstance(payload["holder"], str)
                or payload["holder"] not in self.config.members
                or payload["mode"] not in {"LOCAL", "CENTRAL"}
                or type(signatures) is not list):
            raise Reject("INVALID_CERTIFICATE")
        signers = set()
        for item in signatures:
            if type(item) is not dict or set(item) != {"signer_id", "signature"}:
                raise Reject("INVALID_CERTIFICATE")
            identity = item["signer_id"]
            if not isinstance(identity, str) or identity in signers or identity not in self.config.keys:
                raise Reject("INVALID_CERTIFICATE")
            try:
                signature = base64.b64decode(item["signature"], validate=True)
            except (ValueError, TypeError):
                raise Reject("INVALID_CERTIFICATE") from None
            if not verify(self.config.keys[identity], signature, payload, CERTIFICATE_DOMAIN):
                raise Reject("INVALID_CERTIFICATE")
            signers.add(identity)
        if (command["expected_epoch"] != payload["expected_epoch"]
                or command["expected_revision"] != payload["expected_revision"]):
            raise Reject("INVALID_CERTIFICATE")
        if payload["mode"] == "LOCAL":
            if command["actor_id"] not in self.config.members or not signers <= self.config.members.keys():
                raise Reject("UNAUTHORIZED")
            if len(signers) < self.config.majority:
                raise Reject("NO_QUORUM")
            if payload["epoch"] <= state["epoch"]:
                raise Reject("STALE_EPOCH" if payload["epoch"] < state["epoch"] else "AUTHORITY_CONFLICT")
            if payload["epoch"] != state["epoch"] + 1:
                raise Reject("UNKNOWN_EPOCH")
        else:
            if command["actor_id"] != self.config.controller_id or signers != {self.config.controller_id}:
                raise Reject("UNAUTHORIZED")
            if payload["epoch"] < state["epoch"]:
                raise Reject("STALE_EPOCH")
            if state["epoch"] >= 0 and payload["epoch"] != state["epoch"]:
                raise Reject("UNKNOWN_EPOCH")
            if state["epoch"] == -1 and payload["epoch"] != 0:
                raise Reject("UNKNOWN_EPOCH")
        # Certificates are bound to the exact authority snapshot, including normal handoffs.
        if payload["expected_epoch"] != state["epoch"]:
            raise Reject("STALE_EPOCH")
        if payload["expected_revision"] != state["revision"]:
            raise Reject("STALE_REVISION")
        state.update(epoch=payload["epoch"], mode=payload["mode"], holder=payload["holder"],
                     revision=state["revision"] + 1)
        if state["lane_state"] == "RESERVED":
            self._set_lane(state, "UNKNOWN", now)
        state["grant"] = None

    def _observe(self, operation, command, state, now):
        actor = command["actor_id"]
        expected_actor = self.config.operator_id if operation == "ClearLane" else self.config.simulator_id
        if actor != expected_actor:
            raise Reject("UNAUTHORIZED")
        if command["session_id"] != state["session_id"]:
            raise Reject("STALE_SESSION")
        if not integer(command["observation_revision"]):
            raise Reject("INVALID_REQUEST")
        if command["observation_revision"] != state["observation_revision"]:
            raise Reject("STALE_OBSERVATION")
        if not integer(command["sequence"], 1):
            raise Reject("INVALID_REQUEST")
        if command["sequence"] <= state["observation_sequence"]:
            raise Reject("STALE_OBSERVATION")
        if not integer(command["observed_at_ms"]):
            raise Reject("INVALID_REQUEST")
        observed = command["observed_at_ms"] / 1000
        if (observed > now + self.config.future_skew_seconds
                or now - observed > self.config.observation_timeout_seconds
                or observed < state["observation_not_before"]
                or (state["last_observation_at"] is not None and observed < state["last_observation_at"])):
            raise Reject("STALE_OBSERVATION")
        if operation == "ClearLane":
            if not isinstance(command["reason"], str) or not 1 <= len(command["reason"]) <= 512:
                raise Reject("INVALID_REQUEST")
            lane = "CLEAR"
        else:
            lane = command["lane_state"]
            if lane not in {"CLEAR", "OCCUPIED", "UNKNOWN"}:
                raise Reject("INVALID_REQUEST")
        # A CLEAR observation while RESERVED could precede entry. It must not release the grant.
        if operation == "ReportLaneState" and state["lane_state"] == "RESERVED" and lane == "CLEAR":
            raise Reject("OUTSTANDING_GRANT")
        state["observation_sequence"] = command["sequence"]
        state["last_observation_at"] = min(observed, now)
        self._set_lane(state, lane, now)
        if lane in {"CLEAR", "UNKNOWN"}:
            state["grant"] = None
        elif state["grant"] and not state["grant"]["entered"]:
            # An unexpected physical occupancy blocks entry, rather than validating the grant.
            state["grant"] = None

    def events(self):
        with self.lock:
            return [json.loads(row[0]) for row in self.db.execute("SELECT value FROM events ORDER BY event_id")]
