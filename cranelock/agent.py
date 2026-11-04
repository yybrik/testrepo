# async token and recovery protocol
import asyncio
from collections import deque
from dataclasses import dataclass, field
from enum import IntEnum
import json
import os
from pathlib import Path
import time
import uuid

import grpc
from . import protocol_pb2 as pb
from . import protocol_pb2_grpc as rpc
from . import recovery


class Mode(IntEnum):
    CENTRAL = pb.CENTRAL
    LOCAL = pb.LOCAL
    REJOINING = pb.REJOINING


@dataclass
class AgentState:
    agent_id: str
    current_epoch: int
    token_holder: str
    mode: Mode = Mode.LOCAL
    has_token: bool = False
    request_queue: deque = field(default_factory=deque)
    revision: int = 0


class Agent(rpc.CraneAgentServicer):
    def __init__(self, agent_id, peers, holder, *, epoch=1, mode=Mode.LOCAL,
                 interval=0.5, timeout=2.0, state_file=None, clock=time.monotonic,
                 event_sink=None, gate=None, revision=0,
                 recovery_config=None, recovery_private_key=None, controller_probe=None):
        if not agent_id or epoch < 0 or revision < 0 or interval <= 0 or timeout <= interval:
            raise ValueError("Require ID, nonnegative epoch/revision and 0 < interval < timeout")
        if agent_id in peers or not holder or holder not in {agent_id, *peers}:
            raise ValueError("Holder must be a member; peers must exclude self")
        self.peers = dict(peers)
        self.interval, self.timeout, self.clock = interval, timeout, clock
        self.path = Path(state_file) if state_file else None
        self.lock = asyncio.Lock()
        self.authority_lock = asyncio.Lock()
        self.recovery_lock = asyncio.Lock()
        self.gate = gate
        self.gate_attempt = None
        self.authority_refresh_needed = False
        self.events = event_sink or (lambda e: print(json.dumps(e), flush=True))
        self.state = AgentState(agent_id, epoch, holder, mode, holder == agent_id)
        self.state.revision = revision
        if gate:
            self.state.mode = Mode.REJOINING
            self.state.has_token = False
        self.pending = None
        self.last_handoff = ""
        self.received = set()
        self.observed_handoffs = set()
        self.vote_state = {}  # Opaque reserved storage; Recovery owns vote policy.
        self.recovery_config = recovery_config
        self.recovery_private_key = recovery_private_key
        if (recovery_config is None) != (recovery_private_key is None):
            raise ValueError("Recovery config and private key must be supplied together")
        if recovery_config is not None:
            if self.path is None:
                raise ValueError("Recovery voting requires a durable state file")
            if set(recovery_config.members) != {agent_id, *peers}:
                raise ValueError("Recovery membership must exactly match agent peers")
            recovery.validate_identity(recovery_config, agent_id, recovery_private_key)
        if self.path and self.path.exists():
            saved = json.loads(self.path.read_text())
            if saved["agent_id"] != agent_id:
                raise ValueError("State file belongs to another agent")
            self.state.current_epoch = max(epoch, saved["epoch"])
            if saved["epoch"] > epoch:
                self.state.revision = saved.get("revision", 0)
            elif saved["epoch"] == epoch:
                self.state.revision = max(saved.get("revision", 0), revision)
            self.state.token_holder = saved["holder"]
            self.state.mode = Mode.REJOINING
            self.state.has_token = False
            self.vote_state = saved.get("vote_state", {})
        self.last_seen = {peer: clock() for peer in peers}
        self.suspected = set()
        self.holder_suspicion = None
        self.channels = {p: grpc.aio.insecure_channel(a) for p, a in peers.items()}
        self.stubs = {p: rpc.CraneAgentStub(c) for p, c in self.channels.items()}
        self.tasks = []
        self.server = None
        self.controller_probe = controller_probe
        self.controller_suspected = False
        self.last_controller_seen = clock()
        self.persist()

    def emit(self, event, peer_id=None, **fields):
        self.events(dict(event=event, timestamp=time.time(),
                         monotonic=self.clock(), agent_id=self.state.agent_id,
                         peer_id=peer_id, epoch=self.state.current_epoch,
                         revision=self.state.revision, holder=self.state.token_holder) | fields)

    def persist(self):
        if self.path:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            temporary = self.path.with_suffix(".tmp")
            with temporary.open("w") as stream:
                json.dump(dict(agent_id=self.state.agent_id, epoch=self.state.current_epoch,
                               holder=self.state.token_holder, has_token=self.state.has_token,
                               revision=self.state.revision,
                               vote_state=self.vote_state), stream)
                stream.flush()
                os.fsync(stream.fileno())
            os.replace(temporary, self.path)

    def reply(self, accepted, reason=""):
        return pb.Reply(accepted=accepted, reason=reason, epoch=self.state.current_epoch,
                        revision=self.state.revision)

    def valid(self, epoch):
        return self.state.mode != Mode.REJOINING and epoch == self.state.current_epoch

    async def reconcile(self, epoch, holder, mode=Mode.LOCAL, revision=0):
        """Trusted integration hook ONLY: caller must establish authoritative state.

        Never call based on a heartbeat or a single unverified peer response.
        """
        if self.gate:
            raise ValueError("Gate-enabled agents must use reconcile_from_gate")
        async with self.lock:
            if ((epoch, revision) < (self.state.current_epoch, self.state.revision)
                    or holder not in {self.state.agent_id, *self.peers}):
                raise ValueError("Stale epoch or unknown holder")
            if mode == Mode.REJOINING:
                raise ValueError("Reconciliation requires an active mode")
            self.state.current_epoch = epoch
            self.state.revision = revision
            self.state.token_holder = holder
            self.state.has_token = holder == self.state.agent_id
            self.state.mode = mode
            self.state.request_queue.clear()
            self.pending = None
            self.last_handoff = ""
            self.received.clear()
            self.observed_handoffs.clear()
            self.persist()
            self.emit("MODE_CHANGED", mode=mode.name)

    async def Heartbeat(self, request, context):
        async with self.lock:
            if self.gate and request.agent_id in self.peers:
                # Liveness is independent of authority freshness. A healthy
                # observer may still have an old revision after a handoff.
                self.last_seen[request.agent_id] = self.clock()
                self.suspected.discard(request.agent_id)
                if (request.epoch, request.revision) > (self.state.current_epoch, self.state.revision):
                    self.authority_refresh_needed = True
            if (request.agent_id not in self.peers or
                    (request.epoch, request.revision) < (self.state.current_epoch, self.state.revision)):
                if self.gate and request.agent_id in self.peers:
                    self.emit("HEARTBEAT_RECEIVED", request.agent_id, result="STALE_AUTHORITY")
                return self.reply(False, "Unknown peer or stale epoch")
            self.last_seen[request.agent_id] = self.clock()
            self.suspected.discard(request.agent_id)
            self.emit("HEARTBEAT_RECEIVED", request.agent_id)
            handoff_key = (request.epoch, request.agent_id, request.outgoing_transfer_id)
            # A known holder may announce its normal handoff. This never grants
            # local ownership; only TokenTransfer can do that.
            if (not self.gate and self.valid(request.epoch) and not self.state.has_token
                    and request.agent_id == self.state.token_holder
                    and not request.has_token and request.mode != pb.REJOINING
                    and request.outgoing_transfer_id
                    and handoff_key not in self.observed_handoffs
                    and request.token_holder in {self.state.agent_id, *self.peers}
                    and request.token_holder != self.state.agent_id
                    and request.token_holder != request.agent_id):
                self.state.token_holder = request.token_holder
                self.observed_handoffs.add(handoff_key)
                self.persist()
                self.emit("TOKEN_HOLDER_CHANGED")
            return self.reply(True)

    async def TokenRequest(self, request, context):
        async with self.lock:
            if (not self.valid(request.epoch) or request.revision != self.state.revision
                    or not self.state.has_token or self.pending or self.gate_attempt):
                return self.reply(False, "Not active holder at this epoch")
            if request.agent_id not in self.peers:
                return self.reply(False, "Unknown requester")
            self.emit("TOKEN_REQUEST_RECEIVED", request.agent_id, request_id=request.request_id)
            if request.agent_id not in self.state.request_queue:
                self.state.request_queue.append(request.agent_id)
                self.emit("TOKEN_QUEUED", request.agent_id, request_id=request.request_id)
            return self.reply(True)

    async def TokenTransfer(self, request, context):
        if self.gate:
            return await self.receive_gate_transfer(request)
        async with self.lock:
            if (not self.valid(request.epoch) or request.revision != self.state.revision
                    or request.sender_id not in self.peers
                    or request.receiver_id != self.state.agent_id or not request.transfer_id):
                return self.reply(False, "Invalid transfer or epoch")
            key = (request.epoch, request.sender_id, request.transfer_id)
            if key in self.received:
                return self.reply(True, "Already received")
            if (self.state.has_token or self.pending
                    or self.state.token_holder != request.sender_id):
                return self.reply(False, "Sender is not known holder")
            queue = list(request.request_queue)
            if (len(queue) != len(set(queue)) or any(
                    p not in self.peers or p == self.state.agent_id for p in queue)):
                return self.reply(False, "Invalid queue")
            self.state.has_token = True
            self.last_handoff = ""
            self.state.token_holder = self.state.agent_id
            self.state.request_queue = deque(queue)
            self.received.add(key)
            self.observed_handoffs.add(key)
            self.persist()
            self.emit("TOKEN_TRANSFER_RECEIVED", request.sender_id, transfer_id=request.transfer_id)
            self.emit("TOKEN_HOLDER_CHANGED")
            return self.reply(True)

    # builds a recovery vote response
    def recovery_reply(self, accepted, reason="", signature=b""):
        return pb.RecoveryVote(accepted=accepted, reason=reason,
            epoch=max(0, self.state.current_epoch), voter_id=self.state.agent_id,
            signature=signature)

    # validates and records a recovery vote
    def _vote_recovery(self, raw, proposer_id, proposer_signature):
        if self.recovery_config is None:
            raise recovery.RecoveryError("RECOVERY_DISABLED")
        payload = recovery.parse_proposal(raw, self.recovery_config)
        if not recovery.verify_member(self.recovery_config, proposer_id,
                                      proposer_signature, payload):
            raise recovery.RecoveryError("INVALID_SIGNATURE")
        if self.state.mode == Mode.REJOINING:
            raise recovery.RecoveryError("REJOINING")
        if payload["expected_epoch"] != self.state.current_epoch:
            raise recovery.RecoveryError("STALE_EPOCH")
        if self.state.mode == Mode.CENTRAL:
            if self.controller_probe is None or not self.controller_suspected:
                raise recovery.RecoveryError("CONTROLLER_NOT_SUSPECTED")
        elif self.state.token_holder not in self.suspected:
            raise recovery.RecoveryError("HOLDER_NOT_SUSPECTED")
        recovery.record_vote(self.vote_state, payload)
        self.persist()
        return payload, recovery.sign(self.recovery_private_key, payload)

    # handles a peer recovery proposal
    async def VoteRecovery(self, request, context):
        async with self.lock:
            try:
                payload, signature = self._vote_recovery(
                    request.proposal_json, request.proposer_id, request.proposer_signature)
            except recovery.RecoveryError as error:
                return self.recovery_reply(False, str(error))
            self.emit("RECOVERY_VOTE_CAST", request.proposer_id,
                      target_epoch=payload["epoch"], holder=payload["holder"])
            return self.recovery_reply(True, signature=signature)

    # gathers a strict-majority certificate
    async def propose_recovery(self, gate_state, holder=None):
        if self.recovery_config is None:
            raise recovery.RecoveryError("RECOVERY_DISABLED")
        holder = holder or self.state.agent_id
        payload = recovery.proposal(self.recovery_config, gate_state, holder)
        raw = recovery.canonical(payload)
        proposer_signature = recovery.sign(self.recovery_private_key, payload)
        request = pb.RecoveryProposal(proposer_id=self.state.agent_id,
            proposal_json=raw, proposer_signature=proposer_signature)
        async with self.lock:
            try:
                _, own_signature = self._vote_recovery(
                    raw, self.state.agent_id, proposer_signature)
            except recovery.RecoveryError as error:
                self.emit("RECOVERY_REJECTED", reason=str(error), target_epoch=payload["epoch"])
                return None
        signatures = {self.state.agent_id: own_signature}

        async def ask(peer):
            try:
                vote = await self.stubs[peer].VoteRecovery(request, timeout=self.timeout)
            except grpc.aio.AioRpcError:
                return
            if (vote.accepted and vote.voter_id == peer
                    and recovery.verify_member(self.recovery_config, peer,
                                               vote.signature, payload)):
                signatures[peer] = vote.signature

        await asyncio.gather(*(ask(peer) for peer in self.peers))
        if len(signatures) < self.recovery_config.majority:
            self.emit("RECOVERY_NO_QUORUM", target_epoch=payload["epoch"],
                      votes=len(signatures), required=self.recovery_config.majority)
            return None
        self.emit("RECOVERY_CERTIFICATE_FORMED", target_epoch=payload["epoch"],
                  votes=len(signatures), holder=holder)
        return recovery.certificate(payload, signatures)

    async def GetAgentState(self, request, context):
        async with self.lock:
            s = self.state
            return pb.AgentState(agent_id=s.agent_id, epoch=s.current_epoch, revision=s.revision,
                has_token=s.has_token, token_holder=s.token_holder, mode=s.mode,
                request_queue=list(s.request_queue),
                pending_transfer_id=self.pending.transfer_id if self.pending else "",
                peers=[pb.PeerState(agent_id=p, last_seen_monotonic=t,
                    status="SUSPECTED_UNAVAILABLE" if p in self.suspected else "AVAILABLE")
                    for p, t in self.last_seen.items()])

    async def request_token(self):
        if self.gate and not await self.reconcile_from_gate():
            return self.reply(False, "Gate state unavailable or inconsistent")
        async with self.lock:
            if self.state.mode == Mode.REJOINING or self.state.has_token:
                return self.reply(False, "Rejoining or already holder")
            holder = self.state.token_holder
            message = pb.TokenRequestMessage(agent_id=self.state.agent_id,
                epoch=self.state.current_epoch, revision=self.state.revision, request_id=uuid.uuid4().hex)
            self.emit("TOKEN_REQUEST_SENT", holder, request_id=message.request_id)
        return await self.stubs[holder].TokenRequest(message, timeout=self.timeout)

    async def transfer_next(self):
        if self.gate:
            async with self.authority_lock:
                return await self.transfer_with_gate()
        async with self.lock:
            if self.state.mode == Mode.REJOINING:
                return False
            if not self.pending:
                if not self.state.has_token or not self.state.request_queue:
                    return False
                receiver = self.state.request_queue.popleft()
                self.pending = pb.TokenTransferMessage(sender_id=self.state.agent_id,
                    receiver_id=receiver, epoch=self.state.current_epoch, revision=self.state.revision,
                    transfer_id=uuid.uuid4().hex, request_queue=list(self.state.request_queue))
                # Persist relinquishment BEFORE network I/O. Never roll back on timeout.
                self.state.has_token = False
                self.last_handoff = self.pending.transfer_id
                self.state.token_holder = receiver
                self.state.request_queue.clear()
                self.persist()
                self.emit("TOKEN_TRANSFER_STARTED", receiver, transfer_id=self.pending.transfer_id)
                self.emit("TOKEN_HOLDER_CHANGED")
            message = self.pending
        try:
            result = await self.stubs[message.receiver_id].TokenTransfer(message, timeout=self.timeout)
        except grpc.aio.AioRpcError:
            return False
        async with self.lock:
            if result.accepted and self.pending is message:
                self.pending = None
            return result.accepted

    def suspend(self, reason):
        """Called under state lock. Persist uncertainty, never cached authority."""
        self.state.has_token = False
        self.state.mode = Mode.REJOINING
        self.persist()
        self.emit("MODE_CHANGED", mode="REJOINING", reason=reason)

    async def query_gate(self):
        try:
            authority = await self.gate.state()
        except (grpc.RpcError, RuntimeError) as error:
            async with self.lock:
                self.suspend("Gate GetState unavailable")
                self.emit("GATE_STATE_QUERIED", result="UNAVAILABLE", reason=str(error))
            return None
        self.emit("GATE_STATE_QUERIED", result="OK", authoritative_state={
            k: authority.get(k) for k in ("epoch", "revision", "holder")})
        return authority

    def adopt_gate_state(self, authority, *, allow_owner=True):
        """Apply only gate responses, under lock and authority_lock.

        Epoch/revision are an ordered pair. Gate revisions currently increase
        globally too; a new epoch's revision is still taken from the gate.
        """
        epoch, revision, holder = (authority.get(k) for k in ("epoch", "revision", "holder"))
        if (type(epoch) is not int or type(revision) is not int or epoch < 0 or revision < 0
                or holder not in {self.state.agent_id, *self.peers}
                or authority.get("mode") not in {"CENTRAL", "LOCAL"}):
            self.suspend("Gate authority uninitialized or incompatible with membership")
            self.emit("GATE_STATE_RECONCILED", result="INVALID_AUTHORITY")
            return False
        if (epoch, revision) < (self.state.current_epoch, self.state.revision):
            self.suspend("Gate authority older than local state")
            self.emit("GATE_STATE_RECONCILED", result="INCONSISTENT_OLDER_GATE",
                      authoritative_epoch=epoch, authoritative_revision=revision,
                      authoritative_holder=holder)
            return False
        old = (self.state.current_epoch, self.state.revision, self.state.token_holder)
        self.state.current_epoch, self.state.revision = epoch, revision
        self.state.token_holder = holder
        self.state.has_token = bool(allow_owner and not self.gate_attempt and holder == self.state.agent_id)
        previous_mode = self.state.mode
        self.state.mode = Mode[authority["mode"]]
        self.persist()
        self.emit("GATE_STATE_RECONCILED", result="OK")
        if old != (epoch, revision, holder):
            self.emit("AUTHORITY_UPDATED")
            self.emit("TOKEN_HOLDER_CHANGED")
        if previous_mode != self.state.mode:
            self.emit("MODE_CHANGED", mode=self.state.mode.name)
        return True

    async def reconcile_from_gate(self):
        async with self.authority_lock:
            authority = await self.query_gate()
            if authority is None:
                return False
            async with self.lock:
                old_epoch = self.state.current_epoch
                if not self.adopt_gate_state(authority):
                    return False
                if (old_epoch != self.state.current_epoch
                        or self.state.token_holder != self.state.agent_id):
                    self.state.request_queue.clear()
                return True

    async def reconcile_controller_return(self):
        """Reconcile a fresh gate snapshot after controller heartbeat, outside transfers."""
        async with self.authority_lock:
            async with self.lock:
                if self.pending or self.gate_attempt:
                    self.emit("CONTROLLER_RECONCILIATION_DEFERRED",
                              reason="LOCAL_TRANSFER_IN_PROGRESS")
                    return False
            authority = await self.query_gate()
            if authority is None:
                return False
            async with self.lock:
                if self.pending or self.gate_attempt:
                    self.emit("CONTROLLER_RECONCILIATION_DEFERRED",
                              reason="LOCAL_TRANSFER_IN_PROGRESS")
                    return False
                old = (self.state.current_epoch, self.state.revision,
                       self.state.token_holder)
                if not self.adopt_gate_state(authority):
                    return False
                if old != (self.state.current_epoch, self.state.revision,
                           self.state.token_holder):
                    self.state.request_queue.clear()
                return True

    async def receive_gate_transfer(self, request):
        async with self.authority_lock:
            async with self.lock:
                queue = list(request.request_queue)
                if (self.state.mode == Mode.REJOINING or request.sender_id not in self.peers
                        or request.receiver_id != self.state.agent_id
                        or request.holder != self.state.agent_id or not request.transfer_id
                        or (request.epoch, request.revision) <
                           (self.state.current_epoch, self.state.revision)
                        or len(queue) != len(set(queue)) or any(p not in self.peers for p in queue)):
                    return self.reply(False, "Invalid transfer, epoch or revision")
            authority = await self.query_gate()
            if authority is None:
                return self.reply(False, "Cannot verify gate authority")
            async with self.lock:
                matches = ((authority.get("epoch"), authority.get("revision"), authority.get("holder"))
                           == (request.epoch, request.revision, request.holder))
                if not self.adopt_gate_state(authority, allow_owner=matches):
                    return self.reply(False, "Inconsistent gate authority")
                if not matches:
                    return self.reply(False, "Transfer does not match gate authority")
                key = (request.epoch, request.revision, request.sender_id, request.transfer_id)
                if key not in self.received:
                    self.state.request_queue = deque(queue)
                    self.received.add(key)
                    self.emit("TOKEN_TRANSFER_RECEIVED", request.sender_id,
                              transfer_id=request.transfer_id)
                self.persist()
                return self.reply(True)

    async def transfer_with_gate(self):
        """Serialised by authority_lock; state lock is released for all I/O."""
        async with self.lock:
            if not self.gate_attempt and not self.pending and (
                    self.state.mode == Mode.REJOINING or not self.state.has_token
                    or not self.state.request_queue):
                return False
        authority = await self.query_gate()
        if authority is None:
            return False
        async with self.lock:
            if not self.adopt_gate_state(authority):
                return False
            if self.pending:
                if ((self.pending.epoch, self.pending.revision, self.pending.holder) !=
                        (self.state.current_epoch, self.state.revision, self.state.token_holder)):
                    self.pending = None
                    return False
            elif not self.gate_attempt:
                if not self.state.has_token or not self.state.request_queue:
                    self.state.request_queue.clear()
                    return False
                destination = self.state.request_queue[0]
                transfer_id = uuid.uuid4().hex
                self.gate_attempt = dict(
                    command=self.gate.transfer_command(self.state.current_epoch,
                        self.state.revision, destination, transfer_id),
                    queue=list(self.state.request_queue)[1:])
                self.state.has_token = False  # Suspend BEFORE potentially committing I/O.
                self.persist()
                self.emit("TOKEN_TRANSFER_STARTED", destination, transfer_id=transfer_id)
            attempt = self.gate_attempt

        if attempt:
            payload = attempt["command"]
            target = (payload["epoch"], payload["revision"] + 1, payload["destination"])
            base = (payload["epoch"], payload["revision"], self.state.agent_id)
            observed = tuple(authority[k] for k in ("epoch", "revision", "holder"))
            if observed == base:
                self.emit("GATE_TRANSFER_REQUESTED", payload["destination"],
                          request_id=payload["request_id"])
                try:
                    result = await self.gate.transfer(payload)
                except (grpc.RpcError, RuntimeError) as error:
                    self.emit("GATE_TRANSFER_UNCERTAIN", payload["destination"],
                              request_id=payload["request_id"], reason=str(error))
                    authority = await self.query_gate()
                    if authority is None:
                        return False
                    observed = tuple(authority.get(k) for k in ("epoch", "revision", "holder"))
                    self.emit("GATE_TRANSFER_RESOLVED", payload["destination"],
                              result="COMMITTED" if observed == target else "RETRY_OR_SUPERSEDED",
                              authoritative_state=dict(zip(("epoch", "revision", "holder"), observed)),
                              request_id=payload["request_id"])
                    if observed == base:
                        # Keep ownership suspended and the identical request ID.
                        # Next iteration retries; GetState alone cannot prove no
                        # delayed request is still in flight at the gate.
                        return False
                else:
                    self.emit("GATE_TRANSFER_ACCEPTED" if result["allowed"] else "GATE_TRANSFER_REJECTED",
                              payload["destination"], request_id=payload["request_id"],
                              reason=result["reason"], replayed=result["replayed"],
                              authoritative_state={k: result.get("state", {}).get(k)
                                  for k in ("epoch", "revision", "holder")})
                    authority = result.get("state") or await self.query_gate()
                    if authority is None:
                        return False
                    if not result["allowed"]:
                        async with self.lock:
                            self.gate_attempt = None
                            self.adopt_gate_state(authority)
                        return False
                    observed = tuple(authority.get(k) for k in ("epoch", "revision", "holder"))
            else:
                self.emit("GATE_TRANSFER_RESOLVED", payload["destination"],
                          result="COMMITTED" if observed == target else "SUPERSEDED",
                          authoritative_state=dict(zip(("epoch", "revision", "holder"), observed)),
                          request_id=payload["request_id"])
            async with self.lock:
                if not self.adopt_gate_state(authority):
                    return False
                self.gate_attempt = None
                self.state.request_queue.clear()
                if observed != target:
                    # A replay's allowed=true is historical. Never notify based
                    # on historical success when current authority moved again.
                    self.persist()
                    return False
                self.pending = pb.TokenTransferMessage(sender_id=self.state.agent_id,
                    receiver_id=payload["destination"], epoch=target[0], revision=target[1],
                    holder=target[2], transfer_id=payload["request_id"], request_queue=attempt["queue"])
                self.last_handoff = payload["request_id"]
                self.persist()
        message = self.pending
        if message is None:
            return False
        try:
            result = await self.stubs[message.receiver_id].TokenTransfer(message, timeout=self.timeout)
        except grpc.aio.AioRpcError:
            return False
        async with self.lock:
            if result.accepted and self.pending is message:
                self.pending = None
            return result.accepted

    async def rejoin_loop(self):
        while True:
            async with self.lock:
                refresh = self.state.mode == Mode.REJOINING or self.authority_refresh_needed
                self.authority_refresh_needed = False
            if refresh:
                await self.reconcile_from_gate()
            await asyncio.sleep(self.interval)

    async def detect(self):
        async with self.lock:
            for peer, seen in self.last_seen.items():
                elapsed = self.clock() - seen
                if elapsed > self.timeout and peer not in self.suspected:
                    self.suspected.add(peer)
                    self.emit("PEER_SUSPECTED", peer, last_seen_monotonic=seen,
                              detection_latency_seconds=elapsed, timeout_seconds=self.timeout)
                if peer in self.suspected and peer == self.state.token_holder:
                    key = (self.state.current_epoch, self.state.revision, peer)
                    if self.holder_suspicion != key:
                        self.holder_suspicion = key
                        self.emit("TOKEN_HOLDER_SUSPECTED", peer,
                                  detection_latency_seconds=elapsed)
            if self.state.token_holder not in self.suspected:
                self.holder_suspicion = None

    async def heartbeat_peer(self, peer, message):
        try:
            await self.stubs[peer].Heartbeat(message, timeout=self.interval)
            self.emit("HEARTBEAT_SENT", peer)
        except grpc.aio.AioRpcError:
            pass

    async def heartbeat_loop(self):
        while True:
            async with self.lock:
                message = pb.HeartbeatMessage(agent_id=self.state.agent_id,
                    epoch=self.state.current_epoch, revision=self.state.revision,
                    token_holder=self.state.token_holder,
                    mode=self.state.mode, has_token=self.state.has_token,
                    outgoing_transfer_id=self.last_handoff,
                    sent_unix_seconds=time.time())
            await asyncio.gather(*(self.heartbeat_peer(p, message) for p in self.peers))
            await asyncio.sleep(self.interval)

    async def monitor_loop(self):
        while True:
            await self.detect()
            await asyncio.sleep(self.interval)

    def automatic_recovery_needed(self):
        """Called under the state lock; uncertainty in a handoff takes priority."""
        return not (self.gate_attempt or self.pending) and (
            (self.state.mode == Mode.LOCAL and self.state.token_holder in self.suspected)
            or (self.state.mode == Mode.CENTRAL and self.controller_probe is not None
                and self.controller_suspected))

    async def recover_suspected_holder(self):
        """Recover a LOCAL holder or fall back after CENTRAL controller failure.

        All automatic proposers choose the same fixed successor for a snapshot.
        Do not filter candidates by local suspicion: partitions could then split
        durable votes between conflicting proposals for the same target epoch.
        Controller fallback retains the gate's holder; a simultaneous holder
        failure is handled by the existing LOCAL recovery in the following epoch.
        """
        if not self.gate or self.recovery_config is None or self.recovery_lock.locked():
            return
        async with self.recovery_lock, self.authority_lock:
            async with self.lock:
                if not self.automatic_recovery_needed():
                    return
            authority = await self.query_gate()
            if authority is None:
                return
            async with self.lock:
                old_epoch = self.state.current_epoch
                if not self.adopt_gate_state(authority):
                    return
                if old_epoch != self.state.current_epoch:
                    self.state.request_queue.clear()
                if not self.automatic_recovery_needed():
                    return
                controller_fallback = self.state.mode == Mode.CENTRAL
                if controller_fallback:
                    holder = self.state.token_holder
                else:
                    candidates = sorted(set(self.recovery_config.members) - {self.state.token_holder})
                    if not candidates:
                        return
                    holder = candidates[0]
            certificate = await self.propose_recovery(authority, holder)
            if certificate is None:
                return
            if controller_fallback:
                async with self.lock:
                    # A successful probe during vote collection cancels this
                    # local attempt; already persisted votes remain immutable.
                    if not self.controller_suspected:
                        return
            try:
                result = await self.gate.install_recovery(certificate)
                self.emit("RECOVERY_AUTHORITY_INSTALL_RESULT", accepted=result["allowed"],
                          reason=result["reason"])
            except (grpc.RpcError, RuntimeError) as error:
                # A timed-out install may have committed. Read current authority;
                # never infer ownership from the certificate or historical reply.
                self.emit("RECOVERY_AUTHORITY_INSTALL_UNCERTAIN", reason=str(error))
            authority = await self.query_gate()
            if authority is not None:
                async with self.lock:
                    old_epoch = self.state.current_epoch
                    if self.adopt_gate_state(authority) and old_epoch != self.state.current_epoch:
                        self.state.request_queue.clear()
                        self.pending = None
                        self.last_handoff = ""
                        self.received.clear()
                        self.observed_handoffs.clear()

    async def recovery_loop(self):
        while True:
            await self.recover_suspected_holder()
            await asyncio.sleep(self.interval)

    async def transfer_loop(self):
        while True:
            await self.transfer_next()
            await asyncio.sleep(self.interval)

    async def controller_watch_loop(self):
        """Optional integration hook: probe an actual controller, never infer clearance."""
        if self.controller_probe is None:
            return
        while True:
            try:
                async with asyncio.timeout(self.timeout):
                    available = await self.controller_probe()
            except Exception as error:
                # Timeouts and probe errors are failed observations, not worker
                # termination. Cancellation still propagates during shutdown.
                available = False
                self.emit("CONTROLLER_PROBE_FAILED", reason=type(error).__name__, detail=str(error))
            if available and self.gate and self.state.mode == Mode.LOCAL:
                available = await self.reconcile_controller_return()
            async with self.lock:
                if available:
                    self.last_controller_seen = self.clock()
                    if self.controller_suspected:
                        self.emit("CONTROLLER_RESTORED")
                    self.controller_suspected = False
                    self.emit("CONTROLLER_HEARTBEAT_RECEIVED")
                elif self.clock() - self.last_controller_seen > self.timeout and not self.controller_suspected:
                    self.controller_suspected = True
                    self.emit("CONTROLLER_SUSPECTED",
                        detection_latency_seconds=self.clock() - self.last_controller_seen)
            await asyncio.sleep(self.interval)

    async def start(self, address, auto_transfer=True, *, interceptors=()):
        self.server = grpc.aio.server(interceptors=interceptors)
        rpc.add_CraneAgentServicer_to_server(self, self.server)
        port = self.server.add_insecure_port(address)
        if not port:
            raise RuntimeError("Cannot bind agent address")
        await self.server.start()
        self.emit("AGENT_STARTED", mode=self.state.mode.name)
        if self.gate:
            await self.reconcile_from_gate()
        self.tasks = [asyncio.create_task(self.heartbeat_loop()),
                      asyncio.create_task(self.monitor_loop())]
        if auto_transfer:
            self.tasks.append(asyncio.create_task(self.transfer_loop()))
        if self.gate:
            self.tasks.append(asyncio.create_task(self.rejoin_loop()))
        if self.controller_probe:
            self.tasks.append(asyncio.create_task(self.controller_watch_loop()))
        if self.gate and self.recovery_config is not None:
            self.tasks.append(asyncio.create_task(self.recovery_loop()))
        return port

    async def close(self):
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)
        if self.server:
            await self.server.stop(0)
        await asyncio.gather(*(c.close() for c in self.channels.values()))
        if self.gate:
            await self.gate.close()
