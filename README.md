# CraneLock

This repository contains the normal-operation crane agent (`cranelock`) and the
trusted access gate (`cranelock_gate`). Normal gate-authorized token handoff and
gate-based restart reconciliation are now available in opt-in gate mode.
See [agent/gate integration and exact local demo](docs/AGENT_GATE.md) for setup,
authority rules, failure behavior, validation and team integration decisions.
The separate protobuf services and original standalone baseline are preserved.

Install both components and test dependencies with `python -m pip install -e ".[dev]"`
(or `python -m pip install -r requirements.txt`), then run `python -m pytest -q`.
Run `python scripts/generate_proto.py` to regenerate bindings for both protocols.

Member 4's first evaluation milestone is available with
`python -m cranelock_sim --cranes 3 --duration 60 --seed 20`.
It runs seeded normal traffic through the signed gate core and exports physical
traces, request CSV and summary metrics. See [evaluation instructions and scope](docs/EVALUATION.md).
Its logical FIFO scheduler does not yet measure the distributed CraneLock protocol.
Run `python -m cranelock_sim --mode agents` for the three-agent normal-handoff
integration: real loopback gRPC token requests/transfers, gate entry checks and
physical lane movements, with two rounds of A/B/C crossings.
Run `python -m cranelock_sim --mode recovery` for holder failure before entry and
during occupancy. It prints live `[PASS]` checks and measures actual heartbeat
detection, quorum recovery, old-permission fencing and safe replacement entry.
Run `python -m cranelock_sim --mode partition` to keep all agents alive while
blocking peer RPCs across a majority/minority split or three isolated agents.
The live checks verify majority recovery, stale-holder fencing, occupancy blocking
and refusal to install replacement authority without a quorum.
Run `python -m cranelock_sim --mode restart` to restart the failed holder from
its old state after recovery, verify REJOINING blocks participation, then reconcile
from accepted gate authority and complete a new token-authorized crossing.
Run `python -m cranelock_sim --mode compare` for paired normal/holder-failure
comparisons of a central reference scheduler, basic-token agents and CraneLock.
Install `.[evaluation]` for PNG/SVG graphs. See [baseline comparison scope and metrics](docs/EVALUATION.md#run-the-comparisons).
View saved experiments with `python -m cranelock_sim.dashboard`, then open
`http://127.0.0.1:8765`. The local dashboard includes model charts, run filters,
recorded lane playback and evidence downloads. See [dashboard instructions](docs/EVALUATION.md#open-the-dashboard).

Gate safety evidence (exclusive access under failures) is in
[docs/SAFETY_PROOF.md](docs/SAFETY_PROOF.md); mutual-TLS deployment is in
[docs/DEPLOYMENT.md](docs/DEPLOYMENT.md#mutual-tls-outside-isolated-development).

Both demos default to port 50051. Run them separately or configure distinct ports
when running them together.

## CraneLock — normal crane-agent milestone

The crane agent provides normal-operation token coordination and its shared contract.

Python 3.12+ is intended; validation here used CPython 3.14 on Windows. No existing
formatter, linter, or type checker was configured. Dependencies use pip and a venv.

### Setup (PowerShell, repository root)

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe scripts/generate_proto.py
.\.venv\Scripts\python.exe -m pytest -q
```

On this workstation the default MSYS Python has no pip. To create the environment:

```powershell
& "$env:LOCALAPPDATA\Python\pythoncore-3.14-64\python.exe" -m venv .venv
```

Generated modules are checked in; regenerate them after changing the proto. Do not
edit generated code. The schema uses package `cranelock.v1` and stable numbered
fields; add future recovery/gate/controller services without replacing this API.

### Standalone baseline two-process demo (without the gate)

Use a **fresh demo directory** for each new simulated cluster. All agents must agree
on initial epoch, holder and complete membership. Only one configured initial
holder is allowed. These commands use epoch 1 and LOCAL mode; CENTRAL is also
represented, but controller decisions and automatic mode transitions are deferred.

Terminal 1:

```powershell
.\.venv\Scripts\python.exe -m cranelock run --id crane-a --listen 127.0.0.1:50051 --peer crane-b=127.0.0.1:50052 --holder crane-a --state-file run/demo1/a.json
```

Terminal 2 (B requests after 15 seconds):

```powershell
.\.venv\Scripts\python.exe -m cranelock run --id crane-b --listen 127.0.0.1:50052 --peer crane-a=127.0.0.1:50051 --holder crane-a --state-file run/demo1/b.json --request-after 15
```

Terminal 3, before and after the 15-second delay:

```powershell
.\.venv\Scripts\python.exe -m cranelock state 127.0.0.1:50051
.\.venv\Scripts\python.exe -m cranelock state 127.0.0.1:50052
```

Initially A reports `has_token: true`. B sends TokenRequest, A queues B, and the
automatic transfer worker hands off the token. Afterwards A reports false and B
true. Logs contain request/transfer IDs and holder-change events.

Press **Ctrl+C in Terminal 2** to stop B. After roughly 2–2.5 seconds since its last
received heartbeat, A emits `PEER_SUSPECTED` and `TOKEN_HOLDER_SUSPECTED`. State shows
`SUSPECTED_UNAVAILABLE`; A never regenerates the token. Change timing with
`--interval 0.5 --timeout 2`. Repeat `--peer ID=HOST:PORT` for arbitrary membership.

In standalone mode, restarting with an existing state file intentionally enters REJOINING with
`has_token=false`. There is no CLI bypass. `Agent.reconcile()` is a trusted Python
integration hook whose caller must supply authoritative epoch/holder information.
Do not delete state files to rejoin an existing cluster. For a separate fresh demo,
stop both agents and use a new path such as `run/demo2/a.json` and `run/demo2/b.json`.

### Files and behavior

| File | Purpose |
| --- | --- |
| `cranelock/agent.py` | State model, async lock, FIFO, heartbeats, suspicion, transfer and persistence |
| `cranelock/__main__.py` | Agent process, delayed token request and state query CLI |
| `cranelock/__init__.py` | Package marker |
| `proto/cranelock/protocol.proto` | Shared Heartbeat, TokenRequest, TokenTransfer, GetAgentState contract |
| `cranelock/protocol_pb2.py` | Generated protobuf messages |
| `cranelock/protocol_pb2_grpc.py` | Generated gRPC bindings |
| `scripts/generate_proto.py` | Reproducible protobuf generation |
| `tests/test_agent.py` | Ownership, FIFO, concurrent requests, timing, epochs, restart and retry tests |
| `tests/test_process_demo.py` | Two real CLI processes, handoff, peer termination and suspicion |
| `requirements.txt` | Pinned direct runtime/build/test dependencies |
| `.gitignore` | Exclude local environment, caches and demo state |
| `README.md` | Modified setup, architecture, demo and integration notes |

One asyncio lock protects mutable coordination state. RPC network waits happen
outside it. The queue deduplicates by requesting agent and moves with the token.
The sender persists relinquishment before sending; retry uses the same transfer
ID. The receiver handles duplicate deliveries. An uncertain transfer can leave
zero usable owners; safety takes priority over availability until recovery.

Epochs must match for requests/transfers. Higher heartbeat epochs do not
automatically grant authority; older heartbeats are rejected. Heartbeats from the
known holder can announce an outgoing handoff, but never set local `has_token`.
Only receipt of a valid TokenTransfer grants local ownership.

Persistence uses flushed JSON plus atomic file replacement for epoch/holder/token
and reserved opaque vote storage. Restart always blocks ownership. Queue, transfer
retry and deduplication history are in-memory for this milestone. Recovery votes are
durable and exposed through `VoteRecovery`; a member will reproduce an identical vote
but will not sign a conflicting proposal for the same target epoch. Use one process
and one state file per agent.

Events are JSON lines with Unix and monotonic timestamps, agent/peer IDs, epoch,
and operation IDs where relevant. `Agent(event_sink=...)` is the recovery/evaluation
hook; callbacks run synchronously and must be fast and nonthrowing. Failure events
include last-seen time and observed detection latency. Monotonic clocks measure
durations on one host; do not subtract monotonic timestamps from different hosts.

### Limits and team integration decisions

- **Moe (Recovery):** confirm authoritative reconciliation API, persistent vote
  format, higher-epoch handling, and consumption of TOKEN_HOLDER_SUSPECTED. No
  replacement token, certificate, majority vote or signing is implemented.
- **Max (Gate):** normal handoff and restart integration are documented in
  [the gate integration guide](docs/AGENT_GATE.md). Grant/entry integration remains deferred.
- **Darius (Evaluation):** confirm JSON event fields, operation IDs, clock alignment
  and detection-latency definition. Current heartbeat fanout is O(N²) per round.
- **Kirby (Controller):** confirm CENTRAL command semantics and who authorizes
  LOCAL transitions and rejoining. Automatic controller-failure/return logic is deferred.
- **Arif (Platform):** confirm ports, peer configuration, persistent storage path,
  shutdown behavior and eventual authentication. This is plaintext trusted-peer
  local demo gRPC; IDs are validated against configured membership, not authenticated.

Normal holder announcements can be missed across multiple rapid handoffs or
partitions. A stale holder view then safely rejects transfers; robust discovery
and reconciliation need a later agreed protocol. Persistence is conservative,
not a durable distributed transfer transaction. No claim is made that this alone
solves partitions, crash recovery or gate safety. Initial bootstrap configuration
must be consistent; it is not a consensus algorithm.

Tests cover the eleven requested behaviors across scenario tests, plus uncertain
transfer retry, duplicate reception and restart blocking.

The next milestone after the standalone baseline is now implemented in opt-in gate
mode; see [the integration guide](docs/AGENT_GATE.md) for its remaining boundaries.
Do not add quorum or gate algorithms inside this agent.

## CraneLock: Member 3 trusted access gate

Standalone Python service for a **single simulated lane**, with signed gRPC requests,
strict-majority recovery certificates, fencing, durable SQLite state and fail-closed
occupancy handling. The agent package now produces quorum certificates; controller and
yard-simulator integration are still represented by development clients.

### Run on Windows / PowerShell

From this directory:

```powershell
python -m venv .venv
.\.venv\Scripts\python.exe -m pip install -e ".[dev]"
.\.venv\Scripts\python.exe -m pytest -q
.\.venv\Scripts\python.exe -m cranelock_gate.demo setup --directory demo-data
.\.venv\Scripts\python.exe -m cranelock_gate.demo run --directory demo-data
```

Skip `setup` when `demo-data` already exists. It refuses to overwrite identities
or a database. The demo can be repeated with the same directory: each run starts a new
authority epoch and demonstrates unknown-lane blocking, valid admission, stale-holder
rejection, recovery during occupancy and explicit clearance.

On Linux/macOS, replace `.\.venv\Scripts\python.exe` with `.venv/bin/python`.

### Run as a persistent service

```powershell
.\.venv\Scripts\python.exe -m cranelock_gate.server serve --config demo-data/config.json --database demo-data/gate.sqlite
```

In another terminal:

```powershell
.\.venv\Scripts\python.exe -m cranelock_gate.demo run --directory demo-data --target 127.0.0.1:50051
```

The gRPC port is `50051`; Prometheus metrics are at `http://127.0.0.1:9090/metrics`.
The server prints a listening message. Press Ctrl+C to stop it; Windows Ctrl+Break is
also supported. Shutdown prints `Stopping gate...` and `Gate stopped.`.
The development demo uses plaintext loopback. Network listeners require mutual TLS
unless explicitly enabled for an isolated simulation.

### Team handoff

For the evaluation role, [benefits and costs](docs/EVALUATION.md#read-the-measurements) compares actual
reference-controller outage, lost-token recovery and healthy-operation overhead.
Run `python -m cranelock_sim --mode benefits --output run/evaluation/benefits-completion-validated`,
then `python -m cranelock_sim.dashboard`. The dashboard explains paired gains and
costs; the teammate controller remains an outstanding integration boundary.

- [Contract and signing format](docs/CONTRACT.md): Member 1's proposed shared proto,
  Member 2's certificate format, and Member 4/5 integration rules.
- [Deployment and failure testing](docs/DEPLOYMENT.md): Docker, Kubernetes, persistence,
  health checks, TLS and Member 6 handoff.
- [Report draft](docs/GATE_REPORT.md): gate Design and Implementation sections,
  with tested outcomes separated from outstanding integration claims.
- [Protobuf service](proto/cranelock_gate/v1/gate.proto): seven RPC methods.

Bindings are checked in. After changing the proto, regenerate them:

```powershell
.\.venv\Scripts\python.exe scripts/generate_proto.py
```

### Safety boundary

Run exactly **one active gate with one persistent database**. A quorum certificate
changes digital authority; it never proves that the physical lane is empty. Only the
trusted simulator may accept entry, and only fresh simulator evidence or a trusted
operator may establish clearance. Every physical entry must pass this gate. Clients
must stop on transport failure, denial, or replay acknowledgements.

This is a coursework simulator, not an industrial crane safety controller. End-to-end
CraneLock safety and performance remain unverified until the real member components
are integrated. The Docker image and restart behaviour were validated locally;
Kubernetes execution remains unverified. See [validation results](docs/VALIDATION.md).
