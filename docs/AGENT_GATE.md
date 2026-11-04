# Crane agent / Trusted Access Gate integration

Milestone validation: 22 October 2026, Windows, CPython 3.14. Existing baseline:
64 tests passed after installing the repository's current dependencies. Final result:
**82 passed in 21.34 seconds** (64 existing plus 18 new integration scenarios).
`pip check` and `git diff --check` passed. No formatter,
linter or type checker is configured in `pyproject.toml`. The gate implementation,
gate tests, gate contract and platform manifests are unchanged by this milestone.

## Repository inspection and interface decisions

The pulled repository contains `cranelock/` (async agent), `cranelock_gate/` (signed
gate with SQLite persistence), two separate proto trees, tests for agents and gate,
`docs/`, project PDF/DOCX references, Dockerfiles, Compose and Kubernetes templates.
Packaging/dependencies are now shared through `pyproject.toml`; `requirements.txt`
installs `.[dev]`. Reuse Max's client/config/crypto, gate server and generated
bindings; reuse the shared generation script. No replacement gate API is needed.

| Existing mismatch | Integration choice |
| --- | --- |
| Agent only had epoch | Add `revision` using the gate's existing name |
| Agent default epoch 1; gate bootstrap epoch 0 | Explicit `--epoch 0` in this demo; do not change old deployment defaults |
| Agent/Compose IDs `crane-a/b/c`; gate fixture `crane-1/2/3` | Use gate fixture IDs locally; preserve Arif's files |
| Gate requires >=3 configured members | Configure all three, run two processes; crane-3 remains offline |
| Normal handoff increments revision, not epoch | Send both values with the authoritative holder |
| Gate requires signed commands and fresh CLEAR/no grant | Reuse `Client.send`; clearance stays outside the agent |
| Gate uses synchronous gRPC | Async adapter delegates bounded I/O with `asyncio.to_thread` |

Gate integration is opt-in with `--gate-address`, `--gate-config`, `--gate-keys`.
Without these, the original standalone baseline remains available for existing
tests/platform demos. **Do not mix standalone and gate-enabled agents in one
cluster.** Gate mode never falls back to standalone on an outage. Programmatic
callers supply `Agent(..., gate=GateClient(...), epoch=0)`.

### Controller heartbeat and return from LOCAL

`CentralController.start_heartbeat_server()` exposes a gRPC heartbeat endpoint
for configured agents. A successful heartbeat first reconciles the gate to
CENTRAL using the gate's latest epoch, revision and holder; stale compare-and-swap
attempts are retried from a fresh gate snapshot. The endpoint reports unavailable
if the gate cannot safely establish CENTRAL authority. Agents can connect with
`--controller-address HOST:PORT` (together with the gate options) or pass a
`ControllerHeartbeatClient.probe` callback to `Agent`.

On controller return, each gate-enabled agent reads the gate again and adopts
only that current state. It does not trust heartbeat fields as authority, and it
waits for an in-progress local transfer to finish before reconciling. A transfer
that races controller return is resolved by the gate's epoch/revision check; it
cannot overwrite the restored holder. The controller endpoint itself must be
started alongside the controller's existing gate client, for example:

```python
controller = CentralController(config, controller_key, gate_channel)
port = await controller.start_heartbeat_server("127.0.0.1:50052")
```

Stop the service with `await controller.stop_heartbeat_server()`. The heartbeat
service uses the same transport security configuration as its hosting
application; do not expose an insecure listener outside a trusted network.
Use `start_heartbeat_server(..., credentials=server_credentials)` for a TLS
listener. Agent CLI clients can use `--controller-tls-ca`,
`--controller-tls-cert` and `--controller-tls-key`; non-loopback controller
addresses require these mutual-TLS options.

## Authority and concurrency

New state field `revision` is persisted with epoch/holder/token and the existing
opaque vote state. Old JSON files without revision load as revision zero. Agent
protobuf messages add fields without renumbering existing fields: heartbeat,
request, reply and state expose revision; transfer adds revision and holder.
The gate schema is untouched. Update all agent bindings together: an old sender
defaults revision to zero and will be rejected by a gate-enabled holder.

One `asyncio.Lock` protects mutable state. A separate operation lock serializes gate
handoffs, receiver verification and reconciliation. The state lock is released for
all network I/O. A pending gate command blocks additional handoffs and token
requests; reads and heartbeats continue. Only protocol handlers mutate state on the
event loop; the adapter's worker performs the existing synchronous client calls.

Normal A -> B sequence:

1. B refreshes its holder view using `GetState`, then requests the token from A.
2. A queues B, preserving FIFO and suppressing duplicate agent IDs.
3. A reads `GetState`, checks it is still holder, and suspends local ownership.
4. A calls existing signed `TransferHolder(epoch=E, revision=R, destination=B)`.
5. The gate checks its own authorization/occupancy rules and commits `(E,R+1,B)`.
6. A persists that authoritative tuple and sends it to B with the remaining FIFO.
7. B queries `GetState` itself; only an exact epoch/revision/holder match can grant
   ownership through this notification. Duplicate delivery does not reset its queue.

Rejection does not grant B ownership. A adopts current gate state and may resume as
holder only if the gate still names A. The queued request remains for a later
attempt when appropriate; each explicit denial requires a new request ID because
gate denials are durably cached. Thus a lane-UNKNOWN denial can later succeed after
the external operator confirms clearance. This is bounded by the configured agent
interval; it does not bypass the gate's checks.

For uncertain RPC outcomes, query `GetState`:

- Exact `(E,R+1,B)`: treat authority transfer as committed and notify B.
- Exact original `(E,R,A)`: keep A suspended and retry the **identical payload and
  request ID**. A read alone cannot exclude a delayed command still reaching the gate.
- Later/different authority: reconcile it; never act on historical `allowed=true`.
- Gate unavailable, uninitialized, incompatible or older than local state: retain
  fail-closed REJOINING. Never invent authority or silently roll state back.

Retries use Max's documented durable idempotency cache. Replay acknowledgements
contain *current* state even when their allowed flag describes historical success.
Peer notification failures also retain the transfer ID and retry in memory.

Epoch/revision comparisons are lexicographic. A higher epoch adopts its gate
revision; equal epoch requires nondecreasing revision. At equal versions, the gate
corrects a conflicting local holder. No quorum epoch generation is implemented.

All gate-enabled starts enter REJOINING with ownership disabled, including fresh
starts. Successful `GetState` reconciliation activates the gate's mode and derives
ownership from its holder, never from the persisted boolean. Outages retry while
REJOINING. Higher-version peer heartbeats prompt a gate refresh; heartbeat claims
never assign authority. Stale known-peer heartbeats still count as liveness even
though their authority data is rejected.

New JSON events: `GATE_STATE_QUERIED`, `GATE_STATE_RECONCILED`,
`GATE_TRANSFER_REQUESTED`, `GATE_TRANSFER_ACCEPTED`, `GATE_TRANSFER_REJECTED`,
`GATE_TRANSFER_UNCERTAIN`, `GATE_TRANSFER_RESOLVED`, `AUTHORITY_UPDATED`.
All agent events include Unix/monotonic timestamps, ID, epoch, revision and holder;
transfer events include request ID and next holder as `peer_id`.

## Exact local demonstration (PowerShell, repository root)

Install and regenerate using the existing shared build approach:

```powershell
.\.venv\Scripts\python.exe -m pip install -r requirements.txt
.\.venv\Scripts\python.exe scripts/generate_proto.py
```

Use a fresh directory such as `run/gate-demo1` for a **new experiment**. Existing
setup refuses to overwrite identities/database. Keep the same files for restarts.

Prepare the gate using Max's existing development setup:

```powershell
.\.venv\Scripts\python.exe -m cranelock_gate.demo setup --directory run/gate-demo1
```

Terminal 1 — start the unchanged gate on a distinct port:

```powershell
.\.venv\Scripts\python.exe -m cranelock_gate.server serve --config run/gate-demo1/config.json --database run/gate-demo1/gate.sqlite --bind 127.0.0.1:50060 --metrics-port 9090
```

Terminal 4 — bootstrap initial authority once:

```powershell
.\.venv\Scripts\python.exe -m cranelock.gate_demo bootstrap --directory run/gate-demo1 --target 127.0.0.1:50060
```

This helper calls Max's existing development certificate/bootstrap fixture. It
does not add voting, certificate construction logic, signing logic or recovery to
the agent. It only accepts an uninitialized gate and establishes initial epoch 0,
revision 1, holder crane-1. Combined private keys are development fixtures only.

Terminal 2 — start A (crane-1):

```powershell
.\.venv\Scripts\python.exe -m cranelock run --id crane-1 --listen 127.0.0.1:50051 --peer crane-2=127.0.0.1:50052 --peer crane-3=127.0.0.1:50053 --holder crane-1 --epoch 0 --state-file run/gate-demo1/a.json --gate-address 127.0.0.1:50060 --gate-config run/gate-demo1/config.json --gate-keys run/gate-demo1/private-keys.json
```

Terminal 3 — start B (crane-2), requesting after 15 seconds:

```powershell
.\.venv\Scripts\python.exe -m cranelock run --id crane-2 --listen 127.0.0.1:50052 --peer crane-1=127.0.0.1:50051 --peer crane-3=127.0.0.1:50053 --holder crane-1 --epoch 0 --state-file run/gate-demo1/b.json --gate-address 127.0.0.1:50060 --gate-config run/gate-demo1/config.json --gate-keys run/gate-demo1/private-keys.json --request-after 15
```

Terminal 4 — inspect before the delay expires:

```powershell
.\.venv\Scripts\python.exe -m cranelock.gate_demo state --directory run/gate-demo1 --target 127.0.0.1:50060
.\.venv\Scripts\python.exe -m cranelock state 127.0.0.1:50051
.\.venv\Scripts\python.exe -m cranelock state 127.0.0.1:50052
```

Initially gate/A name crane-1 at epoch 0/revision 1; A owns the token, B does not.
After B's request, A queues B and the gate initially rejects with `LANE_UNKNOWN`.
This demonstrates that token handoff cannot bypass gate occupancy policy.

**After the request is queued**, independently confirm the development lane is
empty (this demo has no physical simulator), then explicitly authorize the existing
operator clearance command:

```powershell
.\.venv\Scripts\python.exe -m cranelock.gate_demo clear --directory run/gate-demo1 --target 127.0.0.1:50060 --confirm-empty
```

A's next attempt should succeed within approximately 0.5 seconds. Repeat the three
state commands: gate and both agents report epoch 0/revision 2/holder crane-2;
A reports `has_token=false`, B true. No grant or physical entry is requested.
The offline crane-3 will be suspected; it is not required for normal handoff.

### Restart B with the same state

Press **Ctrl+C in Terminal 3**, then restart using the same B command **without**
the delayed request:

```powershell
.\.venv\Scripts\python.exe -m cranelock run --id crane-2 --listen 127.0.0.1:50052 --peer crane-1=127.0.0.1:50051 --peer crane-3=127.0.0.1:50053 --holder crane-1 --epoch 0 --state-file run/gate-demo1/b.json --gate-address 127.0.0.1:50060 --gate-config run/gate-demo1/config.json --gate-keys run/gate-demo1/private-keys.json
```

Logs show `AGENT_STARTED` in REJOINING, `GATE_STATE_QUERIED`, then
`GATE_STATE_RECONCILED`/`MODE_CHANGED`. Only after the query does B regain ownership
because the gate still names B. Tests also cover a stale persisted holder losing
ownership to a newer authoritative holder. Do not bootstrap or delete state during
restart. If gate state is older than an existing agent file, use the correct
experiment's files or investigate; the agent intentionally refuses rollback.

### Network deployment

The agent accepts `--gate-tls-ca`, `--gate-tls-cert`, `--gate-tls-key` together and
requires mutual TLS for non-loopback gate targets. Reuse Max's certificate/config
instructions. The adapter retains only its own configured signing key. The combined
fixture key file is suitable only for this local demo; deployment must supply each
agent with a file containing only its own key. Peer agent gRPC remains the original
trusted-network plaintext protocol; authenticating peers is a separate milestone.

## Validation and remaining limits

```powershell
.\.venv\Scripts\python.exe -m pytest -q
git diff --check
git diff --stat
git status --short
```

Tests exercise the actual unchanged gate using signed gRPC, injected reply loss,
unavailable state reads, stale epochs/revisions, restart with matching/stale
ownership, equal-version disagreements, concurrent handoffs, state-lock
responsiveness, FIFO across three agents and historical replay responses. A separate
test starts a gate and two independent CLI agent processes, performs handoff and
restarts B with its persisted state. All pre-existing tests remain unchanged.

Remaining limits:

- Queue contents, notification deduplication and pending command payloads are still
  in memory. Restart reconciles authority safely but does not restore scheduling
  history. Recovery of a lost queue is not provided by the gate.
- Local `has_token` is a cached authority view, not permission to enter the lane.
  Later authority changes can stale that view. Any future lane entry must still
  use Max's gate grant/entry APIs; no gate bypass is added here.
- Read/commit races are resolved through the gate's compare-and-set revision and
  durable retry semantics, not distributed locking or a new consensus mechanism.
- Callback/disk failures and multiple processes sharing one agent state file remain
  outside the existing agent's durability assumptions. Use distinct persistent files.
- The demo's fresh clearance is explicit operator input, not automated physical
  evidence. Do not use it in place of Darius's eventual simulator.
- Docker/Kubernetes and large-scale performance were not exercised here. Existing
  manifests still launch standalone agents. Upgrading them needs membership/key/
  gate-address agreement with Arif and Max; no files were silently renamed.

Team confirmations:

- **Moe:** opaque persistent vote format and authoritative higher-epoch integration;
  preserve this gate reconciliation boundary without embedding quorum logic here.
- **Max:** approve revision-bearing notifications, replay interpretation, lane
  clearance prerequisite and gate availability behavior. RequestGrant remains deferred.
- **Darius:** correlate agent request/transfer IDs with gate events; distinguish
  cached ownership, grant authority and physical entry; agree clock alignment.
- **Kirby:** queue reconciliation and policy for CENTRAL participation; no controller
  commands, timeout transitions or controller-return algorithm were added.
- **Arif:** standardize `crane-a/b/c` versus `crane-1/2/3`, fixed membership, key
  delivery, port 50060 for local gate, TLS and persistent state paths before deployment.

Next milestone recommendation: agree the simulator-facing grant/entry boundary and
durable queue/retry requirements with Max and Darius, then integrate those interfaces.
Do not add quorum recovery or controller logic without a separate review.
