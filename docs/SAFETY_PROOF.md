# Trusted Gate safety evidence

This document collects the evidence that the Trusted Access Gate never accepts
conflicting access, including during failures. Every claim names the automated tests
that demonstrate it. Run them all with:

```powershell
.\.venv\Scripts\python.exe -m pytest -q tests/test_gate_safety.py tests/test_gate_deployment.py tests/test_gate.py tests/test_scenarios.py tests/test_controller_fallback.py tests/test_agent_gate.py tests/test_failure_integration.py tests/test_partition_integration.py tests/test_restart_integration.py
```

## The safety property

1. **Exclusive physical access.** At most one crane is admitted to the lane at any time.
   A crane is admitted only when the gate returns `allowed`, `action_applied` and not
   `replayed` for its `ValidateEntry`.
2. **No stale authority.** A command built against an authority (epoch, revision) or
   physical evidence (session, phase revision, sequence) that the gate has moved past is
   never accepted. This includes resubmissions under a new request ID.
3. **Single authority.** The gate holds exactly one holder at a time. `(epoch, revision)`
   never moves backwards, and each authority change applies at most once.
4. **Digital recovery never clears physical occupancy.** Recovery, fallback, controller
   return and gate restart keep an occupied lane OCCUPIED or UNKNOWN until trusted
   evidence confirms the crane has left.

## Assumptions

The evidence holds under the model in [CONTRACT.md](CONTRACT.md) and
[GATE_REPORT.md](GATE_REPORT.md):

- one active gate per lane and fixed membership;
- secret Ed25519 signing keys, with protocol-following cranes;
- a simulator and operator that report physical reality truthfully;
- synchronised clocks, where a rollback fails closed.

Malicious cranes, replicated gates and industrial certification are out of scope.

## How the gate enforces it

| Fence | Rejects | Reason codes |
| --- | --- | --- |
| Epoch | Commands from before a recovery or fallback | `STALE_EPOCH`, `UNKNOWN_EPOCH` |
| Revision | Commands from before a handoff, install or controller return | `STALE_REVISION` |
| Snapshot binding | Certificates prepared against an older authority snapshot | `STALE_EPOCH`, `STALE_REVISION`, `AUTHORITY_CONFLICT` |
| Grant identity | Old, revoked or reused grants | `INVALID_GRANT`, `ALREADY_ENTERED`, `WRONG_HOLDER` |
| Session, phase and sequence | Old physical evidence | `STALE_SESSION`, `STALE_OBSERVATION` |
| Replay cache | Duplicate request IDs, which never re-apply | `replayed=true`, `action_applied=false` |
| Durable transactions | Lost or partial commits; a failed commit never reports success | `STORAGE_UNAVAILABLE` |

## Evidence by task

### 1. Authority changes during automatic CENTRAL-to-LOCAL fallback

| Scenario | Outcome | Test |
| --- | --- | --- |
| Controller outage with crane-1 inside | LOCAL epoch 1 installed and the lane stays OCCUPIED; the old-epoch grant request is rejected | `test_controller_fallback.py::test_controller_outage_automatically_preserves_healthy_holder_and_occupancy` |
| Suspicion without a majority | No LOCAL authority is installed | `test_controller_fallback.py::test_controller_suspicion_without_majority_cannot_install_local` |
| Gate rejects, is unavailable, loses the reply, or has moved on during fallback | One install at most; agents follow gate state | `test_controller_fallback.py::test_fallback_waits_for_gate_and_serializes_attempts` |
| CENTRAL-era grant presented after fallback, with the same holder | Rejected | `test_gate_safety.py::test_every_old_permission_is_rejected_after_authority_change[central_fallback]` |
| Controller re-sends an old CENTRAL certificate after fallback | `STALE_EPOCH` | `test_gate_safety.py::test_controller_cannot_reassert_old_authority_after_fallback` |
| Controller returns, then a delayed LOCAL proposal arrives | Return accepted with occupancy preserved; the delayed proposal gets `STALE_REVISION` | same test |

### 2. Old permissions are rejected after recovery

`test_gate_safety.py::test_every_old_permission_is_rejected_after_authority_change` runs
three times: after LOCAL recovery, after CENTRAL-to-LOCAL fallback and after a normal
handoff. Each time, the new holder is first admitted to the lane. Then the test submits
every earlier permission twice: as the original message, and under a new request ID.
The earlier permissions are:

- the authority-establishing certificate;
- a used grant request and its entry;
- an outstanding unused grant;
- a never-sent handoff;
- old CLEAR evidence with a huge sequence number, so only session and phase fencing can
  stop it.

Every first-time or new-ID submission is rejected with a fencing reason. Every replay
is marked `replayed` and applies nothing. Authority, lane, session and grant are
unchanged afterwards.

Supporting tests:

- `test_gate.py::test_first_milestone`
- `test_gate.py::test_recovery_during_reservation_invalidates_grant`
- `test_scenarios.py::test_recovery_certificate_cannot_override_a_new_handoff`
- `test_partition_integration.py::test_majority_recovers_and_live_isolated_holder_is_fenced`

### 3. Gate communication fails midway through a transfer

| Failure | Outcome | Test |
| --- | --- | --- |
| Reply lost after commit | Sender resolves the outcome from `GetState`; receiver becomes the only holder | `test_agent_gate.py::test_lost_committed_reply_resolves_from_getstate` |
| Failure before commit | Identical retry; no token is lost or duplicated | `test_agent_gate.py::test_lost_uncommitted_reply_retries_identical_command` |
| Commit happened, then the gate is unreachable | Sender stays suspended; nobody holds the token until the gate answers | `test_agent_gate.py::test_committed_but_gate_unreachable_keeps_sender_suspended` |
| Gate crashes during the commit, or after it before replying | The transfer applies exactly once (revision +1); after restart nobody gets a grant without fresh clearance | `test_gate_safety.py::test_gate_crash_midway_through_transfer_applies_it_at_most_once` |
| Recovery races an in-flight transfer at the same revision (either order, or concurrent) | Exactly one wins and the loser stays fenced | `test_gate_safety.py::test_recovery_racing_a_transfer_at_the_same_revision_has_one_winner` |

### 4. Lane still occupied when recovery occurs

| Scenario | Outcome | Test |
| --- | --- | --- |
| Recovery with a crane inside | Lane stays OCCUPIED; the new holder gets `LANE_BUSY` until a confirmed exit | `test_gate.py::test_recovery_during_occupancy_requires_exit` |
| Real agents: holder fails during occupancy | Quorum recovery, old holder fenced, safe replacement entry after the exit | `test_failure_integration.py::test_actual_holder_detection_quorum_fencing_and_safe_progress[during-occupancy]` |
| Partition during occupancy | Occupant preserved until explicit clearance | `test_partition_integration.py::test_partition_recovery_preserves_occupant_until_explicit_clearance` |
| Gate process hard-killed with a crane inside | Authority persists; lane UNKNOWN; the old grant is refused | `test_gate_deployment.py::test_tls_gate_process_persists_authority_and_fences_after_hard_kill` |

### 5. Persistence and secure communication in deployment

- `test_gate_deployment.py` starts the real `serve` process with mutual TLS. It checks that:
  - the probe identity passes the health check, while plaintext and anonymous-TLS
    clients are refused;
  - after a crane enters and the process is hard-killed, the restarted gate keeps
    epoch, revision, holder and mode, starts a new session with the lane UNKNOWN, and
    refuses the pre-crash grant;
  - the durable event log contains exactly one admission.
- `deploy/compose.tls.yaml` and `deploy/kubernetes/gate-tls-patch.yaml` configure mutual
  TLS for Compose and Kubernetes. See [DEPLOYMENT.md](DEPLOYMENT.md#mutual-tls-outside-isolated-development).
- The Compose TLS stack was validated with Docker 29.8 on 2026-11-03:
  - the container reached `healthy` through the TLS probe;
  - the demo scenario passed all 11 checks over mutual TLS;
  - a plaintext client was refused;
  - after `docker compose restart`, epoch, revision, holder and mode were unchanged,
    with a new session and no grant.
- The Kubernetes patch was checked offline with `kubectl patch --local`. It has **not**
  been run in a live cluster.

## Model check under random failures

`test_gate_safety.py::test_randomised_failures_never_admit_conflicting_access` drives
the gate with 400 random steps for each of 5 seeds (1, 7, 20, 3104, 9001). It runs
against an independent model of which crane is physically inside. Each step is one
of the following:

- a grant request or entry attempt;
- a truthful position report, or a physical exit;
- an operator clearance, only when the lane is physically empty;
- a clock jump past the observation or grant timeouts;
- a CENTRAL controller update (fallback from CENTRAL counts as a mode change);
- a LOCAL recovery certificate, sometimes signed only by a minority;
- a handoff;
- a gate crash and restart;
- a disk failure during commit, followed by a restart and a later retry;
- a replay of any earlier command, either as sent or under a new request ID.

After every step it asserts:

- no second crane is admitted while one is inside;
- an admitted grant matches the current holder, epoch and revision;
- any command that is stale for the current state is rejected;
- replays never apply;
- a physically occupied lane is never CLEAR or RESERVED;
- `(epoch, revision)` never decreases;
- any current grant belongs to the current holder and authority.

The test also fails if a run does not exercise every hazard. Across the five seeds:

| Admissions | Stale commands rejected | CENTRAL→LOCAL fallbacks | Controller returns | Gate crashes | Storage failures |
| ---: | ---: | ---: | ---: | ---: | ---: |
| 31 | 151 | 55 | 50 | 92 | 52 |

## Do the tests catch real bugs?

To check that the tests are strong enough, each gate safety check was deliberately
removed from `cranelock_gate/core.py`, one at a time, and `tests/test_gate_safety.py`
was re-run:

| Check removed | Result |
| --- | --- |
| Epoch fencing | Caught |
| Revision fencing | Caught |
| Revoke grants when installing authority | Caught |
| Certificate snapshot binding (expected revision) | Caught |
| Session ID check alone | Not caught: phase-revision fencing still rejects the same evidence |
| `ALREADY_ENTERED` alone | Not caught: the RESERVED-lane check still rejects reuse |
| Session check and phase-revision check together | Caught |
| `ALREADY_ENTERED` and the RESERVED check together | Caught |

The two survivors are redundant second layers, not gaps. Each is caught when its
partner check is removed with it.

## Limits

This is test-based evidence, not a formal proof. Checking is bounded to the scenarios and
seeds above. The randomised model trusts the simulator to report reality, which is the
contract's assumption. Agent-side behaviour (FIFO queues, vote persistence) is covered
by the agent suites, not here. Live Kubernetes behaviour, and latency or throughput
under failures, remain for the integrated evaluation.
