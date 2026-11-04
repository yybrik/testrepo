# Testing and results (Darius)

This guide covers running the simulator, reading the dashboard and interpreting
the comparisons. Commands below run from the repository root in PowerShell.

## Open the dashboard

```powershell
.\.venv\Scripts\python.exe -m cranelock_sim.dashboard
```

Open **http://127.0.0.1:8765**. The viewer reads `run/evaluation`; it does not start
experiments or control cranes. Use **Refresh results** after generating results
and **Ctrl + F5** after changing the interface. Stop the server with Ctrl+C.

The recommended set is `benefits-completion-validated`. Select a scenario in the
main dropdown. Request count and sequence filters are inside **Test settings and
saved experiments**. A sequence (seed) repeats the same requests; it is not a crane.

- The summary explains why each system continues or stalls.
- Bars compare the median across selected repeated sequences. Missing recovery
  time is never shown as zero seconds.
- The progress chart shows cumulative completed requests for **one selected
  sequence**, using recorded exit times. A flat line means no new completions.
  Its failure marker spans the recorded fault times across systems.
- Playback follows one saved run. **Next event** steps through it; guided play
  holds each important event for four seconds. The clock shows recorded time.
  **Jump to controller stopping** visibly marks the controller stopped. CraneLock
  can subsequently enter LOCAL mode while that controller remains stopped.
- Saved evidence exposes request CSVs, summaries and recorded events. Advanced
  settings also expose older or failed experiments; these are not fresh results.

The dashboard uses local HTML, CSS and JavaScript, system fonts and no npm build
or external assets. Its server is read-only and bound to loopback. Artifact
downloads are allowlisted and bounded to the configured results directory.

## Run the comparisons

Install optional plotting dependencies if needed:

```powershell
.\.venv\Scripts\python.exe -m pip install -e ".[dev,evaluation]"
.\.venv\Scripts\python.exe -m cranelock_sim --mode benefits --duration 5
```

This creates a new timestamped results folder. By default it tests three systems,
three scenarios, seeds 7/20/3104 and request counts 6/18: 54 individual runs.
For a smaller test:

```powershell
.\.venv\Scripts\python.exe -m cranelock_sim --mode benefits --seed 20 --loads 6 --scenario controller-outage --duration 5 --no-plots
```

`--output` chooses a destination; it must be a **new** directory. `--no-plots`
keeps CSV/JSON output without requiring matplotlib. Legacy `--mode compare`
retains the earlier fixed-window healthy/holder-failure comparison.

| System | Healthy operation | Controller stops | Token holder stops |
| --- | --- | --- | --- |
| Central lock | A reference coordinator assigns access | Cannot assign new access | Can reassign while its coordinator is available |
| Basic token | Cranes pass one token | Continues locally; no controller recovery needed | Stalls because it cannot replace the missing token |
| CraneLock | Starts centrally in the benefits experiment | Detects missing heartbeats, obtains majority votes and installs LOCAL authority | Can replace the token with majority-approved recovery |

The controller-outage test stops an actual experimental reference controller
service during steady demand. All cranes and the gate remain running. The holder
failure test instead stops A before entry and exercises LOCAL recovery. Its
central baseline is the earlier direct-sign reference adapter. These are separate
faults; success under controller loss does not imply lost-token recovery.

## Read the measurements

**Healthy batches** submit all requests at time zero and run until every request
finishes. Total completion time excludes setup and teardown. Average waiting
includes every request's request-to-entry delay. Lower values are better. A
separate watchdog fails a stalled batch; it never reports partial work as a fast
successful completion. These runs use `benefits-v2` and
`measurement_policy=complete_all_requests`.

**Failure tests** keep requests arriving over a fixed observation window (five
seconds in the saved evaluation, three by CLI default). This lets a stalled system
finish its test. Completed requests must both enter and exit within that window.
Controller-outage entry counts include only new entries after the fault. Time to
resume spans fault injection to the next new entry, including detection,
permission agreement and waiting for demand or clearance. A system that never
resumes has no successful recovery time. Basic token needs no controller-failure
recovery because it is already local.

Older fixed-window healthy results can exclude a request that exits just after
the cutoff. Such a late completion is not a failed request. Compare models within
the same request sequence, count, workload hash and measurement policy. Do not
compare batch waiting directly with steady-stream waiting or mix old and new
healthy measurements. RPC counts, where recorded, count instrumented attempts;
they are not network packet or byte totals. Holder-failure RPC cost is unmeasured.

The recommended saved collection contains 18 new healthy batch runs and 36 failure
runs copied unchanged from `benefits-validated`. Its root summary records that
provenance. The older validated set remains as historical evidence.

## Other safety tests

```powershell
.\.venv\Scripts\python.exe -m cranelock_sim --mode agents
.\.venv\Scripts\python.exe -m cranelock_sim --mode recovery
.\.venv\Scripts\python.exe -m cranelock_sim --mode partition
.\.venv\Scripts\python.exe -m cranelock_sim --mode restart
.\.venv\Scripts\python.exe -m cranelock_sim --mode logical --seed 20
```

- Agents: real local gRPC token requests and handoffs through A, B and C.
- Recovery: holder failure before entry or during occupancy; majority replacement
  must reject old permissions and wait for confirmed physical clearance.
- Partition: A is disconnected from B/C, or all peers are disconnected. Two peers
  can approve recovery; without a majority recovery must be refused. Disconnection
  alone does not revoke existing authority or prove an occupied lane is empty.
- Restart: A reloads stale state in REJOINING, stays blocked until reconciliation
  against accepted gate authority, then needs a fresh token before entering.
- Logical: seeded traffic through the actual gate with simulated time and FIFO
  scheduling. It does not measure distributed protocol performance.

## Evidence and code layout

Each run keeps its own summary, requests, events, gate decisions and state files
under `run/evaluation`. Separate run directories prevent different seeds, systems
or faults from overwriting each other's evidence. Root CSVs and plots aggregate
these runs. State databases and private keys must not be published.

| Location | Purpose |
| --- | --- |
| `cranelock_sim/__main__.py` | Command entry point for all simulator modes |
| `experiment.py`, `agents.py`, `failures.py` | Workload, live-agent and failure scenarios |
| `comparison.py`, `benefits.py` | Paired baseline and benefits experiments |
| `lane.py`, `network.py`, `controller.py` | Shared physical lane, network faults and reference controller |
| `dashboard.py` | Local results server |
| `web/index.html`, `web/style.css`, `web/app.js` | Page structure, appearance and interaction |
| `tests/test_*.py` | Checks grouped by the behavior they test |

These experiments use three agents on one host, a common trusted gate and fixed
membership. The teammate controller is not integrated. Physical occupancy is
modeled independently of digital authority: changing holder or epoch never clears
the lane. Zero detected safety violations is a bounded observation, not a proof
of safety for every failure or a production port performance benchmark.

## Verify changes

```powershell
.\.venv\Scripts\python.exe -m pytest -q
```

Optional real-browser checks use `.[ui-test]` and an installed Edge or Chrome;
they skip if the browser is unavailable. Dashboard checks cover filters, safe
artifact access, missing measurements, recorded progress, event ordering, guided
playback, controller stopping and mobile layout.

If Windows locks a previous pytest temporary directory, choose a fresh
`--basetemp` outside the repository. Generated test folders, caches, screenshots
and development runs are disposable; validated experiment evidence is retained.
