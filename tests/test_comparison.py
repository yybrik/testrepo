import asyncio
import csv
import json

import pytest

from cranelock_gate.storage import ProcessLease
from cranelock_sim.comparison import make_workload, plot_results, run_case, run_comparison, workload_hash


@pytest.fixture(scope="module")
def comparison(tmp_path_factory):
    output = tmp_path_factory.mktemp("comparison") / "suite"
    # Functional progress needs time for durable votes and gate writes on a busy
    # Windows host. This is not a latency target or a performance assertion.
    result = asyncio.run(run_comparison(output, seeds=(20,), requests=4, duration=3,
        crossing_time=0.01, detection_timeout=0.2, plots=False))
    return output, result


def test_all_models_use_identical_requests_fault_and_fixed_window(comparison):
    output, result = comparison
    assert result["passed"] and result["runs"] == 6
    traces = [json.loads(path.read_text()) for path in output.glob("seed-20/*/*/workload.json")]
    assert len(traces) == 6 and all(trace == traces[0] for trace in traces)
    assert {row["workload_sha256"] for row in result["results"]} == {workload_hash(traces[0])}
    assert all(row["duration_seconds"] == 3 for row in result["results"])
    for row in result["results"]:
        path = output / "seed-20" / row["scenario"] / row["model"]
        events = [json.loads(line) for line in (path / "events.jsonl").read_text().splitlines()]
        assert sum(event["event"] == "holder_stopped" for event in events) == int(row["scenario"] == "holder-failure")
        assert sum(event["event"] == "request" for event in events) == 4
        assert row["completed"] + row["unfinished_at_horizon"] == 4
        assert row["throughput_per_minute"] == pytest.approx(row["completed"] * 60 / 3)
        assert row["safety_violations"] == 0
        assert not (path / "failure.json").exists()
        lease = ProcessLease(path / "gate.sqlite")
        lease.close()


def test_normal_models_progress_and_basic_token_stalls_after_holder_failure(comparison):
    output, result = comparison
    for row in result["results"]:
        if row["scenario"] == "holder-failure" and row["model"] == "basic-token":
            assert row["completed"] == 0 and row["unfinished_at_horizon"] == 4
            assert row["mean_wait_seconds"] is None and row["p95_wait_seconds"] is None
            assert row["recovery_latency_seconds"] is None and not row["resumed_within_window"]
        else:
            assert row["completed"] > 0
            assert row["mean_wait_seconds"] >= 0
            if row["scenario"] == "holder-failure":
                assert row["recovery_latency_seconds"] >= 0 and row["resumed_within_window"]
    cert = output / "seed-20/holder-failure/cranelock/recovery-certificate.json"
    assert {vote["signer_id"] for vote in json.loads(cert.read_text())["signatures"]} == {"crane-2", "crane-3"}
    assert not (output / "seed-20/holder-failure/basic-token/recovery-certificate.json").exists()


def test_gate_acceptance_matches_physical_entries_and_no_overlap(comparison):
    output, result = comparison
    for row in result["results"]:
        path = output / "seed-20" / row["scenario"] / row["model"]
        trace = [json.loads(line) for line in (path / "events.jsonl").read_text().splitlines()]
        audit = [json.loads(line) for line in (path / "gate-events.jsonl").read_text().splitlines()]
        entries = [event for event in trace if event["event"] == "physical_entry"]
        accepted = [event for event in audit if event["operation"] == "ValidateEntry"
                    and event["reason"] == "OK" and not event["replayed"]]
        assert len(entries) == len(accepted)
        occupant = None
        for event in trace:
            if event["event"] == "physical_entry":
                assert occupant is None
                occupant = event["crane"]
            elif event["event"] == "physical_exit":
                assert occupant == event["crane"]
                occupant = None
        assert occupant is None
    with (output / "comparison.csv").open(newline="") as stream:
        assert len(list(csv.DictReader(stream))) == 6


def test_seed_reproducibility_and_distinct_workloads():
    assert make_workload(20, 6, 2) == make_workload(20, 6, 2)
    assert make_workload(20, 6, 2) != make_workload(7, 6, 2)


def test_crossing_drained_after_horizon_does_not_inflate_completions(tmp_path):
    result = asyncio.run(run_case(tmp_path / "late", "central-lock", "normal",
        [dict(request_id="r1", crane="crane-2", requested_at=0)], seed=20,
        duration=0.1, crossing_time=0.2, detection_timeout=0.25))
    assert result["completed"] == 0 and result["unfinished_at_horizon"] == 1
    assert result["throughput_per_minute"] == 0
    with (tmp_path / "late/requests.csv").open(newline="") as stream:
        row = next(csv.DictReader(stream))
    assert row["status"] == "completed-after-horizon" and float(row["exited_at"]) > 0.1


def test_plots_export_real_measured_values(comparison):
    pytest.importorskip("matplotlib")
    output, result = comparison
    plot_results(output, result["results"])
    for name in ("completed", "p95_wait_seconds", "recovery_latency_seconds"):
        assert (output / f"{name}.png").stat().st_size > 1000
        assert "<svg" in (output / f"{name}.svg").read_text(encoding="utf-8")


@pytest.mark.parametrize("options", [dict(seeds=()), dict(seeds=(1, 1)), dict(requests=0),
    dict(scenario="typo"), dict(duration=float("nan")), dict(detection_timeout=0.05)])
def test_invalid_comparison_creates_no_results(tmp_path, options):
    output = tmp_path / "invalid"
    with pytest.raises(ValueError):
        asyncio.run(run_comparison(output, plots=False, **options))
    assert not output.exists()


def test_comparison_does_not_overwrite_existing_results(tmp_path):
    with pytest.raises(FileExistsError):
        asyncio.run(run_comparison(tmp_path, plots=False))
