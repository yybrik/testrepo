import json
import threading
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import urlopen

import pytest

from cranelock_sim.dashboard import Results, make_server


def save(path, name, value):
    path.mkdir(parents=True, exist_ok=True)
    (path / name).write_text(json.dumps(value), encoding="utf-8")


@pytest.fixture
def saved_results(tmp_path):
    root = tmp_path / "evaluation"
    for seed in (7, 20):
        for model in ("central-lock", "basic-token", "cranelock"):
            path = root / "comparison-validated" / f"seed-{seed}" / "holder-failure" / model
            save(path, "experiment.json", dict(model=model, scenario="holder-failure", seed=seed))
            stalled = model == "basic-token"
            save(path, "summary.json", dict(model=model, scenario="holder-failure", seed=seed,
                completed=0 if stalled else 6, requested=6, unfinished_at_horizon=6 if stalled else 0,
                p95_wait_seconds=None if stalled else 0.8, recovery_latency_seconds=None if stalled else 0.4,
                resumed_within_window=not stalled,
                throughput_per_minute=0 if stalled else 180, safety_violations=0,
                limitations="TEST FIXTURE: reference central scheduler; shared gate."))
            trace = [] if stalled else [
                dict(time=0.1, event="holder_stopped", crane="crane-1"),
                dict(time=0.2, event="agent_event", agent_event=dict(event="HEARTBEAT_SENT", agent_id="crane-2")),
                dict(time=0.4, event="agent_snapshot", owner="crane-2", authority_epoch=1,
                     agents={"crane-2": dict(has_token=True, epoch=1, mode="LOCAL")}),
                dict(time=0.5, event="physical_entry", crane="crane-2", request_id="r1"),
                dict(time=0.7, event="physical_exit", crane="crane-2", request_id="r1"),
            ]
            (path / "events.jsonl").write_text("".join(json.dumps(event) + "\n" for event in trace))
            (path / "requests.csv").write_text("request_id,crane\nr1,crane-2\n")
    (root / "comparison-validated/comparison.csv").write_text("model,completed\ncranelock,6\n")
    failed = root / "failed-demo"
    save(failed, "experiment.json", dict(model="holder-restart-rejoin", scenario="before-entry"))
    save(failed, "failure.json", dict(error="RuntimeError", detail='<img src=x onerror="alert(1)"> test failure'))
    return root


@pytest.fixture
def dashboard_server(saved_results):
    server = make_server(saved_results, 0)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield f"http://127.0.0.1:{server.server_port}", saved_results
    server.shutdown()
    server.server_close()
    thread.join(timeout=3)


def test_progress_uses_recorded_exits_and_observation_policy(saved_results):
    identity = "comparison-validated/seed-20/holder-failure/cranelock"
    path = saved_results / identity
    summary = dict(requested=3, completed=2, duration_seconds=5, fault_at_seconds=1.5)
    save(path, "summary.json", summary)
    (path / "requests.csv").write_text("request_id,exited_at\nr1,0.7\nr2,2.3\nr3,5.1\n")
    results = Results(saved_results)
    assert results.progress(identity) == dict(available=True, exits=[0.7, 2.3], window=5, fault_at=1.5)
    # Full-batch tests include completions beyond five seconds.
    summary.update(measurement_policy="complete_all_requests", total_completion_seconds=5.2,
                   duration_seconds=None, completed=3)
    save(path, "summary.json", summary)
    assert results.progress(identity)["exits"] == [0.7, 2.3, 5.1]
    # A missing completion is evidence of a stall, not a missing timestamp of zero.
    summary.update(measurement_policy="fixed_window", duration_seconds=5, completed=0)
    save(path, "summary.json", summary)
    (path / "requests.csv").write_text("request_id,exited_at\nr1,\nr2,\nr3,\n")
    assert results.progress(identity)["exits"] == []
    for invalid in ("nan", "-1", "bad"):
        (path / "requests.csv").write_text(f"request_id,exited_at\nr1,{invalid}\nr2,\nr3,\n")
        assert results.progress(identity)["available"] is False
    (path / "requests.csv").write_text("request_id,exited_at\nr1,1\n")
    assert results.progress(identity)["available"] is False


def test_catalog_has_leaf_runs_and_preserves_missing_metrics(saved_results):
    catalog = Results(saved_results).catalog()
    assert len(catalog["runs"]) == 7 and len(catalog["collections"]) == 2
    assert not catalog["warnings"]
    assert sum(run["status"] == "failed" for run in catalog["runs"]) == 1
    assert sum(run["status"] == "passed" for run in catalog["runs"]) == 6
    basic = next(run for run in catalog["runs"] if run["model"] == "basic-token")
    assert basic["summary"]["recovery_latency_seconds"] is None
    assert basic["summary"]["completed"] == 0


def test_completion_timing_distinguishes_late_finish_from_no_finish(saved_results):
    path = saved_results / "cutoff" / "sample"
    save(path,"experiment.json",dict(model="cranelock",scenario="normal",seed=20))
    save(path,"summary.json",dict(requested=3,completed=1,duration_seconds=5,safety_violations=0))
    (path / "requests.csv").write_text("request_id,exited_at\na,4.992\nb,5.035\nc,\n")
    result = Results(saved_results)
    row = next(row for row in result.catalog()["runs"] if row["collection"]=="cutoff")
    assert row["completion_timing"] == dict(finished_by_cutoff=1,finished_after_cutoff=1,
        no_recorded_completion=1,late_finish_seconds=[5.035])
    (path / "requests.csv").write_text("request_id,exited_at\na,nan\nb,5.035\nc,\n")
    row = next(row for row in result.catalog()["runs"] if row["collection"]=="cutoff")
    assert row["completion_timing"] is None


def test_failed_and_incomplete_runs_are_never_assumed_successful(saved_results):
    path = saved_results / "incomplete"
    save(path, "experiment.json", dict(model="holder-restart-rejoin"))
    assert Results(saved_results).detail("incomplete")["status"] == "incomplete"
    assert Results(saved_results).detail("failed-demo")["status"] == "failed"


def test_legacy_logical_run_model_comes_from_summary(saved_results):
    path = saved_results / "logical"
    save(path, "experiment.json", dict(arrival_rate=2, seed=20))
    save(path, "summary.json", dict(model="local-gate-normal-traffic", completed=4, safety_violations=0))
    run = next(run for run in Results(saved_results).catalog()["runs"] if run["collection"] == "logical")
    assert run["family"] == "logical"


def test_bad_json_is_reported_without_losing_valid_results(saved_results):
    broken = saved_results / "broken"
    broken.mkdir()
    (broken / "experiment.json").write_text("not JSON")
    catalog = Results(saved_results).catalog()
    assert len(catalog["runs"]) == 7 and len(catalog["warnings"]) == 1


def test_event_reader_reports_malformed_and_truncated_data(saved_results):
    path = saved_results / "many"
    save(path, "experiment.json", dict(model="cranelock"))
    (path / "events.jsonl").write_text((json.dumps(dict(time=0, event="request")) + "\n") * 10002 + "broken\n")
    data = Results(saved_results).events("many")
    assert len(data["events"]) == 10000 and data["total"] == 10002
    assert data["truncated"] and data["malformed"] == 1


def test_http_serves_ui_json_and_downloads(dashboard_server):
    url, _ = dashboard_server
    with urlopen(url) as response:
        assert response.status == 200 and b"Crane coordination tests" in response.read()
        assert "script-src 'self'" in response.headers["Content-Security-Policy"]
    with urlopen(url + "/api/catalog") as response:
        assert len(json.load(response)["runs"]) == 7
    identity = "comparison-validated/seed-20/holder-failure/cranelock"
    with urlopen(url + "/api/run?" + urlencode(dict(id=identity))) as response:
        assert json.load(response)["summary"]["completed"] == 6
    with urlopen(url + "/api/artifact?" + urlencode(dict(id=identity, name="requests.csv"))) as response:
        assert 'attachment; filename="requests.csv"' == response.headers["Content-Disposition"]
        assert response.read().startswith(b"request_id,crane")


@pytest.mark.parametrize("identity,name", [("../", "summary.json"), ("failed-demo", "../private-keys.json"),
    ("failed-demo", "private-keys.json"), ("failed-demo", "gate.sqlite")])
def test_downloads_cannot_escape_results_or_expose_unlisted_files(dashboard_server, identity, name):
    url, _ = dashboard_server
    with pytest.raises(HTTPError) as error:
        urlopen(url + "/api/artifact?" + urlencode(dict(id=identity, name=name)))
    assert error.value.code == 400


def test_missing_results_folder_has_real_empty_state(tmp_path):
    assert Results(tmp_path / "missing").catalog()["runs"] == []


def test_api_has_no_mutation_route(dashboard_server):
    from urllib.request import Request
    url, _ = dashboard_server
    with pytest.raises(HTTPError) as error:
        urlopen(Request(url + "/api/run", method="POST", data=b"{}"))
    assert error.value.code == 501
