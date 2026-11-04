import asyncio
import json

import pytest

from cranelock.agent import Agent, Mode
from cranelock_sim.benefits import make_workload, paired, run_case, run_benefits
from cranelock_sim.failures import until


def test_workload_is_repeatable_and_extends_past_outage():
    rows = make_workload(20, 18, 3)
    assert rows == make_workload(20, 18, 3)
    assert rows != make_workload(7, 18, 3)
    assert rows[0]["requested_at"] < 1 < rows[-1]["requested_at"] < 3
    assert {row["crane"] for row in rows} == {"crane-2", "crane-3"}


def test_rpc_counter_excludes_calls_after_horizon_even_if_snapshot_is_late(monkeypatch):
    from cranelock_sim import controller
    current = [1.0]
    monkeypatch.setattr(controller.time, "monotonic", lambda: current[0])
    stats = controller.RPCStats()
    stats.add("warmup")
    stats.reset(deadline=3)
    stats.add("peer.Heartbeat")
    current[0] = 4
    stats.add("gate.GetState")
    assert stats.snapshot() == {"peer.Heartbeat":1}


def test_pairing_rejects_mismatched_and_missing_inputs_and_keeps_cost():
    base = dict(seed=7, requested=6, workload_sha256="same", duration_seconds=3,
                crossing_time_seconds=.03, detection_timeout_seconds=.25,
                evaluation="benefits-v1", scenario="normal", mean_wait_seconds=.1)
    central = dict(base, model="central-lock")
    candidate = dict(base, model="cranelock", mean_wait_seconds=.2)
    assert paired([central, candidate], "normal", "central-lock", "mean_wait_seconds")["median"] == pytest.approx(.1)
    for change in (dict(workload_sha256="different"), dict(duration_seconds=2),
                   dict(evaluation=None), dict(requested=18), dict(seed=8)):
        assert paired([central, dict(candidate, **change)], "normal", "central-lock", "mean_wait_seconds")["median"] is None
    assert paired([central, central, candidate], "normal", "central-lock", "mean_wait_seconds")["median"] is None
    assert paired([dict(model="central-lock", scenario="normal"), dict(model="cranelock", scenario="normal")],
                  "normal", "central-lock", "mean_wait_seconds")["median"] is None


def test_complete_batch_pairing_does_not_mix_fixed_window_results():
    base=dict(seed=7,requested=18,workload_sha256="batch",duration_seconds=None,
        crossing_time_seconds=.03,detection_timeout_seconds=.25,evaluation="benefits-v2",
        scenario="normal",measurement_policy="complete_all_requests",total_completion_seconds=1)
    central=dict(base,model="central-lock")
    crane=dict(base,model="cranelock",total_completion_seconds=1.2)
    assert paired([central,crane],"normal","central-lock","total_completion_seconds")["median"]==pytest.approx(.2)
    old=dict(central,evaluation="benefits-v1",measurement_policy="fixed_window",duration_seconds=5)
    assert paired([old,crane],"normal","central-lock","total_completion_seconds")["median"] is None


@pytest.mark.parametrize("model", ["central-lock", "basic-token", "cranelock"])
def test_normal_finishes_entire_batch_beyond_former_cutoff(tmp_path, model):
    rows=[dict(row,requested_at=0.0) for row in make_workload(7,3,.1)]
    output=tmp_path / model
    result=asyncio.run(run_case(output,model,"normal",rows,seed=7,duration=.1,
        crossing_time=.08,detection_timeout=.2))
    assert result["completed"]==result["requested"]==3
    assert result["completed_all_requests"] is True
    assert result["duration_seconds"] is None
    assert result["measurement_policy"]=="complete_all_requests"
    assert result["total_completion_seconds"]>.1
    assert result["mean_wait_seconds"]>=0
    assert result["unfinished_at_horizon"] is None
    assert result["rpc_per_completed"]>0
    import csv
    with (output / "requests.csv").open() as stream:
        requests=list(csv.DictReader(stream))
    assert all(row["status"]=="completed" for row in requests)
    assert result["total_completion_seconds"]>=max(float(row["exited_at"]) for row in requests)


@pytest.mark.parametrize("model", ["central-lock", "basic-token", "cranelock"])
def test_real_controller_outage_comparison(tmp_path, model):
    output = tmp_path / model
    result = asyncio.run(run_case(output, model, "controller-outage", make_workload(7, 6, 2.4),
        seed=7, duration=2.4, crossing_time=.03, detection_timeout=.2))
    events = [json.loads(line) for line in (output / "events.jsonl").read_text().splitlines()]
    assert any(event["event"] == "controller_rpc_unavailable" for event in events)
    assert result["rpc_attempts"] > 0 and result["safety_violations"] == 0
    assert not any(event["event"] == "holder_stopped" for event in events)
    if model == "central-lock":
        assert result["post_fault_new_entries"] == 0
        assert result["resumed_within_window"] is False
        assert result["outage_to_entry_seconds"] is None
        assert result["initial_mode"] == result["final_mode"] == "CENTRAL"
        assert not any(event["event"] == "central_assignment" and event["time"] > result["fault_at_seconds"] for event in events)
    else:
        assert result["post_fault_new_entries"] > 0 and result["resumed_within_window"]
        assert result["final_mode"] == "LOCAL"
    if model == "cranelock":
        assert result["initial_mode"] == "CENTRAL"
        cert = json.loads((output / "recovery-certificate.json").read_text())
        assert len({signature["signer_id"] for signature in cert["signatures"]}) >= 2
        assert cert["payload"]["epoch"] == 1
        assert result["rpc_by_method"]["peer.VoteRecovery"] >= 2
        assert result["failure_detection_seconds"] > 0
        assert any(event["event"] == "stale_central_authority_rejected" and event["reason"] == "STALE_EPOCH" for event in events)
        assert sum(event.get("agent_event", {}).get("event") == "CONTROLLER_SUSPECTED" for event in events) == 3


def test_switch_preserves_an_occupied_lane(tmp_path):
    output = tmp_path / "occupied"
    result = asyncio.run(run_case(output, "cranelock", "controller-outage", make_workload(7, 6, 3.6),
        seed=7, duration=3.6, crossing_time=1.5, detection_timeout=.2))
    events = [json.loads(line) for line in (output / "events.jsonl").read_text().splitlines()]
    switch = next(event for event in events if event["event"] == "controller_fallback_installed")
    assert switch["occupant"] is not None
    occupant = None
    for event in events:
        if event["event"] == "physical_entry":
            assert occupant is None
            occupant = event["crane"]
        elif event["event"] == "physical_exit":
            assert occupant == event["crane"]
            occupant = None
    assert occupant is None and result["safety_violations"] == 0


def test_controller_watch_requires_timeout_and_healthy_controller_prevents_votes(h, tmp_path):
    async def scenario():
        healthy = True
        async def probe():
            return healthy
        agent = Agent("crane-2", {"crane-1":"localhost:1", "crane-3":"localhost:2"}, "crane-1",
            mode=Mode.CENTRAL, epoch=0, interval=.02, timeout=.08,
            state_file=tmp_path / "voter.json", recovery_config=h.config,
            recovery_private_key=h.keys["crane-2"], controller_probe=probe, event_sink=lambda e: None)
        watch = asyncio.create_task(agent.controller_watch_loop())
        try:
            agent.suspected.add("crane-1")
            assert await agent.propose_recovery({"epoch":0,"revision":1}) is None
            assert not agent.vote_state
            healthy = False
            await asyncio.sleep(.025)
            assert not agent.controller_suspected
            await until(lambda: agent.controller_suspected, 1, "watch timeout")
            # Own suspicion alone cannot produce a strict-majority certificate.
            assert await agent.propose_recovery({"epoch":0,"revision":1}) is None
            healthy = True
            await until(lambda: not agent.controller_suspected, 1, "controller restored")
            assert agent.state.mode == Mode.CENTRAL
            assert await agent.propose_recovery({"epoch":0,"revision":1}) is None
        finally:
            watch.cancel()
            await asyncio.gather(watch, return_exceptions=True)
            await agent.close()
    asyncio.run(scenario())


@pytest.mark.parametrize("kwargs", [dict(loads=(2,)), dict(loads=(6,6)), dict(seeds=(7,7)),
    dict(duration=float("nan")), dict(duration=.2), dict(scenario="unknown")])
def test_invalid_benefit_settings_do_not_create_outputs(tmp_path, kwargs):
    output = tmp_path / "bad"
    with pytest.raises(ValueError):
        asyncio.run(run_benefits(output, **kwargs))
    assert not output.exists()


def test_matrix_exports_mixed_scenario_metrics_without_dropping_fields(tmp_path, monkeypatch):
    import csv
    from cranelock_sim import benefits
    async def fixture_case(output, model, scenario, workload, **settings):
        row = dict(model=model, scenario=scenario, seed=settings["seed"], requested=len(workload),
                   completed=0 if model == "basic-token" else len(workload), safety_violations=0)
        if scenario == "holder-failure":
            row["authority_recovery_seconds"] = None
        else:
            row["rpc_by_method"] = {"controller.Heartbeat": 10}
            row["rpc_attempts"] = 10
        return row
    monkeypatch.setattr(benefits, "run_case", fixture_case)
    output = tmp_path / "matrix"
    result = asyncio.run(run_benefits(output, seeds=(7,), loads=(6,), plots=False))
    with (output / "benefits.csv").open(newline="") as stream:
        rows = list(csv.DictReader(stream))
    assert len(rows) == result["runs"] == 9
    assert "rpc_attempts" in rows[0] and "authority_recovery_seconds" in rows[0]
    assert all(row["rpc_attempts"] == "" for row in rows if row["scenario"] == "holder-failure")
    assert json.loads((output / "summary.json").read_text())["passed"]
