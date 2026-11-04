from concurrent.futures import ThreadPoolExecutor

import pytest

from cranelock_sim.experiment import Experiment, LocalGateClient, run_experiment
from cranelock_sim.lane import Lane, SafetyViolation


def boundary(h):
    h.ready()
    events = []
    client = LocalGateClient(h.gate, h.config, h.keys)
    lane = Lane(client, h.clock, lambda event, **fields: events.append((event, fields)))
    return lane, events


def test_concurrent_entry_only_moves_once(h):
    lane, events = boundary(h)
    authority = h.state
    grant = h.grant()["grant"]
    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(lane.enter, "crane-1", f"r{i}", grant, authority)
                   for i in range(2)]
        assert sorted(future.result() for future in futures) == [False, True]
    assert sum(event == "physical_entry" for event, _ in events) == 1
    assert lane.occupant == "crane-1"
    assert h.state["lane_state"] == "OCCUPIED"


def test_restart_does_not_clear_physical_occupancy(h):
    lane, _ = boundary(h)
    authority = h.state
    assert lane.enter("crane-1", "r1", h.grant()["grant"], authority)
    h.restart()
    lane.client.gate = h.gate
    assert h.state["lane_state"] == "UNKNOWN"
    assert lane.observe()["allowed"]
    assert h.state["lane_state"] == "OCCUPIED"
    assert not h.grant()["allowed"]
    lane.exit("crane-1", "r1")
    assert h.state["lane_state"] == "CLEAR"


def test_stale_authority_cannot_move(h):
    lane, events = boundary(h)
    authority = h.state
    grant = h.grant()["grant"]
    assert h.install("crane-2")["allowed"]
    assert not lane.enter("crane-1", "r1", grant, authority)
    assert lane.occupant is None
    assert events[-1][1]["reason"] == "STALE_EPOCH"


def test_observation_race_resamples_actual_occupant(h, monkeypatch):
    lane, events = boundary(h)
    authority = h.state
    grant = h.grant()["grant"]
    assert lane.enter("crane-1", "r1", grant, authority)
    call = lane.client.call
    samples = []
    def expire_between_read_and_report(*args, **kwargs):
        samples.append(kwargs.copy())
        if len(samples) == 1:
            h.clock.advance(3)
            h.gate.get_state()  # Real phase change races the previously read revision.
        return call(*args, **kwargs)
    monkeypatch.setattr(lane.client, "call", expire_between_read_and_report)
    assert lane.observe()["allowed"]
    assert len(samples) == 2
    assert all(row["lane_state"] == "OCCUPIED" for row in samples)
    assert samples[1]["observation_revision"] > samples[0]["observation_revision"]
    assert samples[1]["observed_at_ms"] > samples[0]["observed_at_ms"]
    assert h.state["lane_state"] == "OCCUPIED"


def test_ambiguous_entry_reply_never_moves(h, monkeypatch):
    lane, _ = boundary(h)
    authority = h.state
    grant = h.grant()["grant"]
    call = lane.client.call

    def lose_reply(*args, **kwargs):
        result = call(*args, **kwargs)
        assert result["allowed"]  # Gate committed; client never gets permission.
        raise TimeoutError("entry response lost")

    monkeypatch.setattr(lane.client, "call", lose_reply)
    assert not lane.enter("crane-1", "r1", grant, authority)
    assert lane.occupant is None
    assert h.state["lane_state"] == "OCCUPIED"


def test_physical_checker_catches_faulty_gate_acceptance(h, monkeypatch):
    lane, _ = boundary(h)
    authority = h.state
    grant = h.grant()["grant"]
    assert lane.enter("crane-1", "r1", grant, authority)
    # A deliberately faulty gate response must not hide a physical overlap.
    monkeypatch.setattr(lane.client, "call", lambda *a, **k: dict(
        allowed=True, action_applied=True, replayed=False, state=authority))
    with pytest.raises(SafetyViolation, match="occupied physical lane"):
        lane.enter("crane-1", "r2", grant, authority)


def test_seed_reproduces_workload_results_and_trace(tmp_path):
    settings = Experiment(duration=8, arrival_rate=4, seed=20)
    first, second = tmp_path / "first", tmp_path / "second"
    a, b = run_experiment(settings, first), run_experiment(settings, second)
    assert a == b
    assert (first / "requests.csv").read_bytes() == (second / "requests.csv").read_bytes()
    assert (first / "events.jsonl").read_bytes() == (second / "events.jsonl").read_bytes()
    assert a["requested"] == a["completed"] + a["waiting"] + a["occupied"]
    assert a["accepted"] == a["completed"] + a["occupied"]
    assert a["waiting"] > 0  # Overload is retained rather than silently discarded.
    assert a["completed"] > 0
    assert a["safety_violations"] == 0


def test_crossing_past_horizon_is_not_counted_completed(tmp_path):
    result = run_experiment(Experiment(duration=1, arrival_rate=10, crossing_time=5),
                            tmp_path / "run")
    assert result["accepted"] == 1
    assert result["occupied"] == 1
    assert result["completed"] == 0
    assert result["jain_fairness"] is None


def test_existing_output_is_never_overwritten(tmp_path):
    marker = tmp_path / "summary.json"
    marker.write_text("keep me")
    with pytest.raises(FileExistsError):
        run_experiment(Experiment(), tmp_path)
    assert marker.read_text() == "keep me"


@pytest.mark.parametrize("field,value", [("cranes", 2), ("duration", 0),
    ("arrival_rate", float("nan")), ("crossing_time", -1)])
def test_invalid_workloads_are_rejected(field, value):
    with pytest.raises(ValueError):
        Experiment(**{field: value})
