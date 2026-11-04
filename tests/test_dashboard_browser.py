"""Optional browser checks; use an installed Edge/Chrome without browser downloads."""
from pathlib import Path

import pytest

from test_dashboard import saved_results, dashboard_server  # Shared fixture data, no simulation jobs.

playwright = pytest.importorskip("playwright.sync_api")


@pytest.fixture
def browser():
    candidates = [Path("C:/Program Files (x86)/Microsoft/Edge/Application/msedge.exe"),
                  Path("C:/Program Files/Google/Chrome/Application/chrome.exe")]
    executable = next((path for path in candidates if path.is_file()), None)
    if executable is None:
        pytest.skip("An installed Edge or Chrome is required for this optional browser check")
    with playwright.sync_playwright() as p:
        b = p.chromium.launch(executable_path=str(executable), headless=True)
        yield b
        b.close()


def test_progress_chart_shows_one_sequence_and_marks_recorded_fault(browser, dashboard_server):
    import json
    url, root = dashboard_server
    for seed in (7, 20):
        for model in ("central-lock", "basic-token", "cranelock"):
            path = root / "comparison-validated" / f"seed-{seed}" / "holder-failure" / model
            summary = json.loads((path / "summary.json").read_text())
            summary.update(duration_seconds=5, completed=0 if model == "basic-token" else 6)
            (path / "summary.json").write_text(json.dumps(summary))
            times = [""] * 6 if model == "basic-token" else [str(i * 0.5) for i in range(1, 7)]
            (path / "requests.csv").write_text("request_id,exited_at\n" + "".join(f"r{i},{t}\n" for i, t in enumerate(times)))
    page = browser.new_page(viewport=dict(width=1440, height=1000))
    page.goto(url)
    playwright.expect(page.locator("#progress-legend")).to_contain_text("Basic token · 0/6 finished")
    assert page.locator("#progress-plot path").count() == 3
    assert page.locator("#progress-plot .fault-marker").count() == 1
    assert page.locator("#progress-plot circle").count() == 12
    playwright.expect(page.locator("#progress-note")).to_contain_text("one sequence, not the median")
    page.select_option("#progress-seed", "7")
    playwright.expect(page.locator("#progress-plot svg")).to_have_attribute("aria-label", "Cumulative completed requests, sequence 7. Exact completion counts are listed below.")
    page.set_viewport_size(dict(width=390, height=844))
    assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
    page.close()


def test_dashboard_filter_playback_download_and_mobile(browser, dashboard_server):
    url, _ = dashboard_server
    page = browser.new_page(viewport=dict(width=1440, height=1000))
    errors = []
    page.on("pageerror", lambda error: errors.append(str(error)))
    page.goto(url)
    playwright.expect(page.locator("#kpi-runs")).to_have_text("6 / 6")
    playwright.expect(page.locator("#kpi-completed")).to_have_text("24")
    assert page.locator(".chart-group").count() == 3
    playwright.expect(page.locator(".undefined")).to_contain_text("2/2 test(s) never resumed")
    page.locator(".advanced-settings > summary").click()
    page.select_option("#seed", "20")
    playwright.expect(page.locator("#kpi-runs")).to_have_text("3 / 3")
    playwright.expect(page.locator("#kpi-completed")).to_have_text("12")
    identity = "comparison-validated/seed-20/holder-failure/cranelock"
    page.select_option("#trace-model", "cranelock")
    page.select_option("#trace-sequence", identity)
    playwright.expect(page.locator("#event-count")).to_have_text("5 recorded")
    page.locator("#scrub").evaluate("el => { el.value=0.6; el.dispatchEvent(new Event('input')); }")
    playwright.expect(page.locator("#occupant")).to_have_text("Crane 2 · occupying lane")
    page.locator("#scrub").evaluate("el => { el.value=0; el.dispatchEvent(new Event('input')); }")
    page.select_option("#speed", "4")
    page.click("#play")
    playwright.expect(page.locator("#playback-state")).to_have_text("Trace complete")
    playwright.expect(page.locator("#occupant")).to_have_text("No recorded occupant")
    before = page.locator(".event-item").count()
    page.check("#technical")
    assert page.locator(".event-item").count() == before + 2  # Heartbeat and state snapshot.
    with page.expect_download() as download:
        page.get_by_role("link", name="↓ requests.csv", exact=True).click()
    assert download.value.suggested_filename == "requests.csv"
    page.set_viewport_size(dict(width=390, height=844))
    assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
    assert not errors
    page.close()


def test_failure_details_render_as_text_not_executable_markup(browser, dashboard_server):
    url, _ = dashboard_server
    page = browser.new_page()
    page.goto(url)
    playwright.expect(page.locator("#kpi-runs")).to_have_text("6 / 6")
    page.locator(".advanced-settings > summary").click()
    page.select_option("#collection", "failed-demo")
    playwright.expect(page.locator("#kpi-runs")).to_have_text("0 / 1")
    playwright.expect(page.locator("#limitations")).to_contain_text("<img src=x")
    assert page.locator("#limitations img").count() == 0
    playwright.expect(page.locator("#play")).to_be_disabled()
    page.close()


def test_explanations_and_simple_choices_hide_repeated_development_runs(browser, dashboard_server):
    import json
    url, root = dashboard_server
    extra = root / "comparison-development" / "sample"
    extra.mkdir(parents=True)
    (extra / "experiment.json").write_text(json.dumps(dict(model="cranelock", scenario="holder-failure", seed=99)))
    page = browser.new_page(viewport=dict(width=1440, height=1000))
    page.goto(url)
    playwright.expect(page.locator("#kpi-runs")).to_have_text("6 / 6")
    playwright.expect(page.locator("#experiment-title")).to_have_text("Compare the three systems")
    playwright.expect(page.locator("#test-explanation")).to_contain_text("software is stopped")
    playwright.expect(page.locator("#expected-explanation")).to_contain_text("Basic token should stall")
    playwright.expect(page.locator("#result-explanation")).to_contain_text("Basic token 0/12 completed")
    assert page.locator("#collection option").count() == 2
    assert page.locator("#trace-model option").count() == 3
    assert page.locator("#trace-scenario option").count() == 1
    assert page.locator("#trace-sequence option").count() == 2
    assert page.locator(".table-section").get_attribute("open") is None
    assert page.locator(".exact-run").get_attribute("open") is None
    page.select_option("#trace-model", "basic-token")
    playwright.expect(page.locator("#run-story")).to_contain_text("did not resume lane access")
    playwright.expect(page.locator("#run-story")).to_contain_text("6 requests remained unfinished")
    page.locator(".advanced-settings > summary").click()
    page.check("#all-collections")
    assert page.locator("#collection option").count() == 3
    page.set_viewport_size(dict(width=390, height=844))
    assert page.evaluate("document.documentElement.scrollWidth <= window.innerWidth")
    page.close()


def test_guided_playback_holds_events_and_supports_back_and_next(browser, dashboard_server):
    url, _ = dashboard_server
    page = browser.new_page()
    page.goto(url)
    playwright.expect(page.locator("#event-count")).to_have_text("5 recorded")
    playwright.expect(page.locator("#speed")).to_have_value("guided")
    page.click("#next-event")
    playwright.expect(page.locator("#moment-title")).to_contain_text("software stops")
    playwright.expect(page.locator("#moment-explanation")).to_contain_text("surviving agents must detect")
    page.click("#next-event")
    playwright.expect(page.locator("#occupant")).to_have_text("Crane 2 · occupying lane")
    playwright.expect(page.locator("#moment-title")).to_have_text("Crane 2 enters the shared lane")
    page.click("#previous-event")
    playwright.expect(page.locator("#occupant")).to_have_text("No recorded occupant")
    page.click("#play")
    page.wait_for_timeout(200)
    playwright.expect(page.locator("#moment-title")).to_contain_text("software stops")
    page.click("#play")
    page.click("#reset-trace")
    page.click("#play")
    playwright.expect(page.locator("#clock")).to_have_text("0.100 s")
    page.wait_for_timeout(650)
    playwright.expect(page.locator("#clock")).to_have_text("0.100 s")
    playwright.expect(page.locator("#moment-title")).to_contain_text("software stops")
    playwright.expect(page.locator("#moment-title")).to_have_text("Crane 2 enters the shared lane", timeout=6000)
    page.click("#play")
    playwright.expect(page.locator("#playback-state")).to_have_text("Paused")
    playwright.expect(page.locator("#kpi-completed")).to_have_text("24")
    page.close()


def test_event_stepping_preserves_order_for_equal_timestamps(browser, dashboard_server):
    import json
    url, root = dashboard_server
    path = root / "equal-times"
    path.mkdir()
    (path / "experiment.json").write_text(json.dumps(dict(model="three-agent-grpc-normal-handoff", scenario="normal")))
    (path / "summary.json").write_text(json.dumps(dict(completed=1, safety_violations=0)))
    (path / "events.jsonl").write_text("\n".join(json.dumps(dict(time=0.5, event=kind, crane="crane-2"))
                                                for kind in ("physical_entry", "physical_exit")))
    page = browser.new_page()
    page.goto(url)
    playwright.expect(page.locator("#kpi-runs")).to_have_text("6 / 6")
    page.locator(".advanced-settings > summary").click()
    page.select_option("#collection", "equal-times")
    playwright.expect(page.locator("#event-count")).to_have_text("2 recorded")
    page.click("#next-event")
    playwright.expect(page.locator("#occupant")).to_have_text("Crane 2 · occupying lane")
    page.click("#next-event")
    playwright.expect(page.locator("#occupant")).to_have_text("No recorded occupant")
    page.click("#previous-event")
    playwright.expect(page.locator("#occupant")).to_have_text("Crane 2 · occupying lane")
    page.close()


def test_paired_benefits_costs_load_filter_and_controller_explanation(browser, dashboard_server):
    from test_dashboard import save
    url, root = dashboard_server
    for load in (6,18):
        for model, wait, rpc in (("central-lock", .1, 20), ("cranelock", .2, 30)):
            path = root / "benefits-validated" / str(load) / model
            settings = dict(model=model, scenario="normal", seed=7)
            save(path, "experiment.json", settings)
            save(path, "summary.json", dict(**settings, evaluation="benefits-v1", requested=load,
                completed=load, safety_violations=0, workload_sha256="paired", duration_seconds=3,
                crossing_time_seconds=.03, detection_timeout_seconds=.25, mean_wait_seconds=wait,
                rpc_per_completed=rpc))
    page = browser.new_page(viewport=dict(width=1440,height=1000))
    errors = []
    page.on("pageerror", lambda e: errors.append(str(e)))
    page.goto(url)
    playwright.expect(page.locator("#collection")).to_have_value("benefits-validated")
    playwright.expect(page.locator("#kpi-runs")).to_have_text("2 / 2")
    playwright.expect(page.locator("#benefit-cards")).to_contain_text("100.00 ms longer")
    assert "RPCs / completion" not in page.locator("#benefits").inner_text()
    assert page.locator(".chart-group").count() == 2
    assert not page.locator(".advanced-settings").evaluate("el => el.open")
    assert not page.locator(".advanced-totals").evaluate("el => el.open")
    page.locator(".advanced-settings > summary").click()
    page.select_option("#load", "6")
    playwright.expect(page.locator("#kpi-runs")).to_have_text("2 / 2")
    assert page.locator("#benefit-cards article").count() == 1
    assert "18 requests" not in page.locator("#benefit-cards").inner_text()
    assert page.evaluate('pairDifferences([{status:"passed",model:"cranelock",scenario:"normal",summary:{evaluation:"benefits-v1",workload_sha256:"x",mean_wait_seconds:0}},{status:"passed",model:"central-lock",scenario:"normal",summary:{evaluation:"benefits-v1",workload_sha256:"different",mean_wait_seconds:1}}],"normal","central-lock","mean_wait_seconds").length') == 0
    story = page.evaluate('describe({family:"comparison",scenario:"controller-outage"})')
    assert "controller service is stopped" in story["what"]
    assert "majority" in story["expected"]
    page.set_viewport_size(dict(width=390,height=844))
    assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
    assert not errors
    page.close()


def test_essential_failure_comparisons_explain_requests_and_do_not_fake_recovery(browser, dashboard_server):
    from test_dashboard import save
    url, root = dashboard_server
    for scenario in ("normal", "controller-outage", "holder-failure"):
        for model in ("central-lock", "basic-token", "cranelock"):
            stalled = (scenario=="controller-outage" and model=="central-lock") or (scenario=="holder-failure" and model=="basic-token")
            path = root / "benefits-validated" / scenario / model
            settings = dict(model=model, scenario=scenario, seed=7)
            save(path, "experiment.json", settings)
            save(path, "summary.json", dict(**settings, evaluation="benefits-v1", requested=6,
                completed=0 if stalled else 6, unfinished_at_horizon=6 if stalled else 0,
                safety_violations=0, workload_sha256="same", duration_seconds=5,
                crossing_time_seconds=.03, detection_timeout_seconds=.25,
                post_fault_new_entries=0 if stalled else 4,
                mean_wait_seconds=.2 if model=="cranelock" else .1,
                resumed_within_window=not stalled,
                failure_detection_seconds=.2 if not stalled else None,
                recovery_latency_seconds=.1 if not stalled else None,
                outage_to_entry_seconds=.3 if not stalled else None))
    page = browser.new_page(viewport=dict(width=1440,height=1100))
    page.goto(url)
    playwright.expect(page.locator("#focus")).to_have_value("controller-outage")
    playwright.expect(page.locator("#focus-context")).to_contain_text("One request means one crane")
    playwright.expect(page.locator("#benefits")).to_contain_text("allowed 4 new requests to start")
    playwright.expect(page.locator("#benefits")).to_contain_text("Central lock allowed 0")
    assert page.locator(".chart-group").count()==3
    assert page.locator("#metric").count()==0
    recovery=page.locator('[data-chart="recovery"]')
    playwright.expect(recovery).to_contain_text("Keeps working")
    playwright.expect(recovery).to_contain_text("Basic token keeps serving requests when the controller stops")
    playwright.expect(recovery).to_contain_text("If the crane holding the token stops instead, basic token gets stuck")
    playwright.expect(recovery).to_contain_text("Did not resume")
    playwright.expect(recovery).to_contain_text("0.300 seconds")
    assert "RPC" not in page.locator("#comparison").inner_text()
    assert not page.locator(".stats").is_visible()
    page.select_option("#focus","holder-failure")
    playwright.expect(page.locator("#benefits")).to_contain_text("CraneLock completes 6 of 6 requests")
    playwright.expect(page.locator("#benefits")).to_contain_text("Basic token completes 0 of 6")
    page.select_option("#focus","normal")
    assert page.locator(".chart-group").count()==2
    assert page.locator('[data-chart="recovery"]').count()==0
    page.set_viewport_size(dict(width=390,height=844))
    assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
    page.close()


def test_normal_request_graph_explains_actual_late_completions(browser,dashboard_server):
    from test_dashboard import save
    url,root=dashboard_server
    for model,last in (("central-lock",4.992),("basic-token",5.068),("cranelock",5.035)):
        path=root / "benefits-validated" / model
        settings=dict(model=model,scenario="normal",seed=20)
        save(path,"experiment.json",settings)
        save(path,"summary.json",dict(**settings,requested=18,completed=18 if last<5 else 17,
            duration_seconds=5,safety_violations=0))
        (path / "requests.csv").write_text("request_id,exited_at\n"+
            "".join(f"r{i},{i*.2}\n" for i in range(1,18))+f"r18,{last}\n")
    page=browser.new_page()
    page.goto(url)
    chart=page.locator('[data-chart="requests"]')
    playwright.expect(chart).to_contain_text("17 of 18 by 5s")
    crane=chart.locator(".bar-row").filter(has_text="CraneLock")
    playwright.expect(crane).to_contain_text("17 of 18 requests finished by the cutoff; 1 finished afterward; 0 had no recorded completion")
    playwright.expect(crane).to_contain_text("finished at 5.035 seconds")
    playwright.expect(crane).to_contain_text("A late finish is not a failed request")
    central=chart.locator(".bar-row").filter(has_text="Central lock")
    playwright.expect(central).to_contain_text("18 of 18 requests finished by the cutoff; 0 finished afterward")
    page.close()


def test_completed_batch_uses_execution_time_without_a_completion_cutoff(browser,dashboard_server):
    from test_dashboard import save
    url,root=dashboard_server
    for model,total,wait in (("central-lock",1,.5),("basic-token",1.5,.75),("cranelock",1.2,.6)):
        path=root / "benefits-completion-validated" / model
        settings=dict(model=model,scenario="normal",seed=7)
        save(path,"experiment.json",settings)
        save(path,"summary.json",dict(**settings,requested=18,completed=18,completed_all_requests=True,
            evaluation="benefits-v2",measurement_policy="complete_all_requests",workload_sha256="batch",
            duration_seconds=None,total_completion_seconds=total,mean_wait_seconds=wait,
            crossing_time_seconds=.03,detection_timeout_seconds=.25,safety_violations=0))
    page=browser.new_page()
    page.goto(url)
    playwright.expect(page.locator("#collection")).to_have_value("benefits-completion-validated")
    playwright.expect(page.locator("#focus-context")).to_contain_text("until every request finishes")
    playwright.expect(page.locator("#focus-context")).to_contain_text("no five-second completion cutoff")
    assert page.locator('[data-chart="requests"]').count()==0
    assert page.locator(".chart-group").count()==2
    playwright.expect(page.locator('[data-chart="execution"]')).to_contain_text("finish all 18 requests")
    playwright.expect(page.locator('[data-chart="execution"]')).to_contain_text("1.200 seconds")
    playwright.expect(page.locator('[data-chart="waiting"]')).to_contain_text("600.0 ms")
    playwright.expect(page.locator("#benefits")).to_contain_text("Each system must finish all 18 requests")
    assert "same fixed time" not in page.locator("#metric-explanation").inner_text()
    page.close()


def test_controller_visibly_stops_and_stays_offline_during_local_fallback(browser,dashboard_server):
    import json
    from test_dashboard import save
    url,root=dashboard_server
    for model in ("central-lock","basic-token","cranelock"):
        path=root / "controller-playback" / model
        settings=dict(model=model,scenario="controller-outage",seed=20)
        save(path,"experiment.json",settings)
        save(path,"summary.json",dict(completed=1,requested=1,safety_violations=0))
        events=[dict(time=0,event="workload_started",mode="LOCAL" if model=="basic-token" else "CENTRAL"),
                dict(time=1,event="controller_stopped")]
        if model=="cranelock":
            events.extend([dict(time=1.1,event="agent_event",agent_event=dict(event="CONTROLLER_SUSPECTED",agent_id="crane-2")),
                           dict(time=1.2,event="controller_fallback_installed",mode="LOCAL",epoch=1,holder="crane-2")])
        (path / "events.jsonl").write_text("\n".join(json.dumps(event) for event in events))
    page=browser.new_page(viewport=dict(width=1440,height=1000))
    errors=[]
    page.on("pageerror",lambda error: errors.append(str(error)))
    page.goto(url)
    page.locator(".advanced-settings > summary").click()
    page.check("#all-collections")
    page.select_option("#collection","controller-playback")
    playwright.expect(page.locator("#event-count")).to_have_text("4 recorded")
    playwright.expect(page.locator("#controller-status")).to_have_text("Before recorded traffic")
    page.click("#next-event")
    playwright.expect(page.locator("#controller-console")).to_have_attribute("data-state","running")
    page.click("#jump-controller-stop")
    playwright.expect(page.locator("#controller-console")).to_have_attribute("data-state","stopped")
    playwright.expect(page.locator("#controller-status")).to_contain_text("STOPPED")
    playwright.expect(page.locator("#moment-title")).to_have_text("The central controller service stops")
    playwright.expect(page.locator("#coordination-status")).to_contain_text("waiting for the cranes to detect")
    assert page.locator(".crane.stopped").count()==0
    page.click("#next-event")
    playwright.expect(page.locator("#coordination-status")).to_contain_text("waiting for majority-approved")
    page.click("#next-event")
    playwright.expect(page.locator("#controller-console")).to_have_attribute("data-state","stopped")
    playwright.expect(page.locator("#coordination-status")).to_contain_text("LOCAL MODE")
    page.click("#previous-event")
    playwright.expect(page.locator("#coordination-status")).to_have_attribute("data-mode","blocked")
    page.click("#reset-trace")
    page.click("#next-event")
    playwright.expect(page.locator("#controller-console")).to_have_attribute("data-state","running")
    page.select_option("#trace-model","basic-token")
    playwright.expect(page.locator("#event-count")).to_have_text("2 recorded")
    page.click("#jump-controller-stop")
    playwright.expect(page.locator("#coordination-status")).to_contain_text("Basic token keeps working locally")
    page.select_option("#trace-model","central-lock")
    playwright.expect(page.locator("#run-story")).to_contain_text("Central lock")
    page.click("#jump-controller-stop")
    playwright.expect(page.locator("#coordination-status")).to_contain_text("New requests blocked")
    page.set_viewport_size(dict(width=390,height=844))
    assert page.evaluate("document.documentElement.scrollWidth <= innerWidth")
    assert not errors
    page.close()
