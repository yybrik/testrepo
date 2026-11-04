"use strict";
const $ = id => document.getElementById(id);
const node = (tag, cls, text) => { const n=document.createElement(tag); if(cls)n.className=cls; if(text!==undefined)n.textContent=text; return n; };
const finite = v => typeof v === "number" && Number.isFinite(v);
const fmt = (v, unit="") => finite(v) ? `${Number.isInteger(v)&&!unit?v:v.toFixed(3)}${unit}` : "—";
const names = {"central-lock":"Central lock", "basic-token":"Basic token", cranelock:"CraneLock",
  "local-gate-normal-traffic":"Logical workload", "three-agent-grpc-normal-handoff":"Agent handoff",
  "holder-service-stop-recovery":"Holder recovery", "holder-restart-rejoin":"Restart & rejoin", "peer-network-partition":"Network partition"};
const title = text => String(text??"").replaceAll("_"," ").replaceAll("-"," ").replace(/^./,c=>c.toUpperCase());
const label = m => names[m] || title(m);
let catalog={runs:[],collections:[],warnings:[]}, visible=[], selected=null, detail=null, trace=[], eventRows=[];
let playing=false, frame=0, lastFrame=0, generation=0, currentTime=0, lastActive=-1;
let steps=[], currentOrder=-1, holdElapsed=0;
const situationNames={normal:"Healthy operation — no failure", "holder-failure":"A stops while holding the token", "before-entry":"A fails before lane entry", "during-occupancy":"A fails inside the lane", "no-majority":"All cranes disconnected — no majority", "majority-before-entry":"A disconnected before entry", "majority-during-occupancy":"A disconnected inside the lane"};
situationNames["controller-outage"]="Central controller stops during traffic";
const situation=s=>situationNames[s]||title(s);
const familyNames={comparison:"Compare the three systems",agents:"Normal token handoff",logical:"Generated lane traffic",recovery:"Crash and majority recovery",partition:"Lost communication / network split",restart:"Restart and safe rejoin",other:"Other saved experiments"};
function describe(r){
  const s=r?.summary||{},f=r?.family||"other",scenario=r?.scenario||"normal";
  if(f==="comparison"&&scenario==="controller-outage")return {what:"The reference controller service is stopped one-third through a steady stream of identical requests. All three cranes and the gate stay running.",expected:"Central lock loses its coordinator. CraneLock starts centrally, detects missing controller heartbeats, obtains real majority votes and installs LOCAL authority. Basic token is already local and needs no switch. Existing occupancy must remain protected."};
  if(f==="comparison")return {
    what:scenario==="holder-failure"?"Crane A starts with the token, then its software is stopped before entry. The same requests from B and C are sent to each system.":"The same requests are sent to three systems while all cranes are healthy. This measures how they coordinate access to one shared lane.",
    expected:scenario==="holder-failure"?"Basic token should stall because it cannot replace a lost token. CraneLock should recover with B and C's approval. The central reference can also reassign because its coordinator remains available.":"Each system should let one authorised crane enter at a time and complete the requests. A recovery advantage is not expected when nothing has failed."};
  if(f==="restart")return {what:"A stops while holding the token. B and C recover first. A is then restarted using its saved, out-of-date state.",expected:"Restarted A must remain blocked. It may rejoin only after reading accepted gate authority, and must receive a fresh token before entering."};
  if(f==="partition"&&s.fault==="no-majority")return {what:"All three cranes keep running, but each is disconnected from the others. No group can reach two of the three members.",expected:"No replacement authority should be installed. Refusing recovery is the safe result here. A's previously valid authority is not automatically revoked."};
  if(f==="partition")return {what:`A stays running but loses peer communication with B and C${scenario.includes("occupancy")?" while physically occupying the lane":" before entry"}. B and C can still communicate.`,expected:"B and C can approve a new holder. The gate must reject A's old permissions. If A occupies the lane, the new holder must wait for confirmed physical clearance."};
  if(f==="recovery")return {what:`A's agent service is stopped${scenario.includes("occupancy")?" while A occupies the lane":" before it enters the lane"}. B and C detect missing heartbeats and attempt recovery.`,expected:"B and C must approve the replacement together. Old permissions must be rejected. Recovery alone must not clear an occupied lane."};
  if(f==="agents")return {what:"Three real software agents exchange a token over local gRPC connections. B and C request access while A uses the lane.",expected:"A must exit before the token passes to B, then C. Only the authorised holder may enter, and the physical movements must never overlap."};
  if(f==="logical")return {what:"Simulated requests arrive at a shared lane. A simple first-come queue sends each request through the actual gate code.",expected:"Only one crane may occupy the lane. This workload measures simulated queue waiting, not the speed of the full distributed protocol."};
  return {what:"This saved experiment records crane coordination and lane-entry checks.",expected:"Read the saved configuration and evidence to establish the exact success criteria."};
}
function outcome(r){
  const s=r.summary||{};
  if(r.status==="failed")return "The experiment failed. Inspect the failure evidence; do not interpret missing measurements as successful checks.";
  if(r.status==="incomplete")return "This experiment has no completed summary yet. Its outcome is not known.";
  if(s.recovered===false)return "The system correctly refused replacement recovery because no majority could communicate. Zero replacement crossings is expected here, not a recovery success.";
  if(s.resumed_within_window===false)return `${label(r.model)} did not resume lane access during the observation window. ${finite(unfinished(r))?`${unfinished(r)} requests remained unfinished. `:""}A missing recovery time means no safe entry was observed, not an instant recovery.`;
  const finished=completed(r);
  if(r.family==="restart")return `The recorded checks passed. The replacement completed ${s.completed_replacement_crossings??"its"} crossing(s), and restarted A completed ${s.completed_rejoined_crossings??"its"} fresh crossing(s) after reconciliation and a new token.`;
  return `${finite(finished)?`${finished} lane use${finished===1?"":"s"} completed. `:""}${finite(unfinished(r))&&unfinished(r)>0?`${unfinished(r)} requests were still unfinished when the observation ended. `:""}${s.safety_violations===0?"No safety violations were detected in this run. This is a bounded check, not a proof for all possible failures.":"Review the recorded safety totals below."}`;
}
function populateCollections(previous){
  let choices=catalog.collections;
  if(!$("all-collections").checked){
    const families=[...new Set(choices.map(c=>c.family))];
    choices=families.map(f=>choices.find(c=>c.family===f&&c.id==="benefits-completion-validated")||choices.find(c=>c.family===f&&c.id==="benefits-validated")||choices.find(c=>c.family===f&&c.id.includes("validated"))||choices.find(c=>c.family===f&&!c.id.includes("development"))||choices.find(c=>c.family===f));
  }
  const preferred=choices.find(c=>c.id==="benefits-completion-validated")||choices.find(c=>c.id==="benefits-validated")||choices.find(c=>c.id==="comparison-validated")||choices.find(c=>c.family==="comparison")||choices[0];
  setOptions($("collection"),choices.map(c=>[c.id,`${familyNames[c.family]||"Saved results"} · ${c.count} runs${$("all-collections").checked?` · ${c.id}`:""}`]),choices.some(c=>c.id===previous)?previous:preferred?.id);
}
function renderExplanation(){
  const first=visible[0];
  if(!first){$("experiment-title").textContent="No saved experiment selected.";$("test-explanation").textContent="Run an experiment and refresh the results, then choose a test set.";$("expected-explanation").textContent="Saved results will appear here; no example numbers are substituted.";$("result-explanation").replaceChildren();return;}
  const description=describe(first);
  $("experiment-title").textContent=familyNames[first.family]||"Understand this experiment";
  const allScenarios=[...new Set(visible.map(r=>r.scenario))];
  $("test-explanation").textContent=first.family==="comparison"&&allScenarios.length>1?"We send the same requests to a central reference, basic token and CraneLock. One situation has no failure; the other stops the token holder before entry. Each request sequence is repeated for all three systems.":description.what;
  $("expected-explanation").textContent=first.family==="comparison"&&allScenarios.length>1?"All three should progress normally. After holder failure, basic token should stall, while CraneLock and the still-running central coordinator can restore progress. Keeping an unsafe request blocked can also be a passed check.":description.expected;
  if(first.family==="comparison"&&allScenarios.length>1&&allScenarios.includes("controller-outage")){
    $("test-explanation").textContent="The same requests test healthy traffic, a stopped controller during traffic, and a stopped token holder before entry. Each demand level and request sequence is repeated for the three systems.";
    $("expected-explanation").textContent="Controller loss should stop the central reference while CraneLock switches locally; basic token is already local. Lost-token recovery should help CraneLock versus basic token. Healthy traffic measures the cost of monitoring, with no assumed speed advantage.";
  }
  const result=$("result-explanation");result.replaceChildren();
  if(first.family==="comparison"){
    for(const scenario of allScenarios){const p=node("p");p.append(node("strong","",`${situation(scenario)}: `));const groups=["central-lock","basic-token","cranelock"].map(model=>{const rows=visible.filter(r=>r.model===model&&r.scenario===scenario),valid=rows.filter(r=>finite(completed(r))&&finite(r.summary.requested));return rows.length?valid.length?`${label(model)} ${valid.reduce((n,r)=>n+completed(r),0)}/${valid.reduce((n,r)=>n+r.summary.requested,0)} completed${valid.length<rows.length?" (some results missing)":""}`:`${label(model)}: no completed summary`:null;}).filter(Boolean);p.append(document.createTextNode(groups.join("; ")+"."));result.append(p);}
    result.append(node("p","", visible.some(r=>r.scenario==="controller-outage"&&r.summary.evaluation==="benefits-v1")?"These tests stop an experimental reference controller. The teammate controller is not integrated. See paired benefits and costs below; normal operation alone does not demonstrate a recovery advantage.":"These are recorded local test results. The central coordinator was not stopped; this does not establish how it behaves during controller failure."));
  }else for(const r of visible.slice(0,3)){const p=node("p");p.append(node("strong","",`${situation(r.scenario)}: `),document.createTextNode(outcome(r)));result.append(p);}
}
function syncTraceChoices(preferred){
  const target=visible.find(r=>r.id===preferred)||visible.find(r=>r.model==="cranelock"&&r.scenario==="controller-outage"&&r.seed===20&&r.summary.requested===6)||visible.find(r=>r.model==="cranelock"&&r.scenario==="controller-outage")||visible.find(r=>r.model==="cranelock"&&r.scenario==="holder-failure"&&r.seed===20)||visible.find(r=>r.model==="cranelock"&&r.scenario==="holder-failure")||visible.find(r=>r.model==="cranelock")||visible[0];
  setOptions($("trace-model"),[...new Set(visible.map(r=>r.model))].map(m=>[m,label(m)+(m==="central-lock"?" · reference":"")]),target?.model);
  const models=visible.filter(r=>r.model===$("trace-model").value);
  setOptions($("trace-scenario"),[...new Set(models.map(r=>r.scenario))].map(s=>[s,situation(s)]),target?.scenario);
  const choices=models.filter(r=>r.scenario===$("trace-scenario").value);
  setOptions($("trace-sequence"),choices.map(r=>[r.id,(r.seed!==null&&r.seed!==undefined?`Request sequence ${r.seed}`:"Main recorded run")+(finite(r.summary.requested)?` · ${r.summary.requested} requests`:"")]),target?.id);
  $("trace-run").value=$("trace-sequence").value;
  return $("trace-sequence").value;
}
function changeTraceChoice(){const target=visible.find(r=>r.model===$("trace-model").value&&r.scenario===$("trace-scenario").value)||visible.find(r=>r.model===$("trace-model").value);const id=syncTraceChoices(target?.id);if(id)selectRun(id);}
const colors={"central-lock":"#d9902f","basic-token":"#8c74d8",cranelock:"#16a18b"};
async function api(path){const r=await fetch(path);const value=await r.json();if(!r.ok)throw Error(value.error||`HTTP ${r.status}`);return value;}
function notice(text){$("message").textContent=text;$("message").classList.toggle("hidden",!text);}
function option(select,value,text){const o=node("option","",text);o.value=value;select.append(o);}
function setOptions(select,items,initial){select.replaceChildren();for(const [value,text]of items)option(select,value,text);if([...select.options].some(o=>o.value===initial))select.value=initial;}
const completed = r => finite(r.summary.completed)?r.summary.completed:
  finite(r.summary.completed_replacement_crossings)?r.summary.completed_replacement_crossings+(r.summary.completed_rejoined_crossings||0):null;
const unfinished = r => finite(r.summary.unfinished_at_horizon)?r.summary.unfinished_at_horizon:
  finite(r.summary.waiting)&&finite(r.summary.occupied)?r.summary.waiting+r.summary.occupied:
  finite(r.summary.requested)&&finite(completed(r))?Math.max(0,r.summary.requested-completed(r)):null;
const valueOf=(r,m)=>m==="completed"?completed(r):r.summary[m];
const median=values=>{const a=[...values].sort((a,b)=>a-b),m=Math.floor(a.length/2);return a.length?(a.length%2?a[m]:(a[m-1]+a[m])/2):null;};
function artifact(id,name){return `/api/artifact?${new URLSearchParams({id,name})}`;}

async function refresh(){
  $("refresh").disabled=true;
  try{
    const old=$("collection").value;
    catalog=await api("/api/catalog");
    populateCollections(old);
    notice(catalog.warnings.length?catalog.warnings.join(" "):"");
    updateFilters();
  }catch(e){notice(`Could not read saved results: ${e.message}`);}finally{$("refresh").disabled=false;}
}
function updateFilters(){
  const rows=catalog.runs.filter(r=>r.collection===$("collection").value);
  const oldScenario=$("scenario").value,oldSeed=$("seed").value;
  const scenarios=[...new Set(rows.map(r=>r.scenario))].sort(),comparison=rows.some(r=>r.family==="comparison");
  const preferred=scenarios.includes("controller-outage")?"controller-outage":scenarios.includes("holder-failure")?"holder-failure":scenarios[0];
  setOptions($("scenario"),[...(!comparison?[["all","All situations"]]:[]),...scenarios.map(s=>[s,situation(s)])],scenarios.includes(oldScenario)?oldScenario:comparison?preferred:"all");
  setOptions($("seed"),[["all","All request sequences"],...[...new Set(rows.map(r=>r.seed).filter(v=>v!==null&&v!==undefined))].sort((a,b)=>a-b).map(v=>[String(v),`Sequence ${v}`])],oldSeed);
  const counts=[...new Set(rows.map(r=>r.summary.requested).filter(finite))].sort((a,b)=>a-b);
  setOptions($("load"),[...(!comparison||!counts.length?[["all","All request counts"]]:[]),...counts.map(v=>[String(v),`${v} requests per test`])],counts.includes(Number($("load").value))?$("load").value:comparison&&counts.length?String(counts[0]):"all");
  filter();
}
function filter(){
  visible=catalog.runs.filter(r=>r.collection===$("collection").value&&($("scenario").value==="all"||r.scenario===$("scenario").value)&&($("seed").value==="all"||String(r.seed)===$("seed").value)&&($("load").value==="all"||String(r.summary.requested)===$("load").value));
  $("source-label").textContent=`${visible.length} saved run${visible.length===1?"":"s"} · ${catalog.collections.find(c=>c.id===$("collection").value)?.family||"no data"}`;
  setOptions($("focus"),[...$("scenario").options].map(o=>[o.value,o.textContent]),$("scenario").value);
  const sample=visible[0],count=Number($("load").value),seconds=sample?.summary.duration_seconds;
  $("focus-context").textContent=sample?.family==="comparison"?`One request means one crane asking to use the shared lane. ${finite(count)&&count>0?`${count} requests are sent to each system`:"Each system receives the same requests"}${finite(seconds)?` during a ${seconds}-second test`:""}. ${$("scenario").value==="controller-outage"?"The controller—the central service that chooses which crane goes next—is stopped during the test.":$("scenario").value==="holder-failure"?"Crane A stops while holding the token: the permission that cranes pass around to decide who may use the lane next.":"All cranes and the controller stay available."}`:"Choose a saved scenario to inspect its explained trace. Performance comparisons need a comparison experiment set.";
  if(sample?.summary.measurement_policy==="complete_all_requests")$("focus-context").textContent=`All ${sample.summary.requested} requests are submitted at the start. Each system runs until every request finishes. We compare the total time to finish the batch and the waiting time before lane entry. There is no five-second completion cutoff.`;
  renderStats();renderBenefits();renderCharts();renderProgress();renderTable();renderExplanation();
  $("empty").classList.toggle("hidden",visible.length>0);
  setOptions($("trace-run"),visible.map(r=>[r.id,`${label(r.model)} / ${title(r.scenario)}${r.seed!==null&&r.seed!==undefined?` / seed ${r.seed}`:""} / ${r.status}`]),selected);
  const id=syncTraceChoices(selected);
  if(visible.length)selectRun(id);else clearRun();
}
function pairDifferences(rows,scenario,baseline,metric){
  const fields=["seed","requested","workload_sha256","duration_seconds","crossing_time_seconds","detection_timeout_seconds","evaluation"];
  const key=r=>JSON.stringify([...fields.map(f=>r.summary[f]),r.summary.measurement_policy||"fixed_window"]);
  const valid=r=>r.status==="passed"&&["benefits-v1","benefits-v2"].includes(r.summary.evaluation)&&typeof r.summary.workload_sha256==="string"&&fields.every(f=>r.summary[f]!==undefined);
  const refs=rows.filter(r=>valid(r)&&r.scenario===scenario&&r.model===baseline);
  return rows.filter(r=>valid(r)&&r.scenario===scenario&&r.model==="cranelock").flatMap(r=>{const matches=refs.filter(other=>key(other)===key(r));if(matches.length!==1)return [];const other=matches[0];return finite(r.summary[metric])&&finite(other.summary[metric])?[r.summary[metric]-other.summary[metric]]:[];});
}
function comparisonRows(scenario){
  return catalog.runs.filter(r=>r.collection===$("collection").value&&r.family==="comparison"&&r.scenario===scenario&&($("load").value==="all"||String(r.summary.requested)===$("load").value)&&($("seed").value==="all"||String(r.seed)===$("seed").value));
}
function typical(rows,model,metric){return median(rows.filter(r=>r.model===model&&r.status==="passed").map(r=>typeof metric==="function"?metric(r):r.summary[metric]).filter(finite));}
function timeToResume(r){
  if(r.summary.resumed_within_window===false)return null;
  if(r.scenario==="controller-outage")return r.model==="basic-token"?null:r.summary.outage_to_entry_seconds;
  const a=r.summary.failure_detection_seconds,b=r.summary.recovery_latency_seconds;
  return finite(a)&&finite(b)?a+b:null;
}
function renderBenefits(){
  const area=$("benefit-cards");area.replaceChildren();
  const first=visible[0],scenario=$("scenario").value,rows=comparisonRows(scenario),healthy=comparisonRows("normal");
  $("benefits").classList.toggle("hidden",first?.family!=="comparison");
  if(first?.family!=="comparison")return;
  $("benefit-title").textContent="Results summary";
  $("benefit-scope").textContent="Read one situation at a time. The same requests go to each system. One completed request means a crane entered the lane and exited. The controller is an experimental reference; the teammate controller is still to be integrated.";
  const card=(heading,text)=>{const a=node("article");a.append(node("h3","",heading),node("p","",text));area.append(a);return a;};
  if(scenario==="normal"&&rows.length&&rows.every(r=>r.summary.measurement_policy==="complete_all_requests")){
    const n=typical(rows,"cranelock","requested"),cl=typical(rows,"cranelock","total_completion_seconds"),central=typical(rows,"central-lock","total_completion_seconds");
    card("Batch completion",`Each system must finish all ${fmt(n)} requests. We measure from submitting the batch until the last request has entered and exited safely. ${finite(cl)&&finite(central)?`The typical total was ${cl.toFixed(3)} seconds for CraneLock and ${central.toFixed(3)} seconds for central lock.`:"Complete batch timings are not available for every model in this selection."} A stalled or failed run is not counted as a fast completion.`);
  }
  if(scenario==="controller-outage"){
    const c=typical(rows,"cranelock","post_fault_new_entries"),b=typical(rows,"central-lock","post_fault_new_entries"),t=typical(rows,"basic-token","post_fault_new_entries");
    const a=card("Controller failure",finite(c)&&finite(b)?`In a typical test, after the controller stopped, CraneLock allowed ${fmt(c)} new requests to start using the lane. Central lock allowed ${fmt(b)} because its coordinator was unavailable. This counts cranes starting work after the failure; it does not count requests that were already running.`:"This set has no matching measurement of new requests after controller failure. Earlier tests that only stopped a crane cannot answer this question.");
    a.append(node("p","","CraneLock lets the cranes agree on replacement permission, then pass the token locally. Cranes already in the lane must still leave safely."));
    if(finite(t))a.append(node("p","",`Basic token also allowed ${fmt(t)} new requests: it already works without a controller. Its weakness appears in the separate test where the token holder stops.`));
  }else if(scenario==="holder-failure"){
    const c=typical(rows,"cranelock","completed"),b=typical(rows,"basic-token","completed"),n=typical(rows,"cranelock","requested");
    const a=card("Token-holder failure",finite(c)&&finite(b)?`Crane A stops before entering, while holding the token—the permission needed to use the lane. In a typical test, CraneLock completes ${fmt(c)} of ${fmt(n)} requests from the other cranes. Basic token completes ${fmt(b)} of ${fmt(n)} because it cannot replace the missing token.`:"This selection has no completed holder-failure comparison.");
    a.append(node("p","","CraneLock requires the remaining cranes to agree before replacing permission. The central reference can also assign another crane while its controller remains available."));
  }
  const comparable=pairDifferences(healthy,"normal","central-lock","mean_wait_seconds").length>0;
  const cl=typical(healthy,"cranelock","mean_wait_seconds"),central=typical(healthy,"central-lock","mean_wait_seconds"),difference=(cl-central)*1000;
  const a=card("Waiting during normal operation",comparable&&finite(cl)&&finite(central)?`When nothing failed, the typical average wait was ${(cl*1000).toFixed(1)} ms with CraneLock and ${(central*1000).toFixed(1)} ms with central lock. That is ${Math.abs(difference).toFixed(2)} ms ${difference>=0?"longer":"shorter"} for the middle values shown in the waiting graph. Waiting means the time from asking for access until the crane is allowed into the lane.`:"Healthy-operation waiting is not measured for these selected requests. A system passing a safety check does not tell us how fast it is.");
  a.append(node("p","",healthy.length&&healthy.every(r=>r.summary.measurement_policy==="complete_all_requests")?"These healthy tests include every request's wait, because the batch runs to completion. Short local runs still vary; this is a reference integration, not a real-port performance benchmark.":"The repeat runs vary, and requests that never entered have no measured waiting time. Use the waiting chart with its unfinished-request notes. These samples do not establish a consistent speed advantage."));
}
function renderStats(){
  const passed=visible.filter(r=>r.status==="passed").length,failed=visible.filter(r=>r.status==="failed").length;
  $("kpi-runs").textContent=`${passed} / ${visible.length}`;
  $("kpi-status").textContent=failed?`${failed} failed; inspect their saved evidence`:visible.length?"Passed checks can include safely refusing entry":"No results selected";
  $("kpi-status").classList.toggle("bad",failed>0);
  const safety=visible.map(r=>r.summary.safety_violations).filter(finite);
  $("kpi-safety").textContent=safety.length?`${safety.reduce((a,b)=>a+b,0)}${safety.length<visible.length?"*":""}`:"—";
  $("kpi-safety").className=safety.length===visible.length&&safety.length&&safety.every(v=>v===0)?"good":safety.some(v=>v>0)?"bad":"";
  $("kpi-safety-note").textContent=safety.length<visible.length?"* Some runs have no recorded safety total":"0 means no violation was detected in these recorded checks";
  const done=visible.map(completed).filter(finite),pending=visible.map(unfinished).filter(finite);
  $("kpi-completed").textContent=done.length?`${done.reduce((a,b)=>a+b,0)}${done.length<visible.length?"+":""}`:"—";
  $("kpi-unfinished").textContent=pending.length?`${pending.reduce((a,b)=>a+b,0)}${pending.length<visible.length?"+":""}`:"—";
  $("kpi-unfinished-note").textContent=pending.length<visible.length?"Some runs do not record request totals":"Requests waiting or still crossing when the test window ended";
}
function renderCharts(){
  const area=$("charts");area.replaceChildren();
  const scenario=$("scenario").value,rows=comparisonRows(scenario),healthy=comparisonRows("normal");
  if(!visible.length||visible.some(r=>r.family!=="comparison")){
    $("comparison-note").textContent="This is a functional scenario. Its explained playback is below. Choose a comparison experiment for performance graphs.";
    $("metric-explanation").textContent="";return;
  }
  const sample=rows[0]?.summary||{},window=sample.duration_seconds;
  const completeAll=scenario==="normal"&&rows.length&&rows.every(r=>r.summary.measurement_policy==="complete_all_requests");
  area.classList.toggle("two-charts",scenario==="normal");
  $("comparison").querySelector(".chart-legend span:last-child").textContent=completeAll?"Every request must finish. Incomplete runs do not get a successful completion time.":"Did not resume is never drawn as zero seconds.";
  $("comparison-title").textContent=completeAll?"Completion time and waiting":"Requests and recovery";
  $("comparison-note").textContent=`Same request count and observation window for each system${finite(window)?`: ${window} seconds per test`:""}. A longer bar means more work on the requests graph, but more delay on the time graphs.`;
  $("metric-explanation").textContent="We show only requests served, time until work resumes, and normal waiting. All systems run for the same fixed time, so total test duration would not be a useful speed comparison.";
  if(completeAll){
    $("comparison-note").textContent="All systems get the same complete batch at the start. They finish every request before the clock stops. Lower times are better; setup and teardown are excluded.";
    $("metric-explanation").textContent="Total completion time measures how long the whole batch takes. Average waiting measures how long an individual request waits before entering the lane. Both include all requests.";
  }else if(healthy.some(r=>r.summary.measurement_policy==="complete_all_requests"))$("metric-explanation").textContent="Failure tests keep a fixed observation window so stalled systems can be compared. The separate healthy batch test runs every request to completion and supplies the waiting-time comparison.";
  function chart(id,heading,explanation,data,metric,kind){
    const group=node("article","chart-group");group.dataset.chart=id;
    group.append(node("h3","chart-title",heading),node("p","chart-explanation",explanation));
    const values=data.filter(r=>r.status==="passed").map(r=>typeof metric==="function"?metric(r):r.summary[metric]).filter(finite);
    const max=Math.max(...values,1e-6);
    for(const model of ["central-lock","basic-token","cranelock"]){
      const selected=data.filter(r=>r.model===model),passed=selected.filter(r=>r.status==="passed");
      const samples=passed.map(r=>typeof metric==="function"?metric(r):r.summary[metric]).filter(finite),mid=median(samples);
      const row=node("div","bar-row"),caption=node("div","bar-caption",label(model));
      const stalled=passed.filter(r=>r.summary.resumed_within_window===false).length;
      const independent=id==="recovery"&&scenario==="controller-outage"&&model==="basic-token"&&passed.length;
      let text=finite(mid)?kind==="ms"?`${(mid*1000).toFixed(1)} ms`:kind==="seconds"?`${mid.toFixed(3)} seconds`:`${fmt(mid)} requests`:independent?"Keeps working":stalled?"Did not resume":"Not measured";
      if(id==="requests"&&scenario==="normal"&&finite(mid)&&finite(window))text=`${fmt(mid)} of ${fmt(typical(data,model,"requested"))} by ${window}s`;
      caption.append(node("strong","",text));row.append(caption);
      if(finite(mid)){
        const track=node("div","bar-track"),fill=node("div","bar-fill");fill.style.width=`${mid/max*100}%`;fill.style.backgroundColor=colors[model];track.append(fill);row.append(track);
      }
      if(independent)row.append(node("p","chart-detail","Basic token keeps serving requests when the controller stops because it does not depend on that controller. There is no controller-failure recovery time to measure. If the crane holding the token stops instead, basic token gets stuck: it cannot replace the missing token. Select the token-holder failure test to see that difference."));
      if(id==="requests"&&scenario==="normal"&&passed.length){
        const recorded=passed.filter(r=>r.completion_timing),totals=recorded.reduce((a,r)=>{const t=r.completion_timing;a.before+=t.finished_by_cutoff;a.late+=t.finished_after_cutoff;a.pending+=t.no_recorded_completion;a.requested+=r.summary.requested;a.times.push(...t.late_finish_seconds);return a;},{before:0,late:0,pending:0,requested:0,times:[]});
        if(recorded.length){
          row.append(node("p","chart-detail cutoff-outcome",`Across ${recorded.length} recorded test(s): ${totals.before} of ${totals.requested} requests finished by the cutoff; ${totals.late} finished afterward; ${totals.pending} had no recorded completion.`));
          if(totals.late)row.append(node("p","chart-detail cutoff-explanation",`For example, one request finished at ${totals.times[0].toFixed(3)} seconds, after the ${window}-second cutoff. It completed successfully, but is excluded from this bar. A late finish is not a failed request.`));
        }
        if(recorded.length<passed.length)row.append(node("p","chart-detail",`Detailed finish times are unavailable for ${passed.length-recorded.length} test(s). Their summaries alone cannot tell us whether an uncounted request finished later.`));
      }
      if(id==="recovery"&&stalled)row.append(node("p","undefined",`${stalled}/${passed.length} test(s) never resumed${finite(window)?` within ${window} seconds`:""}. This is not zero-second recovery.`));
      if(!samples.length&&!stalled&&!independent)row.append(node("p","chart-detail","No suitable saved timing or request measurement. Missing data is not zero."));
      if(id==="waiting"&&passed.length){
        const missing=passed.reduce((n,r)=>n+(unfinished(r)||0),0),requested=passed.reduce((n,r)=>n+(r.summary.requested||0),0);
        row.append(node("p","chart-detail",passed.every(r=>r.summary.measurement_policy==="complete_all_requests")?`All ${requested} requests finished across ${passed.length} healthy test(s); every request's waiting time is included.`:`${missing} of ${requested} requests were unfinished across these healthy tests. Waiting values cover requests that entered only.`));
      }
      if(id==="execution"&&passed.length)row.append(node("p","chart-detail",`${passed.filter(r=>r.summary.completed_all_requests===true).length}/${passed.length} recorded test(s) finished the entire batch. A failed or stalled batch has no successful completion time.`));
      if(selected.some(r=>r.status!=="passed"))row.append(node("p","chart-detail","Some runs failed or are incomplete; they are excluded from the bars."));
      if(samples.length&&samples.length<passed.length&&!independent)row.append(node("p","chart-detail",`${samples.length}/${passed.length} runs have a measured value; the other runs are excluded.`));
      group.append(row);
    }
    area.append(group);return group;
  }
  if(scenario!=="normal"){
    const controller=scenario==="controller-outage";
    chart("requests",controller?"1. Could more requests start after the controller stopped?":"1. How many requests finished after the token holder stopped?",
      controller?"Counts new requests allowed into the lane after controller failure. Requests already running before failure do not count. Higher means more cranes could begin work despite the outage.":"A stops before any crossing. Counts requests from B and C that both entered and exited before the test ended. Higher means more work was completed despite A's failure.",
      rows,controller?"post_fault_new_entries":"completed","requests");
    chart("recovery","2. How long until a waiting crane could move again?",
      "Starts when the failure is injected and ends at the next new lane entry. This includes detecting the failure, agreeing on permission and any wait for a request or a clear lane. Lower means work resumed sooner.",rows,timeToResume,"seconds");
  }else if(completeAll){
    chart("execution",`How long to finish all ${fmt(typical(rows,"cranelock","requested"))} requests?`,"The clock starts when all requests are submitted together and stops after the last crane exits. The number of requests is identical for each system. Lower means the same work finished sooner.",rows,"total_completion_seconds","seconds");
  }else {
    const group=chart("requests",`How many normal requests finished${finite(window)?` within ${window} seconds`:""}?`,"The clock stops counting at the cutoff. A request finishing just afterward is excluded, even though it completes successfully. Bars show the typical result per test; the notes give outcomes across the selected repeats.",rows,"completed","requests");
    group.append(node("p","chart-explanation","Why might a bar show 17 instead of 18? The last request can finish a few milliseconds past the cutoff. The recorded timings above show whether that happened here. This count alone does not establish a consistent speed difference between systems."));
  }
  chart("waiting",scenario==="normal"?"How long did a crane wait for its turn?":"3. What is the waiting time when nothing fails?",
    healthy.some(r=>r.summary.measurement_policy==="complete_all_requests")?"Uses the separate healthy batch test: all requests arrive together and every request finishes. Average the time from asking until actual entry; bars show the middle average across repeats. Lower means less time waiting for permission.":"Uses the separate healthy-traffic test with the same request count and sequences. For each test, average the time from asking to actual entry; bars show the middle average across repeats. Lower is better.",healthy,"mean_wait_seconds","ms");
}
let progressGeneration=0;
function renderProgress(){
  const rows=comparisonRows($("scenario").value).filter(r=>r.status==="passed"),models=["central-lock","basic-token","cranelock"];
  const seeds=[...new Set(rows.map(r=>r.seed))].filter(seed=>models.every(m=>rows.some(r=>r.model===m&&r.seed===seed)));
  const previous=$("progress-seed").value;
  setOptions($("progress-seed"),seeds.map(s=>[String(s),`Sequence ${s}`]),seeds.some(s=>String(s)===previous)?previous:String(seeds.includes(20)?20:seeds[0]));
  $("progress").classList.toggle("hidden",!seeds.length||visible.some(r=>r.family!=="comparison"));
  drawProgress();
}
async function drawProgress(){
  const ticket=++progressGeneration,plot=$("progress-plot"),legend=$("progress-legend");
  plot.replaceChildren();legend.replaceChildren();$("progress-note").textContent="";
  if($("progress").classList.contains("hidden"))return;
  const models=["central-lock","basic-token","cranelock"],rows=comparisonRows($("scenario").value);
  const runs=models.map(m=>rows.find(r=>r.model===m&&String(r.seed)===$("progress-seed").value&&r.status==="passed"));
  $("progress-context").textContent=`One recorded sequence · ${runs[0]?.summary.requested??"?"} requests per system · ${situation($("scenario").value)}`;
  plot.append(node("p","progress-loading","Reading recorded completion times…"));
  try{
    if(runs.some(r=>!r))throw Error("Select a sequence recorded for all three systems.");
    const hashes=runs.map(r=>r.summary.workload_sha256).filter(Boolean);
    if(new Set(hashes).size>1)throw Error("These runs do not share the same request sequence.");
    const data=await Promise.all(runs.map(r=>api(`/api/progress?id=${encodeURIComponent(r.id)}`)));
    if(ticket!==progressGeneration)return;
    plot.replaceChildren();
    if(data.some(d=>!d.available))throw Error("This saved sequence has no complete, consistent set of completion timestamps. No lines are substituted for missing evidence.");
    const width=Math.max(300,Math.min(900,plot.clientWidth)),height=285,left=42,right=20,top=30,bottom=42,horizon=Math.max(...data.map(d=>d.window)),total=Math.max(...runs.map(r=>r.summary.requested),1);
    const x=t=>left+t/horizon*(width-left-right),y=n=>height-bottom-n/total*(height-top-bottom);
    const svg=document.createElementNS("http://www.w3.org/2000/svg","svg");
    svg.setAttribute("viewBox",`0 0 ${width} ${height}`);svg.setAttribute("role","img");svg.setAttribute("aria-label",`Cumulative completed requests, sequence ${$("progress-seed").value}. Exact completion counts are listed below.`);
    const add=(tag,attrs,text,parent=svg)=>{const e=document.createElementNS(svg.namespaceURI,tag);for(const [k,v]of Object.entries(attrs))e.setAttribute(k,v);if(text!==undefined)e.textContent=text;parent.append(e);return e;};
    for(let tick=0;tick<=4;tick++){
      const n=Math.round(total*tick/4),py=y(n);
      add("line",{x1:left,x2:width-right,y1:py,y2:py,class:"plot-grid"});add("text",{x:left-12,y:py+4,"text-anchor":"end",class:"plot-label"},String(n));
      const t=horizon*tick/4;add("text",{x:x(t),y:height-20,"text-anchor":"middle",class:"plot-label"},`${t.toFixed(2)}s`);
    }
    add("text",{x:left,y:15,class:"plot-label"},"Completed requests");
    const faults=data.map(d=>d.fault_at).filter(finite);
    if(faults.length){
      const first=Math.min(...faults),last=Math.max(...faults),px=x(first);
      const marker=add("g",{});add("rect",{x:px,y:top,width:Math.max(2,x(last)-px),height:height-top-bottom,class:"fault-band"},undefined,marker);
      add("line",{x1:px,x2:px,y1:top,y2:height-bottom,class:"fault-marker"},undefined,marker);
      add("title",{},data.map((d,i)=>`${label(models[i])}: ${finite(d.fault_at)?d.fault_at.toFixed(4)+"s":"fault time unavailable"}`).join("; "),marker);
      add("text",{x:Math.min(px+8,width-180),y:top+14,class:"fault-label"},$("scenario").value==="controller-outage"?"Controller stopped":"Token holder stopped");
    }
    data.forEach((d,i)=>{
      let path=`M ${x(0)} ${y(0)}`;
      d.exits.forEach((t,j)=>{path+=` H ${x(t)} V ${y(j+1)}`;});path+=` H ${x(horizon)}`;
      add("path",{d:path,fill:"none",stroke:colors[models[i]],"stroke-width":3,"stroke-dasharray":i===0?"9 5":i===1?"3 4":"none","vector-effect":"non-scaling-stroke"});
      d.exits.forEach((t,j)=>{const dot=add("circle",{cx:x(t),cy:y(j+1),r:3,fill:colors[models[i]],class:"completion-point"});add("title",{},`${label(models[i])}: ${j+1} finished at ${t.toFixed(3)} seconds`,dot);});
      const key=node("span","progress-key"),swatch=node("i");swatch.style.backgroundColor=colors[models[i]];key.append(swatch,document.createTextNode(`${label(models[i])} · ${d.exits.length}/${runs[i].summary.requested} finished`));legend.append(key);
    });
    plot.append(svg);
    const normal=$("scenario").value==="normal",batch=runs.every(r=>r.summary.measurement_policy==="complete_all_requests");
    $("progress-note").textContent=`Each step is one request finishing; a flat line means no further completions during that interval. ${normal?(batch?"All requests arrive at the start. Reaching the top sooner means finishing the same batch faster.":"Requests arrive during the fixed observation window. Completions after the cutoff are excluded."):"Requests continue arriving during the fixed observation window. Use the lines after the failure marker to see whether work continues or stalls."} This is one sequence, not the median bars above. Hover over a dot for its recorded finish time.${faults.length?" The shaded marker spans the recorded failure times across systems.":normal?"":" No failure timestamp is available for this sequence."}`;
  }catch(error){if(ticket===progressGeneration){plot.replaceChildren(node("p","progress-loading",error.message));legend.replaceChildren();}}
}
$("progress-seed").addEventListener("change",drawProgress);
let progressResize;
window.addEventListener("resize",()=>{clearTimeout(progressResize);progressResize=setTimeout(drawProgress,180);});
function renderTable(){
  const body=$("runs");body.replaceChildren();$("run-count").textContent=`${visible.length} runs`;
  for(const r of visible){
    const tr=node("tr",r.id===selected?"selected-row":""),model=node("td");
    model.append(node("strong","",label(r.model)));if(r.model==="central-lock")model.append(node("small","","Reference controller"));else model.append(node("small","",r.collection));tr.append(model);
    for(const text of [title(r.scenario),r.seed??"—"])tr.append(node("td","",text));
    const status=node("td");status.append(node("span",`status ${r.status}`,r.status.toUpperCase()));tr.append(status);
    for(const text of [fmt(completed(r)),fmt(r.summary.p95_wait_seconds," s"),fmt(r.summary.recovery_latency_seconds," s")])tr.append(node("td","",text));
    const action=node("td"),button=node("button","row-open","View explained trace ↗");button.addEventListener("click",()=>{syncTraceChoices(r.id);selectRun(r.id);$("trace").scrollIntoView({behavior:"smooth"});});action.append(button);tr.append(action);body.append(tr);
  }
}
function pause(){playing=false;cancelAnimationFrame(frame);$("play").textContent=$("speed").value==="guided"?"▶ Play guide":"▶ Play trace";$("playback-state").textContent="Paused";holdElapsed=0;}
function clearRun(){generation++;pause();selected=null;detail=null;trace=[];steps=[];eventRows=[];$("events").replaceChildren(node("p","caption","No recorded events selected."));$("downloads").replaceChildren();$("raw-summary").textContent="Select a run.";$("detail-status").textContent="";$("limitations").textContent="";$("run-story").textContent="Choose a saved test to read its explanation.";$("trace-warning").classList.add("hidden");$("play").disabled=true;$("event-count").textContent="0 events";$("scrub").value=0;renderPlayback(0,-1);}
async function selectRun(id){
  pause();const request=++generation;selected=id;$("play").disabled=true;renderTable();
  try{
    const [run,data]=await Promise.all([api(`/api/run?${new URLSearchParams({id})}`),api(`/api/events?${new URLSearchParams({id})}`)]);
    if(request!==generation)return;
    detail=run;trace=data.events.map((e,i)=>({...e,_i:i,_time:finite(e.time)?Math.max(0,e.time):0})).sort((a,b)=>a._time-b._time||a._i-b._i).map((e,i)=>({...e,_order:i}));
    steps=trace.filter(isGuidedEvent);
    $("scrub").max=String(Math.max(...trace.map(e=>e._time),0.001));$("scrub").value=0;
    $("play").disabled=!steps.length;$("event-count").textContent=`${data.total} recorded`;
    const warning=[data.truncated?`Showing first ${trace.length} of ${data.total} events; playback is incomplete.`:"",data.malformed?`${data.malformed} malformed lines were skipped.`:""].filter(Boolean).join(" ");
    $("trace-warning").textContent=warning;$("trace-warning").classList.toggle("hidden",!warning);
    renderEventList();renderEvidence();renderRunStory();renderPlayback(0,-1);
  }catch(e){if(request===generation){clearRun();notice(`Could not open the trace: ${e.message}`);}}
}
function renderRunStory(){
  const r={...catalog.runs.find(r=>r.id===selected),summary:detail.summary||{},status:detail.status};
  const description=describe(r),box=$("run-story");box.replaceChildren(node("h3","",`${label(r.model)} · ${situation(r.scenario)}`));
  const intro=node("p");intro.append(node("strong","","The setup: "),document.createTextNode(description.what));
  const expected=node("p");expected.append(node("strong","","What to watch for: "),document.createTextNode(description.expected));
  const result=node("p");result.append(node("strong","","Recorded outcome: "),document.createTextNode(outcome(r)));box.append(intro,expected,result);
  if(r.seed!==null&&r.seed!==undefined)box.append(node("small","",`Request sequence ${r.seed} reproduces the input requests. It does not identify a different crane or guarantee identical timings.`));
}
const checkStories={
  holder_rpc_unavailable:["A has stopped responding","The stopped service no longer answers RPC calls. This confirms the injected software outage; it does not mean a crane physically left the lane."],
  all_agents_still_running:["The cranes are running, but links are split","This is a communication failure, not a stopped agent. The gate and read-only management queries remain reachable in this test."],
  partition_blocks_token_requests_and_transfers:["The injected split really blocks peer messages","Test requests and token transfers across the separated groups are rejected by the transport. Messages inside a connected group can still pass."],
  nonholders_denied_lane_access:["Cranes without current authority are denied entry","A waiting request or a single vote does not grant lane access. The gate only accepts the current authorised holder."],
  survivors_detected_holder_failure:["B and C detect the missing holder","Their heartbeat timers expired. They suspect A is unavailable, but cannot infer that the physical lane is empty."],
  two_survivors_signed_recovery:["B and C approve replacement authority","Two members form the required majority of three. Their signed certificate can now be submitted to the gate."],
  replacement_authority_installed:["The gate accepts the new authority","The gate records the higher epoch and replacement holder. Old permissions are fenced, but any physical occupant must still be cleared."],
  old_permissions_fenced:["The gate rejects A's old permissions","The old epoch no longer matches the gate's accepted authority. A cannot use its earlier grant to cause another entry."],
  occupied_lane_blocks_replacement:["Recovery succeeds, but B must still wait","A is still physically inside or its clearance is unknown. Digital token recovery does not make the lane empty."],
  physical_clearance_confirmed:["Fresh evidence confirms physical clearance","The trusted simulator reports actual emptiness after observing or explicitly clearing the lane. Silence and token expiry were not treated as clearance."],
  replacement_completed_safe_crossing:["The replacement finishes a lane use","The replacement entered with current permission and then exited. Progress has resumed without a detected overlap."],
  isolated_members_cannot_form_quorum:["No group can approve recovery","Each isolated member has only its own vote, short of the required two. Refusing to create a replacement is the expected safe outcome."],
  gate_rejects_minority_certificate:["One signature is not enough","The gate independently rejects a certificate that lacks a strict majority. An isolated member cannot appoint a replacement alone."],
  authority_unchanged_without_quorum:["Original authority remains unchanged","No replacement was installed. A's existing authority is not automatically revoked just because communication is lost."],
  isolated_live_holder_fenced_and_cannot_recover:["A is alive, but its old token is fenced","A still claims the earlier token. The gate rejects its stale requests and refuses a replacement certificate with only A's signature."],
  restart_loads_old_state_but_blocks_ownership:["A restarts in blocked rejoining mode","Saved state says A once held the token. Restart deliberately clears usable ownership so that old state cannot reactivate A."],
  rejoining_blocks_requests_transfers_and_votes:["A cannot participate while rejoining","Token operations and recovery voting are rejected until A has reconciled from trusted, accepted authority."],
  heartbeats_and_old_permissions_cannot_reactivate_agent:["Hearing peers does not grant ownership","Newer heartbeat information alone cannot unblock A. The gate also continues to reject its old permissions."],
  gate_reconciliation_rejoins_without_granting_token:["A learns the accepted state, but does not own the token","The adapter reads current authority from the gate. A rejoins at the current epoch while B remains the holder."],
  rejoined_agent_completes_new_authorized_crossing:["A completes a fresh, authorised crossing","Only after requesting and receiving a new token, matching gate approval and a fresh grant can restarted A enter and exit."],
};
function isGuidedEvent(e){
  if(e.event==="agent_snapshot")return /restarted_|after_recovery/.test(e.stage||"");
  if(e.event!=="agent_event")return !["peer_rpc_blocked"].includes(e.event);
  return ["CONTROLLER_SUSPECTED","CONTROLLER_RESTORED","TOKEN_REQUEST_SENT","TOKEN_TRANSFER_STARTED","TOKEN_TRANSFER_RECEIVED","TOKEN_HOLDER_SUSPECTED","RECOVERY_VOTE_CAST","RECOVERY_CERTIFICATE_FORMED","RECOVERY_NO_QUORUM","MODE_CHANGED"].includes(e.agent_event?.event);
}
function explainEvent(e){
  const a=e.agent_event||{},who=title(e.crane||a.agent_id||"the crane");
  if(e.event==="check_passed"&&checkStories[e.check])return checkStories[e.check];
  const stories={
    workload_started:[`Traffic starts in ${e.mode} mode`,detail?.summary?.measurement_policy==="complete_all_requests"?"All requests are submitted at the start. The clock runs until every request has entered and exited. This healthy test has no five-second completion cutoff.":"Requests continue throughout the observation window, including after any injected fault. Each model receives the same scheduled requests."],
    central_assignment:["The reference controller assigns lane authority",`${who} receives central authority through the running controller service. Entry still requires gate validation.`],
    controller_stopped:["The central controller service stops",detail?.experiment.model==="basic-token"?"The central service stops, but basic token already passes permission between the cranes without that service. All three cranes remain running and can continue using the token.":detail?.experiment.model==="central-lock"?"The central service stops while all three cranes remain running. This central-lock reference cannot assign new work without its controller.":"The central service stops while all three cranes remain running. CraneLock must detect the outage and obtain majority approval before switching to local token coordination."],
    controller_rpc_unavailable:["The stopped controller no longer answers","A real gRPC heartbeat attempt failed after stopping the reference service. No controller assignment is fabricated by the test."],
    controller_fallback_installed:["CraneLock switches to LOCAL coordination",`The gate accepted a real majority certificate at epoch ${e.epoch}. The agents reconciled from accepted gate authority. ${e.occupant?"An existing occupant remains protected; the switch does not clear the lane.":"New work can now use real token handoffs."}`],
    stale_central_authority_rejected:["Old central authority is rejected",`The gate denied the saved old central certificate (${e.reason}). Authority cannot roll back to the pre-outage version.`],
    cluster_started:["The agent services are ready","Three software crane agents are serving gRPC requests locally. The test can now observe requests, token handoffs and lane entry."],
    request:[`${who} requests lane access`,"A request starts its waiting time. It is not permission to move; the current authority and gate still decide entry."],
    grant_issued:["The gate issues a lane grant","This grant belongs to the current authority. The simulator must still validate it at the physical entry boundary before moving the crane."],
    physical_entry:[`${who} enters the shared lane`,"The gate accepted this entry and the simulator applied the movement. Until this crane exits or is explicitly cleared, another crane must not occupy the lane."],
    physical_exit:[`${who} exits the shared lane`,"The simulator records the physical exit and reports fresh clearance. A later authorised crane can now attempt access."],
    holder_stopped:[`${who}'s software stops`,"Heartbeat and RPC service activity stops. The surviving agents must detect the outage before attempting replacement recovery."],
    holder_isolated:[`${who} loses peer communication`,"The agent keeps running on its side of the split. It may still believe it owns the token, which is why old permissions must be fenced."],
    network_partitioned:["Communication splits into separate groups",`The recorded groups are ${(e.groups||[]).map(g=>g.map(title).join(" + ")).join(" | ")}. Only a group with a strict majority can approve replacement authority.`],
    gate_handoff:[`The gate accepts ${title(e.receiver)} as holder`,"The actual token transfer has completed, and the adapter mirrors that change at the gate. The next entry still needs a fresh grant."],
    agent_restarted:[`${title(e.agent_id)} restarts from saved state`,"This is a new agent instance using the old state file. It must rejoin safely rather than reuse its saved token claim."],
    trusted_simulator_removal:["The test explicitly clears the previous crane","This is an intentional physical-removal fixture. Recovery did not remove the crane automatically."],
    recovery_disabled:["Basic token has no holder-failure recovery","The test does not manufacture a replacement token. Pending requests remain unfinished during the observation window."],
    entry_denied:["The gate denies entry",`Reason: ${e.reason||"recorded denial"}. No physical movement is applied for this attempt.`],
    entry_uncertain:["Entry permission is uncertain","The response was unavailable or lost. The simulator makes no movement and requires reconciliation rather than assuming success."],
    experiment_failed:["The experiment stops on a failed check",e.detail||"Read the retained failure evidence; this run must not be treated as successful."],
    agent_snapshot:["A recorded state check",e.stage?.includes("restarted")?"This snapshot checks whether restarted A is blocked or safely reconciled. It does not itself grant permission.":"The test reads the agents' reported ownership and compares it with accepted authority."],
  };
  if(stories[e.event])return stories[e.event];
  const rawStories={
    CONTROLLER_SUSPECTED:[`${title(a.agent_id)} detects missing controller heartbeats`,"The configured timeout expired. This is suspicion, not permission: a strict majority must sign and the gate must accept the replacement before local entry."],
    CONTROLLER_RESTORED:[`${title(a.agent_id)} hears the controller again`,"A heartbeat alone does not roll local authority back to central mode."],
    AGENT_STARTED:[`${title(a.agent_id)} starts its agent service`,a.mode==="REJOINING"?"Saved state was found, so the agent starts blocked in rejoining mode. Old token ownership is not restored.":"The service is ready for protocol messages. Physical lane access still requires current authority and gate approval."],
    PEER_SUSPECTED:[`${title(a.agent_id)} can no longer hear ${title(a.peer_id)}`,"The peer's heartbeat timeout expired. This may indicate a crash or a communication problem; it does not prove physical clearance."],
    TOKEN_REQUEST_RECEIVED:[`${title(a.agent_id)} receives ${title(a.peer_id)}'s request`,"The current holder receives an access request from another agent. Receiving the request does not immediately transfer the token or allow entry."],
    TOKEN_QUEUED:[`${title(a.peer_id)} joins the token waiting list`,"The holder records the requesting agent in its queue. The request waits until the current holder can safely hand off."],
    TOKEN_HOLDER_CHANGED:[`${title(a.agent_id)} records ${title(a.holder)} as holder`,"This updates the agent's view of token ownership. An announcement alone never gives local permission to enter the physical lane."],
    RECOVERY_REJECTED:["The agent rejects a recovery proposal",`The recorded reason is ${a.reason||"a failed protocol check"}. No replacement certificate is formed by this rejected attempt.`],
    TOKEN_REQUEST_SENT:[`${title(a.agent_id)} asks for the token`,"The request is sent to the currently known holder over gRPC. Waiting for a token is different from waiting for physical clearance."],
    TOKEN_TRANSFER_STARTED:["The sender gives up token ownership","The sender persists relinquishment before sending the token. There can briefly be no usable owner; it must not create two owners."],
    TOKEN_TRANSFER_RECEIVED:[`${title(a.agent_id)} receives the token`,"The real agent accepted the transfer. It can request gate permission, but must still wait for a physically clear lane."],
    TOKEN_HOLDER_SUSPECTED:[`${title(a.agent_id)} suspects the holder is unavailable`,"A heartbeat timeout signals possible failure or disconnection. It cannot distinguish the two or prove that the lane is empty."],
    RECOVERY_VOTE_CAST:[`${title(a.agent_id)} signs a recovery vote`,"The vote is recorded durably before it is returned. A strict majority must approve the same replacement proposal."],
    RECOVERY_CERTIFICATE_FORMED:["A majority certificate has been collected",`${a.votes??"Enough"} votes approved the proposal. The gate must verify and install this certificate before replacement authority is usable.`],
    RECOVERY_NO_QUORUM:["Recovery cannot gather enough votes",`${a.votes??"Too few"} votes were available; ${a.required??"a strict majority"} are required. No replacement certificate is issued.`],
    MODE_CHANGED:[`${title(a.agent_id)} changes coordination mode`,a.mode==="REJOINING"?"The agent is blocked while rejoining. Saved ownership cannot be reused.":"The agent changes mode after trusted reconciliation. This mode change does not replace the need for token ownership and gate approval."],
  };
  return rawStories[a.event]||[title(eventInfo(e).kind),eventInfo(e).subject||"This event was recorded by the experiment. Open the raw evidence for its full fields."];
}
function renderMoment(e){
  if(!e){$("moment-step").textContent=`${steps.length} events in guided playback`;$("moment-title").textContent="Playback ready";$("moment-explanation").textContent="Use Next event to see what happened and why. Play guide holds each event for four seconds, even if the original test finished in milliseconds.";return;}
  const index=steps.findIndex(s=>s._order===e._order),text=explainEvent(e);
  $("moment-step").textContent=index>=0?`Event ${index+1} of ${steps.length} · ${e._time.toFixed(3)} s`:`Recorded at ${e._time.toFixed(3)} s`;
  $("moment-title").textContent=text[0];$("moment-explanation").textContent=text[1];
}
function stepEvent(direction){
  const cursor=currentOrder===null?trace.findLastIndex(e=>e._time<=currentTime):currentOrder;
  const next=direction>0?steps.find(e=>e._order>cursor):steps.findLast(e=>e._order<cursor);
  if(!next)return false;$("scrub").value=String(next._time);renderPlayback(next._time,next._order);return true;
}
function renderEvidence(){
  if(!detail)return;
  $("detail-status").replaceChildren(node("span",`status ${detail.status}`,detail.status.toUpperCase()),document.createTextNode(`  ${detail.id}`));
  const scope=detail.summary?.limitations||detail.experiment.limitations||"Functional scenario evidence. Review the saved configuration and trace for its scope.";
  const outcome=detail.summary?.recovered===false?"Expected safe refusal: no replacement authority was installed because no majority could communicate. ":detail.summary?.resumed_within_window===false?"No progress resumed within the observation window. Undefined recovery time does not mean zero recovery time. ":"";
  $("limitations").textContent=detail.failure?`${detail.failure.error}: ${detail.failure.detail}`:outcome+scope;
  $("raw-summary").textContent=JSON.stringify(detail.summary||detail.failure||detail.experiment,null,2);
  const downloads=$("downloads");downloads.replaceChildren();
  const collection=catalog.collections.find(c=>c.id===$("collection").value);
  const add=(id,name,text)=>{const a=node("a","",`↓ ${text||name}`);a.href=artifact(id,name);a.download=name;downloads.append(a);};
  if(collection)for(const name of collection.artifacts.filter(n=>n==="comparison.csv"||n.endsWith(".png")||n.endsWith(".svg")))add(collection.id,name,`Set · ${name}`);
  for(const name of detail.artifacts)add(detail.id,name);
}
function eventInfo(e){
  if(e.event==="agent_event")return{kind:e.agent_event?.event||"Agent event",subject:`${e.agent_event?.agent_id||"Agent"}${e.agent_event?.peer_id?` → ${e.agent_event.peer_id}`:""}`,raw:e.agent_event};
  return{kind:e.event||"Event",subject:e.check?title(e.check):[e.crane,e.reason,e.sender&&e.receiver?`${e.sender} → ${e.receiver}`:"",e.detail].filter(Boolean).join(" · "),raw:e};
}
function technical(e){const k=eventInfo(e).kind;return k.startsWith("HEARTBEAT")||k==="CONTROLLER_HEARTBEAT_RECEIVED"||k==="peer_rpc_blocked"||k==="agent_snapshot";}
function renderEventList(){
  const list=$("events");list.replaceChildren();eventRows=[];
  const chosen=trace.filter(e=>$("technical").checked||!technical(e));
  for(const e of chosen.slice(0,500)){
    const info=eventInfo(e),row=node("div","event-item"),time=node("time","",`${e._time.toFixed(3)} s`),body=node("div");
    const explanation=explainEvent(e);
    body.append(node("strong","",technical(e)?title(info.kind):explanation[0]),node("small","",technical(e)?info.subject||"Recorded event":explanation[1]));
    row.append(time,body);row.title=JSON.stringify(info.raw);list.append(row);eventRows.push({element:row,event:e});
  }
  if(chosen.length>500)list.append(node("p","caption",`Showing 500 of ${chosen.length} matching events. Download events.jsonl for the full trace.`));
  if(!chosen.length)list.append(node("p","caption","No matching recorded events."));lastActive=-1;
}
const craneSvg='<svg viewBox="0 0 90 66" fill="none" stroke="currentColor" stroke-width="2" aria-hidden="true"><path d="M14 57V17h53v40M14 17L8 10h67M22 17v33m37-33v33M14 23h53M22 35h37M44 12v27l5 6h-9l4-6"/><path d="M9 57h20m25 0h19M19 23l8 12m32-12-9 12"/><rect x="30" y="46" width="22" height="12" rx="1"/><path d="M35 48v8m5-8v8m5-8v8"/><circle cx="17" cy="61" r="3"/><circle cx="65" cy="61" r="3"/></svg>';
function renderPlayback(time,order=null){
  currentTime=time;currentOrder=order;$("clock").textContent=`${time.toFixed(3)} s`;
  let occupant=null,owner=null,epoch=null,partition=false,controllerStopped=false,coordinationMode=null,trafficStarted=false,controllerDetected=false,stopped=new Set(),agentStates={};
  for(const e of trace){if(e._time>time||(order!==null&&e._order>order))break;
    if(e.event==="physical_entry")occupant=e.crane;
    if(e.event==="physical_exit"&&occupant===e.crane)occupant=null;
    if(e.event==="holder_stopped")stopped.add(e.crane);
    if(e.event==="agent_restarted")stopped.delete(e.agent_id);
    if(e.event==="network_partitioned")partition=true;
    if(e.event==="workload_started"){coordinationMode=e.mode;trafficStarted=true;}
    if(e.event==="controller_stopped")controllerStopped=true;
    if(e.event==="agent_event"&&e.agent_event?.event==="CONTROLLER_SUSPECTED")controllerDetected=true;
    if(e.event==="grant_issued"){owner=e.crane;epoch=e.epoch;}
    if(e.event==="controller_fallback_installed"){owner=e.holder;epoch=e.epoch;coordinationMode="LOCAL";}
    if(e.event==="agent_snapshot"){owner=e.owner;epoch=e.authority_epoch??Object.values(e.agents||{})[0]?.epoch??null;agentStates=Object.fromEntries(Object.entries(e.agents||{}).map(([id,state])=>[id,{...state}]));}
    if(e.event==="agent_event"&&["MODE_CHANGED","AGENT_STARTED"].includes(e.agent_event?.event)){agentStates[e.agent_event.agent_id]={...agentStates[e.agent_event.agent_id],mode:e.agent_event.mode};}
    if(e.event==="agent_event"&&e.agent_event?.event==="TOKEN_TRANSFER_STARTED")owner=null;
    if(e.event==="agent_event"&&e.agent_event?.event==="TOKEN_TRANSFER_RECEIVED"){owner=e.agent_event.agent_id;epoch=e.agent_event.epoch;}
  }
  const hasController=trace.some(e=>e.event==="controller_stopped"||e.event==="central_assignment"||(e.event==="workload_started"&&e.mode==="CENTRAL"));
  const console=$("controller-console"),modeStatus=$("coordination-status"),model=detail?.experiment.model;
  console.classList.toggle("hidden",!hasController);modeStatus.classList.toggle("hidden",!hasController);
  $("jump-controller-stop").classList.toggle("hidden",!trace.some(e=>e.event==="controller_stopped"));
  console.dataset.state=controllerStopped?"stopped":trafficStarted?"running":"pending";
  const setText=(id,value)=>{if($(id).textContent!==value)$(id).textContent=value;};
  setText("controller-status",controllerStopped?"STOPPED — service offline":trafficStarted?"RUNNING":"Before recorded traffic");
  setText("controller-explanation",controllerStopped?"The controller service is stopped. All three crane services remain separate and keep running.":trafficStarted?coordinationMode==="LOCAL"?"This run already uses a local token; the controller does not choose its next crane.":"The central service chooses which crane may request lane access next.":"Play the trace or use Next event to begin. The controller will change state at the recorded stop event.");
  let modeMessage="Playback has not reached the start of traffic.";
  if(trafficStarted)modeMessage=coordinationMode==="LOCAL"?"Local coordination: cranes pass the token between themselves.":"Central coordination: the controller assigns lane access.";
  if(controllerStopped&&coordinationMode!=="LOCAL")modeMessage=model==="central-lock"?"New requests blocked: no controller is available to assign access.":controllerDetected?"Outage detected: waiting for majority-approved local authority.":"Controller connection lost: waiting for the cranes to detect the outage.";
  if(controllerStopped&&coordinationMode==="LOCAL")modeMessage=model==="basic-token"?"Basic token keeps working locally; it does not depend on the stopped controller.":"LOCAL MODE: cranes can pass the token again. The controller remains stopped.";
  setText("coordination-status",modeMessage);
  modeStatus.dataset.mode=controllerStopped&&coordinationMode!=="LOCAL"?"blocked":coordinationMode||"pending";
  const cranes=$("cranes");cranes.replaceChildren();
  for(const id of ["crane-1","crane-2","crane-3"]){const n=node("div",`crane${owner===id?" active":""}${stopped.has(id)?" stopped":""}`);const icon=node("div");icon.innerHTML=craneSvg;const state=agentStates[id];n.append(icon,node("span","",title(id)),node("small","",stopped.has(id)?"Service stopped":occupant===id?"Inside lane":state?.mode==="REJOINING"?"Rejoining · blocked":state?.has_token&&epoch!==null&&state.epoch!==epoch?"Stale token · fenced":owner===id?detail?.experiment.model==="central-lock"?"Gate-authorised crane":"Recorded token owner":"Waiting / idle"));cranes.append(n);}
  $("occupant").textContent=occupant?`${title(occupant)} · occupying lane`:"No recorded occupant";
  $("occupant").classList.toggle("in-lane",Boolean(occupant));$("epoch").textContent=`AUTHORITY VERSION ${epoch??"—"}`;
  $("holder").textContent=owner?`Recorded holder: ${title(owner)}`:"No holder recorded yet";
  $("network").textContent=(partition?"Peer network: partition recorded":"Peer network: no partition recorded")+(controllerStopped?` · Controller stopped · ${coordinationMode||"unknown"} mode`:"");
  const cursor=order===null?trace.findLastIndex(e=>e._time<=time):order;
  renderMoment(steps.findLast(e=>e._order<=cursor));
  $("previous-event").disabled=!steps.some(e=>e._order<cursor);$("next-event").disabled=!steps.some(e=>e._order>cursor);$("reset-trace").disabled=!trace.length;
  let active=-1;eventRows.forEach(({element,event},index)=>{const done=event._time<=time&&event._order<=cursor;element.classList.toggle("done",done);element.classList.toggle("future",!done);if(done)active=index;});
  if(playing&&active!==lastActive&&active>=0){const row=eventRows[active].element;$("events").scrollTop=Math.max(0,row.offsetTop-$("events").offsetTop-90);}lastActive=active;
}
function tick(now){
  if(!playing)return;const elapsed=(now-lastFrame)/1000;lastFrame=now;
  if($("speed").value==="guided"){
    holdElapsed+=elapsed;if(holdElapsed>=4){holdElapsed=0;if(!stepEvent(1)){pause();$("playback-state").textContent="Trace complete";return;}}
  }else{
    const end=Number($("scrub").max),next=Math.min(end,currentTime+elapsed*Number($("speed").value));$("scrub").value=String(next);renderPlayback(next);
    if(next>=end){pause();$("playback-state").textContent="Trace complete";return;}
  }frame=requestAnimationFrame(tick);
}
$("refresh").addEventListener("click",refresh);$("collection").addEventListener("change",updateFilters);$("scenario").addEventListener("change",filter);$("seed").addEventListener("change",filter);$("trace-run").addEventListener("change",()=>selectRun($("trace-run").value));
$("all-collections").addEventListener("change",()=>{populateCollections($("collection").value);updateFilters();});
$("load").addEventListener("change",filter);
$("trace-model").addEventListener("change",changeTraceChoice);$("trace-scenario").addEventListener("change",changeTraceChoice);
$("trace-sequence").addEventListener("change",()=>{$("trace-run").value=$("trace-sequence").value;selectRun($("trace-sequence").value);});
$("trace-run").addEventListener("change",()=>syncTraceChoices($("trace-run").value));
$("technical").addEventListener("change",()=>{renderEventList();renderPlayback(currentTime,currentOrder);});
$("scrub").addEventListener("input",()=>{pause();renderPlayback(Number($("scrub").value));});
$("previous-event").addEventListener("click",()=>{pause();stepEvent(-1);});$("next-event").addEventListener("click",()=>{pause();stepEvent(1);});
$("reset-trace").addEventListener("click",()=>{pause();$("scrub").value=0;renderPlayback(0,-1);});$("speed").addEventListener("change",pause);
$("jump-controller-stop").addEventListener("click",()=>{const event=trace.find(e=>e.event==="controller_stopped");if(!event)return;pause();$("scrub").value=String(event._time);renderPlayback(event._time,event._order);});
$("play").addEventListener("click",()=>{if(playing){pause();return;}if(!steps.length)return;if(currentTime>=Number($("scrub").max)||(currentOrder!==null&&currentOrder>=steps.at(-1)._order)){$("scrub").value=0;renderPlayback(0,-1);}if($("speed").value==="guided"&&currentOrder===-1)stepEvent(1);playing=true;holdElapsed=0;$("play").textContent="Ⅱ Pause";$("playback-state").textContent=$("speed").value==="guided"?"Guided · 4 sec / event":"Playing saved trace";lastFrame=performance.now();frame=requestAnimationFrame(tick);});
refresh();

$("focus").addEventListener("change",()=>{$("scenario").value=$("focus").value;filter();});
function markNavigation(){
  const hash=location.hash||"#overview";
  for(const link of document.querySelectorAll(".nav")){
    const active=link.getAttribute("href")===hash;
    link.classList.toggle("active",active);
    if(active)link.setAttribute("aria-current","location");else link.removeAttribute("aria-current");
  }
}
window.addEventListener("hashchange",markNavigation);
markNavigation();
