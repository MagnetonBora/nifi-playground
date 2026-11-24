#!/usr/bin/env python3
"""Lab 9: performance test of the Lab 6 pipeline, scenarios A-D, text + HTML report.

Uses the group "Lab 9 - Performance test" that is already on the canvas (import it first:
make import f=nifi/flows/lab9-performance.json). The scenarios are cumulative, one change each:

  A  baseline: the ValidateRecord Reader infers the schema of every FlowFile (reader-inferred-schema)
  B  + explicit schema: ValidateRecord Reader reader-input-schema
  C  + Concurrent Tasks = 4 at the bottleneck (the processor that was busiest in B)
  D  + larger MergeRecord batches (Minimum 100,000 / Maximum 200,000 records)

Every run measures the same fixed workload: the back pressure threshold of the connection GenerateRecord ->
ValidateRecord (200 FlowFiles in the reference flow) x 10,000 records = 2,000,000 records.
  1. stop the group, empty its queues, reset the lab9.* counters, apply the scenario settings
  2. load: only GenerateRecord runs until the back pressure of its connection stops it
  3. measure: start every other processor, wait until lab9.records.out + lab9.records.invalid = the workload
During the measurement it samples every 0.5 s the active threads of every processor (busy = average
threads), the queues, and at the start / end the JVM GC time (/system-diagnostics) and the CPU time of the
nifi container (cgroup cpu.stat via docker exec). Each scenario runs REPEATS times; the median is reported.

Disk: every run writes the workload four times (~1.9 MB per FlowFile: GenerateRecord, ValidateRecord, UpdateRecord and
MergeRecord each write a new copy of the content, ~1.5 GB per run); NiFi keeps the replaced content
in the content repository archive (up to nifi.content.repository.archive.max.usage.percentage of the disk). Before
the start the free space (the smaller of the content repository and the host disk) must cover all runs plus MIN_FREE_GB (default 10); before every
run at least MIN_FREE_GB must be free, otherwise the measurement stops (settings are restored as usual).

At the end the group's original settings are restored and the group is left stopped with empty queues.
The report goes to labs/lab9-measure-<time>.txt and .html (repository root).

  make lab09-measure
  NIFI_PASSWORD=... SCENARIOS=A,B REPEATS=1 python3 nifi/common/lab9-measure.py   # a short run
Env: NIFI_URL (default https://localhost:8443), NIFI_USERNAME (admin), NIFI_PASSWORD,
     SCENARIOS (default A,B,C,D), REPEATS (default 3), GROUP (default "Lab 9 - Performance test"),
     NIFI_CONTAINER (default nifi), TIMEOUT (seconds per measurement, default 600), MIN_FREE_GB (default 10)
"""
import html
import json
import os
import shutil
import ssl
import statistics
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
RESULTS = os.path.join(HERE, "..", "..", "labs")
BASE = os.environ.get("NIFI_URL", "https://localhost:8443").rstrip("/") + "/nifi-api"
CTX = ssl.create_default_context(cafile=os.path.join(HERE, "..", "certs", "ca.crt"))
GROUP = os.environ.get("GROUP", "Lab 9 - Performance test")
CONTAINER = os.environ.get("NIFI_CONTAINER", "nifi")
SCENARIOS = [s.strip().upper() for s in os.environ.get("SCENARIOS", "A,B,C,D").split(",") if s.strip()]
REPEATS = int(os.environ.get("REPEATS", 3))
TIMEOUT = int(os.environ.get("TIMEOUT", 600))
MIN_FREE_GB = float(os.environ.get("MIN_FREE_GB", 10))
BYTES_PER_RUN_PER_FLOWFILE = 4 * 1.9e6   # GenerateRecord, ValidateRecord, UpdateRecord and MergeRecord each write a copy
RECORDS_PER_FLOWFILE = 10_000
WORKLOAD_FLOWFILES = RECORDS = None   # read from the flow: back pressure of GenerateRecord -> ValidateRecord
CLIENT = "lab9-measure"
COUNTERS = ("lab9.records.out", "lab9.records.invalid", "lab9.flowfiles.failed")
PIPELINE = ["ValidateRecord", "UpdateRecord", "MergeRecord", "UpdateCounter", "UpdateAttribute", "PutFile"]
TITLES = {"A": "baseline: Infer Schema", "B": "explicit schema", "C": "Concurrent Tasks at the bottleneck",
          "D": "larger MergeRecord batches"}
TOKEN = None


# ---------------------------------------------------------------- NiFi REST API

def call(method, path, body=None, raw=False, headers=None):
    data, hdrs = None, {"Authorization": f"Bearer {TOKEN}"} if TOKEN else {}
    if isinstance(body, (dict, list)):
        data, hdrs["Content-Type"] = json.dumps(body).encode(), "application/json"
    elif body is not None:
        data = body
    hdrs.update(headers or {})
    try:
        with urllib.request.urlopen(urllib.request.Request(BASE + path, data=data, headers=hdrs, method=method),
                                    context=CTX, timeout=60) as resp:
            text = resp.read().decode()
    except urllib.error.HTTPError as e:
        sys.exit(f"{method} {path} -> HTTP {e.code}: {e.read().decode()[:500]}")
    return text if raw else json.loads(text or "null")


def login():
    form = urllib.parse.urlencode({"username": os.environ.get("NIFI_USERNAME", "admin"),
                                   "password": os.environ["NIFI_PASSWORD"]}).encode()
    return call("POST", "/access/token", form, raw=True,
                headers={"Content-Type": "application/x-www-form-urlencoded"})


def find_group(name):
    found, queue = [], ["root"]
    while queue:
        for pg in call("GET", f"/flow/process-groups/{queue.pop(0)}")["processGroupFlow"]["flow"]["processGroups"]:
            if pg["component"]["name"] == name:
                found.append(pg["id"])
            queue.append(pg["id"])
    if not found:
        sys.exit(f"no group '{name}' on the canvas: make import f=nifi/flows/lab9-performance.json")
    if len(found) > 1:
        sys.exit(f"{len(found)} groups named '{name}': keep one, or rename the others ({', '.join(found)})")
    return found[0]


def workload(gid):
    for c in call("GET", f"/process-groups/{gid}/connections")["connections"]:
        if c["component"]["source"]["name"] == "GenerateRecord":
            return c["component"]["backPressureObjectThreshold"]
    sys.exit("no connection from GenerateRecord")


def processors(gid):
    return {p["component"]["name"]: p for p in call("GET", f"/process-groups/{gid}/processors")["processors"]}


def services(gid):
    return {s["component"]["name"]: s["id"] for s in
            call("GET", f"/flow/process-groups/{gid}/controller-services?includeAncestorGroups=false")["controllerServices"]}


def set_services(gid, state):
    call("PUT", f"/flow/process-groups/{gid}/controller-services", {"id": gid, "state": state})
    for _ in range(30):
        cs = call("GET", f"/flow/process-groups/{gid}/controller-services?includeAncestorGroups=false")["controllerServices"]
        if all(s["component"]["state"] == state for s in cs):
            return
        time.sleep(1)
    sys.exit(f"controller services did not become {state}")


def set_group(gid, state):
    call("PUT", f"/flow/process-groups/{gid}", {"id": gid, "state": state})


def run_status(pid, state):
    version = call("GET", f"/processors/{pid}")["revision"]["version"]
    call("PUT", f"/processors/{pid}/run-status", {"revision": {"version": version, "clientId": CLIENT}, "state": state})


def configure(pid, properties=None, tasks=None):
    """Change properties / Concurrent Tasks of a stopped processor."""
    current = call("GET", f"/processors/{pid}")
    config = {}
    if properties:
        config["properties"] = properties
    if tasks:
        config["concurrentlySchedulableTaskCount"] = tasks
    call("PUT", f"/processors/{pid}", {"revision": {"version": current["revision"]["version"], "clientId": CLIENT},
                                       "component": {"id": pid, "config": config}})


def snapshot_config(proc):
    c = proc["component"]["config"]
    return {"properties": dict(c["properties"]), "tasks": c["concurrentlySchedulableTaskCount"]}


def wait_stopped(gid):
    for _ in range(60):
        if all(p["status"]["aggregateSnapshot"]["activeThreadCount"] == 0 and p["component"]["state"] != "RUNNING"
               for p in processors(gid).values()):
            return
        time.sleep(0.5)


def empty_queues(gid):
    drop = call("POST", f"/process-groups/{gid}/empty-all-connections-requests", {})["dropRequest"]
    for _ in range(120):
        if call("GET", f"/process-groups/{gid}/empty-all-connections-requests/{drop['id']}")["dropRequest"]["finished"]:
            break
        time.sleep(0.5)
    call("DELETE", f"/process-groups/{gid}/empty-all-connections-requests/{drop['id']}")


COUNTER_OWNERS = {"lab9.records.out": "UpdateCounter", "lab9.records.invalid": "count invalid",
                  "lab9.flowfiles.failed": "count failed"}
COUNTER_PIDS = {}   # counter name -> id of the processor in this group that updates it


def counters():
    """NiFi keeps a counter per processor ("UpdateCounter (<id>)") plus an "All ..." total, and keeps the counters
    of deleted processors until a restart: select by the processor of this group, never by name alone."""
    snap = call("GET", "/counters")["counters"]["aggregateSnapshot"]["counters"]
    return {name: c for c in snap for name, pid in COUNTER_PIDS.items()
            if c["name"] == name and c["context"].endswith(f"({pid})")}


def reset_counters():
    for c in counters().values():
        call("PUT", f"/counters/{c['id']}")


def counter_values():
    cs = counters()
    return {name: int(cs[name]["valueCount"]) if name in cs else 0 for name in COUNTERS}


def group_status(gid):
    snap = call("GET", f"/flow/process-groups/{gid}/status")["processGroupStatus"]["aggregateSnapshot"]
    threads = {p["processorStatusSnapshot"]["name"]: p["processorStatusSnapshot"]["activeThreadCount"]
               for p in snap["processorStatusSnapshots"]}
    queues = {c["connectionStatusSnapshot"]["destinationName"]: c["connectionStatusSnapshot"]["flowFilesQueued"]
              for c in snap["connectionStatusSnapshots"]
              if c["connectionStatusSnapshot"]["destinationName"] in PIPELINE}
    return threads, queues


def gc_millis():
    snap = call("GET", "/system-diagnostics")["systemDiagnostics"]["aggregateSnapshot"]
    return sum(g.get("collectionMillis", 0) for g in snap.get("garbageCollection", []))


def content_free_gb():
    """Free space, GB: the smaller of NiFi's content repository and the host disk. With Docker Desktop NiFi sees
    the disk of the Docker VM, whose image lives on the host disk and grows with it: the host can run out first."""
    snap = call("GET", "/system-diagnostics")["systemDiagnostics"]["aggregateSnapshot"]
    repos = snap.get("contentRepositoryStorageUsage") or []
    frees = [r["freeSpaceBytes"] / 1e9 for r in repos] + [shutil.disk_usage(os.path.expanduser("~")).free / 1e9]
    return min(frees)


def check_disk(runs_left):
    free = content_free_gb()
    if free is None:
        return
    need = MIN_FREE_GB + runs_left * WORKLOAD_FLOWFILES * BYTES_PER_RUN_PER_FLOWFILE / 1e9
    if free < need:
        sys.exit(f"not enough disk for {runs_left} more run(s): {free:.1f} GB free (content repository / host), "
                 f"{need:.1f} GB needed ({MIN_FREE_GB:g} GB reserve + ~{need - MIN_FREE_GB:.1f} GB for the runs). "
                 "Free space or limit the content archive (nifi.content.repository.archive.max.usage.percentage, "
                 "or archive.enabled=false), see Lab 9 \"Disk space\"; fewer runs: REPEATS=1 or SCENARIOS=A,B")


def heap_used_mb():
    return call("GET", "/system-diagnostics")["systemDiagnostics"]["aggregateSnapshot"]["usedHeapBytes"] / 2 ** 20


def container_cpu_seconds():
    r = subprocess.run(["docker", "exec", CONTAINER, "cat", "/sys/fs/cgroup/cpu.stat"], capture_output=True, text=True)
    for line in r.stdout.splitlines():
        if line.startswith("usage_usec"):
            return int(line.split()[1]) / 1e6
    return None


def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


# ---------------------------------------------------------------- one run

def prepare(gid, procs):
    """Stop, empty, reset counters, then let only the generator fill the workload."""
    set_group(gid, "STOPPED")
    wait_stopped(gid)
    empty_queues(gid)
    reset_counters()
    if any(counter_values().values()):
        sys.exit(f"the lab9 counters did not reset: {counter_values()}")
    run_status(procs["GenerateRecord"]["id"], "RUNNING")
    t = time.time()
    while True:
        _, queues = group_status(gid)
        if queues.get("ValidateRecord", 0) >= WORKLOAD_FLOWFILES:
            break
        if time.time() - t > TIMEOUT:
            sys.exit("the workload was not generated in time")
        time.sleep(0.5)
    run_status(procs["GenerateRecord"]["id"], "STOPPED")
    wait_stopped_one(procs["GenerateRecord"]["id"])
    return time.time() - t


def wait_stopped_one(pid):
    for _ in range(60):
        p = call("GET", f"/processors/{pid}")
        if p["status"]["aggregateSnapshot"]["activeThreadCount"] == 0:
            return
        time.sleep(0.5)


def measure(gid, procs):
    gc0, cpu0 = gc_millis(), container_cpu_seconds()
    t0 = time.time()
    for name in PIPELINE:
        run_status(procs[name]["id"], "RUNNING")
    busy = {name: [] for name in PIPELINE}
    queued = {name: [] for name in PIPELINE}
    heap = []
    while True:
        threads, queues = group_status(gid)
        for name in PIPELINE:
            busy[name].append(threads.get(name, 0))
            queued[name].append(queues.get(name, 0))
        if len(heap) < 1 or len(busy["ValidateRecord"]) % 10 == 0:
            heap.append(heap_used_mb())
        c = counter_values()
        if c["lab9.records.out"] + c["lab9.records.invalid"] >= RECORDS or time.time() - t0 > TIMEOUT:
            break
        time.sleep(0.5)
    seconds = time.time() - t0
    gc1, cpu1 = gc_millis(), container_cpu_seconds()
    set_group(gid, "STOPPED")
    c = counter_values()
    return {"seconds": seconds, "records_out": c["lab9.records.out"], "invalid": c["lab9.records.invalid"],
            "failed": c["lab9.flowfiles.failed"], "rate": c["lab9.records.out"] / seconds,
            "gc_ms": gc1 - gc0, "cpu_cores": (cpu1 - cpu0) / seconds if cpu0 is not None and cpu1 is not None else None,
            "cpu_per_m": (cpu1 - cpu0) / (c["lab9.records.out"] / 1e6) if cpu0 is not None and cpu1 is not None
            and c["lab9.records.out"] else None,
            "busy": {n: statistics.mean(v) for n, v in busy.items()},
            "queued": {n: statistics.mean(v) for n, v in queued.items()},
            "heap_max_mb": max(heap) if heap else None, "timed_out": seconds > TIMEOUT}


# ---------------------------------------------------------------- scenarios

def run():
    global TOKEN
    started = time.time()
    TOKEN = login()
    gid = find_group(GROUP)
    procs = processors(gid)
    missing = set(PIPELINE + ["GenerateRecord"] + list(COUNTER_OWNERS.values())) - set(procs)
    if missing:
        sys.exit(f"the group has no {', '.join(sorted(missing))}: is it the Lab 9 reference flow?")
    cs = services(gid)
    for needed in ("reader-inferred-schema", "reader-input-schema"):
        if needed not in cs:
            sys.exit(f"the group has no service '{needed}': re-import nifi/flows/lab9-performance.json")
    for name, owner in COUNTER_OWNERS.items():
        COUNTER_PIDS[name] = procs[owner]["id"]
    global WORKLOAD_FLOWFILES, RECORDS
    WORKLOAD_FLOWFILES = workload(gid)
    RECORDS = WORKLOAD_FLOWFILES * RECORDS_PER_FLOWFILE
    log(f"group '{GROUP}' -> {gid}, workload {WORKLOAD_FLOWFILES} FlowFiles = {RECORDS:,} records")
    check_disk(len(SCENARIOS) * REPEATS)
    free = content_free_gb()
    if free is not None:
        log(f"free disk (content repository / host, the smaller): {free:.1f} GB")
    original = {name: snapshot_config(p) for name, p in procs.items()}
    set_group(gid, "STOPPED")
    wait_stopped(gid)
    set_services(gid, "ENABLED")

    results, applied, bottleneck = {}, {}, None
    try:
        for scenario in SCENARIOS:
            # cumulative settings: A -> B -> C -> D
            settings = {"ValidateRecord": {"properties": {"Record Reader": cs["reader-inferred-schema"]}}}
            if scenario >= "B":
                settings["ValidateRecord"] = {"properties": {"Record Reader": cs["reader-input-schema"]}}
            if scenario >= "C":
                bottleneck = bottleneck or "ValidateRecord"   # measured in A / B; a guess when they were skipped
                settings.setdefault(bottleneck, {})["tasks"] = 4
            if scenario >= "D":
                settings.setdefault("MergeRecord", {}).setdefault("properties", {}).update(
                    {"Minimum Number of Records": "100000", "Maximum Number of Records": "200000"})
            set_group(gid, "STOPPED")
            wait_stopped(gid)
            for name in PIPELINE:   # back to the original first, then the scenario on top
                configure(procs[name]["id"], {k: v for k, v in original[name]["properties"].items()
                                              if k in ("Record Reader", "Minimum Number of Records",
                                                       "Maximum Number of Records")}, original[name]["tasks"])
            for name, s in settings.items():
                configure(procs[name]["id"], s.get("properties"), s.get("tasks"))
            applied[scenario] = describe(scenario, bottleneck)
            results[scenario] = []
            for i in range(REPEATS):
                check_disk(1)
                load = prepare(gid, procs)
                r = measure(gid, procs)
                r["load_seconds"] = load
                results[scenario].append(r)
                log(f"{scenario} run {i + 1}/{REPEATS}: {r['seconds']:.1f} s, {r['rate']:,.0f} records/s, "
                    f"invalid {r['invalid']}, busiest {max(r['busy'], key=r['busy'].get)}")
            if scenario in ("A", "B"):
                med = median_run(results[scenario])
                bottleneck = max(PIPELINE, key=lambda n: med["busy"][n])
    finally:
        set_group(gid, "STOPPED")
        wait_stopped(gid)
        for name, cfg in original.items():
            configure(procs[name]["id"], {k: v for k, v in cfg["properties"].items()
                                          if k in ("Record Reader", "Minimum Number of Records",
                                                   "Maximum Number of Records")}, cfg["tasks"])
        empty_queues(gid)
        log("original settings restored, group stopped, queues empty")
    return {"started": started, "results": results, "applied": applied, "bottleneck": bottleneck,
            "cpus": os.cpu_count(), "heap": heap_setting()}


def describe(scenario, bottleneck):
    return {"A": "ValidateRecord Reader = reader-inferred-schema (Infer Schema)",
            "B": "ValidateRecord Reader = reader-input-schema (explicit schema)",
            "C": f"Concurrent Tasks = 4 on {bottleneck}",
            "D": "MergeRecord Minimum / Maximum Number of Records = 100,000 / 200,000"}[scenario]


def median_run(runs):
    """The run with the median duration, so that its profile belongs to a real run."""
    return sorted(runs, key=lambda r: r["seconds"])[len(runs) // 2]


def heap_setting():
    path = os.path.join(HERE, "..", "single", "docker-compose-nifi-single.yml")
    try:
        lines = open(path).read().splitlines()
    except OSError:
        return "?"
    vals = [l.split(":", 1)[1].strip() for l in lines if "NIFI_JVM_HEAP_INIT" in l or "NIFI_JVM_HEAP_MAX" in l]
    return " / ".join(vals) or "?"


# ---------------------------------------------------------------- text report

def fmt_rate(x):
    return f"{x:,.0f}"


def text_report(r):
    res, scen = r["results"], list(r["results"])
    base = statistics.median(x["rate"] for x in res[scen[0]]) if scen else 0
    lines = [f"Lab 9 - performance test, run of {time.strftime('%Y-%m-%d %H:%M', time.localtime(r['started']))}",
             f"workload {RECORDS:,} records ({WORKLOAD_FLOWFILES} FlowFiles x 10,000), {REPEATS} runs per scenario, "
             f"median reported; heap {r['heap']}, host CPUs {r['cpus']}", "",
             "=== scenarios (cumulative: each adds one change) ===",
             f"  {'':3}{'change':66}{'records/s':>11}{'vs A':>7}{'time, s':>9}{'CPU cores':>10}{'CPU s/1M':>9}{'GC, ms':>8}"]
    for s in scen:
        runs = res[s]
        m = median_run(runs)
        rate = statistics.median(x["rate"] for x in runs)
        cpu = statistics.median(x["cpu_cores"] for x in runs if x["cpu_cores"] is not None) \
            if any(x["cpu_cores"] is not None for x in runs) else float("nan")
        cpu_m = statistics.median(x["cpu_per_m"] or 0 for x in runs)
        lines.append(f"  {s:3}{r['applied'][s][:66]:66}{fmt_rate(rate):>11}{rate / base:>6.2f}x"
                     f"{statistics.median(x['seconds'] for x in runs):>9.1f}{cpu:>10.1f}{cpu_m:>9.1f}"
                     f"{statistics.median(x['gc_ms'] for x in runs):>8.0f}")
    lines += ["", "=== runs ===", f"  {'':3}{'run':>4}{'records/s':>11}{'time, s':>9}{'load, s':>9}"
              f"{'invalid':>9}{'failed':>8}{'heap max, MB':>14}"]
    for s in scen:
        for i, x in enumerate(res[s], 1):
            lines.append(f"  {s:3}{i:>4}{fmt_rate(x['rate']):>11}{x['seconds']:>9.1f}{x['load_seconds']:>9.1f}"
                         f"{x['invalid']:>9}{x['failed']:>8}{x['heap_max_mb'] or 0:>14.0f}"
                         + ("  TIMEOUT" if x["timed_out"] else ""))
    lines += ["", "=== where the time goes: average busy threads per processor (median run) ===",
              f"  {'processor':18}" + "".join(f"{s:>8}" for s in scen)]
    for n in PIPELINE:
        lines.append(f"  {n:18}" + "".join(f"{median_run(res[s])['busy'][n]:>8.2f}" for s in scen))
    lines += ["", "=== average FlowFiles queued in front of each processor (median run) ===",
              f"  {'processor':18}" + "".join(f"{s:>8}" for s in scen)]
    for n in PIPELINE:
        lines.append(f"  {n:18}" + "".join(f"{median_run(res[s])['queued'][n]:>8.1f}" for s in scen))
    lines += ["", f"bottleneck used for C: {r['bottleneck']}"]
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------- HTML report

CSS = """
.viz-root{color-scheme:light;--surface-1:#fcfcfb;--page:#f9f9f7;--text-primary:#0b0b0b;--text-secondary:#52514e;
--muted:#898781;--grid:#e1e0d9;--axis:#c3c2b7;--ring:rgba(11,11,11,.10);--s1:#2a78d6;
--q100:#cde2fb;--q250:#86b6ef;--q400:#3987e5;--q550:#1c5cab;--q700:#0d366b}
@media (prefers-color-scheme:dark){.viz-root{color-scheme:dark;--surface-1:#1a1a19;--page:#0d0d0d;--text-primary:#f0efec;
--text-secondary:#c3c2b7;--muted:#898781;--grid:#2c2c2a;--axis:#383835;--ring:rgba(255,255,255,.10);--s1:#3987e5;
--q100:#104281;--q250:#1c5cab;--q400:#2a78d6;--q550:#6da7ec;--q700:#b7d3f6}}
body{margin:0;background:var(--page)}
.viz-root{font-family:system-ui,-apple-system,"Segoe UI",sans-serif;color:var(--text-primary);background:var(--page);
padding:32px;max-width:1100px;margin:0 auto}
h1{font-size:22px;margin:0 0 4px}h2{font-size:16px;margin:0 0 2px}
.sub{color:var(--text-secondary);font-size:13px;margin:0 0 20px}
.tiles{display:grid;grid-template-columns:repeat(4,1fr);gap:12px;margin-bottom:16px}
.tile,.card{background:var(--surface-1);border-radius:12px;box-shadow:0 0 0 1px var(--ring);padding:16px 18px}
.tile .label{font-size:13px;color:var(--text-secondary)}.tile .value{font-size:30px;font-weight:600;margin:4px 0}
.tile .exp{font-size:12px;color:var(--muted)}
.card{margin-bottom:16px;position:relative}
.legend{display:flex;gap:16px;font-size:12px;color:var(--text-secondary);margin:8px 0 4px;flex-wrap:wrap}
.legend span{display:inline-flex;align-items:center;gap:6px}
.legend i{width:8px;height:8px;border-radius:50%;display:inline-block}
svg{display:block;width:100%;height:auto;overflow:visible}
svg text{font-size:11px;fill:var(--muted);font-variant-numeric:tabular-nums}
svg .lbl{fill:var(--text-secondary);font-size:12px}svg .val{fill:var(--text-primary);font-size:12px}
.tip{position:absolute;pointer-events:none;background:var(--surface-1);box-shadow:0 0 0 1px var(--ring),0 4px 12px rgba(0,0,0,.12);
border-radius:8px;padding:8px 10px;font-size:12px;color:var(--text-primary);display:none;white-space:nowrap;z-index:2}
.tip b{display:block;margin-bottom:4px}
details{margin-top:8px;font-size:12px;color:var(--text-secondary)}summary{cursor:pointer}
table{border-collapse:collapse;margin-top:8px;font-variant-numeric:tabular-nums;font-size:13px}
td,th{padding:4px 14px 4px 0;text-align:right;border-bottom:1px solid var(--grid)}th:first-child,td:first-child{text-align:left}
th{color:var(--text-secondary);font-weight:500}
.note{font-size:12px;color:var(--text-secondary);margin:6px 0 0}
.heat td.cell{text-align:center;min-width:70px;border-radius:4px;border:2px solid var(--surface-1)}
"""

JS = """
document.querySelectorAll('[data-chart]').forEach(card=>{
  const tip=card.querySelector('.tip');
  card.querySelectorAll('[data-tip]').forEach(el=>{
    el.addEventListener('mousemove',ev=>{tip.innerHTML=el.dataset.tip;tip.style.display='block';
      const r=card.getBoundingClientRect();let left=ev.clientX-r.left+14;
      if(left+tip.offsetWidth>r.width)left=ev.clientX-r.left-tip.offsetWidth-14;
      tip.style.left=left+'px';tip.style.top=(ev.clientY-r.top+14)+'px'});
    el.addEventListener('mouseleave',()=>{tip.style.display='none'})});
});
"""


def esc(s):
    return html.escape(str(s), quote=True)


def nice_top(v):
    v = max(v, 1e-9)
    mag = 10 ** len(str(int(v))) / 10 if v >= 1 else 0.1
    for m in (1, 2, 2.5, 5, 10):
        if m * mag >= v:
            return m * mag
    return 10 * mag


def hbar_chart(rows, title, subtitle, unit, note="", fmt=lambda v: f"{v:,.0f}"):
    """rows: [(label, median, [run values])]: one series (slot 1) + a dot per run."""
    W, L, R, rowh = 1000, 290, 90, 36
    H = 16 + rowh * len(rows) + 26
    top = nice_top(max(max([m] + runs) for _, m, runs in rows))
    X = lambda v: L + (W - L - R) * v / top
    out = [f'<svg viewBox="0 0 {W} {H}" role="img" aria-label="{esc(title)}">']
    for i in range(6):
        v = top * i / 5
        out.append(f'<line x1="{X(v):.1f}" x2="{X(v):.1f}" y1="8" y2="{H - 26}" stroke="var(--grid)"/>'
                   f'<text x="{X(v):.1f}" y="{H - 8}" text-anchor="middle">{esc(fmt(v))}</text>')
    for i, (label, m, runs) in enumerate(rows):
        y = 16 + i * rowh
        w = X(m) - L
        tip = esc(f"<b>{label}</b>median {fmt(m)} {unit}<br>runs: " + ", ".join(fmt(v) for v in runs))
        out.append(f'<text class="lbl" x="{L - 12}" y="{y + 15}" text-anchor="end">{esc(label)}</text>')
        if w >= 4:
            out.append(f'<path d="M{L},{y + 3} h{w - 4:.1f} a4,4 0 0 1 4,4 v10 a4,4 0 0 1 -4,4 h-{w - 4:.1f} z" '
                       f'fill="var(--s1)"/>')
        for v in runs:
            out.append(f'<circle cx="{X(v):.1f}" cy="{y + 12}" r="4" fill="var(--text-primary)" '
                       f'stroke="var(--surface-1)" stroke-width="2"/>')
        out.append(f'<text class="val" x="{X(max([m] + runs)) + 10:.1f}" y="{y + 16}">{esc(fmt(m))}</text>'
                   f'<rect x="{L}" y="{y}" width="{W - L - R}" height="24" fill="transparent" data-tip="{tip}"/>')
    out.append(f'<line x1="{L}" x2="{L}" y1="8" y2="{H - 26}" stroke="var(--axis)"/></svg>')
    legend = ('<div class="legend"><span><i style="background:var(--s1);border-radius:2px"></i>median</span>'
              '<span><i style="background:var(--text-primary)"></i>each run</span></div>')
    table = ("<details><summary>Table</summary><table><tr><th></th><th>median</th><th>runs</th></tr>"
             + "".join(f"<tr><td>{esc(l)}</td><td>{esc(fmt(m))}</td><td>{esc(', '.join(fmt(v) for v in runs))}</td></tr>"
                       for l, m, runs in rows) + "</table></details>")
    return (f'<div class="card" data-chart><h2>{esc(title)}</h2><p class="sub" style="margin:0">{esc(subtitle)}</p>'
            f'{legend}{"".join(out)}{f"<p class=note>{esc(note)}</p>" if note else ""}{table}<div class="tip"></div></div>')


def heat_table(r, title, subtitle, key, fmt, note):
    """Rows = processors, columns = scenarios, one-hue sequential ramp by value; the number is always printed."""
    res, scen = r["results"], list(r["results"])
    vals = {(n, s): median_run(res[s])[key][n] for n in PIPELINE for s in scen}
    vmax = max(vals.values()) or 1
    ramp = ["--q100", "--q250", "--q400", "--q550", "--q700"]

    def cell(v):
        step = min(int(v / vmax * len(ramp)), len(ramp) - 1) if v > 0 else -1
        if step < 0:
            return '<td class="cell">0</td>'
        ink = "#ffffff" if step >= 2 else "var(--text-primary)"
        return f'<td class="cell" style="background:var({ramp[step]});color:{ink}">{esc(fmt(v))}</td>'
    head = "<tr><th>processor</th>" + "".join(f"<th style='text-align:center'>{s}</th>" for s in scen) + "</tr>"
    rows = "".join(f"<tr><td>{n}</td>" + "".join(cell(vals[(n, s)]) for s in scen) + "</tr>" for n in PIPELINE)
    return (f'<div class="card"><h2>{esc(title)}</h2><p class="sub" style="margin:0">{esc(subtitle)}</p>'
            f'<table class="heat">{head}{rows}</table><p class="note">{esc(note)}</p></div>')


def html_report(r):
    res, scen = r["results"], list(r["results"])
    med_rate = {s: statistics.median(x["rate"] for x in res[s]) for s in scen}
    first, best = scen[0], max(scen, key=lambda s: med_rate[s])
    busiest = {s: max(PIPELINE, key=lambda n: median_run(res[s])["busy"][n]) for s in scen}
    tiles = [(f"Baseline ({first})", f"{med_rate[first]:,.0f}", "records/s, median"),
             (f"Best ({best})", f"{med_rate[best]:,.0f}", "records/s, median"),
             ("Speed-up", f"{med_rate[best] / med_rate[first]:.1f}×", f"{best} vs {first}"),
             ("Bottleneck", busiest[first], f"in {first}; in {scen[-1]}: {busiest[scen[-1]]}")]
    tiles_html = '<div class="tiles">' + "".join(
        f'<div class="tile"><div class="label">{esc(l)}</div><div class="value" style="font-size:{24 if len(v) > 10 else 30}px">'
        f'{esc(v)}</div><div class="exp">{esc(e)}</div></div>' for l, v, e in tiles) + "</div>"
    label = lambda s: f"{s} · {TITLES[s]}"
    rate = hbar_chart([(label(s), med_rate[s], [x["rate"] for x in res[s]]) for s in scen],
                      "Throughput by scenario", f"records/s for a fixed workload of {RECORDS:,} records; "
                      "the scenarios are cumulative", "records/s",
                      "Each scenario adds one change to the previous one: " +
                      "; ".join(f"{s}: {r['applied'][s]}" for s in scen) + ".")
    cpu = hbar_chart([(label(s), statistics.median(x["cpu_per_m"] or 0 for x in res[s]),
                       [x["cpu_per_m"] or 0 for x in res[s]]) for s in scen],
                     "CPU per million records", "CPU seconds of the nifi container per 1,000,000 records delivered",
                     "CPU s", "Lower is cheaper. A change can save CPU without raising the throughput when it speeds "
                     "up a processor that is not the bottleneck.", fmt=lambda v: f"{v:.1f} s")
    gc = hbar_chart([(label(s), statistics.median(x["gc_ms"] for x in res[s]), [x["gc_ms"] for x in res[s]])
                     for s in scen], "Garbage collection", "JVM GC time during the measurement", "ms",
                    fmt=lambda v: f"{v:,.0f} ms")
    busy = heat_table(r, "Where the time goes", "average busy threads per processor in the median run",
                      "busy", lambda v: f"{v:.2f}",
                      "1.00 = one thread busy all the time. The darkest cell of a column is the bottleneck of that "
                      "scenario; after a fix the bottleneck moves to the next processor.")
    queue = heat_table(r, "Where FlowFiles wait", "average FlowFiles queued in front of each processor",
                       "queued", lambda v: f"{v:.1f}",
                       f"The workload starts as {WORKLOAD_FLOWFILES} FlowFiles in front of ValidateRecord; a queue "
                       "that stays long in front of a later processor marks a slow stage.")
    runs = ("<div class='card'><h2>All runs</h2><table><tr><th>scenario</th><th>run</th><th>records/s</th>"
            "<th>time, s</th><th>load, s</th><th>CPU cores</th><th>invalid</th><th>failed</th><th>heap max, MB</th></tr>"
            + "".join(f"<tr><td>{s}</td><td>{i}</td><td>{x['rate']:,.0f}</td><td>{x['seconds']:.1f}</td>"
                      f"<td>{x['load_seconds']:.1f}</td><td>{(x['cpu_cores'] or 0):.1f}</td><td>{x['invalid']}</td><td>{x['failed']}</td>"
                      f"<td>{x['heap_max_mb'] or 0:.0f}</td></tr>" for s in scen for i, x in enumerate(res[s], 1))
            + "</table><p class='note'>invalid and failed must be 0: the generated data match the input schema. "
              "Anything else means records were lost, and the throughput of that run is not comparable.</p></div>")
    when = time.strftime("%Y-%m-%d %H:%M", time.localtime(r["started"]))
    return (f'<!doctype html><html lang="en"><head><meta charset="utf-8"><title>Lab 9 measurements {when}</title>'
            f'<meta name="viewport" content="width=device-width,initial-scale=1"><style>{CSS}</style></head><body>'
            f'<div class="viz-root"><h1>Lab 9 — performance test</h1>'
            f'<p class="sub">Run of {when}. Workload {RECORDS:,} records ({WORKLOAD_FLOWFILES} FlowFiles × 10,000), '
            f'{REPEATS} runs per scenario, median shown. Heap {esc(r["heap"])}, {r["cpus"]} host CPUs.</p>'
            f'{tiles_html}{rate}{busy}{queue}{cpu}{gc}{runs}</div><script>{JS}</script></body></html>')


def main():
    unknown = set(SCENARIOS) - set(TITLES)
    if unknown:
        sys.exit(f"unknown scenarios: {', '.join(sorted(unknown))} (A, B, C, D)")
    print(f"Lab 9 measurement: scenarios {','.join(SCENARIOS)}, {REPEATS} runs each")
    r = run()
    report = text_report(r)
    print("\n" + report)
    os.makedirs(RESULTS, exist_ok=True)
    base = os.path.join(RESULTS, f"lab9-measure-{time.strftime('%Y%m%d-%H%M%S')}")
    with open(base + ".txt", "w") as f:
        f.write(report)
    with open(base + ".html", "w") as f:
        f.write(html_report(r))
    print(f"report: {os.path.relpath(base, os.getcwd())}.txt and .html")


if __name__ == "__main__":
    main()
