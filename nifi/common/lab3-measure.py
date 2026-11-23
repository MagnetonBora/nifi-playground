#!/usr/bin/env python3
"""Lab 3: run "A - Split-based" and then "B - Record-based" and compare the metrics.

Assumes Lab 3 is imported, its controller services are enabled and both groups are stopped.
For each variant: start all processors except the generator, Run Once on GenerateRecord, wait until
the data reaches every LogAttribute, stop the group, print Tasks / Time / In / Out per processor and
a summary (wall time, peak queue, peak heap, provenance events).

  NIFI_PASSWORD=... python3 nifi/common/lab3-measure.py
Env: NIFI_URL (default https://localhost:8443), NIFI_USERNAME (admin), NIFI_PASSWORD
"""
import json
import os
import ssl
import time
import urllib.parse
import urllib.request

BASE = os.environ.get("NIFI_URL", "https://localhost:8443") + "/nifi-api"
CTX = ssl.create_default_context(cafile=os.path.join(os.path.dirname(__file__), "..", "certs", "ca.crt"))
GROUPS = {"A": "A - Split-based", "B": "B - Record-based"}
TOKEN = None


def call(method, path, body=None):
    req = urllib.request.Request(BASE + path, method=method,
                                 data=json.dumps(body).encode() if body is not None else None,
                                 headers={"Content-Type": "application/json", "Authorization": f"Bearer {TOKEN}"})
    with urllib.request.urlopen(req, context=CTX) as resp:
        return json.loads(resp.read() or "null")


def login():
    form = urllib.parse.urlencode({"username": os.environ.get("NIFI_USERNAME", "admin"),
                                   "password": os.environ["NIFI_PASSWORD"]}).encode()
    with urllib.request.urlopen(urllib.request.Request(BASE + "/access/token", data=form), context=CTX) as resp:
        return resp.read().decode()


def find_groups():
    """Breadth-first search of the canvas for the two variant groups, by name."""
    found, queue = {}, ["root"]
    while queue:
        for pg in call("GET", f"/flow/process-groups/{queue.pop(0)}")["processGroupFlow"]["flow"]["processGroups"]:
            found[pg["component"]["name"]] = pg["id"]
            queue.append(pg["id"])
    return {key: found[name] for key, name in GROUPS.items()}


def status(pg_id):
    return call("GET", f"/flow/process-groups/{pg_id}/status")["processGroupStatus"]["aggregateSnapshot"]


def counters(snapshot):
    return {s["id"]: s["processorStatusSnapshot"] for s in snapshot["processorStatusSnapshots"]}


def used_heap_mb():
    return call("GET", "/system-diagnostics")["systemDiagnostics"]["aggregateSnapshot"]["usedHeapBytes"] / 2**20


def run_status(proc_id, state):
    version = call("GET", f"/processors/{proc_id}")["revision"]["version"]
    call("PUT", f"/processors/{proc_id}/run-status",
         {"revision": {"version": version, "clientId": "lab3-measure"}, "state": state})


def provenance_events(proc_id, since):
    request = {"maxResults": 1, "startDate": time.strftime("%m/%d/%Y %H:%M:%S UTC", time.gmtime(since - 1)),
               "searchTerms": {"ProcessorID": {"value": proc_id, "inverse": False}}}
    query = call("POST", "/provenance", {"provenance": {"request": request}})["provenance"]
    while not query["finished"]:
        time.sleep(0.5)
        query = call("GET", f"/provenance/{query['id']}")["provenance"]
    call("DELETE", f"/provenance/{query['id']}")
    return query["results"]["totalCount"]


def measure(key, pg_id):
    print(f"\n=== {GROUPS[key]} ===")
    procs = [p["component"] for p in call("GET", f"/flow/process-groups/{pg_id}")["processGroupFlow"]["flow"]["processors"]]
    generator = next(p["id"] for p in procs if p["type"].endswith("GenerateRecord"))
    loggers = [p["id"] for p in procs if p["type"].endswith("LogAttribute")]

    # Status counters are a rolling 5-minute window: measure the difference before/after.
    before = counters(status(pg_id))
    heap_before = peak_heap = used_heap_mb()
    peak_queue = 0

    for p in procs:
        if p["id"] != generator:
            run_status(p["id"], "RUNNING")
    start = time.time()
    run_status(generator, "RUN_ONCE")

    # Done when the queues are empty, nothing is running and every LogAttribute got data
    # (in A, MergeContent holds records in its bin for up to 10 s while the queues are already empty).
    while True:
        snap = status(pg_id)
        now = counters(snap)
        peak_queue = max(peak_queue, snap["flowFilesQueued"])
        peak_heap = max(peak_heap, used_heap_mb())
        if (snap["flowFilesQueued"] == 0 and snap["activeThreadCount"] == 0
                and all(now[l]["flowFilesIn"] > before[l]["flowFilesIn"] for l in loggers)):
            break
        time.sleep(0.25)
    wall = time.time() - start

    call("PUT", f"/flow/process-groups/{pg_id}", {"id": pg_id, "state": "STOPPED"})
    time.sleep(3)  # let provenance indexing catch up
    after = counters(status(pg_id))

    print(f"  {'processor':28} {'tasks':>7} {'time, ms':>9} {'in':>7} {'out':>7} {'provenance':>11}")
    total = {"tasks": 0, "ms": 0, "events": 0}
    for p in sorted(procs, key=lambda p: p["position"]["y"]):
        d = {k: after[p["id"]][k] - before[p["id"]][k]
             for k in ("taskCount", "tasksDurationNanos", "flowFilesIn", "flowFilesOut")}
        events = provenance_events(p["id"], start)
        total["tasks"] += d["taskCount"]
        total["ms"] += d["tasksDurationNanos"] / 1e6
        total["events"] += events
        print(f"  {p['name']:28} {d['taskCount']:7} {d['tasksDurationNanos'] / 1e6:9.1f} "
              f"{d['flowFilesIn']:7} {d['flowFilesOut']:7} {events:11}")

    return {"wall time, s": f"{wall:.1f}", "tasks": total["tasks"], "task time, ms": f"{total['ms']:.0f}",
            "provenance events": total["events"], "peak queued": peak_queue,
            "heap growth, MB": f"{peak_heap - heap_before:.0f}"}


TOKEN = login()
groups = find_groups()
results = {}
for key in ("A", "B"):
    results[key] = measure(key, groups[key])
    time.sleep(5)  # let GC settle before the next variant

print("\n=== summary ===")
print(f"  {'metric':20} {'A (split)':>12} {'B (record)':>12}")
for metric in results["A"]:
    print(f"  {metric:20} {results['A'][metric]:>12} {results['B'][metric]:>12}")
