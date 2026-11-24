#!/usr/bin/env python3
"""Lab 8: measure retry and error handling against the flaky API and write a text + HTML report.

Uses the group "Lab 8 - Retry and error handling" that is already on the canvas (import it first:
make import f=nifi/flows/lab8-retry-errors.json). Enables its services and runs it in three phases:

  1. normal    flaky answers 60% 200 / 30% 503 / 10% 400          (PHASE1 s, default 240)
  2. outage    docker stop flaky -> every request ends in Failure  (PHASE2 s, default 150)
  3. recovery  docker start flaky                                  (PHASE3 s, default 90)

Every SAMPLE seconds (default 5) it records the queues of the retry loop. At the end it stops the generator,
lets the retry loop drain, takes the outcome of every FlowFile from provenance (the SEND events of
"PutFile (ok)" and "PutFile (DLQ)": time, error.stage, flowfile.retries, error.reason), compares the normal
phase with the probabilities of lab-services/flaky.py, stops the group and writes
labs/lab8-measure-<time>.txt and .html (repository root). The group is left stopped with its services enabled;
FlowFiles already queued in it are processed as part of the run.

Runs on the host, not in a container: it stops and starts the flaky container with the docker CLI.
flaky is started again whatever happens. Files already in nifi/data/ok and nifi/data/dlq do not matter.

  make lab08-measure
  NIFI_PASSWORD=... PHASE1=60 PHASE2=45 PHASE3=30 python3 nifi/common/lab8-measure.py   # a short run
Env: NIFI_URL (default https://localhost:8443), NIFI_USERNAME (admin), NIFI_PASSWORD,
     PHASE1, PHASE2, PHASE3, SAMPLE (seconds), GROUP (group name, default "Lab 8 - Retry and error handling")
"""
import datetime
import html
import json
import os
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
GROUP = os.environ.get("GROUP", "Lab 8 - Retry and error handling")
RESULTS = os.path.join(HERE, "..", "..", "labs")
BASE = os.environ.get("NIFI_URL", "https://localhost:8443").rstrip("/") + "/nifi-api"
CTX = ssl.create_default_context(cafile=os.path.join(HERE, "..", "certs", "ca.crt"))
PHASES = [("normal", int(os.environ.get("PHASE1", 240))),
          ("outage", int(os.environ.get("PHASE2", 150))),
          ("recovery", int(os.environ.get("PHASE3", 90)))]
SAMPLE = int(os.environ.get("SAMPLE", 5))
FLAKY = "flaky"
CLIENT = "lab8-measure"
TOKEN = None

# lab-services/flaky.py: 60% 200, 30% 503, 10% 400; RetryFlowFile: Maximum Retries 3 -> up to 4 attempts.
P_OK, P_RETRY, P_400, ATTEMPTS = 0.6, 0.3, 0.1, 4
LOOP = sum(P_RETRY ** k for k in range(ATTEMPTS))              # expected number of attempts that are made
EXPECTED = {"ok": P_OK * LOOP, "http-400": P_400 * LOOP, "retries-exceeded": P_RETRY ** ATTEMPTS}
EXPECTED_ATTEMPTS = {k: P_OK * P_RETRY ** (k - 1) / EXPECTED["ok"] for k in range(1, ATTEMPTS + 1)}
# GenerateRecord: 5 records x 4 fields, each field null with 5% -> a FlowFile has an invalid part
EXPECTED_INVALID = 1 - 0.95 ** (4 * 5)
OUTCOMES = ["ok", "validation", "http-400", "retries-exceeded"]


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
    """Breadth-first search of the canvas for exactly one group with this name."""
    found, queue = [], ["root"]
    while queue:
        for pg in call("GET", f"/flow/process-groups/{queue.pop(0)}")["processGroupFlow"]["flow"]["processGroups"]:
            if pg["component"]["name"] == name:
                found.append(pg["id"])
            queue.append(pg["id"])
    if not found:
        sys.exit(f"no group '{name}' on the canvas: make import f=nifi/flows/lab8-retry-errors.json")
    if len(found) > 1:
        sys.exit(f"{len(found)} groups named '{name}': keep one, or rename the others ({', '.join(found)})")
    return found[0]


def components(gid):
    procs = {p["component"]["name"]: p["id"]
             for p in call("GET", f"/process-groups/{gid}/processors?includeDescendantGroups=true")["processors"]}
    conns = {(c["component"]["source"]["name"], c["component"]["destination"]["name"]): c["id"]
             for c in call("GET", f"/process-groups/{gid}/connections")["connections"]}
    return procs, conns


def set_services(gid, state):
    call("PUT", f"/flow/process-groups/{gid}/controller-services", {"id": gid, "state": state})
    for _ in range(30):
        services = call("GET", f"/flow/process-groups/{gid}/controller-services"
                               "?includeAncestorGroups=false&includeDescendantGroups=true")["controllerServices"]
        if all(s["component"]["state"] == state for s in services):
            return
        time.sleep(1)
    sys.exit(f"controller services did not become {state}")


def set_group(gid, state):
    call("PUT", f"/flow/process-groups/{gid}", {"id": gid, "state": state})


def set_processor(pid, state):
    version = call("GET", f"/processors/{pid}")["revision"]["version"]
    call("PUT", f"/processors/{pid}/run-status", {"revision": {"version": version, "clientId": CLIENT}, "state": state})


def queued(gid):
    """Queued FlowFiles per connection (source, destination) and in total, including the DLQ group."""
    snap = call("GET", f"/flow/process-groups/{gid}/status?recursive=true")["processGroupStatus"]["aggregateSnapshot"]
    per = {}

    def walk(s):
        for c in s["connectionStatusSnapshots"]:
            c = c["connectionStatusSnapshot"]
            per[(c["sourceName"], c["destinationName"])] = c["flowFilesQueued"]
        for g in s.get("processGroupStatusSnapshots", []):
            walk(g["processGroupStatusSnapshot"])
    walk(snap)
    return per, snap["flowFilesQueued"]


def provenance(pid, since):
    request = {"maxResults": 10000, "summarize": False, "incrementalResults": False,
               "startDate": time.strftime("%m/%d/%Y %H:%M:%S UTC", time.gmtime(since - 1)),
               "searchTerms": {"ProcessorID": {"value": pid, "inverse": False}}}
    query = call("POST", "/provenance", {"provenance": {"request": request}})["provenance"]
    while not query["finished"]:
        time.sleep(0.5)
        query = call("GET", f"/provenance/{query['id']}")["provenance"]
    call("DELETE", f"/provenance/{query['id']}")
    events = []
    for e in query["results"]["provenanceEvents"]:
        t = datetime.datetime.strptime(e["eventTime"], "%m/%d/%Y %H:%M:%S.%f %Z")
        events.append({"type": e["eventType"], "t": t.replace(tzinfo=datetime.timezone.utc).timestamp(),
                       "attrs": {a["name"]: a["value"] for a in e["attributes"]}})
    return events


# ---------------------------------------------------------------- docker

def docker(*args):
    return subprocess.run(["docker", *args], capture_output=True, text=True)


def flaky_running():
    r = docker("inspect", "-f", "{{.State.Running}}", FLAKY)
    return r.returncode == 0 and r.stdout.strip() == "true"


# ---------------------------------------------------------------- the run

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


def run():
    global TOKEN
    if not flaky_running():
        sys.exit(f"container {FLAKY} is not running: make lab-services")
    TOKEN = login()
    gid = find_group(GROUP)
    log(f"group '{GROUP}' -> {gid}")
    procs, _ = components(gid)
    missing = {"GenerateRecord", "PutFile (ok)", "PutFile (DLQ)"} - set(procs)
    if missing:
        sys.exit(f"the group has no {', '.join(sorted(missing))}: is it the Lab 8 reference flow?")
    set_services(gid, "ENABLED")
    t0 = time.time()
    set_group(gid, "RUNNING")
    log("group started")

    samples, boundaries = [], []
    try:
        for phase, seconds in PHASES:
            if phase == "outage":
                docker("stop", FLAKY)
            elif phase == "recovery":
                docker("start", FLAKY)
            start = time.time()
            boundaries.append((phase, start - t0))
            log(f"phase {phase}: {seconds} s")
            while time.time() - start < seconds:
                per, total = queued(gid)
                samples.append({"t": time.time() - t0, "phase": phase,
                                "retry_wait": per.get(("RetryFlowFile", "InvokeHTTP"), 0),
                                "to_retry": per.get(("InvokeHTTP", "RetryFlowFile"), 0),
                                "total": total})
                time.sleep(SAMPLE)
        end_phases = time.time() - t0

        # drain: no new data, let every FlowFile in the retry loop reach ok or the DLQ
        set_processor(procs["GenerateRecord"], "STOPPED")
        log("generator stopped, draining the retry loop")
        for _ in range(int(60 / SAMPLE) + 1):
            per, total = queued(gid)
            samples.append({"t": time.time() - t0, "phase": "drain",
                            "retry_wait": per.get(("RetryFlowFile", "InvokeHTTP"), 0),
                            "to_retry": per.get(("InvokeHTTP", "RetryFlowFile"), 0), "total": total})
            if total == 0:
                break
            time.sleep(SAMPLE)
    finally:
        set_group(gid, "STOPPED")
        if not flaky_running():
            docker("start", FLAKY)
            log(f"{FLAKY} started again")

    log("reading provenance")
    time.sleep(5)  # provenance is indexed asynchronously
    generated = [e for e in provenance(procs["GenerateRecord"], t0) if e["type"] == "CREATE"]
    outcomes = []
    for proc in ("PutFile (ok)", "PutFile (DLQ)"):
        for e in provenance(procs[proc], t0):
            if e["type"] != "SEND":
                continue
            a = e["attrs"]
            outcomes.append({"t": e["t"] - t0, "stage": a.get("error.stage") or "ok",
                             "attempts": int(a.get("flowfile.retries") or 0) + 1,
                             "reason": a.get("error.reason", "")})
    outcomes.sort(key=lambda o: o["t"])

    log("group stopped")
    return {"started": t0, "boundaries": boundaries, "end": end_phases, "samples": samples,
            "generated": [e["t"] - t0 for e in generated], "outcomes": outcomes}


# ---------------------------------------------------------------- analysis

def phase_of(t, boundaries, end):
    current = boundaries[0][0]
    for name, start in boundaries:
        if t >= start:
            current = name
    return "drain" if t >= end else current


def analyse(r):
    b, end = r["boundaries"], r["end"]
    by_phase = {p: {o: 0 for o in OUTCOMES} for p, _ in PHASES + [("drain", 0)]}
    for o in r["outcomes"]:
        by_phase[phase_of(o["t"], b, end)][o["stage"] if o["stage"] in OUTCOMES else "ok"] += 1
    normal = by_phase["normal"]
    sent = normal["ok"] + normal["http-400"] + normal["retries-exceeded"]
    measured = {k: (normal[k] / sent if sent else 0) for k in EXPECTED}
    delivered = [o for o in r["outcomes"] if o["stage"] == "ok" and phase_of(o["t"], b, end) == "normal"]
    attempts = {k: sum(1 for o in delivered if o["attempts"] == k) for k in range(1, ATTEMPTS + 1)}
    invalid = sum(1 for o in r["outcomes"] if o["stage"] == "validation")
    outage_start = dict(b)["outage"]
    recovery_start = dict(b)["recovery"]
    first_exceeded = next((o["t"] - outage_start for o in r["outcomes"]
                           if o["stage"] == "retries-exceeded" and o["t"] >= outage_start), None)
    first_ok_after = next((o["t"] - recovery_start for o in r["outcomes"]
                           if o["stage"] == "ok" and o["t"] >= recovery_start), None)
    outage_q = [s["retry_wait"] + s["to_retry"] for s in r["samples"] if s["phase"] == "outage"]
    normal_q = [s["retry_wait"] + s["to_retry"] for s in r["samples"] if s["phase"] == "normal"]
    return {"by_phase": by_phase, "sent": sent, "measured": measured, "attempts": attempts,
            "delivered": len(delivered), "invalid": invalid, "generated": len(r["generated"]),
            "first_exceeded": first_exceeded, "first_ok_after": first_ok_after,
            "outage_q_max": max(outage_q, default=0), "normal_q_max": max(normal_q, default=0),
            "reasons": sorted({o["reason"] for o in r["outcomes"] if o["stage"] == "retries-exceeded"})[:3]}


def pct(x):
    return f"{100 * x:.1f}%"


def mmss(t):
    return f"{int(t // 60)}:{int(t % 60):02d}"


def text_report(r, a):
    lines = [f"Lab 8 - retry and error handling, run of {time.strftime('%Y-%m-%d %H:%M', time.localtime(r['started']))}",
             "phases: " + ", ".join(f"{p} {s} s" for p, s in PHASES) + f", sample every {SAMPLE} s", ""]
    lines += ["=== outcomes by phase (FlowFiles, by the time they reached ok / the DLQ) ===",
              f"  {'phase':10}" + "".join(f"{o:>18}" for o in OUTCOMES)]
    for p, counts in a["by_phase"].items():
        lines.append(f"  {p:10}" + "".join(f"{counts[o]:>18}" for o in OUTCOMES))
    lines += ["", "=== normal phase vs flaky.py (60% 200 / 30% 503 / 10% 400, up to 4 attempts) ===",
              f"  {'outcome':18}{'measured':>10}{'expected':>10}    n = {a['sent']} valid FlowFiles sent"]
    for k in EXPECTED:
        lines.append(f"  {k:18}{pct(a['measured'][k]):>10}{pct(EXPECTED[k]):>10}")
    lines += ["", f"=== attempts needed by the delivered FlowFiles (normal phase, n = {a['delivered']}) ===",
              f"  {'attempts':18}{'measured':>10}{'expected':>10}"]
    for k in range(1, ATTEMPTS + 1):
        share = a["attempts"][k] / a["delivered"] if a["delivered"] else 0
        lines.append(f"  {k:<18}{pct(share):>10}{pct(EXPECTED_ATTEMPTS[k]):>10}")
    share_invalid = a["invalid"] / a["generated"] if a["generated"] else 0
    lines += ["", "=== validation ===",
              f"  generated FlowFiles {a['generated']}, with an invalid part {a['invalid']} "
              f"({pct(share_invalid)}, expected {pct(EXPECTED_INVALID)})", "",
              "=== outage and recovery ===",
              f"  retry loop, max queued: normal {a['normal_q_max']}, outage {a['outage_q_max']}",
              "  first retries-exceeded after docker stop: "
              + (f"{a['first_exceeded']:.0f} s" if a["first_exceeded"] is not None else "none"),
              "  first delivery after docker start: "
              + (f"{a['first_ok_after']:.0f} s" if a["first_ok_after"] is not None else "none"),
              "  error.reason of retries-exceeded: " + "; ".join(a["reasons"] or ["-"]), "",
              "=== retry loop over time (queued FlowFiles, every 30 s) ===",
              f"  {'time':>6} {'phase':10}{'waiting to retry':>18}{'to RetryFlowFile':>18}{'whole group':>13}"]
    last = -30
    for s in r["samples"]:
        if s["t"] - last >= 30:
            last = s["t"]
            lines.append(f"  {mmss(s['t']):>6} {s['phase']:10}{s['retry_wait']:>18}{s['to_retry']:>18}{s['total']:>13}")
    return "\n".join(lines) + "\n"


# ---------------------------------------------------------------- HTML report

CSS = """
.viz-root{color-scheme:light;--surface-1:#fcfcfb;--page:#f9f9f7;--text-primary:#0b0b0b;--text-secondary:#52514e;
--muted:#898781;--grid:#e1e0d9;--axis:#c3c2b7;--band:rgba(11,11,11,.045);--ring:rgba(11,11,11,.10);
--s1:#2a78d6;--s2:#eb6834;--s3:#1baf7a;--s4:#eda100}
@media (prefers-color-scheme:dark){.viz-root{color-scheme:dark;--surface-1:#1a1a19;--page:#0d0d0d;--text-primary:#f0efec;
--text-secondary:#c3c2b7;--muted:#898781;--grid:#2c2c2a;--axis:#383835;--band:rgba(255,255,255,.05);
--ring:rgba(255,255,255,.10);--s1:#3987e5;--s2:#d95926;--s3:#199e70;--s4:#c98500}}
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
.legend span{display:inline-flex;align-items:center;gap:6px}.legend i{width:12px;height:2px;border-radius:1px;display:inline-block}
.legend i.dot{width:8px;height:8px;border-radius:50%}.legend i.tick{width:2px;height:12px;background:var(--text-primary)}
svg{display:block;width:100%;height:auto;overflow:visible}
svg text{font-size:11px;fill:var(--muted);font-variant-numeric:tabular-nums}
svg .lbl{fill:var(--text-secondary);font-size:12px}
.tip{position:absolute;pointer-events:none;background:var(--surface-1);box-shadow:0 0 0 1px var(--ring),0 4px 12px rgba(0,0,0,.12);
border-radius:8px;padding:8px 10px;font-size:12px;color:var(--text-primary);display:none;white-space:nowrap;z-index:2}
.tip b{display:block;margin-bottom:4px}.tip i{display:inline-block;width:8px;height:8px;border-radius:50%;margin-right:6px}
details{margin-top:8px;font-size:12px;color:var(--text-secondary)}summary{cursor:pointer}
table{border-collapse:collapse;margin-top:8px;font-variant-numeric:tabular-nums}
td,th{padding:3px 12px 3px 0;text-align:right;border-bottom:1px solid var(--grid)}th:first-child,td:first-child{text-align:left}
.note{font-size:12px;color:var(--text-secondary);margin:6px 0 0}
"""

JS = """
document.querySelectorAll('[data-chart]').forEach(card=>{
  const spec=JSON.parse(card.querySelector('script[type="application/json"]').textContent);
  const svg=card.querySelector('svg'), tip=card.querySelector('.tip'), hit=svg.querySelector('.hit');
  const cross=svg.querySelector('.cross');
  const show=(html,x,y)=>{tip.innerHTML=html;tip.style.display='block';
    const r=card.getBoundingClientRect();let left=x-r.left+14;if(left+tip.offsetWidth>r.width)left=x-r.left-tip.offsetWidth-14;
    tip.style.left=left+'px';tip.style.top=(y-r.top+14)+'px'};
  const hide=()=>{tip.style.display='none';if(cross)cross.style.display='none'};
  if(spec.kind==='line'){
    hit.addEventListener('mousemove',ev=>{
      const pt=svg.createSVGPoint();pt.x=ev.clientX;pt.y=ev.clientY;const p=pt.matrixTransform(svg.getScreenCTM().inverse());
      let best=0,bd=1e9;spec.x.forEach((x,i)=>{const d=Math.abs(spec.px[i]-p.x);if(d<bd){bd=d;best=i}});
      cross.setAttribute('x1',spec.px[best]);cross.setAttribute('x2',spec.px[best]);cross.style.display='block';
      let h='<b>'+spec.xl[best]+' &middot; '+spec.phase[best]+'</b>';
      spec.series.forEach(s=>{h+='<div><i style="background:'+s.color+'"></i>'+s.name+': '+s.y[best]+'</div>'});
      show(h,ev.clientX,ev.clientY)});
    hit.addEventListener('mouseleave',hide);
  } else {
    card.querySelectorAll('[data-tip]').forEach(el=>{
      el.addEventListener('mousemove',ev=>show(el.dataset.tip,ev.clientX,ev.clientY));el.addEventListener('mouseleave',hide)});
  }
});
"""


def ticks(vmax):
    """Round axis: the smallest 1-2-5 step that covers vmax in at most 5 intervals."""
    vmax = max(vmax, 1)
    step = next(s * m for m in (1, 10, 100, 1000, 10000) for s in (1, 2, 5) if s * m * 5 >= vmax)
    n = -(-vmax // step)  # ceil
    return [step * i for i in range(int(n) + 1)], step * n


def esc(s):
    return html.escape(str(s), quote=True)


def line_chart(r, series, title, subtitle, note=""):
    """series: [(name, css var, [values per x])], x = r['xs'] seconds. One y axis, phase bands, hover crosshair."""
    W, H, L, R, T, B = 1000, 300, 48, 120, 28, 30
    xs = r["xs"]
    xmax = max(xs[-1], 1)
    ymax_raw = max((max(v) for _, _, v in series), default=1)
    yt, ytop = ticks(ymax_raw)
    X = lambda t: L + (W - L - R) * t / xmax
    Y = lambda v: T + (H - T - B) * (1 - v / ytop)
    out = [f'<svg viewBox="0 0 {W} {H}" role="img" aria-label="{esc(title)}">']
    # phase bands (neutral, alternating) + labels
    edges = [s for _, s in r["boundaries"]] + [r["end"]]
    for i, (name, start) in enumerate(r["boundaries"]):
        x1, x2 = X(start), X(edges[i + 1])
        if i % 2 == 1:
            out.append(f'<rect x="{x1:.1f}" y="{T}" width="{x2 - x1:.1f}" height="{H - T - B}" fill="var(--band)"/>')
        out.append(f'<text class="lbl" x="{x1 + 6:.1f}" y="{T - 10}">{esc(name)}</text>')
    if r["end"] < xmax:
        out.append(f'<text class="lbl" x="{X(r["end"]) + 6:.1f}" y="{T - 10}">drain</text>')
    for v in yt:
        out.append(f'<line x1="{L}" x2="{W - R}" y1="{Y(v):.1f}" y2="{Y(v):.1f}" stroke="var(--grid)" stroke-width="1"/>'
                   f'<text x="{L - 8}" y="{Y(v) + 4:.1f}" text-anchor="end">{v:g}</text>')
    for t in range(0, int(xmax) + 1, 60):
        out.append(f'<text x="{X(t):.1f}" y="{H - 10}" text-anchor="middle">{mmss(t)}</text>')
    out.append(f'<line x1="{L}" x2="{W - R}" y1="{Y(0):.1f}" y2="{Y(0):.1f}" stroke="var(--axis)" stroke-width="1"/>')
    for name, var, vals in series:
        pts = " ".join(f"{X(t):.1f},{Y(v):.1f}" for t, v in zip(xs, vals))
        out.append(f'<polyline points="{pts}" fill="none" stroke="var({var})" stroke-width="2" '
                   'stroke-linejoin="round" stroke-linecap="round"/>')
    # direct end labels, nudged apart
    ends = sorted(((Y(v[-1]), n, var, v[-1]) for n, var, v in series), key=lambda e: e[0])
    placed = []
    for y, n, var, v in ends:
        y = max(y, placed[-1] + 14) if placed else y
        placed.append(y)
        out.append(f'<circle cx="{X(xs[-1]):.1f}" cy="{Y(v):.1f}" r="4" fill="var({var})" stroke="var(--surface-1)" stroke-width="2"/>'
                   f'<text class="lbl" x="{X(xs[-1]) + 10:.1f}" y="{y + 4:.1f}">{esc(n)} {v}</text>')
    out.append(f'<line class="cross" x1="0" x2="0" y1="{T}" y2="{H - B}" stroke="var(--axis)" stroke-width="1" style="display:none"/>')
    out.append(f'<rect class="hit" x="{L}" y="{T}" width="{W - L - R}" height="{H - T - B}" fill="transparent"/></svg>')
    spec = {"kind": "line", "px": [round(X(t), 1) for t in xs], "xl": [mmss(t) for t in xs], "phase": r["phase_at"],
            "series": [{"name": n, "color": f"var({var})", "y": v} for n, var, v in series]}
    legend = "" if len(series) < 2 else '<div class="legend">' + "".join(
        f'<span><i style="background:var({var})"></i>{esc(n)}</span>' for n, var, _ in series) + "</div>"
    rows = "".join(f"<tr><td>{mmss(t)}</td><td>{esc(p)}</td>" + "".join(f"<td>{v[i]}</td>" for _, _, v in series) + "</tr>"
                   for i, (t, p) in enumerate(zip(xs, r["phase_at"])) if i % 6 == 0 or i == len(xs) - 1)
    table = (f'<details><summary>Table</summary><table><tr><th>time</th><th>phase</th>'
             + "".join(f"<th>{esc(n)}</th>" for n, _, _ in series) + f"</tr>{rows}</table></details>")
    return (f'<div class="card" data-chart><h2>{esc(title)}</h2><p class="sub" style="margin:0">{esc(subtitle)}</p>'
            f'{legend}{"".join(out)}{f"<p class=note>{esc(note)}</p>" if note else ""}{table}<div class="tip"></div>'
            f'<script type="application/json">{json.dumps(spec)}</script></div>')


def bar_chart(rows, title, subtitle, note=""):
    """rows: [(label, measured share, expected share, n)]; one series (slot 1) + expected as an ink tick."""
    W, L, R, rowh = 1000, 150, 80, 34
    H = 20 + rowh * len(rows) + 24
    top = max(max(m, e) for _, m, e, _ in rows) or 1
    top = min(1.0, (int(top * 10) + 1) / 10)
    X = lambda v: L + (W - L - R) * v / top
    out = [f'<svg viewBox="0 0 {W} {H}" role="img" aria-label="{esc(title)}">']
    for i in range(0, int(round(top * 10)) + 1):
        v = i / 10
        out.append(f'<line x1="{X(v):.1f}" x2="{X(v):.1f}" y1="10" y2="{H - 24}" stroke="var(--grid)"/>'
                   f'<text x="{X(v):.1f}" y="{H - 8}" text-anchor="middle">{int(v * 100)}%</text>')
    for i, (label, m, e, n) in enumerate(rows):
        y = 20 + i * rowh
        w = max(X(m) - L, 0)
        tip = esc(f"<b>{label}</b>measured {pct(m)} ({n})<br>expected {pct(e)}")
        bar = (f'<path d="M{L},{y + 4} h{w - 4:.1f} a4,4 0 0 1 4,4 v10 a4,4 0 0 1 -4,4 h-{w - 4:.1f} z" '
               f'fill="var(--s1)" data-tip="{tip}"/>') if w >= 4 else ""
        out.append(f'<text class="lbl" x="{L - 12}" y="{y + 16}" text-anchor="end">{esc(label)}</text>{bar}'
                   f'<line x1="{X(e):.1f}" x2="{X(e):.1f}" y1="{y}" y2="{y + 26}" stroke="var(--text-primary)" stroke-width="2"/>'
                   f'<rect x="{L}" y="{y}" width="{W - L - R}" height="26" fill="transparent" data-tip="{tip}"/>'
                   f'<text class="lbl" x="{X(max(m, e)) + 10:.1f}" y="{y + 16}">{pct(m)}</text>')
    out.append(f'<line x1="{L}" x2="{L}" y1="10" y2="{H - 24}" stroke="var(--axis)"/></svg>')
    legend = ('<div class="legend"><span><i class="dot" style="background:var(--s1)"></i>measured</span>'
              '<span><i class="tick"></i>expected (flaky.py)</span></div>')
    table = ("<details><summary>Table</summary><table><tr><th></th><th>measured</th><th>n</th><th>expected</th></tr>"
             + "".join(f"<tr><td>{esc(l)}</td><td>{pct(m)}</td><td>{n}</td><td>{pct(e)}</td></tr>" for l, m, e, n in rows)
             + "</table></details>")
    return (f'<div class="card" data-chart><h2>{esc(title)}</h2><p class="sub" style="margin:0">{esc(subtitle)}</p>'
            f'{legend}{"".join(out)}{f"<p class=note>{esc(note)}</p>" if note else ""}{table}<div class="tip"></div>'
            '<script type="application/json">{"kind":"bar"}</script></div>')


def html_report(r, a):
    xs = [s["t"] for s in r["samples"]]
    r = dict(r, xs=xs, phase_at=[s["phase"] for s in r["samples"]])
    cum = {o: [] for o in OUTCOMES}
    for t in xs:
        for o in OUTCOMES:
            cum[o].append(sum(1 for x in r["outcomes"] if x["stage"] == o and x["t"] <= t))
    share_invalid = a["invalid"] / a["generated"] if a["generated"] else 0
    tiles = [("Delivered", pct(a["measured"]["ok"]), f"expected {pct(EXPECTED['ok'])} · normal phase"),
             ("HTTP 400 → DLQ", pct(a["measured"]["http-400"]), f"expected {pct(EXPECTED['http-400'])} · no retries"),
             ("Retries exceeded", pct(a["measured"]["retries-exceeded"]),
              f"expected {pct(EXPECTED['retries-exceeded'])} · four 503 in a row"),
             ("FlowFiles with invalid records", pct(share_invalid), f"expected {pct(EXPECTED_INVALID)} · whole run")]
    tiles_html = '<div class="tiles">' + "".join(
        f'<div class="tile"><div class="label">{esc(l)}</div><div class="value">{v}</div><div class="exp">{esc(e)}</div></div>'
        for l, v, e in tiles) + "</div>"
    queue = line_chart(r, [("queued", "--s1",
                            [s["retry_wait"] + s["to_retry"] for s in r["samples"]])],
                       "Retry loop", "FlowFiles queued between InvokeHTTP and RetryFlowFile, every "
                       f"{SAMPLE} s",
                       "While flaky is down the loop holds a steady number of FlowFiles (generator rate × ~30 s "
                       "in the loop) instead of growing: they leave for the DLQ as fast as new ones arrive.")
    outcomes = line_chart(r, [("ok", "--s1", cum["ok"]), ("validation", "--s2", cum["validation"]),
                              ("http-400", "--s3", cum["http-400"]),
                              ("retries-exceeded", "--s4", cum["retries-exceeded"])],
                          "Where the FlowFiles ended up", "Cumulative FlowFiles written to ok and to each DLQ directory")
    out_rows = [(k, a["measured"][k], EXPECTED[k], a["by_phase"]["normal"][k]) for k in EXPECTED]
    bars1 = bar_chart(out_rows, "Outcome of a valid FlowFile — normal phase",
                      f"n = {a['sent']} FlowFiles that passed validation and reached ok or the DLQ")
    att_rows = [(f"{k} attempt{'s' if k > 1 else ''}", (a["attempts"][k] / a["delivered"]) if a["delivered"] else 0,
                 EXPECTED_ATTEMPTS[k], a["attempts"][k]) for k in range(1, ATTEMPTS + 1)]
    bars2 = bar_chart(att_rows, "Attempts needed by a delivered FlowFile — normal phase",
                      f"n = {a['delivered']}; attempts = flowfile.retries + 1",
                      "Four attempts is the maximum: Maximum Retries = 3 in RetryFlowFile.")
    facts = [("max FlowFiles in the retry loop", f"normal {a['normal_q_max']}, outage {a['outage_q_max']}"),
             ("first retries-exceeded after docker stop",
              f"{a['first_exceeded']:.0f} s" if a["first_exceeded"] is not None else "none"),
             ("first delivery after docker start",
              f"{a['first_ok_after']:.0f} s" if a["first_ok_after"] is not None else "none"),
             ("error.reason of retries-exceeded", "; ".join(a["reasons"] or ["-"]))]
    facts_html = ('<div class="card"><h2>Outage and recovery</h2><table>' + "".join(
        f"<tr><td>{esc(k)}</td><td>{esc(v)}</td></tr>" for k, v in facts) + "</table></div>")
    when = time.strftime("%Y-%m-%d %H:%M", time.localtime(r["started"]))
    phases = ", ".join(f"{p} {s // 60}:{s % 60:02d}" for p, s in PHASES)
    return (f'<!doctype html><html lang="en"><head><meta charset="utf-8"><title>Lab 8 measurements {when}</title>'
            f'<meta name="viewport" content="width=device-width,initial-scale=1"><style>{CSS}</style></head><body>'
            f'<div class="viz-root"><h1>Lab 8 — retry and error handling</h1>'
            f'<p class="sub">Run of {when}. Phases: {esc(phases)} (flaky stopped during "outage"). '
            f'{a["generated"]} FlowFiles generated, 5 records each.</p>'
            f'{tiles_html}{queue}{outcomes}{bars1}{bars2}{facts_html}</div><script>{JS}</script></body></html>')


def main():
    total = sum(s for _, s in PHASES)
    print(f"Lab 8 measurement: about {total // 60} min {total % 60} s plus up to 1 min of draining")
    r = run()
    a = analyse(r)
    report = text_report(r, a)
    print("\n" + report)
    os.makedirs(RESULTS, exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M%S", time.localtime(r["started"]))
    base = os.path.join(RESULTS, f"lab8-measure-{stamp}")
    with open(base + ".txt", "w") as f:
        f.write(report)
    with open(base + ".html", "w") as f:
        f.write(html_report(r, a))
    print(f"report: {os.path.relpath(base, os.getcwd())}.txt and .html")


if __name__ == "__main__":
    main()
