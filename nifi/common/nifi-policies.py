#!/usr/bin/env python3
"""Ensure access policies in a NiFi instance (single node or cluster). Idempotent.

- admin:       full access to the root process group and global resources
                (in a cluster the initial admin misses root group policies when
                authorizations.xml is generated before the flow exists)
- cluster nodes (CN=nifiN, OU=NIFI): view/modify data, view provenance, so they can
                proxy queue listing / provenance requests
- prometheus (CN=prometheus, OU=NIFI): read-only /flow, required for
                /nifi-api/flow/metrics/prometheus; the user is created if missing
- registry client "NiFi Registry" -> https://nifi-registry:18443 (env REGISTRY_URL)

Runs inside the nifi-net network (see Makefile target "policies"):
  python3 nifi/common/nifi-policies.py https://nifi1:8443
Env: NIFI_USERNAME, NIFI_PASSWORD, CA_FILE (default /certs/ca.crt), WAIT_SECONDS (default 600)
"""
import json
import os
import re
import ssl
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

BASE = (sys.argv[1] if len(sys.argv) > 1 else "https://nifi:8443").rstrip("/") + "/nifi-api"
USER = os.environ.get("NIFI_USERNAME", "admin")
PASSWORD = os.environ["NIFI_PASSWORD"]
CA_FILE = os.environ.get("CA_FILE", "/certs/ca.crt")
WAIT_SECONDS = int(os.environ.get("WAIT_SECONDS", "600"))

PROMETHEUS_IDENTITY = "CN=prometheus, OU=NIFI"
NODE_IDENTITY = re.compile(r"^CN=nifi\d+, OU=NIFI$")

CTX = ssl.create_default_context(cafile=CA_FILE)


def call(method, path, body=None, token=None, form=None):
    headers = {}
    data = None
    if token:
        headers["Authorization"] = f"Bearer {token}"
    if form is not None:
        data = urllib.parse.urlencode(form).encode()
        headers["Content-Type"] = "application/x-www-form-urlencoded"
    elif body is not None:
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(BASE + path, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, context=CTX, timeout=30) as resp:
        raw = resp.read().decode()
        return raw if form is not None else (json.loads(raw) if raw else None)


def login():
    """Wait until NiFi accepts logins (and, in a cluster, the node has joined)."""
    deadline = time.time() + WAIT_SECONDS
    last_error = None
    while time.time() < deadline:
        try:
            token = call("POST", "/access/token", form={"username": USER, "password": PASSWORD})
            call("GET", "/flow/process-groups/root", token=token)
            return token
        except urllib.error.HTTPError as e:
            if e.code in (400, 401, 403):
                sys.exit(f"Login to {BASE} as '{USER}' failed: HTTP {e.code} {e.read().decode()[:200]}")
            last_error = e  # e.g. 409/503 while the cluster is still forming
            print(f"waiting for NiFi at {BASE} ... (HTTP {e.code})", flush=True)
            time.sleep(10)
        except (urllib.error.URLError, ConnectionError, TimeoutError, OSError) as e:
            last_error = e
            print(f"waiting for NiFi at {BASE} ... ({e})", flush=True)
            time.sleep(10)
    sys.exit(f"NiFi at {BASE} is not ready after {WAIT_SECONDS}s: {last_error}")


token = login()

users = call("GET", "/tenants/users", token=token)["users"]
by_identity = {u["component"]["identity"]: u for u in users}

if PROMETHEUS_IDENTITY not in by_identity:
    created = call("POST", "/tenants/users", {
        "revision": {"version": 0},
        "component": {"identity": PROMETHEUS_IDENTITY},
    }, token=token)
    by_identity[PROMETHEUS_IDENTITY] = created
    print(f"created user {PROMETHEUS_IDENTITY}")

admin = {"id": by_identity[USER]["id"]}
prometheus = {"id": by_identity[PROMETHEUS_IDENTITY]["id"]}
nodes = [{"id": u["id"]} for ident, u in by_identity.items() if NODE_IDENTITY.match(ident)]
root_id = call("GET", "/flow/process-groups/root", token=token)["processGroupFlow"]["id"]

R, W, RW = ("read",), ("write",), ("read", "write")
# (resource, actions, members)
POLICIES = [
    ("/flow", R, [admin, prometheus]),
    ("/system", R, [admin]),
    ("/provenance", R, [admin]),
    ("/counters", RW, [admin]),
    ("/restricted-components", W, [admin]),
    ("/parameter-contexts", RW, [admin]),
    ("/site-to-site", R, [admin]),
    ("/tenants", RW, [admin]),
    ("/policies", RW, [admin]),
    ("/controller", RW, [admin]),
    (f"/process-groups/{root_id}", RW, [admin]),
    (f"/data/process-groups/{root_id}", RW, [admin] + nodes),
    (f"/operation/process-groups/{root_id}", W, [admin]),
    (f"/provenance-data/process-groups/{root_id}", R, [admin] + nodes),
    (f"/policies/process-groups/{root_id}", RW, [admin]),
]

for resource, actions, wanted in POLICIES:
    for action in actions:
        try:
            policy = call("GET", f"/policies/{action}{resource}", token=token)
        except urllib.error.HTTPError as e:
            if e.code != 404:
                raise
            policy = None

        # GET may return an inherited policy for a different resource; only reuse an exact match
        if policy and policy["component"]["resource"] == resource:
            members = policy["component"].get("users", [])
            existing = {m["id"] for m in members}
            missing = [r for r in wanted if r["id"] not in existing]
            if not missing:
                print(f"ok      {action:5} {resource}")
                continue
            policy["component"]["users"] = [{"id": m["id"]} for m in members] + missing
            call("PUT", f"/policies/{policy['id']}",
                 {"revision": policy["revision"], "component": policy["component"]}, token=token)
            print(f"updated {action:5} {resource}")
        else:
            call("POST", "/policies", {
                "revision": {"version": 0},
                "component": {"resource": resource, "action": action, "users": wanted, "userGroups": []},
            }, token=token)
            print(f"created {action:5} {resource}")

print("Policies are up to date.")

# Registry client for the secured NiFi Registry (nifi/registry). No SSL Context Service:
# NiFi falls back to its own keystore/truststore and proxies the logged-in user.
REGISTRY_URL = os.environ.get("REGISTRY_URL", "https://nifi-registry:18443")
REGISTRY_TYPE = "org.apache.nifi.registry.flow.NifiRegistryFlowRegistryClient"

clients = call("GET", "/controller/registry-clients", token=token)["registries"]
client = next((c for c in clients if c["component"]["type"] == REGISTRY_TYPE), None)
if client is None:
    types = call("GET", "/controller/registry-types", token=token)["flowRegistryClientTypes"]
    bundle = next(t["bundle"] for t in types if t["type"] == REGISTRY_TYPE)
    call("POST", "/controller/registry-clients", {
        "revision": {"version": 0},
        "component": {"name": "NiFi Registry", "type": REGISTRY_TYPE, "bundle": bundle,
                      "properties": {"url": REGISTRY_URL}},
    }, token=token)
    print(f"created registry client -> {REGISTRY_URL}")
elif client["component"]["properties"].get("url") != REGISTRY_URL:
    call("PUT", f"/controller/registry-clients/{client['id']}", {
        "revision": client["revision"],
        "component": {"id": client["id"], "properties": {"url": REGISTRY_URL}},
    }, token=token)
    print(f"updated registry client -> {REGISTRY_URL}")
else:
    print(f"ok      registry client -> {REGISTRY_URL}")
