#!/usr/bin/env python3
"""Import flow definitions (JSON) into the root canvas of a NiFi instance via the REST API.

Each file becomes a new Process Group (stopped), placed to the right of what is already on the
canvas. Prints the validation state of every processor and controller service afterwards.

Runs inside nifi-net (see Makefile targets "import" / "import-cluster"):
  python3 nifi-import.py https://nifi:8443 /flows/lab2-multi-source-routing.json [more.json ...]
Env: NIFI_USERNAME, NIFI_PASSWORD, CA_FILE (default /certs/ca.crt)
"""
import json
import os
import ssl
import sys
import urllib.error
import urllib.parse
import urllib.request
import uuid

BASE = sys.argv[1].rstrip("/") + "/nifi-api"
FILES = sys.argv[2:]
USER = os.environ.get("NIFI_USERNAME", "admin")
PASSWORD = os.environ["NIFI_PASSWORD"]
CTX = ssl.create_default_context(cafile=os.environ.get("CA_FILE", "/certs/ca.crt"))


def call(method, path, token=None, data=None, headers=None):
    req = urllib.request.Request(BASE + path, data=data, method=method, headers=dict(headers or {}))
    if token:
        req.add_header("Authorization", f"Bearer {token}")
    try:
        with urllib.request.urlopen(req, context=CTX, timeout=60) as resp:
            return resp.read().decode()
    except urllib.error.HTTPError as e:
        sys.exit(f"{method} {path} -> HTTP {e.code}: {e.read().decode()[:800]}")


def multipart(fields, file_name, file_bytes):
    boundary = uuid.uuid4().hex
    parts = []
    for k, v in fields.items():
        parts.append(f'--{boundary}\r\nContent-Disposition: form-data; name="{k}"\r\n\r\n{v}\r\n'.encode())
    parts.append((f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="{file_name}"\r\n'
                  f"Content-Type: application/json\r\n\r\n").encode() + file_bytes + b"\r\n")
    parts.append(f"--{boundary}--\r\n".encode())
    return b"".join(parts), f"multipart/form-data; boundary={boundary}"


if not FILES:
    sys.exit("usage: nifi-import.py <base-url> <flow.json> [...]")

token = call("POST", "/access/token",
             data=urllib.parse.urlencode({"username": USER, "password": PASSWORD}).encode(),
             headers={"Content-Type": "application/x-www-form-urlencoded"})
root = json.loads(call("GET", "/flow/process-groups/root", token))["processGroupFlow"]
root_id = root["id"]
flow = root["flow"]
xs = [c["position"]["x"] for kind in ("processGroups", "processors", "inputPorts", "outputPorts", "funnels", "labels")
      for c in (x["component"] for x in flow[kind])]
x = (max(xs) + 450) if xs else 0

for path in FILES:
    with open(path, "rb") as f:
        raw = f.read()
    name = json.loads(raw)["flowContents"]["name"]
    body, ctype = multipart({"groupName": name, "positionX": x, "positionY": 0, "clientId": "nifi-import"},
                            os.path.basename(path), raw)
    pg = json.loads(call("POST", f"/process-groups/{root_id}/process-groups/upload", token, body,
                         {"Content-Type": ctype}))
    pg_id = pg["id"]
    print(f"imported '{name}' -> process group {pg_id}")

    procs = json.loads(call("GET", f"/process-groups/{pg_id}/processors?includeDescendantGroups=true", token))
    for p in procs.get("processors", []):
        c = p["component"]
        errors = c.get("validationErrors") or []
        print(f"  processor {c['name']:28} {c['validationStatus']}" + (f"  {errors}" if errors else ""))
    services = json.loads(call("GET", f"/flow/process-groups/{pg_id}/controller-services"
                                      "?includeAncestorGroups=false&includeDescendantGroups=true", token))
    for s in services.get("controllerServices", []):
        c = s["component"]
        errors = c.get("validationErrors") or []
        print(f"  service   {c['name']:28} {c['state']} {c.get('validationStatus', '')}" + (f"  {errors}" if errors else ""))
    x += 450
