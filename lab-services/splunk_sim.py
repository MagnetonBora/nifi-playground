"""Splunk emulator for Lab 7.

  TCP  :5514  newline-delimited events -> /data/splunk_in.log   (PutSplunk)
  HTTP :8088  POST /services/collector/event (HEC) -> /data/hec_in.log, replies like Splunk   (InvokeHTTP)
"""
import os
import socketserver
import threading

from common import Quiet, serve_http

DATA = os.environ.get("DATA_DIR", "/data")


class Hec(Quiet):
    def do_POST(self):
        body = self.read_body()
        with open(os.path.join(DATA, "hec_in.log"), "ab") as f:
            f.write(body if body.endswith(b"\n") else body + b"\n")
        self.reply(200, b'{"text":"Success","code":0}')


class SplunkTcp(socketserver.StreamRequestHandler):
    lock = threading.Lock()

    def handle(self):
        path = os.path.join(DATA, "splunk_in.log")
        for line in self.rfile:
            with self.lock, open(path, "ab") as f:
                f.write(line if line.endswith(b"\n") else line + b"\n")


class ThreadingTcp(socketserver.ThreadingMixIn, socketserver.TCPServer):
    allow_reuse_address = True
    daemon_threads = True


if __name__ == "__main__":
    os.makedirs(DATA, exist_ok=True)
    threading.Thread(target=serve_http, args=(8088, Hec), daemon=True).start()
    print("listening on :5514 (Splunk TCP input)", flush=True)
    ThreadingTcp(("0.0.0.0", 5514), SplunkTcp).serve_forever()
