"""Shared helpers for the lab stub services."""
import sys
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Quiet(BaseHTTPRequestHandler):
    """Request handler with one short log line per request and a small reply helper."""

    def log_message(self, fmt, *args):
        sys.stdout.write("%s %s\n" % (self.command, fmt % args))
        sys.stdout.flush()

    def reply(self, code, body=b"", content_type="application/json"):
        self.send_response(code)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body:
            self.wfile.write(body)

    def read_body(self):
        return self.rfile.read(int(self.headers.get("Content-Length", 0) or 0))


def serve_http(port, handler):
    print("listening on :%d (%s)" % (port, handler.__name__), flush=True)
    ThreadingHTTPServer(("0.0.0.0", port), handler).serve_forever()
