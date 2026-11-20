import random
from http.server import BaseHTTPRequestHandler, HTTPServer


HOST = "0.0.0.0"
PORT = 9099


class RequestHandler(BaseHTTPRequestHandler):

    def do_POST(self):
        # Read the request body
        content_length = int(self.headers.get("Content-Length", 0))
        self.rfile.read(content_length)

        # Generate a random response status
        random_value = random.random()

        if random_value < 0.6:
            status_code = 200
        elif random_value < 0.9:
            status_code = 503
        else:
            status_code = 400

        # Send the response
        self.send_response(status_code)
        self.end_headers()
        self.wfile.write(b"{}")


if __name__ == "__main__":
    server = HTTPServer((HOST, PORT), RequestHandler)
    server.serve_forever()
