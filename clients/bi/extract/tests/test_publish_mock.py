"""Exercise publish_hyper() against a mock Tableau REST API.

Not a Tableau Server — a local HTTP server speaking just enough of the
Tableau REST protocol (serverInfo version negotiation, PAT sign-in,
project listing, datasource publish) to prove the tableauserverclient
request flow this package drives: version negotiation happens, the PAT is
presented, the right project is selected by name, the .hyper payload and
its overwrite mode reach the publish endpoint, and the returned datasource
id is surfaced. A real-site refresh cycle remains the by-construction leg
documented in the README.

Run: python3 -m unittest discover clients/bi/extract/tests
Skips cleanly when tableauserverclient is not installed.
"""

import threading
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer

try:
    import tableauserverclient  # noqa: F401
    HAVE_TSC = True
except ImportError:
    HAVE_TSC = False

NS = "http://tableau.com/api"
SITE_ID = "11111111-2222-3333-4444-555555555555"
USER_ID = "66666666-7777-8888-9999-aaaaaaaaaaaa"
PROJECT_ID = "bbbbbbbb-cccc-dddd-eeee-ffffffffffff"
DS_ID = "12121212-3434-5656-7878-909090909090"


class MockTableauHandler(BaseHTTPRequestHandler):
    """Minimal Tableau REST API: XML in, XML out, everything recorded."""

    requests_seen = []

    def _xml(self, body, code=200):
        payload = f'<?xml version="1.0" encoding="UTF-8"?>{body}'.encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/xml")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        self.requests_seen.append(("GET", self.path, b""))
        if "serverInfo" in self.path:
            self._xml(
                f'<tsResponse xmlns="{NS}"><serverInfo>'
                "<productVersion build=\"20244.1\">2024.4</productVersion>"
                "<restApiVersion>3.24</restApiVersion>"
                "</serverInfo></tsResponse>"
            )
        elif "/projects" in self.path:
            self._xml(
                f'<tsResponse xmlns="{NS}">'
                '<pagination pageNumber="1" pageSize="100" totalAvailable="1"/>'
                f'<projects><project id="{PROJECT_ID}" name="Lakehouse"/></projects>'
                "</tsResponse>"
            )
        else:
            self._xml(f'<tsResponse xmlns="{NS}"/>', 404)

    def do_POST(self):
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        self.requests_seen.append(("POST", self.path, body))
        if self.path.endswith("/auth/signin"):
            self._xml(
                f'<tsResponse xmlns="{NS}"><credentials token="mock-token">'
                f'<site id="{SITE_ID}" contentUrl=""/><user id="{USER_ID}"/>'
                "</credentials></tsResponse>"
            )
        elif self.path.endswith("/auth/signout"):
            self.send_response(204)
            self.send_header("Content-Length", "0")
            self.end_headers()
        elif "/datasources" in self.path:
            self._xml(
                f'<tsResponse xmlns="{NS}">'
                f'<datasource id="{DS_ID}" name="trips">'
                f'<project id="{PROJECT_ID}"/></datasource></tsResponse>',
                201,
            )
        else:
            self._xml(f'<tsResponse xmlns="{NS}"/>', 404)

    def log_message(self, *args):  # keep unittest output clean
        pass


@unittest.skipUnless(HAVE_TSC, "tableauserverclient not installed")
class PublishHyperMockTest(unittest.TestCase):
    def test_publish_flow(self):
        import os
        import sys
        import tempfile

        sys.path.insert(
            0, os.path.join(os.path.dirname(__file__), "..")
        )
        from icegres_bi_extract.core import publish_hyper

        MockTableauHandler.requests_seen = []
        server = HTTPServer(("127.0.0.1", 0), MockTableauHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.NamedTemporaryFile(suffix=".hyper", delete=False) as f:
                f.write(b"HYPERBYTES-not-a-real-extract")
                path = f.name
            ds_id = publish_hyper(
                hyper_path=path,
                server_url=f"http://127.0.0.1:{server.server_address[1]}",
                site="",
                project="Lakehouse",
                token_name="refresh-bot",
                token_value="secret-token-value",
                datasource_name="trips",
            )
        finally:
            server.shutdown()
            thread.join(timeout=5)

        self.assertEqual(ds_id, DS_ID)
        methods_paths = [(m, p) for m, p, _ in MockTableauHandler.requests_seen]
        self.assertTrue(any("serverInfo" in p for _, p in methods_paths),
                        "version negotiation did not happen")
        signin = [b for m, p, b in MockTableauHandler.requests_seen
                  if p.endswith("/auth/signin")]
        self.assertTrue(signin and b"refresh-bot" in signin[0]
                        and b"secret-token-value" in signin[0],
                        "PAT credentials not presented at sign-in")
        publishes = [(p, b) for m, p, b in MockTableauHandler.requests_seen
                     if m == "POST" and "/datasources" in p]
        self.assertTrue(publishes, "no publish request reached the datasources endpoint")
        pub_path, pub_body = publishes[0]
        self.assertIn("overwrite=true", pub_path,
                      "publish did not request overwrite mode")
        self.assertIn(b"HYPERBYTES-not-a-real-extract", pub_body,
                      "the .hyper payload was not uploaded")
        self.assertIn(PROJECT_ID.encode(), pub_body,
                      "the resolved project id was not in the publish payload")


if __name__ == "__main__":
    unittest.main()
