"""Live regression checks for authorization, replicas, schema evolution and commits.

Run against an isolated Lakekeeper/S3 test stack with psycopg2, pyiceberg,
pyarrow and adbc-driver-flightsql installed. Every server and table belongs
to this run. Missing dependencies or services fail the run.
"""

import contextlib
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import psycopg2
import pyarrow as pa
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField, StringType


BIN = Path(os.environ.get("ICEGRES_BIN", Path(__file__).parents[1] / "target/release/icegres"))
CATALOG_URI = os.environ.get("ICEGRES_CATALOG_URI", "http://127.0.0.1:8181/catalog")
WAREHOUSE = os.environ.get("ICEGRES_WAREHOUSE", "lakehouse")
S3_ENDPOINT = os.environ.get("ICEGRES_S3_ENDPOINT", "http://127.0.0.1:9000")


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


class LocalTestCatalog(RestCatalog):
    """Use explicit local S3 credentials for the independent data reader.

    Lakekeeper can advertise remote signing even for this local test stack.
    This suite tests storage contents, not its credential vending service.
    """

    def _load_file_io(self, properties=None, location=None):
        return PyArrowFileIO({
            key: value for key, value in self.properties.items()
            if key.startswith("s3.") and key != "s3.signer"
        })


class LostResponseProxy:
    """Forward a real catalog commit, then deliberately lose its response."""

    def __init__(self):
        self.lose_next = threading.Event()
        self.lost = 0
        self.commits = 0
        self.block_after_loss = False
        self.block_metadata = threading.Event()
        owner = self
        upstream = CATALOG_URI.removesuffix("/catalog")

        class Handler(BaseHTTPRequestHandler):
            def forward(self):
                if self.command == "GET" and "/tables/" in self.path and owner.block_metadata.is_set():
                    self.send_response(503)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
                req = urllib.request.Request(
                    upstream + self.path,
                    data=body if body else None,
                    method=self.command,
                    headers={"Content-Type": "application/json"},
                )
                try:
                    response = urllib.request.urlopen(req, timeout=20)
                except urllib.error.HTTPError as error:
                    response = error
                with response:
                    payload = response.read()
                    status = response.status
                is_commit = self.command == "POST" and (
                    "/tables/" in self.path or self.path.endswith("/transactions/commit")
                )
                if is_commit:
                    owner.commits += 1
                if is_commit and owner.lose_next.is_set() and 200 <= status < 300:
                    owner.lose_next.clear()
                    owner.lost += 1
                    if owner.block_after_loss:
                        owner.block_metadata.set()
                    self.close_connection = True
                    self.connection.shutdown(socket.SHUT_RDWR)
                    self.connection.close()
                    return
                if self.path.startswith("/catalog/v1/config") and status == 200:
                    config = json.loads(payload)
                    config.setdefault("overrides", {})["uri"] = owner.uri
                    payload = json.dumps(config).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            do_GET = forward
            do_POST = forward
            do_HEAD = forward
            do_DELETE = forward

            def log_message(self, *args):
                pass

        class ProxyServer(ThreadingHTTPServer):
            request_queue_size = 128

        self.server = ProxyServer(("127.0.0.1", 0), Handler)
        self.uri = f"http://127.0.0.1:{self.server.server_port}/catalog"
        self.thread = threading.Thread(target=self.server.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        return self

    def __exit__(self, *_):
        self.server.shutdown()
        self.server.server_close()
        self.thread.join(timeout=5)


class SafetyRegressions(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        if not BIN.is_file():
            raise RuntimeError(f"Build the release binary first: {BIN}")
        cls.tmp = tempfile.TemporaryDirectory(prefix="icegres-safety-")
        cls.directory = Path(cls.tmp.name)
        cls.namespace = "safety_" + uuid.uuid4().hex[:12]
        cls.catalog = LocalTestCatalog(
            "safety", type="rest", uri=CATALOG_URI, warehouse=WAREHOUSE,
            **{
                "s3.endpoint": S3_ENDPOINT,
                "s3.access-key-id": os.environ.get("ICEGRES_S3_ACCESS_KEY", "rustfsadmin"),
                "s3.secret-access-key": os.environ.get("ICEGRES_S3_SECRET_KEY", "rustfssecret"),
                "s3.region": os.environ.get("ICEGRES_S3_REGION", "us-east-1"),
            },
        )
        cls.catalog.create_namespace(cls.namespace)
        cls.tables = []

    @classmethod
    def tearDownClass(cls):
        for ident in cls.catalog.list_tables(cls.namespace):
            cls.catalog.drop_table(ident)
        cls.catalog.drop_namespace(cls.namespace)
        cls.tmp.cleanup()

    def table(self, name, *, pk=False):
        ident = (self.namespace, name)
        table = self.catalog.create_table(
            ident,
            schema=Schema(
                NestedField(1, "id", LongType(), required=False),
                NestedField(2, "v", StringType(), required=False),
            ),
            properties={"format-version": "2", **({"icegres.primary-key": "id"} if pk else {})},
        )
        self.tables.append(ident)
        table.append(pa.table({"id": pa.array([1], type=pa.int64()), "v": ["old value"]}))
        return table

    @contextlib.contextmanager
    def server(self, *flags, catalog_uri=CATALOG_URI, env=None, flight=False):
        port = free_port()
        log_path = self.directory / f"server-{port}.log"
        child_env = {k: v for k, v in os.environ.items() if not k.startswith("ICEGRES_")}
        child_env.update(
            ICEGRES_CATALOG_URI=catalog_uri,
            ICEGRES_WAREHOUSE=WAREHOUSE,
            ICEGRES_S3_ENDPOINT=S3_ENDPOINT,
            ICEGRES_S3_ACCESS_KEY=os.environ.get("ICEGRES_S3_ACCESS_KEY", "rustfsadmin"),
            ICEGRES_S3_SECRET_KEY=os.environ.get("ICEGRES_S3_SECRET_KEY", "rustfssecret"),
            ICEGRES_S3_REGION=os.environ.get("ICEGRES_S3_REGION", "us-east-1"),
        )
        child_env.update(env or {})
        with log_path.open("w") as log:
            child = subprocess.Popen(
                [str(BIN), "flight-serve" if flight else "serve", "--host", "127.0.0.1",
                 "--port", str(port), *flags],
                env=child_env, stdout=log, stderr=subprocess.STDOUT,
            )
            try:
                deadline = time.monotonic() + 30
                while time.monotonic() < deadline:
                    if child.poll() is not None:
                        raise RuntimeError(log_path.read_text())
                    try:
                        with socket.create_connection(("127.0.0.1", port), timeout=.2):
                            break
                    except OSError:
                        time.sleep(.1)
                else:
                    raise RuntimeError(f"Server startup timed out: {log_path.read_text()}")
                yield port
            finally:
                child.terminate()
                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait(timeout=5)

    @contextlib.contextmanager
    def connection(self, port, user="postgres", password=None):
        connection = psycopg2.connect(
            host="127.0.0.1", port=port, user=user, password=password,
            dbname="icegres", connect_timeout=5,
        )
        connection.autocommit = True
        try:
            yield connection
        finally:
            connection.close()

    def test_authorization_checks_wrappers_and_quoted_tables(self):
        self.table("allowed")
        self.table("secret")
        self.table("secret.with.dots")
        self.table("allowed@secret")
        self.table("allowed$secret")
        auth = self.directory / "auth"
        grants = self.directory / "grants"
        auth.write_text("reader:test-password\n")
        grants.write_text(
            f"grant reader read {self.namespace}.allowed\n"
            f"grant reader write {self.namespace}.leak\n"
        )
        with self.server("--auth-file", str(auth), "--authz-file", str(grants)) as port:
            with self.connection(port, "reader", "test-password") as connection:
                with connection.cursor() as cursor:
                    cursor.execute(f"SELECT id FROM {self.namespace}.allowed")
                    self.assertEqual(cursor.fetchone()[0], 1)
                    for sql in (
                        f"SELECT * FROM {self.namespace}.secret",
                        f"EXPLAIN ANALYZE SELECT * FROM {self.namespace}.secret",
                        f"CREATE TABLE {self.namespace}.leak AS SELECT * FROM {self.namespace}.secret",
                        f'SELECT * FROM {self.namespace}."secret.with.dots"',
                        f'SELECT * FROM {self.namespace}."allowed@secret"',
                        f'SELECT * FROM {self.namespace}."allowed$secret"',
                    ):
                        with self.subTest(sql=sql), self.assertRaises(psycopg2.Error) as raised:
                            cursor.execute(sql)
                        self.assertEqual(raised.exception.pgcode, "42501")

    def test_pgwire_read_only_rejects_writes(self):
        table = self.table("readonly")
        name = f"{self.namespace}.readonly"
        with self.server("--read-only") as port:
            with self.connection(port) as connection, connection.cursor() as cursor:
                cursor.execute(f"SELECT count(*) FROM {name}")
                self.assertEqual(cursor.fetchone()[0], 1)
                for sql in (
                    f"INSERT INTO {name} VALUES (2, 'forbidden')",
                    f"UPDATE {name} SET id=2",
                    f"DELETE FROM {name}",
                    f"EXPLAIN ANALYZE INSERT INTO {name} VALUES (2, 'forbidden')",
                    f"CREATE TABLE {self.namespace}.readonly_copy AS SELECT * FROM {name}",
                    f"DROP TABLE {name}",
                ):
                    with self.subTest(sql=sql), self.assertRaises(psycopg2.Error) as raised:
                        cursor.execute(sql)
                    self.assertIn(raised.exception.pgcode, ("25006", "42501"))
        self.assertEqual(table.refresh().scan().to_arrow().column("id").to_pylist(), [1])

    def test_readded_field_cannot_inherit_dropped_values(self):
        table = self.table("evolved")
        table.update_schema().delete_column("v").commit()
        table.update_schema().add_column("v", StringType()).commit()
        before = table.refresh().current_snapshot().snapshot_id
        with self.server() as port:
            with self.connection(port) as connection, connection.cursor() as cursor:
                with self.assertRaises(psycopg2.Error) as raised:
                    cursor.execute(f"UPDATE {self.namespace}.evolved SET id=id+10 WHERE id=1")
                self.assertIn("refusing unsafe", str(raised.exception))
        table.refresh()
        self.assertEqual(table.current_snapshot().snapshot_id, before)
        self.assertEqual(table.scan().to_arrow().column("v").to_pylist(), [None])

    def test_transaction_rejects_schema_change_without_snapshot_change(self):
        table = self.table("pinned")
        with self.server() as port:
            with self.connection(port) as connection, connection.cursor() as cursor:
                cursor.execute("BEGIN")
                cursor.execute(f"UPDATE {self.namespace}.pinned SET id=id+10 WHERE id=1")
                old_snapshot = table.refresh().current_snapshot().snapshot_id
                table.update_schema().add_column("extra", StringType()).commit()
                self.assertEqual(table.refresh().current_snapshot().snapshot_id, old_snapshot)
                with self.assertRaises(psycopg2.Error) as raised:
                    cursor.execute("COMMIT")
                self.assertEqual(raised.exception.pgcode, "40001")
        self.assertEqual(table.refresh().scan().to_arrow().column("id").to_pylist(), [1])

    def test_lost_commit_response_never_reports_rollback(self):
        table = self.table("uncertain")
        with LostResponseProxy() as proxy, self.server(catalog_uri=proxy.uri) as port:
            with self.connection(port) as connection, connection.cursor() as cursor:
                cursor.execute("BEGIN")
                cursor.execute(f"UPDATE {self.namespace}.uncertain SET id=id+1 WHERE id=1")
                proxy.lose_next.set()
                try:
                    cursor.execute("COMMIT")
                except psycopg2.Error as error:
                    self.assertEqual(error.pgcode, "40003")
                    self.assertNotIn("no changes were applied", str(error))
                self.assertEqual(proxy.lost, 1)
        self.assertEqual(table.refresh().scan().to_arrow().column("id").to_pylist(), [2])

    def test_flight_enforces_primary_key(self):
        import adbc_driver_flightsql.dbapi as flight

        table = self.table("flight_pk", pk=True)
        with self.server("--enforce-pk", flight=True) as port:
            with flight.connect(f"grpc://127.0.0.1:{port}", autocommit=True) as connection:
                with connection.cursor() as cursor:
                    for sql in (
                        f"INSERT INTO {self.namespace}.flight_pk VALUES (1, 'duplicate')",
                        f"EXPLAIN ANALYZE INSERT INTO {self.namespace}.flight_pk VALUES (1, 'duplicate')",
                        f"WITH incoming AS (SELECT 1 AS id, 'duplicate' AS v) INSERT INTO {self.namespace}.flight_pk SELECT * FROM incoming",
                    ):
                        with self.subTest(sql=sql), self.assertRaises(Exception):
                            cursor.execute(sql)
                            cursor.fetchall()
                    cursor.execute(f"INSERT INTO {self.namespace}.flight_pk VALUES (2, 'new')")
                    cursor.fetchall()
        self.assertEqual(sorted(table.refresh().scan().to_arrow().column("id").to_pylist()), [1, 2])

    def test_unknown_volatile_flush_is_quarantined_until_reconciled(self):
        table = self.table("volatile_unknown")
        with LostResponseProxy() as proxy, self.server(
            "--write-buffer-ms", "600000", catalog_uri=proxy.uri,
        ) as port:
            with self.connection(port) as connection, connection.cursor() as cursor:
                cursor.execute(f"INSERT INTO {self.namespace}.volatile_unknown VALUES (2, 'acknowledged')")
                proxy.block_after_loss = True
                proxy.lose_next.set()
                try:
                    for _ in range(2):
                        with self.assertRaises(psycopg2.Error):
                            cursor.execute("BEGIN")
                    self.assertEqual(proxy.lost, 1)
                    self.assertEqual(proxy.commits, 1, "an unknown flush must not be posted again")
                finally:
                    proxy.block_metadata.clear()
                cursor.execute("BEGIN")
                cursor.execute("COMMIT")
                self.assertEqual(proxy.commits, 1)
        self.assertEqual(sorted(table.refresh().scan().to_arrow().column("id").to_pylist()), [1, 2])

    def test_pgwire_enforces_primary_key_for_wrapped_writes(self):
        table = self.table("pgwire_pk", pk=True)
        with self.server("--enforce-pk") as port:
            with self.connection(port) as connection, connection.cursor() as cursor:
                for sql in (
                    f"INSERT INTO {self.namespace}.pgwire_pk VALUES (1, 'duplicate')",
                    f"EXPLAIN ANALYZE INSERT INTO {self.namespace}.pgwire_pk VALUES (1, 'duplicate')",
                    f"WITH incoming AS (SELECT 1 AS id, 'duplicate' AS v) INSERT INTO {self.namespace}.pgwire_pk SELECT * FROM incoming",
                ):
                    with self.subTest(sql=sql), self.assertRaises(psycopg2.Error):
                        cursor.execute(sql)
                cursor.execute(f"INSERT INTO {self.namespace}.pgwire_pk VALUES (2, 'new')")
        self.assertEqual(sorted(table.refresh().scan().to_arrow().column("id").to_pylist()), [1, 2])

    def test_oversized_buffer_write_is_not_acknowledged(self):
        table = self.table("bounded")
        with self.server(
            "--write-buffer-ms", "600000", "--tail-dir", str(self.directory / "bounded-tail"),
            env={"ICEGRES_WRITE_BUFFER_MAX_BYTES": "4096"},
        ) as port:
            with self.connection(port) as connection, connection.cursor() as cursor:
                with self.assertRaises(psycopg2.Error):
                    cursor.execute(
                        f"INSERT INTO {self.namespace}.bounded VALUES (2, %s)", ("x" * 16384,)
                    )
                cursor.execute(f"SELECT count(*) FROM {self.namespace}.bounded")
                self.assertEqual(cursor.fetchone()[0], 1)
        self.assertEqual(table.refresh().scan().to_arrow().column("id").to_pylist(), [1])


if __name__ == "__main__":
    unittest.main(verbosity=2)
