#!/usr/bin/env python3
"""Measure concurrent transactions and scans against an owned local fixture.

Requires psycopg2, pyarrow and pyiceberg. The catalog and object store must
already be running. The runner creates its own tables and two server processes.
"""

import argparse
from concurrent.futures import ThreadPoolExecutor
import contextlib
import json
import math
import os
import platform
import hashlib
from pathlib import Path
import socket
import sys
import subprocess
import tempfile
import threading
import time
import uuid

import psycopg2
from requests.adapters import HTTPAdapter
import pyarrow as pa
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.io.pyarrow import PyArrowFileIO
from pyiceberg.schema import Schema
from pyiceberg.types import LongType, NestedField

from run_status import Progress, supervise, write_json


class DeadlineAdapter(HTTPAdapter):
    def send(self, request, **kwargs):
        if kwargs.get("timeout") is None:
            kwargs["timeout"] = (5, 10)
        return super().send(request, **kwargs)


class FixtureCatalog(RestCatalog):
    def _create_session(self):
        session = super()._create_session()
        session.mount("http://", DeadlineAdapter())
        session.mount("https://", DeadlineAdapter())
        return session

    def _load_file_io(self, properties=None, location=None):
        # Use explicit test credentials, independent of catalog remote signing.
        return PyArrowFileIO({k: v for k, v in self.properties.items()
                              if k.startswith("s3.") and k != "s3.signer"})


def summarize(values):
    values = sorted(values)
    def percentile(p):
        return round(values[max(0, math.ceil(len(values) * p) - 1)], 4) if values else None
    return {"n": len(values), "p50": percentile(.5), "p95": percentile(.95),
            "p99": percentile(.99), "max": max(values, default=None)}


def port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


@contextlib.contextmanager
def connection(number):
    conn = psycopg2.connect(host="127.0.0.1", port=number, user="postgres",
                            dbname="icegres", connect_timeout=5)
    conn.autocommit = True
    try:
        yield conn
    finally:
        conn.close()


@contextlib.contextmanager
def server(args, namespace, directory, *, read_only=False, progress=None):
    number = port()
    env = {k: v for k, v in os.environ.items() if not k.startswith("ICEGRES_")}
    env.update(ICEGRES_CATALOG_URI=args.catalog_uri, ICEGRES_WAREHOUSE=args.warehouse,
               ICEGRES_S3_ENDPOINT=args.s3_endpoint,
               ICEGRES_S3_ACCESS_KEY=args.s3_access_key,
               ICEGRES_S3_SECRET_KEY=args.s3_secret_key,
               ICEGRES_MEMORY_LIMIT_MB=str(args.memory_mb))
    command = [str(args.binary.resolve()), "serve", "--host", "127.0.0.1", "--port", str(number)]
    if read_only:
        command.append("--read-only")
    with (directory / f"server-{number}.log").open("w") as log:
        child = subprocess.Popen(command, env=env, stdout=log, stderr=subprocess.STDOUT)
        try:
            if progress is not None:
                progress.phase(f"server readiness on port {number}")
            deadline = time.monotonic() + 60
            while time.monotonic() < deadline:
                if child.poll() is not None:
                    raise RuntimeError(f"server exited; see {directory}")
                try:
                    with connection(number) as conn, conn.cursor() as cursor:
                        cursor.execute(f"SELECT count(*) FROM {namespace}.accounts")
                        if cursor.fetchone()[0] == args.rows:
                            break
                except psycopg2.Error:
                    time.sleep(.1)
            else:
                raise TimeoutError("fixture server did not become ready")
            yield number, child
        finally:
            child.terminate()
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=5)


def run(args):
    namespace = args.namespace
    progress = Progress(args.output, namespace, args.logs,
                        ("transaction_ms", "analytics_ms", "point_read_ms", "freshness_ms"))
    progress.phase("catalog setup")
    catalog = FixtureCatalog("mixed", uri=args.catalog_uri, warehouse=args.warehouse,
        **{"s3.endpoint": args.s3_endpoint, "s3.region": "us-east-1",
           "s3.access-key-id": args.s3_access_key, "s3.secret-access-key": args.s3_secret_key,
           "s3.connect-timeout": "5", "s3.request-timeout": "10"})
    catalog.create_namespace(namespace)
    result = {"schema_version": 1, "complete": False, "correctness": False, "errors": 0,
              "workload": {"rows": args.rows, "files": args.files, "transactions": args.samples,
                           "memory_mb": args.memory_mb, "readers": 2,
                           "durability": "synchronous", "freshness": "independent pgwire replica",
                           "fixture_version": 2},
              "environment": {"platform": platform.platform(), "cpus": os.cpu_count(),
                              "catalog_uri": args.catalog_uri, "warehouse": args.warehouse, "s3_endpoint": args.s3_endpoint,
                              "resource_scope": "writer and replica RSS; excludes storage services"},
              "binary_sha256": hashlib.file_digest(args.binary.open("rb"), "sha256").hexdigest(),
              "binary": str(args.binary.resolve()), "label": args.label, "namespace": namespace,
              "logs": str(args.logs), "progress_path": str(progress.path), "metrics": {}}
    samples = progress.samples
    errors = []
    stop = threading.Event()
    barrier = threading.Barrier(3, timeout=60)
    peak_rss = [0]
    try:
        schema = Schema(*[NestedField(i + 1, name, LongType(), required=False)
                          for i, name in enumerate(("id", "balance", "bucket", "revision"))])
        table = catalog.create_table((namespace, "accounts"), schema=schema,
                                     properties={"format-version": "2"})
        for start in range(0, args.rows, args.rows // args.files):
            progress.phase(f"loading fixture rows from {start}")
            ids = list(range(start, min(args.rows, start + args.rows // args.files)))
            table.append(pa.table({"id": pa.array(ids, type=pa.int64()),
                                   "balance": pa.array([100] * len(ids), type=pa.int64()),
                                   "bucket": pa.array([i % 16 for i in ids], type=pa.int64()),
                                   "revision": pa.array([0] * len(ids), type=pa.int64())}))
        args.logs.mkdir(parents=True, exist_ok=True)
        with server(args, namespace, args.logs, progress=progress) as writer, server(args, namespace, args.logs, read_only=True, progress=progress) as reader:
            progress.phase("concurrent workload")
            table_sql = f"{namespace}.accounts"
            started = time.monotonic()
            deadline = started + args.timeout

            def check_time():
                if time.monotonic() > deadline:
                    raise TimeoutError("mixed workload exceeded run deadline")

            def execute(worker, cursor, sql):
                progress.operation(worker, "query", sql=sql,
                                   backend_pid=cursor.connection.get_backend_pid(),
                                   port=cursor.connection.get_dsn_parameters().get("port"))
                cursor.execute(sql)
                progress.operation(worker, "consume result", sql=sql)

            def transactions():
                try:
                    progress.operation("transaction", "connect")
                    with connection(writer[0]) as conn, connection(reader[0]) as visible:
                        with conn.cursor() as cursor, visible.cursor() as probe:
                            progress.operation("transaction", "barrier")
                            barrier.wait()
                            for iteration in range(1, args.samples + 1):
                                check_time()
                                if stop.is_set():
                                    raise RuntimeError("reader failed during workload")
                                row = (iteration * 997) % args.rows
                                begin = time.monotonic()
                                execute("transaction", cursor, "BEGIN")
                                execute("transaction", cursor, f"SELECT balance FROM {table_sql} WHERE id={row}")
                                if len(cursor.fetchall()) != 1:
                                    raise AssertionError("transaction point read lost a row")
                                execute("transaction", cursor, f"UPDATE {table_sql} SET balance=balance+1, revision={iteration} WHERE id={row}")
                                execute("transaction", cursor, "COMMIT")
                                acknowledged = time.monotonic()
                                progress.sample("transaction_ms", (acknowledged - begin) * 1000)
                                while True:
                                    check_time()
                                    execute("transaction", probe, f"SELECT revision FROM {table_sql} WHERE id={row}")
                                    if probe.fetchone()[0] == iteration:
                                        break
                                    time.sleep(.005)
                                progress.sample("freshness_ms", (time.monotonic() - acknowledged) * 1000)
                            progress.operation("transaction", "close connections")
                except Exception as error:
                    errors.append(f"transaction: {error}")
                    barrier.abort()
                finally:
                    progress.operation("transaction", "finished")
                    stop.set()

            def reads(kind):
                try:
                    progress.operation(kind, "connect")
                    with connection(reader[0]) as conn, conn.cursor() as cursor:
                        progress.operation(kind, "barrier")
                        barrier.wait()
                        while not stop.is_set() or len(samples[kind]) < args.samples:
                            check_time()
                            begin = time.monotonic()
                            if kind == "analytics_ms":
                                execute(kind, cursor, f"SELECT bucket, count(*), sum(balance) FROM {table_sql} GROUP BY bucket")
                                rows = cursor.fetchall()
                                if sum(row[1] for row in rows) != args.rows or sum(row[2] for row in rows) < args.rows * 100:
                                    raise AssertionError("analytical result lost rows or balances")
                            else:
                                row = (len(samples[kind]) * 991) % args.rows
                                execute(kind, cursor, f"SELECT balance FROM {table_sql} WHERE id={row}")
                                rows = cursor.fetchall()
                                if len(rows) != 1 or rows[0][0] < 100:
                                    raise AssertionError("point read returned invalid data")
                            progress.sample(kind, (time.monotonic() - begin) * 1000)
                        progress.operation(kind, "close connection")
                except Exception as error:
                    errors.append(f"{kind}: {error}")
                    stop.set()
                    barrier.abort()
                finally:
                    progress.operation(kind, "finished")

            checkpoint_at = time.monotonic()
            with ThreadPoolExecutor(max_workers=3) as pool:
                futures = [pool.submit(transactions), pool.submit(reads, "analytics_ms"),
                           pool.submit(reads, "point_read_ms")]
                while not all(future.done() for future in futures):
                    if time.monotonic() >= checkpoint_at:
                        progress.checkpoint()
                        checkpoint_at = time.monotonic() + .5
                    try:
                        rss = subprocess.check_output(["ps", "-o", "rss=", "-p", f"{writer[1].pid},{reader[1].pid}"], text=True)
                        peak_rss[0] = max(peak_rss[0], sum(int(v) for v in rss.split()) / 1024)
                    except (ValueError, subprocess.CalledProcessError) as error:
                        errors.append(f"RSS sampling: {error}")
                        stop.set()
                    if time.monotonic() > deadline:
                        # Terminate only our children so a stalled driver cannot
                        # keep the executor shutdown waiting without a deadline.
                        for _, process in (writer, reader):
                            process.kill()
                        raise TimeoutError("mixed workload deadline exceeded")
                    time.sleep(.05)
                for future in futures:
                    future.result()
            elapsed = time.monotonic() - started
            progress.phase("independent final verification")
            final = table.refresh().scan().to_arrow()
            expected = {i: [100, 0] for i in range(args.rows)}
            for iteration in range(1, args.samples + 1):
                row = (iteration * 997) % args.rows
                expected[row][0] += 1
                expected[row][1] = iteration
            actual = final.to_pylist()
            result["correctness"] = (len(actual) == args.rows
                and len({row["id"] for row in actual}) == args.rows
                and all(row["id"] in expected
                        and [row["balance"], row["revision"]] == expected[row["id"]]
                        and row["bucket"] == row["id"] % 16 for row in actual))
            result["complete"] = len(samples["transaction_ms"]) == args.samples and all(len(v) >= args.samples for v in samples.values())
            result["metrics"] = {name: summarize(values) for name, values in samples.items()}
            result["metrics"].update(
                operations_per_second={"value": sum(len(v) for k, v in samples.items() if k != "freshness_ms") / elapsed},
                transactions_per_second={"value": len(samples["transaction_ms"]) / elapsed},
                rss_peak_mb={"value": peak_rss[0]},
            )
            result["duration_seconds"] = elapsed
    except Exception as error:
        errors.append(str(error))
    finally:
        result["errors"] = len(errors)
        result["error_details"] = errors
        result["samples_ms"] = samples
        args.output.parent.mkdir(parents=True, exist_ok=True)
        write_json(args.output, result)
        progress.phase("catalog cleanup")
        for ident in catalog.list_tables(namespace):
            catalog.drop_table(ident)
        catalog.drop_namespace(namespace)
        progress.phase("complete")
    print(json.dumps({key: result[key] for key in ("complete", "correctness", "errors")}, indent=2))
    return int(not result["complete"] or not result["correctness"] or result["errors"] != 0)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--namespace", help=argparse.SUPPRESS)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--logs", type=Path)
    parser.add_argument("--catalog-uri", default="http://127.0.0.1:8181/catalog")
    parser.add_argument("--warehouse", default="lakehouse")
    parser.add_argument("--s3-endpoint", default="http://127.0.0.1:9000")
    parser.add_argument("--s3-access-key", default=os.environ.get("ICEGRES_S3_ACCESS_KEY", "rustfsadmin"))
    parser.add_argument("--s3-secret-key", default=os.environ.get("ICEGRES_S3_SECRET_KEY", "rustfssecret"))
    parser.add_argument("--rows", type=int, default=20000)
    parser.add_argument("--files", type=int, default=10)
    parser.add_argument("--samples", type=int, default=100)
    parser.add_argument("--memory-mb", type=int, default=1024)
    parser.add_argument("--timeout", type=float, default=300)
    args = parser.parse_args()
    if args.logs is None:
        args.logs = Path(tempfile.mkdtemp(prefix="icegres-mixed-"))
    if args.samples < 100 or args.rows < args.samples or args.files < 1 or args.rows % args.files or args.memory_mb < 1 or args.timeout <= 0:
        parser.error("require >=100 samples, rows >= samples divisible by files, and positive memory/timeout")
    if args.worker:
        return run(args)
    if args.namespace is not None:
        parser.error("namespace is assigned by the runner")
    namespace = "mixed_" + uuid.uuid4().hex[:12]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    incomplete = {"schema_version": 1, "complete": False, "correctness": False,
                  "errors": 1, "label": args.label, "namespace": namespace, "logs": str(args.logs),
                  "error_details": ["run did not complete"], "metrics": {}}
    return supervise(
        [sys.executable, str(Path(__file__).resolve()), *sys.argv[1:],
         "--worker", "--namespace", namespace, "--logs", str(args.logs)],
        args.timeout, args.output, incomplete,
    )


if __name__ == "__main__":
    raise SystemExit(main())
