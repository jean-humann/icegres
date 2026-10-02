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
import signal
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
def server(args, namespace, directory, *, read_only=False):
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
    catalog = FixtureCatalog("mixed", uri=args.catalog_uri, warehouse=args.warehouse,
        **{"s3.endpoint": args.s3_endpoint, "s3.region": "us-east-1",
           "s3.access-key-id": args.s3_access_key, "s3.secret-access-key": args.s3_secret_key,
           "s3.connect-timeout": "5", "s3.request-timeout": "10"})
    catalog.create_namespace(namespace)
    result = {"schema_version": 1, "complete": False, "correctness": False, "errors": 0,
              "workload": {"rows": args.rows, "files": args.files, "transactions": args.samples,
                           "memory_mb": args.memory_mb, "readers": 2,
                           "durability": "synchronous", "freshness": "independent pgwire replica",
                           "fixture_version": 1},
              "environment": {"platform": platform.platform(), "cpus": os.cpu_count(),
                              "catalog_uri": args.catalog_uri, "warehouse": args.warehouse, "s3_endpoint": args.s3_endpoint,
                              "resource_scope": "writer and replica RSS; excludes storage services"},
              "binary_sha256": hashlib.file_digest(args.binary.open("rb"), "sha256").hexdigest(),
              "binary": str(args.binary.resolve()), "label": args.label, "metrics": {}}
    samples = {name: [] for name in ("transaction_ms", "analytics_ms", "point_read_ms", "freshness_ms")}
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
            ids = list(range(start, min(args.rows, start + args.rows // args.files)))
            table.append(pa.table({"id": pa.array(ids, type=pa.int64()),
                                   "balance": pa.array([100] * len(ids), type=pa.int64()),
                                   "bucket": pa.array([i % 16 for i in ids], type=pa.int64()),
                                   "revision": pa.array([0] * len(ids), type=pa.int64())}))
        args.logs.mkdir(parents=True, exist_ok=True)
        with server(args, namespace, args.logs) as writer, server(args, namespace, args.logs, read_only=True) as reader:
            table_sql = f"{namespace}.accounts"
            started = time.monotonic()
            deadline = started + args.timeout

            def check_time():
                if time.monotonic() > deadline:
                    raise TimeoutError("mixed workload exceeded run deadline")

            def transactions():
                try:
                    with connection(writer[0]) as conn, connection(reader[0]) as visible:
                        with conn.cursor() as cursor, visible.cursor() as probe:
                            barrier.wait()
                            for iteration in range(1, args.samples + 1):
                                check_time()
                                if stop.is_set():
                                    raise RuntimeError("reader failed during workload")
                                row = (iteration * 997) % args.rows
                                begin = time.monotonic()
                                cursor.execute("BEGIN")
                                cursor.execute(f"SELECT balance FROM {table_sql} WHERE id={row}")
                                if len(cursor.fetchall()) != 1:
                                    raise AssertionError("transaction point read lost a row")
                                cursor.execute(f"UPDATE {table_sql} SET balance=balance+1, revision={iteration} WHERE id={row}")
                                cursor.execute("COMMIT")
                                acknowledged = time.monotonic()
                                samples["transaction_ms"].append((acknowledged - begin) * 1000)
                                while True:
                                    check_time()
                                    probe.execute(f"SELECT revision FROM {table_sql} WHERE id={row}")
                                    if probe.fetchone()[0] == iteration:
                                        break
                                    time.sleep(.005)
                                samples["freshness_ms"].append((time.monotonic() - acknowledged) * 1000)
                except Exception as error:
                    errors.append(f"transaction: {error}")
                    barrier.abort()
                finally:
                    stop.set()

            def reads(kind):
                try:
                    with connection(reader[0]) as conn, conn.cursor() as cursor:
                        barrier.wait()
                        while not stop.is_set() or len(samples[kind]) < args.samples:
                            check_time()
                            begin = time.monotonic()
                            if kind == "analytics_ms":
                                cursor.execute(f"SELECT bucket, count(*), sum(balance) FROM {table_sql} GROUP BY bucket")
                                rows = cursor.fetchall()
                                if sum(row[1] for row in rows) != args.rows or sum(row[2] for row in rows) < args.rows * 100:
                                    raise AssertionError("analytical result lost rows or balances")
                            else:
                                row = (len(samples[kind]) * 991) % args.rows
                                cursor.execute(f"SELECT balance FROM {table_sql} WHERE id={row}")
                                rows = cursor.fetchall()
                                if len(rows) != 1 or rows[0][0] < 100:
                                    raise AssertionError("point read returned invalid data")
                            samples[kind].append((time.monotonic() - begin) * 1000)
                except Exception as error:
                    errors.append(f"{kind}: {error}")
                    stop.set()
                    barrier.abort()

            with ThreadPoolExecutor(max_workers=3) as pool:
                futures = [pool.submit(transactions), pool.submit(reads, "analytics_ms"),
                           pool.submit(reads, "point_read_ms")]
                while not all(future.done() for future in futures):
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
        args.output.write_text(json.dumps(result, indent=2) + "\n")
        for ident in catalog.list_tables(namespace):
            catalog.drop_table(ident)
        catalog.drop_namespace(namespace)
    print(json.dumps({key: result[key] for key in ("complete", "correctness", "errors")}, indent=2))
    return int(not result["complete"] or not result["correctness"] or result["errors"] != 0)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--worker", action="store_true", help=argparse.SUPPRESS)
    parser.add_argument("--namespace", help=argparse.SUPPRESS)
    parser.add_argument("--binary", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--label", required=True)
    parser.add_argument("--logs", type=Path, default=Path(tempfile.mkdtemp(prefix="icegres-mixed-")))
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
    if args.samples < 100 or args.rows < args.samples or args.files < 1 or args.rows % args.files or args.memory_mb < 1 or args.timeout <= 0:
        parser.error("require >=100 samples, rows >= samples divisible by files, and positive memory/timeout")
    if args.worker:
        return run(args)
    if args.namespace is not None:
        parser.error("namespace is assigned by the runner")
    namespace = "mixed_" + uuid.uuid4().hex[:12]
    args.output.parent.mkdir(parents=True, exist_ok=True)
    incomplete = {"schema_version": 1, "complete": False, "correctness": False,
                  "errors": 1, "label": args.label, "namespace": namespace,
                  "error_details": ["run did not complete"], "metrics": {}}
    args.output.write_text(json.dumps(incomplete, indent=2) + "\n")
    # Own a process group so setup, final verification and cleanup also have
    # a wall-clock bound. Only this run's worker and child servers are killed.
    worker = subprocess.Popen([sys.executable, str(Path(__file__).resolve()),
                               *sys.argv[1:], "--worker", "--namespace", namespace],
                              start_new_session=True)
    try:
        code = worker.wait(timeout=args.timeout)
        if code != 0:
            # A worker can fail during cleanup after writing measurements.
            # Its exit status must invalidate those measurements too.
            try:
                failed = json.loads(args.output.read_text())
            except (OSError, ValueError):
                failed = incomplete
            failed["complete"] = False
            failed["errors"] = max(1, failed.get("errors", 0))
            failed.setdefault("error_details", []).append(f"worker exited with status {code}")
            args.output.write_text(json.dumps(failed, indent=2) + "\n")
        return code
    except KeyboardInterrupt:
        if worker.poll() is None:
            os.killpg(worker.pid, signal.SIGKILL)
        worker.wait(timeout=5)
        incomplete["error_details"] = ["run interrupted; owned processes killed; catalog cleanup may remain"]
        args.output.write_text(json.dumps(incomplete, indent=2) + "\n")
        return 130
    except subprocess.TimeoutExpired:
        os.killpg(worker.pid, signal.SIGKILL)
        worker.wait(timeout=5)
        incomplete["error_details"] = ["whole-run deadline exceeded; owned processes killed; "
                                        "catalog namespace may require cleanup"]
        args.output.write_text(json.dumps(incomplete, indent=2) + "\n")
        print(incomplete["error_details"][0], file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
