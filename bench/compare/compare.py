#!/usr/bin/env python3
"""Multi-engine lakehouse comparison harness.

One harness, four connectors, identical query set, identical timing method:
every sample is time.perf_counter() around cursor.execute() PLUS a full
cursor.fetchall() (results fully materialized client-side).

Engines / connectors:
  icegres   psycopg2                 -> 127.0.0.1:5439  (tables: demo.*)
  trino     trino.dbapi              -> 127.0.0.1:8082  (tables: iceberg.demo.*)
  spark     pyhive.hive (thrift)     -> 127.0.0.1:10000 (tables: lake.demo.*)
  flightsql adbc_driver_flightsql    -> 127.0.0.1:50051 (tables: demo.*)

Query set (identical SQL modulo the catalog prefix of the table names):
  q1_point_lookup_ms   SELECT * row by trip_id = const           (demo.trips)
  q2_filtered_scan_ms  rows WHERE city+distance filter           (demo.trips)
  q3_aggregate_ms      GROUP BY city w/ count+avg, ordered       (demo.trips)
  q4_join_ms           trips JOIN cities, GROUP BY country       (demo.trips)
  q5_big_scan_agg_ms   GROUP BY city over ~5M rows               (demo.trips_big)
  q6_big_filter_count_ms COUNT(*) WHERE city+distance over 5M    (demo.trips_big)
  q7_big_selective_ms  ~100 rows by trip_id BETWEEN over 5M      (demo.trips_big)

Per query: WARMUP discarded runs, then ITERS timed runs -> p50/p95/min/max.
Also per engine:
  connect_ms   fresh connection + 'SELECT 1' round trip (some drivers
               connect lazily, so a bare connect() would measure nothing),
               CONNECT_RUNS samples -> p50/p95.
  qps_8way     8 threads, each with its own connection, cycling the mixed
               read set [q1,q2,q3,q4,q6,q7] for QPS_SECONDS wall seconds;
               qps = total completed queries / elapsed.

Usage:
  compare.py run --engine icegres [--out DIR] [--iters N] [--pid PID]
                 [--startup-ms MS] [--skip-concurrency]
  compare.py merge --out-dir DIR ...   # combine per-engine jsons ->
                                       # bench/results/compare-<ts>.json + .md
"""

import argparse
import concurrent.futures
import datetime
import json
import os
import platform
import statistics
import subprocess
import sys
import threading
import time

WARMUP = 3
ITERS = 15
CONNECT_RUNS = 10
QPS_SECONDS = 10
QPS_THREADS = 8

RESULTS_DIR = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "results"
)

# ---------------------------------------------------------------- queries

def queries(prefix: str) -> dict:
    """prefix is the catalog-qualified namespace, e.g. 'demo' or
    'iceberg.demo' or 'lake.demo'."""
    t = f"{prefix}.trips"
    c = f"{prefix}.cities"
    b = f"{prefix}.trips_big"
    return {
        "q1_point_lookup_ms": (
            f"SELECT trip_id, city, distance_km, fare, ts FROM {t} "
            f"WHERE trip_id = 137"
        ),
        "q2_filtered_scan_ms": (
            f"SELECT trip_id, city, distance_km, fare FROM {t} "
            f"WHERE city = 'Paris' AND distance_km > 20"
        ),
        "q3_aggregate_ms": (
            f"SELECT city, count(*) AS n, avg(fare) AS avg_fare FROM {t} "
            f"GROUP BY city ORDER BY n DESC, city ASC"
        ),
        "q4_join_ms": (
            f"SELECT c.country, count(*) AS n FROM {t} tr "
            f"JOIN {c} c ON tr.city = c.city "
            f"GROUP BY c.country ORDER BY n DESC, c.country ASC"
        ),
        "q5_big_scan_agg_ms": (
            f"SELECT city, count(*) AS n, avg(distance_km) AS avg_km, "
            f"sum(fare) AS total_fare FROM {b} "
            f"GROUP BY city ORDER BY n DESC, city ASC"
        ),
        "q6_big_filter_count_ms": (
            f"SELECT count(*) AS n FROM {b} "
            f"WHERE city = 'Berlin' AND distance_km > 30"
        ),
        "q7_big_selective_ms": (
            f"SELECT trip_id, city, fare FROM {b} "
            f"WHERE trip_id BETWEEN 2500000 AND 2500100"
        ),
    }


MIXED_KEYS = [
    "q1_point_lookup_ms",
    "q2_filtered_scan_ms",
    "q3_aggregate_ms",
    "q4_join_ms",
    "q6_big_filter_count_ms",
    "q7_big_selective_ms",
]

# --------------------------------------------------------------- engines

class Engine:
    name = ""
    prefix = "demo"

    def connect(self):
        raise NotImplementedError

    def version(self, conn) -> str:
        return ""

    def run(self, conn, sql):
        """execute + FULL fetch; returns rows. Identical across engines."""
        cur = conn.cursor()
        try:
            cur.execute(sql)
            return cur.fetchall()
        finally:
            cur.close()


class Icegres(Engine):
    name = "icegres"
    prefix = "demo"

    def __init__(self):
        self.host = os.environ.get("ICEGRES_HOST", "127.0.0.1")
        self.port = int(os.environ.get("ICEGRES_PORT", "5439"))

    def connect(self):
        import psycopg2

        conn = psycopg2.connect(
            host=self.host, port=self.port, dbname="icegres", user="icegres"
        )
        conn.autocommit = True
        return conn

    def version(self, conn):
        return self.run(conn, "SELECT version()")[0][0]


class Trino(Engine):
    name = "trino"
    prefix = "iceberg.demo"

    def __init__(self):
        self.host = os.environ.get("TRINO_HOST", "127.0.0.1")
        self.port = int(os.environ.get("TRINO_PORT", "8082"))

    def connect(self):
        import trino as trino_mod

        return trino_mod.dbapi.connect(
            host=self.host, port=self.port, user="bench", catalog="iceberg"
        )

    def version(self, conn):
        return "trino " + str(self.run(conn, "SELECT version()")[0][0])


class Spark(Engine):
    name = "spark"
    prefix = "lake.demo"

    def __init__(self):
        self.host = os.environ.get("SPARK_HOST", "127.0.0.1")
        self.port = int(os.environ.get("SPARK_THRIFT_PORT", "10000"))

    def connect(self):
        from pyhive import hive

        return hive.connect(host=self.host, port=self.port, username="bench")

    def version(self, conn):
        return "spark " + str(self.run(conn, "SELECT version()")[0][0])


class FlightSQL(Engine):
    name = "flightsql"
    prefix = "demo"

    def __init__(self):
        self.host = os.environ.get("FLIGHTSQL_HOST", "127.0.0.1")
        self.port = int(os.environ.get("FLIGHTSQL_PORT", "50051"))

    def connect(self):
        import adbc_driver_flightsql.dbapi as flightsql

        return flightsql.connect(f"grpc://{self.host}:{self.port}")

    def version(self, conn):
        import adbc_driver_flightsql

        return f"adbc-flightsql {adbc_driver_flightsql.__version__}"


ENGINES = {e.name: e for e in (Icegres(), Trino(), Spark(), FlightSQL())}

# --------------------------------------------------------------- metrics

def pcts(samples):
    s = sorted(samples)
    n = len(s)
    return {
        "p50": round(statistics.median(s), 2),
        "p95": round(s[min(n - 1, max(0, int(round(0.95 * n)) - 1))], 2),
        "min": round(s[0], 2),
        "max": round(s[-1], 2),
        "n": n,
    }


def time_query(engine, conn, sql, warmup=WARMUP, iters=ITERS):
    rows = None
    for _ in range(warmup):
        rows = engine.run(conn, sql)
    samples = []
    for _ in range(iters):
        t0 = time.perf_counter()
        rows = engine.run(conn, sql)
        samples.append((time.perf_counter() - t0) * 1000.0)
    return pcts(samples), rows


def measure_connect(engine, runs=CONNECT_RUNS):
    samples = []
    for _ in range(runs):
        t0 = time.perf_counter()
        conn = engine.connect()
        engine.run(conn, "SELECT 1")
        samples.append((time.perf_counter() - t0) * 1000.0)
        conn.close()
    return pcts(samples)


def measure_qps(engine, qmap, seconds=QPS_SECONDS, threads=QPS_THREADS):
    stop = threading.Event()
    counts = [0] * threads
    errors = []

    def worker(i):
        try:
            conn = engine.connect()
        except Exception as exc:  # noqa: BLE001
            errors.append(f"connect: {exc}")
            return
        try:
            j = i  # stagger starting points across threads
            while not stop.is_set():
                sql = qmap[MIXED_KEYS[j % len(MIXED_KEYS)]]
                engine.run(conn, sql)
                counts[i] += 1
                j += 1
        except Exception as exc:  # noqa: BLE001
            errors.append(f"worker{i}: {exc}")
        finally:
            conn.close()

    with concurrent.futures.ThreadPoolExecutor(max_workers=threads) as ex:
        futs = [ex.submit(worker, i) for i in range(threads)]
        t0 = time.perf_counter()
        time.sleep(seconds)
        stop.set()
        concurrent.futures.wait(futs)
        elapsed = time.perf_counter() - t0
    total = sum(counts)
    return {
        "qps": round(total / elapsed, 1),
        "total_queries": total,
        "elapsed_s": round(elapsed, 2),
        "threads": threads,
        "errors": errors,
    }


def rss_mb(pid):
    """Current VmHWM (peak RSS) of pid in MB, or None."""
    try:
        with open(f"/proc/{pid}/status") as f:
            for line in f:
                if line.startswith("VmHWM"):
                    return round(int(line.split()[1]) / 1024.0, 1)
    except OSError:
        return None
    return None

# ------------------------------------------------------------------ run

def run_engine(args):
    engine = ENGINES[args.engine]
    global ITERS
    if args.iters:
        ITERS = args.iters
    qmap = queries(engine.prefix)

    result = {"metrics": {}, "meta": {}}

    result["metrics"]["connect_ms"] = measure_connect(engine)

    conn = engine.connect()
    try:
        result["meta"]["engine_version"] = engine.version(conn)
    except Exception as exc:  # noqa: BLE001
        result["meta"]["engine_version"] = f"unknown ({exc})"

    checks = {}
    for key in sorted(qmap):
        stats, rows = time_query(engine, conn, qmap[key])
        result["metrics"][key] = stats
        checks[key] = len(rows)
        print(f"{engine.name} {key}: p50={stats['p50']}ms "
              f"p95={stats['p95']}ms rows={len(rows)}", flush=True)
    conn.close()
    result["meta"]["result_row_counts"] = checks

    if not args.skip_concurrency:
        result["metrics"]["qps_8way"] = measure_qps(engine, qmap)
        print(f"{engine.name} qps_8way: {result['metrics']['qps_8way']['qps']}",
              flush=True)

    if args.pid:
        peak = rss_mb(args.pid)
        if peak is not None:
            result["metrics"]["rss_peak_mb"] = {"value": peak}
    if args.startup_ms is not None:
        result["metrics"]["startup_ms"] = {"value": args.startup_ms}
    if args.rss_idle_mb is not None:
        result["metrics"]["rss_idle_mb"] = {"value": args.rss_idle_mb}
    if args.footprint:
        result["meta"]["footprint"] = args.footprint

    result["meta"].update(
        {
            "engine": engine.name,
            "table_prefix": engine.prefix,
            "warmup": WARMUP,
            "iterations": ITERS,
            "timestamp": datetime.datetime.now(datetime.timezone.utc)
            .isoformat(timespec="seconds"),
        }
    )

    out_dir = args.out or RESULTS_DIR
    os.makedirs(out_dir, exist_ok=True)
    path = os.path.join(out_dir, f"engine-{engine.name}.json")
    with open(path, "w") as f:
        json.dump(result, f, indent=2, sort_keys=True)
    print(f"wrote {path}")
    return 0

# ---------------------------------------------------------------- merge

TABLE_METRICS = [
    "connect_ms",
    "q1_point_lookup_ms",
    "q2_filtered_scan_ms",
    "q3_aggregate_ms",
    "q4_join_ms",
    "q5_big_scan_agg_ms",
    "q6_big_filter_count_ms",
    "q7_big_selective_ms",
]


def dataset_meta():
    """Row/file counts for demo.trips and demo.trips_big via pyiceberg."""
    try:
        from pyiceberg.catalog import load_catalog

        cat = load_catalog(
            "lake",
            **{
                "type": "rest",
                "uri": "http://127.0.0.1:8181/catalog",
                "warehouse": "lakehouse",
                "s3.endpoint": "http://127.0.0.1:9000",
                "s3.access-key-id": "rustfsadmin",
                "s3.secret-access-key": "rustfssecret",
                "s3.region": "us-east-1",
                "s3.path-style-access": "true",
            },
        )
        out = {}
        for name in ("trips", "cities", "trips_big"):
            try:
                files = list(cat.load_table(("demo", name)).scan().plan_files())
                out[f"demo.{name}"] = {
                    "rows": sum(f.file.record_count for f in files),
                    "files": len(files),
                    "bytes": sum(f.file.file_size_in_bytes for f in files),
                }
            except Exception as exc:  # noqa: BLE001
                out[f"demo.{name}"] = {"error": str(exc)}
        return out
    except Exception as exc:  # noqa: BLE001
        return {"error": str(exc)}


def merge(args):
    ts = datetime.datetime.now().strftime("%Y%m%d-%H%M%S")
    engines = {}
    for name in ENGINES:
        p = os.path.join(args.out_dir, f"engine-{name}.json")
        if os.path.exists(p):
            with open(p) as f:
                engines[name] = json.load(f)
    if not engines:
        print(f"no engine-*.json found in {args.out_dir}", file=sys.stderr)
        return 1

    combined = {
        "engines": engines,
        "meta": {
            "timestamp": datetime.datetime.now(datetime.timezone.utc)
            .isoformat(timespec="seconds"),
            "host": platform.node(),
            "python": platform.python_version(),
            "cpus": os.cpu_count(),
            "datasets": dataset_meta(),
            "timing": "perf_counter around execute + full fetchall; "
            f"{WARMUP} warmup discarded, p50/p95 over timed iterations",
            "connect_ms": "fresh connection + SELECT 1 round trip",
            "qps_8way": f"{QPS_THREADS} threads x {QPS_SECONDS}s mixed reads "
            f"({', '.join(MIXED_KEYS)})",
        },
    }

    os.makedirs(RESULTS_DIR, exist_ok=True)
    jpath = os.path.join(RESULTS_DIR, f"compare-{ts}.json")
    with open(jpath, "w") as f:
        json.dump(combined, f, indent=2, sort_keys=True)

    # ---- markdown table
    names = [n for n in ENGINES if n in engines]
    lines = [
        f"# Engine comparison — {ts}",
        "",
        "Datasets: "
        + ", ".join(
            f"`{k}` {v.get('rows', '?')} rows/{v.get('files', '?')} files"
            for k, v in combined["meta"]["datasets"].items()
            if isinstance(v, dict) and "rows" in v
        ),
        "",
        "All latencies in ms (p50 / p95), execute + full fetch, "
        f"{WARMUP} warmups discarded.",
        "",
        "| metric | " + " | ".join(names) + " |",
        "|---" * (len(names) + 1) + "|",
    ]
    for m in TABLE_METRICS:
        row = [m]
        for n in names:
            s = engines[n]["metrics"].get(m)
            row.append(f"{s['p50']} / {s['p95']}" if s else "—")
        lines.append("| " + " | ".join(row) + " |")
    for m, label in (
        ("qps_8way", "qps_8way (qps)"),
        ("startup_ms", "startup_ms"),
        ("rss_idle_mb", "rss_idle_mb"),
        ("rss_peak_mb", "rss_peak_mb"),
    ):
        row = [label]
        for n in names:
            s = engines[n]["metrics"].get(m)
            if not s:
                row.append("—")
            elif "qps" in s:
                row.append(str(s["qps"]))
            else:
                row.append(str(s.get("value", "—")))
        lines.append("| " + " | ".join(row) + " |")
    row = ["footprint (install/binary)"]
    for n in names:
        row.append(str(engines[n]["meta"].get("footprint", "—")))
    lines.append("| " + " | ".join(row) + " |")
    lines += [
        "",
        "Versions: "
        + "; ".join(
            f"{n}: {engines[n]['meta'].get('engine_version', '?')}"
            for n in names
        ),
        "",
    ]
    mpath = os.path.join(RESULTS_DIR, f"compare-{ts}.md")
    with open(mpath, "w") as f:
        f.write("\n".join(lines))
    print(f"wrote {jpath}")
    print(f"wrote {mpath}")
    print("\n".join(lines))
    return 0


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    sub = ap.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("run", help="benchmark one engine")
    r.add_argument("--engine", required=True, choices=sorted(ENGINES))
    r.add_argument("--out", help="dir for engine-<name>.json "
                   "(default bench/results)")
    r.add_argument("--iters", type=int, default=None)
    r.add_argument("--pid", type=int, default=None,
                   help="engine pid: record VmHWM as rss_peak_mb after run")
    r.add_argument("--startup-ms", type=int, default=None)
    r.add_argument("--rss-idle-mb", type=float, default=None,
                   help="idle RSS measured right after startup, before bench")
    r.add_argument("--footprint", default=None,
                   help="human-readable install/binary size, e.g. '443M'")
    r.add_argument("--skip-concurrency", action="store_true")
    r.set_defaults(func=run_engine)

    m = sub.add_parser("merge", help="merge per-engine jsons + render table")
    m.add_argument("--out-dir", required=True,
                   help="dir containing engine-<name>.json files")
    m.set_defaults(func=merge)

    args = ap.parse_args()
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
