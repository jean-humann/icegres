#!/usr/bin/env python3
"""Memory-under-load driver for icegres.

One operation per invocation, so the orchestrator (bench/mem.sh) can attribute
the server process's peak RSS (VmHWM) to a single operation at a single volume.
Every subcommand prints exactly one JSON line to stdout: {"op","rows","seconds"}.

Subcommands
  create-table --table T          pgwire CREATE TABLE demo.T (5-col trips shape)
  drop-table   --table T          pgwire DROP TABLE demo.T
  ingest --rows N --table T        Flight ADBC bulk ingest (CommandStatementIngest),
                                   mode=append into demo.T — the write path under test
  read-flight  --table T          Flight DoGet full scan of demo.T, batches drained
  read-pg      --table T          pgwire COPY (SELECT * FROM demo.T) TO STDOUT, drained

The client builds/holds data in ITS OWN process; the orchestrator samples the
SERVER's RSS, so what we measure is server-side buffering, not client memory.
"""

import argparse
import json
import os
import sys
import time
import warnings

warnings.filterwarnings("ignore")

FLIGHT_URI = os.environ.get("MEM_FLIGHT_URI", "grpc://127.0.0.1:50051")
PG_HOST = os.environ.get("MEM_PG_HOST", "127.0.0.1")
PG_PORT = int(os.environ.get("MEM_PG_PORT", "5459"))
NS = "demo"

# 5-column trips-like shape. Kept deliberately simple so the schema is stable
# across create-table / ingest and both read lanes.
PG_DDL_COLS = (
    "trip_id bigint, city text, distance_km double precision, "
    "fare double precision"
)


def _pg_connect():
    import psycopg2

    return psycopg2.connect(
        host=PG_HOST, port=PG_PORT, user="postgres", password="postgres",
        dbname="icegres", connect_timeout=10,
    )


def _emit(op, rows, seconds, **extra):
    rec = {"op": op, "rows": rows, "seconds": round(seconds, 4)}
    rec.update(extra)
    sys.stdout.write(json.dumps(rec) + "\n")
    sys.stdout.flush()


def cmd_create_table(args):
    conn = _pg_connect()
    conn.autocommit = True
    with conn.cursor() as cur:
        cur.execute(f'CREATE TABLE {NS}.{args.table} ({PG_DDL_COLS})')
    conn.close()
    _emit("create-table", 0, 0.0, table=args.table)


def cmd_drop_table(args):
    # Fully best-effort DROP-IF-EXISTS: this is cleanup, so it must never fail
    # the run (no such table on the first pass, etc.). Always exits 0.
    try:
        conn = _pg_connect()
        conn.autocommit = True
        with conn.cursor() as cur:
            try:
                cur.execute(f'DROP TABLE {NS}.{args.table}')
            except Exception:
                conn.rollback()
                try:
                    cur.execute(f'DELETE FROM {NS}.{args.table}')
                except Exception:
                    conn.rollback()
        conn.close()
    except Exception:
        pass
    _emit("drop-table", 0, 0.0, table=args.table)


def _build_batches(n, batch_rows):
    """Yield pyarrow RecordBatches totalling n rows, batch_rows at a time, so the
    client itself never materializes all n rows of Python objects at once."""
    import pyarrow as pa

    cities = ["Paris", "Lyon", "Rome", "Berlin"]
    produced = 0
    while produced < n:
        m = min(batch_rows, n - produced)
        base = produced
        trip = pa.array(range(base, base + m), pa.int64())
        city = pa.array([cities[(base + i) & 3] for i in range(m)], pa.string())
        dist = pa.array([((base + i) % 3000) / 10.0 for i in range(m)], pa.float64())
        fare = pa.array([2.5 + ((base + i) % 2000) / 7.0 for i in range(m)], pa.float64())
        yield pa.record_batch(
            [trip, city, dist, fare],
            names=["trip_id", "city", "distance_km", "fare"],
        )
        produced += m


def cmd_ingest(args):
    import pyarrow as pa
    import adbc_driver_flightsql.dbapi as flight_dbapi

    n = args.rows
    # Build a single Arrow table on the client, then hand it to adbc_ingest. The
    # server-side buffering (or streaming) of this upload is exactly what we
    # measure; the client-side table lives in this process, not the server's.
    tbl = pa.Table.from_batches(list(_build_batches(n, args.batch_rows)))
    conn = flight_dbapi.connect(FLIGHT_URI)
    cur = conn.cursor()
    t0 = time.perf_counter()
    count = cur.adbc_ingest(args.table, tbl, mode="append", db_schema_name=NS)
    dt = time.perf_counter() - t0
    cur.close()
    conn.close()
    _emit("ingest", int(count if count and count > 0 else n), dt, table=args.table)


def cmd_read_flight(args):
    import adbc_driver_flightsql.dbapi as flight_dbapi

    conn = flight_dbapi.connect(FLIGHT_URI)
    cur = conn.cursor()
    t0 = time.perf_counter()
    cur.execute(f'SELECT * FROM {NS}.{args.table}')
    reader = cur.fetch_record_batch()  # streaming RecordBatchReader
    rows = 0
    for batch in reader:  # drain server-side stream, discard batches
        rows += batch.num_rows
    dt = time.perf_counter() - t0
    cur.close()
    conn.close()
    _emit("read-flight", rows, dt, table=args.table)


class _Sink:
    """A write-only file-like that counts bytes and discards them, so COPY TO
    STDOUT drains the whole server stream without buffering it client-side."""

    __slots__ = ("n",)

    def __init__(self):
        self.n = 0

    def write(self, b):
        self.n += len(b)
        return len(b)

    def flush(self):
        pass


def cmd_read_pg(args):
    conn = _pg_connect()
    conn.autocommit = True
    sink = _Sink()
    t0 = time.perf_counter()
    with conn.cursor() as cur:
        cur.copy_expert(
            f'COPY (SELECT * FROM {NS}.{args.table}) TO STDOUT (FORMAT binary)',
            sink,
        )
    dt = time.perf_counter() - t0
    conn.close()
    # Row count isn't cheaply available from binary COPY; report bytes drained.
    _emit("read-pg", -1, dt, table=args.table, bytes=sink.n)


def main():
    ap = argparse.ArgumentParser(description="icegres memory-under-load driver")
    sub = ap.add_subparsers(dest="cmd", required=True)
    for name in ("create-table", "drop-table", "read-flight", "read-pg"):
        p = sub.add_parser(name)
        p.add_argument("--table", required=True)
    pi = sub.add_parser("ingest")
    pi.add_argument("--table", required=True)
    pi.add_argument("--rows", type=int, required=True)
    pi.add_argument("--batch-rows", type=int, default=100_000)
    args = ap.parse_args()

    dispatch = {
        "create-table": cmd_create_table,
        "drop-table": cmd_drop_table,
        "ingest": cmd_ingest,
        "read-flight": cmd_read_flight,
        "read-pg": cmd_read_pg,
    }
    dispatch[args.cmd](args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
