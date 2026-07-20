#!/usr/bin/env python3
"""Per-BI-stack fetch benchmark (companion: npgsql-fetch/ for the .NET lane).

Seeds bench.wide1m (1M rows x 5 cols) via adbc_ingest once, then times a
LIMIT-N fetch into each stack's natural client structure. Median of 5
(1 warmup). Prints one JSON line per (client, rows).
"""
import json
import os
import statistics
import sys
import time

import pyarrow as pa

FLIGHT_ZSTD = os.environ.get("BIBENCH_FLIGHT", "grpc://127.0.0.1:50051")
PG = dict(host="127.0.0.1", port=int(os.environ.get("BIBENCH_PG_PORT", "5439")), dbname="icegres", user="postgres")
SIZES = [10_000, 100_000, 1_000_000]
REPS = 5

def seed():
    import adbc_driver_flightsql.dbapi as flight
    import numpy as np
    conn = flight.connect(FLIGHT_ZSTD)
    cur = conn.cursor()
    try:
        cur.execute("SELECT count(*) FROM demo.wide1m")
        n = cur.fetchone()[0]
        if n >= 1_000_000:
            print(f"# seed present: {n} rows", file=sys.stderr)
            return
    except Exception:
        conn.close()
        conn = flight.connect(FLIGHT_ZSTD)
        cur = conn.cursor()
    rng = np.random.default_rng(7)
    cities = np.array(["Paris", "London", "Berlin", "Lisbon", "Zurich", "Oslo"])
    for i in range(4):
        n = 250_000
        tbl = pa.table({
            "trip_id": pa.array(np.arange(i * n, (i + 1) * n), pa.int64()),
            "city": pa.array(cities[rng.integers(0, len(cities), n)]),
            "distance_km": pa.array(rng.uniform(0.5, 60, n), pa.float64()),
            "fare": pa.array(rng.uniform(2, 200, n), pa.float64()),
            "flag": pa.array(rng.integers(0, 2, n).astype(bool)),
        })
        cur.adbc_ingest("wide1m", tbl, mode="create" if False else "append", db_schema_name="demo")
        print(f"# ingested chunk {i+1}/4", file=sys.stderr)
    conn.close()

def timed(fn):
    fn()  # warmup
    times = []
    for _ in range(REPS):
        t0 = time.perf_counter()
        fn()
        times.append((time.perf_counter() - t0) * 1000)
    return round(statistics.median(times), 1)

def sql(n):
    return f"SELECT * FROM demo.wide1m LIMIT {n}"

def bench_psycopg2():
    import psycopg2
    conn = psycopg2.connect(**PG); conn.autocommit = True
    for n in SIZES:
        def go():
            with conn.cursor() as cur:
                cur.execute(sql(n)); cur.fetchall()
        yield "psycopg2 (rows)", n, timed(go)
    conn.close()

def bench_adbc_pg():
    import adbc_driver_postgresql.dbapi as apg
    conn = apg.connect(f"postgresql://postgres:x@127.0.0.1:{PG['port']}/icegres")
    for n in SIZES:
        def go():
            with conn.cursor() as cur:
                cur.execute(sql(n)); cur.fetch_arrow_table()
        yield "ADBC postgres (COPY)", n, timed(go)
    conn.close()

def bench_adbc_flight():
    import adbc_driver_flightsql.dbapi as flight
    conn = flight.connect(FLIGHT_ZSTD)
    for n in SIZES:
        def go():
            with conn.cursor() as cur:
                cur.execute(sql(n)); cur.fetch_arrow_table()
        yield "ADBC Flight SQL", n, timed(go)
    conn.close()

def bench_flightsql_dbapi():
    from flightsql import FlightSQLClient, connect as fconnect
    client = FlightSQLClient(host="127.0.0.1", port=int(os.environ.get("BIBENCH_FLIGHT_PORT", "50051")), insecure=True)
    conn = fconnect(client)
    for n in SIZES:
        def go():
            cur = conn.cursor()
            cur.execute(sql(n)); cur.fetchall()
        yield "flightsql-dbapi (Superset)", n, timed(go)
    conn.close()

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "seed":
        seed(); sys.exit(0)
    for gen in (bench_psycopg2, bench_adbc_pg, bench_adbc_flight, bench_flightsql_dbapi):
        try:
            for client, n, ms in gen():
                print(json.dumps({"client": client, "rows": n, "ms": ms}))
        except Exception as e:
            print(json.dumps({"client": gen.__name__, "error": str(e)[:160]}))
