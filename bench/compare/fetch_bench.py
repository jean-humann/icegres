#!/usr/bin/env python3
"""Client fetch benchmark: the case that actually distinguishes ADBC.

Earlier driver timings measured tiny scalar queries where the per-query
protocol floor dominates and ADBC's columnar transport never shows. ADBC's
real advantage is on LARGE result sets fetched into Arrow / pandas — data
stays columnar end to end, avoiding the row-by-row object materialisation that
psycopg2 / pyodbc / JDBC pay. This measures exactly that, across two table
shapes and four row-count buckets so the recommendation can be per-case.

Tables (served by icegres over its live Iceberg lakehouse):
  * demo.trips_big  — 5,000,000 rows x  5 cols  (narrow)
  * demo.wide15     — 5,000,000 rows x 15 cols  (wide, mixed int/double/text)

Row-count buckets: 5k (<10k), 50k (<100k), 500k (<1M), 5M (full table).

For each (table, N, client) we time:
  * fetch_arrow_ms  — SELECT ... LIMIT N  ->  pyarrow.Table
  * fetch_pandas_ms — SELECT ... LIMIT N  ->  pandas.DataFrame

Clients:
  * pgwire (psycopg2)        — row-oriented; Arrow/pandas built from Python tuples
  * ODBC (psqlODBC)          — row-oriented; same penalty
  * ADBC Flight SQL          — Arrow-native (fetch_arrow_table / fetch_df)
  * ADBC postgres (COPY)     — libpq COPY binary -> Arrow in the driver
  * DuckDB (local Iceberg parquet) — Arrow-native gold standard, reading the
      lakehouse's actual current-snapshot data files (staged locally because
      DuckDB's httpfs/iceberg extensions are CDN-blocked in this environment;
      identical Parquet bytes)

Output: JSON to stdout + a human table to stderr.
"""
import json
import statistics
import sys
import time

HOST = "127.0.0.1"
PG_PORT = 5439
FLIGHT_PORT = 50051
LAKEDATA = "/tmp/claude-0/-home-user-jean-humann/917b2dd2-1f49-560f-8a42-71e5677bbc01/scratchpad/lakedata"

# per-size iteration counts: fewer reps at 5M to keep wall-clock and RAM sane
SIZE_PLAN = [
    (5_000, 5, 2),
    (50_000, 5, 2),
    (500_000, 5, 1),
    (5_000_000, 3, 1),
]
SIZES = [s for s, _, _ in SIZE_PLAN]
REPS = {s: (it, wu) for s, it, wu in SIZE_PLAN}

TABLES = {
    "trips_big": ["trip_id", "city", "distance_km", "fare", "ts"],
    "wide15": [
        "c_l1", "c_l2", "c_l3", "c_l4", "c_l5", "c_l6",
        "c_d1", "c_d2", "c_d3", "c_d4", "c_d5",
        "c_s1", "c_s2", "c_s3", "c_s4",
    ],
}

results = {}  # results[table][client][N] = {...}


def timed(fn, n_iter, n_warm):
    for _ in range(n_warm):
        fn()
    ts = []
    for _ in range(n_iter):
        t0 = time.perf_counter()
        n = fn()
        ts.append((time.perf_counter() - t0) * 1000)
    ts.sort()
    return round(statistics.median(ts), 1), round(ts[-1], 1), n


def sql(table, n):
    cols = ", ".join(TABLES[table])
    return f"select {cols} from demo.{table} limit {n}"


def record(table, client, n, arrow_med, pandas_med, rows):
    results.setdefault(table, {}).setdefault(client, {})[n] = {
        "fetch_arrow_ms": arrow_med,
        "fetch_pandas_ms": pandas_med,
        "rows": rows,
    }
    print(
        f"  [{table:9}] {client:22} N={n:>9,}  arrow={arrow_med:>9.1f}  "
        f"pandas={pandas_med:>9.1f}  ({rows} rows)",
        file=sys.stderr,
    )


def rows_to_arrow(rows, names):
    import pyarrow as pa
    cols = list(zip(*rows)) if rows else []
    if not cols:
        return pa.table({})
    return pa.table({names[i]: list(cols[i]) for i in range(len(cols))})


# ---- pgwire (psycopg2): row-oriented -------------------------------------
def run_psycopg2():
    import psycopg2
    import pandas as pd

    cn = psycopg2.connect(host=HOST, port=PG_PORT, dbname="icegres", user="postgres")
    cn.autocommit = True
    for table, names in TABLES.items():
        for n in SIZES:
            it, wu = REPS[n]
            q = sql(table, n)

            def to_pandas(q=q):
                return len(pd.read_sql(q, cn))

            def to_arrow(q=q, names=names):
                cur = cn.cursor(); cur.execute(q); rows = cur.fetchall(); cur.close()
                return rows_to_arrow(rows, names).num_rows

            a_med, _, r = timed(to_arrow, it, wu)
            p_med, _, _ = timed(to_pandas, it, wu)
            record(table, "pgwire (psycopg2)", n, a_med, p_med, r)
    cn.close()


# ---- ODBC (psqlODBC): row-oriented ---------------------------------------
def run_pyodbc():
    import pyodbc
    import pandas as pd

    cn = pyodbc.connect(
        "DRIVER={PostgreSQL Unicode};Server=%s;Port=%d;Database=icegres;UID=postgres;"
        "SSLmode=disable;UseDeclareFetch=0" % (HOST, PG_PORT),
        autocommit=True,
    )
    for table, names in TABLES.items():
        for n in SIZES:
            it, wu = REPS[n]
            q = sql(table, n)

            def to_pandas(q=q):
                cur = cn.cursor(); cur.execute(q)
                cols = [c[0] for c in cur.description]
                df = pd.DataFrame.from_records(cur.fetchall(), columns=cols); cur.close()
                return len(df)

            def to_arrow(q=q, names=names):
                cur = cn.cursor(); cur.execute(q); rows = cur.fetchall(); cur.close()
                return rows_to_arrow(rows, names).num_rows

            a_med, _, r = timed(to_arrow, it, wu)
            p_med, _, _ = timed(to_pandas, it, wu)
            record(table, "ODBC (psqlODBC)", n, a_med, p_med, r)
    cn.close()


# ---- ADBC Flight SQL: Arrow-native ---------------------------------------
def run_adbc_flight():
    from adbc_driver_flightsql import dbapi as fl
    cn = fl.connect(f"grpc://{HOST}:{FLIGHT_PORT}")
    for table in TABLES:
        for n in SIZES:
            it, wu = REPS[n]
            q = sql(table, n)

            def to_arrow(q=q):
                cur = cn.cursor(); cur.execute(q); t = cur.fetch_arrow_table(); cur.close()
                return t.num_rows

            def to_pandas(q=q):
                cur = cn.cursor(); cur.execute(q); df = cur.fetch_df(); cur.close()
                return len(df)

            a_med, _, r = timed(to_arrow, it, wu)
            p_med, _, _ = timed(to_pandas, it, wu)
            record(table, "ADBC (Flight SQL)", n, a_med, p_med, r)
    cn.close()


# ---- ADBC postgres (COPY binary -> Arrow) --------------------------------
def run_adbc_pg():
    from adbc_driver_postgresql import dbapi as pg
    cn = pg.connect(f"postgresql://postgres@{HOST}:{PG_PORT}/icegres")
    for table in TABLES:
        for n in SIZES:
            it, wu = REPS[n]
            q = sql(table, n)

            def to_arrow(q=q):
                cur = cn.cursor(); cur.execute(q); t = cur.fetch_arrow_table(); cur.close()
                return t.num_rows

            def to_pandas(q=q):
                cur = cn.cursor(); cur.execute(q); df = cur.fetch_df(); cur.close()
                return len(df)

            a_med, _, r = timed(to_arrow, it, wu)
            p_med, _, _ = timed(to_pandas, it, wu)
            record(table, "ADBC (postgres/COPY)", n, a_med, p_med, r)
    cn.close()


# ---- DuckDB reading the lakehouse's Iceberg data files (local parquet) ----
def run_duckdb():
    import duckdb
    con = duckdb.connect()
    for table, names in TABLES.items():
        con.execute(
            f"create view {table} as "
            f"select * from read_parquet('{LAKEDATA}/{table}/*.parquet')"
        )
        cols = ", ".join(names)
        for n in SIZES:
            it, wu = REPS[n]
            q = f"select {cols} from {table} limit {n}"

            def to_arrow(q=q):
                return con.execute(q).fetch_arrow_table().num_rows

            def to_pandas(q=q):
                return len(con.execute(q).df())

            a_med, _, r = timed(to_arrow, it, wu)
            p_med, _, _ = timed(to_pandas, it, wu)
            record(table, "DuckDB (iceberg parquet)", n, a_med, p_med, r)
    con.close()


for label, fn in [
    ("psycopg2", run_psycopg2),
    ("pyodbc", run_pyodbc),
    ("adbc-flight", run_adbc_flight),
    ("adbc-pg", run_adbc_pg),
    ("duckdb", run_duckdb),
]:
    try:
        fn()
    except Exception as e:
        print(f"{label}: {str(e)[:220]}", file=sys.stderr)

print(json.dumps(
    {"sizes": SIZES, "reps": {str(k): v for k, v in REPS.items()},
     "tables": {k: len(v) for k, v in TABLES.items()}, "results": results},
    indent=2))
