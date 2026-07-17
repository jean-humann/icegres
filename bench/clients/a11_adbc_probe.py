#!/usr/bin/env python3
"""A11 — ADBC first-class probe against live icegres endpoints, two lanes.

Lane 1 — Arrow Flight SQL (`icegres flight-serve`, adbc_driver_flightsql):
  1.  connect + simple query (GetFlightInfo -> DoGet Arrow stream)
  2.  adbc_get_objects depth=all: catalog/schema/tables/columns
  3.  get_table_types + adbc_get_info (CommandGetTableTypes/GetSqlInfo)
  4.  parameterized query ($1 bind over DoPut(CommandPreparedStatementQuery))
  5.  prepared statement reuse (same SQL, two parameter sets)
  6.  DML via ExecuteUpdate (CommandStatementUpdate): INSERT/UPDATE/DELETE
      with real affected-row counts (UPDATE/DELETE run the same
      copy-on-write engine as the pgwire DmlHook)
  7.  executemany: prepared INSERT with 2 bound rows (documented: one
      Iceberg commit per row — bulk data belongs in adbc_ingest)
  8.  BULK INGEST (CommandStatementIngest): cursor.adbc_ingest(...,
      mode="append") into demo.adbc_ingest — verifies row count AND that
      the whole stream landed as exactly ONE Iceberg commit ($snapshots)
  9.  ingest mode="create" is rejected loudly (append-only scope)
  10. statement schema metadata (adbc_execute_schema)
  11. basic-auth variants (only when ICEGRES_PROBE_FLIGHT_SECURE_* set):
      no creds rejected, wrong password rejected, right password queries

Lane 2 — Postgres wire (`icegres serve`, adbc_driver_postgresql/libpq —
requires the COPY ... TO STDOUT (FORMAT binary) hook, icegres/src/ops.rs):
  12. connect (driver probes pg_catalog.pg_type on connect) + COPY-backed
      reads: count, point lookup, GROUP BY (autocommit mode)
  13. Arrow fetch (cursor.fetch_arrow_table over the COPY binary stream)
  14. parameterized query ($1, libpq prepared statement, binary results)
  15. adbc_get_objects (catalog introspection over pg_catalog)
  16. DML INSERT/DELETE with rowcounts
  17. XFAIL bulk ingest: driver issues COPY ... FROM STDIN (FORMAT binary)
      — out of scope by design; ADBC ingest goes through the Flight lane
  18. XFAIL parameterized SELECT inside a driver transaction (autocommit
      off): pre-existing 0A000 limit (extended-protocol SELECT in explicit
      transactions), unchanged by this round

Environment:
  ICEGRES_PROBE_FLIGHT_HOST / ICEGRES_PROBE_FLIGHT_PORT   (default 127.0.0.1:50051)
  ICEGRES_PROBE_PG_HOST / ICEGRES_PROBE_PG_PORT           (default 127.0.0.1:5439)
  ICEGRES_PROBE_FLIGHT_SECURE_PORT / _USER / _PASSWORD    (step 11; skipped when unset)
  ICEGRES_PROBE_SKIP_PG=1                                 skip lane 2 entirely

Write hygiene: this probe appends/deletes only trip_id >= 940000 in
demo.trips and clears demo.adbc_ingest (a dedicated scratch table seeded
empty by `icegres seed`) — deterministic assertions elsewhere are safe.

Exit code 0 when every non-XFAIL/SKIP step passed, 2 otherwise. Output is
one line per step: PASS/FAIL/XFAIL/SKIP <name> -- <detail>.
"""

import os
import sys
import time
import warnings

warnings.filterwarnings("ignore")  # dbapi autocommit warning is expected

FLIGHT_HOST = os.environ.get("ICEGRES_PROBE_FLIGHT_HOST", "127.0.0.1")
FLIGHT_PORT = int(os.environ.get("ICEGRES_PROBE_FLIGHT_PORT", "50051"))
PG_HOST = os.environ.get("ICEGRES_PROBE_PG_HOST", "127.0.0.1")
PG_PORT = int(os.environ.get("ICEGRES_PROBE_PG_PORT", "5439"))
SECURE_PORT = os.environ.get("ICEGRES_PROBE_FLIGHT_SECURE_PORT")
SECURE_USER = os.environ.get("ICEGRES_PROBE_FLIGHT_SECURE_USER")
SECURE_PASSWORD = os.environ.get("ICEGRES_PROBE_FLIGHT_SECURE_PASSWORD")
SKIP_PG = os.environ.get("ICEGRES_PROBE_SKIP_PG") == "1"

FLIGHT_URI = f"grpc://{FLIGHT_HOST}:{FLIGHT_PORT}"
PG_URI = f"postgresql://postgres:postgres@{PG_HOST}:{PG_PORT}/icegres"

RESULTS = []


def record(status, name, detail=""):
    line = f"{status} {name}"
    if detail:
        line += f" -- {detail}"
    print(line, flush=True)
    RESULTS.append((status, name, detail))


def step(name, fn, xfail=None, skip=None):
    if skip:
        record("SKIP", name, skip)
        return None
    try:
        detail = fn()
        record("PASS", name, str(detail)[:220])
        return detail
    except Exception as ex:  # noqa: BLE001 - a probe must report, not crash
        msg = f"{type(ex).__name__}: {str(ex)[:280]}"
        if xfail:
            record("XFAIL", name, f"{xfail} [{msg}]")
        else:
            record("FAIL", name, msg)
        return None


def exec_update(conn, sql):
    """Affected-row count via the low-level ADBC ExecuteUpdate path."""
    cur = conn.cursor()
    try:
        cur.adbc_statement.set_sql_query(sql)
        return cur.adbc_statement.execute_update()
    finally:
        cur.close()


def main():
    import adbc_driver_flightsql.dbapi as flight_dbapi
    import pyarrow as pa

    # ======================================================================
    # Lane 1: Flight SQL / adbc_driver_flightsql
    # ======================================================================
    conn = flight_dbapi.connect(FLIGHT_URI)
    cur = conn.cursor()

    def fs_query():
        cur.execute("SELECT count(*) FROM demo.trips WHERE trip_id BETWEEN 1 AND 280")
        n = cur.fetchone()[0]
        assert n == 280, f"expected 280 seeded trips, got {n}"
        return f"seeded count={n} via GetFlightInfo->DoGet"

    step("flight: connect + query (Arrow stream)", fs_query)

    def fs_get_objects():
        objs = conn.adbc_get_objects(depth="all").read_all().to_pylist()
        cats = [c["catalog_name"] for c in objs]
        assert "icegres" in cats, f"catalogs: {cats}"
        schemas = {
            s["db_schema_name"]: s
            for c in objs
            for s in (c["catalog_db_schemas"] or [])
        }
        assert "demo" in schemas, f"schemas: {list(schemas)}"
        tables = {t["table_name"]: t for t in (schemas["demo"]["db_schema_tables"] or [])}
        for expect in ("trips", "cities", "adbc_ingest"):
            assert expect in tables, f"missing table {expect} in {sorted(tables)[:8]}"
        cols = [c["column_name"] for c in (tables["trips"]["table_columns"] or [])]
        assert cols == ["trip_id", "city", "distance_km", "fare", "ts"], cols
        return f"catalog=icegres schema=demo tables>={len(tables)} trips columns={cols}"

    step("flight: adbc_get_objects catalogs/schemas/tables/columns", fs_get_objects)

    def fs_metadata():
        tts = conn.adbc_get_table_types()
        assert tts == ["TABLE"], tts
        info = conn.adbc_get_info()
        vendor = info.get("vendor_name")
        assert vendor == "icegres", info
        return f"table_types={tts} vendor={vendor} {info.get('vendor_version')}"

    step("flight: get_table_types + get_info (SqlInfo)", fs_metadata)

    def fs_param():
        cur.execute("SELECT city, fare FROM demo.trips WHERE trip_id = $1", parameters=(7,))
        row = cur.fetchone()
        assert row is not None and row[0] == "Paris", row
        return f"trip 7 -> {row}"

    step("flight: parameterized query ($1 bind)", fs_param)

    def fs_prepared_reuse():
        out = []
        for tid in (7, 8):
            cur.execute("SELECT city FROM demo.trips WHERE trip_id = $1", parameters=(tid,))
            out.append((tid, cur.fetchone()[0]))
        return f"two executions, params 7/8 -> {out}"

    step("flight: prepared statement reuse (2 param sets)", fs_prepared_reuse)

    def fs_dml():
        n1 = exec_update(
            conn,
            "INSERT INTO demo.trips (trip_id, city, distance_km, fare, ts) VALUES "
            "(940001, 'Oslo', 1.5, 4.0, TIMESTAMP '2026-07-06 08:00:00')",
        )
        n2 = exec_update(conn, "UPDATE demo.trips SET fare = 5.5 WHERE trip_id = 940001")
        cur.execute("SELECT fare FROM demo.trips WHERE trip_id = 940001")
        fare = cur.fetchone()[0]
        n3 = exec_update(conn, "DELETE FROM demo.trips WHERE trip_id = 940001")
        assert (n1, n2, n3) == (1, 1, 1), (n1, n2, n3)
        assert abs(fare - 5.5) < 1e-9, fare
        return f"INSERT={n1} UPDATE={n2} (fare read back {fare}) DELETE={n3}"

    step("flight: DML via ExecuteUpdate with affected counts", fs_dml)

    def fs_executemany():
        cur.executemany(
            "INSERT INTO demo.trips (trip_id, city, distance_km, fare, ts) VALUES "
            "($1, $2, $3, $4, TIMESTAMP '2026-07-06 08:30:00')",
            [(940002, "Lyon", 2.0, 6.0), (940003, "Rome", 3.0, 7.0)],
        )
        rc = cur.rowcount
        cur.execute("SELECT count(*) FROM demo.trips WHERE trip_id IN (940002, 940003)")
        n = cur.fetchone()[0]
        ndel = exec_update(conn, "DELETE FROM demo.trips WHERE trip_id IN (940002, 940003)")
        assert rc == 2 and n == 2 and ndel == 2, (rc, n, ndel)
        return f"rowcount={rc} visible={n} cleanup={ndel} (per-row commits; bulk -> adbc_ingest)"

    step("flight: executemany prepared INSERT (bound rows)", fs_executemany)

    def snapshots_count():
        cur.execute('SELECT * FROM demo."adbc_ingest$snapshots"')
        return len(cur.fetchall())

    def fs_bulk_ingest():
        exec_update(conn, "DELETE FROM demo.adbc_ingest")  # clear scratch
        n = 5000
        tbl = pa.table(
            {
                "trip_id": pa.array(range(950000, 950000 + n), pa.int64()),
                "city": pa.array(["Paris", "Lyon", "Rome", "Berlin"] * (n // 4)),
                "distance_km": pa.array([float(i % 300) / 10.0 for i in range(n)]),
                "fare": pa.array([2.5 + (i % 200) / 7.0 for i in range(n)]),
                "ts": pa.array([None] * n, pa.timestamp("us")),
            }
        )
        t0 = time.perf_counter()
        count = cur.adbc_ingest("adbc_ingest", tbl, mode="append", db_schema_name="demo")
        dt = time.perf_counter() - t0
        cur.execute("SELECT count(*) FROM demo.adbc_ingest")
        visible = cur.fetchone()[0]
        exec_update(conn, "DELETE FROM demo.adbc_ingest")
        assert count == n and visible == n, (count, visible)
        return f"{n} rows in {dt * 1000:.0f} ms ({n / dt:,.0f} rows/s), visible={visible}"

    def fs_bulk_ingest_one_commit():
        # dedicated, unambiguous one-commit assertion on a clean table
        exec_update(conn, "DELETE FROM demo.adbc_ingest")
        before = snapshots_count()
        tbl = pa.table(
            {
                "trip_id": pa.array(range(960000, 960100), pa.int64()),
                "city": pa.array(["Oslo"] * 100),
                "distance_km": pa.array([1.0] * 100),
                "fare": pa.array([2.0] * 100),
                "ts": pa.array([None] * 100, pa.timestamp("us")),
            }
        )
        count = cur.adbc_ingest("adbc_ingest", tbl, mode="append", db_schema_name="demo")
        delta = snapshots_count() - before
        exec_update(conn, "DELETE FROM demo.adbc_ingest")
        assert count == 100 and delta == 1, (count, delta)
        return f"100 rows -> exactly {delta} Iceberg commit"

    step("flight: BULK INGEST adbc_ingest(mode=append)", fs_bulk_ingest)
    step("flight: bulk ingest = ONE Iceberg commit ($snapshots)", fs_bulk_ingest_one_commit)

    def fs_ingest_create_rejected():
        tbl = pa.table({"a": pa.array([1], pa.int64())})
        try:
            cur.adbc_ingest("a11_no_such_table", tbl, mode="create", db_schema_name="demo")
        except Exception as ex:
            msg = str(ex)
            assert "append" in msg, msg
            return f"rejected as designed: {msg[:120]}"
        raise AssertionError("mode=create unexpectedly succeeded")

    step("flight: ingest mode=create rejected loudly (append-only scope)", fs_ingest_create_rejected)

    def fs_execute_schema():
        sch = cur.adbc_execute_schema("SELECT trip_id, fare FROM demo.trips")
        got = [(f.name, str(f.type)) for f in sch]
        assert got == [("trip_id", "int64"), ("fare", "double")], got
        return f"schema without execution: {got}"

    step("flight: statement schema metadata (execute_schema)", fs_execute_schema)

    secure_skip = (
        None
        if (SECURE_PORT and SECURE_USER and SECURE_PASSWORD)
        else "ICEGRES_PROBE_FLIGHT_SECURE_* not set"
    )

    def fs_auth():
        uri = f"grpc://{FLIGHT_HOST}:{SECURE_PORT}"
        try:
            c = flight_dbapi.connect(uri)
            c.cursor().execute("SELECT 1")
            raise AssertionError("no-credential connect unexpectedly allowed")
        except Exception as ex:
            assert "UNAUTHENTICATED" in str(ex) or "authorization" in str(ex), str(ex)[:120]
        try:
            c = flight_dbapi.connect(
                uri, db_kwargs={"username": SECURE_USER, "password": "definitely-wrong"}
            )
            c.cursor().execute("SELECT 1")
            raise AssertionError("wrong-password connect unexpectedly allowed")
        except Exception as ex:
            assert "authentication failed" in str(ex) or "UNAUTHENTICATED" in str(ex), str(ex)[:120]
        c = flight_dbapi.connect(
            uri, db_kwargs={"username": SECURE_USER, "password": SECURE_PASSWORD}
        )
        cc = c.cursor()
        cc.execute("SELECT count(*) FROM demo.trips WHERE trip_id BETWEEN 1 AND 280")
        n = cc.fetchone()[0]
        cc.close()
        c.close()
        return f"no-creds rejected, wrong-pw rejected, right creds query count={n}"

    step("flight: basic auth handshake (--auth-file)", fs_auth, skip=secure_skip)

    cur.close()
    conn.close()

    # ======================================================================
    # Lane 2: Postgres wire / adbc_driver_postgresql (libpq + COPY)
    # ======================================================================
    if SKIP_PG:
        record("SKIP", "pg lane (all)", "ICEGRES_PROBE_SKIP_PG=1")
    else:
        import adbc_driver_postgresql.dbapi as pg_dbapi

        pconn = step(
            "pg: adbc_driver_postgresql connect (pg_type probe on connect)",
            lambda: pg_dbapi.connect(PG_URI, autocommit=True),
        )
        if pconn is None:
            record("FAIL", "pg lane aborted", "connect failed; remaining pg steps not run")
        else:
            pcur = pconn.cursor()

            def pg_reads():
                pcur.execute("SELECT count(*) FROM demo.trips WHERE trip_id BETWEEN 1 AND 280")
                n = pcur.fetchone()[0]
                pcur.execute("SELECT city, fare FROM demo.trips WHERE trip_id = 7")
                point = pcur.fetchone()
                pcur.execute(
                    "SELECT city, count(*) AS n FROM demo.trips "
                    "WHERE trip_id BETWEEN 1 AND 280 GROUP BY city ORDER BY n DESC, city LIMIT 2"
                )
                top = pcur.fetchall()
                assert n == 280 and point[0] == "Paris", (n, point)
                return f"count={n} point={point} top={top} (all via COPY ... TO STDOUT binary)"

            step("pg: COPY-backed reads (count/point/GROUP BY)", pg_reads)

            def pg_arrow():
                pcur.execute(
                    "SELECT trip_id, distance_km FROM demo.trips "
                    "WHERE trip_id <= 5 ORDER BY trip_id"
                )
                t = pcur.fetch_arrow_table()
                assert t.num_rows == 5 and t.column_names == ["trip_id", "distance_km"], (
                    t.num_rows,
                    t.column_names,
                )
                return f"fetch_arrow_table: {t.num_rows} rows, cols={t.column_names}"

            step("pg: Arrow result fetch (COPY binary -> Arrow)", pg_arrow)

            def pg_param():
                pcur.execute("SELECT city FROM demo.trips WHERE trip_id = $1", parameters=(7,))
                row = pcur.fetchone()
                assert row[0] == "Paris", row
                return f"trip 7 -> {row}"

            step("pg: parameterized query ($1 prepared, binary result)", pg_param)

            def pg_get_objects():
                objs = pconn.adbc_get_objects(depth="all").read_all().to_pylist()
                schemas = {
                    s["db_schema_name"]
                    for c in objs
                    for s in (c["catalog_db_schemas"] or [])
                }
                assert "demo" in schemas, schemas
                return f"schemas via pg_catalog: {sorted(schemas)[:6]}"

            step("pg: adbc_get_objects over pg_catalog", pg_get_objects)

            def pg_dml():
                pcur.execute(
                    "INSERT INTO demo.trips (trip_id, city, distance_km, fare, ts) VALUES "
                    "(940005, 'Oslo', 1.0, 2.0, TIMESTAMP '2026-07-06 09:00:00')"
                )
                rc1 = pcur.rowcount
                pcur.execute("SELECT count(*) FROM demo.trips WHERE trip_id = 940005")
                vis = pcur.fetchone()[0]
                pcur.execute("DELETE FROM demo.trips WHERE trip_id = 940005")
                rc2 = pcur.rowcount
                assert (rc1, vis, rc2) == (1, 1, 1), (rc1, vis, rc2)
                return f"INSERT rowcount={rc1} visible={vis} DELETE rowcount={rc2}"

            step("pg: DML INSERT/DELETE with rowcounts", pg_dml)

            def pg_ingest():
                t = pa.table(
                    {
                        "trip_id": pa.array([940006], pa.int64()),
                        "city": ["Oslo"],
                        "distance_km": [1.0],
                        "fare": [2.0],
                        "ts": pa.array([None], pa.timestamp("us")),
                    }
                )
                n = pcur.adbc_ingest("adbc_ingest", t, mode="append", db_schema_name="demo")
                return f"unexpectedly succeeded: {n}"

            step(
                "pg: bulk ingest via COPY FROM STDIN",
                pg_ingest,
                xfail="out of scope by design: the pg driver ingests via COPY ... FROM "
                "STDIN (FORMAT binary), which icegres rejects — ADBC bulk ingest is "
                "served by the Flight SQL lane (CommandStatementIngest)",
            )

            pcur.close()
            pconn.close()

            def pg_param_in_txn():
                c2 = pg_dbapi.connect(PG_URI)  # autocommit off -> driver BEGINs
                try:
                    cc = c2.cursor()
                    cc.execute("SELECT city FROM demo.trips WHERE trip_id = $1", parameters=(7,))
                    return f"row={cc.fetchone()}"
                finally:
                    c2.close()

            step(
                "pg: parameterized SELECT inside driver transaction",
                pg_param_in_txn,
                xfail="pre-existing documented limit (0A000): extended-protocol SELECT "
                "inside an explicit transaction is simple-protocol only; use "
                "autocommit=True (as the read steps above do)",
            )

    # -- summary -----------------------------------------------------------
    n = {s: sum(1 for r in RESULTS if r[0] == s) for s in ("PASS", "FAIL", "XFAIL", "SKIP")}
    print(
        f"A11 RESULT: pass={n['PASS']} fail={n['FAIL']} xfail={n['XFAIL']} skip={n['SKIP']}",
        flush=True,
    )
    return 0 if n["FAIL"] == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
