#!/usr/bin/env python3
"""A8 — real ORM/driver compatibility probe against a live icegres server.

Exercises, with real clients (psycopg2, pg8000, SQLAlchemy 2.x, pandas):

  1.  psycopg2 connect + simple query               (simple query protocol)
  2.  pg8000 connect + query                        (extended query protocol)
  3.  auth+TLS connect variants                     (only when ICEGRES_PROBE_SECURE_* set)
  4.  SQLAlchemy inspect(): schemas / table names
  5.  SQLAlchemy inspect(): columns + types of demo.trips
  6.  Reflection of demo.trips into a Table object
  7.  ORM expression language: SELECT + filter
  8.  ORM expression language: aggregate GROUP BY
  9.  pandas read_sql of a join (trips x cities)
  10. psycopg2 server-side (named) cursor           (XFAIL: DECLARE CURSOR is
      not implemented by the DataFusion pgwire front-end — documented limit)
  11. prepared-statement reuse (pg8000, two executions, different params)
  12. transactions via the driver: BEGIN/ROLLBACK discards, BEGIN/COMMIT
      persists (write uses trip_id >= 930000, cleaned up afterwards)
  13. SQLAlchemy over pg8000 (reflection + query on the extended protocol)

Environment:
  ICEGRES_PROBE_HOST / ICEGRES_PROBE_PORT       target server (default 127.0.0.1:5439)
  ICEGRES_PROBE_SECURE_PORT / _SECURE_USER /    optional auth+TLS server for step 3
  _SECURE_PASSWORD / _SECURE_ROOTCERT           (skipped when unset)

Exit code 0 when every non-XFAIL/SKIP step passed, 2 otherwise. Output is
one line per step: PASS/FAIL/XFAIL/SKIP <name> -- <detail>.
"""

import os
import random
import sys

HOST = os.environ.get("ICEGRES_PROBE_HOST", "127.0.0.1")
PORT = int(os.environ.get("ICEGRES_PROBE_PORT", "5439"))
SECURE_PORT = os.environ.get("ICEGRES_PROBE_SECURE_PORT")
SECURE_USER = os.environ.get("ICEGRES_PROBE_SECURE_USER")
SECURE_PASSWORD = os.environ.get("ICEGRES_PROBE_SECURE_PASSWORD")
SECURE_ROOTCERT = os.environ.get("ICEGRES_PROBE_SECURE_ROOTCERT")

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
        record("PASS", name, str(detail)[:200])
        return detail
    except Exception as ex:  # noqa: BLE001 - a probe must report, not crash
        msg = f"{type(ex).__name__}: {str(ex)[:260]}"
        if xfail:
            record("XFAIL", name, f"{xfail} [{msg}]")
        else:
            record("FAIL", name, msg)
        return None


def main():
    import pandas as pd
    import pg8000.native
    import psycopg2
    import sqlalchemy as sa
    from sqlalchemy import inspect

    # -- 1/2: raw driver connects ------------------------------------------
    def psycopg2_connect():
        conn = psycopg2.connect(host=HOST, port=PORT, user="postgres", dbname="icegres")
        cur = conn.cursor()
        cur.execute("select count(*) from demo.trips")
        n = cur.fetchone()[0]
        sv = conn.get_parameter_status("server_version")
        conn.close()
        return f"count={n}, server_version={sv}"

    step("psycopg2 connect+query (simple protocol)", psycopg2_connect)

    def pg8000_connect():
        conn = pg8000.native.Connection(
            "postgres", host=HOST, port=PORT, database="icegres", password="ignored"
        )
        n = conn.run("select count(*) from demo.cities")[0][0]
        conn.close()
        return f"count={n}"

    step("pg8000 connect+query (extended protocol)", pg8000_connect)

    # -- 3: auth + TLS variants --------------------------------------------
    secure_skip = None
    if not (SECURE_PORT and SECURE_USER and SECURE_PASSWORD):
        secure_skip = "ICEGRES_PROBE_SECURE_* not set"

    def secure_connect():
        conn = psycopg2.connect(
            host=HOST,
            port=int(SECURE_PORT),
            user=SECURE_USER,
            password=SECURE_PASSWORD,
            dbname="icegres",
            sslmode="require",
        )
        cur = conn.cursor()
        cur.execute("select 1")
        one = cur.fetchone()[0]
        ssl_in_use = conn.info.ssl_in_use
        conn.close()
        return f"select 1={one}, ssl_in_use={ssl_in_use} (SCRAM-SHA-256 + TLS)"

    step("psycopg2 connect with SCRAM auth over TLS", secure_connect, skip=secure_skip)

    def wrong_password_rejected():
        try:
            psycopg2.connect(
                host=HOST,
                port=int(SECURE_PORT),
                user=SECURE_USER,
                password="definitely-wrong",
                dbname="icegres",
                sslmode="require",
                connect_timeout=5,
            )
        except psycopg2.OperationalError as ex:
            return f"rejected as expected: {str(ex).strip()[:120]}"
        raise AssertionError("wrong password was ACCEPTED")

    step("wrong password rejected (28P01)", wrong_password_rejected, skip=secure_skip)

    # -- 4-8: SQLAlchemy engine, inspection, reflection, ORM queries -------
    engine = sa.create_engine(
        f"postgresql+psycopg2://postgres:ignored@{HOST}:{PORT}/icegres"
    )

    insp = inspect(engine)
    step(
        "SQLAlchemy inspect(): schemas+tables",
        lambda: {"schemas": insp.get_schema_names(), "tables": sorted(insp.get_table_names(schema="demo"))[:4]},
    )
    cols = step(
        "SQLAlchemy inspect(): columns+types of demo.trips",
        lambda: [(c["name"], str(c["type"])) for c in insp.get_columns("trips", schema="demo")],
    )
    if cols is not None:
        names = [c[0] for c in cols]
        assert names == ["trip_id", "city", "distance_km", "fare", "ts"], names

    metadata = sa.MetaData()
    trips = step(
        "reflect demo.trips into a Table object",
        lambda: sa.Table("trips", metadata, schema="demo", autoload_with=engine),
    )

    if trips is not None:
        with engine.connect() as conn:
            step(
                "ORM expression: SELECT + filter",
                lambda: "rows(distance_km>10)="
                + str(
                    conn.execute(
                        sa.select(sa.func.count())
                        .select_from(trips)
                        .where(trips.c.distance_km > 10)
                    ).scalar()
                ),
            )
            step(
                "ORM expression: aggregate GROUP BY",
                lambda: conn.execute(
                    sa.select(trips.c.city, sa.func.count().label("n"))
                    .group_by(trips.c.city)
                    .order_by(trips.c.city)
                ).fetchall()[:3],
            )

    # -- 9: pandas read_sql of a join --------------------------------------
    def pandas_join():
        with engine.connect() as conn:
            df = pd.read_sql(
                sa.text(
                    "select t.city, c.population, count(*) as trips, avg(t.fare) as avg_fare "
                    "from demo.trips t join demo.cities c on t.city = c.city "
                    "group by t.city, c.population order by t.city"
                ),
                conn,
            )
        assert len(df) > 0 and set(df.columns) == {"city", "population", "trips", "avg_fare"}
        return f"shape={df.shape}, first={df.iloc[0].to_dict()}"

    step("pandas read_sql of a join", pandas_join)

    # -- 10: server-side cursor (documented limit) -------------------------
    def server_side_cursor():
        conn = psycopg2.connect(host=HOST, port=PORT, user="postgres", dbname="icegres")
        try:
            cur = conn.cursor(name="a8_probe_cursor")  # named => DECLARE CURSOR
            cur.execute("select trip_id from demo.trips order by trip_id limit 10")
            rows = cur.fetchmany(5)
            return f"fetched {len(rows)} rows via named cursor"
        finally:
            conn.close()

    step(
        "psycopg2 server-side (named) cursor",
        server_side_cursor,
        xfail="DECLARE CURSOR/FETCH not implemented by the DataFusion pgwire "
        "front-end (architecturally out of scope; use client-side cursors)",
    )

    # -- 11: prepared-statement reuse ---------------------------------------
    def prepared_reuse():
        conn = pg8000.native.Connection(
            "postgres", host=HOST, port=PORT, database="icegres", password="ignored"
        )
        try:
            ps = conn.prepare("select city from demo.trips where trip_id = :tid")
            a = ps.run(tid=1)
            b = ps.run(tid=2)
            ps.close()
            assert a and b and a != [] and b != []
            return f"same prepared stmt, two executions: trip 1 -> {a[0][0]}, trip 2 -> {b[0][0]}"
        finally:
            conn.close()

    step("prepared-statement reuse (pg8000)", prepared_reuse)

    # -- 12: transactions via the driver ------------------------------------
    probe_id = 930000 + random.randint(0, 9999)

    def txn_rollback():
        conn = psycopg2.connect(host=HOST, port=PORT, user="postgres", dbname="icegres")
        try:
            cur = conn.cursor()  # psycopg2 opens a transaction implicitly
            cur.execute(
                "insert into demo.trips values (%s, 'A8Rollback', 1.0, 1.0, timestamp '2026-01-01 00:00:00')",
                (probe_id,),
            )
            cur.execute("select count(*) from demo.trips where trip_id = %s", (probe_id,))
            seen_inside = cur.fetchone()[0]
            conn.rollback()
            cur = conn.cursor()
            cur.execute("select count(*) from demo.trips where trip_id = %s", (probe_id,))
            after = cur.fetchone()[0]
            assert seen_inside == 1, f"read-your-own-writes failed: {seen_inside}"
            assert after == 0, f"rollback leaked a row: {after}"
            return f"insert visible in txn ({seen_inside}), gone after rollback ({after})"
        finally:
            conn.close()

    step("transaction BEGIN/ROLLBACK via psycopg2", txn_rollback)

    def txn_commit():
        conn = psycopg2.connect(host=HOST, port=PORT, user="postgres", dbname="icegres")
        try:
            cur = conn.cursor()
            cur.execute(
                "insert into demo.trips values (%s, 'A8Commit', 2.0, 2.0, timestamp '2026-01-01 00:00:00')",
                (probe_id,),
            )
            conn.commit()
        finally:
            conn.close()
        # fresh connection must see the committed row
        conn = psycopg2.connect(host=HOST, port=PORT, user="postgres", dbname="icegres")
        try:
            cur = conn.cursor()
            cur.execute("select count(*) from demo.trips where trip_id = %s", (probe_id,))
            seen = cur.fetchone()[0]
            cur.execute("delete from demo.trips where trip_id = %s", (probe_id,))
            conn.commit()
            assert seen == 1, f"committed row not visible cross-connection: {seen}"
            return f"committed row visible from a new connection ({seen}), cleaned up"
        finally:
            conn.close()

    step("transaction BEGIN/COMMIT via psycopg2 (cross-connection)", txn_commit)

    # -- 13: SQLAlchemy over pg8000 (extended protocol end-to-end) ----------
    def sqlalchemy_pg8000():
        # AUTOCOMMIT: icegres answers extended-protocol SELECT inside an
        # EXPLICIT transaction with a clean 0A000 (the hook cannot see the
        # portal's result format) — see the XFAIL below. Reflection and
        # queries over the extended protocol work outside transactions.
        e2 = sa.create_engine(
            f"postgresql+pg8000://postgres:ignored@{HOST}:{PORT}/icegres",
            isolation_level="AUTOCOMMIT",
        )
        md2 = sa.MetaData()
        t2 = sa.Table("cities", md2, schema="demo", autoload_with=e2)
        with e2.connect() as conn:
            n = conn.execute(sa.select(sa.func.count()).select_from(t2)).scalar()
        e2.dispose()
        return f"reflected demo.cities {t2.columns.keys()}, count={n}"

    step("SQLAlchemy over pg8000, autocommit (reflect+query)", sqlalchemy_pg8000)

    def pg8000_select_in_explicit_txn():
        conn = pg8000.native.Connection(
            "postgres", host=HOST, port=PORT, database="icegres", password="ignored"
        )
        try:
            conn.run("BEGIN")
            rows = conn.run("select count(*) from demo.trips")
            conn.run("COMMIT")
            return f"count={rows[0][0]}"
        finally:
            conn.close()

    step(
        "pg8000 SELECT inside explicit transaction",
        pg8000_select_in_explicit_txn,
        xfail="documented limit: extended-protocol SELECT inside an explicit "
        "transaction is rejected with 0A000 (transactional SELECT is simple-"
        "protocol only; failing query: BEGIN; select count(*) from demo.trips)",
    )

    engine.dispose()

    # -- summary -------------------------------------------------------------
    n = {s: sum(1 for r in RESULTS if r[0] == s) for s in ("PASS", "FAIL", "XFAIL", "SKIP")}
    print(
        f"A8 RESULT: pass={n['PASS']} fail={n['FAIL']} xfail={n['XFAIL']} skip={n['SKIP']}",
        flush=True,
    )
    return 0 if n["FAIL"] == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
