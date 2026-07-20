#!/usr/bin/env python3
"""A13 — flightsql-dbapi probe (the Superset stack) against live flight-serve.

Exercises InfluxData's `flightsql-dbapi` (DB-API 2 + SQLAlchemy dialect for
Flight SQL — the library Superset connects through, and whose primary
dialect targets DataFusion, exactly the engine icegres runs) against
`icegres flight-serve`. This is the Superset fast lane's stack probe: if
these steps pass, a Superset `datafusion+flightsql://…` database URI has a
verified driver path under it (the Superset *product* is a separate smoke).

Covers:
  1.  DB-API connect + trivial query (SELECT 1)
  2.  data query over demo.trips with cursor.description sanity
  3.  SQLAlchemy engine connect (datafusion+flightsql:// URI)
  4.  reflection: get_schema_names (Superset schema browser)
  5.  reflection: get_table_names(schema=demo) (Superset table list)
  6.  reflection: get_columns(demo.trips) (Superset column panel / SQL Lab)
  7.  SQL Lab-shaped query: GROUP BY aggregate through the engine
  8.  Superset preview shape: SELECT * ... LIMIT n
  9.  auth variants against a secured listener (only when
      ICEGRES_PROBE_FLIGHT_SECURE_* are set): no creds rejected, wrong
      password rejected, right password queries

Environment:
  ICEGRES_PROBE_FLIGHT_HOST / ICEGRES_PROBE_FLIGHT_PORT   (default 127.0.0.1:50051)
  ICEGRES_PROBE_FLIGHT_SECURE_PORT / _USER / _PASSWORD    (step 9; skipped when unset)

Read-only: touches no table content (demo.trips is only read).

Exit: 0 = all non-XFAIL/SKIP steps passed, 2 = failures,
3 = the stack not installed (flightsql-dbapi, sqlalchemy, or pandas —
flightsql-dbapi's cursor materializes results through pandas without
declaring it as a dependency; Superset ships pandas, so requiring it here
matches the stack under test).
Prints one line per step and a final "A13 RESULT: pass=N fail=N xfail=N skip=N".
"""

import os
import sys

HOST = os.environ.get("ICEGRES_PROBE_FLIGHT_HOST", "127.0.0.1")
PORT = int(os.environ.get("ICEGRES_PROBE_FLIGHT_PORT", "50051"))

passes = []
fails = []
xfails = []
skips = []


def ok(name, detail=""):
    passes.append(name)
    print(f"PASS {name}" + (f" -- {detail}" if detail else ""))


def bad(name, err):
    fails.append(name)
    print(f"FAIL {name} -- {str(err)[:220]}")


def skip(name, why):
    skips.append(name)
    print(f"SKIP {name} -- {why}")


try:
    from flightsql import FlightSQLClient, connect as fsql_connect
    import sqlalchemy as sa
    import flightsql.sqlalchemy  # noqa: F401  (registers datafusion+flightsql)
    import pandas  # noqa: F401  (undeclared runtime dep of flightsql's cursor)
except ModuleNotFoundError as exc:
    print(f"A13 SKIP: flightsql-dbapi stack not available ({exc}) "
          "(pip install flightsql-dbapi sqlalchemy pandas)", file=sys.stderr)
    print("A13 RESULT: pass=0 fail=0 xfail=0 skip=1")
    sys.exit(3)


# -- 1. DB-API connect + trivial query --------------------------------------
conn = None
try:
    client = FlightSQLClient(host=HOST, port=PORT, insecure=True)
    conn = fsql_connect(client)
    cur = conn.cursor()
    cur.execute("SELECT 1")
    row = cur.fetchall()[0]
    assert int(row[0]) == 1, f"SELECT 1 returned {row!r}"
    ok("dbapi connect + SELECT 1")
except Exception as e:
    bad("dbapi connect + SELECT 1", e)

# -- 2. data query + description --------------------------------------------
try:
    cur = conn.cursor()
    cur.execute('SELECT trip_id, city FROM demo.trips ORDER BY trip_id LIMIT 5')
    rows = cur.fetchall()
    cols = [d[0] for d in cur.description]
    assert cols == ["trip_id", "city"], f"description columns {cols!r}"
    assert len(rows) == 5, f"expected 5 rows, got {len(rows)}"
    ok("dbapi demo.trips query", f"5 rows, description={cols}")
except Exception as e:
    bad("dbapi demo.trips query", e)

# -- 3..8 SQLAlchemy: exactly what Superset does -----------------------------
engine = None
try:
    engine = sa.create_engine(
        f"datafusion+flightsql://{HOST}:{PORT}?insecure=True")
    with engine.connect() as c:
        val = c.execute(sa.text("SELECT 1")).scalar()
    assert int(val) == 1
    ok("sqlalchemy engine connect (datafusion+flightsql)")
except Exception as e:
    bad("sqlalchemy engine connect (datafusion+flightsql)", e)

try:
    insp = sa.inspect(engine)
    schemas = insp.get_schema_names()
    assert "demo" in schemas, f"'demo' not in {schemas!r}"
    ok("reflection get_schema_names", f"{len(schemas)} schemas, demo present")
except Exception as e:
    bad("reflection get_schema_names", e)

try:
    tables = insp.get_table_names(schema="demo")
    assert "trips" in tables, f"'trips' not in {tables!r}"
    ok("reflection get_table_names(demo)", f"{len(tables)} tables, trips present")
except Exception as e:
    bad("reflection get_table_names(demo)", e)

try:
    columns = insp.get_columns("trips", schema="demo")
    names = [c["name"] for c in columns]
    assert "trip_id" in names and "city" in names, f"columns {names!r}"
    ok("reflection get_columns(demo.trips)", f"columns={names}")
except Exception as e:
    bad("reflection get_columns(demo.trips)", e)

try:
    with engine.connect() as c:
        agg = c.execute(sa.text(
            "SELECT city, count(*) AS trips FROM demo.trips "
            "GROUP BY city ORDER BY trips DESC LIMIT 3")).fetchall()
    assert 1 <= len(agg) <= 3 and agg[0][1] >= agg[-1][1]
    ok("SQL Lab-shaped aggregate", f"top city={agg[0][0]} trips={agg[0][1]}")
except Exception as e:
    bad("SQL Lab-shaped aggregate", e)

try:
    with engine.connect() as c:
        prev = c.execute(sa.text('SELECT * FROM demo.trips LIMIT 10')).fetchall()
    assert len(prev) == 10
    ok("preview SELECT * LIMIT 10", f"{len(prev)} rows x {len(prev[0])} cols")
except Exception as e:
    bad("preview SELECT * LIMIT 10", e)

# -- 9. auth against a secured listener (optional) ---------------------------
SEC_PORT = os.environ.get("ICEGRES_PROBE_FLIGHT_SECURE_PORT")
SEC_USER = os.environ.get("ICEGRES_PROBE_FLIGHT_SECURE_USER")
SEC_PASS = os.environ.get("ICEGRES_PROBE_FLIGHT_SECURE_PASSWORD")
if SEC_PORT and SEC_USER and SEC_PASS:
    def _sec_query(**kw):
        c = fsql_connect(FlightSQLClient(host=HOST, port=int(SEC_PORT),
                                         insecure=True, **kw))
        try:
            cur = c.cursor()
            cur.execute("SELECT 1")
            return cur.fetchall()
        finally:
            c.close()

    try:
        _sec_query()
        bad("secured: no credentials rejected", "query unexpectedly succeeded")
    except Exception:
        ok("secured: no credentials rejected")
    try:
        _sec_query(user=SEC_USER, password="definitely-wrong")
        bad("secured: wrong password rejected", "query unexpectedly succeeded")
    except Exception:
        ok("secured: wrong password rejected")
    try:
        rows = _sec_query(user=SEC_USER, password=SEC_PASS)
        assert int(rows[0][0]) == 1
        ok("secured: correct credentials query")
    except Exception as e:
        bad("secured: correct credentials query", e)
else:
    skip("secured-listener auth variants",
         "ICEGRES_PROBE_FLIGHT_SECURE_{PORT,USER,PASSWORD} unset")

if conn is not None:
    try:
        conn.close()
    except Exception:
        pass

print(f"A13 RESULT: pass={len(passes)} fail={len(fails)} "
      f"xfail={len(xfails)} skip={len(skips)}")
sys.exit(0 if not fails else 2)
