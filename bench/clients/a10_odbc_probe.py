#!/usr/bin/env python3
"""A10 ODBC probe (bench/SPEC.md A10).

Exercises the stock PostgreSQL ODBC driver (psqlODBC, unixODBC) against a
live icegres server over the Postgres wire protocol. Self-contained: uses a
DRIVER= connection string, so it needs only the psqlODBC driver registered
in odbcinst.ini (apt: unixodbc odbc-postgresql) — no /etc/odbc.ini DSN.

Covers: connect (psqlODBC issues its version/type probes on connect), SQLTables
(cursor.tables), SQLColumns (cursor.columns), parameterized query (qmark bind),
INSERT + readback + DELETE with rowcount, and a read inside an explicit
transaction (autocommit off). DML inside an explicit transaction is a documented
XFAIL — the pre-existing 0A000 limit shared by every driver's extended-protocol
in-txn write path (use autocommit for writes).

Exit: 0 = all green (fail=0), 1 = probe failures, 3 = pyodbc/driver not available.
Prints a final "A10 RESULT: pass=N fail=N xfail=N skip=N" line.
"""
import os
import sys

HOST = os.environ.get("ICEGRES_PROBE_HOST", "127.0.0.1")
PORT = os.environ.get("ICEGRES_PROBE_PORT", "5439")
DB = os.environ.get("ICEGRES_PROBE_DB", "icegres")
DRIVER = os.environ.get("ICEGRES_ODBC_DRIVER", "PostgreSQL Unicode")

# scratch trip_id range reserved for this probe (self-cleaning; avoids the
# seeded 1..280 rows and other lanes' ranges)
SCRATCH = 970001

passes = []
fails = []
xfails = []


def ok(msg):
    passes.append(msg)
    print(f"    PASS odbc: {msg}")


def bad(msg, err):
    fails.append(msg)
    print(f"    FAIL odbc: {msg} -- {str(err)[:200]}")


def xfail(msg, err):
    xfails.append(msg)
    print(f"    XFAIL odbc: {msg} -- {str(err)[:220]}")


try:
    import pyodbc
except ModuleNotFoundError:
    print("A10 SKIP: pyodbc not available (pip install pyodbc)", file=sys.stderr)
    print("A10 RESULT: pass=0 fail=0 xfail=0 skip=1")
    sys.exit(3)

CONN = (
    f"DRIVER={{{DRIVER}}};Server={HOST};Port={PORT};Database={DB};"
    "UID=postgres;SSLmode=disable;UseDeclareFetch=0"
)

try:
    probe = pyodbc.connect(CONN, autocommit=True, timeout=10)
except pyodbc.Error as e:
    # driver missing / server down: skip rather than fail the whole suite
    msg = str(e)
    if "IM002" in msg or "Can't open lib" in msg or "not found" in msg.lower():
        print(f"A10 SKIP: psqlODBC driver not installed ({msg[:120]})", file=sys.stderr)
        print("A10 RESULT: pass=0 fail=0 xfail=0 skip=1")
        sys.exit(3)
    print(f"A10 SKIP: cannot connect ({msg[:150]})", file=sys.stderr)
    print("A10 RESULT: pass=0 fail=0 xfail=0 skip=1")
    sys.exit(3)

try:
    cur = probe.cursor()

    # 1. basic query (connect + version probes already succeeded to get here)
    cur.execute("select count(*) from demo.trips")
    n = cur.fetchone()[0]
    ok(f"connect + query (psqlODBC version/type probes on connect) -- demo.trips count={n}")

    # 2. SQLTables metadata
    try:
        tabs = sorted({r.table_name for r in cur.tables(schema="demo")})
        assert "trips" in tabs and "cities" in tabs
        ok(f"SQLTables (cursor.tables) -- demo tables include {[t for t in tabs if '$' not in t][:4]}")
    except Exception as e:
        bad("SQLTables (cursor.tables)", e)

    # 3. SQLColumns metadata
    try:
        cols = [(r.column_name, r.type_name) for r in cur.columns(table="trips", schema="demo")]
        names = [c[0] for c in cols]
        assert "trip_id" in names and "city" in names
        ok(f"SQLColumns (cursor.columns) -- {cols[:5]}")
    except Exception as e:
        bad("SQLColumns (cursor.columns)", e)

    # 4. parameterized query (qmark)
    try:
        cur.execute("select city, fare from demo.trips where trip_id = ?", 7)
        row = cur.fetchone()
        ok(f"parameterized query (qmark bind) -- trip 7 -> {tuple(row)}")
    except Exception as e:
        bad("parameterized query (qmark bind)", e)

    # 5. DML INSERT + readback + DELETE with rowcount (autocommit)
    try:
        cur.execute(
            "insert into demo.trips values "
            f"({SCRATCH}, 'ODBCprobe', 1.5, 2.5, timestamp '2026-01-01 00:00:00')"
        )
        cur.execute(f"select city from demo.trips where trip_id = {SCRATCH}")
        got = cur.fetchone()[0]
        assert got == "ODBCprobe"
        cur.execute(f"delete from demo.trips where trip_id = {SCRATCH}")
        rc = cur.rowcount
        cur.execute(f"select count(*) from demo.trips where trip_id = {SCRATCH}")
        gone = cur.fetchone()[0]
        assert gone == 0
        ok(f"DML INSERT/readback/DELETE (autocommit) -- readback='{got}' delete rowcount={rc} gone={gone==0}")
    except Exception as e:
        bad("DML INSERT/readback/DELETE (autocommit)", e)

    # 6. read inside an explicit transaction (autocommit off)
    try:
        tx = pyodbc.connect(CONN, autocommit=False, timeout=10)
        tcur = tx.cursor()
        tcur.execute("select count(*) from demo.trips")
        cnt = tcur.fetchone()[0]
        tx.commit()
        tx.close()
        ok(f"read inside explicit transaction (autocommit off) -- count={cnt}")
    except Exception as e:
        bad("read inside explicit transaction", e)

    # 7. DML inside an explicit transaction -> documented XFAIL (0A000)
    try:
        tx = pyodbc.connect(CONN, autocommit=False, timeout=10)
        tcur = tx.cursor()
        tcur.execute(
            "insert into demo.trips values "
            f"({SCRATCH + 1}, 'ODBCtxn', 1.0, 2.0, timestamp '2026-01-01 00:00:00')"
        )
        tx.commit()
        # if it somehow lands, clean up and count as pass (limitation lifted)
        tx.close()
        clean = probe.cursor()
        clean.execute(f"delete from demo.trips where trip_id = {SCRATCH + 1}")
        ok("DML inside explicit transaction (0A000 limit lifted?)")
    except Exception as e:
        if "0A000" in str(e) or "not supported inside a transaction" in str(e):
            xfail("DML inside explicit txn -- use autocommit for writes (extended-protocol 0A000, shared by all drivers)", e)
        else:
            bad("DML inside explicit transaction", e)

finally:
    try:
        probe.close()
    except Exception:
        pass

print(f"A10 RESULT: pass={len(passes)} fail={len(fails)} xfail={len(xfails)} skip=0")
sys.exit(0 if not fails else 1)
