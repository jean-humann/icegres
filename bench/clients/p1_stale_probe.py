#!/usr/bin/env python3
"""PF1 stale-ticket probe: a Flight ticket minted BEFORE a write must serve
FRESH results at DoGet.

The regression shape this guards (adversarial review PF1): GetFlightInfo
plans the query once and hands DoGet a ticket; a broken server pins the
pre-write physical plan under that ticket, so a row written between the two
RPCs silently vanishes from the DoGet result (count=0 where a re-plan says
count=1).  The fixed server re-validates every planned table's version at
DoGet and re-plans on any mismatch — and never pins a plan at all for
tables it cannot re-validate (default mode, overlay-bearing tables).

Steps (raw pyarrow.flight so the GetFlightInfo/DoGet pair is controlled):

  1. baseline: mint a ticket for  SELECT count(*) ... WHERE trip_id = K,
     DoGet it -> 0 rows counted (K is this probe's own sentinel).
  2. the repro: mint a NEW ticket for the same query, THEN insert the
     sentinel row over pgwire, THEN DoGet the pre-write ticket.
     Assert count == 1 (FRESH), the PF1 fix.
  3. sanity: a fresh GetFlightInfo -> DoGet pair also counts 1.

Environment:
  ICEGRES_PROBE_FLIGHT_HOST / ICEGRES_PROBE_FLIGHT_PORT  (127.0.0.1:50051)
  ICEGRES_PROBE_PG_HOST / ICEGRES_PROBE_PG_PORT          (127.0.0.1:5432)
  ICEGRES_PROBE_TABLE                                    (demo.e2e_p1)
  ICEGRES_PROBE_TRIP_ID                                  (980600)

Owns trip_id = ICEGRES_PROBE_TRIP_ID (default 980600, inside the probes'
reserved >= 980000 range) and deletes it on exit.  Exit 0 on pass, 2 on
failure.  Final line:  P1STALE RESULT: pass=<n> fail=<n> status=<PASS|FAIL>
"""

import os
import sys
import warnings

warnings.filterwarnings("ignore")

FLIGHT_HOST = os.environ.get("ICEGRES_PROBE_FLIGHT_HOST", "127.0.0.1")
FLIGHT_PORT = int(os.environ.get("ICEGRES_PROBE_FLIGHT_PORT", "50051"))
PG_HOST = os.environ.get("ICEGRES_PROBE_PG_HOST", "127.0.0.1")
PG_PORT = int(os.environ.get("ICEGRES_PROBE_PG_PORT", "5432"))
TABLE = os.environ.get("ICEGRES_PROBE_TABLE", "demo.e2e_p1")
TRIP_ID = int(os.environ.get("ICEGRES_PROBE_TRIP_ID", "980600"))

PASS = 0
FAIL = 0


def check(ok, what):
    global PASS, FAIL
    if ok:
        PASS += 1
        print(f"PASS {what}")
    else:
        FAIL += 1
        print(f"FAIL {what}")


def _ld(tag: int, payload: bytes) -> bytes:
    """One length-delimited protobuf field."""
    out = bytes([tag])
    n = len(payload)
    while True:
        b = n & 0x7F
        n >>= 7
        out += bytes([b | (0x80 if n else 0)])
        if not n:
            return out + payload


def statement_descriptor(sql: str):
    """FlightDescriptor for a Flight SQL CommandStatementQuery, hand-rolled:
    Any{type_url, value=CommandStatementQuery{query=sql}}."""
    from pyarrow import flight

    cmd = _ld(0x0A, sql.encode())  # CommandStatementQuery.query (field 1)
    any_msg = _ld(
        0x0A, b"type.googleapis.com/arrow.flight.protocol.sql.CommandStatementQuery"
    ) + _ld(0x12, cmd)
    return flight.FlightDescriptor.for_command(any_msg)


def main():
    try:
        import psycopg2
        from pyarrow import flight
    except ImportError as e:
        print(f"SKIP p1-stale-probe -- client libraries missing: {e}")
        print("P1STALE RESULT: pass=0 fail=0 status=SKIP")
        return 0

    sql = f"SELECT count(*) FROM {TABLE} WHERE trip_id = {TRIP_ID}"
    client = flight.FlightClient(f"grpc://{FLIGHT_HOST}:{FLIGHT_PORT}")
    pg = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, user="postgres", dbname="icegres"
    )
    pg.autocommit = True

    def mint_ticket():
        info = client.get_flight_info(statement_descriptor(sql))
        return info.endpoints[0].ticket

    def count_via(ticket) -> int:
        table = client.do_get(ticket).read_all()
        return table.column(0)[0].as_py()

    def pg_exec(stmt):
        with pg.cursor() as cur:
            cur.execute(stmt)

    try:
        pg_exec(f"delete from {TABLE} where trip_id = {TRIP_ID}")

        # 1. Baseline: the sentinel row does not exist yet.
        check(count_via(mint_ticket()) == 0, "baseline -- sentinel row absent (count=0)")

        # 2. The PF1 repro: ticket minted BEFORE the write must be FRESH at
        #    DoGet (a stale pinned plan would still answer 0).
        pre_write_ticket = mint_ticket()
        pg_exec(
            f"insert into {TABLE} (trip_id, city, distance_km, fare, ts) values "
            f"({TRIP_ID}, 'stale-probe', 1.0, 2.0, TIMESTAMP '2026-07-11 00:00:02')"
        )
        got = count_via(pre_write_ticket)
        check(
            got == 1,
            f"pre-write ticket serves FRESH results at DoGet (count={got}, want 1)",
        )

        # 3. Sanity: a fresh GetFlightInfo -> DoGet pair agrees.
        check(count_via(mint_ticket()) == 1, "fresh ticket also counts the row")
    finally:
        try:
            pg_exec(f"delete from {TABLE} where trip_id = {TRIP_ID}")
        finally:
            pg.close()
            client.close()

    status = "PASS" if FAIL == 0 else "FAIL"
    print(f"P1STALE RESULT: pass={PASS} fail={FAIL} status={status}")
    return 0 if FAIL == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
