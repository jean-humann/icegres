#!/usr/bin/env python3
"""Verify the Flight SQL endpoint returns the same results as icegres
for count / filter / aggregate queries over demo.trips.

Usage: python3 flightsql_verify.py
Requires: icegres flight-serve on grpc://127.0.0.1:50051 (flightsql-start.sh)
          icegres serve on 127.0.0.1:5439
Exits 0 and prints PASS lines if all results match; exits 1 on mismatch.
"""

import sys

import adbc_driver_flightsql.dbapi as flightsql
import psycopg2

FLIGHT_URI = "grpc://127.0.0.1:50051"
PG_DSN = "host=127.0.0.1 port=5439 dbname=icegres user=icegres"

# Same shapes the bench harness uses (bench/harness/src/main.rs).
QUERIES = [
    ("count", "select count(*) from demo.trips"),
    (
        "filter",
        "select count(*) from demo.trips "
        "where city = 'Paris' and distance_km > 20",
    ),
    (
        "aggregate",
        "select city, count(*) as trips from demo.trips "
        "group by city order by trips desc, city asc limit 5",
    ),
]


def normalize(rows):
    return [tuple(str(v) for v in row) for row in rows]


def main() -> int:
    fconn = flightsql.connect(FLIGHT_URI)
    pconn = psycopg2.connect(PG_DSN)
    failed = False
    try:
        for name, sql in QUERIES:
            with fconn.cursor() as fcur:
                fcur.execute(sql)
                frows = normalize(fcur.fetchall())
            with pconn.cursor() as pcur:
                pcur.execute(sql)
                prows = normalize(pcur.fetchall())
            if frows == prows:
                print(f"PASS {name}: {frows}")
            else:
                failed = True
                print(f"FAIL {name}: flightsql={frows} icegres={prows}")
    finally:
        fconn.close()
        pconn.close()
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
