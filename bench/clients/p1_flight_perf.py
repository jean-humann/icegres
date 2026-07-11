#!/usr/bin/env python3
"""P1 Flight small-query latency measurement (roadmap-v2 P1 / bench caveat 4).

Replicates bench/compare/compare.py's flightsql q1 recipe exactly, so the
number is apples-to-apples with the historical ~48 ms flat p50:

  1. ONE adbc_driver_flightsql dbapi connection, reused for all samples.
  2. Query q1: SELECT trip_id, city, distance_km, fare, ts
     FROM demo.trips WHERE trip_id = 137  (1 row of the seeded fixture).
  3. Per sample: fresh cursor, perf_counter around execute() + fetchall(),
     cursor closed.  WARMUP=3 discarded, ITERS=15 timed.
  4. p50 = statistics.median; p95 = sorted[min(n-1, round(0.95*n)-1)].

Run it against whichever server you want to measure (old binary for the
"before", new binary default mode, new binary --freshness-ms for the
stretch target).  Environment:

  ICEGRES_PROBE_FLIGHT_HOST / ICEGRES_PROBE_FLIGHT_PORT   (127.0.0.1:50051)
  ICEGRES_P1_WARMUP / ICEGRES_P1_ITERS                    (3 / 15)
  ICEGRES_P1_ASSERT_MS   assert p50 <= this bound; empty/unset = report only
                         (the P1 scope target is 15, stretch 10 with
                         --freshness-ms)

Read-only: no writes, no cleanup needed.  Exit 0 on pass/report, 2 on a
failed assertion or error.  Final line:
  P1PERF RESULT: p50=<ms> p95=<ms> n=<n> target=<ms|none> status=<PASS|FAIL|REPORT>
"""

import os
import statistics
import sys
import time
import warnings

warnings.filterwarnings("ignore")

HOST = os.environ.get("ICEGRES_PROBE_FLIGHT_HOST", "127.0.0.1")
PORT = int(os.environ.get("ICEGRES_PROBE_FLIGHT_PORT", "50051"))
WARMUP = int(os.environ.get("ICEGRES_P1_WARMUP", "3"))
ITERS = int(os.environ.get("ICEGRES_P1_ITERS", "15"))
ASSERT_MS = os.environ.get("ICEGRES_P1_ASSERT_MS", "").strip()

Q1 = "SELECT trip_id, city, distance_km, fare, ts FROM demo.trips WHERE trip_id = 137"


def pcts(samples):
    xs = sorted(samples)
    n = len(xs)
    p50 = statistics.median(xs)
    p95 = xs[min(n - 1, int(round(0.95 * n)) - 1)]
    return round(p50, 2), round(p95, 2)


def main():
    try:
        import adbc_driver_flightsql.dbapi as flight_sql
    except ImportError as e:
        print(f"SKIP p1-flight-perf -- adbc_driver_flightsql not installed: {e}")
        print("P1PERF RESULT: p50=nan p95=nan n=0 target=none status=SKIP")
        return 0

    uri = f"grpc://{HOST}:{PORT}"
    try:
        conn = flight_sql.connect(uri)
    except Exception as e:
        print(f"FAIL p1-flight-perf -- cannot connect to {uri}: {e}")
        print("P1PERF RESULT: p50=nan p95=nan n=0 target=none status=FAIL")
        return 2

    def sample():
        cur = conn.cursor()
        t0 = time.perf_counter()
        cur.execute(Q1)
        rows = cur.fetchall()
        dt = (time.perf_counter() - t0) * 1000.0
        cur.close()
        return dt, rows

    # Correctness first: q1 must return exactly the fixture row.
    _, rows = sample()
    if len(rows) != 1 or rows[0][0] != 137:
        print(f"FAIL p1-flight-perf -- q1 returned {len(rows)} rows (want 1, trip_id=137)")
        print("P1PERF RESULT: p50=nan p95=nan n=0 target=none status=FAIL")
        conn.close()
        return 2
    print(f"PASS q1 correctness -- 1 row, trip_id=137, city={rows[0][1]!r}")

    for _ in range(WARMUP):
        sample()
    samples = [sample()[0] for _ in range(ITERS)]
    conn.close()

    p50, p95 = pcts(samples)
    print(f"PASS q1 timing -- {ITERS} iters against {uri}: p50={p50}ms p95={p95}ms")

    if ASSERT_MS:
        bound = float(ASSERT_MS)
        status = "PASS" if p50 <= bound else "FAIL"
        verdict = "meets" if status == "PASS" else "MISSES"
        print(f"{status} q1 p50 bound -- p50={p50}ms {verdict} the {bound}ms target")
        print(
            f"P1PERF RESULT: p50={p50} p95={p95} n={ITERS} target={bound} status={status}"
        )
        return 0 if status == "PASS" else 2
    print(f"P1PERF RESULT: p50={p50} p95={p95} n={ITERS} target=none status=REPORT")
    return 0


if __name__ == "__main__":
    sys.exit(main())
