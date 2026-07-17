#!/usr/bin/env python3
"""icegres single-node SCALE curve — one engine (icegres/pgwire), the same
four query classes measured against a `demo.<table>` scale table at several
row counts, so we can publish WHERE the interactive-serving advantage holds
as data grows and WHERE the full-scan gap opens.

This is the icegres-side extension of `bench/compare` (which measures four
engines at one 5M size). The cross-engine Trino/Spark/FlightSQL columns are
NOT re-run here (they are cited from bench/COMPARISON.md at 5M); this driver
extends only the icegres scale curve to larger N. Two of the four queries
(`filtered_count`, `full_agg`) are BYTE-IDENTICAL to COMPARISON's q6/q5, so
the 5M point of this curve cross-checks the published cross-engine table.

Query classes (all against demo.<table>, whose trip_id is sorted 1..N):
  point_lookup     WHERE trip_id = N/2            -> sort-key row-group skip
  filtered_count   COUNT(*) WHERE city+distance   -> = COMPARISON q6 (full scan)
  selective_join   trip_id slice JOIN demo.cities -> selective probe, 20-row build
  full_agg         GROUP BY city over ALL N       -> = COMPARISON q5 (full scan)

Timing method is identical to compare.py: perf_counter around execute + a
full fetchall(), WARMUP discarded, then timed iters -> p50/p95/min/max. The
scan-bound classes may use fewer iters at large N (recorded as `n`).

Reuses compare.py's Icegres engine, time_query and pcts so the harness is
one implementation. Writes one JSON per size to --out.
"""

import argparse
import datetime
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from compare import Icegres, pcts, rss_mb, time_query  # noqa: E402


def scale_queries(table: str, rows: int) -> dict:
    """The four scale query classes against demo.<table>. Predicates are
    deterministic functions of the row count so the same logical query runs
    at every size (point/join key scale with N; filter/agg predicates are
    fixed and identical to COMPARISON q6/q5)."""
    b = f"demo.{table}"
    c = "demo.cities"
    mid = rows // 2  # trip_id exists 1..rows, sorted
    return {
        # interactive band: selective on the sorted trip_id -> row-group skip
        "point_lookup_ms": (
            f"SELECT trip_id, city, distance_km, fare, ts FROM {b} "
            f"WHERE trip_id = {mid}"
        ),
        # selective join: bounded trip_id probe against the 20-row build side
        "selective_join_ms": (
            f"SELECT c.country, count(*) AS n FROM {b} tr "
            f"JOIN {c} c ON tr.city = c.city "
            f"WHERE tr.trip_id BETWEEN {mid} AND {mid + 100} "
            f"GROUP BY c.country ORDER BY n DESC, c.country ASC"
        ),
        # scan-bound (= COMPARISON q6): count over the whole table
        "filtered_count_ms": (
            f"SELECT count(*) AS n FROM {b} "
            f"WHERE city = 'Berlin' AND distance_km > 30"
        ),
        # scan-bound (= COMPARISON q5): full GROUP BY city aggregation
        "full_agg_ms": (
            f"SELECT city, count(*) AS n, avg(distance_km) AS avg_km, "
            f"sum(fare) AS total_fare FROM {b} "
            f"GROUP BY city ORDER BY n DESC, city ASC"
        ),
    }


# Which classes are full-table scans (grow ~linearly with N) vs interactive
# (sort-key selective, near-flat). Drives the smaller iter budget at large N
# and the rows/sec interpretation in the SCORECARD.
SCAN_BOUND = {"filtered_count_ms", "full_agg_ms"}
INTERACTIVE = {"point_lookup_ms", "selective_join_ms"}


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--table", required=True, help="demo.<table> scale table")
    ap.add_argument("--rows", type=int, required=True)
    ap.add_argument("--size-label", required=True, help="e.g. 5M, 300M")
    ap.add_argument("--pid", type=int, help="icegres pid for peak RSS (VmHWM)")
    ap.add_argument("--warmup", type=int, default=3,
                    help="warmup iters for interactive queries")
    ap.add_argument("--iters", type=int, default=15,
                    help="timed iters for interactive queries")
    ap.add_argument("--scan-warmup", type=int, default=3,
                    help="warmup iters for scan-bound queries (fewer at large N)")
    ap.add_argument("--scan-iters", type=int, default=15,
                    help="timed iters for scan-bound queries (fewer at large N)")
    ap.add_argument("--out", required=True, help="output JSON path")
    args = ap.parse_args()

    engine = Icegres()
    qmap = scale_queries(args.table, args.rows)

    result = {
        "size_label": args.size_label,
        "rows": args.rows,
        "table": f"demo.{args.table}",
        "metrics": {},
        "meta": {},
    }

    conn = engine.connect()
    try:
        result["meta"]["engine_version"] = engine.version(conn)
    except Exception as exc:  # noqa: BLE001
        result["meta"]["engine_version"] = f"unknown ({exc})"

    # confirm the table really holds `rows` before we trust the curve
    try:
        got = engine.run(conn, f"SELECT count(*) FROM demo.{args.table}")[0][0]
        result["meta"]["counted_rows"] = int(got)
    except Exception as exc:  # noqa: BLE001
        result["meta"]["counted_rows"] = f"error ({exc})"

    for key in ("point_lookup_ms", "filtered_count_ms",
                "selective_join_ms", "full_agg_ms"):
        if key in SCAN_BOUND:
            warmup, iters = args.scan_warmup, args.scan_iters
        else:
            warmup, iters = args.warmup, args.iters
        stats, rows = time_query(engine, conn, qmap[key],
                                 warmup=warmup, iters=iters)
        p50_s = stats["p50"] / 1000.0
        stats["result_rows"] = len(rows)
        # eff_rows_per_s = table rows / p50 wall-second. For the scan-bound
        # classes this is genuine scan throughput (all N rows are read); for
        # the interactive classes it reflects sort-key skip (few rows are
        # actually touched), so it is reported but flagged latency-bound.
        stats["eff_rows_per_s"] = (
            round(args.rows / p50_s) if p50_s > 0 else None
        )
        stats["class"] = "scan_bound" if key in SCAN_BOUND else "interactive"
        result["metrics"][key] = stats
        print(f"{args.size_label} {key}: p50={stats['p50']}ms "
              f"p95={stats['p95']}ms rows={len(rows)} "
              f"eff_rows/s={stats['eff_rows_per_s']} n={stats['n']}",
              flush=True)
    conn.close()

    if args.pid:
        peak = rss_mb(args.pid)
        if peak is not None:
            result["metrics"]["rss_peak_mb"] = {"value": peak}
            print(f"{args.size_label} rss_peak_mb={peak}", flush=True)

    result["meta"]["timestamp"] = (
        datetime.datetime.now(datetime.timezone.utc).isoformat(timespec="seconds")
    )

    os.makedirs(os.path.dirname(os.path.abspath(args.out)), exist_ok=True)
    with open(args.out, "w") as f:
        json.dump(result, f, indent=2, sort_keys=True)
    print(f"wrote {args.out}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
