#!/usr/bin/env python3
"""Fail closed on incomplete measurements and median/tail regressions."""

import argparse
import json
import math
from pathlib import Path


LATENCIES = (
    "connect_ms", "point_lookup_ms", "filtered_scan_ms", "aggregate_ms",
    "join_ms", "insert_single_ms", "insert_batch100_ms", "freshness_ms",
    "cold_start_ms",
)
RESOURCES = {"rss_peak_mb": 1.25, "rss_idle_mb": 1.25, "binary_size_mb": 1.1}


def number(value, label, *, positive=False):
    if isinstance(value, bool) or not isinstance(value, (float, int)):
        raise ValueError(f"{label}: expected a number")
    if not math.isfinite(value) or value < 0 or (positive and value == 0):
        raise ValueError(f"{label}: expected a finite {'positive' if positive else 'nonnegative'} number")
    return value


def distribution(metric, label, minimum=20, tails=("p50", "p95")):
    n = metric.get("n")
    if isinstance(n, bool) or not isinstance(n, int) or n < minimum:
        raise ValueError(f"{label}: at least {minimum} measured samples required")
    values = [number(metric.get(p), f"{label}.{p}") for p in tails]
    if values != sorted(values):
        raise ValueError(f"{label}: percentiles are not ordered")
    return dict(zip(tails, values))


def compare(baseline, candidate, *, mixed=False):
    failures = []
    if mixed:
        if baseline.get("schema_version") != 1 or candidate.get("schema_version") != 1:
            raise ValueError("unsupported mixed workload result schema")
        for label, result in (("baseline", baseline), ("candidate", candidate)):
            for field, required in (
                ("workload", {"rows", "files", "transactions", "memory_mb", "readers", "durability", "freshness", "fixture_version"}),
                ("environment", {"platform", "cpus", "catalog_uri", "warehouse", "s3_endpoint", "resource_scope"}),
            ):
                if not isinstance(result.get(field), dict) or not required.issubset(result[field]):
                    raise ValueError(f"{label}: incomplete {field} descriptor")
        if baseline.get("workload") != candidate.get("workload") or baseline.get("environment") != candidate.get("environment"):
            raise ValueError("workload configuration differs; results are not comparable")
        names = ("transaction_ms", "analytics_ms", "point_read_ms", "freshness_ms")
        minimum, tails = 100, ("p50", "p95", "p99")
        for label, result in (("baseline", baseline), ("candidate", candidate)):
            if result.get("complete") is not True:
                raise ValueError(f"{label}: incomplete workload")
            errors = number(result.get("errors"), f"{label}.errors")
            if errors != 0:
                failures.append(f"{label}: {errors} operation errors")
            if result.get("correctness") is not True:
                failures.append(f"{label}: final data verification failed")
    else:
        names, minimum, tails = LATENCIES, 20, ("p50", "p95")
    for name in names:
        b = distribution(baseline["metrics"][name], f"baseline.{name}", minimum, tails)
        c = distribution(candidate["metrics"][name], f"candidate.{name}", minimum, tails)
        for percentile in tails:
            if c[percentile] > b[percentile] * 1.2:
                failures.append(f"{name}.{percentile}: {b[percentile]} -> {c[percentile]} exceeds +20%")
    throughputs = ("operations_per_second", "transactions_per_second") if mixed else ("qps_8conn",)
    for throughput in throughputs:
        b = number(baseline["metrics"][throughput]["value"], f"baseline.{throughput}", positive=True)
        c = number(candidate["metrics"][throughput]["value"], f"candidate.{throughput}", positive=True)
        if c < b * .9:
            failures.append(f"{throughput}: {b} -> {c} exceeds -10%")
    resources = {"rss_peak_mb": 1.25} if mixed else RESOURCES
    for name, factor in resources.items():
        b = number(baseline["metrics"][name]["value"], f"baseline.{name}", positive=True)
        c = number(candidate["metrics"][name]["value"], f"candidate.{name}", positive=True)
        if c > b * factor:
            failures.append(f"{name}: {b} -> {c} exceeds +{round((factor - 1) * 100)}%")
    return failures


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("--mixed", action="store_true")
    args = parser.parse_args()
    try:
        failures = compare(json.loads(args.baseline.read_text()), json.loads(args.candidate.read_text()), mixed=args.mixed)
    except (ValueError, KeyError, TypeError, OSError) as error:
        print(f"GATE: INVALID: {error}")
        return 1
    for failure in failures:
        print(f"FAIL {failure}")
    print("METRICS: FAIL" if failures else "METRICS: PASS")
    return bool(failures)


if __name__ == "__main__":
    raise SystemExit(main())
