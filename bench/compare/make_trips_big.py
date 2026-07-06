#!/usr/bin/env python3
"""Create demo.trips_big — a ~5,000,000-row Iceberg table with the same
schema as demo.trips — directly against the Lakekeeper REST catalog via
pyiceberg + pyarrow (no query engine involved).

Layout: NUM_BATCHES pyiceberg appends of BATCH_ROWS rows each, so the table
lands as NUM_BATCHES well-sized parquet files (one data file per append).

Deterministic: numpy PCG64 seeded with SEED; rerunning after a drop
reproduces byte-identical logical content (trip_id 1..N, same cities,
distances, fares, timestamps).

Idempotent: if demo.trips_big already exists with the expected row count it
is left alone; with a wrong count it is dropped (purged) and rebuilt.

Usage: python3 make_trips_big.py [--rows N] [--batches B] [--force]
"""

import argparse
import sys
import time

import numpy as np
import pyarrow as pa
from pyiceberg.catalog import load_catalog

SEED = 20260705
DEFAULT_ROWS = 5_000_000
DEFAULT_BATCHES = 10  # -> 10 parquet data files

# Must match demo.cities (join key for Q4-style queries).
CITIES = [
    "Paris", "Lyon", "Berlin", "Munich", "Madrid", "Barcelona", "Rome",
    "Milan", "London", "Manchester", "Amsterdam", "Brussels", "Vienna",
    "Zurich", "Lisbon", "Dublin", "Copenhagen", "Stockholm", "Oslo",
    "Warsaw",
]

TS_START_US = int(time.mktime((2025, 1, 1, 0, 0, 0, 0, 0, 0))) * 1_000_000
TS_RANGE_US = 550 * 24 * 3600 * 1_000_000  # ~18 months

ARROW_SCHEMA = pa.schema(
    [
        pa.field("trip_id", pa.int64()),
        pa.field("city", pa.string()),
        pa.field("distance_km", pa.float64()),
        pa.field("fare", pa.float64()),
        pa.field("ts", pa.timestamp("us")),
    ]
)


def catalog():
    return load_catalog(
        "lake",
        **{
            "type": "rest",
            "uri": "http://127.0.0.1:8181/catalog",
            "warehouse": "lakehouse",
            "s3.endpoint": "http://127.0.0.1:9000",
            "s3.access-key-id": "rustfsadmin",
            "s3.secret-access-key": "rustfssecret",
            "s3.region": "us-east-1",
            "s3.path-style-access": "true",
        },
    )


def make_batch(rng: np.random.Generator, start_id: int, n: int) -> pa.Table:
    trip_id = np.arange(start_id, start_id + n, dtype=np.int64)
    # Zipf-ish skew over the 20 cities so GROUP BY has uneven groups.
    weights = 1.0 / np.arange(1, len(CITIES) + 1)
    weights /= weights.sum()
    city_idx = rng.choice(len(CITIES), size=n, p=weights)
    city = pa.array(np.array(CITIES, dtype=object)[city_idx], type=pa.string())
    distance = np.round(rng.lognormal(mean=1.6, sigma=0.9, size=n), 2)
    distance = np.clip(distance, 0.1, 400.0)
    fare = np.round(2.5 + distance * 1.75 + rng.normal(0, 1.5, size=n), 2)
    fare = np.clip(fare, 2.5, None)
    ts_us = TS_START_US + rng.integers(0, TS_RANGE_US, size=n, dtype=np.int64)
    return pa.Table.from_arrays(
        [
            pa.array(trip_id),
            city,
            pa.array(distance),
            pa.array(fare),
            pa.array(ts_us, type=pa.timestamp("us")),
        ],
        schema=ARROW_SCHEMA,
    )


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=DEFAULT_ROWS)
    ap.add_argument("--batches", type=int, default=DEFAULT_BATCHES)
    ap.add_argument("--force", action="store_true", help="drop and rebuild")
    args = ap.parse_args()

    cat = catalog()
    ident = ("demo", "trips_big")

    if cat.table_exists(ident):
        tbl = cat.load_table(ident)
        have = sum(
            f.file.record_count for f in tbl.scan().plan_files()
        )
        if have == args.rows and not args.force:
            print(f"demo.trips_big already has {have} rows — nothing to do")
            return 0
        print(f"dropping existing demo.trips_big (rows={have})")
        cat.drop_table(ident, purge_requested=True)

    trips_schema = cat.load_table(("demo", "trips")).schema()
    tbl = cat.create_table(ident, schema=trips_schema)

    rng = np.random.default_rng(SEED)
    per = args.rows // args.batches
    written = 0
    t0 = time.perf_counter()
    for b in range(args.batches):
        n = per if b < args.batches - 1 else args.rows - written
        batch = make_batch(rng, written + 1, n)
        tbl.append(batch)
        written += n
        print(f"batch {b + 1}/{args.batches}: appended {n} rows "
              f"(total {written}) [{time.perf_counter() - t0:.1f}s]")

    tbl = cat.load_table(ident)
    files = list(tbl.scan().plan_files())
    total_bytes = sum(f.file.file_size_in_bytes for f in files)
    total_rows = sum(f.file.record_count for f in files)
    print(f"done: {total_rows} rows in {len(files)} parquet files, "
          f"{total_bytes / 1e6:.1f} MB")
    return 0 if total_rows == args.rows else 1


if __name__ == "__main__":
    sys.exit(main())
