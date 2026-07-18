#!/usr/bin/env python3
"""Seed demo.dash_trips for the frontend data-path bench.

1M synthetic taxi-ish rows ingested in one Iceberg commit through the Flight
SQL bulk lane (adbc_ingest), per docs/clients.md — COPY FROM STDIN is not
supported on the pgwire side.
"""

import numpy as np
import pyarrow as pa
import adbc_driver_flightsql.dbapi as flight

N = 1_000_000
CITIES = [
    "paris", "lyon", "marseille", "toulouse", "nice",
    "nantes", "lille", "bordeaux", "rennes", "grenoble",
]

rng = np.random.default_rng(42)
tbl = pa.table(
    {
        "trip_id": pa.array(np.arange(N, dtype=np.int64)),
        "city": pa.array(np.array(CITIES)[rng.integers(0, len(CITIES), N)]),
        "fare": pa.array(np.round(rng.gamma(3.0, 7.5, N), 2)),
        "distance_km": pa.array(np.round(rng.gamma(2.0, 3.0, N), 2)),
        "ts": pa.array(
            (np.datetime64("2026-06-01T00:00:00") + rng.integers(0, 30 * 86400, N).astype("timedelta64[s]")).astype("datetime64[us]")
        ),
    }
)

conn = flight.connect("grpc://127.0.0.1:50051")
cur = conn.cursor()
cur.adbc_ingest("dash_trips", tbl, mode="create_append", db_schema_name="demo")
conn.commit()
cur.execute("SELECT count(*) FROM demo.dash_trips")
print("dash_trips rows:", cur.fetchone()[0])
