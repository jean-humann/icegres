# icegres vs Trino vs Spark Thrift vs Flight SQL — quiesced single-box comparison

Run: **2026-07-06** (`bench/results/compare-20260706-042733.{json,md}`).
One harness (`bench/compare/compare.py`), four connectors, identical SQL
(modulo catalog prefix), identical timing method, engines run strictly
one-at-a-time on a quiesced box (quiesce verified by process scan between
legs; base lakehouse stack — Postgres :5433, RustFS :9000, Lakekeeper :8181 —
running throughout for all engines equally).

## Contenders

| engine | version | wire protocol / client | tables |
|---|---|---|---|
| icegres | icegres 0.1.0 (DataFusion 52.5.0, iceberg-rust 0.9.1), reports "PostgreSQL 16.6" | pgwire :5439 / psycopg2 | `demo.*` |
| trino | Trino **446** single node (not latest — see caveat 6) | HTTP :8082 / trino 0.338.0 dbapi | `iceberg.demo.*` |
| spark | Spark 3.5.8 Thrift server + iceberg-spark-runtime 1.11.0 | HiveServer2 thrift :10000 / pyhive | `lake.demo.*` |
| flightsql | **same engine as icegres** (DataFusion 52.5.0 + iceberg-rust 0.9.1) behind Arrow Flight SQL — a transport experiment, not an independent engine (caveat 4) | gRPC :50051 / adbc-driver-flightsql 1.11.0 | `demo.*` |

All four read the **same** Iceberg tables through the same Lakekeeper REST
catalog (warehouse `lakehouse`) on the same RustFS S3 store. Result row
counts were identical across all engines for every query (1/3/20/15/20/1/101
rows for Q1–Q7).

## Datasets

| table | rows | files | bytes | notes |
|---|---|---|---|---|
| `demo.trips` | 280 | 1 | 12.5 KB | canonical tiny table (trip_id, city, distance_km, fare, ts) |
| `demo.cities` | 20 | 1 | 1.9 KB | join dimension (city, country, population) |
| `demo.trips_big` | 5,000,000 | 10 | 71.2 MB | same schema as trips; deterministic (seed 20260705), zipf-skewed cities, one 500k-row parquet file per append |

## Results (p50 / p95 ms unless noted)

| metric | icegres | trino | spark | flightsql |
|---|---|---|---|---|
| connect_ms | **1.14** / 18.59 | 91.8 / 238.81 | 110.4 / 219.86 | 48.94 / 443.87 |
| q1_point_lookup_ms | **7.24** / 7.67 | 138.05 / 188.16 | 157.99 / 291.91 | 48.0 / 48.4 |
| q2_filtered_scan_ms | **7.06** / 7.3 | 115.48 / 151.65 | 127.12 / 187.51 | 47.96 / 48.26 |
| q3_aggregate_ms | **7.51** / 7.83 | 184.59 / 198.64 | 259.9 / 427.19 | 48.04 / 51.97 |
| q4_join_ms | **10.16** / 10.61 | 210.86 / 342.01 | 436.16 / 539.56 | 48.13 / 51.8 |
| q5_big_scan_agg_ms (5M rows) | 404.22 / 427.04 | **335.87** / 359.5 | 732.66 / 787.79 | 390.29 / 413.45 |
| q6_big_filter_count_ms (5M rows) | **238.02** / 242.88 | 315.25 / 352.49 | 542.36 / 621.18 | 261.87 / 274.45 |
| q7_big_selective_ms (5M rows, 101-row slice) | **49.02** / 50.5 | 120.53 / 132.67 | 318.69 / 330.92 | 91.97 / 95.99 |
| qps_8conn (mixed reads, 10 s) | **51.7** | 12.2 | 7.9 | 38.2 |
| startup_ms (cold start → first successful client query) | 329 | 14,116 | 9,944 | 643 |
| rss_idle_mb (VmRSS at first query) | 63.2 | 824.4 | 478.1 | **47.9** |
| rss_peak_mb (VmHWM after full run incl. 8-way) | 225.9 | 2,162.3 | 1,802.6 | **164.9** |
| footprint | 123M (one static binary) | 443M (install dir, plugins pruned to iceberg) | 536M (install dir + 2 iceberg jars) | 110M (one static binary) |

Concurrency legs completed with **zero errors** on every engine
(icegres 531 queries / trino 128 / spark 84 / flightsql 390 in ~10 s).

### Harness stability — icegres run twice

The icegres leg was executed twice end-to-end (fresh process each time);
acceptance bar was Q1–Q7 p50 agreement within 25 %.

| query | run 1 p50 | run 2 p50 | diff |
|---|---|---|---|
| q1 | 7.24 | 6.68 | 8.4 % |
| q2 | 7.06 | 6.41 | 10.1 % |
| q3 | 7.51 | 7.82 | 4.1 % |
| q4 | 10.16 | 11.29 | 11.1 % |
| q5 | 404.22 | 345.32 | 17.1 % |
| q6 | 238.02 | 243.23 | 2.2 % |
| q7 | 49.02 | 52.41 | 6.9 % |

All within bounds (worst: q5 at 17.1 %). Run 2 also reproduced qps_8conn
(51.2 vs 51.7) and rss_idle (64.5 vs 63.2 MB). Run 1 (part of the merged
comparison) is canonical.

## Method

- Every latency sample = `time.perf_counter()` around `cursor.execute()`
  **plus a full `fetchall()`** (results fully materialized client-side).
  3 warmup runs discarded, 15 timed iterations → p50/p95.
- `connect_ms` = fresh connection + `SELECT 1` round trip (some drivers
  connect lazily), 10 samples.
- `qps_8conn` = 8 threads × own connection, cycling [q1,q2,q3,q4,q6,q7]
  for 10 s wall; qps = completed queries / elapsed.
- `startup_ms` = wall clock from start-script/binary invocation until the
  first successful client query over the engine's own wire protocol.
- `rss_idle_mb` = /proc VmRSS immediately after that first query;
  `rss_peak_mb` = /proc VmHWM after the engine's whole leg.
- Queries: Q1 point lookup by trip_id; Q2 city+distance filter; Q3 GROUP BY
  city (count+avg, ordered); Q4 trips⋈cities GROUP BY country; Q5 GROUP BY
  city over 5M rows (count/avg/sum); Q6 count over 5M with city+distance
  predicate; Q7 trip_id BETWEEN slice returning 101 of 5M rows.
- Orchestration: `bench/compare/run_all.sh` (quiesce check → start → bench →
  stop → verify-exited, per engine; merge at the end).

## Honest caveats

1. **Single small box.** 4 cores / 15 GB RAM, everything (client, engine,
   catalog, S3 store, Postgres) colocated. Distributed engines (Trino,
   Spark) are architecturally built for clusters; single-node numbers
   understate them at scale and overstate their per-query fixed costs.
2. **JVMs memory-capped.** Trino ran with -Xmx3g, Spark with a 2 GB driver
   heap on local[3]. rss/qps for the JVM engines reflect those caps, not
   tuned production configs.
3. **Small data.** 280-row and 5M-row/71 MB tables — the whole dataset fits
   in page cache. This benchmark measures per-query overhead, planning and
   transport cost far more than scan throughput. Trino already overtakes
   icegres on the largest scan (Q5); at 100× the data the JVM engines'
   parallelism would matter far more.
4. **FlightSQL is not an independent engine.** No OSS Flight SQL Iceberg
   server was installable here (Docker registry blocked), so the Flight SQL
   endpoint is the *same* DataFusion+iceberg-rust engine as icegres behind
   gRPC/Arrow instead of pgwire. It isolates the transport variable only.
   Its flat ~48 ms small-query latency is dominated by the two-round-trip
   Flight SQL flow (GetFlightInfo plans the query, DoGet re-plans and
   executes) plus ADBC/gRPC overhead; on big results (Q5) the Arrow-native
   stream nearly closes the gap with pgwire.
5. **One run for the JVM engines.** Time budget allowed a single leg for
   Trino/Spark/FlightSQL (15 iterations per query). Only icegres was run
   twice to validate harness stability. JVM startup_ms/rss numbers are
   single-shot; JIT means longer-running JVMs would improve latencies.
6. **Trino 446, not latest.** Trino ≥447 requires Java 22+; only Java 21 is
   available in this environment and JDK downloads were blocked, so the last
   Java-21-compatible release (446, mid-2024) is measured. Its plugin dir
   was pruned to iceberg-only (stock 446 also fails to boot with its pinot
   plugin present here).
7. **startup_ms asymmetry.** icegres/flightsql binaries are started
   directly; trino/spark go through their start scripts (which include
   their own readiness polling at 0.2–1 s granularity). All four are
   measured to the same finish line (first successful client query).
8. **Footprint units differ in kind.** icegres/flightsql are single
   stripped static binaries; trino/spark are install directories (JRE not
   included in either count).

## Raw artifacts

- Merged: `bench/results/compare-20260706-042733.json` + `.md`
- icegres single-engine baseline (small-table suite): `bench/results/baseline.json`
- Harness: `bench/compare/compare.py`, `bench/compare/run_all.sh`,
  dataset generator `bench/compare/make_trips_big.py`
- Engine start/stop: `infra/scripts/{trino,spark,flightsql}-{start,stop}.sh`
