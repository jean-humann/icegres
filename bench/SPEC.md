# icegres Comparison Engine & Benchmark Specification

Contract for the parity scorecard and benchmark harness. The goal: measure icegres
against the behavioral bar set by Databricks Lakebase (= Neon architecture) and
Moonlink (Postgres⇄Iceberg mirroring), then drive improvements **gated on zero
regression** (e2e suite green + no benchmark metric worse than baseline beyond noise).

Honest framing: icegres is a serve-in-place lakehouse server (Lakegres-shaped), not a
disaggregated OLTP Postgres. The scorecard therefore has three verdict classes:
- `PASS` — icegres exhibits the behavior (verified by an automated probe);
- `GAP` — behavior missing/partial; probe demonstrates the failure honestly;
- `N/A-BY-DESIGN` — the behavior is architecturally unnecessary for icegres (e.g.
  CDC-out: Moonlink exists to copy Postgres data *into* Iceberg; icegres data is
  *already* Iceberg — parity achieved by construction).

## 1. Parity matrix (comparison engine)

Runner: `bench/parity.sh` — executes every probe against a live stack, emits
`bench/results/parity-<ts>.json` and regenerates `bench/SCORECARD.md`.
Each probe: id, area, reference behavior (Lakebase/Neon/Moonlink), probe command,
pass criteria, verdict, evidence line.

### Area A — Postgres wire & SQL surface (Lakebase bar: full Postgres)
| id | behavior | probe sketch |
|---|---|---|
| A1 | psql connects, simple query protocol | `psql -c 'select 1'` |
| A2 | extended protocol / parameterized statements | tokio-postgres `query` with `$1` params |
| A3 | `\dt` / pg_catalog introspection | `psql -c '\dt demo.*'` lists tables |
| A4 | information_schema | `select * from information_schema.tables where table_schema='demo'` |
| A5 | multiple concurrent connections | 8 parallel psql SELECTs all succeed |
| A6 | server-side auth | connect with/without password — expect GAP (noop auth), documented |
| A7 | TLS | expect GAP, documented |

### Area B — OLTP semantics (Lakebase bar: real Postgres OLTP)
| id | behavior | probe sketch |
|---|---|---|
| B1 | INSERT via wire, durable | insert → new-connection readback (from e2e) |
| B2 | UPDATE | attempt; expect GAP (iceberg-datafusion 0.9 append-only) |
| B3 | DELETE | attempt; expect GAP |
| B4 | explicit transactions BEGIN/COMMIT/ROLLBACK | probe actual behavior, record honestly |
| B5 | PK/constraint enforcement | duplicate insert; expect GAP (Iceberg has no constraints) |

### Area C — Lakehouse integration (Moonlink bar)
| id | behavior | probe sketch |
|---|---|---|
| C1 | data in open format, other engines can read | independent reader (pyiceberg or duckdb if installable; else raw REST+parquet inspection) reads demo.trips count == server's answer |
| C2 | catalog registration (REST) | Lakekeeper `GET .../namespaces/demo/tables` lists both tables |
| C3 | CDC Postgres→Iceberg | N/A-BY-DESIGN (data born in Iceberg; no copy to sync) |
| C4 | write freshness (commit → readable elsewhere) | measured in §2 (freshness_ms); Moonlink bar: sub-second |
| C5 | Iceberg metadata surfaces | `select count(*) from demo.trips."$snapshots"` (however named) works |

### Area D — Serverless / elasticity (Neon bar)
| id | behavior | probe sketch |
|---|---|---|
| D1 | stateless compute: restart durability | kill serve, restart, data intact (from e2e) |
| D2 | multiple stateless computes on shared storage (read replicas) | 2nd `icegres serve --port 5440` against same catalog answers identically **including data committed after both started** |
| D3 | cold start | measured in §2 (cold_start_ms); Neon bar: ~500ms–few s |
| D4 | time-travel read (branching/PITR analogue) | query an older Iceberg snapshot (metadata tables/snapshot id); record actual support level honestly |
| D5 | scale-to-zero | expect GAP (no idle-shutdown supervisor); document as roadmap |

### Area E — Ops
| id | behavior | probe sketch |
|---|---|---|
| E1 | structured startup logs | grep serve log for catalog/warehouse/listen fields |
| E2 | health-checkable | connect probe doubles as health check; note absence of dedicated endpoint if so |
| E3 | full config via env vars | boot with only env vars, no flags |

## 2. Benchmark harness

Runner: `bench/bench.sh` (drives a small Rust bench binary or psql+timing loops —
implementer's choice; must use the **release** binary, warm up first, report p50/p95
over ≥20 iterations per metric, and emit machine-readable JSON).
Output: `bench/results/bench-<ts>.json` + human table appended to SCORECARD.md.

Metrics (all against the live local stack, table demo.trips ~280+ rows):
| metric | definition |
|---|---|
| connect_ms | TCP connect + startup to ReadyForQuery |
| point_lookup_ms | `select * from demo.trips where trip_id = <const>` |
| filtered_scan_ms | `... where city='Paris' and distance_km > 20` |
| aggregate_ms | GROUP BY city ORDER BY count LIMIT 5 |
| join_ms | trips ⋈ cities GROUP BY country |
| insert_single_ms | 1-row INSERT (full Iceberg commit — expect 100s of ms; honest number) |
| insert_batch100_ms | 100-row multi-VALUES INSERT |
| freshness_ms | commit in conn A → first successful readback of that row in conn B (poll 10ms) |
| qps_8conn | mixed read queries, 8 connections, 10 s, total queries/s |
| cold_start_ms | `serve` spawn → first successful `select 1` |
| binary_size_mb, rss_idle_mb | footprint |

Noise control: pin release build, quiesce stack, 3 warmup iterations discarded,
report p50/p95, run the full suite twice and require the two baselines to agree
within 25% on every metric before accepting a baseline.

## 3. Regression gate

`bench/gate.sh <baseline.json> <candidate.json>`:
- FAIL if any latency metric p50 worsens >20% or qps drops >10%;
- FAIL if `icegres/tests/e2e.sh` not green;
- FAIL if any parity verdict downgrades (PASS → GAP).
Every improvement lands only through this gate.

## 4. Improvement backlog (candidates for the loop, in expected-impact order)

1. **Release build as the served artifact** (current baseline binary is dev profile).
2. **Table/metadata caching** — avoid per-query catalog `load_table` + Parquet footer
   re-reads (DataFusion session/runtime caches, ListingTable-level or provider-level).
3. **DataFusion tuning** — target_partitions vs 4 cores, batch sizes, object-store
   connection pool / retry config for RustFS.
4. **Seed/file layout** — single-file seed commits (fewer small Parquet files) to
   speed scans; optional compaction pass.
5. **Snapshot/time-travel UX** (D4) and **idle scale-to-zero flag** (D5) if cheap.
6. **Second-compute smoke** (D2) formalized into e2e.

Published reference numbers (context only; hardware differs, not a gate): Neon
cold start ~500 ms–2 s; Moonlink freshness sub-second; Lakebase CDC apply
~150 rows/s/CU vs bulk 2k rows/s/CU.
