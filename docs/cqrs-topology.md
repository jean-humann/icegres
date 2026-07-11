# CQRS reference topology — OLTP + API + BI on icegres

**Status: reference recommendation (round 8).** This document turns the
session's measured results (`bench/SCORECARD.md`, `bench/results/*.json`)
into a concrete production topology: where each kind of workload should
live, which icegres mode serves it, and — just as importantly — which
workloads icegres is the *wrong* tool for. Every latency below was
measured on the 4-core/15 GB dev box against the local stack
(Lakekeeper + RustFS); treat them as relative guidance, not SLAs.

The architectural bet is CQRS with **one copy of the data**: all durable
state is Iceberg on object storage, commands and queries are separated by
*endpoint and mode*, not by replicating data between systems. The only
tier that owns non-Iceberg state is the optional hot-OLTP tier, and its
working set is explicitly a cache in front of the lake, not a second
source of truth.

## The three tiers

```
            writes (hot, contended)          writes (events, moderate txn)        reads
                    │                                   │                           │
        ┌───────────▼───────────┐            ┌──────────▼──────────┐     ┌──────────▼──────────┐
        │ TIER 1 · hot OLTP     │            │ TIER 2 · icegres    │     │ TIER 3 · BI / batch │
        │ external Postgres     │            │ serving computes    │     │ branch endpoints +  │
        │ (small working set)   │            │ (API reads+writes)  │     │ extra read computes │
        └───────────┬───────────┘            └──────────┬──────────┘     └──────────┬──────────┘
                    │ Moonlink-style CDC stream         │ direct Iceberg commits    │ reads @ branch/
                    │ (sub-second freshness)            │ (REST, single copy)       │ snapshot pins
                    ▼                                   ▼                           ▼
        ┌─────────────────────────────────────────────────────────────────────────────────────┐
        │                  Iceberg tables on object storage (THE one copy)                    │
        │                  REST catalog (Lakekeeper)  ·  Parquet on S3                        │
        └─────────────────────────────────────────────────────────────────────────────────────┘
                                    all tiers fronted by icegresd
                       (one public port, dbname routing, wake-on-connect, warm pool)
```

### Tier 1 — hot OLTP: a real Postgres in front, streamed to Iceberg

Some write shapes are heap-storage problems, not lakehouse problems:
counters, per-row optimistic locking, queues, anything where the *same
rows* are updated many times per second by concurrent writers. Keep those
in a small external Postgres holding only the hot working set, and stream
it into Iceberg Moonlink-style (logical replication → Iceberg appends,
sub-second freshness bar). icegres itself deliberately has no CDC
ingester (parity probe C3 is N/A-by-design — icegres-born data never
needs one); this tier is the one place a second engine earns its keep.
The Postgres here is a *derived-latency* tier: sized to the working set,
rebuildable, never the system of record for analytical history.

Route to this tier when: row update frequency ≫ 1/s per row, lock-based
patterns (`SELECT … FOR UPDATE`), queue tables, sub-10 ms durable-write
SLO.

### Tier 2 — icegres serving computes: API reads + two write modes

The workhorse tier. One or more `icegres serve` computes (stateless — all
state in the catalog/object store), fronted by `icegresd`, serving three
workload classes on the same single-copy tables:

1. **API reads** — point lookups, filtered scans, aggregates, joins at
   **~7 ms p50** (point 6.9 / filtered 6.8 / aggregate 7.2 / join
   9.9 ms; ~380 qps over 8 connections on 4 cores). Freshness is exact:
   every scan checks the current snapshot, no TTL, no staleness window.
   Opt-in `--freshness-ms N` trades that exactness for latency, boundedly:
   scans skip the per-scan catalog check (point lookup **~4.4 ms p50**,
   repeated identical statements **~2.8–3.6 ms** via the physical-plan
   cache); this server's own writes stay read-your-own-writes exact, and
   commits from OTHER computes become visible within ~N ms + one refresh
   round trip — tables refresh concurrently with a per-table timeout, so a
   slow table delays only itself (staleness age exported on `/metrics`,
   sampled at refresher pass start). Suited to read-mostly API computes;
   leave it off where cross-compute exactness matters.
2. **Buffered event writes** (`--write-buffer-ms N`, opt-in) — telemetry,
   audit logs, clickstream, anything append-only and loss-tolerant for
   ≤ N ms: INSERT acks at **~1.3–1.5 ms p50** from the in-memory buffer,
   a background task group-commits every N ms, same-server readers see
   rows immediately via union reads (**~8–10 ms p50** cross-connection
   freshness). Trade-off, stated plainly: an unclean kill loses up to
   N ms of acked writes — that is why the default is 0 (fully
   synchronous) and enabling it logs a WARN. Run buffered writers as a
   *dedicated* compute per event stream so the durability window applies
   only to workloads that opted in.
3. **Transactional moderate-rate writes** (default synchronous path) —
   orders, registrations, state transitions: each statement (or explicit
   `BEGIN…COMMIT` group) is one Iceberg REST commit at **~50–60 ms p50**
   (insert_single 56 ms; batch of 100 rows ~100 ms ≈ 1 ms/row amortized),
   readable from any other connection/compute **~60–73 ms** after the
   statement (sync freshness p50). Real BEGIN/COMMIT/ROLLBACK with
   snapshot isolation and first-committer-wins (`40001` on conflict);
   opt-in PK enforcement (`--enforce-pk`, 23502/23505). ORM traffic
   (SQLAlchemy/psycopg2/pg8000/pandas) works against this tier as-is
   (parity A8).

### Tier 3 — BI and batch: read replicas + branch endpoints via icegresd

Analytics never touches the serving computes. Because computes are
stateless over shared storage (parity D2), a "read replica" is just
another `icegres serve` process — no WAL shipping, no replication lag
mechanism, freshness = one catalog metadata load (~tens of ms). `icegresd`
makes this operable from ONE public port:

- **dbname routing** — `psql -d icegres` reaches the main compute,
  `psql -d 'icegres@reporting'` reaches a per-branch compute spawned on
  demand (`--branch reporting`), so BI tools get a stable DSN per
  workload with zero client-side knowledge of ports.
- **Zero-copy branches for stable BI snapshots** — `icegres branch create
  demo.trips reporting` is ONE metadata commit (no data copied); the BI
  endpoint reads a frozen-then-controllably-advanced view while OLTP
  writes continue on `main`, and writes on the branch (e.g. materializing
  a report table) can never leak onto `main` (`assert-ref-snapshot-id`).
  Ad-hoc reproducibility comes free from time travel
  (`demo."trips@<snapshot_id>"`).
- **Scale-to-zero for bursty BI** — a reporting compute that idles simply
  exits (`--idle-shutdown-secs`); the next dashboard query wakes it
  through icegresd in **~73 ms p50** (wake-after-idle through the proxy,
  incl. connect overhead; ~45 ms direct cold start; 85–96 ms measured
  end-to-end in the D5 probes). With the warm session pool
  (`--pool-size`, default 8), repeat short-lived connections — the
  BI-tool and serverless-API pattern — reach ReadyForQuery in
  **~0.4–0.7 ms** (`connect_via_proxy_ms`), ~165× faster than a wake.
  The pool is session-scoped only: one client per backend connection,
  never reused, and deliberately NO transaction pooling (session state —
  txn buffers, `SET`, prepared statements — makes statement-hopping
  unsafe). Proxy tax on throughput: ~6% (359 vs 382 qps direct).
- Anything Postgres-incompatible (Spark, Trino, DuckDB, pyiceberg) reads
  the *same tables* through the REST catalog directly (parity C1) — the
  single copy is the integration point, not a wire protocol.

## Measured numbers (one table)

| metric | value (p50 unless noted) | source |
|---|---|---|
| API read latency (point/filtered/agg/join) | 6.9 / 6.8 / 7.2 / 9.9 ms | `baseline.json` (= `bench-20260706T033809Z`) |
| read throughput, 8 conns, 4 cores | ~382 qps direct / ~359 via icegresd | same |
| synchronous INSERT (durable Iceberg commit) | ~50–60 ms (56.1 ms; p95 72 ms) pre-increment; **~46 ms** after the write-latency increment (redundant metadata fetch killed + independent uploads overlapped; timing-mode probe, this tree) | same + write-latency probe |
| batch INSERT, 100 rows | ~99 ms (≈1 ms/row) pre-increment; **~40 ms** timing-mode probe, this tree | same |
| buffered INSERT ack (`--write-buffer-ms 100`) | ~1.3–1.5 ms | same + R6.4 acceptance |
| durable tail ack (`--tail-dir` / `--tail-url` / `--tail-quorum`) | ~1.5–2.5 / ~2.2 / ~2.5 ms; local WAL group fsync under concurrency: 8 writers p50 ~6 ms (was ~9 ms serialized), p95 ~11 ms (was ~24 ms) | write-latency probe, this tree; `durable_ack_*_ms` in bench |
| keyed UPDATE ack (`icegres.tail-upsert`, hot row) | **~5.2 ms** with `--freshness-ms 25` (~7.0 ms exact-freshness; was ~9.5 ms pre-increment) | write-latency probe, this tree |
| freshness, buffered mode (same server, new conn) | ~8–10 ms (p95 bimodal ~80 ms) | same |
| freshness, synchronous mode (any reader) | ~60–73 ms | same |
| API read latency with `--freshness-ms 25` (point, distinct literals / repeated point / repeated filtered agg) | 4.4 / 3.6 / 2.8 ms | `ICEGRES_QUERY_TIMING` 30-query probe, this tree |
| cross-compute visibility bound with `--freshness-ms 25` | ~N ms + 1 refresh round trip, per table (a slow table delays only itself, up to its min(4·N, 2 s) refresh timeout); measured ≤ 45 ms end-to-end incl. poll granularity | e2e §(z) |
| cold start, direct `icegres serve` | ~45 ms | same |
| wake-after-idle through icegresd | ~73 ms (manual probes 85–96 ms) | `cold_start_via_proxy_ms`, D5 |
| warm-pool connect via icegresd | 0.44 ms (p95 0.66 ms) | `connect_via_proxy_ms` |
| branch create/drop | one metadata commit, zero data copied | D6, e2e §(m) |

## When to use which

| workload | tier / mode | why |
|---|---|---|
| API point reads, dashboards' hot queries | Tier 2 default serve | ~7 ms reads, exact freshness, ORM-compatible |
| Read-mostly API computes that tolerate ~tens-of-ms staleness from OTHER writers | Tier 2 + `--freshness-ms 25` | ~4.4 ms point reads (2.8–3.6 ms repeated shapes); own writes stay exact; staleness bounded per table (~N ms + 1 refresh round trip; a slow table delays only itself) + gauged on `/metrics` |
| Event/telemetry ingest (append-only, loss-tolerant ≤N ms) | Tier 2 + `--write-buffer-ms` on a dedicated compute | 1.5 ms ack, ~40× cheaper than sync; group commit amortizes the REST round-trip |
| Orders/state transitions (moderate rate, must be durable + atomic) | Tier 2 default serve, transactions | 50–60 ms durable commit, snapshot isolation, 40001 conflict semantics |
| Bulk load / backfill | Tier 2, multi-row INSERT batches (or direct Iceberg writers) | ~1 ms/row amortized at batch 100; engines can write via the catalog directly |
| BI, ad-hoc analytics, scheduled reports | Tier 3 branch/replica endpoints via icegresd | isolation from serving path, zero-copy stable views, scale-to-zero when idle |
| Reproducible reporting / audits | Tier 3 + time travel / tags | any retained snapshot queryable read-only |
| Sub-10 ms durable single-statement writes | Tier 2 + `--write-buffer-ms` + a durable tail (`--tail-dir`/`--tail-url`/`--tail-quorum`) | ~1.5–2.5 ms tail-durable INSERT ack, ~5.2 ms keyed UPDATE with `--freshness-ms`; durability class = the chosen tail (see the ladder in the READMEs); explicit transactions stay sync by design |
| Hot counters, queues, `FOR UPDATE` row locking, sub-ms SLOs, multi-statement transactions at sub-10 ms | **Tier 1 external Postgres** → stream to Iceberg | copy-on-write + optimistic concurrency is the wrong engine for lock choreography, and a tail-staged COMMIT would break `40001` (refused — docs/limitations.md) |
| Spark/Trino/Python batch jobs | direct Iceberg REST + S3 (no icegres at all) | the single copy is open; don't proxy bulk scans through pgwire |

## Anti-patterns (honest list)

- **Hot row contention.** UPDATE on icegres is copy-on-write: each
  statement rewrites the Parquet file(s) containing the row and commits a
  snapshot under first-committer-wins. Two writers updating the same
  table concurrently means one eats a `40001` retry; N writers hammering
  the same row serialize at ~50–60 ms per winner with retry storms for
  the rest. Symptom to watch: 40001 rate climbing with writer
  concurrency. **Opt-in mitigation (roadmap Phase 2, shipped):** for
  upsert-shaped traffic, keyed tail upserts (`icegres.tail-upsert` +
  `icegres.primary-key` + `--write-buffer-ms` + a durable tail) ack
  exact-PK UPDATE/DELETE from the tail in ~5.2 ms p50 with
  `--freshness-ms 25` (~7.0 ms without) and coalesce per
  key into ONE commit per flush window — no per-statement snapshots, no
  client-visible 40001 for acked keyed ops (see `icegres/README.md`
  "Hot rows" and docs/limitations.md for the LWW semantics trade).
  Lock choreography (`SELECT ... FOR UPDATE`) and sub-ms SLOs remain
  Tier 1 work.
- **High-QPS single-table synchronous writers.** Every sync commit is an
  Iceberg REST commit: one table sustains on the order of **~15–20
  commits/s** (1000 ms / ~55 ms), regardless of how many computes you
  add — the ref CAS on the catalog is the serialization point. Batch
  (100 rows ≈ 1 commit), buffer (`--write-buffer-ms` turns 100 acks into
  1 commit), or shard by table; do not scale writers horizontally against
  one hot table and expect linear throughput.
- **Small-write snapshot/file bloat.** Sustained 1-row sync INSERTs
  produce one small Parquet file + one snapshot each; scans slow by
  ~0.3–1 ms per extra file (measured — see "Layout drift" in the
  scorecard) and there is no `icegres compact` yet (recovery = re-seed /
  external rewrite). Buffered or batched ingest is not just faster, it
  keeps the layout healthy.
- **Treating buffered mode as durable.** The ≤N ms loss window on unclean
  kill is real (e2e proves committed rows survive SIGKILL — acked-
  uncommitted rows are the window). Never put orders/payments on a
  buffered endpoint; that's what the sync path is for.
- **Transaction pooling / connection multiplexing assumptions.** icegresd
  pools *sessions*, one client per backend connection, no reuse. PgBouncer
  `pool_mode=transaction` semantics do not exist here by design; sizing
  math that assumes statement-level multiplexing will overload computes.
- **BI on the serving endpoint.** A full-scan dashboard competes for the
  same 4 cores as your 7 ms API reads. Branch/replica endpoints cost one
  metadata commit and idle to zero — there is no excuse to co-locate.
- **Expecting a CDC ingester in icegres.** Tier 1's Postgres→Iceberg
  stream is external machinery (Moonlink or equivalent); icegres reads
  the resulting tables like any others but does not ship the pipeline.

## Operational sketch

```sh
# Tier 2: public endpoint, wake-on-connect, warm pool for API reconnects
icegresd serve --port 5432 --idle-shutdown-secs 300 --pool-size 8

# Dedicated buffered compute for the event stream (opt-in durability window)
ICEGRES_WRITE_BUFFER_MS=100 icegres serve --port 5441 &

# Tier 3: BI branch endpoint, routed by dbname through the same public port
icegres branch create demo.trips reporting
psql -h api.internal -p 5432 -d 'icegres@reporting'   # spawns/wakes the BI compute

# production hardening on any compute
icegres serve --auth-file /etc/icegres/auth.conf \
  --tls-cert /etc/icegres/tls.crt --tls-key /etc/icegres/tls.key --enforce-pk
```

Related reading: `bench/SCORECARD.md` (all measurements + gate history),
`icegres/README.md` (flags, modes, ORM compatibility, limits),
`docs/lakebase-lakegres-architecture-study.md` (the architecture study
this system implements).
