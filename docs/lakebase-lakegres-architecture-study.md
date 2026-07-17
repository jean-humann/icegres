# Postgres on the Lakehouse: An Architectural Study of Databricks Lakebase and Onehouse Lakegres

> A complete study of the two dominant "Postgres-for-the-lakehouse" architectures, their
> connection to the Apache Iceberg (and Hudi/Delta) lakehouse, and a blueprint for building
> a similar system from scratch in Rust.
>
> **Method**: fan-out web research (5 angles, 22 sources), 90 extracted claims, top 25
> adversarially verified with 3 independent votes each — 23 confirmed, 2 refuted.
> Confidence levels are annotated throughout. Research date: July 2026.

---

## Table of contents

1. [Executive summary](#1-executive-summary)
2. [Databricks Lakebase: disaggregated Postgres (the Neon architecture)](#2-databricks-lakebase-disaggregated-postgres-the-neon-architecture)
3. [How Lakebase connects to the lakehouse](#3-how-lakebase-connects-to-the-lakehouse)
4. [Onehouse Lakegres: the inverse architecture](#4-onehouse-lakegres-the-inverse-architecture)
5. [Head-to-head comparison and design trade-offs](#5-head-to-head-comparison-and-design-trade-offs)
6. [Marketing vs. reality: two refuted claims](#6-marketing-vs-reality-two-refuted-claims)
7. [Building your own in Rust: blueprint and building blocks](#7-building-your-own-in-rust-blueprint-and-building-blocks)
8. [Open questions](#8-open-questions)
9. [Sources](#9-sources)

---

## 1. Executive summary

**Lakebase and Lakegres are opposite answers to the same question**: how do you get
Postgres-shaped, low-latency SQL next to a lakehouse?

- **Databricks Lakebase** (built directly on **Neon**, acquired May 2025) is a **true
  Postgres OLTP engine** whose storage layer has been replaced. Postgres compute is
  stateless; WAL durability moves to a Paxos-quorum of **SafeKeepers**, and a
  **PageServer** asynchronously materializes pages into object storage and reconstructs
  8&nbsp;KB pages on demand by replaying WAL deltas over base images. This disaggregation is
  what enables disabling full-page writes, metadata-only copy-on-write branching, and
  scale-to-zero. Its lakehouse connection is **copy-based**: managed CDC pipelines sync
  Unity Catalog tables into read-only Postgres "synced tables" (and a reverse path,
  powered by the Rust engine **Moonlink**, streams Postgres changes out to Delta/Iceberg).

- **Onehouse Lakegres** is **not a Postgres engine at all**. It is a Postgres-*compatible*,
  low-latency **serving endpoint that queries Hudi/Iceberg tables in place** — no CDC, no
  second copy. Internally it is an event-driven, non-blocking server layering **global
  indexes and columnar caching** over slow object storage, executing queries on Onehouse's
  proprietary **Quanton** engine.

**For a Rust rebuild**, these define two composable subsystems:

1. **A disaggregated OLTP core** (the Lakebase half): stateless Postgres + a Rust quorum-WAL
   service + a Rust page-materialization service over object storage. The open-source
   **Neon codebase is the reference implementation and is already mostly Rust** — the
   verifiable blueprint.
2. **A lake-integration layer** (choose or combine): copy-based CDC sync into Postgres
   (Lakebase model — **Moonlink/pg_mooncake** are the Rust open-source analogues), or a
   serve-in-place query layer over Iceberg (Lakegres model — **DataFusion + iceberg-rust +
   object_store/OpenDAL** are the building blocks).

The rest of this document details each layer, with confidence annotations and the design
trade-offs that matter when you build your own.

---

## 2. Databricks Lakebase: disaggregated Postgres (the Neon architecture)

*Confidence: HIGH — every claim in this section passed 3-0 adversarial verification and is
cross-checkable against the open-source Neon codebase (which is written largely in Rust).*

### 2.1 The core idea: Lakebase *is* Neon

Lakebase and Neon share the same architectural foundation. **Postgres itself is not
rewritten** — from the perspective of the query engine, nothing changes. Neon's patches
live at the storage layer (a `smgr` storage-manager replacement). What changes is where
state lives:

```
                         ┌──────────────────────────────┐
                         │   Postgres compute (STATELESS)│
                         │   unmodified query engine     │
                         │   shared_buffers (RAM)        │
                         │   local NVMe file cache       │
                         └──────┬────────────────┬───────┘
                       WAL stream│              │GetPage@LSN
                                ▼                ▼
              ┌─────────────────────┐   ┌──────────────────────┐
              │  SafeKeepers (×3)   │──▶│     PageServer       │
              │  Paxos quorum WAL   │WAL│  WAL → layer files   │
              │  commit = 2/3 acks  │   │  page reconstruction  │
              └─────────────────────┘   └──────────┬───────────┘
                                                   │ async upload/download
                                                   ▼
                                        ┌──────────────────────┐
                                        │   Object storage (S3)│
                                        │   long-term durability│
                                        │   immutable layers    │
                                        └──────────────────────┘
```

Compute holds **no durable state**. Two externalized services own it:

- **SafeKeeper** — the WAL/log service (durability).
- **PageServer** — the page-serving/caching backend over cloud object storage (reads).

This single move is credited with unlimited storage, elastic compute, durable writes,
simpler HA, and instant branching — without meaningful added latency.

### 2.2 Write path: Paxos-quorum WAL replaces local fsync

A commit is durable when the WAL record has been **replicated across a quorum of
SafeKeeper nodes (typically 2-of-3) via a Paxos-inspired protocol** — not when a local
disk fsync completes. Consequences:

- **Commit latency is bounded by network RTT**, not disk fsync. (Nuance: SafeKeepers do
  fsync WAL to *their own* local disks before acking — it's a quorum of persisted copies.)
- SafeKeepers retain WAL only until the PageServer has processed it and uploaded layers
  to object storage — **object storage is the long-term durability tier**; SafeKeepers
  are a durable buffer.
- The compute→SafeKeeper protocol is Neon's `walproposer` (running inside Postgres) talking
  to the `safekeeper` daemon (Rust). Jack Vanlightly's independent analysis corroborates
  the consensus design.

### 2.3 Read path: GetPage@LSN and page reconstruction

The PageServer consumes the WAL stream from SafeKeepers and **asynchronously** (off the
commit path) organizes it into immutable **layer files** on object storage:

- **Image layers**: materialized snapshots of page ranges at an LSN.
- **Delta layers**: WAL records covering a page range over an LSN range.

When compute needs a page it issues **`GetPage@LSN`**: the pageserver finds the most
recent materialized image of that page at or below the requested LSN and **replays the
chain of WAL delta records** on top to reconstruct the exact 8&nbsp;KB page version. (The
replay uses a sandboxed WAL-redo process derived from Postgres itself — see
`docs/pageserver-walredo.md` in the Neon repo.)

**The cache hierarchy keeps object storage off the hot path**:

1. Postgres `shared_buffers` (RAM) —
2. local NVMe file cache on the compute node —
3. PageServer (its own RAM/disk caches) —
4. object storage, read **only inside the pageserver** during reconstruction.

Compute never reads object storage directly; hundreds-of-milliseconds S3 reads happen only
in the pageserver, on cache miss.

### 2.4 WAL-volume optimizations unlocked by disaggregation

Two verified optimizations fall directly out of the architecture:

1. **Full Page Writes (FPW) disabled.** Vanilla Postgres writes a full page image after
   each checkpoint to guard against torn pages on local disk — inflating WAL volume **up
   to 15×** on write-heavy workloads. With stateless compute there is *no local data
   directory*, so the torn-page failure mode cannot occur; Lakebase/Neon turn FPW off
   safely. (Neon published the same design independently.)
2. **Image-generation pushdown.** Full-page-image creation moves into the storage tier:
   the pageserver materializes a new page image once a page accumulates **more delta
   records than a configured threshold** — driven by actual page churn, not the Postgres
   checkpoint cycle. This bounds reconstruction cost per page while writing images only
   where write activity demands it.

Both echo Amazon Aurora's "the log is the database" principle, but with a cleaner split:
Aurora's storage nodes apply redo internally; Neon additionally versions all history in
immutable layers on commodity object storage.

### 2.5 Branching, PITR, and scale-to-zero as metadata operations

Because durable state lives entirely in an externalized, **versioned, immutable-history**
storage layer:

- **Branching/cloning is copy-on-write**: a new branch points at an existing LSN in
  history and diverges from there; only new/modified data consumes storage. A large
  production database branches in **~1 second, O(1) regardless of size**. This is the
  killer feature for dev/test workflows and AI-agent sandboxing.
- **Point-in-time restore** and **read replicas** are the same mechanism: attach compute
  at an LSN.
- **Scale-to-zero**: since compute holds no state, idle compute is simply shut down
  (Lakebase defaults to a 5-minute idle timeout) and cold-started on the next connection.
- Cost caveat: branches pin retained parent history for billing — idle branches are not
  strictly free.

---

## 3. How Lakebase connects to the lakehouse

*Confidence: HIGH for the lake→Postgres direction (verified 3-0 against Databricks docs);
MEDIUM for the reverse direction (surfaced in extraction, not adversarially verified).*

### 3.1 Lake → Postgres: synced tables (copy-based managed CDC)

Lakebase's lakehouse connection is **a managed CDC pipeline that copies data**, not direct
lake reads. Creating a "synced table" provisions a **three-part object**:

1. a **Unity Catalog synced-table entry** referencing the pipeline (the table is a
   first-class catalog object with lineage/governance),
2. the **managed Lakeflow Spark Declarative Pipeline** doing the work,
3. a **read-only Postgres serving table** inside Lakebase ("read-only" is a strict
   recommendation rather than a hard block).

```
Unity Catalog (Delta/Iceberg)          Lakebase (Postgres)
┌──────────────────────┐   Lakeflow Spark    ┌────────────────────┐
│  source table        │   Declarative       │  synced table       │
│  (Delta CDF enabled) │──▶ Pipeline (CDC) ──▶│  (read-only serving)│
└──────────────────────┘                     └────────────────────┘
          ▲                                            │
          └────────── Moonlink / wal2delta CDC ◀───────┘
                     (reverse path: Postgres → Delta/Iceberg)
```

**Three sync modes**, with a hard dependency worth noting:

| Mode | Behavior | Requirement |
|---|---|---|
| **Snapshot** | full copy, re-runnable | none |
| **Triggered** | explicit incremental refresh | source must have **Delta Change Data Feed** |
| **Continuous** | always-on streaming apply | source must have **Delta Change Data Feed** |

Sources without CDF — **views, and notably Iceberg tables — support Snapshot only**. The
incremental machinery is Delta-CDF-specific today.

**The bulk-load vs. row-level-CDC asymmetry** (vendor-stated, key sizing input):

| SKU | Continuous/Triggered (CDC apply) | Snapshot (bulk load) | Gap |
|---|---|---|---|
| Lakebase Autoscaling | ~150 rows/sec/CU | up to 2,000 rows/sec/CU | ~13× |
| Lakebase Provisioned | ~1,200 rows/sec/CU | ~15,000 rows/sec/CU | ~13× |

Row-by-row upsert into Postgres (index maintenance, MVCC, WAL per row) is an order of
magnitude more expensive than sorted bulk load. **Any system you build must engineer
around this**: batch CDC applies, use `COPY`-based apply paths, coalesce updates per key,
consider deferred index maintenance.

### 3.2 Postgres → lake: the reverse path (Moonlink)

The reverse direction — streaming Lakebase Postgres changes out to Delta/Iceberg — is
powered by technology from the **Mooncake Labs acquisition (October 2025)**: the Rust
engine **Moonlink** consumes Postgres logical replication and commits changes to open
table formats without ETL pipelines. This is covered in depth in §7.3 because it is
open-source and directly reusable. *(This direction surfaced in research extraction but
was not among the adversarially verified claims — treat operational details as
provisional.)*

---

## 4. Onehouse Lakegres: the inverse architecture

*Confidence: MEDIUM — all claims passed 3-0 verification against the primary source, but
Lakegres is closed-source and brand-new (announced ~late 2025/2026); everything rests on
Onehouse's own announcement. Treat internals as vendor-stated design intent with no
independent implementation or performance verification.*

### 4.1 What it is

Lakegres is a **Postgres-compatible, highly available, autoscaling, low-latency SQL
serving layer that queries Apache Hudi and Apache Iceberg tables in place**. There is:

- **no separate authoritative OLTP store** — the lakehouse tables remain the single
  source of truth,
- **no data copy, no CDC sync** — the exact inversion of Lakebase's synced tables,
- a Postgres *wire-protocol* endpoint, **not** a Postgres engine.

```
   Clients (Postgres wire protocol)
        │
        ▼
┌───────────────────────────────────────────┐
│  Lakegres serving layer (event-driven,     │
│  non-blocking; HTTP/2-3; backpressure)     │
│  ┌──────────────┐  ┌─────────────────────┐│
│  │ global indexes│  │ columnar cache      ││
│  └──────────────┘  └─────────────────────┘│
│  Quanton engine instances (scale up & out) │
│  point lookups · O(N) index joins          │
└───────────────┬───────────────────────────┘
                │ reads in place (no copy)
                ▼
   Apache Hudi / Iceberg tables on object storage
   (single source of truth)
```

### 4.2 Stated internal design

Onehouse explicitly rejects the "analytical engine that just opened a JDBC/ODBC port"
approach. Their argument: low latency and high concurrency over slow cloud object storage
require:

- an **event-driven server architecture** — non-blocking I/O, event queues, HTTP/2-3
  networking, graceful degradation, backpressure handling — rather than
  thread-per-connection blocking designs;
- **global indexing** and **intelligent/columnar caching** layered over the lake to speed
  up storage access;
- execution on **vertically and horizontally scaled instances of the proprietary Quanton
  engine**, using point lookups, O(N) index joins, and columnar caching to reach
  database-class latencies on lakehouse data.

Nuance: derived index/cache structures *do* exist inside Lakegres — the "no copies" claim
means the lakehouse tables remain authoritative, not that no auxiliary state is built.

### 4.3 What this implies (and what's unknown)

The unknowns are substantial: the Quanton engine internals, the global index structure,
how indexes stay consistent with Hudi/Iceberg commits, cache invalidation on new
snapshots, and whether the claimed tail latencies hold under independent benchmarks. None
of that is public. What *is* clear is the workload contract: **Lakegres serves reads on
lake data; it is not where your application's transactional writes go.**

---

## 5. Head-to-head comparison and design trade-offs

| Dimension | Databricks Lakebase | Onehouse Lakegres |
|---|---|---|
| **What it fundamentally is** | Real Postgres OLTP engine, disaggregated storage | Postgres-compatible read-serving endpoint |
| **Postgres role** | Actual unmodified engine (compute tier) | Wire protocol compatibility only |
| **Source of truth** | Postgres storage (WAL → layers on S3) | Hudi/Iceberg tables on the lake |
| **Writes** | Full OLTP: transactions, constraints, indexes | Not an OLTP write path |
| **Lake connection** | Copy-based managed CDC (synced tables), bidirectional | Serve-in-place, zero-copy |
| **Freshness on lake data** | Sync lag (pipeline latency; ~150 rows/s/CU CDC apply) | As fresh as the last table commit |
| **Storage cost** | Second copy in Postgres + lake copy | One copy + indexes/caches |
| **Branching / PITR** | Yes — CoW metadata ops, O(1) | N/A (lake table time-travel instead) |
| **Scale-to-zero** | Yes (stateless compute, 5-min default idle) | Autoscaling serving fleet (vendor-stated) |
| **Iceberg support** | Snapshot-only sync (no CDF); reverse path via Moonlink | Native in-place reads (with Hudi) |
| **Openness** | Core = open-source Neon (Rust); control plane proprietary | Fully closed-source |
| **Verifiability** | High (code + docs + third-party analyses) | Low (vendor announcement only) |

**The essential trade-off** is *where the data-freshness/write-authority boundary sits*:

- **Lakebase**: your app gets a real database (constraints, transactions, sub-ms indexed
  reads *and writes*), and pays for it with **a second copy and sync lag** against the
  lake — plus the 13× CDC-apply penalty on the copy pipeline.
- **Lakegres**: your app gets **zero-copy freshness** on lake data and pays with **no
  transactional write path** and dependence on index/cache quality to hit database-class
  latencies over object storage.

They are not actually substitutes. A complete platform (and your Rust system) wants both
halves: an authoritative OLTP core *and* a low-latency serve-in-place layer, sharing one
lake.

---

## 6. Marketing vs. reality: two refuted claims

Adversarial verification killed two claims (0-3 votes each) that you should **not** build
assumptions on:

1. **"LTAP stores operational data exactly once in Delta/Iceberg, read directly by both
   Postgres and lakehouse engines, eliminating CDC."** — REFUTED. This is Databricks'
   *vision* framing. Today's shipping Lakebase still uses CDC/synced-table **copies**;
   single-copy LTAP does not exist as a product. If you build the single-copy dream, you
   are ahead of what ships — plan for the copy-based reality first.

2. **"Open-source Neon natively ships autoscaling, branching, and scale-to-zero as
   first-class OSS capabilities."** — REFUTED. Significant parts of the serverless feature
   set (autoscaling agent, scale-to-zero orchestration, branch lifecycle management) live
   in Neon's **proprietary control plane**, not the open repo. The open repo gives you the
   *mechanisms* (timelines, LSN-addressed storage, stateless compute); **a Rust rebuild
   must implement its own control plane** for the serverless behavior.

Also calibrate: the "5× faster writes", "15× WAL inflation", and rows/sec/CU numbers are
vendor approximations, not independent benchmarks.

---

## 7. Building your own in Rust: blueprint and building blocks

*This section is synthesis (interpretive, built from individually verified claims plus
extraction-phase findings on the Rust ecosystem — the crate-level claims below were
researched but not adversarially verified; validate versions/APIs when you start).*

### 7.1 Target architecture

Build **two subsystems around one lake**, mirroring what the market converged on:

```
                        ┌─────────────────────────────────────────┐
                        │            CONTROL PLANE (Rust)          │
                        │  tenant/branch lifecycle · autoscaling    │
                        │  scale-to-zero orchestration · placement  │
                        └──────┬──────────────────────────┬────────┘
        SUBSYSTEM A: OLTP core │                          │ SUBSYSTEM B: serve-in-place
                               ▼                          ▼
┌──────────────────────────────────────┐   ┌─────────────────────────────────────┐
│ Postgres compute (stateless, vanilla) │   │ Postgres-wire endpoint (pgwire)      │
│        │ WAL          ▲ GetPage@LSN   │   │ event-driven, tokio, backpressure    │
│        ▼              │               │   │        │                             │
│ ┌────────────┐  ┌───────────────┐    │   │        ▼                             │
│ │ walkeeper   │  │ pageserver     │    │   │ DataFusion query engine              │
│ │ (Rust,      │  │ (Rust, layers, │    │   │  + iceberg-rust TableProvider        │
│ │ Paxos quorum│  │ WAL-redo,      │    │   │  + global index + columnar cache     │
│ │ ×3)         │  │ image pushdown)│    │   │    (NVMe, e.g. foyer)                │
│ └─────┬──────┘  └──────┬────────┘    │   └────────────┬────────────────────────┘
│       └───────────┬────┘             │                │ zero-copy reads
└───────────────────┼──────────────────┘                │
                    ▼                                   ▼
            ┌──────────────────────────────────────────────────┐
            │        Object storage (S3/GCS/Azure)              │
            │  OLTP layer files        Iceberg tables (Parquet)  │
            └──────────────────▲───────────────────────────────┘
                               │
                 ┌─────────────┴──────────────┐
                 │  CDC bridge (Rust,          │
                 │  Moonlink-style):           │
                 │  PG logical replication →   │
                 │  Arrow buffer → Parquet +   │
                 │  deletion vectors → Iceberg │
                 └────────────────────────────┘
```

### 7.2 Subsystem A — disaggregated OLTP core (the Lakebase half)

**The Neon codebase (github.com/neondatabase/neon) is your verified blueprint — the
pageserver and safekeeper are already written in Rust.** Study (or fork) these before
writing a line:

- `safekeeper/` — quorum WAL service; the Paxos-inspired consensus with `walproposer`.
- `pageserver/` — layer files, `GetPage@LSN`, WAL-redo, compaction/image generation
  (`docs/pageserver-storage.md`, `docs/pageserver-walredo.md` are excellent).
- The Postgres side is a thin patch set: `smgr` hooks + walproposer. Keep Postgres
  unmodified at the query layer, exactly as Neon/Lakebase do.

Design decisions verified to matter:

1. **Commit = quorum ack of WAL, not fsync.** Build the WAL service first; it defines your
   durability contract. SafeKeepers fsync locally *and* quorum-replicate.
2. **Page materialization off the commit path.** The pageserver is an async consumer;
   never let S3 latency touch a commit.
3. **Turn FPW off, push image generation into storage** with a delta-count threshold per
   page. This is worth ~an-order-of-magnitude WAL reduction on write-heavy loads.
4. **Immutable layer files + LSN addressing** buy you branching, PITR, and replicas for
   free as metadata operations. Design layer format and timeline metadata early.
5. **Cache hierarchy**: shared buffers → compute-local NVMe → pageserver caches → S3.
   Compute must never touch S3 directly.
6. **You must build the control plane yourself** (refuted claim #2): idle detection,
   compute suspend/resume, branch lifecycle, pageserver placement/sharding. In Rust this
   is a natural fit for a `tokio` service with a reconciliation-loop (Kubernetes-operator)
   pattern.

### 7.3 The CDC bridge — Postgres ⇄ Iceberg (the synced-tables half)

The open-source Rust analogue of Lakebase's sync machinery already exists — **Mooncake
Labs' stack (acquired by Databricks, Oct 2025)**:

- **Moonlink** (github.com/Mooncake-Labs/moonlink, BSL license, ~all Rust): an
  Iceberg-native streaming ingestion engine. Ingests **Postgres CDC via logical
  replication** (millisecond-level latency) plus REST events; deployable as a Postgres
  background worker **or** a standalone service (`moonlink_service`, REST on :3030).
  Key techniques to copy:
  - **Buffer in Arrow, index on NVMe, then commit size-tuned Parquet files** — avoids the
    small-file/metadata-explosion problem of naive streaming Iceberg writes and eliminates
    periodic Spark compaction jobs.
  - **Deletes/updates → Iceberg v3 deletion vectors**, via a row-position index Moonlink
    maintains. Output is readable by any Iceberg engine (DuckDB, Spark, DataFusion).
  - **Union reads** for sub-second freshness *before* the Iceberg commit lands
    (`pg_mooncake` / `duckdb_mooncake` / DataFusion integration read buffer + table).
- **pg_mooncake** (~91% Rust, PG 14–18): a Postgres extension keeping a **columnstore
  mirror of Postgres tables in Iceberg format** — metadata in Postgres for transactional
  consistency, data as Parquet on object storage; read path via embedded DuckDB
  (pg_duckdb), write path via Moonlink; requires `wal_level = logical`.

Engineering guidance from the verified Lakebase numbers: the **~13× bulk-vs-CDC gap** is
the enemy. Batch logical-replication applies; group commits per Iceberg snapshot; use
`COPY` for the lake→Postgres direction; coalesce multiple updates to the same key within
a batch.

For the lake→Postgres direction (what synced tables do), note the verified constraint:
incremental sync needs a **change feed** from the source. Delta has CDF; **Iceberg
sources were snapshot-only in Lakebase** at research time. If you target Iceberg as
source of truth, design your own incremental strategy (snapshot-diff via
`changelog`-style scans, or make your CDC bridge the change feed).

### 7.4 Subsystem B — serve-in-place query layer (the Lakegres half)

Lakegres is closed, but its stated design maps cleanly onto the Rust ecosystem:

| Lakegres concept | Rust building block |
|---|---|
| Postgres-compatible endpoint | `pgwire` crate (Postgres wire protocol server) |
| Event-driven, non-blocking server, backpressure | `tokio` + bounded channels/semaphores |
| Query execution engine (Quanton) | **Apache DataFusion** (Arrow-native, extensible) |
| Iceberg table access | **iceberg-rust** (`iceberg` crate) — DataFusion integration ships SQL DDL/DML (CREATE/DROP/INSERT), LIMIT + rich predicate pushdown as of 0.9.0; Arrow/Parquet reader optimizations (byte-range coalescing, metadata size hints) |
| Object storage access | `object_store` (Apache Arrow) or **OpenDAL** — see below |
| Columnar/NVMe caching | hybrid memory+disk cache (e.g. `foyer`), caching Parquet footers, column chunks, and index blocks |
| Global indexing | your differentiator — see below |

**`object_store` vs OpenDAL** (researched trade-off):

- `object_store`: originated in InfluxDB IOx, donated to Apache Arrow; exposes the
  `ObjectStore` trait directly (`Arc<dyn ObjectStore>`); focused on S3/GCS/Azure/local/
  memory; **it is what DataFusion uses** — a DataFusion/iceberg-rust stack integrates most
  naturally with it.
- **OpenDAL**: broader backend coverage; hides access behind an `Operator`; composable
  middleware **Layers** (RetryLayer, TracingLayer, MetricsLayer, MetadataCacheLayer) vs
  `object_store`'s thinner adapters. Powers Databend, GreptimeDB, RisingWave.
- iceberg-rust 0.9.0 made storage **trait-based** (new `Storage` trait in FileIO), demoted
  OpenDAL to an optional crate (`iceberg-storage-opendal`), with `object_store` backend on
  the roadmap — both are viable; **default to `object_store` for a DataFusion-centric
  stack**, reach for OpenDAL if you need its middleware or exotic backends.
- Historical note: **icelake is sunset; iceberg-rust is the official successor** (ASF
  governance, 43+ contributors in year one). Build on `iceberg`, not `icelake`.

**What you must build yourself (the actual hard part, per Onehouse's own framing)**: an
analytical engine with a pgwire port is explicitly *not* enough. Database-class latency
over object storage requires:

1. **A global secondary index** over lake tables (key → file/row-group/row-position),
   updated on each table commit — this is what turns full scans into point lookups. A
   Moonlink-style row-position index is a starting design; an LSM of key→position
   postings on NVMe is a SOTA-consistent shape.
2. **Snapshot-consistent cache/index invalidation**: pin queries to an Iceberg snapshot;
   refresh indexes per commit; serve stale-bounded reads during refresh.
3. **Admission control and backpressure end-to-end** (bounded queues from pgwire accept
   loop through DataFusion execution to storage I/O), with graceful degradation — the
   event-driven contract Lakegres describes.

### 7.5 Suggested build order

1. **Phase 0 — serve-in-place MVP** (fastest to value, all-OSS): pgwire + DataFusion +
   iceberg-rust + object_store + foyer-style NVMe cache. Measure point-lookup latency;
   add the global index next. *This is a mini-Lakegres.*
2. **Phase 1 — CDC bridge**: Postgres logical replication → Arrow buffering → size-tuned
   Parquet + deletion vectors → Iceberg v3 (Moonlink's recipe, or embed Moonlink itself
   where its BSL license permits). Add union reads for sub-second freshness.
3. **Phase 2 — disaggregated OLTP core**: quorum WAL service, then pageserver with
   GetPage@LSN + WAL-redo, FPW off, image pushdown. Fork or heavily crib from Neon —
   don't reinvent verified machinery.
4. **Phase 3 — control plane**: scale-to-zero, branch lifecycle, autoscaling — the part
   the OSS world does *not* give you (refuted claim #2).

---

## 8. Open questions

Carried forward from verification; each is a research/prototyping task:

1. **Quanton/Lakegres internals**: index structure, consistency with Hudi/Iceberg commits,
   cache invalidation on new snapshots — and do any independent benchmarks validate the
   claimed tail latencies?
2. **Lakebase's reverse path mechanics** (wal2delta / Lakehouse Sync): WAL decoding
   format, exactly-once semantics, end-to-end latency — only the lake→Postgres direction
   was verified.
3. **Neon OSS vs. control plane boundary**: precisely which serverless capabilities live
   in the open repo vs. Neon/Databricks' proprietary control plane, and what a minimal
   Rust control plane must implement.
4. **Can the Rust lake stack close the gap?** Can a Moonlink-style mirror fully replace
   Spark-based CDC pipelines, and can DataFusion-over-Iceberg with a global index reach
   Lakegres-class point-lookup latencies?

---

## 9. Sources

**Primary (highest weight):**

- Databricks — [From monolith to Lakebase to LTAP: rethinking the database from storage up](https://www.databricks.com/blog/lakebase-ltap-rethinking-database-storage)
- Databricks — [How Lakebase architecture delivers 5x faster Postgres writes](https://www.databricks.com/blog/how-lakebase-architecture-delivers-5x-faster-postgres-writes)
- Databricks docs — [Synced tables](https://docs.databricks.com/aws/en/oltp/projects/sync-tables) · [Branches](https://docs.databricks.com/aws/en/oltp/projects/branches)
- Neon — [Architecture overview](https://neon.com/docs/introduction/architecture-overview) · [GetPage@LSN deep dive](https://neon.com/blog/get-page-at-lsn) · [Turning off FPW](https://neon.com/blog/turning-off-fpw-for-faster-writes)
- Neon source — [github.com/neondatabase/neon](https://github.com/neondatabase/neon) (`docs/pageserver-storage.md`, `docs/pageserver-walredo.md`)
- Onehouse — [Announcing Onehouse Lakegres](https://www.onehouse.ai/blog/announcing-onehouse-lakegres-database-speeds-finally-on-the-lakehouse) · [Product page](https://www.onehouse.ai/product/lakegres)
- Mooncake Labs — [pg_mooncake](https://github.com/Mooncake-Labs/pg_mooncake) · [moonlink](https://github.com/Mooncake-Labs/moonlink) · [architecture docs](https://docs.mooncake.dev/pg/intro/architecture)
- Apache Iceberg — [iceberg-rust 0.9.0 release](https://iceberg.apache.org/blog/apache-iceberg-rust-0.9.0-release/)
- Apache OpenDAL — [OpenDAL vs object_store comparison](https://opendal.apache.org/docs/rust/opendal/docs/comparisons/vs_object_store/index.html)
- Amazon — [Aurora SIGMOD paper](https://web.stanford.edu/class/cs245/readings/aurora.pdf)

**Independent analyses:**

- Jack Vanlightly — [Neon: serverless PostgreSQL (ASDS ch. 3)](https://jack-vanlightly.com/analyses/2023/11/15/neon-serverless-postgresql-asds-chapter-3)
- Xuanwo — [From icelake to iceberg-rust](https://xuanwo.io/2024/05-from-icelake-to-iceberg-rust/)

**Caveats**: nearly all Lakebase/Lakegres material is 2025–2026 vendor primary source for
fast-moving products — throughput figures, sync-mode constraints, and defaults will
change. Neon claims are strongest (cross-checked against open Rust code); all Lakegres
claims rest on Onehouse's own announcement for a closed-source product.
