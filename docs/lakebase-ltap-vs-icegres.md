# Lakebase / LTAP vs icegres — a full comparison

> Comparison of Databricks' LTAP architecture — as described in the engineering
> blog post ["From monolith to Lakebase to LTAP: rethinking the database from
> storage up"](https://www.databricks.com/blog/lakebase-ltap-rethinking-database-storage)
> (Reynold Xin, June 30, 2026) — against the icegres implementation in this
> repository. Companion to `lakebase-lakegres-architecture-study.md` (the
> pre-LTAP research this system was designed from); this doc incorporates the
> full published article, which post-dates that study's research window.
>
> Article content below is a summary of the post, not a reproduction; read the
> original for the author's full argument and diagrams. icegres claims cite the
> measured numbers in `bench/SCORECARD.md` / `bench/COMPARISON.md` and the
> feature docs in `icegres/README.md`.

---

## 1. What the article says

### 1.1 The monolith and its five pains

The post starts from the storage layer of a traditional monolithic database
(Postgres as the running example): one provisioned machine holds the query
engine plus the two disk structures that matter — the WAL (makes writes fast
and safe: one sequential append instead of scattered random I/O) and the data
files (make reads fast: current state instead of history replay). Five
problems all trace to WAL + data files living on a single machine:

1. **Data loss from misconfiguration** — a commit is only as durable as the
   disk flush behind it, and flush settings are subtle and fail silently.
2. **Data loss from node loss** — WAL and data files die with the machine's
   disk; RAID/NAS mitigates but does not fundamentally solve it.
3. **Scaling reads requires a physical clone** — a read replica is a full
   physical copy plus WAL catch-up; slow and risky for large databases.
4. **HA requires a physical clone** — a standby is another complete copy;
   double the infrastructure plus synchronous replication to avoid loss.
5. **Analytics contend with transactions** — a reporting query or GDPR
   cleanup degrades OLTP on the same hardware; even a dedicated replica is
   row-oriented storage, wrong for analytics.

*Figure 1 of the post*: side-by-side boxes — "Database Monolith" (query
engine over WAL "speed up writes" + data files "speed up reads", ✗ single
machine, ✗ physical clones, ✗ data loss) vs "Lakebase" (stateless Postgres
streaming WAL to SafeKeeper, PageServer serving data pages, both over
"Object stores / Lake (PG data pages)", ✓ data in object storage, ✓ elastic
compute, ✓ durable writes).

### 1.2 Lakebase: externalize the WAL and the data files

The core move (the Neon architecture — Lakebase is built on it) is stateless
Postgres compute with both disk structures externalized into independent,
scalable services:

- **SafeKeeper (scaling writes).** The WAL becomes a distributed service; a
  commit is durable when the log record is replicated across a quorum of
  SafeKeeper nodes via Paxos-based replication — no disk whose failure loses
  data, no misconfigured flush. The post argues the extra network hop costs
  nothing vs a serious deployment (which needs synchronous replication
  anyway), and claims the SafeKeeper+PageServer combination can yield **5×
  higher write throughput and 2× lower read latency**.
- **PageServer (scaling reads).** Consumes the WAL stream from SafeKeeper and
  asynchronously materializes pages into cloud object storage — "a
  write-through cache for the underlying object storage". If asked for a page
  newer than what it holds, it applies SafeKeeper logs to reconstruct the
  latest state.
- **Read latency defended by a cache hierarchy** (*Figure 2*: Buffer Pool
  (memory) → Local file cache → PageServer → Object Stores): compute nodes
  can carry the same memory/disk as a monolith, so local hit rates are
  unchanged and object storage stays off the hot path.

What this unlocks (shipping in Lakebase/Neon per the post): still real
Postgres (wire protocol, SQL, drivers, extensions); unlimited storage;
serverless elastic compute incl. scale-to-zero; durable writes/zero data
loss via quorum; simpler HA (durable state already replicated, no promoted
physical copy); **instant branching, cloning and PITR** as metadata
operations on versioned storage — "the database finally moves as fast as
your code".

### 1.3 LTAP: one copy for transactions and analytics

Compute/storage separation is not new; the differentiator claimed is storing
operational data on commodity object storage **in an open format**, so other
engines can read it directly. Even with Lakebase, pages in object storage
were still Postgres row-format — so analytics had to pay per-read conversion
or (the common case) keep a second copy synced by a pipeline, which is
brittle and a governance problem.

**LTAP (Lake Transactional/Analytical Processing)** unifies at the storage
layer, not the engine layer — explicitly *not* HTAP's one-engine-for-both
(which fails on engine-ecosystem maturity and on re-introducing hardware
contention). Postgres keeps full ACID transactions; lakehouse engines keep
analytics; underneath both sits **one durable copy in open columnar formats
(Delta and Iceberg, stored as Parquet)** plus caches.

Mechanism ("Materializing in columnar form"):

- The **PageServer transcodes row pages into Parquet as it materializes them
  into the lake**, using spare CPU in the PageServer tier — zero burden on
  the Postgres compute serving transactions. Unlike CDC (logical change
  events into a foreign schema), the **exact Postgres representation of every
  value is preserved down to the bits**.
- **Type system**: most Postgres types map to native Parquet types; the rest
  (NaN/±Infinity, out-of-range NUMERICs, extension types) go to a structured
  **overflow field** in the same table — queryable by any engine and
  sufficient to reconstruct the original Postgres bytes.
- **Multi-versioning**: durability is separated from visibility. Every
  materialized row carries its **physical heap address (block, offset)**, so
  heap pages remain reconstructable; the classic heap page becomes a
  point-read **cache**, while the durable source of truth is the columnar
  files. Postgres **indexes are not transcoded** — they are served and
  rebuilt from the hot cache tier. Intermediate row versions are retained
  (MVCC + PITR) but invisible to Iceberg/Delta readers and eventually GC'd:
  analytics sees clean snapshot-consistent tables, Postgres keeps full
  time-travelable history.
- Columnar compresses **often >10×** vs rows, cutting network volume between
  cache tier and object store — cheap enough that Databricks currently
  **dual-writes row and columnar formats** during LTAP's transitional
  rollout, for verification.

Freshness ("Reading the latest data without affecting Postgres", *Figure 3*:
"classic CDC or mirroring" ✗ selected tables, ✗ two copies, ✗ pipeline
delays — vs "LTAP Reads" ✓ all tables automatically, ✓ one copy, ✓ always
up-to-date): an analytical query (e.g. from Lakehouse//RT) (1) asks Postgres
for the **current LSN** — a cheap metadata lookup and the *only* load
analytics puts on Postgres; (2) reads the overwhelming majority of data from
object storage as of that LSN; (3) fetches the small set of very recent
not-yet-materialized changes **from the PageServer and merges them on top**.
Result: consistent, fully up-to-date reads with no OLTP slowdown.
Optimization: **very small tables are not converted to columnar/Iceberg at
all** (bookkeeping would cost more than it saves); they remain queryable as
part of the single copy.

Final section ("Every table, automatically"): CDC/"mirroring"/"zero-ETL"
pipelines cost per table, so teams replicate only selected tables; LTAP's
conversion happens in the storage layer for **every table automatically**.

### 1.4 The open-source substrate behind the article

Both halves of the described system exist as inspectable code, which
anchors the summary above to implementations:

- The entire "Lakebase architecture" section describes
  [neondatabase/neon](https://github.com/neondatabase/neon)
  (**Apache-2.0**): SafeKeeper's proposer–acceptor quorum protocol is
  `safekeeper/src/safekeeper.rs` (+ `docs/safekeeper-protocol.md`), the
  PageServer's layer materialization and page reconstruction are
  `pageserver/src/tenant/storage_layer/` and `walredo.rs`, the read-cache
  hierarchy figure maps to `page_cache.rs` plus the compute-side file
  cache, and branching/PITR are the LSN-addressed `tenant/timeline/`
  machinery.
- LTAP's row→columnar tail lineage is
  [Mooncake-Labs/moonlink](https://github.com/Mooncake-Labs/moonlink)
  (**BSL 1.1**; Mooncake Labs was acquired by Databricks in Oct 2025): a
  per-table WAL over pluggable storage (`storage/wal.rs`), keyed memory
  buffers with deletion vectors (`storage/mooncake_table/`), Iceberg
  materialization incl. v3 deletion vectors
  (`storage/table/iceberg/iceberg_table_syncer.rs`), and the
  LSN-consistent union-read merge (`union_read/`,
  `moonlink_datafusion`'s TableProvider) — the article's freshness
  mechanism in miniature. The LTAP production transcoding tier itself is
  proprietary.

What each piece means for icegres — including the license and
dependency-matrix constraints on actual reuse — is worked through in
`sota-roadmap.md` §2.

## 2. The two architectures in one sentence each

- **LTAP** is *row-authoritative Postgres pushed down into the lake*: a real
  Postgres engine whose storage tier transcodes its pages into Parquet, so
  the columnar lake copy becomes the durable store and heap pages/indexes
  become caches above it.
- **icegres** is *the lake pulled up to the Postgres wire*: Iceberg tables on
  object storage **are** the database — an embedded DataFusion engine serves
  them over pgwire and Arrow Flight SQL, and every write is natively an
  Iceberg commit. There is no row store, page format, or WAL at any layer.

Both end at the same slogan — one open-format copy that transactions and
analytics share — but they arrive from opposite directions, and every
concrete difference below follows from that polarity.

## 3. Dimension-by-dimension

| dimension | Lakebase / LTAP (per the post) | icegres (measured / implemented) |
|---|---|---|
| Engine | real Postgres (full SQL, extensions, drivers) | DataFusion 52 behind pgwire + Flight SQL; pg_catalog emulation, ORM/JDBC/ODBC/ADBC verified (`compat.rs`) |
| Source of truth | columnar Parquet (Delta/Iceberg) in object storage; heap pages + indexes are rebuildable caches | Iceberg Parquet in object storage, full stop — no derived row tier exists |
| Copies of data | one durable copy + row-page cache tier + retained MVCC row versions (invisible to lake readers); dual row+columnar write during rollout | one copy; only caches are in-memory manifest metadata (`cache.rs`) and the opt-in ≤N ms write buffer |
| Durable write path | WAL record quorum-replicated across SafeKeepers (Paxos) — low-ms durable commits, hot-row friendly; claimed 5× write throughput | one Iceberg REST commit per statement/txn: ~50–60 ms p50 durable, ~1 ms/row amortized at batch 100; ~15–20 commits/s/table ceiling (catalog ref CAS) |
| Low-latency writes | native — the engine is Postgres | opt-in buffered mode (`--write-buffer-ms`): ~1.3–1.5 ms ack, group commit, union reads — but an unclean kill loses ≤N ms of acked writes (no quorum-WAL tier; the honest gap, see §4) |
| Concurrency / MVCC | full Postgres MVCC, row locks, retained intermediate versions | snapshot isolation per Iceberg snapshot; first-committer-wins `40001`; hot-row contention is a documented anti-pattern (CQRS Tier 1 = external Postgres) |
| Multi-table atomicity | yes (single Postgres engine) | single-table atomic; multi-table COMMIT = N ordered commits, `40003` on partial failure, or `ICEGRES_TXN_STRICT` to refuse up front |
| Indexes / constraints | full Postgres indexes (rebuilt from cache tier), constraints, sub-ms point reads | no index objects — Parquet stats/pruning + manifest caching give 6.9 ms point-lookup p50; opt-in PK enforcement (`--enforce-pk`, 23502/23505) |
| Type fidelity | bit-exact Postgres types; overflow field for non-Parquet-mappable values | Iceberg/Arrow type system only — no Postgres-exotic types to preserve, nothing to overflow |
| Analytics engine | delegated to lakehouse engines (Spark/Photon/Lakehouse//RT) — distributed scale | embedded DataFusion: 16–43× faster than Trino/Spark on interactive queries (7–10 ms vs 115–436 ms p50, same tables) but loses the largest scans (5M-row agg: 404 vs Trino 336 ms) and is not distributed |
| Analytics freshness | LSN handshake + PageServer tail-merge: always up-to-date, across engines | sync mode: exact by construction — the commit *is* the lake write; any engine reads it immediately (~60–73 ms after statement). Buffered mode: union reads on the buffering server only; external readers wait ≤N ms |
| OLTP/analytics isolation | analytics touches Postgres only for one LSN read | separate stateless computes over shared storage; BI on branch/replica endpoints via `icegresd` — serving path untouched |
| Branching / PITR | whole-database branch/PITR at any LSN, seconds, metadata-only | per-table zero-copy branches via Iceberg snapshot refs (one metadata commit, `assert-ref-snapshot-id` isolation); time travel to any *retained* snapshot; no whole-database branch, no arbitrary-point PITR between snapshots |
| Scale-to-zero | stateless compute, managed control plane | `--idle-shutdown-secs` + `icegresd` wake-on-connect (~73–85 ms wake, 0.4 ms warm-pool connect), crash supervision, branch routing |
| Small tables | skipped from columnar conversion (bookkeeping > benefit) | everything is Iceberg; small-file/snapshot bloat is a documented anti-pattern with no compaction command yet |
| Cache hierarchy | buffer pool → local file cache → PageServer → object store | manifest/metadata cache + OS page cache → object store (no page tier to cache; Parquet is read directly) |
| Maintenance | PageServer compaction/GC machinery, managed | snapshot expiry is shipped (`maintain expire-snapshots`); compaction and orphan-file GC are explicit gaps (pinned iceberg-rust 0.9.1) |
| Openness / deployment | managed service (Neon core is OSS; the LTAP transcoding tier and control plane are Databricks-proprietary) | one ~126 MB static binary + `icegresd`, self-hostable on any REST catalog + S3; open-core (auth/authz backends are the managed add-on); ~0.3 s cold start |

## 4. What LTAP has that icegres does not

> How icegres closes these without giving up §5 is designed in
> **`sota-roadmap.md`** (the durable-tail architecture).

1. **A quorum-WAL durability tier.** This is the deepest difference. LTAP
   commits are durable at network-RTT cost *before* anything reaches the
   lake; icegres' only durable act is the Iceberg commit itself (~50–60 ms),
   and its low-latency answer (`--write-buffer-ms`) explicitly trades a ≤N ms
   loss window on unclean kill. A SafeKeeper-shaped service in front of the
   write buffer (quorum-ack the buffered rows, replay into the Iceberg
   flusher after a crash) would close the durability hole while keeping the
   1.5 ms ack — the natural next phase from the architecture study (§7.2).
2. **Hot-row OLTP.** Postgres heap + indexes + MVCC make same-row
   update-heavy workloads native. icegres' copy-on-write UPDATE serializes
   winners at ~50–60 ms with `40001` retries — which is why the CQRS topology
   (`cqrs-topology.md`) puts that workload on an external Postgres (Tier 1).
   LTAP makes that external tier unnecessary in Databricks' world.
3. **Cross-engine freshness for buffered writes.** LTAP's LSN + tail-merge
   lets *any* engine see un-materialized changes; icegres' union read is the
   same idea confined to the buffering process. External readers of a
   buffered icegres table are ≤N ms stale.
4. **Whole-database branches and any-LSN PITR.** Iceberg refs give icegres
   per-table branches and snapshot-granular time travel only.
5. **Postgres type-system fidelity and extensions** — real Postgres above,
   bit-exact preservation below.
6. **Managed compaction/GC** of the lake tier.

## 5. What icegres has that LTAP does not

1. **The lake is plain Iceberg, born that way.** No transcoding tier, no
   heap-address bookkeeping, no invisible row-version retention, no
   dual-write rollout: an icegres table is byte-for-byte an ordinary Iceberg
   table because it was never anything else. Any engine can also *write* it
   through the catalog — in LTAP the lake copy is Postgres-owned; foreign
   writers cannot commit into it.
2. **Lake-native writes need no Postgres at all.** LTAP data enters through
   Postgres; icegres INSERT/UPDATE/DELETE/ingest *are* Iceberg commits
   (including Flight SQL bulk ingest = one commit per Arrow stream), so
   icegres also serves tables produced by Spark/Trino/pyiceberg — its "synced
   table" is a no-op.
3. **Interactive analytics inside the serving endpoint** — 7–10 ms
   point/filter/join p50s over lakehouse data, measured against Trino/Spark
   on identical tables; LTAP explicitly delegates analytics to separate
   (heavier, cluster-shaped) engines.
4. **Two first-class wire protocols** — pgwire *and* Arrow Flight SQL/ADBC
   with Arrow end-to-end.
5. **Self-hostability and verifiability.** Single static binary, local stack,
   133-assertion e2e (per `bench/SCORECARD.md`, R20), published benchmark
   harness; every performance claim
   above is reproducible from `bench/`. LTAP's 5×/2×/10× figures are vendor
   statements about a managed service (the post is transparent that rollout
   is still transitional/dual-write).
6. **A shipped minimal control plane in OSS** (`icegresd`): wake-on-connect,
   branch routing, warm session pool — the part the architecture study found
   proprietary in Neon/Lakebase (refuted claim #2).

## 6. Same questions, opposite answers

| design question | LTAP's answer | icegres' answer |
|---|---|---|
| Where do transactions get their semantics? | Postgres engine (MVCC, WAL) — lake format is made to carry them | Iceberg itself (snapshots, ref CAS) — no second semantics layer |
| How does analytics see a write from 1 s ago? | LSN handshake + merge PageServer tail over lake data | the write already *is* lake data (sync); union read (buffered, local) |
| What is the row format's role? | cache above the columnar truth | none — rows exist only in flight (Arrow) |
| How do you get fast point reads? | Postgres indexes on the heap cache tier | pruning + manifest caching (6.9 ms); no index objects |
| Branch unit | whole database at an LSN | one table at a snapshot ref |
| Who may write the lake copy? | only Postgres (via PageServer) | any Iceberg writer, icegres included |
| Cost of covering every table | automatic in the storage tier (tiny tables opted out) | automatic trivially — there is only the lake |

The post's own HTAP critique — don't build one engine for both jobs; unify
storage and keep the best engine per job — is precisely the bet icegres
makes from the other shore: icegres is the *serving* engine over the shared
copy and happily leaves 100 GB+ distributed scans to Trino/Spark
(`bench/COMPARISON.md`), just as LTAP leaves them to Photon/Spark.

## 7. Verdict against this repo's earlier study

`lakebase-lakegres-architecture-study.md` (research cut: pre-publication)
had **refuted** "LTAP stores operational data exactly once, read directly by
both sides, eliminating CDC" as vision-not-product. This article (June 30,
2026) is Databricks describing that machinery as built and rolling out —
with the honest caveats that it is transitional (row+columnar dual-write for
verification) and nuanced about "one copy" (heap-page cache tier, retained
invisible row versions, small tables kept row-only). The study's refuted
claim should be read as *superseded in design detail, still open in shipping
status*; independent verification of latency/freshness under load remains
unavailable. The study's other conclusions — Neon-based write path, the
serve-in-place blueprint icegres implements, the proprietary control plane —
are all consistent with the post.

**Net positioning.** LTAP and icegres validate each other's thesis: the
industry's endpoint is one open columnar copy under both workloads. LTAP
gets there Postgres-first and wins on OLTP semantics (quorum durability,
hot rows, indexes, whole-DB branching) at the cost of a proprietary,
Postgres-owned lake tier. icegres gets there lake-first and wins on
openness, engine-neutral writes, footprint, and interactive serving latency
at the cost of commit-rate ceilings, no quorum-WAL durability below ~50 ms,
and per-table (not per-database) branching. The roadmap item this article
most strongly motivates for icegres is #4.1: a replicated WAL/ack tier in
front of the write buffer, turning the ≤N ms loss window into a durable
low-latency write path.
