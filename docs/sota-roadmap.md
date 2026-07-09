# SOTA roadmap — closing the Lakebase gaps without giving up the lake

> Design study for the next phases of icegres, written against the gap
> analysis in `lakebase-ltap-vs-icegres.md` §4/§5. Goal: absorb what
> Lakebase/LTAP genuinely does better — durable low-latency writes, hot-row
> updates, fleet-wide freshness, whole-database branching — **without
> surrendering any of the properties Lakebase cannot offer**: plain Iceberg
> tables writable by any engine, one authoritative copy, a single
> self-hostable binary, embedded interactive analytics, and reproducible
> claims.
>
> §2 grounds the plan in the two open-source codebases Lakebase is actually
> built from — [neondatabase/neon](https://github.com/neondatabase/neon)
> (Apache-2.0) and
> [Mooncake-Labs/moonlink](https://github.com/Mooncake-Labs/moonlink)
> (BSL 1.1) — mapping each concept in the Databricks article to the code
> that implements it, with an explicit license/version posture per reuse.

---

## 0. Invariants (what must survive every phase)

These are the icegres advantages the comparison doc identified; each phase
below is designed against them and any phase that would break one is
redesigned or rejected.

- **I1 — The lake is the only source of truth.** Every icegres table remains
  a byte-ordinary Iceberg table. No tier of icegres is ever authoritative
  for committed history; anything icegres adds above the lake is
  transient-by-contract and reconstructible from it.
- **I2 — Foreign writers keep working.** Spark/Trino/pyiceberg can commit to
  any table at any time; icegres semantics (first-committer-wins, retry)
  already absorb that and must continue to.
- **I3 — Opt-in, zero-dependency default.** `icegres serve` with no flags
  must keep working against nothing but a REST catalog + S3, one static
  binary, ~0.3 s cold start. New tiers are per-table/per-compute opt-ins.
- **I4 — Honesty ledger.** Every durability/latency/freshness trade a mode
  makes is stated in docs, warned at startup where surprising, and locked by
  e2e proofs (the `--write-buffer-ms` SIGKILL test is the template).

## 1. The one architectural move: a durable tail, subordinate to the lake

Four of the six Lakebase advantages are symptoms of a single missing organ:

| Lakebase advantage | icegres symptom today |
|---|---|
| quorum-WAL durable commits (low ms) | durable ack = Iceberg REST commit, ~50–60 ms |
| low-latency acks without loss | `--write-buffer-ms` acks in ~1.5 ms but loses ≤N ms on unclean kill |
| hot-row updates | COW UPDATE serializes winners at ~55 ms with `40001` storms |
| fresh reads across the fleet | union reads are process-local; other computes wait for the commit |

Lakebase's SafeKeeper/PageServer solve all four — by making the row tier
authoritative and demoting the lake to a materialization target, which is
exactly the inversion that costs them I1/I2 (their lake copy is
Postgres-owned and write-closed). The icegres answer is the same organ with
the opposite allegiance:

> **The durable tail**: a small, durable, shared log of acked-but-not-yet-
> committed rows, keyed per table, bounded by the flush window, and
> **truncated at every Iceberg commit**. It is SafeKeeper's job description
> with Iceberg's chain of command — never a second source of truth, only
> the staging buffer between the ack and the commit.

This is not a new subsystem so much as a durability upgrade to code that
already exists: `buffer.rs` already implements group commit, the
`pending → flushed(S)` union-read protocol with proven exactly-once overlay
semantics, ordering fences, and flush-retry against fresh metadata. The tail
replaces "pending lives only in this process's RAM" with "pending is durable
and shared"; everything downstream of the flush is unchanged.

## 2. The open-source substrate: what Lakebase is actually made of

The Databricks article describes a system whose two halves exist as
inspectable code. Local reference clones live at `~/neon` and `~/moonlink`
on the dev box (reference material only — **not** vendored into this repo).

| repo | license | role in the article | reuse posture for icegres |
|---|---|---|---|
| [neondatabase/neon](https://github.com/neondatabase/neon) | **Apache-2.0** | the entire "Lakebase architecture" section — SafeKeeper, PageServer, cache hierarchy, branching — is a description of this codebase | **freely vendorable/forkable**; adopt code and test harnesses directly |
| [Mooncake-Labs/moonlink](https://github.com/Mooncake-Labs/moonlink) | **BSL 1.1** — internal use & embedding permitted; offering it (or derivatives) *as a managed service to third parties* requires a commercial license; each release converts to Apache-2.0 four years out (beta: 2029-06-03) | the Postgres⇄Iceberg streaming lineage Databricks acquired (Oct 2025) — the closest public ancestor of LTAP's row→columnar tail machinery | **study-and-reimplement** for anything shipped in icegres core (see §2.3); optional feature-gated integration at most |

### 2.1 Article concept → code map

| article concept | where the code lives |
|---|---|
| SafeKeeper: "commit = quorum replication via Paxos-based protocol" | `neon/safekeeper/src/safekeeper.rs` (proposer–acceptor state machine, `TermHistory`), `wal_storage.rs` (segment fsync discipline), `control_file.rs` (persisted acceptor state), protocol spec in `neon/docs/safekeeper-protocol.md`; compute side in `neon/libs/walproposer` + `pgxn/` |
| PageServer: "write-through cache… materializes pages into object storage" | `neon/pageserver/src/tenant/` (`storage_layer/`, `layer_map/` — image/delta layers), `walredo.rs` + `walredo/` (page reconstruction), docs `pageserver-storage.md`, `pageserver-walredo.md`, `pageserver-compaction.md` |
| the read-cache hierarchy figure (buffer pool → local file cache → PageServer → object store) | `neon/pageserver/src/page_cache.rs`, `tenant/ephemeral_file.rs`; compute-side local file cache in Neon's Postgres extension |
| "branch a large production database in seconds" / PITR | `neon/pageserver/src/tenant/timeline/` — LSN-addressed timelines over immutable layers |
| durable object-storage access done right | `neon/libs/remote_storage` — one trait, S3/GCS/Azure/local backends, **configurable timeouts + retries**, even a `simulate_failures` backend for fault-injection tests |
| LTAP's row→columnar tail: buffer in memory, persist to lake, merge for freshness | `moonlink/src/moonlink/src/storage/mooncake_table/` — `mem_slice`, `disk_slice`, `delete_vector`, `snapshot_read`, `transaction_stream`; per-table WAL over pluggable storage in `storage/wal.rs`; the LSN bookkeeping taxonomy (commit/flush/iceberg-snapshot/persisted LSNs) documented at the top of `table_handler.rs` |
| the "ask for the LSN, merge the recent tail" freshness read | `moonlink/src/moonlink/src/union_read/` (`ReadStateManager`) — and `moonlink_datafusion/` ships a **DataFusion `TableProvider` that serves union reads**, i.e. the same engine family icegres embeds |
| Iceberg materialization incl. deletion vectors | `moonlink/src/moonlink/src/storage/table/iceberg/` (`iceberg_table_syncer.rs`, `deletion_vector.rs`, manifest managers) — note it builds against a **git revision of iceberg-rust**, ahead of any crates.io release, to write v3 deletion vectors/puffin |
| key → row-position index ("turns scans into point lookups") | `moonlink/src/moonlink/src/storage/index/` (`mem_index.rs`, `persisted_bucket_hash_map.rs` — the GlobalIndex) |
| file compaction | `moonlink/src/moonlink/src/storage/compaction/` |
| CDC from a real Postgres (CQRS Tier 1's machinery) | `moonlink/src/moonlink_connectors/src/pg_replicate/` (logical replication → table events) |

### 2.2 Immediately actionable, license-clean wins (pre-Phase-1)

- **Port `remote_storage`'s timeout/retry discipline** (or vendor the crate)
  behind icegres' OpenDAL storage factory — this closes the documented
  `limitations.md` gap "no object-store request timeout/retry" with
  Apache-2.0 code that has run in production for years. Its
  `simulate_failures` backend is also the right tool for hardening the
  flush/retry paths in `overwrite.rs`/`buffer.rs`.
- **Adopt Neon's deterministic-simulation testing idea** (`neon/libs/desim`)
  for the tail's consensus/recovery protocol before any quorum backend
  exists — cheap insurance the e2e harness can't provide.

### 2.3 The BSL and version reality (stated plainly, per I4)

Moonlink is the proof that the roadmap's Phase 1–2 shape works in
production Rust — and it is **not linkable into icegres today**, for two
independent reasons:

1. **License.** BSL 1.1 permits internal use and embedding, but icegres is
   open-core software whose deployers may themselves offer managed icegres
   endpoints; embedding BSL code would encumber exactly that use, and the
   commercial licensor is now Databricks. Default posture: **reimplement
   the shape, cite the design** (icegres' `buffer.rs` union-read protocol
   was already independently designed and proven — the precedent exists).
   A feature-gated optional integration or post-2029 adoption (as each
   release's change date converts it to Apache-2.0) remain fallbacks.
2. **Dependency matrix.** Moonlink pins arrow 56 / datafusion 50 / a git
   revision of iceberg-rust; icegres pins arrow 57 / datafusion 52 /
   iceberg 0.9.1 as an interlocked unit (`limitations.md`). Even at
   Apache-2.0 the crates could not link into one binary without matrix
   surgery. Its value is as a **reference implementation and test oracle**:
   the module boundaries, the LSN taxonomy, and the DV-over-iceberg-rust
   write path (evidence for what a future matrix bump unlocks).

Neon is the opposite case: Apache-2.0 and organized as reusable library
crates — fork, vendor, or depend at will.

## 3. Phase 1 — the durable tail (`TailStore`)

```text
trait TailStore {
    append(table, rows) -> seq        // durable before return = the ack
    subscribe(table, from_seq)        // stream for other computes' overlays
    truncate(table, upto_seq)         // called after the Iceberg commit lands
    lease(table) -> FlushLeadership   // exactly one flusher per table
}
```

Backends, in build order:

1. **`local` — fsync'd WAL segment on the compute.** Cheapest correct step:
   group-fsync every ~1 ms, replay the tail into the buffer on restart.
   Kills the unclean-kill loss window on a surviving disk; does *not* give
   node-loss durability or cross-compute visibility (single-writer per table
   enforced). Honest framing: this reintroduces the article's "durability
   tied to one machine's disk" — a strictly-better stopgap, not the goal.
   *Reuse:* segment/fsync discipline from `neon/safekeeper/src/
   wal_storage.rs` (Apache-2.0, adaptable); moonlink's `storage/wal.rs`
   (per-table WAL + replay-on-recovery over a pluggable accessor) is the
   design to mirror, not link (§2.3).
2. **`postgres` — the recommended shared backend.** Any Postgres URL
   (`ICEGRES_TAIL_URL`); the natural zero-new-infra choice is a dedicated
   database on the instance already backing Lakekeeper — every icegres
   deployment already runs one, already treats it as availability-critical,
   and already gets its HA story from it. One insert-mostly table
   (`tail(table_id, seq, pk?, payload)`); group-committed multi-row inserts
   amortize the ack to a **~1–3 ms durable p50** on the same box.
   `LISTEN/NOTIFY` (+ poll fallback) feeds other computes' overlays;
   advisory locks provide the flush lease; `ON CONFLICT`/row locks arbitrate
   per-key writes (Phase 2). Quorum durability = the Postgres instance's own
   replication, delegated rather than rebuilt. No OSS analogue needed —
   plain SQL.
3. **`quorum` — the SafeKeeper-class backend (future).** Here Neon changes
   the math: the article's SafeKeeper **is Apache-2.0 Rust in
   `neon/safekeeper/`** — the proposer–acceptor consensus
   (`safekeeper.rs`), persisted acceptor state (`control_file.rs`), WAL
   segment storage, and the full protocol spec. It is deeply shaped around
   Postgres WAL framing (`postgres_ffi`, LSNs, timelines), so the realistic
   reuse is a **fork that swaps the record format for tail entries** while
   keeping the consensus state machine, term history, and recovery logic
   verbatim — plus `desim`-style simulation tests. This is a large but
   de-risked lift; it exists so deployments that reject the Postgres
   dependency still get node-loss durability. An S3-Express segment backend
   stays on the list for fully-serverless tails.

### Exactly-once across crashes: the watermark lives in the lake

Every flush commit stamps the tail sequence it drained into the snapshot
summary (`icegres.tail-seq = <n>`). That single property makes the whole
protocol reconcilable from the lake alone:

- **Recovery**: a new flush leader reads the table's current snapshot chain,
  finds the highest `icegres.tail-seq`, truncates the tail to it, replays
  the remainder into the buffer. A crash between commit and truncate cannot
  double-apply; a crash before commit cannot lose an acked row.
  (Moonlink's `table_handler.rs` LSN taxonomy — commit/flush/
  iceberg-snapshot/persisted LSNs — is the same reconciliation problem
  solved with the same "the lake carries the watermark" answer; use it as
  the design cross-check.)
- **Foreign writers (I2)**: their commits carry no watermark and simply
  interleave; the flusher retries against fresh metadata exactly as
  `overwrite.rs` does today.
- **Scan overlay simplifies**: a scan's overlay is "tail rows with
  `seq > tail-seq(scan's metadata)`" — the existing generation-GC dance
  becomes a comparison against a number the metadata itself carries.

### What Phase 1 closes

Durable ~1–3 ms write acks with **zero loss on unclean kill** (e2e: SIGKILL
mid-window, replay, assert every acked row present — the inversion of
today's documented-loss test); fleet-wide union reads (any icegres compute
overlays the shared tail, closing the "buffered rows are process-local"
gap); and the ack path detaches from the ~15–20 commits/s/table catalog
ceiling (acks scale with the tail store, commits stay at cadence).
Freshness for *foreign* engines remains commit-cadence (≤N ms) — which is
exactly what LTAP offers third-party readers too: its LSN tail-merge is a
protocol only Databricks' own engines speak. Parity, not deficit.

## 4. Phase 2 — hot rows: PK upserts on the tail

With a durable shared tail, hot-row traffic stops being a COW problem:

- Tables opt in via the existing `icegres.primary-key` property (+
  `icegres.tail = true`). `INSERT`/`UPDATE ... WHERE pk = k`/`DELETE` on
  such a table write a keyed version to the tail and ack durably in ~1–3 ms.
  Per-key last-writer-wins inside the window; the `postgres` backend's row
  locks give atomic read-modify-write across computes for free.
- The flusher **coalesces per key** and applies one batched commit per
  window through the existing COW overwrite path — N updates to one row
  become one file rewrite per window instead of N serialized ~55 ms commits
  with `40001` storms.
- Reads merge lake + tail by key (point lookups check the in-memory tail
  mirror first — it is small by construction; scans dedupe by PK against
  the overlay).
- **Semantics shift, documented (I4)**: tail tables trade
  snapshot-isolation-at-commit for per-key last-writer-wins within the
  window; explicit `BEGIN…COMMIT` keeps today's fence-flush-then-sync path.
  Foreign concurrent writes to the same keys resolve by commit order, as
  Iceberg always has.

*Reuse:* moonlink's `mooncake_table` is the production reference for this
exact pipeline — `mem_slice` (the keyed buffer), `delete_vector` +
`storage/index/mem_index.rs` (key → position for deletes), `disk_slice` →
`iceberg_table_syncer.rs` (batched materialization), `union_read` (the
merge). Its `storage/table/iceberg/deletion_vector.rs` proves iceberg-rust
(at a git revision) can write v3 deletion vectors — the evidence that
icegres' matrix bump will unlock the cheaper merge-on-read apply path.
Until then, COW-batched-per-window is correct and ships first. All of this
is study-and-reimplement per §2.3; `moonlink_datafusion`'s union-read
`TableProvider` additionally confirms the overlay composes cleanly with a
DataFusion engine — the same engine icegres embeds.

This retires the two loudest anti-patterns in `cqrs-topology.md` — hot-row
contention and the single-table commit ceiling — and with them most of the
reason Tier 1 (the external Postgres) exists. Tier 1 remains the honest
answer only for `SELECT … FOR UPDATE`-style lock choreography and true
sub-ms SLOs; where Tier 1 does survive, `moonlink_connectors/pg_replicate`
is the reference for its Postgres→Iceberg stream (or run Moonlink itself —
the BSL permits a deployer's internal use).

## 5. Phase 3 — multi-table atomicity and whole-lakehouse branches

The comparison doc scores Lakebase's whole-database branching and icegres'
`40003` multi-table caveat as two separate gaps. They share one fix, and it
is cheaper than it looks: the **Iceberg REST spec's multi-table transaction
endpoint (`POST /v1/{prefix}/transactions/commit`)**, which Lakekeeper —
already the assumed catalog — implements. The pinned iceberg-rust 0.9.1
doesn't surface it, but icegres already speaks raw authenticated REST to
the catalog; building the `CommitTransactionRequest` (per-table
`assert-ref-snapshot-id` requirements + updates) directly is a contained
addition. (First step: verify endpoint behavior against the deployed
Lakekeeper version in e2e, then adopt.)

- **Atomic multi-table COMMIT**: a transaction touching N tables becomes
  one all-or-nothing catalog request — `40003` and `ICEGRES_TXN_STRICT`
  retire; `40001` semantics stay (the requirements express exactly the
  pins). Falls back to today's ordered-commit path on catalogs without the
  endpoint, preserving I3.
- **Whole-lakehouse branches**: `icegres branch create-all <name>` sets the
  ref on every table in one atomic transaction — a consistent cross-table
  cut, which is the branch unit Lakebase brags about. `serve --branch` and
  `icegresd`'s `icegres@<branch>` routing already work per-ref and need no
  change. Tags give the same for PITR-style restore points; snapshot-expiry
  already preserves ref-reachable history.
- PITR granularity: with tail-mode cadence commits, snapshot granularity
  *is* the flush window — "any LSN" vs "any N-ms snapshot" ceases to be a
  meaningful difference. (Neon's LSN-addressed `timeline/` machinery is the
  maximalist alternative and exactly what I1 forbids importing: it would
  make history live outside the lake.)

## 6. Phase 4 — table health: compaction and orphan GC

Independent of Lakebase parity, sustained writes need it (documented
anti-pattern; today's answer is drop-and-reseed). Order of attack: (a) the
tail already fixes the *source* — cadence commits produce well-sized files
instead of per-INSERT confetti; (b) bin-pack rewrite lands when the pinned
matrix gains a replace-files action (moonlink's `storage/compaction/` and
Neon's `pageserver-compaction.md` are the reference designs; manifest
surgery on the custom COW path was already rejected as
correctness-critical); (c) orphan-file GC after snapshot expiry via
object-store listing against live manifests, or a documented external
`remove_orphan_files` run. The pinned matrix moves as a unit behind a full
gate run, per `limitations.md`.

## 7. Phase 5 (deferred) — secondary index tier

The architecture study's §7.4 global index (key → file/row-group postings,
refreshed per snapshot) now has a concrete reference: moonlink's
`storage/index/persisted_bucket_hash_map.rs` (the GlobalIndex) layered over
`mem_index`. It is deliberately last: measured point lookups are already
6.9 ms at current scale without any index object, and Phases 1–2 change the
write path that index must track. Build it against real scale evidence, not
against Lakebase's feature list.

## 8. Non-goals (the moat, restated)

- **No Postgres engine, types, or extensions.** Bit-exact `NUMERIC`
  overflow fields and extension ecosystems are the reward for being
  Postgres; chasing them converts icegres into a worse Lakebase. The
  Arrow/Iceberg type system stays the contract.
- **No authoritative row tier.** However capable the tail becomes, anything
  older than the flush window lives only in the lake (I1). The moment the
  tail holds unbounded history, icegres has re-invented the monolith the
  article opens by burying. This is also the line that keeps Neon's
  pageserver — for all its Apache-2.0 reusability — a quarry for parts
  (caching, layer/compaction ideas, `remote_storage`, test harnesses)
  rather than a component: its entire purpose is to *be* the authoritative
  versioned store.
- **No distributed query engine.** 100 GB+ scans remain Trino/Spark's job,
  reading the same tables (I2). LTAP's own thesis — best engine per job
  over shared storage — argues for this, not against it.
- **No transaction pooling** in icegresd (session state makes it unsafe by
  construction — unchanged).

## 9. Scorecard after Phases 1–3

| Lakebase advantage (comparison §4) | status after roadmap |
|---|---|
| quorum-WAL durable low-latency writes | **matched in class**: ~1–3 ms durable acks, zero-loss on kill (durability delegated to the tail store's replication; a safekeeper-fork quorum backend as the endgame) |
| hot-row OLTP | **matched for upsert-shaped traffic** (PK tail, per-key LWW, one commit/window); lock-choreography workloads still Tier 1 |
| cross-engine freshness | **matched where LTAP is real** (fleet-wide via shared tail; foreign engines at commit cadence — same as LTAP's non-Databricks readers) |
| whole-DB branches / any-LSN PITR | **matched**: atomic whole-lakehouse refs; PITR at flush-window granularity |
| Postgres fidelity/extensions | **conceded by design** (non-goal) |
| managed compaction | **partially closed** (Phase 4; source fixed by cadence commits) |

| icegres advantage (comparison §5) | preserved by |
|---|---|
| plain Iceberg, foreign-writable | I1/I2: tail is transient + watermark-reconciled; flusher retries around foreign commits |
| lake-native writes, no Postgres in the path | tail is icegres-internal machinery *under* the same SQL surface |
| single binary, self-host, 0.3 s start | I3: everything opt-in; `local` backend needs zero infra; `postgres` backend reuses the catalog's instance |
| embedded interactive analytics + ADBC | untouched; tail overlay plugs into the existing scan path on both protocols |
| reproducible claims | each phase lands with bench deltas + e2e proofs (I4); `remote_storage`-style fault injection extends the harness |
| clean licensing for open-core | Apache-2.0 reuse only in core (Neon); BSL material (moonlink) is design reference, feature-gated option, or post-change-date adoption (§2.3) |

The one-line strategy: **Lakebase proved which organ was missing — and
published half of it as Apache-2.0 code; icegres adds that organ with the
chain of command inverted** — the tail serves the lake instead of the lake
serving the row store. That is the whole difference between catching up to
Lakebase and becoming it.
