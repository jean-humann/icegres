# SOTA roadmap — closing the Lakebase gaps without giving up the lake

> Design study for the next phases of icegres, written against the gap
> analysis in `lakebase-ltap-vs-icegres.md` §4/§5. Goal: absorb what
> Lakebase/LTAP genuinely does better — durable low-latency writes, hot-row
> updates, fleet-wide freshness, whole-database branching — **without
> surrendering any of the properties Lakebase cannot offer**: plain Iceberg
> tables writable by any engine, one authoritative copy, a single
> self-hostable binary, embedded interactive analytics, and reproducible
> claims.

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

### 1.1 `TailStore` — one trait, pluggable durability

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
   replication, delegated rather than rebuilt.
3. **Future backends, same trait:** an embedded-raft trio (`openraft`) for
   deployments that refuse a Postgres dependency; S3 Express One Zone
   segments for a fully serverless tail. Neither blocks Phases 1–2.

Purist note, stated up front (I4): the tail *is* a second physical location
for ≤flush-window data — the same bounded-staging nuance LTAP accepts in
SafeKeeper/PageServer ("retained only until materialized"). The contract
that preserves I1 is truncation-at-commit plus the watermark below.

### 1.2 Exactly-once across crashes: the watermark lives in the lake

Every flush commit stamps the tail sequence it drained into the snapshot
summary (`icegres.tail-seq = <n>`). That single property makes the whole
protocol reconcilable from the lake alone:

- **Recovery**: a new flush leader reads the table's current snapshot chain,
  finds the highest `icegres.tail-seq`, truncates the tail to it, replays
  the remainder into the buffer. A crash between commit and truncate cannot
  double-apply; a crash before commit cannot lose an acked row.
- **Foreign writers (I2)**: their commits carry no watermark and simply
  interleave; the flusher retries against fresh metadata exactly as
  `overwrite.rs` does today.
- **Scan overlay simplifies**: a scan's overlay is "tail rows with
  `seq > tail-seq(scan's metadata)`" — the existing generation-GC dance
  becomes a comparison against a number the metadata itself carries.

### 1.3 What Phase 1 closes

Durable ~1–3 ms write acks with **zero loss on unclean kill** (e2e: SIGKILL
mid-window, replay, assert every acked row present — the inversion of
today's documented-loss test); fleet-wide union reads (any icegres compute
overlays the shared tail, closing the "buffered rows are process-local"
gap); and the ack path detaches from the ~15–20 commits/s/table catalog
ceiling (acks scale with the tail store, commits stay at cadence).
Freshness for *foreign* engines remains commit-cadence (≤N ms) — which is
exactly what LTAP offers third-party readers too: its LSN tail-merge is a
protocol only Databricks' own engines speak. Parity, not deficit.

## 2. Phase 2 — hot rows: PK upserts on the tail

With a durable shared tail, hot-row traffic stops being a COW problem:

- Tables opt in via the existing `icegres.primary-key` property (+
  `icegres.tail = true`). `INSERT`/`UPDATE ... WHERE pk = k`/`DELETE` on
  such a table write a keyed version to the tail and ack durably in ~1–3 ms.
  Per-key last-writer-wins inside the window; the `postgres` backend's row
  locks give atomic read-modify-write across computes for free.
- The flusher **coalesces per key** and applies one batched commit per
  window through the existing COW overwrite path — N updates to one row
  become one file rewrite per window instead of N serialized ~55 ms commits
  with `40001` storms. (Iceberg v3 deletion vectors are the cheaper apply
  path once the pinned matrix can express them; COW-batched is correct and
  ships first.)
- Reads merge lake + tail by key (point lookups check the in-memory tail
  mirror first — it is small by construction; scans dedupe by PK against
  the overlay).
- **Semantics shift, documented (I4)**: tail tables trade
  snapshot-isolation-at-commit for per-key last-writer-wins within the
  window; explicit `BEGIN…COMMIT` keeps today's fence-flush-then-sync path.
  Foreign concurrent writes to the same keys resolve by commit order, as
  Iceberg always has.

This retires the two loudest anti-patterns in `cqrs-topology.md` — hot-row
contention and the single-table commit ceiling — and with them most of the
reason Tier 1 (the external Postgres) exists. Tier 1 remains the honest
answer only for `SELECT … FOR UPDATE`-style lock choreography and true
sub-ms SLOs.

## 3. Phase 3 — multi-table atomicity and whole-lakehouse branches

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
  meaningful difference.

## 4. Phase 4 — table health: compaction and orphan GC

Independent of Lakebase parity, sustained writes need it (documented
anti-pattern; today's answer is drop-and-reseed). Order of attack: (a) the
tail already fixes the *source* — cadence commits produce well-sized files
instead of per-INSERT confetti; (b) bin-pack rewrite lands when the pinned
matrix gains a replace-files action (tracked; manifest surgery on the
custom COW path was already rejected as correctness-critical); (c)
orphan-file GC after snapshot expiry via object-store listing against live
manifests, or a documented external `remove_orphan_files` run. The pinned
matrix moves as a unit behind a full gate run, per `limitations.md`.

## 5. Phase 5 (deferred) — secondary index tier

The architecture study's §7.4 global index (LSM of key → file/row-group
postings on NVMe, refreshed per snapshot) is the right shape *when data
volume demands it*. It is deliberately last: measured point lookups are
already 6.9 ms at current scale without any index object, and Phases 1–2
change the write path that index must track. Build it against real scale
evidence, not against Lakebase's feature list.

## 6. Non-goals (the moat, restated)

- **No Postgres engine, types, or extensions.** Bit-exact `NUMERIC`
  overflow fields and extension ecosystems are the reward for being
  Postgres; chasing them converts icegres into a worse Lakebase. The
  Arrow/Iceberg type system stays the contract.
- **No authoritative row tier.** However capable the tail becomes, anything
  older than the flush window lives only in the lake (I1). The moment the
  tail holds unbounded history, icegres has re-invented the monolith the
  article opens by burying.
- **No distributed query engine.** 100 GB+ scans remain Trino/Spark's job,
  reading the same tables (I2). LTAP's own thesis — best engine per job
  over shared storage — argues for this, not against it.
- **No transaction pooling** in icegresd (session state makes it unsafe by
  construction — unchanged).

## 7. Scorecard after Phases 1–3

| Lakebase advantage (comparison §4) | status after roadmap |
|---|---|
| quorum-WAL durable low-latency writes | **matched in class**: ~1–3 ms durable acks, zero-loss on kill (durability delegated to the tail store's replication) |
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
| reproducible claims | each phase lands with bench deltas + e2e proofs (I4) |

The one-line strategy: **Lakebase proved which organ was missing; icegres
adds that organ with the chain of command inverted** — the tail serves the
lake instead of the lake serving the row store. That is the whole
difference between catching up to Lakebase and becoming it.
