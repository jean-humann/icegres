# Lakebase feature gap audit — second pass, after roadmap phases 1–4

> Re-audit of every capability described in the Databricks post ["From
> monolith to Lakebase to LTAP"](https://www.databricks.com/blog/lakebase-ltap-rethinking-database-storage)
> against icegres **as merged** (PR #2, phases 1/1b/2/3/4 of
> `sota-roadmap.md`). Companion to `lakebase-ltap-vs-icegres.md` (written
> before any phase shipped) and the roadmap's §10 status ledger. Second
> question answered explicitly in §3: **what, concretely, was reused from
> Neon and Moonlink — code or concepts.**

---

## 1. Feature-by-feature audit

Legend: ✅ equivalent shipped · 🟡 partial (gap stated) · ❌ missing ·
⛔ non-goal by design (roadmap §8 invariants — the lake stays the only
source of truth, foreign writers keep working).

| # | Lakebase/LTAP capability (per the article) | icegres status after phases 1–4 |
|---|---|---|
| 1 | Real Postgres engine — full SQL, drivers, **extensions** work as-is | 🟡/⛔ pgwire + `pg_catalog` emulation verified against psql/ORMs/JDBC/ODBC/ADBC, but not Postgres: no extensions, no server-side cursors, extended-protocol SELECT limits in transactions. Full fidelity is a declared non-goal (becoming Postgres = becoming a worse Lakebase). |
| 2 | SafeKeeper: commit = **Paxos-quorum** WAL ack; no single node's loss loses data | 🟡 The durable tail covers buffered writes: `--tail-dir` = this-disk durability (~3.2 ms ack), `--tail-url` = node-loss durability *delegated* to one Postgres instance's own fsync/replication. **No quorum consensus of our own** — the safekeeper-fork quorum backend is designed (roadmap §3 backend 3) and not started. Sync-path durability is the Iceberg commit itself (~50–80 ms), which needs no WAL tier. |
| 3 | PageServer: WAL→page materialization, GetPage@LSN, page reconstruction | ⛔ No page tier exists to materialize — icegres data is *born* columnar in the lake. This is the deliberate architecture inversion (invariant I1), not an unbuilt feature. |
| 4 | Read-cache hierarchy: buffer pool → **local NVMe file cache** → PageServer → object store | ❌ Real gap. icegres has snapshot-aware *metadata/manifest* caching + the OS page cache, but no managed local cache tier for Parquet footers/column chunks (the foyer-style NVMe cache from the architecture study §7.4). Matters as data outgrows RAM; invisible at current bench scale. |
| 5 | Unlimited storage (data in object storage) | ✅ Same substrate by construction. |
| 6 | Serverless, elastic compute; scale to zero; instant wake | 🟡 Scale-to-zero + wake-on-connect shipped and measured (~73–85 ms wake, 0.4 ms warm-pool connects via `icegresd`). **No autoscaling** (scale up/out under load) and `icegresd` itself is a single unsupervised process — the control plane is minimal by scope. (Note: Neon's autoscaling agent is proprietary control-plane code too — study, refuted claim #2.) |
| 7 | Durable writes / zero data loss | 🟡 Sync path: yes (the commit is the durability event). Buffered path: closed by the tail (disk- or node-loss-class, backend-dependent), proven by 50 kill -9 e2e assertions. No quorum class (see #2). |
| 8 | Simpler HA: durable state in a replicated layer; failover without promoting a physical copy | 🟡 Stateless computes make replacement trivial and the durable tier (catalog + object store, plus the PG tail) delegates its own HA. But there is no automated failover orchestration: `icegresd` supervises crashes on one box only, and a tail-writer host death needs the documented (fast, but manual-ish) advisory-lock takeover. |
| 9 | Instant branching/cloning — metadata-only, whole database, seconds | ✅ Per-table refs + **whole-lakehouse `branch create-all`/`drop-all`** (one atomic multi-table commit, consistent-or-nothing, nested namespaces included). Granularity note: a branch cut is at snapshot boundaries, not an arbitrary WAL LSN. |
| 10 | Point-in-time recovery to any moment | 🟡 Time travel to any *retained snapshot*; in buffered mode snapshot cadence ≈ the flush window, so granularity converges on N ms. No arbitrary-LSN replay between snapshots (needs WAL history — see #2/#3). |
| 11 | Read replicas without physical copies | 🟡 Any number of stateless serving computes over the same lake = replicas with no copy and no replication lag mechanism. Gap: **fleet-shared tail overlays** are not built (roadmap §3 backend-2 "next increment") — replicas see a buffering peer's un-flushed rows only at commit cadence. |
| 12 | LTAP single copy: row-authoritative data transcoded to Parquet, bit-exact Postgres types, overflow field, hidden MVCC versions | ⛔ Achieved by inversion, not imitation: there is exactly one copy because there was never a row copy. No transcoder, no overflow field, no hidden row versions — and none needed. Foreign engines can also *write* our copy, which LTAP's Postgres-owned lake tier does not allow. |
| 13 | LTAP freshness: analytics asks for the LSN, merges the un-materialized tail | 🟡 Sync writes are immediately visible to every engine (the commit *is* the lake write — no freshness gap exists to merge). Buffered/keyed rows: same-process union reads only; other icegres computes and foreign engines wait ≤ flush-window. LTAP's merge protocol is also proprietary-engine-only — third-party readers of LTAP tables see materialized data, same as ours. Fleet-wide overlay = the open half. |
| 14 | Postgres indexes (point reads via btree, rebuilt from the cache tier) | ❌ Deferred by design (roadmap §7): pruning + manifest caching deliver 6.9 ms point lookups at current scale; the key→row-position index tier waits for scale evidence. |
| 15 | Hot-row OLTP (MVCC row updates without rewrite storms) | 🟡→✅ for the upsert shape: keyed tail upserts ack exact-PK UPDATE/DELETE in 9.5 ms p50 (vs 71 ms), one coalesced commit per window, no `40001` storms — single-compute only. Lock choreography (`SELECT … FOR UPDATE`, queues) remains Tier-1 external Postgres territory. |
| 16 | Managed maintenance (compaction, GC) | 🟡 Snapshot expiry + fail-closed orphan GC shipped; **bin-pack compaction still gated** on the pinned iceberg-rust matrix (the small-file *source* was fixed by cadence commits). |
| 17 | 5× write throughput / 2× read latency (SafeKeeper+PageServer claim) | n/a — engine-specific vendor claim; icegres' own measured ledger lives in `bench/` and roadmap §10. |

**Net:** the four gaps the roadmap targeted (durable low-latency writes,
hot rows, whole-DB branching, multi-table atomicity) are closed in their
designed scope. The honest remaining list, in priority order:

1. **Fleet-shared tail overlay** (#11/#13) — LISTEN/NOTIFY mirror over the
   PG tail; designed, not started. Biggest freshness delta vs LTAP.
2. **Quorum tail backend** (#2/#7) — the Neon-safekeeper fork; designed,
   not started. Upgrades buffered durability from delegated to consensus.
3. **Local Parquet cache tier** (#4) — unaddressed by any phase; becomes
   the dominant read-latency factor beyond page-cache scale.
4. **Autoscaling / HA control plane** (#6/#8) — beyond `icegresd`'s scope.
5. **Compaction** (#16) — blocked on the dependency matrix, tracked.
6. **Index tier** (#14) — deferred until scale evidence, on purpose.
7. Arbitrary-point PITR (#10) and Postgres fidelity (#1) — non-goals in
   their full form; revisit only if the invariants change.

## 2. Did we implement the *full* Lakebase feature set?

No — and the roadmap never aimed to. Three feature families were
**translated** (same job, inverted architecture: tail-before-commit
instead of authoritative WAL/pages), two were **matched** (branching,
scale-to-zero economics), and the families that only make sense inside a
row-authoritative engine (PageServer, page caches, LSN-addressed history,
extensions) are non-goals guarded by invariant I1. Everything still open
is listed above and in roadmap §10 — nothing is silently absent.

## 3. Neon / Moonlink reuse — the explicit ledger

**Direct answer: zero lines of Neon (or Moonlink) code are vendored,
forked, or linked into icegres today.** All five shipped phases are
icegres-native Rust. What was actually taken, and what remains available:

| source | what we used (shipped) | what we did NOT use (open) |
|---|---|---|
| **neon** (Apache-2.0, clone at `~/neon`) | *Concepts and specs only*: the branch-per-endpoint model (pre-dating this work), `wal_storage.rs`'s segment/fsync discipline as the design reference for the local tail's WAL layout, `docs/safekeeper-protocol.md` to shape the durability review attacks. | **No code.** The three concrete reuse items stay open: (a) `libs/remote_storage` timeout/retry port for the object-store gap (roadmap §2.2 — STATUS: not shipped); (b) `libs/desim`-style deterministic simulation tests; (c) the `safekeeper/` **fork** as the quorum `TailStore` backend (§3 backend 3 — the one item where wholesale code reuse is the plan, since the consensus state machine is the hard, verified part). |
| **moonlink** (BSL 1.1, clone at `~/moonlink`) | *Design blueprint only, by license posture*: the statement-atomic frame/WAL shape (`storage/wal.rs`), the union-read pattern (already independently present in `buffer.rs`), the LSN-taxonomy reconciliation idea cross-checked during the watermark design, `mooncake_table`'s keyed mem-slice → coalesced-commit pipeline as the Phase-2 reference. | **No code, deliberately**: BSL forbids managed-service derivatives (licensor: Databricks) and its matrix (arrow 56 / datafusion 50 / iceberg git-rev) cannot link against icegres' pinned arrow 57 / datafusion 52 / iceberg 0.9.1 anyway. Its `deletion_vector.rs` remains the evidence for what a future matrix bump unlocks (merge-on-read applies). |

Why this is the right posture rather than a shortcut missed: Neon's
reusable crates implement the *authoritative-storage* architecture —
pageserver/safekeeper own history. Everything icegres shipped subordinates
staging state to the lake, so the shapes differ even where the job rhymes;
lifting code wholesale would have imported the inversion we exist to
avoid. The one place the architectures genuinely coincide — a replicated
log service with leader takeover — is exactly the item where the roadmap
prescribes forking `neon/safekeeper` rather than rewriting it, and that
item has not started.

## 4. Verification trail for this audit

Phase gates as merged: 141/141 unit tests, 163-assertion e2e,
50/50 `tail_durability.sh` (both backends, kill -9 / double-crash /
seq-floor proofs), per-phase A/B benchmarks with drift-controlled
comparisons (`bench/`), measured numbers in roadmap §10. Every 🟡/❌
above cites its roadmap STATUS line; none contradicts the shipped docs.
