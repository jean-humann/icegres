# Scope: sub-10 ms durable writes — measure, optimize, and bless the tail path

Target: /home/user/icegres. Prerequisite: the sub-5 ms read increment
(--freshness-ms + plan cache) must be COMMITTED first — item 3 rides its
machinery. Physics framing (goes in the docs): an Iceberg commit is
several object-store PUTs + a catalog POST and can never ack < 10 ms on
real S3 — sub-10 ms durable writes MEAN the tail path (the same trick as
Lakebase's SafeKeeper ack). This increment makes that path complete,
measured, and first-class.

## 1. Write-path instrumentation (first, like the read budget)

Extend the ICEGRES_QUERY_TIMING mechanism to the write hot paths:
- sync commit: DML plan/apply, Parquet encode, data-file PUT(s), manifest/
  manifest-list PUTs, catalog POST, retry loops — per-stage p50s.
- tail ack: encode, fsync (local) / INSERT+commit (pg) / quorum round trip
  (proposer send -> 2/3 AppendResp), buffer bookkeeping.
Report measured budgets for: sync INSERT, batch-100 INSERT, buffered
INSERT on all THREE tail backends, keyed UPDATE. Zero cost when unset.

## 2. Tail ack optimization + bench legs

- Local WAL group-fsync: concurrent appends within a small window (e.g.
  coalesce whatever is queued when the current fsync completes — natural
  batching, no timer) share one sync_data; per-statement frames unchanged;
  ordering/seq invariants preserved (frames still written in seq order
  under the existing lock discipline). Expect ~3.2 -> ~1-2 ms under
  concurrency, unchanged single-writer latency (never WORSE than today —
  gate on that).
- Quorum: verify appends to the three acceptors are pipelined (next batch
  sent before previous acks) and the ack path has no needless await
  serialization; measure localhost ack p50.
- bench/bench.sh: add an ungated extra section reporting durable-ack p50s
  for the three backends (start/stop its own icekeeperd trio like the
  durability script; keep it clearly labeled and cheap), so the write
  ladder lives in results JSON.

## 3. Keyed RMW fast path (< 7 ms keyed writes)

- The 9.5 ms keyed UPDATE is ~7 ms union-view RMW read + tail fsync. Use
  the new read machinery: the RMW read goes through the freshness-cached
  provider + plan-cache path when enabled; ALSO add a keyed-map shortcut —
  if the key's current version is already in the keyed map or pending
  window (upsert row available in memory), skip the engine read entirely.
- Measure: keyed UPDATE p50 with --freshness-ms 25 + warm plan cache;
  target < 7 ms, gate at < 10 ms.

## 4. Sync-path cheap wins (honest target ~30-40 ms, NOT sub-10)

- Parallelize independent uploads inside one commit (data file(s) +
  manifest artifacts) where ordering permits; confirm/enable HTTP
  keep-alive + connection reuse for both the catalog client and the S3
  path (check what reqwest/opendal/iceberg FileIO already pool — measure,
  don't assume); eliminate any redundant load_table/metadata fetch inside
  the commit sequence (instrument first).
- Gate: sync insert p50 must improve or hold; NO durability/atomicity
  semantics change; conflict/retry behavior unchanged.

## 5. Docs: the write-latency ladder

README (root + icegres/) gain a measured write-ladder table: path,
durable-ack p50, durability class, cross-server/engine visibility,
semantics trade. limitations.md: explicit transactions REMAIN sync —
tail-staged COMMIT would ack before conflict detection and break 40001;
this trade is refused, documented. cqrs-topology + roadmap ledger updated
with the new numbers.

## Gates
Full ladder as always: fmt/clippy -D warnings; cargo test --release (PG
env); FULL tests/tail_durability.sh (71 baseline must hold; group-fsync
must not break any kill -9 proof) and tests/e2e.sh; bench A/B vs the
preserved pre-change binary (default paths regress nothing) + the new
write-ladder section results; the keyed/tail targets above measured and
reported. Zero new dependencies. Adversarial review round on the
group-fsync ordering/durability (the fsync-before-ack invariant must be
provably intact for every statement in a coalesced batch) and on the
sync-path parallelization (commit atomicity: nothing may become visible
to the catalog before ALL its artifacts are durable on S3 — upload
ordering constraints of Iceberg commits must be verified against the
spec, not assumed).
