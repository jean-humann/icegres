# Scope: P2 — the dependency-matrix bump (deletion vectors + compaction)

Roadmap-v2 P2 (docs/roadmap-v2-beyond-lakebase.md §P2). The pinned
iceberg-rust 0.9.1 gates the two biggest remaining economics items vs
Lakebase: hot-row flush cost (COW file rewrites) and table compaction.
One PR, two hard-gated stages with independent revert points.

## Stage 0 — recon (binding)

Determine the TARGET MATRIX with evidence, not hope:
- The newest iceberg-rust (crates.io release preferred; an exact git-rev
  pin like moonlink's is acceptable if no release suffices — Apache-2.0,
  exact pins only) that has: positional/deletion-vector (puffin) WRITE
  support, replace-files/rewrite-files transaction actions, and a read
  path that applies deletes through iceberg-datafusion.
- The datafusion + arrow pair that iceberg-datafusion rev requires, the
  datafusion-postgres release matching that datafusion (pgwire layer),
  sqlparser alignment, arrow-flight/tonic compatibility, opendal (GC)
  compatibility, and the MSRV — the pinned toolchain 1.96.1 may bump if
  and only if the matrix requires it (rust-toolchain.toml stays exact).
- Existence proof harvest: moonlink pins iceberg-rust git rev 4a6ea15
  (arrow 56 / datafusion 50) for its DV path — an OLDER matrix than ours,
  so it proves the FEATURE exists in-tree, not which version we want.
  Map the API entry points it uses (DV write, puffin, rewrite) to their
  shape at the target rev.
- Output: the exact pin set, the API-churn map for every icegres call
  site (overwrite.rs transaction paths, scan/provider integration,
  catalog REST types, maintain.rs snapshot APIs), and a go/no-go per
  stage-2 feature (DV flush, compact, native multi-table txn).

## Stage 1 — the bump alone (its own revert point)

Apply the target matrix with ZERO feature/behavior change. Every call
site mechanically migrated; semantics identical. Gate: the FULL ladder —
fmt/clippy -D warnings; cargo test --release (live env); tail_durability
(71); FULL e2e (191); bench A/B vs the preserved pre-P2 baseline binary
(scratchpad icegres-pre-p2 + bench-20260711T223700Z.json), three-way
drift control if deltas exceed noise. Any unfixable regression ⇒ revert
the bump; the PR dies here honestly. No stage-2 work may begin before
stage 1 is fully green.

## Stage 2 — the three unlocks (each gated, in this order)

### 2a. Merge-on-read keyed flushes
Keyed tail windows (icegres.tail-upsert tables) flush as deletion
vectors + appended data files instead of COW rewrites of every touched
file. Requirements:
- The watermark/property protocol is UNTOUCHED: same atomic commit stamps
  icegres.tail-seq.<id>; exactly-once semantics and the tail durability
  suites must pass unchanged.
- Reads apply DVs everywhere rows are served: local scans, time travel,
  branches, the tail API union, peer mirrors, foreign readers (I2 — a
  DV-writing table must stay readable/writable by other engines; verify
  with the harness's existing foreign-engine legs).
- Fallback: tables/writers where DV write is unsupported keep COW —
  per-table opt-in via property (icegres.flush-mode=dv|cow, default cow
  in this PR; flipping the default is a later decision) so the default
  path stays byte-identical (I3).
- Orphan GC (maintain remove-orphans) MUST recognize puffin/DV files as
  live — extend the manifest walk before any DV write ships; add an e2e
  leg proving GC never deletes a live DV.
- New bench metric keyed_flush_ms at two table sizes (small + after a
  10× data load): the DV curve must be ~flat where COW scales with size.

### 2b. maintain compact (bin-pack)
Data-file bin-pack via the rewrite/replace-files action: combine
under-sized files per partition, one atomic commit, snapshot lineage
preserved, dry-run default like remove-orphans, size threshold flags.
e2e: row-set identical pre/post, foreign reader agrees, orphan GC +
snapshot expiry interplay, compaction of a DV-bearing table preserves
delete semantics (or refuses loudly if the lib cannot — honest scope).
Bench: fragmented-table scan p50 restored to near-baseline post-compact.

### 2c. Native multi-table transactions (conditional)
Only if the target rev ships multi-table/transactions commit support:
prove byte-equivalence with the raw-REST shim (same REST bodies against
Lakekeeper, same conflict semantics incl. the capability probe), then
swap and delete the shim. Otherwise: document and keep the shim — not a
failure.

## Constraints
Invariants I1–I4. Exact pins only (crates.io version or git rev). No new
runtime dependencies beyond what the bumped libs pull. Default behavior
byte-identical (DV mode opt-in). Neon/moonlink code may be studied;
moonlink is BSL 1.1 — study-only, no code copying (docs/sota-roadmap.md
§2 posture unchanged).

## Gates (per stage, all foreground)
fmt/clippy -D warnings → cargo test --release (live) → tail_durability
71 → FULL e2e (191 + new legs) → bench A/B vs icegres-pre-p2 with drift
control → a11 ADBC probe green → parity.sh green. Fix-or-revert per
house rule at every stage boundary.
