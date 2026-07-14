# Scope: P2 — re-scoped after recon: `maintain compact` at the current pin

> Original scope: bump the pinned matrix past iceberg-rust 0.9.1 to unlock
> deletion-vector keyed flushes, bin-pack compaction, and native
> multi-table transactions. Stage-0 recon (binding, evidence-first)
> falsified the premise, so this document records the verdict and the
> re-scoped increment. The gate discipline working as designed: the bump
> died at the recon gate, before any churn.

## Stage-0 verdict (2026-07-14, apache/iceberg-rust main @ 85c365f5)

**No rev of iceberg-rust — 0.9.1 (crates.io max), v0.10.0-rc.3, or main —
delivers any of the three features.** The bump has zero payload and is
skipped; every pin stays exactly where it is (iceberg =0.9.1, datafusion
=52.5.0, arrow =57.3.1, datafusion-postgres =0.15.0, toolchain 1.96.1).

- **Deletion-vector flushes (2a): blocked upstream, both directions.**
  Read is the hard blocker: the delete-file loader cannot apply puffin
  deletion vectors (`caching_delete_file_loader.rs:54` "TODO: Delete
  Vector loader from Puffin files"; the dataflow diagram marks Del Vec
  "Not yet Implemented") — icegres could never read its own DVs,
  violating this repo's I2 (foreign readers) and the scope's
  "reads apply DVs everywhere" bar. Write: no DV/position-delete writer
  exists; `fast_append` hard-rejects delete content; `PuffinWriter` hides
  the per-blob offsets `DataFile.content_offset/content_size_in_bytes`
  require. Moonlink (BSL, study-only) proves the gap the hard way: it
  unsafe-transmutes `PuffinWriter` to reach private fields and ships its
  own non-iceberg-datafusion read path. Not a road we take.
- **Rewrite/replace-files action (for 2b): absent at every rev** (the
  transaction layer has fast_append, expire_snapshots, replace_sort_order
  and update_* only). But icegres does not need it: `overwrite.rs`
  already hand-builds EXISTING/DELETED/ADDED manifests, the manifest
  list, the Snapshot, and commits over raw REST — and `Operation::Replace`
  exists at 0.9.1. **Compaction is GO at the current pin.**
- **Native multi-table transactions (2c): absent at every rev** (the
  Catalog trait commits one table at a time; the REST client has no
  `/transactions/commit`). Our raw-REST shim stays — per the original
  scope, "not a failure".
- Re-check trigger: revisit the bump when a crates.io release ships DV
  write + puffin-DV read application (watch `caching_delete_file_loader`)
  or a rewrite action. A candidate churn map for the v0.10 line (API
  renames, datafusion 53.1/arrow 58.3/datafusion-postgres 0.16 pairing,
  MSRV all-clear at 1.96.1) is recorded in the session recon log so the
  future bump starts from a worksheet, not from scratch.

## The increment that ships: `maintain compact` (bin-pack)

Closes the last table-maintenance gap vs Lakebase (pairs with the shipped
snapshot expiry + orphan GC), at zero dependency risk.

### Deliverables
1. `icegres maintain compact --table <ns.t> [--target-file-mb N]
   [--min-input-files N] [--execute]` — dry-run by DEFAULT (prints the
   plan: candidate files per partition, projected output); `--execute`
   commits. Follows `maintain remove-orphans` CLI conventions.
2. Mechanics: per partition, select under-target data files (respecting
   partition spec + current schema), stream-read them, rewrite as
   combined files via the existing `new_data_writer` stack, commit ONE
   snapshot with `Operation::Replace` carrying DELETED(old)+ADDED(new)
   entries through the existing hand-built-manifest machinery. Row set
   byte-identical; snapshot lineage preserved (old files remain reachable
   via time travel until snapshot expiry).
3. Safety rails:
   - First-committer-wins: the commit races foreign writers through the
     same requirement-checked REST commit as every other icegres write;
     on conflict the compact aborts cleanly (retry is the operator's
     choice) — never a blind overwrite.
   - Tables bearing delete manifests (foreign-written DVs/position
     deletes): REFUSE loudly (the machinery already bails) — compacting
     under deletes we cannot apply would corrupt semantics.
   - Schema-divergent tables (any manifest carrying a schema id other
     than the current one — foreign engines legally evolve schemas):
     REFUSE loudly — the rewrite aligns columns by position + name, not
     field id, so touching old-schema files could silently resurrect
     dropped-column values.
   - Buffered/keyed tables: compact coordinates with the local buffer
     exactly like other maintenance (no interleaving with an in-flight
     flush of the same table).
   - Orphan-GC interplay: replaced files are NOT orphans (still
     referenced by older snapshots); an e2e leg proves remove-orphans
     keeps them until expiry, and expiry+GC after compact reclaims them.
4. Tests/e2e: unit tests on the planner (selection, thresholds, partition
   grouping); e2e legs — row-set identity pre/post (count + checksum
   query), foreign reader (pyiceberg/REST leg per existing harness
   conventions) agrees post-compact, dry-run mutates nothing, conflict
   abort clean, refusal on delete-manifest tables, refusal on
   schema-divergent tables (REST-evolved schema, nothing mutated),
   expiry-then-GC reclaims replaced files, time travel to pre-compact
   snapshot intact.
5. Bench: new ungated extra `compact_scan_restore_ms` — fragment
   demo-scale table into many small files (loop of small INSERTs), record
   degraded scan p50, compact, record restored p50 ≈ pre-fragmentation
   baseline; plus compact wall-time. Recorded in SCORECARD.
6. Docs: README maintenance row, deployment.md operator section,
   limitations.md gains the honest upstream-DV paragraph (the flush-
   economics gap stays open at the library layer; roadmap-v2 §P2 updated
   with the verdict and re-check trigger).

### Constraints & gates
Invariants I1–I4 unchanged; zero dependency changes of any kind; default
behavior untouched (compact runs only when invoked). Full ladder on the
increment: fmt/clippy -D warnings → cargo test --release (live) →
tail_durability (71) → FULL e2e (191 + new legs) → bench A/B vs the
pre-P2 baseline (scratchpad icegres-pre-p2 + bench-20260711T223700Z.json,
drift-controlled) → a11 ADBC probe → parity.sh. Fix-or-revert per house
rule.
