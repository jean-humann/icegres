# Scope: P5+P7 — branch diff/merge DX + `icegres verify` (one PR)

Roadmap-v2 P5 and P7 combined per user direction: the two low-core-risk,
high-differentiation increments. Neither touches the write path, the tail
protocol, or the serving hot path — the risk profile of this PR is CLI +
metadata + harness packaging.

## P5 — branch diff / merge / AS OF: the lakehouse preview-environment loop

We ship whole-lakehouse branches (`branch create-all`) + branch-endpoint
routing. Complete the workflow: branch per PR → query it → **diff it** →
**merge or discard**.

### 1. `icegres branch diff <a> <b> [--table ns.t] [--json]`
Per-table comparison over Iceberg metadata only (no data scans by
default):
- ref resolution (branch name or `main`), fork-point discovery via
  snapshot lineage (walk parent-snapshot-id chains to the common
  ancestor; the create-all pinning gives every table an exact fork
  snapshot).
- Per table: unchanged / advanced-on-a / advanced-on-b / diverged;
  snapshot counts each side; rows added/deleted per side from snapshot
  SUMMARY properties (added-records/deleted-records — metadata-honest:
  labeled as summary-reported, not recounted); schema changes (current
  schema id + column add/drop/rename listing); property changes worth
  surfacing (keyed/tail settings).
- `--table` narrows to one table with per-snapshot lineage detail.
- Tables existing on only one side reported as created/dropped.
- Cheap: metadata reads only; `--json` for tooling.

### 2. `icegres branch merge <from> <to> [--table ns.t] [--execute]`
Fast-forward only, honest scope (no three-way row merge — refuse loudly):
- Eligible iff `to` has NOT moved since the fork point for that table
  (to's head == fork snapshot): merge = atomically move to's ref to
  from's head. Multi-table: the whole set commits via the existing
  multi-table transactions/commit shim — all tables fast-forward in ONE
  atomic cut or nothing (partial eligibility => refuse whole run unless
  `--table` narrows).
- Diverged tables => per-table conflict report (both heads, fork point,
  row deltas) and refusal; the operator rebases by re-branching.
- Dry-run by DEFAULT (prints the plan: per-table ff/conflict); --execute
  commits. First-committer-wins: the commit pins the observed to-heads
  as requirements — a foreign commit racing the merge aborts it cleanly.
- Refuse if `to` == the branch a serving compute is currently writing
  with a buffered/keyed window unflushed for affected tables? NO —
  out of scope: merge operates on catalog refs; the buffer flushes to
  its own branch ref; document the operational rule (quiesce writers on
  `to` before merging into it, same rule as any Iceberg ref surgery).

### 3. `AS OF TIMESTAMP` SQL sugar
`SELECT ... FROM t AS OF TIMESTAMP '...'` (and `AS OF <snapshot_id>`)
rewritten to the existing `table@snapshot` time-travel path by resolving
the snapshot-log entry at/just-before the timestamp. Parser-level rewrite
in the existing statement-rewrite layer (sqlparser AST is available);
document dialect notes (this is DuckDB/Databricks-style sugar, not
Postgres syntax — pgwire clients pass it as plain SQL).

## P7 — `icegres verify`: the trust moat, productized

Package the durability harness as a first-class operator command: re-prove
the claims against THEIR deployment, not our CI box.

### 4. `icegres verify [--catalog ...] [--tail-dir|--tail-url|--tail-quorum ...] [--keep-evidence DIR] [--suite all|durability|freshness|fencing|exactly-once]`
- Spawns its OWN scratch icegres server processes against the operator's
  REAL catalog + object store + tail backend (the things whose behavior
  is being verified); never touches a running production server.
- Uses a dedicated scratch namespace (`icegres_verify_<nonce>`); creates,
  tests, and DROPS it; refuses to run if the namespace pre-exists;
  cleanup guaranteed on every exit path (trap); all writes confined to
  the scratch namespace — verified before any kill.
- Suites (adapted from tests/tail_durability.sh + e2e legs, compiled-in
  as library code, not shell): (a) durable-ack kill -9 recovery: ack N
  rows, kill -9, restart, assert all acked rows land exactly once;
  (b) exactly-once replay: crash between flush and truncate, assert no
  doubles via the watermark rule; (c) fencing: two writers same identity,
  assert the stale one cannot ack (quorum) or is excluded (pg advisory);
  (d) freshness bound: measure foreign-commit visibility under
  --freshness-ms and assert the documented bound; (e) failover (quorum +
  lease flags set): kill the writer, assert replacement fences+replays.
- Output: a pass/fail report (human table + --json), each check naming
  the claim it re-proves and the doc section that makes the claim;
  exit 0 iff all selected checks pass. --keep-evidence saves logs/
  artifacts for support.
- Honesty rails: checks that need a backend the operator didn't
  configure SKIP loudly (never silently pass); the report states box
  caveats (timings are theirs, not ours); docs/limitations.md notes what
  verify does NOT cover (object-store durability itself, catalog HA).

### 5. Tests / e2e / bench / docs
- Unit: fork-point discovery, ff-eligibility matrix, AS OF timestamp
  resolution (boundary: exact ts, between snapshots, before first),
  verify's namespace guard + skip logic.
- e2e legs: branch diff on a mutated branch (created/dropped/diverged
  table matrix); merge ff happy path (atomic multi-table cut proven:
  to-heads move together), merge refusal on divergence + on race
  (injected foreign commit), dry-run purity; AS OF returns the pinned
  rows vs now; `icegres verify --suite all` runs GREEN against the
  harness stack end-to-end (dir + pg + quorum variants) and FAILS
  correctly when pointed at a sabotaged tail (e.g. tail dir on tmpfs
  wiped mid-test — prove the report catches a real lie).
- Bench: none gated; record verify wall-time per suite (ungated note).
- Docs: README rows (branch workflow + verify), deployment.md verify
  runbook (when to run it: after install, after infra changes),
  limitations.md (no three-way merge; AS OF dialect note; verify
  non-coverage), roadmap-v2 §P5/§P7 status.

## Constraints
Invariants I1-I4. Zero new dependencies. Default behavior byte-identical
(new subcommands + a parser rewrite gated to the exact AS OF syntax).
Pinned matrix untouched. Merge writes ONLY via the existing
requirement-checked commit paths.

## Gates
Full ladder: fmt/clippy -D warnings → cargo test --release (live) →
tail_durability (71) → FULL e2e (238 + new legs) → tests/helm.sh (100,
untouched but must stay green) → bench A/B vs pre-P5P7 baseline
(drift-controlled, paired) → a11 + parity green. Adversarial review ×2 +
refutation before the PR. Fix-or-revert per house rule.
