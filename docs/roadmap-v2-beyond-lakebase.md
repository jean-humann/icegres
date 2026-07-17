# Roadmap v2 — beyond parity: where icegres beats Lakebase

> Successor to `sota-roadmap.md`, which closed the parity gaps (its §10/§11
> ledgers record what shipped: consensus-class 2.7–4.1 ms durable writes,
> 5.2 ms hot rows, 3.4–4.4 ms reads, whole-lakehouse branches, atomic
> multi-table transactions, orphan GC). This document is the *offensive*
> plan: "better than Lakebase" defined precisely, then the increments that
> get there. Invariants I1–I4 and the workflow discipline (adversarial
> review + full gate ladder per increment) carry over unchanged.

## 0. What "better" means (and doesn't)

icegres will not out-Postgres Lakebase — extensions, lock choreography,
and full SQL fidelity stay conceded (a non-goal, not a defeat). "Better"
decomposes into three claims, each measurable:

- **B1 — Beat them on the axes the lake-first architecture structurally
  favors**: openness of the copy, interactive serving latency, footprint,
  self-hostability, verifiability.
- **B2 — Neutralize their remaining structural advantages**: fleet-wide
  freshness, managed HA/failover, at-scale read caching, table
  maintenance economics.
- **B3 — Ship things their architecture cannot**: an OPEN tail-merge
  protocol (theirs is proprietary-engine-only), multi-engine writers on
  the serving copy (theirs is write-closed), branch diff/merge over open
  refs, serve-any-catalog breadth, prove-it-yourself durability.

## 1. Priority ladder

### P1 — The open tail protocol + fleet overlays (kills their last freshness edge, converts it to an openness win)
The one remaining freshness gap: a buffering compute's un-flushed rows are
process-local; replicas and foreign engines wait one flush window. LTAP's
answer (LSN handshake + tail merge) only works for Databricks' own
engines. Ours will be open, in two layers:
1. **Fleet overlays** (designed in sota-roadmap §3 backend-2): icegres
   computes subscribe to the shared tail — PG backend via LISTEN/NOTIFY
   mirror; quorum backend via an acceptor subscription stream (the Read
   machinery + commit watermarks already exist) — and union-read a peer's
   tail with the same exactly-once overlay rules the local buffer proves.
2. **The open tail read API**: an Arrow Flight endpoint (we already ship
   Flight) exposing, per table: `(watermark, un-flushed tail as Arrow
   batches)` with the same suppression semantics — so ANY engine (Spark
   job, DuckDB, pyiceberg script) can do the merge LTAP reserves for its
   own products. Document it as a small open spec. This is the sharpest
   possible contrast with the article: same mechanism, no gatekeeping.
Measured targets: peer-compute freshness ≤ tail-event latency (~ms, vs
flush-window today); a demo external reader doing a merged-fresh read.

### P2 — The matrix bump: deletion vectors + compaction (one gated unlock, three wins)
**Status (2026-07): recon FALSIFIED the premise; re-scoped and shipped as
`maintain compact` at the current pin** (docs/p2-matrix-bump-scope.md).
Stage-0 recon against apache/iceberg-rust 0.9.1, v0.10.0-rc.3, AND main:
no rev delivers ANY of the three payloads — DV/puffin writes don't exist
(no delete writer, `fast_append` rejects delete content, `PuffinWriter`
hides blob offsets), DV READ application doesn't either
(`caching_delete_file_loader`: puffin-DV loader is a TODO), there is no
rewrite/replace-files action, and the Catalog trait still commits one
table at a time. The bump had zero payload, so every pin stays put, and:
1. **Merge-on-read keyed flushes (2a): blocked upstream, both directions**
   — the read side is the hard blocker (icegres could never read its own
   DVs; violates I2). Waits for the library.
2. **Bin-pack compaction (2b): SHIPPED at the current pin** — `icegres
   maintain compact` rides the existing hand-built-manifest + raw-REST
   machinery (`Operation::Replace` exists at 0.9.1); dry-run default,
   first-committer-wins abort, loud refusal on foreign delete-manifest
   tables, e2e-proven row-set identity + foreign-reader agreement + GC
   interplay; bench `compact_scan_restore_ms`.
3. **Native multi-table txn (2c): absent at every rev** — our raw-REST
   shim stays (per the original scope: not a failure).
Re-check trigger: revisit the bump when a crates.io release ships DV write
+ puffin-DV read application (watch `caching_delete_file_loader`) or a
rewrite action. The rc.3 candidate churn map (API renames, datafusion
53.1/arrow 58.3/datafusion-postgres 0.16 pairing, MSRV clear at 1.96.1)
is recorded in the session recon log so the future bump starts from a
worksheet, not from scratch.

### P3 — icegresd-ha: automated failover + autoscaling-lite (their managed-ops edge, self-hosted)
Neon's control plane is proprietary (study, refuted claim #2) — the OSS
world gets nothing. We already own the hard part: term fencing means a
new compute taking over a quorum tail IS a safe failover. Ship:
1. **Automated tail-writer failover**: icegresd health-checks the serving
   compute; on failure, spawns a replacement whose tail open() fences the
   old writer (proven machinery) and replays — target: failover <
   wake-time + election (~sub-second on LAN), zero acked-row loss (the
   suite already proves the data half).
2. **icegresd redundancy**: N icegresd instances with leader election over
   the icekeeperd trio (it IS a consensus service; a tiny lease atop it
   avoids a second system).
3. **Autoscaling-lite**: session/qps thresholds spawn additional read
   computes (branch/replica endpoints) and reap them when idle — the
   scale-OUT half of scale-to-zero, single-digit-node scope, honestly not
   Kubernetes.
Claim afterward: the only self-hostable lakehouse-Postgres with automated
HA — a sentence Lakebase's OSS story cannot say.

### P4 — Local NVMe cache tier (the PageServer idea that translates, minus its hardest problem)
Foyer-style read cache for Parquet footers + column chunks keyed by
(path, byte-range). Immutable files make invalidation trivial — the
problem that makes PageServer's cache hard (LSN-addressed pages) simply
does not exist here. Gate on evidence at scale: build the 100× bench
first (P6), cache second if it shows object-store latency dominating.
Pairs with the deferred key index tier (sota-roadmap §7 bar unchanged).

### P5 — Branch diff/merge: the lakehouse preview-environment DX
We have whole-lakehouse branches + dbname routing. Complete the loop:
- `icegres branch diff <a> <b>`: per-table snapshot lineage comparison
  (rows added/deleted, schema changes, diverged tables) — cheap over
  Iceberg metadata.
- `icegres branch merge <from> <to>`: fast-forward refs where `to` has
  not moved since the fork; conflict report otherwise (no three-way row
  merge — honest scope).
- `AS OF TIMESTAMP` SQL sugar over time travel.
The pitch — branch the lakehouse per PR, query it, diff it, merge or
discard — is a workflow neither Lakebase (per-database, closed engine)
nor bare Iceberg (no serving endpoint) delivers end to end.

**Status (2026-07): SHIPPED** (scope: docs/p5p7-scope.md, one PR with P7).
`icegres branch diff <a> <b> [--table] [--json]`: metadata-only per-table
comparison — fork point by walking parent-snapshot-id chains to the common
ancestor, unchanged/advanced/diverged/created/dropped statuses,
summary-reported row deltas, field-id-matched schema add/drop/rename.
`icegres branch merge <from> <to> [--table] [--execute]`: fast-forward
ONLY (no three-way row merge, ever — documented in limitations.md); dry
run by default; the whole eligible set commits as ONE atomic multi-table
transaction with the observed to/from heads pinned as requirements, so an
injected foreign commit 409s with nothing applied (e2e-proven); any
diverged table refuses the whole run with a per-table conflict report.
`AS OF TIMESTAMP '...'` / `AS OF <snapshot_id>` sugar rewrites — on the
raw statement text, gated to that exact syntax — to the existing
`table@snapshot` path on both pgwire protocols and `icegres sql`
(dialect note in limitations.md; before-first-snapshot errors loudly).

### P6 — Prove it at 100×: the scale bench + serve-any-catalog
1. **Scale bench**: extend `bench/compare` to ~500M rows on the dev box
   (still honest about single-node), publish where the interactive-band
   advantage holds vs Trino/Spark and where it ends; this drives P4's
   go/no-go and updates the README's honest-fit line with data.
2. **Catalog breadth**: verify + document against Polaris and AWS Glue
   REST (capability probes exist; auth flows differ). Every catalog
   icegres serves is a market Lakebase's write-closed tier cannot enter.

**Status (2026-07): catalog-breadth half SHIPPED** (scope:
`docs/p6-scale-catalog-scope.md`). The one real coupling was auth: the
catalog client is a stock iceberg-rust `RestCatalogBuilder` using only
REST-spec-standard endpoints, so breadth = threading the crate's existing
auth props through `CatalogOpts`. Added `--catalog-token` (pre-minted
bearer), `--catalog-credential` (OAuth2 client-credentials),
`--catalog-oauth2-uri`, `--catalog-scope` (env `ICEGRES_CATALOG_*`) —
inserted into the catalog props map ONLY when set, so the default open
Lakekeeper path is byte-identical (invariant I3); zero new dependencies
(OAuth2 is already vendored in iceberg-rust 0.9.1); secrets carry a
redacting `Debug`. Proven end to end against a spec-conformant OAuth2
gateway (`bench/clients/catalog-gateway`, Go stdlib) that fronts the real
Lakekeeper and genuinely 401s unauthenticated calls: full CRUD +
time-travel on the `token` path, OAuth2 client-credentials reads on the
`credential` path (e2e section `(cat)`). **Glue/SigV4 is blocked at the
pin** (no SigV4 in `iceberg-catalog-rest 0.9.1`; re-check trigger on any
bump). **Polaris is spec-compatible by construction but un-buildable on
this box** (Gradle 9.6.1 download proxy-denied), so the second-catalog
proof is a labeled auth harness, not a Polaris run. Full matrix + honest
per-catalog labels: `docs/catalog-support.md`.

### P7 — `icegres verify`: the trust moat, productized
Package the durability suites as a first-class command run against the
OPERATOR'S deployment: kill -9 recovery on their tail backend, fencing,
exactly-once replay, freshness-bound checks — a pass/fail report of the
claims that matter. No database vendor lets users re-prove the marketing
locally; the harness already exists, this is packaging. Cheap, loud
differentiation aligned with I4.

**Status (2026-07): SHIPPED** (scope: docs/p5p7-scope.md, one PR with P5).
`icegres verify [--tail-dir|--tail-url|--tail-quorum] [--suite ...]
[--json] [--keep-evidence DIR]` spawns its OWN scratch servers (the
current executable) against the operator's real catalog/store/tail,
inside a dedicated `icegres_verify_<nonce>` namespace (refused if it
pre-exists; created, tested, dropped on every exit path), and re-runs the
tail_durability suites as library code: durable-ack kill -9 recovery,
exactly-once watermark replay + sequence floor, stale-writer fencing
(pg advisory lock / quorum term), the freshness bound, and quorum
failover replay. Unconfigured backends SKIP loudly; each check names the
claim and the doc section that makes it; exit 0 iff all selected pass.
e2e-proven green for dir+pg+quorum AND proven to FAIL (nonzero, durability
marked FAIL) against a sabotaged tail. Runbook: deployment.md §12;
non-coverage (object-store durability itself, catalog HA): limitations.md.

## 2. Explicitly still refused (unchanged non-goals)
Postgres extensions/full fidelity; an authoritative row tier; arbitrary-
LSN PITR (snapshot/flush-window granularity stands); distributed query
execution; transaction pooling. The moment any of these tempt, reread
invariant I1.

## 3. Sequencing & dependencies
P1 and P3 are independent and can interleave; P2 (matrix bump) should land
early because P2.1 changes hot-row economics that P6's bench should
measure; P4 waits for P6's evidence; P5 and P7 are low-risk fillers
between heavy increments. Every increment: scope doc → ultracode workflow
(recon → implement → adversarial review ×2 → fix rounds) → full gate
ladder (unit + durability + e2e + drift-controlled bench A/B) → commit.

## 4. The scoreboard "better" will be judged on
| axis | Lakebase today | icegres after v2 |
|---|---|---|
| freshness for OTHER engines | conversion-cadence (merge is proprietary) | open tail API: merged-fresh reads for anyone |
| serving copy writable by Spark/Trino | no | yes (and conflict-ergonomic) |
| self-hosted HA/failover | none OSS | automated, consensus-fenced |
| hot-row flush economics | native heap | DV merge-on-read (post matrix bump) |
| interactive analytics in-endpoint | delegated to clusters | 3–4 ms in-process, proven at 100× scale |
| durability claims | vendor-stated | operator-verifiable (`icegres verify`) |
| branch workflow | whole-DB branch (closed engine) | branch + diff + merge over open refs |
