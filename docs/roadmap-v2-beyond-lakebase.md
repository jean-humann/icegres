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
The pinned iceberg-rust 0.9.1 gates the two biggest remaining economics
items. One deliberate, fully-gated dependency-matrix bump (iceberg-rust ≥
the rev with DV/puffin writes + replace-files, arrow/datafusion aligned;
moonlink's tree is the existence proof) unlocks:
1. **Merge-on-read keyed flushes**: hot-row windows apply as deletion
   vectors + appends instead of COW file rewrites — flush cost stops
   scaling with file size; hot-row throughput ceiling rises accordingly.
2. **Bin-pack compaction** (`maintain compact`): closes the last
   maintenance gap; pairs with the shipped orphan GC.
3. Native multi-table txn support if the lib gained it (drop our raw-REST
   shim only if byte-equivalent).
Risk-managed: its own increment, full ladder (269 tests / 71 durability /
173 e2e / bench A/B) must hold BEFORE any feature uses the new surface;
revert-on-regression per house rule. This is the highest-leverage single
increment on the board.

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

### P6 — Prove it at 100×: the scale bench + serve-any-catalog
1. **Scale bench**: extend `bench/compare` to ~500M rows on the dev box
   (still honest about single-node), publish where the interactive-band
   advantage holds vs Trino/Spark and where it ends; this drives P4's
   go/no-go and updates the README's honest-fit line with data.
2. **Catalog breadth**: verify + document against Polaris and AWS Glue
   REST (capability probes exist; auth flows differ). Every catalog
   icegres serves is a market Lakebase's write-closed tier cannot enter.

### P7 — `icegres verify`: the trust moat, productized
Package the durability suites as a first-class command run against the
OPERATOR'S deployment: kill -9 recovery on their tail backend, fencing,
exactly-once replay, freshness-bound checks — a pass/fail report of the
claims that matter. No database vendor lets users re-prove the marketing
locally; the harness already exists, this is packaging. Cheap, loud
differentiation aligned with I4.

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
