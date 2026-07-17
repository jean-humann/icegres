# Scope: P6 — prove it at scale + serve any Iceberg REST catalog

Roadmap-v2 P6, two independent halves in one PR. Half A answers "does the
interactive-serving advantage hold as data grows, and where does it end?"
with data. Half B answers the user's direct question — "can icegres run
with ANY Iceberg REST catalog, not just Lakekeeper?" — by removing the one
real coupling (no auth surface) and proving it against a second catalog.

## Binding recon findings (2026-07-16, already established)

- The catalog client is a stock iceberg-rust `RestCatalogBuilder`
  (context.rs:53); the `.load("lakekeeper", …)` string is a NAME label,
  not coupling. `/v1/config?warehouse=` prefix discovery and all CRUD are
  REST-spec-standard.
- The multi-table `transactions/commit` path is ALREADY catalog-agnostic:
  a capability probe (overwrite.rs, seeded from the config `endpoints`
  list + a data-free probe) detects whether the catalog implements it and
  the shim degrades cleanly when it does not.
- **The one real gap is AUTH.** `CatalogOpts` (main.rs:70) exposes only
  uri/warehouse/S3 creds — no token/OAuth2 knobs. iceberg-rust 0.9.1's
  REST client DOES support them as plain props: `token` (pre-minted
  bearer), `credential` (`client_id:client_secret`, OAuth2
  client-credentials), `oauth2-server-uri`, `scope` (catalog.rs
  174/219/231/293). Threading these through is the whole code change for
  breadth.
- **Glue REST is blocked at our pin**: no SigV4 support exists in
  `iceberg-catalog-rest 0.9.1` (no `rest.sigv4-enabled`/`signing-region`/
  `signing-name` constants). Honest verdict + re-check trigger, same shape
  as the P2 DV finding — recon confirms and records the exact upstream
  state; we do NOT hand-roll SigV4.

## Half A — the scale bench

### Deliverable A1: honest max-scale bench
Disk is the binding constraint (~6 GB writable allowance). Recon computes
the largest row count whose Parquet + working set fits with margin (target
the roadmap's 100× over demo; if 500M does not fit, the deliverable is the
largest N that does, with the ceiling documented — never a number we
cannot actually run). Bench harness (extend `bench/compare` / a new
`bench/scale.sh`):
- Generate N rows into a real Iceberg table (batched loads, one or few
  commits; reuse the ingest path).
- Measure at 2–3 sizes (demo, mid, max) the SAME query classes: point
  lookup, filtered scan, a selective join, AND a full-table aggregation
  (the class Trino/Spark win). Record p50/p95, peak RSS, and rows/sec.
- The point: publish WHERE the interactive-band advantage (single-digit
  ms point/filtered/join) holds as data grows, and WHERE the full-scan
  gap opens — with numbers, on this single box, honestly labeled
  single-node.
- If a live Trino/Spark comparison is not re-runnable at max scale on this
  box, cite the existing `bench/compare` cross-engine numbers and extend
  only the icegres-side scale curve, documenting the honest comparison
  scope.

### Deliverable A2: README honest-fit line, updated with data
Replace the current prose ("sub-second point/filtered/join … leave 100 GB+
scans to Trino/Spark") with the measured crossover from A1 — the size band
where icegres wins interactive serving and the size/query where it stops.
SCORECARD gets the scale table.

## Half B — serve any Iceberg REST catalog

### Deliverable B1: catalog auth surface
Add to `CatalogOpts` (flags + env, matching the existing style):
`--catalog-token`/`ICEGRES_CATALOG_TOKEN`,
`--catalog-credential`/`ICEGRES_CATALOG_CREDENTIAL`,
`--catalog-oauth2-uri`/`ICEGRES_CATALOG_OAUTH2_URI`,
`--catalog-scope`/`ICEGRES_CATALOG_SCOPE`. Thread into
`connect_catalog`'s props map ONLY when set (absent ⇒ byte-identical to
today — the Lakekeeper default path is untouched). Redact secrets in logs.
The `.load()` name label becomes generic ("rest") or stays — cosmetic;
document that it is a label.

### Deliverable B2: proof against a SECOND real catalog
Recon picks the most feasible real second REST catalog and the auth flow
to exercise:
- Preferred: Apache Polaris (Apache-2.0, buildable from source like we
  built Lakekeeper/RustFS) with its OAuth2 client-credentials flow — a
  genuine second implementation exercising B1's `credential` path end to
  end (create namespace/table, INSERT, query, time travel).
- If Polaris cannot be stood up on this box (JVM/Gradle/Docker
  constraints — docker daemon is absent), fall back to: (a) a
  token-secured Lakekeeper instance proving the `token`/OAuth2 props flow
  through and authenticate against a REAL server that rejects
  unauthenticated calls, PLUS (b) a documented capability/conformance
  matrix of what the REST-spec surface icegres uses maps to across
  Lakekeeper / Polaris / Glue / Nessie / Unity-REST (spec-standard vs
  extension per endpoint) — honest about which are proven-live vs
  spec-compatible-by-construction.
- Whichever path: an e2e leg (or a documented, reproducible smoke) that
  proves an authenticated non-Lakekeeper-shaped flow works.

### Deliverable B3: the catalog-breadth verdict doc
`docs/catalog-support.md`: the endpoint-by-endpoint surface icegres relies
on, spec-standard vs Lakekeeper-extension, the auth flows now supported
(token, OAuth2 client-credentials) vs blocked (SigV4/Glue at the pin, with
the re-check trigger), and a per-catalog status (Lakekeeper: proven;
Polaris: proven or spec-compatible; Glue: blocked-at-pin; Nessie/others:
spec-compatible-by-construction, untested). Every claim labeled
proven-live vs by-construction. limitations.md + README breadth line +
roadmap-v2 §P6 updated.

## Tests / gates
- Unit: CatalogOpts auth-prop threading (set ⇒ present in props, unset ⇒
  absent — default path byte-identical), redaction.
- e2e: the B2 second-catalog leg (live or documented-smoke); scale bench
  is ungated (its own script, recorded in SCORECARD).
- Full ladder MUST stay green: fmt/clippy -D warnings → cargo test
  --release (live) → tail_durability (71) → FULL e2e (272 + new legs) →
  tests/helm.sh (103) → bench A/B vs pre-P6 baseline (scratchpad
  icegres-pre-p6, drift-controlled paired) → a11 + parity. Adversarial
  review ×2 + refutation before the PR. Fix-or-revert per house rule.

## Constraints
Invariants I1–I4. Zero new dependencies (OAuth2 is already in
iceberg-rust; the auth props are strings). Pinned matrix untouched.
Default (Lakekeeper, no auth flags) byte-identical. No hand-rolled SigV4.
