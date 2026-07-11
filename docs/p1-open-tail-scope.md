# Scope: P1 — open tail protocol + fleet overlays, with Flight-path performance

Roadmap-v2 P1 (read it first: docs/roadmap-v2-beyond-lakebase.md §P1) plus
the Arrow Flight performance debt (bench/COMPARISON.md caveat 4: ~48 ms
flat small-query latency, dominated by double planning across
GetFlightInfo/DoGet + gRPC round trips). One increment, one PR.

## Design keystone (recon may refine, not discard)

The open tail API is served BY the buffering compute over Arrow Flight —
NOT by the tail backends. The buffering compute already holds the exact
overlay state (pending + keyed + flushed generations with their
watermarks and suppression rules); serving it makes the protocol
backend-agnostic (dir/pg/quorum identical) and collapses P1 into ONE
mechanism consumed by two audiences:
- PEER icegres computes subscribe → fleet-wide union reads.
- ANY external engine calls it → merged-fresh reads (the thing LTAP
  reserves for Databricks' own engines).
Honesty: if the buffering compute dies, peers/externals fall back to
commit-cadence freshness (rows themselves are tail-durable and replay on
takeover — durability is NOT at stake, only the freshness bonus).

## Deliverables

### 1. The open tail read API (flight.rs extension + small spec doc)
- New Flight actions/endpoints on flight-serve AND on a lightweight
  in-process Flight listener inside `icegres serve` when buffering is on
  (one port, --tail-api-port or piggyback on flight-serve if the recon
  finds serve can host it cleanly; opt-in flag, off = today):
  - `TailSnapshot { table }` -> (own tail watermark seq, suppression key
    set summary, un-flushed rows as Arrow record batches, per the SAME
    visibility rules a local union read applies).
  - `TailSubscribe { table, from_seq }` -> incremental stream (new
    statements as they ack; heartbeats carrying the advancing watermark).
- Read-only, best-effort, versioned header; auth rides the existing
  Flight basic-auth when --auth-file is set. Document as
  docs/open-tail-protocol.md (request/response schema, semantics,
  fallback contract, versioning) — the "open spec" of roadmap P1.

### 2. Fleet overlays (buffer.rs + cache.rs consumer side)
- `--peer-tail <host:port>[,...]` (env ICEGRES_PEER_TAILS), opt-in: a
  read compute maintains per-table mirrors of each peer's tail via
  TailSubscribe (reconnect with backoff; mirror dropped on disconnect ->
  fall back to commit cadence, WARN once, gauge icegres_peer_tail_age_ms).
- Scans union the peer mirror with the SAME exactly-once rule the local
  overlay uses: peer rows included iff peer_watermark(scan metadata via
  the icegres.tail-seq.<peer-id> property) < row seq — the property
  protocol already carries per-identity watermarks; the mirror carries
  the peer identity from the handshake. Keyed suppression from the peer
  keyed set applies to committed rows exactly as locally.
- Scope guard: peer overlays are read-side only; no cross-compute write
  coordination changes; single-buffering-writer-per-table remains the
  deployment model (document).

### 3. Flight performance (the ~48 ms -> target <=15 ms, stretch <=10 with freshness)
- Kill the double planning: GetFlightInfo plans once, caches the prepared
  physical plan/stream under the ticket (bounded TTL + LRU); DoGet
  executes the cached plan instead of re-planning. Measure each leg.
- Thread the freshness/plan-cache machinery into the Flight query path
  (same eligibility rules as pgwire; --freshness-ms applies).
- Cheap wins recon must check: gRPC server settings (initial window
  sizes, tcp_nodelay), reusing the session context, avoiding per-request
  catalog loads outside freshness mode... measure before/after per leg
  with ICEGRES_QUERY_TIMING extended to flight stages.

### 4. Tests / e2e / Python clients (explicit user ask)
- Unit: tail-API snapshot/subscribe framing, watermark/suppression
  correctness of served batches, peer-mirror exactly-once rule (property
  watermark vs mirror seq), plan-ticket cache (hit/expiry/invalidation on
  snapshot change), subscribe reconnect/fallback.
- e2e.sh new section: buffering server A + read server B with
  --peer-tail: INSERT on A visible on B within the event bound (poll
  deadline), keyed UPDATE on A suppresses/replaces on B, kill A -> B
  falls back to commit cadence without error, rows still land via flush.
- Python clients (bench/clients/, runnable standalone + wired into the
  e2e section where the stack allows):
  - p1_tail_reader.py: adbc/pyarrow Flight client doing the FULL external
    merged-fresh read: read committed Parquet state via the pgwire/ADBC
    query path, call TailSnapshot, apply suppression + union, assert the
    merged view equals a direct icegres union read (the demo that ANY
    engine can do LTAP's trick).
  - p1_flight_perf.py: measures Flight small-query p50 before/after
    (against the old binary if present, else documents after-only) and
    asserts the target bound.
  Use ONLY libraries already used by bench/clients (adbc_driver_flightsql,
  pyarrow, psycopg2 — check what a11_adbc_probe.py imports; pip install
  into the env if absent, matching the harness's existing skip-if-missing
  conventions).
- Bench: extend bench.sh's ungated extras with flight_q1_ms (small query
  via Flight) so the improvement is a tracked metric; record
  before/after in the PR.

## Constraints
Invariants I1-I4. Zero new Rust dependencies unless already lock-present
at exact pins (tonic/arrow-flight are in the tree). Default-off for every
new surface (no --tail-api / --peer-tail => byte-identical; Flight
perf work must not change results or ADBC compatibility — the a11 probe
must stay green). Durability suites must stay green (tail semantics
untouched — this increment READS the tail state, never mutates it).

## Gates
Full ladder: fmt/clippy -D warnings; cargo test --release (PG env +
ICEGRES_LIVE_TESTS=1); tail_durability.sh (71); FULL e2e.sh (incl. the
new section); bench A/B vs the pre-change binary (no default-path
regression; flight_q1_ms improvement recorded); the two python clients
run green against the live stack. Commit only via the orchestrator.
