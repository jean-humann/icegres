# icegres limitations

What icegres deliberately does NOT do, or does with a documented caveat. Read
this before adopting it for a workload — every item here is a conscious
trade-off of the "Postgres wire + Arrow Flight SQL over an Iceberg lakehouse"
design, not a bug. Each notes the workaround and, where relevant, why it is not
yet closed (usually a constraint of the pinned dependency matrix: iceberg-rust
0.9.1, datafusion 52, arrow 57, tonic 0.14).

---

## Transactions

- **Multi-table transactions are atomic only when the catalog implements the
  Iceberg REST multi-table transaction endpoint**
  (`POST /v1/{prefix}/transactions/commit`). When it does — **verified
  against Lakekeeper**, the assumed catalog — a COMMIT touching N tables is
  ONE all-or-nothing catalog request carrying every table's
  `assert-ref-snapshot-id` pin: every table commits or none does, and a
  conflict is a clean, retryable **`40001`** with nothing applied. Support is
  read from the catalog's `GET /v1/config` capability list (or probed once on
  first use — 404/405/501 = unsupported) and cached. On a catalog WITHOUT the
  endpoint, a transaction touching N tables falls back to N commits in
  deterministic (sorted) order after re-validating every pin; if commit *k*
  fails after *k−1* succeeded, the COMMIT returns SQLSTATE **`40003`
  (statement_completion_unknown)** naming exactly which tables committed and
  which did not — **do not blindly retry** (that would double-apply the
  committed tables). Single-table transactions are always fully atomic.
  `ICEGRES_TXN_STRICT=true` now only bites on catalogs without the endpoint:
  it refuses such multi-table COMMITs up front (`0A000`, nothing applied);
  with the endpoint, strict mode is satisfied by atomicity and never refuses.
- **Concurrency is first-committer-wins, no auto-retry.** A COMMIT (or
  autocommit DML) whose pinned snapshot was moved by another writer returns
  **`40001` (serialization_failure)**; the application retries. Row counts were
  computed against the pin, so silently retrying against different data would
  make them lies.
- **`SELECT` inside an explicit transaction is simple-protocol only.**
  Extended-protocol (parameterized) `SELECT` inside `BEGIN … COMMIT` is
  rejected with `0A000`: the hook API cannot see the portal's requested result
  format, and answering a binary portal with text rows would be silent
  corruption. Workaround: run reads in autocommit, or use `preferQueryMode=
  simple` (JDBC) / autocommit reads (psycopg2). Autocommit queries use the full
  extended protocol normally.
- **DDL and non-DML statements inside a transaction are rejected** (`0A000`),
  never half-applied.

## Ingestion and cursors

- **`COPY … FROM STDIN` is not supported on pgwire.** Bulk ingest is served by
  the Arrow Flight SQL lane (`CommandStatementIngest`), which is far faster
  (~one Iceberg commit + one Parquet file per stream). `COPY … TO STDOUT`
  (binary/text/csv) *is* supported on both protocols for reads.
- **Server-side (named) cursors are not implemented.** `DECLARE CURSOR` / `FETCH`
  are not supported by the DataFusion pgwire front-end. Use client-side cursors
  (the default in most drivers).

## Iceberg metadata tables

- **`count(*)` and bare single-column projections over `$snapshots` /
  `$manifests` metadata tables can fail.** A DataFusion logical/physical schema
  mismatch surfaces in the pg row encoder; a bare `select snapshot_id from
  demo."t$snapshots"` (no `ORDER BY`) can even abort the connection's worker.
  Workaround: always add an `ORDER BY` (which inserts a sort that re-establishes
  the schema), e.g. `select snapshot_id, committed_at from demo."t$snapshots"
  order by committed_at`. Column projections with `ORDER BY` work reliably.

## Table maintenance

- **No compaction command yet.** The pinned iceberg-rust 0.9.1 `Transaction`
  API has no rewrite/replace-files action, so small-file compaction would
  require correctness-critical manifest surgery on the custom copy-on-write
  path plus cross-engine (Trino/Spark) verification. Until then, drop-and-reseed
  is the documented canonicalization path.
- **Snapshot expiry is metadata-only.** `icegres maintain expire-snapshots`
  drops snapshots from table metadata but leaves their data/manifest files in
  object storage; reclaiming those bytes needs a separate orphan-file GC (run
  Spark/Trino `remove_orphan_files`, or an object-store lifecycle policy scoped
  to the table prefix).

## Timeouts

- **No object-store (S3) request timeout/retry configuration.** The pinned
  `iceberg-storage-opendal` 0.9.1 exposes no timeout/retry keys (only
  endpoint/keys/region/path-style/SSE/assume-role). A hung object store relies
  on the OS/TCP timeouts. Closing this needs a custom OpenDAL storage factory
  wrapping timeout+retry layers — its own hardening round. The *catalog* path
  IS bounded (`ICEGRES_CATALOG_TIMEOUT_MS`/`_RETRIES`).
- **No per-statement (query) timeout yet.** A pathological query is bounded by
  the memory pool (it spills, then errors with `ResourcesExhausted`) but not by
  wall-clock. Statement-timeout integration at the execution layer is a
  follow-up.

## Write buffer (opt-in)

- **`--write-buffer-ms > 0` trades durability for latency.** In buffered mode an
  INSERT acks from an in-memory buffer and is group-committed every N ms; an
  *unclean* kill (SIGKILL, power loss) loses up to N ms of acked-but-uncommitted
  writes. A *clean* shutdown (SIGTERM/SIGINT, e.g. a rolling deploy) flushes the
  buffer before exiting, so a graceful stop loses nothing. Default is `0`
  (fully synchronous); buffered mode logs a `WARN` on enable. Leave it off for
  durability-critical writes that cannot tolerate the unclean-kill window.
- **`--tail-dir` closes the unclean-kill window with a durable local tail.**
  Added to buffered mode, `--tail-dir <dir>` fsyncs every buffered INSERT to a
  per-table WAL segment before its ack and replays un-flushed rows into the
  buffer on the next boot (exactly-once via the `icegres.tail-seq.<tail-id>`
  table property each flush commit records, belt-and-braced with a local
  watermark sidecar), so SIGKILL/power loss of the *process* loses nothing.
  The caveat moves one honest level down: the tail is this node's disk, so
  losing the node or the disk still loses acked-but-uncommitted rows — disk
  durability, not node-loss durability. Known bounds, stated plainly: the
  tail dir grows without bound during a catalog outage (it mirrors the
  pending buffer, and nothing truncates until a flush commits); boot replay
  materializes the whole surviving tail in memory before the flusher drains
  it; and one residual double-apply window remains (a crash between the
  commit and the sidecar write combined with a foreign writer dropping the
  watermark property). Two more operational notes: the tail fsync runs under
  the buffer lock, so a slow tail disk stalls other tables' buffered INSERTs
  *and* same-server union reads for that fsync's window (per-table locking is
  the known follow-up); and the single-writer guard is an advisory `flock`,
  which is unreliable on NFS — put the tail dir on a local filesystem.
  Default is off (no tail, behavior above unchanged);
  requires `--write-buffer-ms > 0` or startup fails.
- **`--tail-url` (Postgres tail) buys node-loss durability, with its own honest
  bounds.** The tail lives in a Postgres database (schema `icegres_tail`), so
  it survives losing the compute node — but durability is *delegated*: it is
  exactly as strong as that database's own `synchronous_commit`/`fsync`/
  replication settings, no stronger. A tail-database outage **blocks buffered
  writes** (the INSERT's statement fails — backpressure, never a silent
  downgrade to non-durable buffering), and the worker never reconnects after a
  broken connection (the connection *is* the one-writer advisory lock;
  re-acquiring it silently would race a replacement process) — restart the
  server instead. The one-writer advisory lock is **best-effort boot-time
  mutual exclusion, not the correctness guard**: it releases with its session,
  so a replacement server can take over while a half-dead predecessor is still
  flushing buffered rows — that overlap cannot double-apply, because
  exactly-once is enforced by the in-commit watermark property + the catalog's
  `assert-ref-snapshot-id` CAS + the fresh metadata reload before every flush
  attempt (`buffer.rs`), never by the lock. The takeover window, honestly: a
  cleanly dying process releases the lock immediately, but a dead **host**
  (power loss, partition) releases it only when the tail database notices the
  dead TCP peer — icegres sets a 30 s keepalive probe on the tail connection
  (unless the URL carries its own `keepalives*` parameters), so expect roughly
  tens of seconds; an operator can force takeover sooner with `SELECT
  pg_terminate_backend(<pid>)` (the boot refusal prints the holder's pid) —
  only after confirming the old process is really gone. `--tail-url` must be a
  **direct connection or session-pooled**: a transaction-mode pooler
  (pgbouncer transaction pooling, RDS Proxy) scatters the session's statements
  across backends and would silently void the lock, so boot verifies the lock
  is visible from its own backend and refuses otherwise. Every buffered ack
  pays one round trip + commit to the tail
  database (~1–3 ms same-box; a remote tail taxes every ack accordingly), and
  like the local tail it runs under the buffer lock, so a slow tail database
  stalls other tables' buffered INSERTs and same-server union reads. The
  `frames` table grows without bound during a catalog outage (nothing
  truncates until a flush commits). TLS connections to the tail database are
  not supported yet (`NoTls` client), and the tail is still **single-writer,
  single-reader**: fleet-shared overlays (several computes reading one tail,
  LISTEN/NOTIFY, flush leases) are the roadmap's explicit next increment
  (docs/sota-roadmap.md §3), not this backend. Mutually exclusive with
  `--tail-dir`; requires `--write-buffer-ms > 0` or startup fails.

- **Keyed tail upserts (`icegres.tail-upsert`, opt-in) shift semantics and
  have a real ack cost — both stated plainly.** On a table with
  `icegres.primary-key` + `icegres.tail-upsert=true` under buffered mode
  with a tail, exact-PK `UPDATE`/`DELETE` acks are a read-modify-write:
  one catalog `load_table` + one union-view point lookup + one tail fsync
  (~9.5 ms p50 measured vs ~71 ms synchronous — better, but not the ~1.5 ms
  of a buffered INSERT; the lookup is the price of returning honest row
  counts and a full replacement row). Routing is **exact-PK-equality
  only**: every PK column bound once by `=` with a literal (AND-composed
  for composite keys), literal SET values, no other predicates, no
  `RETURNING`/joins/subqueries/binds, PK columns never assigned; PK types
  limited to Iceberg `int`/`long`/`string`/`boolean`/`date`; and the key
  must currently match at most one row (declaration is not enforcement —
  duplicate keys fall back). Anything else silently takes the unchanged
  fence-then-synchronous path — on a keyed-activated table that fenced
  path (and an explicit-txn COMMIT touching the table) serializes with
  in-flight keyed statements, so a committed synchronous write is never
  clobbered by a concurrent keyed statement's stale row image. **Within a
  flush window the table trades snapshot-isolation-per-statement for
  per-key last-writer-wins in ack (tail-sequence) order**: N writes to a
  key net to the newest one in the single window commit — a plain INSERT
  of `k` acked after a keyed delete/update of `k` becomes the key's newest
  version (a delete-then-reinsert in one window leaves the row present
  with the inserted values), and one acked before it loses, exactly as
  wall-clock ack order suggests. Statement-time row counts reflect the
  union view at ack time; the flush re-resolves against fresh metadata, so
  a foreign commit racing the window resolves by commit order (acks were
  tail-time, not commit-exact). Flush conflicts retry internally (WARN
  with attempt counters) — an acked keyed op never surfaces a `40001` and
  is never silently lost (it stays tail-durable until a commit covers it).
  Scans on a table with buffered keyed ops pay a per-scan key-suppression
  filter (row-encoding the PK columns of committed and buffered rows);
  explicit transactions, time travel, and metadata tables are unaffected.
  The tail's on-disk payload is format v2 (op-discriminated); a pre-v2
  tail dir/schema is refused loudly at open — recover it with the version
  that wrote it, or delete it to acknowledge the loss. **Arrow Flight SQL
  is out of scope for keyed routing**: `flight-serve` is a separate
  process with no write buffer and no tail — it executes every DML
  synchronously, so keyed routing never applies there, its row counts are
  sync-exact, and it sees another server's in-window keyed ops only at
  the commit cadence (the existing cross-server freshness rule).

## Transport / security

- **Arrow Flight SQL TLS is in-process** (`flight-serve --tls-cert/--tls-key`),
  terminated with the same rustls stack as pgwire and advertising the `h2`
  ALPN so `grpc+tls://` clients connect directly. tonic 0.14 removed
  server-side TLS from its transport, so this is done by handshaking each
  connection ourselves and handing tonic already-terminated streams; a
  TLS-terminating gRPC proxy in front still works if you prefer that topology.
- **Without `--auth-file` the server is permissive** (any user/password) and
  logs a startup `WARN`. Remote binds are guarded: binding a non-loopback
  interface with auth off requires `--insecure`. Always enable auth in
  production.

## Build / dependency matrix

- **The dependency matrix is pinned and must move as a unit.** iceberg* 0.9.1,
  datafusion 52.5.0, arrow 57.3.1, datafusion-postgres 0.15.0 (pgwire 0.38.3),
  sqlparser 0.62.0, tonic 0.14, prost 0.14, and the toolchain (1.96.1, in
  `rust-toolchain.toml`) are chosen to interlock. Bump them together, behind a
  full gate run — never independently.

---

For the operational counterpart (how to run around these), see
`docs/deployment.md`. For the full pre-GA assessment that enumerated these, see
`docs/production-readiness-audit.md`.
