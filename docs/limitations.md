# icegres limitations

What icegres deliberately does NOT do, or does with a documented caveat. Read
this before adopting it for a workload — every item here is a conscious
trade-off of the "Postgres wire + Arrow Flight SQL over an Iceberg lakehouse"
design, not a bug. Each notes the workaround and, where relevant, why it is not
yet closed (usually a constraint of the pinned dependency matrix: iceberg-rust
0.9.1, datafusion 52, arrow 57, tonic 0.14).

---

## Transactions

- **Multi-table transactions are not atomic across tables.** The Iceberg REST
  protocol commits one table per request. A transaction touching N tables
  issues N commits in deterministic (sorted) order after re-validating every
  pin. If commit *k* fails after *k−1* succeeded, the COMMIT returns SQLSTATE
  **`40003` (statement_completion_unknown)** naming exactly which tables
  committed and which did not — **do not blindly retry** (that would
  double-apply the committed tables). Single-table transactions are fully
  atomic. Set `ICEGRES_TXN_STRICT=true` to refuse multi-table COMMITs up front
  (`0A000`, nothing applied) and guarantee all-or-nothing.
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

## Transport / security

- **Arrow Flight SQL TLS is terminate-in-front.** In-process Flight TLS is not
  wired against the pinned tonic 0.14 stack (which moved TLS to a separate
  stack); run Flight SQL behind a TLS-terminating gRPC proxy. pgwire TLS is
  in-process (rustls, `--tls-cert`/`--tls-key`).
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
