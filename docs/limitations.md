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
- **Explicit transactions REMAIN synchronous — the tail-staged COMMIT was
  evaluated and REFUSED.** Buffered/tail mode never applies to
  `BEGIN … COMMIT`: `BEGIN` is an ordering fence (the buffer flushes first)
  and `COMMIT` is a synchronous Iceberg commit (~50 ms+), even when every
  autocommit INSERT on the same server acks from the tail in ~2 ms. The
  tempting optimization — ack the COMMIT after fsyncing the transaction's
  ops to the durable tail and post the catalog commit asynchronously —
  would acknowledge BEFORE the catalog's `assert-ref-snapshot-id` check
  runs, i.e. before conflict detection. A losing transaction could then no
  longer answer its COMMIT with `40001` (the client is gone, told
  "committed"); the only remaining options are silently dropping the
  transaction (lost acked writes) or silently re-applying it against data
  it never read (row counts and read-your-writes become lies). That breaks
  the first-committer-wins contract above, so the trade is refused, not
  deferred. Use autocommit statements (buffered INSERT, keyed
  UPDATE/DELETE) when you need tail-latency acks; use transactions when
  you need multi-statement atomicity with honest conflict reporting.

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

- **Compaction ships, with honest edges.** `icegres maintain compact` (P2)
  bin-packs each partition's under-target files into ~target-size files as
  ONE `replace` snapshot — dry-run by default, row set identical,
  first-committer-wins abort on any concurrent commit. Its edges are loud
  refusals, not silent risk: tables bearing **foreign merge-on-read delete
  manifests** (deletion vectors / position deletes written by Spark, Trino,
  moonlink, …) are refused — see the next bullet — **partitioned
  tables** are refused (the icegres write stack is unpartitioned-only), and
  **schema-divergent files** are refused: before anything is staged, every
  candidate input's Parquet footer is read and each column's embedded field
  id (`PARQUET:field_id`) is verified against the table's current schema —
  a mismatched or missing field id refuses the whole run, because the
  rewrite aligns columns by position and name rather than field id and
  could otherwise resurrect a dropped column's values under a re-added
  name. (A manifest carrying a non-current schema id also refuses, but only
  as a cheap fast path: a manifest's schema id records the manifest
  writer's schema, not the schema its listed files were physically written
  under — after a foreign `rewrite_manifests` or copy-on-write commit it
  proves nothing, which is why the per-file check is the guarantee.)
  Rewrite the old files under the current schema (full-table rewrite) or
  wait for field-id-aware compaction.
  Buffered/tail mode still fixes the *source* of small files — cadence
  commits write one well-sized file per flush window instead of one per
  INSERT — so compaction is mostly for tables fed by per-statement commits
  or foreign micro-batchers.
- **Upstream deletion-vector support does not exist yet, and that keeps two
  gaps open.** As of iceberg-rust 0.9.1 — and verified against v0.10.0-rc.3
  and main (2026-07) — the library can neither WRITE puffin deletion
  vectors (no DV/position-delete writer; `fast_append` rejects delete
  content; `PuffinWriter` hides the per-blob offsets the manifest needs)
  nor APPLY them on READ (`caching_delete_file_loader.rs`: "TODO: Delete
  Vector loader from Puffin files"). Consequences, stated plainly: (1) the
  keyed-flush economics gap stays open — hot-row flushes remain
  copy-on-write file rewrites instead of tiny DV appends, so flush cost
  still scales with data-file size; (2) icegres refuses to write to or
  compact any table where a foreign engine left delete manifests, because
  it could not even read such a table correctly, let alone rewrite it.
  This is an upstream library boundary, not a design choice — it re-opens
  when a crates.io release ships DV write + puffin-DV read application
  (see `docs/roadmap-v2-beyond-lakebase.md` §P2 for the re-check trigger).
- **Snapshot expiry is metadata-only; pair it with `remove-orphans`.**
  `icegres maintain expire-snapshots` drops snapshots from table metadata but
  leaves their data/manifest files in object storage. The shipped counterpart,
  `icegres maintain remove-orphans <table> [--older-than-hours N] [--execute]`,
  reclaims those bytes: it lists the table's S3 prefix and deletes what no
  retained snapshot/ref references — dry-run by default, and fail-closed
  everywhere ambiguous (unreadable metadata/manifests abort; unknown-age or
  unrecognized objects are never deleted; a recorded file path outside the
  listed bucket aborts the whole run, since liveness cannot be verified
  against a listing that cannot see it). The guard model, plainly: the
  **grace window** (`--older-than-hours`, default 72) is THE protection for
  files written by in-flight commits — ours or a foreign writer's — that
  have not landed in the catalog yet; a fixed **15-minute clock-skew
  allowance** is folded into the cutoff, and `--execute` verifies the real
  host-vs-store skew with a tiny write/stat/delete probe object under
  `metadata/`, aborting beyond the allowance (probe failure also aborts).
  `--execute` with a grace window under 1 h is refused unless
  `--unsafe-grace` is passed — that flag is for **quiescent tables only**
  (e.g. tests); concurrent writers WILL lose in-flight files. Caveats:
  S3-compatible stores only (the listing backend is built from the `--s3-*`
  options), and run it per-table — there is no all-tables sweep yet.

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
  watermark property). Two more operational notes: the tail fsync is
  GROUP-COMMITTED outside the buffer lock (frames are staged under the
  lock, concurrent statements share one `sync_data`; measured: 8 concurrent
  writers p50 ~6 ms vs ~9 ms when it serialized) — a statement that fails
  at the fsync-WAIT stage (a dying disk) errors to the client AND its
  routed rows are removed from the buffer window (exact failure; during
  the brief staging window the rows were transiently visible to
  same-server union reads, acceptable for buffered mode), UNLESS a flush
  snapshot claimed the rows first — that narrow window keeps the old
  disclosed ambiguity (the error may still commit) and the server WARNs
  loudly with the burned sequence; the frame itself never replays and its
  sequence is never reused, so replay stays exactly-once either way — and
  the single-writer guard is an advisory `flock`, which is unreliable on
  NFS — put the tail dir on a local filesystem.
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
- **`--tail-quorum` (consensus tail) removes the delegated single system —
  its honest bounds are operational.** Three `icekeeperd` acceptors, ack =
  2-of-3 fsyncs (Neon SafeKeeper's protocol, adapted — NOTICE): acked rows
  survive losing ANY single node, including the compute. Bounds, stated
  plainly. **Trusted network only**: there is no TLS and no authentication
  between the proposer and the acceptors — anyone who can reach an
  acceptor's port can read/replace the log; bind them to a private segment
  (they default to 127.0.0.1). **Static membership**: exactly three
  acceptors, no add/remove/replace protocol; a replacement acceptor with an
  empty data dir joins only at the next election (server restart), and one
  that missed an election and lagged below the proposer's retained log is
  dropped from catch-up until the next election. **Fewer than two live
  acceptors blocks buffered writes** — the statement errors and the tail
  POISONS itself until restart (deliberate: a quorum-timeout record may
  still become durable later, and re-numbering past it could double-apply;
  the restart's election replays it exactly once — the classic ambiguous-
  commit shape). **Horizon lag**: acceptors truncate their logs lazily (the
  horizon piggybacks on later appends), so acceptor disk usage and boot
  replay may briefly include already-covered frames — replay stays exact
  via the watermark records + the in-commit property; the latest watermark
  record per table is always retained (it IS the replay sidecar), and a
  dormant table's blocking watermark is refreshed automatically once it
  falls ~1 MiB behind the head. Each buffered ack pays one LAN round trip +
  the slower of two acceptor fsyncs, under the buffer lock like the other
  backends. Proposer-driven catch-up only (no acceptor-to-acceptor gossip),
  no acceptor S3 offload. Mutually exclusive with `--tail-dir`/`--tail-url`;
  requires `--write-buffer-ms > 0` or startup fails.

- **Keyed tail upserts (`icegres.tail-upsert`, opt-in) shift semantics and
  have a real ack cost — both stated plainly.** On a table with
  `icegres.primary-key` + `icegres.tail-upsert=true` under buffered mode
  with a tail, exact-PK `UPDATE`/`DELETE` acks are a read-modify-write:
  activation gate + current-row resolution + row fold + one tail fsync
  (~7.0 ms p50 measured, ~5.2 ms with `--freshness-ms 25` — the gate then
  serves the freshness-cached metadata and a hot key resolves from the
  keyed map with no engine read — vs ~46 ms synchronous; better, but not
  the ~1.5 ms of a buffered INSERT: the read-modify-write is the price of
  returning honest row counts and a full replacement row). With
  `--freshness-ms N`, the activation gate and the `icegres.primary-key`
  declaration resolve from metadata up to ~N ms stale, so a FOREIGN writer
  flipping `icegres.primary-key`/`icegres.tail-upsert` inside that window
  can have a keyed write address rows under the old declaration (DDL
  through THIS server invalidates the caches synchronously). Routing is **exact-PK-equality
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

## Open tail API + peer overlays (opt-in, `--tail-api-port` / `--peer-tail`)

- **The freshness bonus dies with the buffering compute.** Peer mirrors and
  external tail readers get event-bound visibility ONLY while the buffering
  server is up. If it dies, consumers drop their mirrors and fall back to
  commit-cadence freshness — silently, by design (one WARN per outage; the
  per-peer `icegres_peer_tail_age_ms{peer=…}` gauge keeps growing, and
  `icegres_peer_tail_age_max_ms` carries the worst case for alerting).
  Nothing is lost but the bonus: acked rows are tail-durable and land via
  replay + flush on the next boot/takeover, and the watermark rule keeps
  every consumer exactly-once across the gap.
- **Replay is keyed to the tail IDENTITY, not the machine.** Acked
  un-flushed rows replay on a SAME-identity restart — the same tail dir
  (its persisted `identity` file), tail database, or quorum log.
  Re-minting the tail identity (a fresh/emptied tail dir, a new
  `icegres_tail` schema, wiped acceptors) abandons the old identity's
  un-flushed frames: nothing replays them, and their
  `icegres.tail-seq.<old-id>` watermark no longer describes anything a new
  writer produces. Point a replacement server at the SAME tail identity if
  the acked window must survive the restart.
- **A hung peer is served for at most the mirror age bound.** The peer
  subscriber channel runs HTTP/2 keepalive pings (10 s interval, 5 s
  timeout, plus TCP keepalive), so a dead/partitioned peer surfaces as a
  stream error and drops its mirror. Independently, scans stop consulting
  a mirror whose peer delivered no event (heartbeats included, so a
  healthy idle peer never trips it) for over 5 s — the mirror is treated
  as ABSENT (commit-cadence fallback, one WARN per stall) and serving
  resumes with the next applied event. Protocol v1's handshake carries no
  flush-cadence hint, so the bound is a constant (≥ 3× the 1 Hz heartbeat
  interval).
- **`--peer-tail` + `--freshness-ms` raises mirror retention.** A reader
  whose committed metadata may be ~S ms stale must not GC mirror items its
  snapshot has not caught up to, or rows would transiently vanish from the
  union. Watermark-covered mirror items are therefore retained for
  max(30 s, 4×S) — computed at startup, with a WARN stating the retention
  chosen — and, while scans are actively consulting the mirror,
  additionally until a scan's OWN metadata has observed the covering
  watermark (the stale-read-on-catalog-error default can freeze scan
  metadata for the length of a reader-side catalog outage, unbounded by S);
  the cost is memory, never correctness.
- **Single buffering writer per table, unchanged.** Peer overlays are
  read-side only; there is no cross-compute write coordination. Two servers
  buffering writes to the same table remains an unsupported deployment
  (the tail one-writer locks fence it), and a reader mirroring two peers
  that both claim a table WARNs once and keeps the FIRST claim — the
  second peer's ingest/drop are refused so it can neither interleave its
  seq space into the owner's mirror nor kill it, and it takes over
  automatically when the owner's mirror drops.
- **The tail API is read-only and plaintext (v1).** The listener rejects
  every Flight write (a write executed there would bypass the pgwire
  ordering fences); auth rides `--auth-file` basic-auth — the `--peer-tail`
  subscriber authenticates with `ICEGRES_PEER_TAIL_USER` /
  `ICEGRES_PEER_TAIL_PASSWORD` (one identity for all peers) — but there is
  no TLS on this port yet: run it on a trusted network. Concurrent
  TailSubscribe streams are capped at 64 per server (RESOURCE_EXHAUSTED
  beyond). SQL SELECTs on it are served (union reads), which is a
  feature, not an accident.
- **Peer mirror visibility is best-effort, not a bound.** Discovery polls
  the peer every ~2 s and events ride a broadcast channel; a lagged/slow
  consumer is disconnected (DATA_LOSS) and must re-snapshot. Expect
  sub-second visibility in steady state, a few seconds after a table's
  first-ever write or a reconnect — always strictly better than the flush
  cadence it falls back to, never a guaranteed event bound.
- **Flight plan-once tickets are version-validated, never blindly pinned.**
  GetFlightInfo still builds the physical plan once (the double-planning
  fix), but the paired DoGet executes it ONLY if every planned table is
  still at its plan-time metadata version — the same rule as the pgwire
  plan cache — and re-plans on any mismatch (a miss, never an error). Plans
  the server cannot re-validate are never pinned at all: default mode (no
  `--freshness-ms`), overlay-bearing (buffered/mirrored) tables, and
  time-travel/volatile statements always re-plan at DoGet. Consequently a
  ticket can never serve results staler than the freshness bound already
  allows, and in default mode DoGet keeps the exact per-scan catalog
  check.

## icegresd-ha (opt-in, `--health-check-ms` / `--lease-quorum` / `--read-replicas-max`)

- **Automated writer failover is QUORUM-TAIL mode only.** icegresd's
  health-check-and-replace loop (`--health-check-ms`) restores write
  availability only because a replacement's `--tail-quorum` open() fences
  the old term and replays the un-flushed window by consensus. `--tail-dir`
  is single-node by nature (the tail IS that node's disk — a replacement on
  another node has nothing to replay), and `--tail-url` failover is
  MANUAL: the tail database's own HA answers for the frames; point a
  replacement server at the same `--tail-url` yourself, after confirming
  the old process is really gone (the advisory lock guards double-attach,
  not liveness). The wedged-but-alive detection (a poisoned tail answers
  `/health` 503 while still accepting TCP) applies to any tail backend,
  but only the quorum backend heals without operator action.
- **Failover is visible to clients as errors, not a pause.** In-flight
  sessions on the killed/wedged compute get connection resets; new
  connections during the respawn window can time out. Clients are expected
  to retry (the e2e failover leg's load driver treats an errored INSERT as
  not-acked and retries it). Measured on this box (debug build, e2e (ha1)):
  kill -9 → first successful write ≈ 0.3–3 s; the bench `failover_ms`
  extra records the release number.
- **The leader lease needs its OWN icekeeperd trio.** One acceptor process
  serves one log (the identity is adopted permanently), and the lease IS a
  proposer election — running it against the computes' data trio would
  fence the tail writer. icegresd refuses a `--lease-quorum` that shares an
  address with `ICEGRES_TAIL_QUORUM` at boot — compared on RESOLVED socket
  addresses, so plain host aliases (`localhost:7101` vs `127.0.0.1:7101`, a
  DNS name vs its IPs) are refused too, not just byte-identical spellings.
  The check is best-effort by nature: a spelling the resolver cannot see
  through (resolution failure, split-horizon DNS, two hostnames for one
  box that resolve differently) still slips past and the first lease
  election then fences the tail writer into a mutual-fencing flap — keep
  the trios OBVIOUSLY disjoint. The lease trio is one more
  thing to keep alive: if IT loses quorum, the leader demotes (stops taking
  new clients, terminates computes) within ~TTL/3 + the lease append
  timeout (~TTL) even though the data path may be perfectly healthy —
  deliberate, since an unrenewable lease cannot exclude a second leader.
- **Split-brain window, stated plainly.** A deposed leader learns it lost
  only at its next renew: for up to ~TTL/3 + TTL two icegresd instances can
  both ANSWER connections — and both can SPAWN writers: a demote racing a
  compute (re)spawn can slip a deposed leader's writer through (both spawn
  paths re-check leadership under the spawn lock, which shrinks the window
  to the spawn itself but cannot close it without coupling the data-tail
  election to the lease term). Data stays safe regardless — the two
  writers fence each other on the DATA tail, and a fenced writer errors,
  never acks — but the fencing can land on the NEW leader's healthy
  writer, leaving it wedged-but-alive (accepts TCP, never acks). That is
  why `--lease-quorum` with a quorum data tail DEFAULTS `--health-check-ms`
  on (1000 ms; set the flag to tune): the health loop replaces the wedged
  writer, degrading the race to one spurious ~1 s failover cycle instead
  of a permanent write outage. Clients still connected to the stale
  leader see errors until they reconnect. Takeover itself needs >= TTL of
  quorum-observed silence, so leader loss means ~1–2× TTL of write
  unavailability (default TTL 6 s). A leader killed with `kill -9` cannot
  terminate its computes; the orphaned writer keeps its port until fenced
  and must be reaped by the operator/supervisor (SIGTERM shuts computes
  down cleanly).
- **Autoscaling-lite is sessions-based, process-mode, single-digit-node.**
  `<db>:ro` is a ROUTING label, not enforcement: a client that issues
  writes through it gets ordinary synchronous Iceberg commits (safe via
  first-committer-wins, but unbuffered and outside the writer's tail —
  keep writes on the main endpoint). Replicas are spawned with the
  buffered/tail environment stripped (a replica opening the writer's tail
  would fence it) and see the writer's un-flushed window only when
  `--replica-peer-tail` points at its tail API — otherwise their freshness
  is the commit cadence. The spawn threshold counts SESSIONS (icegresd's
  own active-connection gauge), not qps; scale-up is one replica per
  routing decision, reap is the existing idle scale-to-zero. In Kubernetes
  this whole feature maps to HPA guidance instead (the chart's `-read`
  Deployment + a plain HPA): in k8s mode icegresd scales exactly ONE
  workload — the writer, for wake-on-connect/scale-to-zero via
  `--k8s-scale` — and never the read pods.

## Helm chart / Kubernetes mode (opt-in, `deploy/helm/icegres` + `--k8s-compute`/`--k8s-scale`)

- **The chart gate renders and validates; it does not run a cluster.**
  `tests/helm.sh` proves lint + committed golden renders + strict schema
  validation (Kubernetes v1.31.0 and v1.34.0, vendored schemas pinned by
  sha) + invariant asserts — offline. The real-cluster smoke procedure in
  `docs/deployment.md` §11 is documented for operators and was NOT
  CI-run here (this repo's gate box has no Docker daemon or cluster).
  The k8s API client itself (the scale GET/PATCH) is unit-tested against
  a mock API server and process-smoke-tested; the in-cluster CA/token
  path is exercised only by the documented smoke.
- **StatefulSet replacement re-mints the writer's tail identity.** A
  container restart (the liveness kill on a wedged tail) or pod
  reschedule starts a NEW server whose quorum election takes a higher
  term: fencing and replay are the design, so this is safe — but it
  means the nth incarnation of `writer-0` is a new proposer, not a
  resumed one, and anything watching tail terms sees them advance on
  every replacement. For `tail.mode=dir` the WAL PVC follows the POD
  NAME: replay works across restarts/reschedules that reattach the
  volume, and does not work across anything that cannot (zone-pinned
  PVs, deleted claims) — dir stays single-node HA, in k8s as anywhere.
  And the reschedule itself is NOT automatic on a hard node loss: a
  StatefulSet pod on an unreachable node sits `Terminating` until the
  pod object is confirmed gone, which on clusters without
  node-lifecycle GC (self-hosted/bare-metal) takes a human running
  `kubectl delete pod --force` — see the runbook's writer-node-loss
  entry (docs/deployment.md §11).
- **k8s scale-to-zero counts only proxied traffic.** The idle clock is
  icegresd's; clients connecting to the writer Service directly (e.g.
  `sslmode=require`, since TLS terminates at the computes and icegresd
  answers `SSLRequest` with `N`) are invisible to it and can be cut by
  an idle park. Keep direct-connect clients off scale-to-zero writers,
  or k8sScaling off. A `helm upgrade` resets a parked writer to 1
  replica (the chart pins `replicas: 1`); the idle loop parks it again.
  A connection racing the park PATCH is severed and must reconnect —
  the same race process mode has with `--idle-shutdown-secs` itself.
  With `ha.enabled` there is one more residual race: a
  deposed-but-unaware leader (its leadership watch lags a lost lease by
  up to ~TTL/3 + the lease append timeout, and its idle clock reads
  idle precisely because traffic moved) can park the SHARED writer
  under the true leader's sessions. The scale loop re-checks
  leadership immediately before the PATCH, shrinking the window to one
  API round trip, but cannot close it; the damage is an availability
  cut, never data loss — the severed sessions reconnect, the next cold
  connection re-wakes the writer, and fence + replay keep every acked
  row.
- **Process-mode features are refused, not emulated, in k8s mode.**
  `--health-check-ms` (the kubelet's liveness probe owns compute
  replacement), `--read-replicas-max` (the `-read` Deployment + HPA owns
  read scaling), and branch endpoints (`<db>@<branch>` answers a clear
  error; deploy a per-branch compute and connect to its Service) — all
  boot- or connect-time refusals with the reason spelled out.
- **The chart renders `tail.mode` none|dir|quorum only.** `--tail-url`
  (Postgres-backed tail) is deliberately not a chart mode: its HA story
  is the tail database's own replication/failover, which the chart
  cannot honestly manage; wire it via `writer.extraEnv` if you own that
  database, and treat failover as manual (previous section).
- **The acceptor protocol still has no TLS/auth** (hardening backlog):
  the `-keeper`/`-lease` trios trust their network segment. The chart
  keeps them on ClusterIP headless Services and, with
  `networkPolicy.enabled`, restricts ingress to this release's pods —
  that NetworkPolicy is only as good as the cluster's CNI enforcement.
- **`ha.enabled` fails over the ENDPOINT, not the writer.** Both
  icegresd instances front the same writer Service; icegresd failover
  and writer failover are independent legs (see the runbook). The
  standby is deliberately unready (0/1 in `kubectl get pods`) — that is
  the leadership readiness probe, not a broken pod.

## Bounded-staleness reads (opt-in, `--freshness-ms`)

- **`--freshness-ms N` with `N > 0` trades exact freshness for read latency,
  boundedly.** Default (`0`) keeps today's contract byte-identical: every
  scan performs one catalog `load_table` (~2–3 ms locally) and observes the
  catalog's current snapshot with no staleness window. With `N > 0`, scans
  serve the cached snapshot with no catalog round trip and ONE background
  task per server re-polls every mounted table each `N` ms, refreshing
  tables concurrently (up to 8 in flight) with a retry-free per-table
  timeout of min(4·`N`, 2 s) — the next pass is the retry — so a slow or
  stalled table delays only its OWN visibility, never other tables'. What
  stays EXACT: this server's own writes (every local commit path — sync
  DML, PK-enforced INSERT, transaction COMMIT, buffer flush, plain INSERT —
  synchronously invalidates the table, so the next local read loads fresh
  metadata), time travel (`table@snapshot`), and branch-pinned reads
  (snapshot-addressed, immutable). What becomes BOUNDED: commits by OTHER
  writers (other icegres servers, Spark, any catalog committer) are visible
  within ~`N` ms plus one refresh round trip (up to that table's refresh
  timeout when the catalog is slow for it). Enabling the mode logs a WARN
  stating the bound.
- **Catalog outage under freshness mode serves stale by default, visibly —
  with an explicit fail-loud opt-out.** The refresher keeps serving the
  last refreshed snapshot (reads do not start failing), WARNs rate-limited,
  and exports the worst-case staleness age as the
  `icegres_freshness_age_ms` gauge on `/metrics` — alert on it. The gauge
  is sampled at each refresher pass START (the age a read could have
  observed just before that pass refreshed), so a healthy value reads
  ≈ `N` ms and it grows monotonically through an outage; the refresher runs
  under a supervisor whose watchdog keeps the gauge growing even if the
  refresher task itself dies (and respawns it, budgeted per minute). Two
  honest edges of stale-serving: read-your-own-writes can regress to
  last-snapshot for a write that committed in the instant before the
  catalog became unreachable (the post-write refresh cannot complete), and
  in buffered mode (`--write-buffer-ms > 0`) rows flushed just before the
  outage stay served through the union overlay for the whole outage — a
  flushed overlay generation is garbage-collected only once metadata
  containing its commit has actually been observed (plus a 30 s age), so
  committed rows never vanish from reads mid-outage; the cost is that the
  overlay memory for those generations is retained until the catalog
  returns. `ICEGRES_STALE_READ_ON_CATALOG_ERROR`
  controls the trade in BOTH modes: default mode fails reads during an
  outage unless it is set truthy; freshness mode serves stale unless it is
  set to `0`/`false` — the explicit fail-loud override for deployments
  where erroring beats silently regressing read-your-own-writes.
- **The physical-plan cache only hits under `--freshness-ms > 0`, and only
  for repeated *identical* statements.** A hit must be sound without a
  catalog check, so it requires the freshness contract; and Iceberg plans
  bake in plan-time file pruning, so a plan for `id = 5` is not reusable
  for `id = 6` — statements that vary literals re-plan every time (the
  extended protocol's prepared statements already reuse their logical plan
  upstream). Overlay-bearing (buffered) tables, time-travel/metadata
  tables, and non-immutable expressions (`now()`, `random()`, …) are never
  cached.

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

## Hardening backlog

Known-and-accepted sharp edges, queued rather than closed — none is a
correctness hole under the documented deployment posture, each would harden
an operational edge:

- **Acceptor idle/read timeout + greeting-stage length cap** (`icekeeperd`):
  a connected-but-silent client holds its connection task forever, and the
  pre-greeting frame length is bounded only by the generic message cap —
  acceptable under the trusted-network posture above, worth tightening.
- **Acceptor fsync via `spawn_blocking`**: acceptor fsyncs run inline on the
  connection task's runtime thread; a slow disk stalls that acceptor's
  other traffic.
- **`run_peer`/supervisor `JoinHandle` watchers**: the proposer's per-peer
  streaming tasks and the re-election supervisor are fire-and-forget; a
  panicked task is only noticed indirectly (stalls → re-election → poison)
  rather than by a watcher that logs the panic itself.
- **Per-table sequence lock for quorum appends** (pipelining refinement):
  staged quorum appends already pipeline their round trips, but sequence
  allocation still serializes on ONE map-wide mutex; per-table locks would
  remove the last cross-table serialization point.
- **Validation-only record walk**: quorum-tail replay decodes every
  surviving record's Arrow payload eagerly; a cheap validation-only walk
  would bound boot memory on very large recovered suffixes.
- **Plan-cache miss-counter noise in buffered+freshness mode**: overlay-
  bearing tables are (correctly) never cached, but each of their SELECTs
  still counts as a plan-cache miss, skewing the hit-ratio metric on mixed
  workloads.

## Build / dependency matrix

- **The dependency matrix is pinned and must move as a unit.** iceberg* 0.9.1,
  datafusion 52.5.0, arrow 57.3.1, datafusion-postgres 0.15.0 (pgwire 0.38.3),
  sqlparser 0.62.0, tonic 0.14, prost 0.14, and the toolchain (1.96.1, in
  `rust-toolchain.toml`) are chosen to interlock. Bump them together, behind a
  full gate run — never independently. The P2 recon (2026-07) verified there
  is currently nothing to gain from a bump: no iceberg-rust rev up to main
  ships DV writes, DV read application, a rewrite-files action, or
  multi-table commit — the pins stay until the re-check trigger in
  `docs/roadmap-v2-beyond-lakebase.md` §P2 fires.

---

For the operational counterpart (how to run around these), see
`docs/deployment.md`. For the full pre-GA assessment that enumerated these, see
`docs/production-readiness-audit.md`.
