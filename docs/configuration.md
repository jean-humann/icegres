# Configuration reference

Every operator-facing knob of icegres, in one place. This is the authoritative
list; the crate READMEs and deployment guides link here rather than restating
defaults.

**Scope.** This reference covers server and daemon configuration (`icegres
serve` / `flight-serve`, `icegresd`, `icekeeperd`) plus the recurring
maintenance tuning knobs. One-shot action parameters of the other subcommands
(`verify`, `branch`, `seed`, `sql`) are documented by their `--help` and the
crate README — they are deliberate per-invocation arguments, not standing
configuration. One deliberate exception to note: `icegres verify` **ignores**
the `ICEGRES_TAIL_*` env vars (its tail flags are flag-only), so a
production tail configuration can never leak into a verification run.

**How settings resolve.** Every setting has a working default for the local dev
stack. Precedence is **CLI flag → environment variable → built-in default**: a
flag overrides its `ICEGRES_*` env var, which overrides the default. Flags that
carry no default are `Option`al — unset means the feature is off (or the
property is simply not sent). Boolean env toggles parse lowercase `1`, `true`,
`on`, `yes` as true — use lowercase; any other set value counts as false.
Secrets (catalog tokens/credentials, S3 keys) are redacted in logs.

**Two servers, distinct endpoints.** `icegres serve` (pgwire) and `icegres
flight-serve` (Arrow Flight SQL) use **separate** host/port/TLS env vars
(`ICEGRES_HOST`/`ICEGRES_PORT`/`ICEGRES_TLS_CERT` vs.
`ICEGRES_FLIGHT_HOST`/`ICEGRES_FLIGHT_PORT`/`ICEGRES_FLIGHT_TLS_CERT`) so a
co-located process can bind each independently. Catalog/S3, auth, freshness, and
the caches are shared.

---

## Catalog & storage

Accepted by every subcommand (`serve`, `flight-serve`, `maintain`, `verify`,
`sql`, …) — they all talk to the same catalog and object store.

| Env var · flag | Default | Meaning |
|---|---|---|
| `ICEGRES_CATALOG_URI` · `--catalog-uri` | `http://127.0.0.1:8181/catalog` | Iceberg REST catalog base URI (Lakekeeper serves it under `/catalog`). |
| `ICEGRES_WAREHOUSE` · `--warehouse` | `lakehouse` | Warehouse name registered in the REST catalog. |
| `ICEGRES_S3_ENDPOINT` · `--s3-endpoint` | `http://127.0.0.1:9000` | S3-compatible endpoint holding the table data (path-style is forced). |
| `ICEGRES_S3_ACCESS_KEY` · `--s3-access-key` | `rustfsadmin` | S3 access key id. |
| `ICEGRES_S3_SECRET_KEY` · `--s3-secret-key` | `rustfssecret` | S3 secret access key. |
| `ICEGRES_S3_REGION` · `--s3-region` | `us-east-1` | S3 region. |
| `ICEGRES_CATALOG_TOKEN` · `--catalog-token` | none | Pre-minted OAuth2 bearer token sent verbatim on every catalog request (never refreshed). Also used by the copy-on-write DML commit client. **Secret.** |
| `ICEGRES_CATALOG_CREDENTIAL` · `--catalog-credential` | none | OAuth2 `client_id:client_secret` for the client-credentials grant (auto-refreshed). **Secret.** |
| `ICEGRES_CATALOG_OAUTH2_URI` · `--catalog-oauth2-uri` | none → `{catalog-uri}/v1/oauth/tokens` | OAuth2 token endpoint for the client-credentials grant. |
| `ICEGRES_CATALOG_SCOPE` · `--catalog-scope` | none → `catalog` | OAuth2 scope requested during the grant. |
| `ICEGRES_CATALOG_TIMEOUT_MS` | `5000` (`0` = no timeout) | Per-attempt timeout for a catalog `load_table`. |
| `ICEGRES_CATALOG_RETRIES` | `2` | Retries after the first failed `load_table`, with exponential backoff (50 ms · 2^attempt). |

See [`catalog-support.md`](catalog-support.md) for the catalog compatibility matrix.

## Serving — pgwire (`icegres serve`)

| Env var · flag | Default | Meaning |
|---|---|---|
| `ICEGRES_HOST` · `--host` | `0.0.0.0` | Bind address for the pgwire listener. |
| `ICEGRES_PORT` · `--port` | `5439` | Bind port for the pgwire listener. |
| `ICEGRES_MAX_CONNECTIONS` | `512` (`0` disables the cap) | Concurrent-connection cap on the accept loop (excess connections wait rather than spawning unbounded tasks). |
| `ICEGRES_IDLE_SHUTDOWN_SECS` · `--idle-shutdown-secs` | off | Scale-to-zero: exit cleanly (code 0) after N consecutive seconds with no client connections (countdown also starts at boot). Run under a socket-activating supervisor for scale-from-zero. |
| `ICEGRES_HEALTH_PORT` · `--health-port` | off | Serve a minimal HTTP `200 OK` liveness endpoint (and `/metrics`) on this port. |
| `ICEGRES_BRANCH` · `--branch` | `main` | Serve a zero-copy branch: reads pin to the branch head, writes commit to the branch ref with `assert-ref-snapshot-id`. |
| `ICEGRES_ENFORCE_PK` · `--enforce-pk` | off | Enforce `icegres.primary-key` table properties: NOT NULL (23502) + uniqueness (23505) on INSERT and PK-assigning UPDATE, anchored to the commit snapshot. Also honored by `icegres sql`. |
| `ICEGRES_TXN_STRICT` | off | Refuse a multi-table `COMMIT` up front with `0A000` when the catalog cannot apply it atomically (only bites on catalogs lacking the multi-table transactions endpoint; with it — e.g. Lakekeeper — COMMITs are always atomic and this never triggers). |

## Serving — Arrow Flight SQL (`icegres flight-serve`)

The ADBC / Arrow Flight SQL entry point. Uses **distinct** host/port/TLS vars
from pgwire (so both can run in one process), but shares auth/freshness.

| Env var · flag | Default | Meaning |
|---|---|---|
| `ICEGRES_FLIGHT_HOST` · `--host` | `0.0.0.0` | Bind address for the Flight SQL gRPC listener (separate from `ICEGRES_HOST`). |
| `ICEGRES_FLIGHT_PORT` · `--port` | `50051` | Bind port for the Flight SQL gRPC listener (separate from `ICEGRES_PORT`). |
| `ICEGRES_FLIGHT_TLS_CERT` · `--tls-cert` | none | PEM certificate (chain) enabling in-process TLS on the Flight listener (advertises the `h2` ALPN; `grpc+tls://` clients connect directly, no front proxy). Requires the key; any TLS setup error aborts startup. |
| `ICEGRES_FLIGHT_TLS_KEY` · `--tls-key` | none | PEM private key (PKCS#8/RSA/SEC1) for `--tls-cert`. Must be supplied together with the cert. |
| `ICEGRES_GRPC_WEB` · `--grpc-web` | off | Also answer gRPC-web on the same port so browsers run Flight SQL directly (`@icegres/flight-web`, [frontend-dashboards.md](frontend-dashboards.md)); native gRPC clients are unaffected. Auth over gRPC-web is a per-RPC `authorization: Basic …` header verified against `--auth-file` (the Handshake RPC does not exist in that protocol; verified credentials are cached server-side so the SCRAM KDF is off the hot path, and failed attempts pay the same per-source-IP backoff as pgwire SASL). Startup WARNs if auth is on without TLS — the password would cross the wire per-RPC in cleartext. With TLS, the listener additionally advertises `http/1.1` ALPN. |
| `ICEGRES_CORS_ORIGIN` · `--cors-origin` | `*` | Origin allowed on gRPC-web CORS preflights/responses. Pin to the dashboard origin whenever `--auth-file` is set. |
| `ICEGRES_RESULT_COMPRESSION` · `--result-compression` | `zstd` | Flight result-batch compression at the Arrow IPC buffer level. `zstd` is the measured ~5× wire reduction; `none` serves uncompressed batches for clients whose arrow build lacks the zstd feature (e.g. `@lakehouse-rs/flight-sql-client` 0.0.10). |
| `ICEGRES_FLIGHT_STATEMENT_TIMEOUT_MS` · `--flight-statement-timeout-ms` | `0` (unbounded) | Wall-clock ceiling per DoGet query stream; a query past it aborts with `DEADLINE_EXCEEDED` rather than holding an executor thread. Fires even mid-scan (live timer). Data path only — metadata RPCs are exempt. |
| `ICEGRES_FLIGHT_MAX_RESULT_BYTES` · `--flight-max-result-bytes` | `0` (unbounded) | Byte ceiling per DoGet result over the Arrow IPC body streamed; a result past it is cut with `RESOURCE_EXHAUSTED`, so a `SELECT *` on a huge table cannot stream gigabytes into a browser tab. |
| `ICEGRES_FLIGHT_MAX_CONCURRENT_RPCS` · `--flight-max-concurrent-rpcs` | `0` (uncapped) | Cap on concurrent in-flight DoGet query streams — the Flight analogue of pgwire `--max-connections`; excess RPCs wait at the choke point rather than spawning unbounded scans. |
| `ICEGRES_FLIGHT_MAX_PREPARED_STATEMENTS` · `--flight-max-prepared-statements` | `1024` | Process-wide cap on retained prepared handles. The least-recently-used handle is evicted before admitting another. |
| `ICEGRES_FLIGHT_PREPARED_STATEMENT_TTL_SECS` · `--flight-prepared-statement-ttl-secs` | `900` | Maximum retained lifetime of a prepared handle; expired handles are rejected and removed lazily. |
| `ICEGRES_FLIGHT_MAX_AUTH_CACHE_ENTRIES` · `--flight-max-auth-cache-entries` | `4096` | Cap on successful Basic-auth cache entries and bearer tokens. Expiry and least-recently-used eviction keep both stores bounded. |
| `ICEGRES_HEALTH_PORT` · `--health-port` (flight-serve) | off | Serve `/health`, `/ready`, and `/metrics` on this port for a **standalone** flight-serve (the Flight per-RPC metrics — `icegres_flight_*` — render here). Shared env var with `serve`; each process binds its own. |
| `ICEGRES_FLIGHT_READ_ONLY` · `--read-only` (flight-serve) | off | Reject every write on the listener — INSERT/UPDATE/DELETE/DROP (query flow, prepared statements, and bulk ingest) return `PERMISSION_DENIED` before execution. Statement-form based (reuses the authz analyzer), independent of `--authz-file`. The posture for a browser SQL explorer. |

## TLS & auth

| Env var · flag | Default | Meaning |
|---|---|---|
| `ICEGRES_TLS_CERT` · `--tls-cert` (serve) | none | PEM cert enabling TLS on the pgwire listener (requires key). Plaintext startup is still accepted — clients opt in with `sslmode=require`/`verify-full`. |
| `ICEGRES_TLS_KEY` · `--tls-key` (serve) | none | PEM private key for the pgwire `--tls-cert`. |
| `ICEGRES_AUTH_FILE` · `--auth-file` (serve & flight-serve) | off (permissive, WARN) | Require SCRAM-SHA-256 (pgwire) / basic-auth (Flight) against this `user:password` file. A per-source-IP failed-auth backoff slows brute-forcing (failures decay after 60 s). |
| `ICEGRES_AUTHZ_FILE` · `--authz-file` (serve & flight-serve) | off (open — any authenticated user, all tables) | Enforce Lakekeeper-style ReBAC from this policy file; a denied statement returns SQLSTATE 42501. Pair with `--auth-file`; Flight requires it. |
| `ICEGRES_INSECURE` · `--insecure` (serve & flight-serve) | off | Acknowledge an unauthenticated listener on a non-loopback interface. Without it, binding a public address while `--auth-file` is unset is refused at startup (secure-by-default guard). |

See [`deployment.md`](deployment.md) for auth/authz file formats and rollout.

## Freshness & caches

| Env var · flag | Default | Meaning |
|---|---|---|
| `ICEGRES_FRESHNESS_MS` · `--freshness-ms` (serve & flight-serve) | `0` (exact freshness) | Bounded-staleness reads: scans serve the cached snapshot with no per-scan catalog round trip; one background task polls the catalog every N ms and swaps changed snapshots. Own writes stay read-your-own-writes exact; foreign commits visible within ~N ms + one refresh round trip. Also activates the physical-plan cache (and, with `ICEGRES_RESULT_CACHE_BYTES` set, the result cache). |
| `ICEGRES_STALE_READ_ON_CATALOG_ERROR` | mode-dependent (exact mode fails loud; freshness mode serves the last snapshot) | On a catalog `load_table` failure, whether to serve the last cached snapshot (`1`) or error (`0`). Overrides the mode default either way. |
| `ICEGRES_PLAN_CACHE_ENTRIES` | `256` (`0` disables) | LRU capacity of the physical-plan cache (active only with `--freshness-ms > 0`). |
| `ICEGRES_RESULT_CACHE_BYTES` | `0` (disabled) | Total decoded-byte budget for the **result** cache: a repeated identical query at an unchanged snapshot is served straight from cached result batches (no planning, execution, or IO). A single result larger than budget/4 is never cached. Active only with `--freshness-ms > 0`. Invalidated by the same version machinery as the plan cache. See [`cqrs-topology.md`](cqrs-topology.md). |
| `ICEGRES_MEMORY_LIMIT_MB` | 70% of system RAM (`0` = unbounded) | Bound on the DataFusion memory pool (FairSpillPool + disk spill) so heavy queries degrade to spill / `ResourcesExhausted` instead of OOM. If `/proc/meminfo` is unreadable, RAM is assumed to be 1 GiB (pool ≈ 716 MiB). An invalid value WARNs and uses the default. |
| `ICEGRES_DF_OPTS` | none | `;`-separated `datafusion.<section>.<key>=<value>` pairs applied on top of icegres's tuned DataFusion `SessionConfig`. An invalid entry (bad shape or unknown key) fails startup loudly. An escape hatch for tuning execution options without a rebuild. |

## Scan & query tuning

| Env var | Default | Meaning |
|---|---|---|
| `ICEGRES_SCAN_CONCURRENCY` | `32` (`0` disables the wrapper) | IO concurrency for Iceberg scans — manifest files, manifest entries, and data files fetched in parallel. Tuned against local RustFS on a 4-core box; raise it for higher-latency object stores, lower it to cap concurrent S3 requests. |
| `ICEGRES_SCAN_BATCH_SIZE` | `8192` (`0` = the reader's 1024 default) | Parquet reader batch size; matching DataFusion's execution batch size cuts per-batch overhead on large scans. |
| `ICEGRES_SCAN_ROW_SELECTION` | off | When enabled, the Parquet reader consults the page (column) index to skip non-matching data pages inside surviving row groups. **Off by default** because iceberg-rust's own writer emits no page index, so enabling it makes the reader error on icegres-written files — only enable it for external datasets whose files carry a page index. |
| `ICEGRES_TABLE_STATS` | on (`0`/`false`/`off`/`no` disables) | Feed each scanned snapshot's live row count to the DataFusion optimizer so hash joins pick the smaller build side. The count comes from the manifest *list* (one small object GET per snapshot, cached per `(table, snapshot)`); tables with delete manifests or missing counts honestly report no statistics. Deliberately advisory-only (reported inexact): statistics can never answer a query — `COUNT(*)` always executes the real scan, so results never depend on metadata. |

## Write buffer & durable tail (`icegres serve`)

Opt-in. The tail options require `--write-buffer-ms > 0` and are mutually
exclusive with each other. See [`limitations.md`](limitations.md) for the
durability/loss model and [`open-tail-protocol.md`](open-tail-protocol.md) for
the tail read API.

| Env var · flag | Default | Meaning |
|---|---|---|
| `ICEGRES_WRITE_BUFFER_MS` · `--write-buffer-ms` | `0` (fully synchronous) | Buffered writes: INSERTs ack after an in-memory append, group-committed every N ms. An unclean kill loses ≤ N ms of acked writes (WARN on enable). |
| `ICEGRES_WRITE_BUFFER_MAX_ROWS` | `50000` (`0`/invalid → the default) | Row threshold that forces an early flush before the time interval elapses. |
| `ICEGRES_TAIL_DIR` · `--tail-dir` | off | Durable local fsync'd per-table WAL written before each buffered ack, replayed on boot. Single-node durability (losing the node/disk still loses the un-flushed tail). |
| `ICEGRES_TAIL_URL` · `--tail-url` | off | Durable Postgres-backed tail (mutually exclusive with `--tail-dir`); survives losing the compute node. An unreachable tail database blocks buffered writes (errors, never silent loss). |
| `ICEGRES_TAIL_QUORUM` · `--tail-quorum` | off | Three `host:port` `icekeeperd` acceptors; each buffered write is fsynced by 2 of 3 before ack — survives losing any single node incl. this one. |
| `ICEGRES_TAIL_API_PORT` · `--tail-api-port` | off | Serve the open tail read API (TailSnapshot/TailSubscribe over Arrow Flight); requires buffered mode + a durable tail. Auth rides `--auth-file`. |

## HA & fleet

| Env var · flag | Default | Meaning |
|---|---|---|
| `ICEGRES_TAIL_QUORUM_TIMEOUT_MS` | `10000` (floor `1000`) | How long a quorum append waits for a quorum of flush acks before one internal re-election, then poisoning the tail. |
| `ICEGRES_PEER_TAILS` · `--peer-tail` | off | Comma-separated tail APIs of buffering peer computes to mirror into this reader's scans (fleet overlays; a dead/silent peer falls back to commit cadence with one WARN). |
| `ICEGRES_PEER_TAIL_USER` | none (connect without credentials) | Single basic-auth username the `--peer-tail` subscriber presents to authed peers. |
| `ICEGRES_PEER_TAIL_PASSWORD` | empty (when user is set) | Password paired with `ICEGRES_PEER_TAIL_USER`. |

## Observability

| Env var | Default | Meaning |
|---|---|---|
| `RUST_LOG` | `info` | Standard `tracing` `EnvFilter` log-level filter (all three binaries). Each connection runs in a correlation span (`conn` id + peer). |
| `ICEGRES_LOG_FORMAT` | human-readable (`json` = structured lines) | Log output format for the `icegres` binary; `json` for log shippers (`icegresd`/`icekeeperd` log human-readable only). |
| `ICEGRES_SLOW_QUERY_MS` | `1000` (`0` disables) | Threshold above which a query logs a slow-query WARN and increments `icegres_queries_slow_total`. |
| `ICEGRES_QUERY_TIMING` | off | Emit per-stage timing diagnostic lines for reads and writes (parse/plan/execute, commit stages). Reads are buffered rather than streamed while enabled — diagnostics only, never in production. |

Metrics are exported in Prometheus format on `--health-port` at `/metrics`
(queries, connections, commit conflicts, freshness age, plan/result-cache
hits/misses, peer-tail ages).

## Maintenance tuning (`icegres maintain`)

Recurring knobs an operator running maintenance on a schedule will tune
(flag-only, no env bindings; all support `--execute` vs dry-run — see the crate
README's maintenance section for the workflow):

| Flag | Default | Meaning |
|---|---|---|
| `maintain compact --target-file-mb` | `128` | Target output data-file size for bin-pack compaction. |
| `maintain compact --min-input-files` | `2` | Minimum small files in a group before it is worth rewriting. |
| `maintain expire-snapshots --keep` | `10` | Newest snapshots to keep by commit time (branch/tag heads are kept regardless of age). |
| `maintain remove-orphans --older-than-hours` | `72` | Only delete unreferenced files older than this (the grace window that keeps in-flight commits safe). |

---

## Control plane daemon — `icegresd`

The optional wake-on-connect / scale-to-zero / branch-routing proxy. All knobs
are `ICEGRESD_*` (note the `D`). See [`p3-ha-scope.md`](p3-ha-scope.md) and
[`deployment.md`](deployment.md).

| Env var · flag | Default | Meaning |
|---|---|---|
| `ICEGRESD_HOST` · `--host` | `0.0.0.0` | Public listener bind address. |
| `ICEGRESD_PORT` · `--port` | `5432` | Public port clients connect to. |
| `ICEGRESD_MAX_CONNECTIONS` · `--max-connections` | `512` (`0` disables the cap) | Hard ceiling on accepted client sessions. Once full, the accept loop waits for a session to end while still responding to shutdown signals. |
| `ICEGRESD_ICEGRES_BIN` · `--icegres-bin` | sibling `icegres`, else PATH | Path to the `icegres` binary to spawn. |
| `ICEGRESD_COMPUTE_HOST` · `--compute-host` | `127.0.0.1` | Host computes bind/are dialed on (plain TCP; keep local). |
| `ICEGRESD_MAIN_PORT` · `--main-port` | `5439` | Fixed port of the main compute. |
| `ICEGRESD_IDLE_SHUTDOWN_SECS` · `--idle-shutdown-secs` | `300` | `--idle-shutdown-secs` passed to every spawned compute. |
| `ICEGRESD_WAKE_TIMEOUT_MS` · `--wake-timeout-ms` | `10000` | Budget for a spawned/scaled compute to accept TCP. |
| `ICEGRESD_STATUS_FILE` · `--status-file` | `<tmpdir>/icegresd-status.json` | JSON status file rewritten on every state change (also read by `icegresd status`). |
| `ICEGRESD_POOL_SIZE` · `--pool-size` | `8` (`0` disables pooling) | Warm pre-handshaked backend connections kept per compute. Silently forced off (with a boot WARN) when `ICEGRES_AUTH_FILE` is set in icegresd's environment — SCRAM sessions cannot be pre-authenticated, so all sessions go direct. |
| `ICEGRESD_POOL_USER` · `--pool-user` | `postgres` | `user` startup param the pool warms sessions with (mismatch → direct connect). |
| `ICEGRESD_POOL_IDLE_SECS` · `--pool-idle-secs` | `60` | Drain the warm pool after this many zero-client-session seconds. |
| `ICEGRESD_HEALTH_CHECK_MS` · `--health-check-ms` | `0` (off) | Process mode only: poll each compute's `/health` every N ms; 3 consecutive failures trigger supervised replacement. Auto-defaults to `1000` when `--lease-quorum` is set together with `ICEGRES_TAIL_QUORUM` (process mode). In k8s mode setting it is **refused at boot** — the kubelet liveness probe owns compute health there. |
| `ICEGRESD_LEASE_QUORUM` · `--lease-quorum` | none (single-instance mode) | Three `icekeeperd` acceptors forming a leader-lease log for icegresd redundancy. |
| `ICEGRESD_LEASE_TTL_MS` · `--lease-ttl-ms` | `6000` (floor `1000`) | Lease TTL; leader renews every TTL/3, standbys take over after ≥ TTL frozen. |
| `ICEGRESD_LEASE_HOLDER_ID` · `--lease-holder-id` | `icegresd-<pid>@<host>:<port>` | Diagnostic holder id written into lease records. |
| `ICEGRESD_READ_REPLICAS_MAX` · `--read-replicas-max` | `0` (off) | Max stateless read computes for `<db>:ro` autoscaling-lite. |
| `ICEGRESD_READ_REPLICA_SESSIONS` · `--read-replica-sessions` | `4` | Active-session threshold per replica before spawning another. |
| `ICEGRESD_REPLICA_PEER_TAIL` · `--replica-peer-tail` | none | `--peer-tail` address passed to every read replica. |
| `ICEGRESD_REPLICA_FRESHNESS_MS` · `--replica-freshness-ms` | none | `--freshness-ms` passed to every read replica. |
| `ICEGRESD_K8S_COMPUTE` · `--k8s-compute` | off | Kubernetes mode: the main compute is a remote pod (never forked/supervised). |
| `ICEGRESD_K8S_SCALE` · `--k8s-scale` | none | apps/v1 scale target (`deployments/<name>` or `statefulsets/<name>`) for wake-on-connect + idle scale-to-zero (implies `--k8s-compute`). |

## Quorum acceptor daemon — `icekeeperd serve`

The consensus acceptor for `--tail-quorum` / `--lease-quorum`. Plain TCP, no
TLS/auth — deploy on a trusted/loopback network. See
[`p1-open-tail-scope.md`](p1-open-tail-scope.md).

| Env var · flag | Default | Meaning |
|---|---|---|
| `ICEKEEPER_HOST` · `--host` | `127.0.0.1` | Bind address. |
| `ICEKEEPER_PORT` · `--port` | **required** | Bind port. |
| `ICEKEEPER_DATA_DIR` · `--data-dir` | **required** | Data directory (control file + log segments), exclusively `flock`ed. |
| `ICEKEEPER_NODE_ID` · `--node-id` | `0` | Diagnostic node id, pinned into the data dir on first start. |

---

## Test / internal only — not operator knobs

These exist for the test suite, CI, or build and are **not** meant to be set in
production: `ICEGRES_DML_INJECT_CONFLICT`, `ICEGRES_MERGE_INJECT_CONFLICT`,
`ICEGRES_COMPACT_INJECT_CONFLICT` (409-retry fault injection),
`ICEGRES_TXN_DISABLE_ATOMIC`, `ICEGRES_LIVE_TESTS`, `ICEGRES_TEST_PG_URL`,
`ICEGRESD_K8S_SA_DIR` / `ICEGRESD_K8S_API_URL` (test service-account overrides;
real deployments use the injected `KUBERNETES_SERVICE_HOST`/`_PORT`),
`ICEGRES_HELM_BIN` / `ICEGRES_KUBECONFORM_BIN` / `ICEGRES_K8S_SCHEMA_DIR`
(tool paths for the `tests/helm.sh` chart gate), and the build-time
`ICEGRES_LONG_VERSION`.
