# icegres deployment guide

Operating icegres as a GA service: how to build and run the image, wire up
health/readiness/metrics, size resources, shut down cleanly, secure the
endpoints, and keep table metadata from growing without bound.

This is the operator's counterpart to `icegres/README.md` (feature/flag
reference), `docs/cqrs-topology.md` (read/write topology), and
`docs/limitations.md` (what icegres deliberately does NOT do). Every knob below
is a flag on `icegres serve` with a matching `ICEGRES_*` environment variable —
the env var is the deployment-friendly form and is what the examples use.

---

## 1. Build and run

### Container image

The repository ships a multi-stage `Dockerfile` (pinned `rust:1.96.1` builder →
`debian:bookworm-slim` runtime, non-root UID 10001, CA certificates + tini for
signal forwarding). It builds both binaries (`icegres`, `icegresd`).

```bash
docker build -t icegres:$(git rev-parse --short HEAD) .

docker run --rm \
  -p 5439:5439            `# pgwire`            \
  -p 50051:50051          `# Arrow Flight SQL`  \
  -p 8080:8080            `# health/metrics`    \
  -e ICEGRES_CATALOG_URI=https://catalog.example.com/catalog \
  -e ICEGRES_WAREHOUSE=prod \
  -e ICEGRES_S3_ENDPOINT=https://s3.example.com \
  -e ICEGRES_S3_ACCESS_KEY=... \
  -e ICEGRES_S3_SECRET_KEY=... \
  -e ICEGRES_MEMORY_LIMIT_MB=3072 \
  -e ICEGRES_MAX_CONNECTIONS=512 \
  -v /etc/icegres:/etc/icegres:ro \
  icegres:$(git rev-parse --short HEAD) \
  serve --host 0.0.0.0 --health-port 8080 \
        --auth-file /etc/icegres/users --authz-file /etc/icegres/policy
```

`icegres --version` prints the exact commit the binary was built from
(`icegres 0.1.0 (<sha> <date>)`, stamped by `build.rs`), so a running container
is always traceable to a source revision.

### From source

```bash
cd icegres && cargo build --release
./target/release/icegres serve --host 0.0.0.0 --health-port 8080
```

The toolchain is pinned in `rust-toolchain.toml` (1.96.1) so every build is
reproducible against the large pinned dependency matrix.

---

## 2. Connecting to the lakehouse

| Env var | Default | Purpose |
|---|---|---|
| `ICEGRES_CATALOG_URI` | `http://127.0.0.1:8181/catalog` | Iceberg REST catalog base URI (Lakekeeper) |
| `ICEGRES_WAREHOUSE` | `lakehouse` | Warehouse name registered in the catalog |
| `ICEGRES_S3_ENDPOINT` | `http://127.0.0.1:9000` | S3-compatible object store for table data |
| `ICEGRES_S3_ACCESS_KEY` / `_SECRET_KEY` | dev creds | Object-store credentials |
| `ICEGRES_S3_REGION` | `us-east-1` | Object-store region |

Point these at your production catalog and object store. TLS to both is
verified with rustls against the system trust store (the image ships
`ca-certificates`); no OpenSSL is linked.

---

## 3. Health, readiness, and metrics

Enable the HTTP side-channel with `--health-port <port>` (env
`ICEGRES_HEALTH_PORT`). It serves three path-routed endpoints on that port:

| Path | Meaning | Response |
|---|---|---|
| `/health` (and any other path) | **Liveness** — the process is up and the listener is bound. Does NOT touch the catalog. | `200 ok` |
| `/ready`, `/readyz` | **Readiness** — a bounded `list_namespaces` round-trip to the catalog (3 s timeout). | `200 ready` / `503 not ready` |
| `/metrics` | Prometheus text exposition. | `icegres_queries_total`, `icegres_connections_total`, `icegres_connections_active`, `icegres_commit_conflicts_total`, `icegres_queries_in_flight` (gauge), `icegres_queries_slow_total`, `icegres_query_duration_ms_total` |

Query observability: every connection is wrapped in a `conn` correlation span
(id + peer) so concurrent-connection logs de-multiplex, and each query is
timed — one exceeding `ICEGRES_SLOW_QUERY_MS` (default 1000; `0` disables)
logs a slow-query WARN (kind + duration + connection) and bumps
`icegres_queries_slow_total`. `icegres_queries_in_flight` is the live
concurrency; if it stays high while qps is low, queries are stuck, and a
forced shutdown logs each still-running query by kind + age.

Kubernetes example — liveness must NOT depend on the catalog (a catalog blip
should not restart healthy pods), readiness pulls a pod out of rotation when
its dependency is down:

```yaml
livenessProbe:
  httpGet: { path: /health, port: 8080 }
  periodSeconds: 10
readinessProbe:
  httpGet: { path: /ready, port: 8080 }
  periodSeconds: 5
```

Scrape metrics with a `prometheus.io/scrape` annotation on port 8080, path
`/metrics`. Ship logs as structured JSON with `ICEGRES_LOG_FORMAT=json`
(defaults to human-readable text); log level via `RUST_LOG` (e.g.
`RUST_LOG=info,icegres=debug`).

---

## 4. Graceful shutdown

On `SIGTERM`/`SIGINT` icegres stops accepting new connections and drains
in-flight requests for up to 30 s before exiting (both the pgwire and Flight
listeners). In buffered-write mode (`--write-buffer-ms > 0`) it also flushes
the write buffer after draining, so a clean stop commits every acked row —
only an *unclean* kill drops the in-memory window. Give the orchestrator enough
grace so a rolling deploy never severs a query mid-flight:

```yaml
terminationGracePeriodSeconds: 40   # > the 30 s drain window
```

The image runs the binary under `tini` as PID 1 so the signal is actually
delivered.

---

## 5. Resource bounds

icegres runs DataFusion queries in-process; a heavy sort/join/aggregate must
degrade, not OOM-kill the pod.

| Env var | Default | Effect |
|---|---|---|
| `ICEGRES_MEMORY_LIMIT_MB` | 70% of `/proc/meminfo` (1 GiB floor) | Size of the `FairSpillPool`. `0` = unbounded (not recommended). Over budget → operators spill to disk, then `ResourcesExhausted`, never OOM. |
| `ICEGRES_MAX_CONNECTIONS` | 512 | Accept-loop concurrency cap; excess connections wait in the OS backlog instead of spawning unbounded per-connection state. |

Set the pool a few hundred MB below the container memory limit to leave room
for the Arrow decode buffers and the runtime. Give the container a writable
scratch volume for spill (DataFusion's disk manager uses the system temp dir).

Example k8s resources with headroom for spill and Arrow buffers:

```yaml
resources:
  requests: { memory: "4Gi", cpu: "2" }
  limits:   { memory: "4Gi" }
env:
  - { name: ICEGRES_MEMORY_LIMIT_MB, value: "3072" }   # ~0.75 × 4Gi
```

---

## 6. Catalog resilience

The catalog is on the read path (every scan resolves the table's current
snapshot). A Lakekeeper blip must surface as a bounded error, not a hang.

| Env var | Default | Effect |
|---|---|---|
| `ICEGRES_CATALOG_TIMEOUT_MS` | 5000 | Per `load_table` timeout. `0` = no timeout. |
| `ICEGRES_CATALOG_RETRIES` | 2 | Retries with exponential backoff on timeout/failure. |
| `ICEGRES_STALE_READ_ON_CATALOG_ERROR` | off | If `true`, serve the last cached snapshot when the catalog is unreachable (availability over exact freshness). Default is to propagate the error. |

Object-store request timeouts are NOT yet configurable (a limitation of the
pinned `iceberg-storage-opendal` 0.9.1) — see `docs/limitations.md`.

---

## 7. Security posture

icegres is **secure-by-default for remote binds**: `serve`/`flight-serve`
refuse to bind a non-loopback interface with authentication disabled unless you
pass `--insecure`. In production always enable auth.

| Concern | Flag / env | Notes |
|---|---|---|
| Authentication | `--auth-file` / `ICEGRES_AUTH_FILE` | SCRAM-SHA-256 against a `user:password` file. Without it the server is permissive and logs a `WARN`. |
| Authorization (ReBAC) | `--authz-file` / `ICEGRES_AUTHZ_FILE` | Lakekeeper-style read/write/drop/own grants gate every statement (`42501` on deny), enforced identically on pgwire AND Flight SQL. Requires `--auth-file`. Part of the `managed` build feature. |
| TLS (pgwire) | `--tls-cert` / `--tls-key` | rustls; clients opt in with `sslmode=require`/`verify-full`. Any TLS setup error aborts startup (no silent plaintext fallback). |
| TLS (Flight SQL) | `flight-serve --tls-cert`/`--tls-key` | In-process TLS (same rustls stack as pgwire, `h2` ALPN); `grpc+tls://` clients connect directly. A front proxy still works if preferred. |
| Auth brute-force | per-IP backoff (automatic) | With `--auth-file`, repeated bad credentials from an IP get an escalating delay; failures decay after 60 s. No config. |
| Remote-bind guard | `--insecure` | Required to bind `0.0.0.0` with auth off. Do not set it in production. |

Mount credential/policy files read-only (`-v /etc/icegres:/etc/icegres:ro`) and
protect them like `.pgpass`.

---

## 8. Table maintenance (snapshot expiry + orphan GC + compaction)

Every write adds an Iceberg snapshot forever; unbounded, `$snapshots` and the
metadata JSON the catalog re-reads on every table open grow without limit. Run
expiry periodically per hot table:

```bash
icegres maintain expire-snapshots demo.trips --keep 50
```

It is a metadata-only, live-safe REST commit (keeps the newest `--keep` by
commit time plus every snapshot still reachable from a branch/tag ref; anchored
so a concurrent write can never strand a ref). Schedule it as a `CronJob`
running the same image with the `maintain expire-snapshots` command.

Expiry strands the expired snapshots' data/manifest files in object storage;
the GC step reclaims them:

```bash
# dry-run first: prints orphan count + bytes + sample paths, deletes nothing
icegres maintain remove-orphans demo.trips
# then delete; only objects older than the grace window are eligible
icegres maintain remove-orphans demo.trips --execute --older-than-hours 72
```

`remove-orphans` lists the table's S3 prefix and deletes only what NO retained
snapshot, branch, or tag references (data files, manifests, manifest lists —
including files still named by DELETED manifest entries — the metadata-JSON
log, and statistics files all count as live). The guard model, plainly:

- **The grace window IS the in-flight-commit guard.** Keep the default
  72-hour `--older-than-hours` in production: only an object's young age
  protects files written by commits — icegres' own or a foreign writer's
  (Spark, Trino) — that have not landed in the catalog yet; shrink it only
  on tables no foreign engine writes to.
- **Clock skew is covered by allowance + probe.** A fixed 15-minute
  clock-skew allowance is folded into the cutoff (effective cutoff =
  now − grace − 15 min), and `--execute` measures the real host-vs-store
  skew by writing, stat'ing, and deleting a tiny probe object under the
  table's `metadata/` prefix — the run aborts if the skew exceeds the
  allowance (or if the probe itself fails).
- **`--unsafe-grace` is for quiescent tables only** (e.g. test rigs): it
  permits `--execute` with a sub-1-hour grace window and drops the skew
  allowance. On a table with concurrent writers it WILL lose in-flight
  files — never pass it in a production CronJob.
- **Foreign-bucket references abort.** If table metadata records a file
  path outside the listed bucket (bucket aliases, endpoint rewrites), the
  run aborts: liveness cannot be verified against a listing that cannot
  see the file.

The command fails closed everywhere else too (unreadable metadata or
manifests abort the run; unknown-age or unrecognized objects are never
deleted), so a failed run leaves everything in place and is safe to re-run.
Schedule it as a second `CronJob` after the expiry one — expiry first, GC
after — per hot table.

The third command is bin-pack compaction. Tables written one small commit at
a time (per-statement INSERTs, foreign micro-batchers) fragment into many
under-sized Parquet files, and every extra file costs a scan an object-store
round trip. Compaction rewrites them in place:

```bash
# dry-run first: prints the plan (candidates per partition, projected
# outputs), rewrites nothing
icegres maintain compact --table demo.trips
# then rewrite + commit; tune the output size / payoff bar if needed
icegres maintain compact --table demo.trips --execute
icegres maintain compact --table demo.trips --target-file-mb 256 --min-input-files 4 --execute
```

Each partition's data files under `--target-file-mb` (default 128) are
rewritten into ~target-size files as ONE `replace` snapshot — the row set is
identical, and the old files stay time-travel-readable until the expiry + GC
pair above reclaims them (they are NOT orphans while any retained snapshot
references them; the GC understands this). Live-safe by construction: the
commit is anchored to the exact snapshot the plan was computed against, so
any concurrent writer — a foreign engine, a serving endpoint's DML or
buffered flush — makes the compact abort cleanly with nothing changed
(first-committer-wins; just re-run it in a quieter window). It refuses loudly
on tables bearing foreign merge-on-read delete manifests (icegres cannot
apply those deletes), on partitioned tables, and on schema-divergent tables —
manifests carrying a schema id other than the current one (a foreign engine
evolved the schema); rewrite those files under the current schema first.
Schedule it per hot
fragmented table BEFORE the expiry + GC pair — compact, then expire, then GC
reclaims the replaced files one cycle later. Buffered/tail mode already keeps
new files well-sized at the source (one file per flush window), so computes
running with a write buffer need compaction far less often.

---

## 9. Topology and scale-to-zero

For the read-replica / single-writer CQRS layout (many stateless read computes
over the shared lake, writes anchored first-committer-wins), see
`docs/cqrs-topology.md`.

`icegresd` is the optional control plane: a wake-on-connect proxy that scales
computes to zero on idle and re-wakes them on the next connection, with
branch-endpoint routing and warm session pooling. `icegres serve
--idle-shutdown-secs N` makes a compute exit cleanly after N idle seconds so a
supervisor (icegresd, systemd socket activation, or a k8s scale-to-zero
controller) can implement scale-from-zero.

### Quorum tail topology (3 × `icekeeperd`)

For consensus-class buffered-write durability (`--tail-quorum`), run three
acceptor daemons on **independent nodes/disks** — placing two on one node
quietly reduces the promise to that node's survival:

```
node A:  icekeeperd serve --host 10.0.0.11 --port 5471 --data-dir /var/lib/icekeeper --node-id 1
node B:  icekeeperd serve --host 10.0.0.12 --port 5471 --data-dir /var/lib/icekeeper --node-id 2
node C:  icekeeperd serve --host 10.0.0.13 --port 5471 --data-dir /var/lib/icekeeper --node-id 3

compute: icegres serve --write-buffer-ms 200 \
           --tail-quorum 10.0.0.11:5471,10.0.0.12:5471,10.0.0.13:5471
```

Acceptors are tiny (one fsync'd record log + a JSON control file; no
catalog/S3 access, a few MB binary) and restart-safe: kill -9 loses nothing
acked, and a restarted acceptor is caught up by the live server. Keep the
acceptor ports on a **trusted private segment** — the protocol has no
TLS/authentication yet (docs/limitations.md). A second compute pointed at
the same quorum FENCES the first (its INSERTs fail with "superseded by a
newer server") — that is the intended failover story: start the
replacement, the old one loses the log, no lock files to clean up. Run the
acceptors under a restarting supervisor (systemd `Restart=always` or a k8s
StatefulSet with one PVC per acceptor); writes survive any single failure
and pause (statement errors, no loss) only when two of three are down.

How long an append waits for the 2-of-3 ack before giving up is tunable:
`ICEGRES_TAIL_QUORUM_TIMEOUT_MS` (default 10000, floor 1000). A stalled
append first attempts one internal re-election (recovering from a crashed
competitor's term bump without a restart); a second timeout poisons the
tail (statement errors until restart — the ambiguous record is replayed
exactly once by the restart's election).

---

## 10. Quick reference — operational env vars

```
# Lakehouse
ICEGRES_CATALOG_URI, ICEGRES_WAREHOUSE
ICEGRES_S3_ENDPOINT, ICEGRES_S3_ACCESS_KEY, ICEGRES_S3_SECRET_KEY, ICEGRES_S3_REGION
# Listeners
ICEGRES_HOST, ICEGRES_PORT, ICEGRES_HEALTH_PORT
# Security
ICEGRES_AUTH_FILE, ICEGRES_AUTHZ_FILE, ICEGRES_TLS_CERT, ICEGRES_TLS_KEY   (+ --insecure)
# Resources / resilience
ICEGRES_MEMORY_LIMIT_MB, ICEGRES_MAX_CONNECTIONS
ICEGRES_CATALOG_TIMEOUT_MS, ICEGRES_CATALOG_RETRIES, ICEGRES_STALE_READ_ON_CATALOG_ERROR
# Semantics
ICEGRES_TXN_STRICT, ICEGRES_ENFORCE_PK, ICEGRES_BRANCH
ICEGRES_WRITE_BUFFER_MS, ICEGRES_WRITE_BUFFER_MAX_ROWS
ICEGRES_TAIL_DIR, ICEGRES_TAIL_URL, ICEGRES_TAIL_QUORUM   (durable tail backends)
ICEGRES_TAIL_QUORUM_TIMEOUT_MS   (quorum-ack timeout; default 10000, min 1000)
ICEKEEPER_HOST, ICEKEEPER_PORT, ICEKEEPER_DATA_DIR, ICEKEEPER_NODE_ID   (icekeeperd)
ICEGRES_IDLE_SHUTDOWN_SECS
# Observability
ICEGRES_LOG_FORMAT=json, RUST_LOG
```
