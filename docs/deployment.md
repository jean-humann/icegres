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
| `/metrics` | Prometheus text exposition. | `icegres_queries_total`, `icegres_connections_total`, `icegres_connections_active`, `icegres_commit_conflicts_total` |

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
listeners). Give the orchestrator enough grace so a rolling deploy never severs
a query mid-flight:

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
| TLS (Flight SQL) | terminate-in-front | Run behind a TLS-terminating gRPC proxy; in-process Flight TLS is not wired against the pinned tonic 0.14 stack. |
| Remote-bind guard | `--insecure` | Required to bind `0.0.0.0` with auth off. Do not set it in production. |

Mount credential/policy files read-only (`-v /etc/icegres:/etc/icegres:ro`) and
protect them like `.pgpass`.

---

## 8. Table maintenance (snapshot expiry)

Every write adds an Iceberg snapshot forever; unbounded, `$snapshots` and the
metadata JSON the catalog re-reads on every table open grow without limit. Run
expiry periodically per hot table:

```bash
icegres maintain expire-snapshots demo.trips --keep 50
```

It is a metadata-only, live-safe REST commit (keeps the newest `--keep` by
commit time plus every snapshot still reachable from a branch/tag ref; anchored
so a concurrent write can never strand a ref). Schedule it as a `CronJob`
running the same image with the `maintain expire-snapshots` command. The
expired snapshots' data/manifest files are left in object storage for a
separate orphan-file GC — see `docs/limitations.md`.

There is intentionally no `compact` command yet (pinned iceberg-rust 0.9.1 has
no rewrite-files action); drop-and-reseed remains the canonicalization path.

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
ICEGRES_IDLE_SHUTDOWN_SECS
# Observability
ICEGRES_LOG_FORMAT=json, RUST_LOG
```
