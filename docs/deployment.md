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
signal forwarding). It builds all three binaries (`icegres`, `icegresd`,
`icekeeperd`) — one image runs any role, which is what the Helm chart
(§11) relies on.

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
apply those deletes), on partitioned tables, and on schema-divergent files —
every candidate input's Parquet footer is verified against the current
schema's field ids before anything is staged (a non-current manifest schema
id refuses earlier as a fast path), so files physically written under an
older schema version (a foreign engine evolved the schema) are refused even
when later manifest rewrites re-stamped them; rewrite those files under the
current schema first.
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

---

## 11. Kubernetes: the Helm chart + HA runbook

The chart lives at `deploy/helm/icegres` (apiVersion v2). One `helm
install` deploys the whole topology; the HA values light up exactly the
flag-gated machinery the process-mode e2e suite proves (`icegres/tests/
e2e.sh` section (ha)) — same code paths, pod-shaped.

```bash
# Build and push the image first (no public registry is published):
docker build -t registry.example.com/icegres:0.1.0 .
docker push registry.example.com/icegres:0.1.0

helm install icegres deploy/helm/icegres \
  --namespace icegres --create-namespace \
  --set image.repository=registry.example.com/icegres \
  --set catalog.uri=https://catalog.example.com/catalog \
  --set s3.endpoint=https://s3.example.com \
  --set s3.existingSecret=my-s3-creds
```

Clients connect to the `<release>-icegres` Service (pgwire through
icegresd). The writer compute is a StatefulSet behind a headless
`-writer` Service; icegresd runs in **k8s mode** (`--k8s-compute`): it
never forks processes — it dials the writer Service and, with
`k8sScaling.enabled`, wakes/parks the writer by patching its scale
subresource (RBAC scoped to exactly that one object, verbs
`get`+`patch`).

### Install matrix

| Values | What you get | HA promise |
|---|---|---|
| defaults | icegresd + 1 always-on writer | none (crash = kubelet restart; buffered window would be lost, so buffering is off by default) |
| `tail.mode=dir` + `writer.writeBufferMs` | buffered writes, WAL on a writer PVC | survives writer crash/restart on the same volume; **no** automated node failover |
| `tail.mode=quorum` + `writer.writeBufferMs` | `-keeper` icekeeperd trio (anti-affinity required, PDB minAvailable 2), 2-of-3 fsync per ack | acked rows survive losing the writer pod/node and any single acceptor; **automated failover** at the container/pod level — a HARD writer-node loss needs one manual force-delete on clusters without node-lifecycle GC (runbook below) |
| `ha.enabled` | ≥2 icegresd + dedicated `-lease` trio, leader lease | endpoint survives icegresd pod/node loss within ~1–2× `ha.leaseTtlMs` |
| `k8sScaling.enabled` | scale-subresource RBAC + wake/park wiring | writer scale-to-zero: parks after `icegresd.idleShutdownSecs` idle, wakes on the next connection |
| `computes.readReplicas=N` | `-read` Deployment + Service, peer-tailed to the writer with `computes.freshnessMs` bounds | read capacity scales independently; add a plain HPA (values comment) — deliberately no chart-managed HPA |
| `auth.enabled` / `tls.*` | SCRAM on every compute, TLS at the computes | without auth the computes run `ICEGRES_INSECURE=true` and anything in-cluster can connect |

### HA runbook — what fails how, what heals itself, what pages a human

**Writer container dies or pod is deleted (OOM, `kill -9`, eviction,
liveness kill).** Kubelet restarts the container / the StatefulSet
controller replaces the pod. The replacement's `--tail-quorum` open()
FENCES the old term at the acceptors and replays the
acked-but-unflushed window **before** its pgwire listener binds — zero
acked-row loss (quorum mode; `dir` replays only on the same PVC; with
no tail there is no window to lose). Clients see connection resets and
reconnect; nobody is paged.

**Writer NODE dies or becomes unreachable (power/kernel/network).**
Honesty required here: a StatefulSet pod on an unreachable node is
**never replaced automatically** — the at-most-one guarantee makes the
controller wait until the old pod object is confirmed gone, and a dead
kubelet can never confirm the pending delete. On managed clouds the
node-lifecycle controller usually deletes the Node object after a few
minutes, after which the pod reschedules; on self-hosted/bare-metal
clusters (no such GC) the writer sits `Terminating` **indefinitely** —
an unbounded write outage. **This pages a human**: confirm the node is
really down, then

```bash
kubectl -n <ns> delete pod <release>-writer-0 --grace-period=0 --force
# (or delete the Node object / apply the out-of-service taint)
```

Safe in quorum mode even if the old machine later comes back: the
replacement's election has already fenced the old writer (it can never
ack again), and the acked window replays from the acceptors. For
`tail.mode=dir` the force-delete only helps if the PVC can follow the
pod to another node — dir stays single-node HA (limitations.md).

**Writer wedges without dying** (fenced by a competing writer, or its
quorum went unreachable → the tail poisons itself: the process still
accepts TCP but can never ack a buffered write again). Its `/health`
answers `503 tail unhealthy: ...`, the liveness probe fails 3×, the
kubelet restarts the container, fence-and-replay as above. This is the
k8s translation of `icegresd --health-check-ms` — the kubelet is the
health checker, so that flag is refused in k8s mode. Heals itself.

**One acceptor of three dies.** Writes continue (2-of-3). The
StatefulSet restarts it; the live proposer catches it up. The PDB stops
voluntary disruptions from taking a second one. Heals itself — but **a
second acceptor down stops buffered writes** (statement errors,
backpressure, never silent loss) until one returns: that pages a human
if it persists (check PVCs/nodes; `kubectl -n <ns> get pods -l
app.kubernetes.io/component=keeper`).

**icegresd leader pod dies (`ha.enabled`).** A standby observes the
lease log frozen for ≥ TTL, takes it over (fenced election), flips its
leadership readiness probe, and enters the Service endpoints; the old
leader — if merely partitioned, not dead — fails its next renew and
demotes (refuses clients with a retryable `57P03`). Expect ~1–2× TTL
(default 6 s → ~6–12 s) of connection errors; clients retry/reconnect.
Heals itself. Both icegresd instances point at the SAME writer Service,
so no compute is restarted on control-plane failover.

**icegresd leader NODE drained (`kubectl drain`, managed node-pool
upgrade, cluster-autoscaler scale-down).** The eviction is allowed by
design. Leadership readiness pins the icegresd Ready count at exactly 1,
so ANY availability-demanding PDB would compute `disruptionsAllowed: 0`
forever and deadlock the drain — the eviction API refuses (429) before
any signal reaches the pod, the leader keeps renewing, and the standby
can never turn Ready first; the chart's icegresd PDB is therefore
`maxUnavailable: 100%`. On eviction a short preStop keeps the leader
serving while its endpoint removal propagates, then SIGTERM stops lease
renewal immediately (silence is the release — standbys watch the lease
log freeze) and the pre-existing standby takes over exactly as in the
leader-kill entry above: expect ~1–2× TTL (default 6 s → ~6–12 s) of
connection errors, then the standby is 1/1. Heals itself; nobody is
paged. Honest corollary: a parallel drain that evicts BOTH icegresd
instances at once is also allowed — cost is one pod reschedule plus the
same takeover.

**The lease trio loses quorum.** The leader demotes within ~TTL even if
the data path is healthy (an unrenewable lease cannot exclude a second
leader) — the endpoint goes dark with `57P03` until the lease trio is
back. Pages a human. (The data trio and the lease trio are DISJOINT
StatefulSets by construction; icegresd refuses a shared address at
boot.)

**Catalog or S3 outage.** Computes stay alive (liveness never touches
the catalog) but turn unready (`/ready` 503, catalog-aware). The writer
stays REACHABLE through it: its headless Service publishes not-ready
addresses (the wake signal is "no pod", not "unready pod"), so icegresd
keeps dialing/splicing and buffered writes keep acking against the tail
(the catalog is needed at flush, not at ack) — established sessions and
new connections both work, statements that need fresh metadata error.
Read replicas with `computes.freshnessMs > 0` deliberately keep their
readiness OFF the catalog (`/health`): they exist to serve bounded-stale
reads through exactly this outage (`ICEGRES_STALE_READ_ON_CATALOG_ERROR`
defaults on with a freshness bound). Exact-freshness replicas
(`freshnessMs: 0`) leave the `-read` Service until the catalog returns.
Heals itself when the dependency returns; page whoever owns the catalog.

**What is NOT automated, stated plainly** (see `docs/limitations.md`):
writer **node**-loss replacement on clusters without node-lifecycle GC
(one `kubectl delete pod --force` — the StatefulSet bullet above);
`tail.mode=dir` node failover (manual: the PVC must follow); a
`helm upgrade` resets a parked writer to 1 replica (the idle loop parks
it again); scale-to-zero counts only traffic THROUGH icegresd — clients
connecting to the writer Service directly (e.g. TLS-require) hold no
idle clock; branch endpoints are process-mode only (deploy a per-branch
compute instead); the acceptor protocol has no TLS/auth — keep it
namespace-internal (enable `networkPolicy.enabled`).

### Chart validation (offline) — and an honest label

`tests/helm.sh` is the CI gate for the chart: pinned `helm` v3.21.3 and
`kubeconform` v0.8.0 built from source (shas hardcoded; overridable with
`ICEGRES_HELM_BIN`/`ICEGRES_KUBECONFORM_BIN`/`ICEGRES_K8S_SCHEMA_DIR`),
`helm lint`, committed golden renders for five values profiles
(`deploy/helm/tests/`), strict schema validation of every manifest
against Kubernetes v1.31.0 AND v1.34.0 (vendored schemas fetched by
commit sha), and invariant asserts (anti-affinity, PDBs, probe paths,
runAsNonRoot everywhere, RBAC scoped to the one scale subresource, no
tail/buffer env on read replicas). **It renders and validates manifests;
it does not run a cluster.** The procedure below is how an operator
smoke-tests the chart on a real cluster — it is NOT executed by this
repository's CI (the gate box has no Docker daemon or cluster).

### Real-cluster smoke procedure (kind) — not CI-run

```bash
kind create cluster --name icegres
docker build -t icegres:smoke . && kind load docker-image icegres:smoke --name icegres

helm install icegres deploy/helm/icegres -n icegres --create-namespace \
  --set image.repository=icegres --set image.tag=smoke \
  --set catalog.uri=http://<your-lakekeeper>/catalog \
  --set s3.endpoint=http://<your-s3> \
  --set s3.accessKey=... --set s3.secretKey=... \
  --set tail.mode=quorum --set writer.writeBufferMs=200 \
  --set k8sScaling.enabled=true --set ha.enabled=true \
  --set keeper.antiAffinity=soft --set lease.antiAffinity=soft   # single-node kind

kubectl -n icegres get pods
# expected: icegres-<hash> x2 (one 1/1 READY — the leader; one 0/1 —
# the standby, by design), icegres-writer-0 1/1, icegres-keeper-{0,1,2}
# and icegres-lease-{0,1,2} 1/1

# 1. wake-on-connect + a write through the endpoint
kubectl -n icegres run psql --rm -it --image=postgres:16 --command -- \
  psql "host=icegres port=5432 user=postgres dbname=icegres" \
  -c "create table demo.t (a int)" -c "insert into demo.t values (1)"

# 2. writer failover under a wedged tail: fence it by scaling a second
#    writer is not possible here, so kill the acceptor quorum instead:
kubectl -n icegres delete pod icegres-keeper-0 icegres-keeper-1
#    inserts error (backpressure) until the pods return; then succeed.

# 3. writer kill: the pod is REALLY replaced and acked rows survive.
#    (Do NOT `kubectl exec ... kill -9 1`: PID 1 in the container is
#    tini, and the kernel silently ignores SIGKILL sent to a PID
#    namespace's init from inside it — nothing dies and the "check"
#    passes without any failover having happened.)
OLD_UID=$(kubectl -n icegres get pod icegres-writer-0 -o jsonpath='{.metadata.uid}')
kubectl -n icegres delete pod icegres-writer-0 --grace-period=0 --force
kubectl -n icegres wait --for=condition=Ready pod/icegres-writer-0 --timeout=180s
NEW_UID=$(kubectl -n icegres get pod icegres-writer-0 -o jsonpath='{.metadata.uid}')
[ "$OLD_UID" != "$NEW_UID" ] && echo "REPLACED (new pod identity)" \
  || echo "FAIL: same pod — no failover was exercised"
# the replacement ran the quorum election + replay before binding pgwire
# (a fresh term FENCES the old writer; either line proves the path ran):
kubectl -n icegres logs icegres-writer-0 | grep -E 'recovered .* rows|nothing to replay'
kubectl -n icegres run psql2 --rm -it --image=postgres:16 --command -- \
  psql "host=icegres port=5432 user=postgres dbname=icegres" \
  -c "select count(*) from demo.t"   # acked row survived the pod's death

# 4. icegresd leader kill: the WARM STANDBY takes over within ~2x TTL.
#    Delete ONLY the leader — the standby is unready (0/1) but its pod
#    phase is still Running, so a `--field-selector status.phase=Running`
#    delete would kill BOTH pods and "validate" a cold restart instead of
#    the standby takeover ha.enabled pays two replicas for. The leader is
#    the Ready pod:
LEADER=$(kubectl -n icegres get pods -l app.kubernetes.io/component=icegresd \
  -o jsonpath='{range .items[?(@.status.conditions[?(@.type=="Ready")].status=="True")]}{.metadata.name}{"\n"}{end}')
STANDBY=$(kubectl -n icegres get pods -l app.kubernetes.io/component=icegresd \
  -o jsonpath='{range .items[*]}{.metadata.name}{"\n"}{end}' | grep -v "^$LEADER\$")
echo "leader: $LEADER  standby: $STANDBY"
kubectl -n icegres delete pod "$LEADER" --grace-period=0 --force
# the PRE-EXISTING standby must turn 1/1 within ~2x ha.leaseTtlMs — that
# is the warm standby HOLDING THE LEASE, not a restart of the deleted pod:
kubectl -n icegres wait --for=condition=Ready "pod/$STANDBY" --timeout=30s
kubectl -n icegres exec "$STANDBY" -- grep -o '"leader": true' /tmp/icegresd-status.json

# 5. scale-to-zero: with no traffic for icegresd.idleShutdownSecs,
kubectl -n icegres get statefulset icegres-writer   # replicas 0
#    then connect again (step 1) — the writer scales back to 1 and serves.

kind delete cluster --name icegres
```

## 12. `icegres verify` — re-prove the durability claims on YOUR deployment

The claims this repo makes about buffered-write durability are proven in
CI by `icegres/tests/tail_durability.sh` — on our box. `icegres verify`
re-proves them against *your* deployment: your catalog, your object
store, your tail backend, your disks. Run it **after install** and
**after any infrastructure change** that touches the durability path (new
tail backend, moved tail database, replaced acceptors, storage-class or
filesystem changes, kernel/fsync-behavior updates).

```sh
# local WAL tail: durability + exactly-once + freshness
# (fencing/failover SKIP: a local WAL has no cross-writer identity)
icegres verify --tail-dir /var/lib/icegres/verify-tail

# Postgres tail: adds one-writer fencing (advisory lock).
# Use a DEDICATED, EMPTY database on the same instance as your real tail —
# verify REFUSES a database that already carries an icegres_tail schema.
icegres verify --tail-url postgresql://user:pw@tail-host:5432/icegres_verify

# quorum tail: the full matrix incl. failover. DEDICATED acceptors only:
# verify refuses (before writing anything) if the quorum log already
# carries foreign frames, and a live writer on the same quorum WOULD be
# fenced by the run.
icegres verify --tail-quorum k1:5471,k2:5471,k3:5471

# one suite, machine-readable report, keep the evidence for support
icegres verify --suite exactly-once --json --keep-evidence /tmp/evidence
```

What it does, mechanically: creates a dedicated scratch namespace
`icegres_verify_<nonce>` (refused if it pre-exists; dropped — with its
tables purged — on every exit path, including Ctrl-C), spawns its own
scratch `icegres serve` processes against your catalog flags, drives them
over pgwire, and SIGKILLs *only those children* exactly like the CI
harness does. Suites: `durability` (acked rows survive kill -9 via tail
replay), `exactly-once` (watermark replay + post-flush sequence floor),
`fencing` (a second writer on the same tail identity is excluded),
`freshness` (a foreign commit becomes visible within `--freshness-ms` +
one refresh round trip), `failover` (a replacement writer on the quorum
fences and replays). Every check names the claim it re-proves and the doc
section that makes the claim; exit code 0 iff all selected checks pass;
checks whose backend is not configured **SKIP loudly** — never a silent
pass. Timings in the report are your box's, not a reference number.

What it does NOT cover (see also `docs/limitations.md`): the object
store's own durability, catalog HA, Kubernetes scheduling/failover (use
the §11 runbook for that), and it never touches a running production
server — but the tail resources you point it at must be dedicated to the
run (that is enforced where possible, refused loudly where it can be
detected, and documented here where it cannot: quorum).
