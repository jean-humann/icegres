# Troubleshooting

The operator's error catalog: what clients see (SQLSTATEs), what refuses to
start and why, the runtime failure modes with their remedies, and what to
alert on. Knobs referenced here are defined in
[`configuration.md`](configuration.md); deliberate non-goals in
[`limitations.md`](limitations.md).

**First stop — classify the outage** (all on `--health-port`):

| Signal | Meaning |
|---|---|
| `/health` → `503 tail unhealthy: …` | Wedged writer (poisoned/fenced tail). It must be **replaced**, not routed to — this is what drives automated failover. |
| `/ready` → 503, `/health` → 200 | Catalog unreachable (exact-freshness mode); process itself is fine. |
| `icegres_freshness_age_ms` growing monotonically | Catalog outage or dead refresher in freshness mode (a watchdog keeps the gauge honest even if the refresher dies). |

## SQLSTATEs clients can receive

| Code | Trigger(s) | What to do |
|---|---|---|
| `0A000` feature not supported | Unsupported UPDATE/DELETE shapes (parameterized, `FROM`/`RETURNING`, subqueries); `COPY … FROM` (use Flight `adbc_ingest`) or non-STDOUT COPY targets — transaction or not; extended-protocol SELECT inside an explicit transaction; DDL inside a transaction; strict-mode multi-table COMMIT on a catalog without the transactions endpoint. (`COPY … TO STDOUT` *inside* a transaction is supported — it is the ADBC postgres driver's read flow) | Change usage — never retry. Each message says what to do instead (e.g. "inline the values", "load data with ADBC bulk ingest"). |
| `40001` serialization failure | Commit lost the first-committer-wins race: another writer moved the table between the snapshot pin/anchor and the commit. On COMMIT the message is wrapped "COMMIT failed, transaction rolled back (no changes were applied)". Flight surfaces it as gRPC `ABORTED` with `40001: …` | **The one code an application should auto-retry** (re-run the whole transaction/statement, with backoff). A storm of these = too many writers on one table — see "commit conflict storms" below. |
| `40003` statement completion unknown | Multi-table COMMIT on a catalog **without** the atomic transactions endpoint, where table k failed after k−1 committed. The message lists exactly which tables committed and which did not | **Do NOT blindly retry** — reconcile per-table state. Prevent it with an atomic-capable catalog (Lakekeeper) or `ICEGRES_TXN_STRICT=true` (refuses up front). |
| `23502` not-null violation | `--enforce-pk`: NULL in a primary-key column | Fix the data. |
| `23505` unique violation | `--enforce-pk`: duplicate key, validated against the very snapshot the commit anchors to (racing INSERTs of one key cannot both land) | Fix/dedupe the data. |
| `42501` permission denied | `--authz-file` ReBAC denial ("role X cannot SELECT / write / DROP on ns.table"); same text on Flight as `PERMISSION_DENIED` | Grant the relation in the policy file. |
| `28P01` auth failed (FATAL) | Wrong password or unknown user under `--auth-file` (identical message for both — no username-existence leak). Repeated failures from one IP are throttled (escalating delay, decays after 60 s) | Fix the credentials in the auth file. |
| `57014` query canceled | Session `SET statement_timeout`, applied to the **planning phase** of cached-plan SELECTs | Note: there is **no execution wall-clock timeout yet** — a pathological query is bounded by the memory pool, not by `57014` (see limitations). |
| `25P02` in failed transaction | Any statement after an in-transaction error, until ROLLBACK (or COMMIT, which answers ROLLBACK) | Roll back and re-run. |
| `XX000` internal error | Fallback for engine/catalog/object-store errors on the write path | Investigate the server logs. |

**From `icegresd`** (the control-plane proxy): `57P03` "cannot connect now" —
standby without the leader lease, or compute wake failed (**retryable**:
reconnect, you'll land on the leader/woken compute); `3D000` — bad
branch/`:ro` routing database name; `08P01` — malformed startup packet.

**Honest notes:** `53300` (too many connections) is never emitted —
`ICEGRES_MAX_CONNECTIONS` makes excess connections *wait* in the accept
backlog, not fail. Memory-pool exhaustion surfaces as DataFusion's
`ResourcesExhausted` text without a specific SQLSTATE.

## Refuses to start (deliberately)

Every one of these aborts boot with a message that names the fix:

| Symptom (message pattern) | Fix |
|---|---|
| "refusing to bind … with authentication DISABLED" | Pass `--auth-file`, bind loopback, or acknowledge with `--insecure`. |
| "--authz-file … requires --auth-file" (flight-serve) | Add `--auth-file` (on `serve` this is only a WARN: principals become client-asserted). |
| "… is a managed add-on: this build was compiled without the `managed` feature" | Rebuild with `--features managed` or drop the flag. |
| TLS setup error at boot | Fix the cert/key pair — there is deliberately no silent plaintext fallback; `--tls-cert`/`--tls-key` go together. |
| "--tail-dir/--tail-url/--tail-quorum requires buffered writes" | Set `--write-buffer-ms N` (the synchronous default already commits before ack). |
| "--tail-dir, --tail-url, and --tail-quorum are mutually exclusive" | Pick one tail backend. |
| "tail dir X is LOCKED by another process" | One writer per tail dir (flock guard; advisory — don't put the tail on NFS). |
| "the tail database is LOCKED by another session (pid N)" | One server per tail database/schema; the message includes the `pg_terminate_backend` takeover recipe for a dead holder. |
| "--tail-url almost certainly points at a TRANSACTION-mode connection pooler" | The tail needs a direct or session-pooled connection (the connection *is* the advisory lock). |
| Quorum open fails at boot | Needs 2 of 3 acceptors reachable for the opening election. |
| "ICEGRES_DF_OPTS entry … is not of the form key=value" / "cannot set …" | Fix the `;`-separated `datafusion.<section>.<key>=<value>` pairs — invalid entries fail loudly, never silently ignored. |
| icegresd: "--health-check-ms is process mode only" / read-replicas in k8s mode | Remove the flag — the kubelet probe / an HPA owns that job in Kubernetes. |
| icegresd: "--lease-quorum address also appears in ICEGRES_TAIL_QUORUM" | The lease trio must be disjoint from the data trio (sharing would fence the tail writer). |

## Runtime failure modes

| Symptom | Cause | Remedy |
|---|---|---|
| Reads fail after ~5 s; `/ready` 503 | Catalog unreachable in **exact mode** (per-scan check; timeout 5000 ms × retries with backoff) | Restore the catalog; or `ICEGRES_STALE_READ_ON_CATALOG_ERROR=1` to serve the last snapshot; or run freshness mode. |
| WARN "catalog unreachable from the freshness refresher"; `icegres_freshness_age_ms` climbing | Catalog outage in **freshness mode** — reads keep serving bounded-stale by default | Restore the catalog; alert on the gauge. `ICEGRES_STALE_READ_ON_CATALOG_ERROR=0` opts into fail-loud instead. |
| Buffered acks fine but memory/tail growing during a catalog outage | The flusher can't commit; buffer + tail grow **unbounded for the outage duration** | Bound the outage window or the write rate (documented limit). |
| Buffered INSERTs error "tail-pg append … failed" / "tail-pg worker is gone" | Tail database unreachable — backpressure by design, never silent loss. The worker never reconnects (its connection is the lock) | Restore the DB, **restart the server**. Dead-host takeover: ~30 s keepalive or `pg_terminate_backend` after confirming the old process is gone. |
| "quorum append timed out … tail is now POISONED"; `/health` 503 | <2 of 3 acceptors acking past the quorum timeout (one internal re-election is attempted first) | Restore acceptors; restart (or let icegresd/kubelet replace the process — replay is exactly-once). |
| "superseded by a newer server (term X)" then poison | A newer writer fenced this one — the **intended** failover mechanism | No cleanup needed; verify the intended writer is serving. |
| `icegres_commit_conflicts_total` spiking; clients see `40001` | Concurrent writers colliding on the same table (first-committer-wins; no server-side retry by design) | Client retry with backoff; reduce writer fan-in (single-writer CQRS topology). Buffered flush conflicts self-retry with a WARN. |
| Query errors `ResourcesExhausted` after heavy spill | Memory pool cap reached (spill → error, never OOM) | Raise `ICEGRES_MEMORY_LIMIT_MB` (keep headroom below the container limit), give a writable spill volume, check the per-operation memory limits in limitations.md. |
| WARN "slow query"; `icegres_queries_in_flight` high while qps low | Stuck/pathological queries | `ICEGRES_QUERY_TIMING=1` to stage-profile; a forced shutdown logs each still-running query with kind + age. |
| WARN "peer tail mirror is stale/unavailable/dropped" | Silent or dead `--peer-tail` peer — reads fall back to commit-cadence freshness | Nothing is lost but the freshness bonus; alert on `icegres_peer_tail_age_max_ms`; fix the peer. |
| Clients see `57P03` for ~1–2× lease TTL | icegresd leader loss → standby takeover (or lease-trio quorum loss, which demotes the leader even with a healthy data path) | Takeover is automatic; a dark endpoint that stays dark = lease trio down — **pages a human**. |
| WARN "tail durability wait FAILED … AMBIGUOUS" | Dying disk under `--tail-dir` — the client saw an error but the rows may still commit with the in-flight flush | Treat that statement as in-doubt; replace the disk. The sequence is burned, so replay stays exactly-once. |

## Alert on these

**Metrics** (`/metrics` on `--health-port`): `icegres_freshness_age_ms`
(monotonic growth), `icegres_commit_conflicts_total` (rate spike),
`icegres_queries_slow_total` + `icegres_queries_in_flight` (high while qps
low), `icegres_peer_tail_age_max_ms` (growth; per-peer breakdown available).
`/health` 503 anywhere = replace that writer.

**Log patterns**: "quorum tail POISONED", "superseded by a newer server",
"catalog unreachable from the freshness refresher", "peer tail mirror is
stale", "failed authentication; throttling this peer", "a statement's tail
durability wait FAILED", "buffered flush conflict (409), retrying".

## Diagnostic tools

| Tool | Reach for it when |
|---|---|
| `ICEGRES_QUERY_TIMING=1` | Latency triage — per-stage timings for reads and commits. Buffers reads instead of streaming: diagnostics only, never production. |
| `icegres verify --tail-… <dedicated>` | After install or any infra change: re-proves durability/exactly-once/fencing/failover on *your* deployment. Deliberately ignores `ICEGRES_TAIL_*` env vars — always point it at a **dedicated** scratch tail, never the production quorum (it would fence the live writer). |
| `icegresd status` | Control-plane state: leader flag, per-compute phase, last exits. |
| `RUST_LOG=info,icegres=debug` + `ICEGRES_LOG_FORMAT=json` | Per-connection correlation spans de-multiplex concurrent logs; JSON for shippers. |
| `/metrics`, `/ready`, `/health` | First stop — classify catalog vs tail vs process (table at the top). |
