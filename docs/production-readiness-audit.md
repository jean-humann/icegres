<!-- Production-readiness audit by an 8-dimension multi-agent review (reliability,
security, observability, resilience/scale, deployment, data/catalog, testing, docs),
each finding adversarially verified against the code. 54 findings collected, 36 verified
(1 blocker + 27 major + minors). Reviews the code at the commit noted below; grep/line
references are from that state. This is an audit, not a set of applied changes. -->

# icegres — Production Readiness Audit

## Verdict

**NO-GO for General Availability. Conditional GO for a narrow, tightly-fenced pilot** (single trusted tenant, pgwire-only, synchronous durability, TLS-fronted, behind a connection-limiting proxy).

The core engine is genuinely well-built. The synchronous Iceberg commit path is spec-faithful and safe by construction: the catalog CAS is the only mutation, compute is stateless, first-committer-wins is enforced with real `assert-ref-snapshot-id` requirements and honest `40001`s, and the copy-on-write/snapshot/cache machinery is correct and bounded. This is not a prototype. What is missing is the *operational shell* a production data plane needs: there is exactly one true security BLOCKER (the Flight SQL port enforces no authorization at all), and then a broad, consistent pattern of "the happy path is solid, the failure path and the operations surface are absent" — no graceful shutdown, no resource bounds (memory pool, connection cap, statement timeout), no timeouts/retries on the catalog and S3 hot paths, no metrics, no CI, no container/release artifact, and no LICENSE. None of these individually corrupt data on the default durable path, but collectively they mean the server runs blind, unbounded, and un-rebuildable.

**Single biggest risk:** the Flight SQL listener is a complete ReBAC bypass (`flight.rs` has zero authz references) while `authz.rs:22-23` *falsely documents that Flight enforces authorization* — any deployment that runs both listeners with authz enabled has a silent, total confidentiality+integrity hole on one of its two headline protocols.

---

## What is already production-grade

- **Durability & correctness core.** Catalog commit is the only mutation (`overwrite.rs:57-63`); readers see old-or-new atomically; stateless compute makes crash recovery a no-op. First-committer-wins never loses silently — `commit_pinned` (`overwrite.rs:447-510`) pre-checks the pin *and* posts `assert-ref-snapshot-id`, returning `40001`; autocommit retries bounded `MAX_COMMIT_ATTEMPTS=3` from fresh metadata then bails loudly. Concurrent *external* writers (Spark/Flink) are safe against lost updates via `assert-table-uuid` + `assert-ref-snapshot-id` (`overwrite.rs:1265-1289`).
- **Invariant guard rails fail loud, never silent-wrong.** Row-count mismatch aborts the commit (`evaluate_rows`, `overwrite.rs:1480-1506`); `align_batch` rejects wrong shapes (`overwrite.rs:1601-1643`); unsupported table shapes are rejected *before* any write. `txn_select` collects eagerly so a mid-stream runtime error aborts the txn instead of returning a false `Ok`.
- **Write-buffer read protocol** (when enabled) is carefully race-designed and matches its spec — tag-flushed-before-POST, union overlay, insert-order restore on conflict (`buffer.rs:39-67, 246-262, 390-411`).
- **pgwire security done right.** SCRAM-SHA-256 never puts cleartext on the wire even without TLS; user enumeration closed (byte-identical errors, fixed-length compare); TLS misconfig **fails the boot** rather than downgrading to plaintext (`ops.rs:200-226`); open-core gating is fail-loud; AST-driven authz (`visit_relations`) covers JOINs/CTEs/subqueries.
- **Caching & bounding.** Snapshot cache gives exact freshness with no staleness window; pinned time-travel cache is a real bounded LRU (`MAX_PINNED_PER_TABLE=16`) with an RSS proof; scan IO is concurrency-limited (default 32) and batch-bounded.
- **icegresd (control plane) is the mature component.** Per-slot `spawn_lock` collapses thundering herds, generation fencing, capped exponential backoff, crash-safe atomic status file, **both SIGTERM and SIGINT handled** with deterministic child reaping — everything the two data-plane servers lack.
- **Docs & tests where they exist are strong.** 58 hermetic unit tests exercise real SQLSTATE/ReBAC/protocol logic; `e2e.sh` (110 assertions) proves conflict-injected retry, buffered union reads, and mid-session compute kill against a *real* Lakekeeper; `gate.sh` is a genuine quantitative regression contract; the "Anti-patterns (honest list)" doc is unusually candid.

---

## Launch blockers (must-fix before ANY production traffic)

| # | Blocker | Dimension | Why it blocks (failure scenario) | Fix | Effort |
|---|---------|-----------|----------------------------------|-----|--------|
| 1 | Flight SQL endpoint enforces **no authorization** — full ReBAC bypass, and docs claim the opposite | security | A tenant limited by `--authz-file` on pgwire connects to the Flight port on the *same* lakehouse and SELECT/INSERT/UPDATE/DELETE/ingests into **any** table in any namespace | Thread a `SharedAuthorizer` into `flight::run`/`FlightSqlServiceImpl`; resolve the handshake token to a user and call `required_checks` + `authorize_sql` on every data RPC; add `--authz-file` to `flight-serve`; upgrade the token store to token→username | L |

**On #1:** `flight.rs` contains zero references to `Authorizer`/`authz` (grep-empty); `authorize()` (`flight.rs:142-160`) checks only bearer-token set membership. `flight::run` (`flight.rs:990-996`) has no authorizer parameter and `Command::FlightServe` (`main.rs:325-330`) never wires one. The only production caller of `Authorizer::authorize_sql` is the pgwire `AuthzHook::gate` (`authz.rs:527`). The claim at `authz.rs:22-23` that "the Flight SQL path calls `Authorizer::authorize_sql` directly" is **factually false** — do_get/execute_update/do_put_ingest all reach DataFusion execution unchecked. This is a genuine confidentiality+integrity hole, not hardening, and it is the one finding that must be closed (or the Flight listener must be physically disabled) before any managed/multi-tenant deployment. Fix is low-effort because the pgwire seam already exists to reuse. **Note:** for a single-trusted-tenant pilot where authz isn't required, this can be *fenced* rather than fixed by not exposing Flight — see launch path.

---

## Major gaps (fix before GA; tolerable for a fenced narrow pilot)

| # | Gap | Dimension | Why it matters (failure scenario) | Fix | Effort |
|---|-----|-----------|-----------------------------------|-----|--------|
| 1 | No SIGTERM/drain on either data-plane server; write-buffer acked rows lost on graceful stop | reliability/deploy | `systemctl stop`/`kubectl delete` (SIGTERM) kills mid-query with no drain; with `--write-buffer-ms` a *clean* stop silently drops acked-but-unflushed rows on **every rolling deploy** (`ops.rs:271-311` no signal branch; `flight.rs:1075` ctrl_c-only; buffer never handed to `serve_custom`) | Add SIGTERM+SIGINT select branch (pattern exists at `icegresd.rs:538`); flush buffer + drain tasks before exit; correct false scale-to-zero safety doc (`ops.rs:26-29`) | M |
| 2 | Write buffer has no backpressure → unbounded `pending` → OOM → total buffer loss | reliability | Catalog down or persistent `409` while clients keep INSERTing (always acked, `buffer.rs:236-239`); `pending` grows unbounded → process OOM kills all sessions and loses every buffered row; in-code "memory bounded" claim (`buffer.rs:66-67`) is false | Hard high-water mark: block on synchronous flush or reject with retriable error before acking; surface buffer depth | M |
| 3 | DataFusion session has no memory-pool bound; heavy sort/join/aggregate OOMs the process | resilience | One `ORDER BY`/hash-join over the 5M-row table (or a few concurrent) buffers the working set in RAM with no spill → hard OOM (no `RuntimeEnvBuilder`/`with_memory_pool` anywhere; `context.rs:126` default unbounded pool) | `FairSpillPool` sized from container mem + `DiskManager`; over-limit → `ResourcesExhausted` not death | M |
| 4 | No connection cap / brute-force throttle on either listener | security/resilience | Unauthenticated client (auth is opt-in) opens thousands of conns; each builds per-conn DataFusion+Arrow state → FD/mem exhaustion → OOM; no failed-auth backoff (`ops.rs:271-295`, `flight.rs:1085-1098`) | Bounded connection semaphore (reject with `53300`); per-peer failed-auth backoff | M |
| 5 | No statement timeout / query load-shedding; runaway query can't be cancelled via proxy | resilience/obs | Accidental cross-join runs forever on the shared runtime; `SET statement_timeout` is silently swallowed (`txn.rs:901`); icegresd drops `CancelRequest` (`icegresd.rs:743`) so only recourse is killing the whole compute | Deadline-abort stream (`57014`); bounded in-flight semaphore; document direct-connection cancel | M |
| 6 | Catalog `load_table` on hot path of every scan with no timeout/retry/degradation | resilience | A Lakekeeper restart/blip makes `load_table` (`cache.rs:159`, every scan) hang or error → **all reads fail** even for unchanged tables; warm cache is never used as fallback | Short timeout + bounded retry; opt-in stale-read from cached snapshot on catalog-unreachable | L |
| 7 | S3/object-store IO has no request timeout or retry | resilience | One hung GET among hundreds of Parquet files holds a scan slot forever (query never completes/errors); transient 5xx surface as hard failures (`context.rs:33-48`, 32-way fan-out) | Set FileIO S3 timeouts + OpenDAL retry layer (needs `OpenDalStorageFactory` custom on pinned 0.9.1); pair with query deadline | M |
| 8 | No metrics endpoint or operational counters anywhere | observability | Server runs blind: no qps, latency, error rate, `40001` conflict rate, cache hit ratio, or connection gauge; the 1.6x infra drift would be invisible in prod | `/metrics` route on the existing health listener; export query/conflict/cache/connection counters on both paths | L |
| 9 | No query logging, duration, or in-flight visibility on pgwire | observability | Plain SELECTs bypass all logging into `DfSessionService`; a stuck/slow query can't be identified, timed, or attributed to a connection | Pass-through QueryHook logging start/end+duration+slow-query WARN; in-flight query list | M |
| 10 | Health endpoint liveness-only; no catalog-aware readiness | observability | If Lakekeeper/S3 dies after boot, health keeps returning 200 (`ops.rs:628-659`) and K8s keeps the failing pod in rotation while every query fails | Add `/ready` doing a cheap cached catalog round-trip → 503 on failure | M |
| 11 | No correlation/request IDs; concurrent-connection logs can't be tied together | observability | Under load, interleaved logs can't be de-multiplexed; can't follow one client's GetFlightInfo→DoGet or attribute a slow query to a user | Per-connection tracing span carrying id+peer+user through hooks and Flight RPCs | M |
| 12 | Flight bearer tokens carry no identity, never expire, grow unbounded | security | `Mutex<HashSet<String>>` of raw UUIDs (`flight.rs:135`); leaked token valid until restart, no revocation/logout, slow memory leak, and no principal to authorize against even after #1 | token→(user, issued_at) map with TTL + prune-on-check | M |
| 13 | Flight basic-auth sends password cleartext; no in-process TLS | security | If the front TLS proxy is forgotten/misconfigured, nothing refuses plaintext (`flight.rs:400-410, 1066-1080`); on-path observer captures reusable `user:password` — strictly worse than pgwire SCRAM | Add `--tls-cert/--tls-key` to `flight-serve` (fail boot on error) or at least `--require-tls` | M |
| 14 | Default-open posture even in managed build; both listeners default to `0.0.0.0` | security | No-flags default is "authenticate nobody, authorize everything," network-exposed, guarded only by an easily-missed startup WARN | Require `--insecure/--dev` to run non-loopback without auth+authz; elevate WARN to a gate for `0.0.0.0` binds | M |
| 15 | Multi-table COMMIT is non-atomic with a non-distinct error code | reliability/data | `BEGIN; INSERT orders; INSERT inventory; COMMIT` can commit orders and not inventory; only signal is a human-readable string reusing a generic sqlstate (`txn.rs:663-699, 1299-1305`) | Distinct documented sqlstate; opt-in single-table-only strict mode; structural log of committed/uncommitted sets | M |
| 16 | No compaction; small Parquet files accumulate unboundedly | data-catalog | Steady single-row/buffered INSERTs → thousands of small objects; scan latency and per-INSERT PK-check cost grow linearly (`cache.rs:6-9`: ~220ms after a couple hundred commits); no in-product remedy but drop-and-reseed | `icegres compact` via existing `Operation::Overwrite` add+delete machinery; or target-file-size roll-up | L |
| 17 | No snapshot expiry / metadata GC | data-catalog | Every commit adds a snapshot with no retention (`overwrite.rs:1280`); metadata.json bloats and is re-parsed per scan; storage fills with never-reclaimed manifests | `icegres expire-snapshots` via `TableUpdate::RemoveSnapshots` (exists in 0.9.1); or document external maintenance | L |
| 18 | Hard lock to Iceberg v2 / unpartitioned / copy-on-write-only | data-catalog | Any Spark/Flink table with a partition spec, merge-on-read deletes, or v3 (timestamp_ns) makes **all** icegres writes fail (`overwrite.rs:891-959`) — fails loudly but a real mixed-engine interop restriction | Document supported-table matrix now; scope partitioned/MoR/v3 post-launch as the pinned matrix allows | XL |
| 19 | No CI — nothing runs tests or the regression gate on push | testing-qa | Every gate run is honor-system; a refactor breaking the `40001` path or a SQLSTATE merges with zero signal (no `.github/workflows`) | GitHub Actions: hermetic `cargo test`/`fmt`/`clippy` PR job (branch-protected) + stack-backed nightly gate | M |
| 20 | Integration/perf suite un-runnable in CI (no Dockerfile/compose) | testing-qa | `e2e.sh`/`gate.sh` need the author's hand-provisioned live stack (host Postgres 16, pre-placed rustfs binary, hand-built lakekeeper); "e2e green" isn't independently reproducible | Pin Postgres+S3+Lakekeeper in docker-compose driving up.sh's logic | L |
| 21 | Volatile write-buffer (top durability risk) has zero unit tests; e2e sleeps past the flush and never exercises the acked-loss window | testing-qa | 558 lines of coalescing/ordering/flush untested; a regression dropping/mis-ordering acked rows ships invisibly (`buffer.rs`, `e2e.sh:659-673`) | Unit tests for coalescing/triggers/INSERT-then-UPDATE ordering; e2e that SIGKILLs with rows provably pending + a clean-shutdown-flush test | M |
| 22 | No fault injection for catalog-down / S3-down / kill-mid-commit | testing-qa | Behavior under the *most likely* real incidents is unverified; combined with #6/#7 (no timeouts), a catalog 503 or S3 hang may wedge the executor with no test proving otherwise | toxiproxy/failpoint lane: catalog 5xx/timeout mid-commit, S3 unreachable, SIGKILL mid-commit + consistency check | L |
| 23 | No container image, release artifact, or CI-built binary; no LICENSE file | deploy/docs | Deploy = `cargo build --release` on each node against an unpinned toolchain; no versioned/checksummed artifact to pin or roll back to. Separately, **no LICENSE** despite `Apache-2.0` in Cargo.toml + an open-core pitch blocks downstream legal/redistribution review | `rust-toolchain.toml` + multi-stage slim non-root Dockerfile + CI-published checksummed image; add top-level `LICENSE` (+ managed-add-on terms) | M/S |

**No metrics/query-logging plus no timeouts/bounds is the compounding theme:** items 3, 4, 5, 8, 9 together mean a single bad query can OOM or wedge a single-node compute that no one can see, bound, or cancel. Fixing the resource bounds (3, 4, 5) and the metrics/readiness surface (8, 10) should move as one workstream.

---

## Minor / post-launch

- **Poisoned `StdMutex` on the write buffer** → server-wide read/write black-hole while health stays green (`buffer.rs` `.expect("...poisoned")` everywhere; flusher never respawns). Use `unwrap_or_else(|e| e.into_inner())`; make flusher panic-resilient.
- **Sustained flush conflicts are invisible** until OOM/shutdown-loss — expose buffer depth / oldest-pending age / consecutive-failure count.
- **`--authz-file` without `--auth-file` trusts spoofable client-asserted principals** (`authz.rs:520-524`) — make it a hard start error, not a WARN.
- **Logs are human-format only; pgwire errors logged at `info!`** (`ops.rs:153`) — add `ICEGRES_LOG_FORMAT=json`; split server-internal (`error!`) vs client-input (`debug!`).
- **Full SQL text logged at INFO on the Flight path** (`flight.rs:449,472,...`) — PII/secret leak into logs; demote to `debug!` or log a fingerprint.
- **icegresd status only via a poll/rewrite status file** written with blocking IO on the connection hot path — debounce off the hot path; expose a scrape endpoint.
- **Spliced proxy sessions have no idle timeout** (`icegresd.rs:691`) — a leaked/idle connection pins a compute forever, defeating scale-to-zero.
- **Fixed 10s wake timeout with `spawn_lock` held** — a slow-but-healthy cold start (noisy box) gets SIGKILLed and fails all queued clients together.
- **Binary reports static `0.1.0` with no git SHA/build metadata** — can't tell which commit a running compute is; slows rollback/forensics. Inject via `build.rs`/vergen.
- **Orphan data/metadata files** from kill-mid-commit, retried commits, and rejected PK inserts are never reclaimed (`overwrite.rs:57-63, 1127-1204`) — add an `icegres`-side orphan sweep or document an external `remove_orphan_files` job.
- **Advertised schema cached once at construction** (`cache.rs:136`) — external schema evolution desyncs planning until restart.
- **e2e lanes skip-green when tooling is absent** (ADBC/JDBC/ODBC/ORM/managed-authz) and the banner still says "all assertions passed" — pin drivers into the gate image, make required-lane skips fail.
- **No load/soak/endurance test** (only `qps_8conn` median-of-3) — FD/memory creep is invisible; add an RSS-over-time soak.
- **No wire/SQL fuzzing** of the untrusted-byte surface (`compat.rs`, Flight DoPut, pgwire startup/bind) — a malformed packet panicking a worker is an unauthenticated DoS on a single-node server.
- **No upgrade/version-compat test, no CHANGELOG, no stability statement** — upgrades performed blind.
- **Root `README.md` is the author's personal profile** — operators landing at the repo root see zero product docs; promote `icegres/README.md`.
- **Limitations scattered across five docs** and there's **no consolidated deploy runbook / outage runbook / documented metrics story** — collect into `docs/limitations.md` + `docs/deployment.md`, and add a startup WARN when S3 creds equal the demo defaults.
- **Demo S3 creds baked as clap defaults** (`main.rs:64-69`, `rustfsadmin`/`rustfssecret`) and the secret is argv-passable (visible in `ps`) with no `--s3-secret-key-file` — real fail-fast happens because the endpoint co-defaults to localhost, so this is hygiene, not a data hole; drop defaults or gate behind `--dev`, add a file-based secret variant.
- **Single-node compute ceiling** (`icegresd` runs one process per branch, itself a SPOF) — this is the *stated architecture*, not a defect; document the ceiling + a sizing guide and ship a supervisor unit. Horizontal read-replicas are roadmap.

---

## Recommended launch path

### Critical path

1. **Close or fence the BLOCKER (Flight authz).** Either (a) implement per-RPC authorization on Flight (effort L, the pgwire seam is reusable), *or* (b) for the pilot, **do not expose the Flight listener at all** — run `icegres serve` (pgwire) only. Fencing is a config decision, not code.
2. **Add graceful shutdown (Major #1).** SIGTERM+SIGINT drain on both data-plane servers, buffer flush on exit, and correct the false scale-to-zero durability doc. This is the difference between "every rolling deploy is safe" and "every deploy severs connections / risks buffered rows."
3. **Add resource bounds (Majors #3, #4, #5).** Memory pool + spill, connection semaphore, statement timeout. On a single-node arbitrary-SQL endpoint these are what keep one bad query or one client from taking down all tenants.
4. **Add the timeout/retry/readiness triad (Majors #6, #7, #10).** Catalog + S3 timeouts and a catalog-aware `/ready` so a control-plane blip degrades gracefully instead of hanging every query and keeping a dead pod in rotation.
5. **Add minimum observability (Majors #8, #9, #11).** `/metrics`, query duration logging, per-connection correlation IDs — you cannot operate this blind.
6. **Establish the build/test spine (Majors #19, #20, #23).** CI (hermetic job branch-protected), docker-compose for the e2e stack, a container/release artifact + toolchain pin, and the LICENSE file.
7. **Data-lifecycle before write-heavy GA (Majors #16, #17).** `icegres compact` and `expire-snapshots`, or a documented external-maintenance runbook, before any table takes sustained write traffic.

### LIMITED-PILOT bar (achievable in ~1–2 focused weeks)

A pilot is safe **only if fenced to a shape that sidesteps the unfixed gaps**:

- **Single trusted tenant / no hostile multi-tenancy** — this neutralizes the Flight-authz BLOCKER and the default-open posture without code.
- **pgwire only; Flight listener disabled** (or, if Flight is needed, TLS-fronted *and* the single-tenant condition covers the missing authz).
- **`--auth-file` + `--tls-cert/--tls-key` on** (pgwire SCRAM+TLS is already production-grade); bind to a non-`0.0.0.0` interface or put it behind a proxy that enforces a connection cap and TLS.
- **Write-buffer OFF** (default, synchronous 54ms durable path) — this removes the buffer-loss, buffer-OOM, and untested-buffer risks entirely; the volatility trade is explicitly opt-in.
- **`--enforce-pk` used sparingly** given the small-file linear cost until compaction exists.
- **Front proxy providing statement timeout + connection limit**, and an operator runbook for catalog/S3 outage (server will hang/error until deps return — document it).
- **Must still ship before pilot traffic:** graceful shutdown (#1), memory-pool bound (#3), and a container/build artifact + LICENSE (#23) so deploys are reproducible and legally clear.

At this bar, the residual risk is a single-node availability ceiling and blind-spot operations you accept knowingly for a small, watched pilot — not silent data loss or a security hole.

### GENERAL-AVAILABILITY bar (realistically ~6–10 focused weeks on top of the pilot)

Everything above plus: **Flight authz actually implemented** (#1) and Flight token identity/TLS (#12, #13); the full resource/timeout/observability suite (#4–#11) in-process rather than proxy-delegated; secure-by-default posture (#14); multi-table COMMIT given a distinct sqlstate or single-table strict mode (#15); **compaction + snapshot expiry shipped** (#16, #17); CI + reproducible stack + fault-injection green (#19, #20, #22) and write-buffer unit coverage if buffered mode is to be offered (#21); documented supported-table matrix (#18) and the consolidated runbooks/LICENSE/observability docs.

**Bottom line:** icegres is not launchable for general availability yet, and the honest list of what stands between it and GA is specific and finite — one authorization hole to close, one graceful-shutdown path to add, resource bounds and timeouts on four hot paths, a metrics/readiness surface, and a CI/packaging/data-lifecycle spine. A fenced single-tenant pilot with the write buffer off and Flight disabled is defensible today after fixing graceful shutdown, the memory bound, and packaging.
