//! icegresd — the minimal icegres control plane (the OSS piece of the Neon
//! architecture that `icegres serve --idle-shutdown-secs` alone cannot
//! provide): a tiny pgwire-aware proxy/supervisor that turns stateless
//! icegres computes into wake-on-connect, scale-to-zero, branch-routed
//! endpoints.
//!
//! # What it does
//!
//! * **Wake-on-connect scale-to-zero.** icegresd listens on a public port
//!   (default 5432). When a client connects and the target compute is not
//!   running, it spawns `icegres serve --idle-shutdown-secs N`, polls the
//!   compute's TCP port until it accepts (the icegres listener binds only
//!   after the catalog session is fully built, so accept == ready), then
//!   splices bytes both ways (`tokio::io::copy_bidirectional`). The compute
//!   exiting cleanly after `N` idle seconds is the scale-to-zero half;
//!   icegresd re-spawning it on the next connection completes the loop.
//!   First-connection-after-idle latency = compute cold start + splice
//!   setup (measured: `cold_start_via_proxy_ms` in bench/bench.sh and e2e
//!   section (n)).
//!
//! * **Branch-endpoint routing.** Only the pgwire `StartupMessage`
//!   (protocol 3.0 parameters — sent before any authentication) is parsed,
//!   to read the requested `database`: `icegres` routes to the main
//!   compute, `icegres@<branch>` (any `<db>@<branch>`) routes to a
//!   per-branch compute spawned on demand on an ephemeral localhost port
//!   with `icegres serve --branch <branch>`. The ORIGINAL startup bytes are
//!   forwarded to the compute, so the session proceeds untouched (auth, if
//!   the compute is configured for it, runs end-to-end between client and
//!   compute). Idle branch computes exit like the main one and are reaped;
//!   the next `<db>@<branch>` connection re-wakes them.
//!
//! * **Session pooling (warm backend connections).** icegresd keeps up to
//!   `--pool-size` (default 8) WARM, pre-handshaked pgwire connections per
//!   compute. A client whose startup matches the pool's identity
//!   (`user == --pool-user`, `database` == the compute's canonical name,
//!   no `options`/`replication` startup parameter) is spliced onto a warm
//!   connection immediately: icegresd replays the cached backend greeting
//!   (AuthenticationOk .. ReadyForQuery) and the client is at
//!   ReadyForQuery without any compute-side handshake. Everything else —
//!   pool empty (overflow), different user/database, `options` present —
//!   falls through to a DIRECT compute connection with the client's
//!   ORIGINAL startup bytes forwarded verbatim, exactly the pre-pool path.
//!
//!   **A backend connection is never reused across client sessions.** Our
//!   computes hold real per-session state (transaction buffers, SET
//!   variables, prepared statements) and datafusion-postgres has no
//!   `DISCARD ALL`-style reset, so handing a used session to a second
//!   client could leak state. Each warm connection serves EXACTLY ONE
//!   client and is closed when that client disconnects; the pool is a
//!   warm-SPARE pool refilled in the background (this is the
//!   correctness-first fallback: per-session backend conns + warm spares).
//!   For the same reason icegresd does NOT do transaction pooling
//!   (PgBouncer `pool_mode=transaction`): statements between transactions
//!   would hop across backend sessions and silently lose SET state,
//!   prepared statements, and buffered-write ordering. Session state makes
//!   transaction pooling unsafe here by construction.
//!
//!   Pooling coexists with scale-to-zero: warm connections count as
//!   active sessions on the compute, so after `--pool-idle-secs` (default
//!   60) with zero CLIENT sessions the pool is drained, which lets the
//!   compute's own `--idle-shutdown-secs` clock run; the next wake
//!   re-warms the pool. The pool is also cleared and re-warmed when a
//!   compute crashes and is restarted. With `ICEGRES_AUTH_FILE` set the
//!   computes demand SCRAM and icegresd cannot pre-authenticate for a
//!   client, so pooling disables itself (all sessions go direct).
//!   Non-identity startup parameters of pooled clients (e.g.
//!   `application_name`) are ignored, like PgBouncer's
//!   `ignore_startup_parameters`.
//!
//! * **TLS.** icegresd itself speaks plain TCP: an `SSLRequest`/
//!   `GSSENCRequest` preamble is answered with `N` (exactly like a
//!   non-TLS-enabled Postgres listener), after which libpq clients fall
//!   back to a plaintext startup (default `sslmode=prefer`). TLS terminates
//!   at the compute: clients that require TLS (`sslmode=require`) must
//!   connect directly to a compute started with `--tls-cert/--tls-key`.
//!   icegresd-to-compute traffic is plain TCP on localhost by design.
//!
//! * **Supervision.** Every compute is a child process watched by a monitor
//!   task. A clean exit (code 0 = idle shutdown) is scale-to-zero and just
//!   marks the compute stopped. An UNCLEAN exit (non-zero, signal, crash)
//!   is restarted with capped exponential backoff (0.5 s / 1 s / 2 s, max 3
//!   attempts per crash episode; the counter resets after 10 s of healthy
//!   uptime) and logged loudly. A connection arriving during backoff
//!   short-circuits the wait and respawns immediately.
//!
//! * **Automated tail-writer failover (`--health-check-ms N`, opt-in).**
//!   Computes are spawned with an ephemeral `--health-port` and each
//!   running compute is polled over HTTP `/health` every N ms. A compute
//!   that fails [`HEALTH_MAX_FAILS`] consecutive probes — crashed, hung,
//!   or WEDGED-BUT-ALIVE (its durable quorum tail poisoned itself after
//!   being fenced or losing its quorum: such a process still accepts TCP
//!   and answers queries but can never ack a buffered write again; the
//!   compute's `/health` reports it as 503) — is killed and respawned by
//!   the supervisor. The replacement's `--tail-quorum` open() runs the
//!   consensus election: it FENCES the old term (a zombie writer can never
//!   ack) and replays the un-flushed window before the pgwire listener
//!   binds, so "compute accepts TCP" already means "fenced + replayed".
//!   Quorum tail mode only: `--tail-dir` is single-node by nature and
//!   `--tail-url` failover is the tail database's own HA (documented as
//!   manual in docs/limitations.md).
//!
//! * **Leader lease (`--lease-quorum h:p,h:p,h:p`, opt-in).** N icegresd
//!   instances, one leader: a tiny lease log served by three DEDICATED
//!   icekeeperd acceptors (never the computes' data trio), held by owning
//!   the proposer election on it and renewed every TTL/3 with quorum-acked
//!   holder records. Standbys poll the acceptors read-only and take over
//!   once the log sits frozen at a quorum for >= TTL; the old leader's
//!   next renew is term-fenced, so it DEMOTES (refuses new clients,
//!   terminates its computes, re-enters standby). Honesty: until that
//!   renew a deposed leader does not KNOW it lost — it can still route,
//!   and a demote racing a compute (re)spawn can still spawn a writer
//!   (both spawn paths re-check leadership under the spawn lock, which
//!   shrinks the window to the spawn itself but cannot close it). Data
//!   stays safe — the writers fence each other on the data tail — but
//!   the fencing can land on the NEW leader's writer, so with a quorum
//!   data tail the health checker below defaults ON as the recovery
//!   route. See `src/lease.rs`.
//!
//! * **Autoscaling-lite (`--read-replicas-max N`, opt-in).** Clients
//!   connecting with database `<db>:ro` are routed to a pool of up to N
//!   stateless READ computes over the same single copy of the data,
//!   spawned on demand when every running replica already carries
//!   `--read-replica-sessions` active sessions (least-loaded routing
//!   otherwise) and reaped by the existing idle scale-to-zero. Replicas
//!   are spawned with the buffered-write/tail environment STRIPPED (a
//!   replica must never open the writer's tail — it would fence it) and,
//!   when configured, `--peer-tail`/`--freshness-ms` toward the writer's
//!   tail API so they see its un-flushed window. Honest scope: sessions
//!   (not qps) drive the threshold, single-digit nodes, process mode — in
//!   Kubernetes this maps to HPA guidance instead.
//!
//! * **Kubernetes mode (`--k8s-compute` / `--k8s-scale`, opt-in).** In a
//!   cluster the compute is a POD behind a Service, so icegresd never
//!   forks: `--k8s-compute` makes the main endpoint REMOTE — dialed at
//!   `--compute-host:--main-port` (the writer Service DNS name), TCP-
//!   readiness-polled, never spawned or supervised (the kubelet's
//!   liveness probe on the compute's `/health` owns replacement — the
//!   same wedged-tail 503 the process-mode health checker acts on).
//!   `--k8s-scale deployments/<name>|statefulsets/<name>` (implies
//!   `--k8s-compute`) adds the two halves a Service cannot provide, by
//!   patching that workload's apps/v1 `scale` subresource with the pod
//!   serviceaccount (src/k8s.rs): wake-on-connect (`GET` scale, `PATCH`
//!   replicas 0 -> 1, then the normal readiness poll) and idle
//!   scale-to-zero (zero proxied sessions for `--idle-shutdown-secs` →
//!   `PATCH` replicas -> 0, leader-gated). Process-mode-only features are
//!   refused loudly in k8s mode: `--health-check-ms` (kubelet liveness
//!   owns compute health), `--read-replicas-max` (read replicas are a
//!   Deployment behind their own Service; scale them with HPA), and
//!   branch endpoints (deploy a per-branch compute and connect to its
//!   Service). Session pooling still applies — warm conns are plain TCP.
//!
//! * **Status.** The daemon rewrites `--status-file` (JSON) on every state
//!   change; `icegresd status` pretty-prints it: computes, branches, ports,
//!   PIDs, active connections, idle timers, restart counts.
//!
//! # What it deliberately does not do
//!
//! * `CancelRequest` (a separate TCP connection carrying a backend key) is
//!   not routed — icegresd does not track backend keys, so query
//!   cancellation through the proxy is dropped (connect directly to the
//!   compute port for that).
//! * No config file: computes inherit icegresd's environment, so all
//!   `ICEGRES_*` variables (catalog, S3, auth file, write buffer, health
//!   port, ...) apply to every compute it spawns; `--host/--port/--branch/
//!   --idle-shutdown-secs` are passed explicitly as flags and therefore
//!   always win over stray env.

// The consensus tree is shared source with `icegres`/`icekeeperd` (this
// crate has no lib target); icegresd drives only the proposer half — via
// the leader lease (src/lease.rs). src/quorum/mod.rs documents that
// nothing in the tree touches the arrow/iceberg/datafusion stack, so
// icegresd keeps linking a few MB, not 120.
#[path = "../k8s.rs"]
mod k8s;
#[path = "../lease.rs"]
mod lease;
#[allow(dead_code)]
#[path = "../quorum/mod.rs"]
mod quorum;
#[allow(dead_code)]
#[path = "../segment.rs"]
mod segment;

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context as _, Result};
use clap::{Args, Parser, Subcommand};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::process::{Child, Command};
use tracing::{error, info, warn};

/// pgwire preamble codes (each arrives as 4-byte length + 4-byte code).
/// A protocol-3.x StartupMessage has `code >> 16 == 3` (3.0 = 196608).
const SSL_REQUEST: u32 = 80_877_103;
const GSSENC_REQUEST: u32 = 80_877_104;
const CANCEL_REQUEST: u32 = 80_877_102;

/// Crash-episode restart policy: 0.5 s / 1 s / 2 s, max 3 attempts; an
/// episode ends (counter resets) after this much healthy uptime.
const RESTART_BASE_DELAY: Duration = Duration::from_millis(500);
const RESTART_MAX_ATTEMPTS: u32 = 3;
const HEALTHY_UPTIME: Duration = Duration::from_secs(10);

/// Consecutive `--health-check-ms` probe failures before the compute is
/// killed for supervised replacement (one blip is a GC pause; three in a
/// row is a corpse or a wedged tail).
const HEALTH_MAX_FAILS: u32 = 3;

/// `--health-check-ms` default when the leader lease runs over a quorum
/// data tail in process mode (see [`effective_health_check_ms`]): the
/// wedged-writer recovery route must exist, so 0 means this, not off.
const LEASE_QUORUM_HEALTH_CHECK_MS: u64 = 1_000;

#[derive(Parser)]
#[command(
    name = "icegresd",
    version = env!("ICEGRES_LONG_VERSION"),
    about = "Minimal icegres control plane: wake-on-connect scale-to-zero proxy, \
             branch-endpoint routing, compute supervision"
)]
struct Cli {
    #[command(subcommand)]
    command: DCommand,
}

#[derive(Subcommand)]
enum DCommand {
    /// Run the control plane: listen publicly, spawn/route/supervise computes.
    /// (Boxed: ServeArgs outgrew the other variant by enough that clippy
    /// flags the size spread — the daemon parses argv exactly once.)
    Serve(Box<ServeArgs>),
    /// Print the daemon's status file (computes, branches, ports, idle
    /// timers, restart counts).
    Status {
        /// Status file written by `icegresd serve` (same default).
        #[arg(long, env = "ICEGRESD_STATUS_FILE")]
        status_file: Option<PathBuf>,
    },
}

#[derive(Args, Clone)]
struct ServeArgs {
    /// Address to bind the public listener on.
    #[arg(long, env = "ICEGRESD_HOST", default_value = "0.0.0.0")]
    host: String,

    /// Public port clients connect to.
    #[arg(long, env = "ICEGRESD_PORT", default_value_t = 5432)]
    port: u16,

    /// Maximum concurrent public client connections. The permit is acquired
    /// before accept, so excess clients remain in the kernel backlog instead
    /// of creating unbounded tasks and per-session buffers. 0 explicitly
    /// disables the cap.
    #[arg(long, env = "ICEGRESD_MAX_CONNECTIONS", default_value_t = 512)]
    max_connections: usize,

    /// Path to the `icegres` binary (default: next to this executable,
    /// falling back to `icegres` on PATH).
    #[arg(long, env = "ICEGRESD_ICEGRES_BIN")]
    icegres_bin: Option<PathBuf>,

    /// Host computes bind/are dialed on. Compute traffic is plain TCP —
    /// keep this on localhost (TLS, if any, terminates at the compute).
    #[arg(long, env = "ICEGRESD_COMPUTE_HOST", default_value = "127.0.0.1")]
    compute_host: String,

    /// Fixed port of the main compute (database `icegres`); branch computes
    /// get ephemeral localhost ports.
    #[arg(long, env = "ICEGRESD_MAIN_PORT", default_value_t = 5439)]
    main_port: u16,

    /// `--idle-shutdown-secs` passed to every compute (scale-to-zero).
    #[arg(long, env = "ICEGRESD_IDLE_SHUTDOWN_SECS", default_value_t = 300)]
    idle_shutdown_secs: u64,

    /// Budget for a spawned compute to accept TCP (poll every 10 ms).
    #[arg(long, env = "ICEGRESD_WAKE_TIMEOUT_MS", default_value_t = 10_000)]
    wake_timeout_ms: u64,

    /// JSON status file the daemon rewrites on every state change
    /// (default: <tmpdir>/icegresd-status.json).
    #[arg(long, env = "ICEGRESD_STATUS_FILE")]
    status_file: Option<PathBuf>,

    /// Warm, pre-handshaked backend connections kept per compute (SESSION
    /// pooling: each warm connection serves exactly one client session and
    /// is never reused — see the module docs). 0 disables pooling; clients
    /// that do not match the pool identity overflow to direct connections.
    #[arg(long, env = "ICEGRESD_POOL_SIZE", default_value_t = 8)]
    pool_size: usize,

    /// `user` startup parameter the pool warms sessions with. Clients whose
    /// startup `user` differs bypass the pool (direct compute connection).
    #[arg(long, env = "ICEGRESD_POOL_USER", default_value = "postgres")]
    pool_user: String,

    /// Drain the warm pool after this many seconds with zero CLIENT
    /// sessions on a compute, so the compute's own --idle-shutdown-secs
    /// clock can run (warm conns count as active sessions on the compute;
    /// without the drain, pooling would defeat scale-to-zero). The pool
    /// re-warms on the next wake.
    #[arg(long, env = "ICEGRESD_POOL_IDLE_SECS", default_value_t = 60)]
    pool_idle_secs: u64,

    /// Poll each running compute's HTTP /health every N ms (0 = off,
    /// byte-identical to before the flag existed — EXCEPT with
    /// --lease-quorum plus a quorum data tail in process mode, where 0
    /// defaults to 1000: a demote racing a (re)spawn can leave the new
    /// leader's writer fenced-but-alive, and only this loop replaces it;
    /// see the module docs). Computes are spawned with an ephemeral
    /// --health-port; a compute failing 3 consecutive probes — crashed,
    /// hung, or wedged-but-alive (its quorum tail poisoned itself: still
    /// accepts TCP, can never ack a write) — is killed and respawned by
    /// the supervisor, whose --tail-quorum open() fences the old term and
    /// replays the un-flushed window (automated tail-writer failover).
    #[arg(long, env = "ICEGRESD_HEALTH_CHECK_MS", default_value_t = 0)]
    health_check_ms: u64,

    /// Leader lease for icegresd redundancy: exactly three DEDICATED
    /// icekeeperd acceptors (host:port,host:port,host:port) forming the
    /// lease log — a different trio from the computes' data quorum (one
    /// acceptor process serves one log; sharing an address is refused at
    /// boot). When set, this icegresd serves clients and spawns computes
    /// only while it HOLDS the lease; otherwise it answers connections
    /// with a retryable error. Off when unset (single-instance behavior,
    /// byte-identical).
    #[arg(long, env = "ICEGRESD_LEASE_QUORUM")]
    lease_quorum: Option<String>,

    /// Lease TTL in ms (floor 1000): the leader renews every TTL/3;
    /// standbys take over after observing the lease log frozen at a
    /// quorum of acceptors for >= TTL.
    #[arg(long, env = "ICEGRESD_LEASE_TTL_MS", default_value_t = 6000)]
    lease_ttl_ms: u64,

    /// Holder id written into lease records (diagnostics; default
    /// icegresd-<pid>@<host>:<port>).
    #[arg(long, env = "ICEGRESD_LEASE_HOLDER_ID")]
    lease_holder_id: Option<String>,

    /// Autoscaling-lite: route clients whose startup database is
    /// "<db>:ro" across up to N stateless READ computes, spawned on
    /// demand (see --read-replica-sessions) and reaped by the normal idle
    /// scale-to-zero. Replicas never inherit the buffered-write/tail
    /// environment (a replica opening the writer's tail would fence it).
    /// 0 = off (byte-identical; "<db>:ro" then routes like any other
    /// database name).
    #[arg(long, env = "ICEGRESD_READ_REPLICAS_MAX", default_value_t = 0)]
    read_replicas_max: usize,

    /// Spawn another read replica when every running one already carries
    /// this many active sessions (least-loaded routing below that; at
    /// --read-replicas-max the least-loaded replica absorbs the overflow
    /// — never a refusal).
    #[arg(long, env = "ICEGRESD_READ_REPLICA_SESSIONS", default_value_t = 4)]
    read_replica_sessions: usize,

    /// --peer-tail address passed to every read replica (the writer
    /// compute's --tail-api-port listener), so replicas serve the
    /// writer's acked-but-unflushed window instead of waiting for the
    /// commit cadence.
    #[arg(long, env = "ICEGRESD_REPLICA_PEER_TAIL")]
    replica_peer_tail: Option<String>,

    /// --freshness-ms passed to every read replica (bounded-staleness
    /// reads; see `icegres serve --freshness-ms`).
    #[arg(long, env = "ICEGRESD_REPLICA_FRESHNESS_MS")]
    replica_freshness_ms: Option<u64>,

    /// Kubernetes mode: the main compute is a POD dialed at
    /// --compute-host:--main-port (the compute Service's DNS name) —
    /// icegresd never forks, supervises, or health-kills processes (the
    /// kubelet's liveness probe on the compute's /health owns
    /// replacement). Off by default (process mode, byte-identical).
    #[arg(long, env = "ICEGRESD_K8S_COMPUTE", num_args = 0..=1,
          default_missing_value = "true", default_value = "false",
          value_parser = clap::builder::BoolishValueParser::new())]
    k8s_compute: bool,

    /// apps/v1 scale target ("deployments/<name>" or
    /// "statefulsets/<name>", same namespace as this pod) icegresd may
    /// GET/PATCH with its serviceaccount; implies --k8s-compute. Adds
    /// wake-on-connect (scale 0 -> 1 on a cold connection, then the
    /// normal TCP-readiness poll — budget --wake-timeout-ms, which pod
    /// scheduling + image pull deserve more of than a process fork) and
    /// idle scale-to-zero (zero proxied sessions for --idle-shutdown-secs
    /// scales the workload to 0; leader-gated; 0 disables the
    /// scale-down). Needs RBAC for exactly [get, patch] on that one
    /// object's scale subresource. Off by default.
    #[arg(long, env = "ICEGRESD_K8S_SCALE")]
    k8s_scale: Option<String>,
}

fn default_status_file() -> PathBuf {
    std::env::temp_dir().join("icegresd-status.json")
}

// Match the main icegres binary: mimalloc as the global allocator.
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    match Cli::parse().command {
        DCommand::Serve(args) => run_serve(*args).await,
        DCommand::Status { status_file } => {
            let path = status_file.unwrap_or_else(default_status_file);
            let raw = std::fs::read_to_string(&path).with_context(|| {
                format!(
                    "could not read status file {} — is `icegresd serve` running with the same --status-file?",
                    path.display()
                )
            })?;
            let v: serde_json::Value = serde_json::from_str(&raw)
                .with_context(|| format!("status file {} is not valid JSON", path.display()))?;
            println!("{}", serde_json::to_string_pretty(&v)?);
            Ok(())
        }
    }
}

/// Lifecycle phase of one compute slot.
#[derive(Clone, Debug, PartialEq)]
enum Phase {
    /// Never started, or exited cleanly (idle scale-to-zero).
    Stopped,
    /// Spawned, waiting for the TCP listener to accept.
    Starting,
    /// Accepting connections.
    Running,
    /// Crashed; supervisor is between restart attempts.
    Backoff,
    /// Crash-looped past the restart cap; next connection retries.
    Failed,
}

impl Phase {
    fn as_str(&self) -> &'static str {
        match self {
            Phase::Stopped => "stopped",
            Phase::Starting => "starting",
            Phase::Running => "running",
            Phase::Backoff => "backoff",
            Phase::Failed => "failed",
        }
    }
}

/// Mutable state of one compute, guarded by a std Mutex (never held across
/// an await point).
struct SlotState {
    phase: Phase,
    pid: Option<u32>,
    port: u16,
    /// The compute's --health-port (ephemeral, allocated per spawn) when
    /// --health-check-ms is on; None otherwise.
    health_port: Option<u16>,
    spawned_at: Option<SystemTime>,
    last_exit: Option<String>,
}

/// One WARM backend connection: TCP established, pgwire startup handshake
/// already completed by icegresd (as `--pool-user`), backend greeting
/// (AuthenticationOk .. ReadyForQuery) cached for replay to the client it
/// is eventually handed to. Serves EXACTLY ONE client session, then dies
/// with it (no cross-session reuse — see the module docs).
struct WarmConn {
    stream: TcpStream,
    greeting: Vec<u8>,
    /// Slot generation at warm time; a conn from a superseded compute
    /// process is discarded at handout.
    generation: u64,
}

/// Why warming a connection failed — auth-required disables pooling for
/// good (icegresd cannot pre-authenticate on a client's behalf), anything
/// else is transient (compute just idle-exited, mid-restart, ...).
enum WarmError {
    AuthRequired,
    Other(anyhow::Error),
}

/// Warm-spare connection pool of one compute slot.
struct Pool {
    conns: tokio::sync::Mutex<VecDeque<WarmConn>>,
    /// Mirrors `conns.len()` so the sync status writer never touches the
    /// async lock.
    warm: AtomicUsize,
    /// Client sessions served from a warm conn / via a direct (overflow or
    /// identity-mismatch) connection.
    handouts: AtomicU64,
    direct: AtomicU64,
    /// Instant of the last client-session start or end on this slot; the
    /// idle-drain loop compares it against --pool-idle-secs.
    last_client: Mutex<Instant>,
    /// Serializes background refills (one warm loop per slot at a time).
    warm_lock: tokio::sync::Mutex<()>,
    /// Set once if the compute demands authentication: pooling is then off
    /// for this slot and every session goes direct.
    disabled: AtomicBool,
}

impl Pool {
    fn new() -> Self {
        Pool {
            conns: tokio::sync::Mutex::new(VecDeque::new()),
            warm: AtomicUsize::new(0),
            handouts: AtomicU64::new(0),
            direct: AtomicU64::new(0),
            last_client: Mutex::new(Instant::now()),
            warm_lock: tokio::sync::Mutex::new(()),
            disabled: AtomicBool::new(false),
        }
    }

    fn touch(&self) {
        *self.last_client.lock().expect("pool clock lock poisoned") = Instant::now();
    }

    async fn clear(&self) {
        self.conns.lock().await.clear();
        self.warm.store(0, Ordering::SeqCst);
    }
}

/// One compute endpoint: the main one (`branch == None`, fixed port), a
/// per-branch one (ephemeral port, `icegres serve --branch <name>`), or an
/// autoscaled read replica (`replica == true`, ephemeral port, stateless).
struct ComputeSlot {
    key: String,
    branch: Option<String>,
    /// An autoscaled read compute: ephemeral port, buffered/tail env
    /// stripped at spawn, optional --peer-tail/--freshness-ms wiring,
    /// pooling disabled (its endpoint identity "<db>:ro" never matches
    /// the pool's canonical database).
    replica: bool,
    /// A REMOTE compute (k8s mode): a pod dialed at
    /// --compute-host:--main-port, never spawned/supervised here. Waking
    /// it is `ensure_remote` (optionally a scale PATCH); everything
    /// process-shaped (monitors, health kills, PIDs) never applies.
    remote: bool,
    /// Serializes spawn/respawn decisions for this slot.
    spawn_lock: tokio::sync::Mutex<()>,
    state: Mutex<SlotState>,
    /// Bumped on every successful spawn; a monitor task only acts while its
    /// generation is current (a newer spawn supersedes it).
    generation: AtomicU64,
    active: AtomicUsize,
    restarts: AtomicU64,
    pool: Pool,
}

impl ComputeSlot {
    /// The database name the pool warms sessions with — and the ONLY
    /// database a pooled handout is allowed for (anything else goes
    /// direct): `icegres` on main, `icegres@<branch>` on a branch slot.
    fn canonical_db(&self) -> String {
        match &self.branch {
            None => "icegres".to_string(),
            Some(b) => format!("icegres@{b}"),
        }
    }
}

struct Daemon {
    args: ServeArgs,
    bin: PathBuf,
    status_file: PathBuf,
    slots: Mutex<HashMap<String, Arc<ComputeSlot>>>,
    /// Pooling is configured on (--pool-size > 0) and possible (computes
    /// run without SCRAM — with ICEGRES_AUTH_FILE in the environment the
    /// spawned computes demand credentials icegresd does not have).
    pool_enabled: bool,
    /// Flipped to `true` exactly once, on daemon shutdown: monitor tasks
    /// then terminate AND REAP their computes (see `monitor_compute`).
    shutdown: tokio::sync::watch::Sender<bool>,
    /// Leadership: constant `true` when --lease-quorum is unset (the
    /// sender is dropped after init; a watch keeps serving its last
    /// value); driven by the lease loop otherwise. Gates client routing
    /// and compute spawning; monitors terminate computes on demote.
    leader: tokio::sync::watch::Receiver<bool>,
    lease_enabled: bool,
    /// --k8s-compute (or --k8s-scale): computes are pods, never children.
    k8s_mode: bool,
    /// --k8s-scale: the workload whose scale subresource wake-on-connect
    /// and idle scale-to-zero PATCH.
    k8s: Option<k8s::K8sScaler>,
    /// When this instance last BECAME leader (boot for the lease-less
    /// case; refreshed by the leadership watch task on every
    /// acquisition). The k8s idle-park loop refuses to park within one
    /// idle window of a takeover: a demote in k8s mode terminates
    /// nothing, so sessions that survived a failover keep flowing
    /// through the DEPOSED instance — invisible to this instance's idle
    /// clock, which reads idle precisely because traffic moved. The
    /// grace window gives them time to drain before the shared writer
    /// can be parked under them (see `k8s_park_decision`).
    leader_since: Mutex<Instant>,
}

impl Daemon {
    /// Does this instance currently hold the lease (or run lease-less)?
    fn is_leader(&self) -> bool {
        *self.leader.borrow()
    }

    fn slot(self: &Arc<Self>, branch: Option<&str>) -> Arc<ComputeSlot> {
        let key = match branch {
            None => "main".to_string(),
            Some(b) => format!("branch:{b}"),
        };
        let mut slots = self.slots.lock().expect("slots lock poisoned");
        slots
            .entry(key.clone())
            .or_insert_with(|| {
                Arc::new(ComputeSlot {
                    key,
                    branch: branch.map(str::to_string),
                    replica: false,
                    // In k8s mode the main endpoint is the remote compute
                    // Service (branches are refused before slot lookup).
                    remote: self.k8s_mode && branch.is_none(),
                    spawn_lock: tokio::sync::Mutex::new(()),
                    state: Mutex::new(SlotState {
                        phase: Phase::Stopped,
                        pid: None,
                        // Fixed for main; branch slots get an ephemeral port
                        // at each (re)spawn.
                        port: if branch.is_none() {
                            self.args.main_port
                        } else {
                            0
                        },
                        health_port: None,
                        spawned_at: None,
                        last_exit: None,
                    }),
                    generation: AtomicU64::new(0),
                    active: AtomicUsize::new(0),
                    restarts: AtomicU64::new(0),
                    pool: Pool::new(),
                })
            })
            .clone()
    }

    /// The read-replica slot at `idx` (created on first use). Pooling is
    /// disabled per slot: a replica's endpoint identity ("<db>:ro") never
    /// matches the pool's canonical database, so warm conns would only rot.
    fn replica_slot(self: &Arc<Self>, idx: usize) -> Arc<ComputeSlot> {
        let key = format!("replica:{idx}");
        let mut slots = self.slots.lock().expect("slots lock poisoned");
        slots
            .entry(key.clone())
            .or_insert_with(|| {
                let slot = Arc::new(ComputeSlot {
                    key,
                    branch: None,
                    replica: true,
                    remote: false, // k8s mode refuses --read-replicas-max at boot
                    spawn_lock: tokio::sync::Mutex::new(()),
                    state: Mutex::new(SlotState {
                        phase: Phase::Stopped,
                        pid: None,
                        port: 0, // ephemeral at each (re)spawn
                        health_port: None,
                        spawned_at: None,
                        last_exit: None,
                    }),
                    generation: AtomicU64::new(0),
                    active: AtomicUsize::new(0),
                    restarts: AtomicU64::new(0),
                    pool: Pool::new(),
                });
                slot.pool.disabled.store(true, Ordering::SeqCst);
                slot
            })
            .clone()
    }

    /// Pick the read-replica slot for a new "<db>:ro" session (see
    /// [`route_read`] for the decision itself, kept pure for tests).
    fn read_slot(self: &Arc<Self>) -> Arc<ComputeSlot> {
        let max = self.args.read_replicas_max;
        let states: Vec<Option<usize>> = {
            let slots = self.slots.lock().expect("slots lock poisoned");
            (0..max)
                .map(|i| {
                    slots.get(&format!("replica:{i}")).and_then(|s| {
                        let running = {
                            let st = s.state.lock().expect("slot state lock poisoned");
                            matches!(st.phase, Phase::Running | Phase::Starting)
                        };
                        running.then(|| s.active.load(Ordering::SeqCst))
                    })
                })
                .collect()
        };
        self.replica_slot(route_read(&states, self.args.read_replica_sessions.max(1)))
    }

    /// Rewrite the status file. Failures are logged, never fatal.
    fn write_status(&self) {
        let computes: Vec<serde_json::Value> = {
            let slots = self.slots.lock().expect("slots lock poisoned");
            let mut v: Vec<_> = slots.values().cloned().collect();
            v.sort_by(|a, b| a.key.cmp(&b.key));
            v.iter()
                .map(|s| {
                    let st = s.state.lock().expect("slot state lock poisoned");
                    let pool_on = self.pool_enabled && !s.pool.disabled.load(Ordering::SeqCst);
                    let mut entry = serde_json::json!({
                        "key": s.key,
                        "branch": s.branch,
                        "port": st.port,
                        "state": st.phase.as_str(),
                        "pid": st.pid,
                        "active_connections": s.active.load(Ordering::SeqCst),
                        "restarts": s.restarts.load(Ordering::SeqCst),
                        "spawned_at_epoch_ms": st.spawned_at.map(epoch_ms),
                        "last_exit": st.last_exit,
                        "idle_shutdown_secs": self.args.idle_shutdown_secs,
                        "pool": {
                            "size": if pool_on { self.args.pool_size } else { 0 },
                            "warm": s.pool.warm.load(Ordering::SeqCst),
                            "pooled_sessions": s.pool.handouts.load(Ordering::SeqCst),
                            "direct_sessions": s.pool.direct.load(Ordering::SeqCst),
                        },
                    });
                    // Present only with --health-check-ms (I3: the default
                    // status document stays byte-identical without flags).
                    if let Some(hp) = st.health_port {
                        entry["health_port"] = hp.into();
                    }
                    entry
                })
                .collect()
        };
        let mut doc = serde_json::json!({
            "daemon_pid": std::process::id(),
            "listen": format!("{}:{}", self.args.host, self.args.port),
            "icegres_bin": self.bin.display().to_string(),
            "updated_at_epoch_ms": epoch_ms(SystemTime::now()),
            "computes": computes,
        });
        // Present only with --lease-quorum (I3, as above).
        if self.lease_enabled {
            doc["lease_enabled"] = true.into();
            doc["leader"] = self.is_leader().into();
        }
        // Present only with --k8s-compute / --k8s-scale (I3, as above).
        if self.k8s_mode {
            doc["k8s_compute"] = true.into();
            if let Some(k8s) = &self.k8s {
                doc["k8s_scale"] = k8s.target().into();
            }
        }
        // Atomic replace (write tmp + rename): readers polling the file
        // must never observe a truncated document.
        let tmp = self.status_file.with_extension("json.tmp");
        let res = std::fs::write(&tmp, format!("{doc:#}\n"))
            .and_then(|()| std::fs::rename(&tmp, &self.status_file));
        if let Err(e) = res {
            warn!(file = %self.status_file.display(), "could not write status file: {e}");
        }
    }
}

fn epoch_ms(t: SystemTime) -> u128 {
    t.duration_since(UNIX_EPOCH).unwrap_or_default().as_millis()
}

async fn run_serve(mut args: ServeArgs) -> Result<()> {
    let bin = match &args.icegres_bin {
        Some(p) => p.clone(),
        None => {
            // Prefer the sibling `icegres` of this executable (same build).
            let sibling = std::env::current_exe()
                .ok()
                .and_then(|p| p.parent().map(|d| d.join("icegres")));
            match sibling {
                Some(p) if p.is_file() => p,
                _ => PathBuf::from("icegres"),
            }
        }
    };
    let status_file = args.status_file.clone().unwrap_or_else(default_status_file);

    let addr = format!("{}:{}", args.host, args.port);
    let listener = TcpListener::bind(&addr)
        .await
        .with_context(|| format!("failed to bind public listener on {addr}"))?;
    info!(
        listen_addr = %addr,
        icegres_bin = %bin.display(),
        main_port = args.main_port,
        idle_shutdown_secs = args.idle_shutdown_secs,
        status_file = %status_file.display(),
        "icegresd control plane listening (wake-on-connect; database 'icegres' -> main compute, '<db>@<branch>' -> per-branch compute)"
    );

    // Session pooling needs computes that accept the warm handshake without
    // credentials; with ICEGRES_AUTH_FILE the spawned computes demand SCRAM
    // and icegresd cannot answer a client's SCRAM exchange from a cached
    // greeting — pooling turns itself off (every session goes direct).
    let auth_env = std::env::var_os("ICEGRES_AUTH_FILE").is_some();
    let pool_enabled = args.pool_size > 0 && !auth_env;
    if args.pool_size > 0 && auth_env {
        warn!(
            "ICEGRES_AUTH_FILE is set: computes require SCRAM, which icegresd cannot \
             pre-authenticate — session pooling is DISABLED (all sessions direct)"
        );
    } else if pool_enabled {
        info!(
            pool_size = args.pool_size,
            pool_user = %args.pool_user,
            pool_idle_secs = args.pool_idle_secs,
            "session pooling enabled (warm spare conns; one client per backend conn, never reused)"
        );
    }

    // Leader lease (opt-in): validate the trio up front — a lease trio
    // overlapping the computes' data quorum must fail loudly at boot, not
    // fence the tail writer at the first election.
    let lease_cfg = match &args.lease_quorum {
        Some(spec) => {
            let data_env = std::env::var("ICEGRES_TAIL_QUORUM").ok();
            let addrs = lease::parse_lease_addrs(spec, data_env.as_deref())?;
            let ttl = Duration::from_millis(args.lease_ttl_ms.max(1_000));
            let holder_id = args.lease_holder_id.clone().unwrap_or_else(|| {
                format!(
                    "icegresd-{}@{}:{}",
                    std::process::id(),
                    args.host,
                    args.port
                )
            });
            Some(lease::LeaseConfig {
                addrs,
                ttl,
                holder_id,
            })
        }
        None => None,
    };
    let lease_enabled = lease_cfg.is_some();
    let (leader_tx, leader_rx) = tokio::sync::watch::channel(!lease_enabled);

    // A fenced-but-alive writer accepts TCP but can never ack a buffered
    // write again — invisible to the dial path's bare TCP probe. With the
    // lease on AND a quorum data tail in the computes' environment, a
    // demote racing a compute (re)spawn can put the NEW leader's writer in
    // exactly that state (module docs), and only the health loop recovers
    // it: default the checker ON for that combination. Process mode only —
    // in k8s mode the kubelet's liveness probe owns compute health (and
    // --health-check-ms is refused below). Explicit flag always wins.
    let k8s_mode = args.k8s_compute || args.k8s_scale.is_some();
    let effective = effective_health_check_ms(
        args.health_check_ms,
        lease_enabled,
        std::env::var_os("ICEGRES_TAIL_QUORUM").is_some(),
        k8s_mode,
    );
    if effective != args.health_check_ms {
        warn!(
            health_check_ms = effective,
            "--lease-quorum with a quorum data tail and no --health-check-ms: \
             defaulting the compute health checker ON — without it a demote racing \
             a compute (re)spawn can fence this leader's writer into a permanent \
             wedged-but-alive write outage (set --health-check-ms to tune)"
        );
        args.health_check_ms = effective;
    }

    if args.health_check_ms > 0 {
        info!(
            health_check_ms = args.health_check_ms,
            "compute health checks enabled: computes spawn with --health-port; \
             {HEALTH_MAX_FAILS} consecutive /health failures (crash, hang, or a \
             poisoned/fenced quorum tail) kill the compute for supervised \
             replacement (fence + replay by the replacement's tail election)"
        );
    }
    if args.read_replicas_max > 0 {
        info!(
            read_replicas_max = args.read_replicas_max,
            read_replica_sessions = args.read_replica_sessions,
            replica_peer_tail = args.replica_peer_tail.as_deref().unwrap_or("(none)"),
            "autoscaling-lite enabled: database '<db>:ro' routes across up to \
             {} read computes (spawn at {} sessions each; reap = idle \
             scale-to-zero; buffered/tail env stripped from replicas)",
            args.read_replicas_max,
            args.read_replica_sessions
        );
    }

    // Kubernetes mode (opt-in): computes are pods behind Services, so the
    // process-shaped features cannot mean anything — refuse them loudly at
    // boot instead of doing something subtly wrong at the first connection.
    let k8s_scaler = if k8s_mode {
        if args.health_check_ms > 0 {
            bail!(
                "--health-check-ms is process mode only: in k8s mode the kubelet's \
                 liveness probe on the compute's /health kills a wedged compute \
                 (same 503, same fence-and-replay on the replacement) — remove the flag"
            );
        }
        if args.read_replicas_max > 0 {
            bail!(
                "--read-replicas-max is process mode only: in k8s mode read replicas \
                 are a Deployment behind their own Service (scale with HPA or \
                 spec.replicas) — remove the flag"
            );
        }
        // The scaler is built (and the serviceaccount contract checked) at
        // boot: a pod that cannot reach the API must fail its first rollout,
        // not its first cold connection.
        let scaler = match &args.k8s_scale {
            Some(target) => Some(k8s::K8sScaler::from_env(target)?),
            None => None,
        };
        info!(
            compute = format!("{}:{}", args.compute_host, args.main_port),
            k8s_scale = args
                .k8s_scale
                .as_deref()
                .unwrap_or("(none: no scale PATCH)"),
            idle_shutdown_secs = args.idle_shutdown_secs,
            "k8s mode: the main compute is a remote pod (never forked); wake = \
             TCP-readiness poll{}",
            if scaler.is_some() {
                " after a scale-subresource PATCH 0 -> 1; idle scale-to-zero PATCHes back to 0"
            } else {
                ""
            }
        );
        scaler
    } else {
        None
    };

    let daemon = Arc::new(Daemon {
        args,
        bin,
        status_file,
        slots: Mutex::new(HashMap::new()),
        pool_enabled,
        shutdown: tokio::sync::watch::channel(false).0,
        leader: leader_rx,
        lease_enabled,
        k8s_mode,
        k8s: k8s_scaler,
        leader_since: Mutex::new(Instant::now()),
    });
    daemon.write_status();

    if daemon.pool_enabled {
        tokio::spawn(pool_idle_drain_loop(daemon.clone()));
    }
    if daemon.k8s.is_some() && daemon.args.idle_shutdown_secs > 0 {
        tokio::spawn(k8s_idle_scale_loop(daemon.clone()));
    }

    match lease_cfg {
        Some(cfg) => {
            // Keep the status file honest about leadership flips, and
            // stamp the takeover instant (the idle-park loop's new-leader
            // grace window — see `k8s_park_decision`).
            let daemon2 = daemon.clone();
            let mut rx = daemon.leader.clone();
            tokio::spawn(async move {
                while rx.changed().await.is_ok() {
                    if *rx.borrow() {
                        *daemon2
                            .leader_since
                            .lock()
                            .expect("leader clock lock poisoned") = Instant::now();
                    }
                    daemon2.write_status();
                }
            });
            tokio::spawn(lease::lease_loop(
                cfg,
                leader_tx,
                daemon.shutdown.subscribe(),
            ));
        }
        // No lease: drop the sender — the watch keeps serving `true`.
        None => drop(leader_tx),
    }

    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("failed to install SIGTERM handler")?;
    let conn_limiter = (daemon.args.max_connections > 0)
        .then(|| Arc::new(tokio::sync::Semaphore::new(daemon.args.max_connections)));
    if let Some(limit) = conn_limiter.as_ref().map(|s| s.available_permits()) {
        info!(max_connections = limit, "public connection cap enabled");
    } else {
        warn!("public connection cap DISABLED (--max-connections=0)");
    }
    'accept: loop {
        // Acquire before accept to put overload backpressure in the OS
        // backlog. Signal handling remains live while all permits are held.
        let permit = match &conn_limiter {
            Some(limiter) => {
                tokio::select! {
                    permit = limiter.clone().acquire_owned() => {
                        Some(permit.expect("connection semaphore closed"))
                    }
                    _ = tokio::signal::ctrl_c() => break 'accept,
                    _ = sigterm.recv() => break 'accept,
                }
            }
            None => None,
        };
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((client, peer)) => {
                    let daemon = daemon.clone();
                    tokio::spawn(async move {
                        let _permit = permit;
                        if let Err(e) = handle_client(daemon, client).await {
                            warn!(%peer, "connection failed: {e:#}");
                        }
                    });
                }
                Err(e) => warn!("accept error: {e}"),
            },
            _ = tokio::signal::ctrl_c() => break,
            _ = sigterm.recv() => break,
        }
    }

    // Shutdown: tell every monitor task to terminate AND REAP its compute
    // (SIGTERM, then SIGKILL after 2 s — see monitor_compute), and only exit
    // once no compute is left Running/Starting, so a supervisor watching
    // icegresd can rely on "icegresd gone => its computes gone".
    // kill_on_drop(true) on each Child remains the SIGKILL backstop.
    info!("shutting down: terminating computes");
    let _ = daemon.shutdown.send(true);
    for _ in 0..100 {
        let busy = {
            let slots = daemon.slots.lock().expect("slots lock poisoned");
            // Remote (k8s) computes are not children: nothing to reap.
            slots.values().filter(|s| !s.remote).any(|s| {
                let st = s.state.lock().expect("slot state lock poisoned");
                matches!(st.phase, Phase::Running | Phase::Starting)
            })
        };
        if !busy {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let _ = std::fs::remove_file(&daemon.status_file);
    Ok(())
}

/// One client connection end to end: read the startup preamble, route by
/// database name, wake the target compute if needed, then either hand out a
/// WARM pooled backend connection (identity match: replay the cached
/// greeting, no compute-side handshake) or fall through to a DIRECT compute
/// connection with the original startup bytes forwarded verbatim; splice.
async fn handle_client(daemon: Arc<Daemon>, mut client: TcpStream) -> Result<()> {
    let _ = client.set_nodelay(true);
    let startup =
        match tokio::time::timeout(Duration::from_secs(10), read_startup(&mut client)).await {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => {
                send_pg_error(&mut client, "08P01", &format!("icegresd: {e}")).await;
                return Err(e);
            }
            Err(_) => bail!("timed out waiting for the client startup message"),
        };

    if !daemon.is_leader() {
        // Standby (or freshly demoted): never route, never wake. 57P03
        // ("cannot connect now") is the retryable shape — clients fail
        // over to the leader's endpoint and reconnect.
        send_pg_error(
            &mut client,
            "57P03",
            "icegresd: this instance does not hold the leader lease; retry against the \
             current leader",
        )
        .await;
        info!("refused a client connection while standby (not the lease leader)");
        return Ok(());
    }
    let route = match route_database(
        startup.database.as_deref(),
        daemon.args.read_replicas_max > 0,
    ) {
        Ok(r) => r,
        Err(e) => {
            send_pg_error(&mut client, "3D000", &format!("icegresd: {e}")).await;
            return Err(e);
        }
    };
    if daemon.k8s_mode {
        if let Route::Branch(b) = &route {
            // Spawning a per-branch child inside the icegresd pod would be
            // exactly the process model k8s mode exists to avoid.
            let e = anyhow::anyhow!(
                "branch endpoint {b:?} is process mode only: in Kubernetes deploy a \
                 per-branch compute (`icegres serve --branch {b}`) and connect to its \
                 Service directly"
            );
            send_pg_error(&mut client, "3D000", &format!("icegresd: {e}")).await;
            return Err(e);
        }
    }
    let slot = match &route {
        Route::Main => daemon.slot(None),
        Route::Branch(b) => daemon.slot(Some(b)),
        Route::Read => daemon.read_slot(),
    };

    let t0 = Instant::now();
    let (port, woke) = match ensure_running(&daemon, &slot).await {
        Ok(v) => v,
        Err(e) => {
            send_pg_error(
                &mut client,
                "57P03",
                &format!("icegresd: compute for {} unavailable: {e:#}", slot.key),
            )
            .await;
            return Err(e);
        }
    };

    // Pooled handout requires an exact identity match: same user the pool
    // warmed with, the slot's canonical database, and no session-shaping
    // startup parameters (`options`/`replication` change backend behavior
    // and must reach the compute in the real startup message). Everything
    // else — including pool exhaustion — overflows to a direct connection.
    let pool_on = daemon.pool_enabled && !slot.pool.disabled.load(Ordering::SeqCst);
    let identity_ok = pool_on
        && startup.user.as_deref() == Some(daemon.args.pool_user.as_str())
        && startup.database.as_deref() == Some(slot.canonical_db().as_str())
        && !startup.has_options;
    let warm = if identity_ok {
        take_warm(&slot).await
    } else {
        None
    };
    if pool_on {
        // Refill in the background: after a handout (replace the spare),
        // and right after a wake (fill the pool for the sessions to come).
        tokio::spawn(warm_pool(daemon.clone(), slot.clone()));
    }
    slot.pool.touch();

    let (mut compute, pooled) = match warm {
        Some(w) => {
            // Warm path: the backend session already exists; bring the
            // client to ReadyForQuery by replaying the cached greeting.
            client
                .write_all(&w.greeting)
                .await
                .context("failed to replay the cached backend greeting to the client")?;
            slot.pool.handouts.fetch_add(1, Ordering::SeqCst);
            (w.stream, true)
        }
        None => {
            let compute_addr = format!("{}:{}", daemon.args.compute_host, port);
            let mut compute = match TcpStream::connect(&compute_addr).await {
                Ok(c) => c,
                Err(first_err) => {
                    // The compute may have idle-exited in the instant between
                    // the readiness check and the dial: wake once more.
                    warn!(key = %slot.key, "compute dial failed ({first_err}); re-waking once");
                    let (port, _) = ensure_running(&daemon, &slot).await?;
                    TcpStream::connect(format!("{}:{}", daemon.args.compute_host, port))
                        .await
                        .with_context(|| {
                            format!("could not dial compute for {} on {compute_addr}", slot.key)
                        })?
                }
            };
            let _ = compute.set_nodelay(true);
            compute
                .write_all(&startup.raw)
                .await
                .context("failed to forward the startup message to the compute")?;
            slot.pool.direct.fetch_add(1, Ordering::SeqCst);
            (compute, false)
        }
    };

    if woke {
        info!(
            key = %slot.key,
            port,
            wake_ms = t0.elapsed().as_millis() as u64,
            "compute woken on connect; splicing"
        );
    }
    slot.active.fetch_add(1, Ordering::SeqCst);
    daemon.write_status();
    let res = tokio::io::copy_bidirectional(&mut client, &mut compute).await;
    slot.active.fetch_sub(1, Ordering::SeqCst);
    // A backend connection is NEVER returned to the pool: this client's
    // session state (SET, prepared statements, transactions) dies with it.
    slot.pool.touch();
    daemon.write_status();
    // EOF/reset at either end just ends the session; not an icegresd error.
    if let Err(e) = res {
        info!(key = %slot.key, pooled, "splice ended: {e}");
    }
    Ok(())
}

/// A parsed client StartupMessage: the raw bytes (length prefix included,
/// forwarded verbatim on the direct path) plus the parameters the router
/// and the pool identity check need.
struct Startup {
    raw: Vec<u8>,
    database: Option<String>,
    user: Option<String>,
    /// `options` or `replication` present: these shape the backend session
    /// and must reach the compute in a real startup — never pooled.
    has_options: bool,
}

/// Read the pgwire preamble from a fresh client connection, answering
/// SSLRequest/GSSENCRequest with `N` (plaintext only at the proxy), until
/// the protocol-3 StartupMessage arrives.
async fn read_startup(client: &mut TcpStream) -> Result<Startup> {
    loop {
        let mut lenb = [0u8; 4];
        client
            .read_exact(&mut lenb)
            .await
            .context("client closed before sending a startup message")?;
        let len = u32::from_be_bytes(lenb) as usize;
        if !(8..=64 * 1024).contains(&len) {
            bail!("implausible startup message length {len}");
        }
        let mut payload = vec![0u8; len - 4];
        client
            .read_exact(&mut payload)
            .await
            .context("client closed mid-startup-message")?;
        let code = u32::from_be_bytes(payload[0..4].try_into().expect("4-byte slice"));
        match code {
            SSL_REQUEST | GSSENC_REQUEST => {
                // Plaintext at the proxy; libpq's default sslmode=prefer
                // retries in the clear. TLS terminates at the compute.
                client.write_all(b"N").await?;
            }
            CANCEL_REQUEST => {
                bail!("CancelRequest is not routed by icegresd (no backend-key tracking)");
            }
            _ if code >> 16 == 3 => {
                let mut database = None;
                let mut user = None;
                let mut has_options = false;
                let mut it = payload[4..].split(|&b| b == 0);
                while let Some(k) = it.next() {
                    if k.is_empty() {
                        break;
                    }
                    let v = it.next().unwrap_or(&[]);
                    match k {
                        b"database" => database = Some(String::from_utf8_lossy(v).into_owned()),
                        b"user" => user = Some(String::from_utf8_lossy(v).into_owned()),
                        b"options" | b"replication" => has_options = true,
                        _ => {}
                    }
                }
                let mut raw = Vec::with_capacity(len);
                raw.extend_from_slice(&lenb);
                raw.extend_from_slice(&payload);
                return Ok(Startup {
                    raw,
                    database,
                    user,
                    has_options,
                });
            }
            _ => bail!("unsupported protocol version {code} (icegresd speaks pgwire 3.x)"),
        }
    }
}

/// Where a client's requested database name routes.
#[derive(Debug, PartialEq)]
enum Route {
    Main,
    Branch(String),
    /// The autoscaled read pool (`<db>:ro`, only with --read-replicas-max
    /// > 0).
    Read,
}

/// Map the requested database name to a compute: `icegres` (or anything
/// without `@`, or none) -> main; `<db>@<branch>` -> the branch endpoint;
/// `<db>:ro` -> the read-replica pool WHEN autoscaling is on (off, the
/// suffix means nothing and the name routes exactly as before —
/// byte-identical default). Routing stays by endpoint IDENTITY, never SQL
/// parsing; branch read pools (`<db>@<branch>:ro`) are not supported.
fn route_database(database: Option<&str>, read_pool: bool) -> Result<Route> {
    let Some(db) = database else {
        return Ok(Route::Main);
    };
    if read_pool {
        if let Some(base) = db.strip_suffix(":ro") {
            if base.contains('@') {
                bail!(
                    "read-replica routing for a BRANCH endpoint ({db:?}) is not supported; \
                     connect to <db>@<branch> directly"
                );
            }
            return Ok(Route::Read);
        }
    }
    let Some((_, branch)) = db.split_once('@') else {
        return Ok(Route::Main);
    };
    if branch.is_empty()
        || !branch
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        bail!("invalid branch endpoint {db:?} (expected <db>@<branch>, branch = [A-Za-z0-9_-]+)");
    }
    Ok(Route::Branch(branch.to_string()))
}

/// The autoscale-lite routing decision, pure for unit tests: each read
/// replica's state is `None` (not running) or `Some(active sessions)`.
/// Least-loaded running replica while one has headroom (`active <
/// sessions_per`); the first non-running slot once every running one is
/// at/over the threshold (waking it IS the scale-up); at capacity the
/// least-loaded absorbs the overflow — a read session is never refused.
/// Nothing running: slot 0.
fn route_read(states: &[Option<usize>], sessions_per: usize) -> usize {
    let least = states
        .iter()
        .enumerate()
        .filter_map(|(i, s)| s.map(|active| (active, i)))
        .min();
    match least {
        None => 0,
        Some((active, idx)) if active < sessions_per => idx,
        Some((_, idx)) => states.iter().position(Option::is_none).unwrap_or(idx),
    }
}

/// The effective `--health-check-ms`, pure for unit tests: the flag value
/// as given — except 0 (unset) with the leader lease on AND a quorum data
/// tail in the computes' environment, in process mode, which defaults to
/// [`LEASE_QUORUM_HEALTH_CHECK_MS`]. Rationale: in that combination a
/// demote racing a compute (re)spawn can fence the NEW leader's writer
/// into a wedged-but-alive state (accepts TCP, never acks — invisible to
/// the dial path's TCP probe), and the health loop is the only automated
/// recovery route. k8s mode never defaults it: the kubelet's liveness
/// probe owns compute health there and the flag is refused at boot.
fn effective_health_check_ms(
    configured_ms: u64,
    lease_enabled: bool,
    quorum_tail_env: bool,
    k8s_mode: bool,
) -> u64 {
    if configured_ms == 0 && lease_enabled && quorum_tail_env && !k8s_mode {
        LEASE_QUORUM_HEALTH_CHECK_MS
    } else {
        configured_ms
    }
}

/// Make sure the slot's compute is accepting connections, spawning it if
/// needed; returns (port, whether a wake was performed). Serialized per
/// slot so concurrent connections to a cold endpoint spawn exactly one
/// process.
async fn ensure_running(daemon: &Arc<Daemon>, slot: &Arc<ComputeSlot>) -> Result<(u16, bool)> {
    let _guard = slot.spawn_lock.lock().await;

    // Defense in depth against a demote racing this connection: a standby
    // must never spawn (the new leader owns the computes now).
    if !daemon.is_leader() {
        bail!("this instance does not hold the leader lease; retry against the current leader");
    }

    // Remote (k8s) compute: readiness-poll a pod, never spawn one.
    if slot.remote {
        return ensure_remote(daemon, slot).await;
    }

    // Fast path: state says running AND the port actually accepts (the
    // probe catches a kill -9 the monitor task has not observed yet).
    let (phase, port) = {
        let st = slot.state.lock().expect("slot state lock poisoned");
        (st.phase.clone(), st.port)
    };
    if phase == Phase::Running && tcp_ready(&daemon.args.compute_host, port).await {
        return Ok((port, false));
    }

    let port = if slot.branch.is_none() && !slot.replica {
        daemon.args.main_port
    } else {
        ephemeral_port()?
    };
    info!(key = %slot.key, port, "waking compute (spawning `icegres serve`)");
    let (child, generation) = spawn_compute(daemon, slot, port).await?;
    let daemon2 = daemon.clone();
    let slot2 = slot.clone();
    tokio::spawn(async move {
        monitor_compute(daemon2, slot2, child, generation, port).await;
    });
    Ok((port, true))
}

/// k8s-mode `ensure_running` (caller holds `spawn_lock`): the compute is a
/// pod at `--compute-host:--main-port`. Accepting TCP == ready, exactly as
/// in process mode (the compute binds its listener only after the catalog
/// session is built). Not accepting == cold or mid-(re)start: issue the
/// wake PATCH if `--k8s-scale` is configured (0 -> 1 only; a workload an
/// operator or HPA already scaled up is left alone), then poll readiness
/// until `--wake-timeout-ms` — pod scheduling and image pulls deserve a
/// far bigger budget than a process fork (the chart raises the default).
async fn ensure_remote(daemon: &Arc<Daemon>, slot: &Arc<ComputeSlot>) -> Result<(u16, bool)> {
    let host = &daemon.args.compute_host;
    let port = daemon.args.main_port;
    if tcp_ready(host, port).await {
        let flipped = {
            let mut st = slot.state.lock().expect("slot state lock poisoned");
            let flip = st.phase != Phase::Running;
            if flip {
                st.phase = Phase::Running;
                st.spawned_at.get_or_insert(SystemTime::now());
            }
            flip
        };
        if flipped {
            daemon.write_status();
        }
        return Ok((port, false));
    }

    if let Some(k8s) = &daemon.k8s {
        match k8s.wake().await {
            Ok(true) => {
                info!(
                    key = %slot.key,
                    target = k8s.target(),
                    "compute workload scaled 0 -> 1 (wake-on-connect)"
                );
            }
            Ok(false) => {} // replicas >= 1 already: pod starting/restarting
            // A failed PATCH is not instantly fatal — the pod may be coming
            // up anyway (rollout, HPA) — but it is loud: if replicas really
            // is 0, the readiness poll below will time out and say so.
            Err(e) => warn!(key = %slot.key, "k8s wake failed (still polling readiness): {e:#}"),
        }
    }

    {
        let mut st = slot.state.lock().expect("slot state lock poisoned");
        st.phase = Phase::Starting;
        st.spawned_at = Some(SystemTime::now());
    }
    daemon.write_status();
    let deadline = Instant::now() + Duration::from_millis(daemon.args.wake_timeout_ms);
    loop {
        if tcp_ready(host, port).await {
            {
                let mut st = slot.state.lock().expect("slot state lock poisoned");
                st.phase = Phase::Running;
            }
            daemon.write_status();
            info!(key = %slot.key, port, "remote compute ready");
            return Ok((port, true));
        }
        if Instant::now() >= deadline {
            {
                let mut st = slot.state.lock().expect("slot state lock poisoned");
                st.phase = Phase::Stopped;
                st.last_exit =
                    Some("remote compute not ready within --wake-timeout-ms".to_string());
            }
            daemon.write_status();
            bail!(
                "remote compute for {} not accepting on {host}:{port} within {} ms \
                 (pod still scheduling/pulling, or the scale PATCH was refused — \
                 check the workload and the icegresd serviceaccount RBAC)",
                slot.key,
                daemon.args.wake_timeout_ms
            );
        }
        // Pods come up in seconds, not milliseconds: poll gently (this also
        // spares the cluster DNS a 10 ms hammering on Service names).
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// One idle-park tick's verdict, from local state only (pure — the
/// decision matrix is unit-tested below). `Park` = this instance proxied
/// traffic to the workload (slot `Running`) and everything has been idle
/// for one full window: park it. `ProbeThenPark` = everything local says
/// idle but the slot phase is NOT `Running` — remote slots boot `Stopped`
/// and only a proxied connection flips them, so after an icegresd restart
/// (or a `helm upgrade` resetting a parked writer to the chart's pinned
/// `replicas: 1`), and after a failed wake whose 0 -> 1 PATCH landed
/// before the readiness poll timed out, an idle-but-up workload would
/// otherwise NEVER park again: the caller must consult the authoritative
/// source (the scale subresource) and park only a workload that is
/// actually up. `Skip` = everything else: standby ticks (the leader owns
/// the lifecycle — this snapshot lags a DEPOSED leader by up to ~TTL/3 +
/// the lease append timeout, hence the caller's re-check right before
/// the PATCH and the residual-window note in limitations.md), live
/// sessions, a client inside the idle window, or leadership YOUNGER than
/// one idle window — a NEW leader parking instantly could sever sessions
/// still flowing through the demoted instance (a k8s demote terminates
/// nothing, and those sessions are invisible to this instance's clocks),
/// so they get one idle window to drain first.
#[derive(Debug, PartialEq)]
enum ParkDecision {
    Skip,
    Park,
    ProbeThenPark,
}

fn k8s_park_decision(
    is_leader: bool,
    phase: &Phase,
    active_sessions: usize,
    idle_for: Duration,
    led_for: Duration,
    idle_window: Duration,
) -> ParkDecision {
    if !is_leader || active_sessions != 0 || idle_for < idle_window || led_for < idle_window {
        return ParkDecision::Skip;
    }
    if *phase == Phase::Running {
        ParkDecision::Park
    } else {
        ParkDecision::ProbeThenPark
    }
}

/// The scale-down half of k8s-mode scale-to-zero (`--k8s-scale` with
/// `--idle-shutdown-secs > 0`): every few seconds, if this instance has
/// led for at least one idle window, NO proxied client session has
/// existed for `--idle-shutdown-secs` (the same pool clock the idle drain
/// uses — touched at every session start and end), and the workload is
/// actually up ([`k8s_park_decision`]: the local slot phase when this
/// instance proxied to it, a scale-subresource GET when it never did —
/// fresh restart, `helm upgrade`, failed-wake residue), PATCH the
/// workload to 0 replicas. The next cold connection wakes it back through
/// [`ensure_remote`]. Honest edges: only traffic THROUGH icegresd counts
/// (direct-to-Service clients are invisible — the chart keeps the compute
/// Service cluster-internal for this reason), and a connection racing the
/// PATCH is cut and must reconnect (the same race process mode has with
/// `--idle-shutdown-secs` itself); after an icegresd restart the clock
/// starts at boot, so an already-idle workload parks one idle window
/// later.
async fn k8s_idle_scale_loop(daemon: Arc<Daemon>) {
    let idle = Duration::from_secs(daemon.args.idle_shutdown_secs);
    let mut shutdown = daemon.shutdown.subscribe();
    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(5)) => {}
            _ = shutdown.changed() => return,
        }
        let slot = daemon.slot(None);
        // Serialize against wakes without ever delaying one: skip the tick
        // if a connection is mid-wake.
        let Ok(_guard) = slot.spawn_lock.try_lock() else {
            continue;
        };
        let idle_for = slot
            .pool
            .last_client
            .lock()
            .expect("pool clock lock poisoned")
            .elapsed();
        let decision = {
            let phase = slot
                .state
                .lock()
                .expect("slot state lock poisoned")
                .phase
                .clone();
            let led_for = daemon
                .leader_since
                .lock()
                .expect("leader clock lock poisoned")
                .elapsed();
            k8s_park_decision(
                daemon.is_leader(),
                &phase,
                slot.active.load(Ordering::SeqCst),
                idle_for,
                led_for,
                idle,
            )
        };
        let Some(k8s) = &daemon.k8s else { return };
        match decision {
            ParkDecision::Skip => continue,
            ParkDecision::Park => {}
            ParkDecision::ProbeThenPark => {
                // The local phase proves nothing here (this process never
                // proxied to the workload): ask the scale subresource.
                // replicas == 0 = already parked, nothing to do; a GET
                // error = skip, the next tick retries.
                match k8s.replicas().await {
                    Ok(n) if n > 0 => {}
                    Ok(_) => continue,
                    Err(e) => {
                        warn!(key = %slot.key, "idle-park replica probe failed (skipping this tick): {e:#}");
                        continue;
                    }
                }
            }
        }
        // Re-verify leadership at the last instant, mirroring the spawn
        // paths' re-checks: the PATCH target is the SHARED writer workload,
        // and a deposed-but-unaware leader parking it would sever the true
        // leader's live sessions (its own idle clock reads idle precisely
        // BECAUSE traffic moved to the new leader). The re-check shrinks
        // the stale-watch window to the PATCH round trip; the residual race
        // is documented (recovery is automatic: the next cold connection
        // re-wakes, fence + replay keep every acked row). The OPPOSITE
        // direction — the true leader parking under sessions surviving on
        // a DEPOSED instance — is narrowed by the new-leader grace window
        // in `k8s_park_decision`, and its residue is documented alongside.
        if !daemon.is_leader() {
            continue;
        }
        match k8s.set_replicas(0).await {
            Ok(()) => {
                info!(
                    key = %slot.key,
                    target = k8s.target(),
                    idle_secs = idle_for.as_secs(),
                    "compute workload scaled to zero (idle); the next connection re-wakes it"
                );
                slot.pool.clear().await;
                let mut st = slot.state.lock().expect("slot state lock poisoned");
                st.phase = Phase::Stopped;
                st.pid = None;
                st.last_exit = Some("scaled to zero (idle)".to_string());
                drop(st);
                daemon.write_status();
            }
            Err(e) => warn!(key = %slot.key, "idle scale-to-zero PATCH failed: {e:#}"),
        }
    }
}

/// Spawn `icegres serve` for this slot on `port` and wait for TCP
/// readiness. Returns the child and its registered generation. Caller must
/// hold `spawn_lock` and arrange monitoring.
async fn spawn_compute(
    daemon: &Arc<Daemon>,
    slot: &Arc<ComputeSlot>,
    port: u16,
) -> Result<(Child, u64)> {
    let mut cmd = Command::new(&daemon.bin);
    cmd.arg("serve")
        .arg("--host")
        .arg(&daemon.args.compute_host)
        .arg("--port")
        .arg(port.to_string())
        .arg("--idle-shutdown-secs")
        .arg(daemon.args.idle_shutdown_secs.to_string());
    if let Some(b) = &slot.branch {
        cmd.arg("--branch").arg(b);
    }
    if slot.replica {
        // A read replica must NEVER inherit the buffered-write/tail
        // environment: a replica opening the writer's --tail-quorum would
        // run a higher-term election and FENCE the writer, and an
        // inherited tail-api/health port would collide with the writer's
        // listener. Replicas are stateless computes over the same single
        // copy; their tail visibility comes from --peer-tail below.
        for var in [
            "ICEGRES_WRITE_BUFFER_MS",
            "ICEGRES_WRITE_BUFFER_MAX_ROWS",
            "ICEGRES_TAIL_DIR",
            "ICEGRES_TAIL_URL",
            "ICEGRES_TAIL_QUORUM",
            "ICEGRES_TAIL_API_PORT",
            "ICEGRES_HEALTH_PORT",
            "ICEGRES_PEER_TAILS",
            "ICEGRES_FRESHNESS_MS",
        ] {
            cmd.env_remove(var);
        }
        if let Some(peer) = &daemon.args.replica_peer_tail {
            cmd.arg("--peer-tail").arg(peer);
        }
        if let Some(ms) = daemon.args.replica_freshness_ms {
            cmd.arg("--freshness-ms").arg(ms.to_string());
        }
    }
    // Health checking (--health-check-ms): every compute gets its own
    // ephemeral --health-port (explicit flags beat any inherited
    // ICEGRES_HEALTH_PORT, which would collide across computes).
    let health_port = if daemon.args.health_check_ms > 0 {
        let hp = ephemeral_port()?;
        cmd.arg("--health-port").arg(hp.to_string());
        Some(hp)
    } else {
        None
    };
    // Compute logs interleave with icegresd's own stream (loud by design);
    // kill_on_drop is the backstop that reaps children if icegresd dies.
    cmd.stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .kill_on_drop(true);
    let mut child = cmd
        .spawn()
        .with_context(|| format!("failed to spawn {} serve", daemon.bin.display()))?;
    let pid = child.id();

    {
        let mut st = slot.state.lock().expect("slot state lock poisoned");
        st.phase = Phase::Starting;
        st.pid = pid;
        st.port = port;
        st.health_port = health_port;
        st.spawned_at = Some(SystemTime::now());
    }
    daemon.write_status();

    // Readiness = the compute's TCP listener accepts. icegres binds it only
    // after the catalog session is fully built, so accept == ready.
    let deadline = Instant::now() + Duration::from_millis(daemon.args.wake_timeout_ms);
    loop {
        if tcp_ready(&daemon.args.compute_host, port).await {
            break;
        }
        if let Some(status) = child.try_wait().context("compute wait failed")? {
            fail_slot(daemon, slot, format!("exited during startup: {status}"));
            bail!("compute for {} exited during startup ({status})", slot.key);
        }
        if Instant::now() >= deadline {
            let _ = child.start_kill();
            fail_slot(
                daemon,
                slot,
                "killed: not ready within --wake-timeout-ms".to_string(),
            );
            bail!(
                "compute for {} not accepting on port {port} within {} ms",
                slot.key,
                daemon.args.wake_timeout_ms
            );
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    let generation = slot.generation.fetch_add(1, Ordering::SeqCst) + 1;
    {
        let mut st = slot.state.lock().expect("slot state lock poisoned");
        st.phase = Phase::Running;
    }
    daemon.write_status();
    info!(key = %slot.key, port, pid, "compute ready");
    if let (Some(hp), Some(pid)) = (health_port, pid) {
        tokio::spawn(health_watch(
            daemon.clone(),
            slot.clone(),
            generation,
            hp,
            pid,
        ));
    }
    Ok((child, generation))
}

fn fail_slot(daemon: &Daemon, slot: &ComputeSlot, why: String) {
    let mut st = slot.state.lock().expect("slot state lock poisoned");
    st.phase = Phase::Failed;
    st.pid = None;
    st.last_exit = Some(why);
    drop(st);
    daemon.write_status();
}

/// Watch one compute child until it is superseded or gone for good. Clean
/// exit (code 0 = idle shutdown) marks the slot Stopped (scale-to-zero).
/// Unclean exit triggers supervised restarts with capped exponential
/// backoff; the attempt counter resets after `HEALTHY_UPTIME` of uptime.
async fn monitor_compute(
    daemon: Arc<Daemon>,
    slot: Arc<ComputeSlot>,
    mut child: Child,
    mut generation: u64,
    port: u16,
) {
    let mut attempts: u32 = 0;
    let mut started = Instant::now();
    let mut shutdown = daemon.shutdown.subscribe();
    let mut leader = daemon.leader.clone();
    loop {
        let status = tokio::select! {
            status = child.wait() => status,
            _ = demoted(&mut leader) => {
                // Lost the leader lease: terminate the compute like a
                // shutdown, but keep the daemon alive (standby). The new
                // leader spawns its own compute, whose data-tail election
                // fences this one anyway — a lingering fenced zombie would
                // only burn the port and confuse the status file.
                if let Some(pid) = child.id() {
                    info!(key = %slot.key, pid, "lost the leader lease: sending SIGTERM to compute");
                    let _ = std::process::Command::new("kill")
                        .args(["-TERM", &pid.to_string()])
                        .status();
                }
                if tokio::time::timeout(Duration::from_secs(2), child.wait())
                    .await
                    .is_err()
                {
                    warn!(key = %slot.key, "compute ignored SIGTERM for 2s after demote; killing");
                    let _ = child.kill().await;
                }
                let mut st = slot.state.lock().expect("slot state lock poisoned");
                st.phase = Phase::Stopped;
                st.pid = None;
                st.last_exit = Some("terminated: lost the leader lease".into());
                drop(st);
                daemon.write_status();
                return;
            }
            _ = shutdown.changed() => {
                // Daemon shutdown: terminate politely, then reap — icegresd
                // must not exit while its compute is alive or a zombie.
                if let Some(pid) = child.id() {
                    info!(key = %slot.key, pid, "shutdown: sending SIGTERM to compute");
                    let _ = std::process::Command::new("kill")
                        .args(["-TERM", &pid.to_string()])
                        .status();
                }
                if tokio::time::timeout(Duration::from_secs(2), child.wait())
                    .await
                    .is_err()
                {
                    warn!(key = %slot.key, "compute ignored SIGTERM for 2s; killing");
                    let _ = child.kill().await;
                }
                let mut st = slot.state.lock().expect("slot state lock poisoned");
                st.phase = Phase::Stopped;
                st.pid = None;
                st.last_exit = Some("terminated by icegresd shutdown".into());
                drop(st);
                daemon.write_status();
                return;
            }
        };
        if slot.generation.load(Ordering::SeqCst) != generation {
            return; // superseded by a newer spawn
        }
        let exit_desc = match &status {
            Ok(s) => s.to_string(),
            Err(e) => format!("wait error: {e}"),
        };
        // Whatever the exit was, every pooled conn to this process is dead:
        // drop them now so no handout has to trip over a corpse.
        slot.pool.clear().await;
        if matches!(&status, Ok(s) if s.code() == Some(0)) {
            info!(
                key = %slot.key,
                port,
                "compute exited cleanly (idle scale-to-zero); the next connection re-wakes it"
            );
            let mut st = slot.state.lock().expect("slot state lock poisoned");
            st.phase = Phase::Stopped;
            st.pid = None;
            st.last_exit = Some(format!("clean idle exit ({exit_desc})"));
            drop(st);
            daemon.write_status();
            return;
        }

        // Unclean exit: crash episode.
        if started.elapsed() >= HEALTHY_UPTIME {
            attempts = 0;
        }
        error!(
            key = %slot.key,
            port,
            active_connections = slot.active.load(Ordering::SeqCst),
            "compute exited UNCLEANLY ({exit_desc}) — supervised restart with backoff"
        );
        {
            let mut st = slot.state.lock().expect("slot state lock poisoned");
            st.phase = Phase::Backoff;
            st.pid = None;
            st.last_exit = Some(format!("UNCLEAN: {exit_desc}"));
        }
        daemon.write_status();

        // Restart with backoff until it sticks or the episode cap is hit.
        loop {
            attempts += 1;
            if attempts > RESTART_MAX_ATTEMPTS {
                error!(
                    key = %slot.key,
                    "restart cap reached ({RESTART_MAX_ATTEMPTS} attempts) — giving up; the next client connection will retry"
                );
                let mut st = slot.state.lock().expect("slot state lock poisoned");
                if st.phase == Phase::Backoff {
                    st.phase = Phase::Failed;
                }
                drop(st);
                daemon.write_status();
                return;
            }
            let delay = RESTART_BASE_DELAY * 2u32.pow(attempts - 1);
            warn!(key = %slot.key, attempt = attempts, delay_ms = delay.as_millis() as u64, "restarting compute");
            tokio::select! {
                _ = tokio::time::sleep(delay) => {}
                _ = shutdown.changed() => return, // never respawn during shutdown
                _ = demoted(&mut leader) => {
                    // Never respawn while not the leader; the compute is
                    // already dead, so just settle the slot.
                    let mut st = slot.state.lock().expect("slot state lock poisoned");
                    if st.phase == Phase::Backoff {
                        st.phase = Phase::Stopped;
                        st.last_exit = Some("not respawned: lost the leader lease".into());
                    }
                    drop(st);
                    daemon.write_status();
                    return;
                }
            }

            let _guard = slot.spawn_lock.lock().await;
            if slot.generation.load(Ordering::SeqCst) != generation {
                return; // a connection re-woke the compute during backoff
            }
            // Mirror ensure_running's defense in depth: a demote can land
            // AFTER the backoff select's sleep arm already won, and a
            // deposed leader must never spawn a writer — its --tail-quorum
            // election would FENCE the new leader's healthy writer. (The
            // residual window — a demote arriving DURING spawn_compute —
            // cannot be closed without coupling the data-tail election to
            // the lease term; the module docs state it, and the health
            // checker is the recovery route.)
            if !daemon.is_leader() {
                let mut st = slot.state.lock().expect("slot state lock poisoned");
                if st.phase == Phase::Backoff {
                    st.phase = Phase::Stopped;
                    st.last_exit = Some("not respawned: lost the leader lease".into());
                }
                drop(st);
                daemon.write_status();
                return;
            }
            match spawn_compute(&daemon, &slot, port).await {
                Ok((c, g)) => {
                    child = c;
                    generation = g;
                    started = Instant::now();
                    slot.restarts.fetch_add(1, Ordering::SeqCst);
                    daemon.write_status();
                    info!(key = %slot.key, port, restarts = slot.restarts.load(Ordering::SeqCst), "supervised restart succeeded");
                    if daemon.pool_enabled {
                        // Re-warm the pool for the fresh process.
                        tokio::spawn(warm_pool(daemon.clone(), slot.clone()));
                    }
                    break; // back to waiting on the new child
                }
                Err(e) => {
                    error!(key = %slot.key, "supervised restart attempt {attempts} failed: {e:#}");
                }
            }
        }
    }
}

/// Resolves when leadership is LOST (watch value `false`). With the lease
/// disabled the value is a constant `true` and the sender is dropped, so
/// this pends forever — the select arms built on it are inert.
async fn demoted(rx: &mut tokio::sync::watch::Receiver<bool>) {
    loop {
        if !*rx.borrow_and_update() {
            return;
        }
        if rx.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }
}

/// One compute generation's health loop (`--health-check-ms`): probe
/// `GET /health` every interval; [`HEALTH_MAX_FAILS`] consecutive failures
/// — unreachable/hung, or a non-200 such as the `503 tail unhealthy` a
/// compute with a POISONED quorum tail reports (alive on TCP, can never
/// ack a write: the case a bare TCP probe misses) — kill the compute so
/// the supervisor replaces it. The replacement's `--tail-quorum` open()
/// fences the old term and replays the un-flushed window before its
/// pgwire listener binds. The kill is logged with an epoch-ms timestamp
/// for failover_ms measurement.
async fn health_watch(
    daemon: Arc<Daemon>,
    slot: Arc<ComputeSlot>,
    generation: u64,
    health_port: u16,
    pid: u32,
) {
    let interval = Duration::from_millis(daemon.args.health_check_ms.max(100));
    let probe_timeout = interval.clamp(Duration::from_millis(250), Duration::from_secs(2));
    let mut shutdown = daemon.shutdown.subscribe();
    let mut fails: u32 = 0;
    let mut last_err = String::new();
    loop {
        tokio::select! {
            _ = tokio::time::sleep(interval) => {}
            _ = shutdown.changed() => return,
        }
        if slot.generation.load(Ordering::SeqCst) != generation {
            return; // superseded by a newer spawn
        }
        {
            let st = slot.state.lock().expect("slot state lock poisoned");
            if st.phase != Phase::Running || st.pid != Some(pid) {
                return; // exited/stopped: the monitor owns the aftermath
            }
        }
        match http_health(&daemon.args.compute_host, health_port, probe_timeout).await {
            Ok(()) => fails = 0,
            Err(why) => {
                fails += 1;
                last_err = why;
                warn!(
                    key = %slot.key, pid, health_port, fails,
                    "compute health probe failed ({fails}/{HEALTH_MAX_FAILS}): {last_err}"
                );
            }
        }
        if fails >= HEALTH_MAX_FAILS {
            if slot.generation.load(Ordering::SeqCst) != generation {
                return;
            }
            error!(
                key = %slot.key,
                pid,
                unhealthy_at_epoch_ms = epoch_ms(SystemTime::now()) as u64,
                "compute failed {HEALTH_MAX_FAILS} consecutive health probes ({last_err}) \
                 — killing it for supervised replacement (a wedged tail can never ack; \
                 the replacement's quorum-tail election fences the old term and replays)"
            );
            let _ = std::process::Command::new("kill")
                .args(["-KILL", &pid.to_string()])
                .status();
            return; // monitor_compute observes the unclean exit and respawns
        }
    }
}

/// Minimal `GET /health` over raw TCP (the control plane carries no HTTP
/// client): `Ok` on a 200 status line, `Err(reason)` otherwise.
async fn http_health(host: &str, port: u16, timeout: Duration) -> Result<(), String> {
    let probe = async {
        let mut s = TcpStream::connect(format!("{host}:{port}"))
            .await
            .map_err(|e| format!("connect: {e}"))?;
        s.write_all(b"GET /health HTTP/1.1\r\nhost: icegresd\r\nconnection: close\r\n\r\n")
            .await
            .map_err(|e| format!("write: {e}"))?;
        let mut buf = Vec::with_capacity(512);
        let mut chunk = [0u8; 512];
        loop {
            match s.read(&mut chunk).await {
                Ok(0) => break,
                Ok(n) => {
                    buf.extend_from_slice(&chunk[..n]);
                    if buf.len() >= 4096 {
                        break;
                    }
                }
                Err(e) => return Err(format!("read: {e}")),
            }
        }
        parse_health_response(&buf)
    };
    match tokio::time::timeout(timeout, probe).await {
        Ok(res) => res,
        Err(_) => Err(format!("no response within {timeout:?}")),
    }
}

/// Status-line check (pure, unit-tested): 200 = healthy; anything else is
/// the failure reason, with the body carried along (e.g. `tail unhealthy:
/// ... POISONED ...`) so the kill log names the real cause.
fn parse_health_response(raw: &[u8]) -> Result<(), String> {
    let text = String::from_utf8_lossy(raw);
    let status = text.lines().next().unwrap_or("").trim().to_string();
    let mut parts = status.split_whitespace();
    match (parts.next(), parts.next()) {
        (Some(proto), Some("200")) if proto.starts_with("HTTP/") => Ok(()),
        _ if status.is_empty() => Err("empty response".to_string()),
        _ => {
            let body = text.split("\r\n\r\n").nth(1).unwrap_or("").trim();
            if body.is_empty() {
                Err(status)
            } else {
                Err(format!("{status} — {body}"))
            }
        }
    }
}

/// Pop a live, current-generation warm connection from the slot's pool.
/// Dead conns (compute exited: EOF/err on a zero-length peek) and stale
/// conns (older compute generation) are silently discarded.
async fn take_warm(slot: &Arc<ComputeSlot>) -> Option<WarmConn> {
    let gen_now = slot.generation.load(Ordering::SeqCst);
    {
        let st = slot.state.lock().expect("slot state lock poisoned");
        if st.phase != Phase::Running {
            return None;
        }
    }
    let mut conns = slot.pool.conns.lock().await;
    while let Some(w) = conns.pop_front() {
        slot.pool.warm.store(conns.len(), Ordering::SeqCst);
        if w.generation != gen_now {
            continue;
        }
        // Liveness: an idle warm conn must have NOTHING to read. EOF or
        // stray bytes (e.g. a dying compute's error) mean it is unusable.
        let mut b = [0u8; 1];
        match w.stream.try_read(&mut b) {
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => return Some(w),
            _ => continue,
        }
    }
    None
}

/// Refill the slot's pool to --pool-size in the background (serialized per
/// slot). Stops quietly when the compute is not Running, the pool is
/// idle-eligible for drain, or warming fails transiently; disables pooling
/// for good if the compute demands authentication.
async fn warm_pool(daemon: Arc<Daemon>, slot: Arc<ComputeSlot>) {
    let _guard = slot.pool.warm_lock.lock().await;
    let mut added = false;
    loop {
        if *daemon.shutdown.borrow() || slot.pool.disabled.load(Ordering::SeqCst) {
            break;
        }
        let gen_now = slot.generation.load(Ordering::SeqCst);
        let port = {
            let st = slot.state.lock().expect("slot state lock poisoned");
            if st.phase != Phase::Running {
                break;
            }
            st.port
        };
        // Don't refill a pool the idle-drain loop is about to empty.
        let idle_for = slot
            .pool
            .last_client
            .lock()
            .expect("pool clock lock poisoned")
            .elapsed();
        if slot.active.load(Ordering::SeqCst) == 0
            && idle_for >= Duration::from_secs(daemon.args.pool_idle_secs)
        {
            break;
        }
        if slot.pool.conns.lock().await.len() >= daemon.args.pool_size {
            break;
        }
        match tokio::time::timeout(
            Duration::from_secs(10),
            warm_one(&daemon, &slot, port, gen_now),
        )
        .await
        {
            Ok(Ok(w)) => {
                let mut conns = slot.pool.conns.lock().await;
                conns.push_back(w);
                slot.pool.warm.store(conns.len(), Ordering::SeqCst);
                added = true;
            }
            Ok(Err(WarmError::AuthRequired)) => {
                slot.pool.disabled.store(true, Ordering::SeqCst);
                warn!(
                    key = %slot.key,
                    "compute requires authentication — session pooling DISABLED for this \
                     compute (icegresd cannot pre-authenticate on a client's behalf); all \
                     sessions go direct"
                );
                break;
            }
            Ok(Err(WarmError::Other(e))) => {
                // Transient (compute idle-exited mid-warm, restarting, ...):
                // the next wake or handout re-triggers warming.
                info!(key = %slot.key, "pool warm attempt stopped: {e:#}");
                break;
            }
            Err(_) => {
                warn!(key = %slot.key, "pool warm handshake timed out");
                break;
            }
        }
    }
    if added {
        daemon.write_status();
    }
}

/// Open ONE warm backend connection: TCP connect, send a StartupMessage as
/// `--pool-user` on the slot's canonical database, and cache the backend's
/// greeting (AuthenticationOk .. ReadyForQuery) for later replay to the
/// client this connection is handed to.
async fn warm_one(
    daemon: &Arc<Daemon>,
    slot: &Arc<ComputeSlot>,
    port: u16,
    generation: u64,
) -> Result<WarmConn, WarmError> {
    let addr = format!("{}:{}", daemon.args.compute_host, port);
    let mut stream = TcpStream::connect(&addr)
        .await
        .with_context(|| format!("pool warm dial to {addr} failed"))
        .map_err(WarmError::Other)?;
    let _ = stream.set_nodelay(true);

    // StartupMessage: protocol 3.0 + user/database/application_name.
    let db = slot.canonical_db();
    let mut body: Vec<u8> = Vec::with_capacity(64);
    body.extend_from_slice(&196_608u32.to_be_bytes());
    for (k, v) in [
        ("user", daemon.args.pool_user.as_str()),
        ("database", db.as_str()),
        ("application_name", "icegresd-pool"),
    ] {
        body.extend_from_slice(k.as_bytes());
        body.push(0);
        body.extend_from_slice(v.as_bytes());
        body.push(0);
    }
    body.push(0);
    let mut msg = ((body.len() as u32 + 4).to_be_bytes()).to_vec();
    msg.extend_from_slice(&body);
    stream
        .write_all(&msg)
        .await
        .context("pool warm startup write failed")
        .map_err(WarmError::Other)?;

    // Read backend messages until ReadyForQuery, caching the raw bytes.
    // First message MUST be AuthenticationOk (type 'R', code 0) — anything
    // else means the compute wants an auth exchange we cannot cache.
    let mut greeting: Vec<u8> = Vec::with_capacity(512);
    let mut first = true;
    loop {
        let mut hdr = [0u8; 5];
        stream
            .read_exact(&mut hdr)
            .await
            .context("compute closed during the pool warm handshake")
            .map_err(WarmError::Other)?;
        let ty = hdr[0];
        let len = u32::from_be_bytes(hdr[1..5].try_into().expect("4-byte slice")) as usize;
        if !(4..=256 * 1024).contains(&len) {
            return Err(WarmError::Other(anyhow::anyhow!(
                "implausible backend message length {len} during warm handshake"
            )));
        }
        let mut payload = vec![0u8; len - 4];
        stream
            .read_exact(&mut payload)
            .await
            .context("compute closed mid-message during the pool warm handshake")
            .map_err(WarmError::Other)?;
        if first {
            if ty != b'R' || payload.len() < 4 {
                return Err(WarmError::Other(anyhow::anyhow!(
                    "warm handshake: expected an Authentication message, got type {:?}",
                    ty as char
                )));
            }
            let code = u32::from_be_bytes(payload[0..4].try_into().expect("4-byte slice"));
            if code != 0 {
                return Err(WarmError::AuthRequired);
            }
            first = false;
        }
        if ty == b'E' {
            return Err(WarmError::Other(anyhow::anyhow!(
                "compute rejected the warm session: {}",
                String::from_utf8_lossy(&payload)
            )));
        }
        greeting.extend_from_slice(&hdr);
        greeting.extend_from_slice(&payload);
        if greeting.len() > 256 * 1024 {
            return Err(WarmError::Other(anyhow::anyhow!(
                "warm handshake greeting exceeded 256 KiB without ReadyForQuery"
            )));
        }
        if ty == b'Z' {
            break;
        }
    }
    Ok(WarmConn {
        stream,
        greeting,
        generation,
    })
}

/// Every 2 s: drain the warm pool of any compute that has had zero CLIENT
/// sessions for --pool-idle-secs. Warm conns are active sessions from the
/// compute's point of view, so the drain is what re-arms the compute's own
/// --idle-shutdown-secs scale-to-zero clock; the next wake re-warms.
async fn pool_idle_drain_loop(daemon: Arc<Daemon>) {
    let mut shutdown = daemon.shutdown.subscribe();
    loop {
        tokio::select! {
            _ = tokio::time::sleep(Duration::from_secs(2)) => {}
            _ = shutdown.changed() => return,
        }
        let slots: Vec<Arc<ComputeSlot>> = {
            let slots = daemon.slots.lock().expect("slots lock poisoned");
            slots.values().cloned().collect()
        };
        for slot in slots {
            if slot.active.load(Ordering::SeqCst) != 0 {
                continue;
            }
            let idle_for = slot
                .pool
                .last_client
                .lock()
                .expect("pool clock lock poisoned")
                .elapsed();
            if idle_for < Duration::from_secs(daemon.args.pool_idle_secs) {
                continue;
            }
            let drained = {
                let mut conns = slot.pool.conns.lock().await;
                let n = conns.len();
                conns.clear();
                slot.pool.warm.store(0, Ordering::SeqCst);
                n
            };
            if drained > 0 {
                info!(
                    key = %slot.key,
                    drained,
                    idle_secs = idle_for.as_secs(),
                    "pool idle-drained; the compute's idle-shutdown clock can now run"
                );
                daemon.write_status();
            }
        }
    }
}

/// Does `host:port` accept a TCP connection right now? (100 ms cap.)
async fn tcp_ready(host: &str, port: u16) -> bool {
    matches!(
        tokio::time::timeout(
            Duration::from_millis(100),
            TcpStream::connect(format!("{host}:{port}")),
        )
        .await,
        Ok(Ok(_))
    )
}

/// Ask the kernel for a free localhost port (bind :0, read, drop). The tiny
/// TOCTOU window is acceptable on a single-host control plane.
fn ephemeral_port() -> Result<u16> {
    let l = std::net::TcpListener::bind("127.0.0.1:0")
        .context("could not allocate an ephemeral port")?;
    Ok(l.local_addr()?.port())
}

/// Best-effort pgwire ErrorResponse so psql shows a real message instead of
/// a bare connection reset.
async fn send_pg_error(client: &mut TcpStream, sqlstate: &str, message: &str) {
    let mut fields = Vec::new();
    for (tag, val) in [(b'S', "FATAL"), (b'C', sqlstate), (b'M', message)] {
        fields.push(tag);
        fields.extend_from_slice(val.as_bytes());
        fields.push(0);
    }
    fields.push(0);
    let mut msg = Vec::with_capacity(fields.len() + 5);
    msg.push(b'E');
    msg.extend_from_slice(&((fields.len() as u32 + 4).to_be_bytes()));
    msg.extend_from_slice(&fields);
    let _ = client.write_all(&msg).await;
    let _ = client.shutdown().await;
}

// ---------------------------------------------------------------------------
// Unit tests: the routing decisions (endpoint identity + autoscale) and the
// health-probe response parse. The lease state machine's own tests live in
// src/lease.rs (compiled into this binary via the #[path] include).
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_connection_cap_has_a_finite_default_and_explicit_opt_out() {
        let cli = Cli::try_parse_from(["icegresd", "serve"]).unwrap();
        let DCommand::Serve(args) = cli.command else {
            panic!("serve command did not parse");
        };
        assert_eq!(args.max_connections, 512);

        let cli = Cli::try_parse_from(["icegresd", "serve", "--max-connections", "0"]).unwrap();
        let DCommand::Serve(args) = cli.command else {
            panic!("serve command did not parse");
        };
        assert_eq!(args.max_connections, 0);
    }

    // -------- route_database: endpoint identity, never SQL parsing --------

    #[test]
    fn routes_main_branch_and_read_endpoints() {
        assert_eq!(route_database(None, false).unwrap(), Route::Main);
        assert_eq!(route_database(Some("icegres"), false).unwrap(), Route::Main);
        assert_eq!(
            route_database(Some("icegres@dev"), false).unwrap(),
            Route::Branch("dev".into())
        );
        // ":ro" with the pool ON routes to the read pool; the prefix is
        // ignored like main routing ignores the database name.
        assert_eq!(
            route_database(Some("icegres:ro"), true).unwrap(),
            Route::Read
        );
        assert_eq!(
            route_database(Some("anything:ro"), true).unwrap(),
            Route::Read
        );
        // Branch read pools are refused loudly, not misrouted.
        assert!(route_database(Some("icegres@dev:ro"), true).is_err());
    }

    #[test]
    fn read_suffix_is_inert_when_the_pool_is_off() {
        // Byte-identical default: without --read-replicas-max, ":ro" means
        // nothing — same routing as before the flag existed.
        assert_eq!(
            route_database(Some("icegres:ro"), false).unwrap(),
            Route::Main
        );
        // "<db>@<branch>:ro" was an invalid branch name before and stays one.
        assert!(route_database(Some("icegres@dev:ro"), false).is_err());
        assert!(route_database(Some("icegres@"), false).is_err());
    }

    // -------- route_read: the autoscale-lite decision --------

    #[test]
    fn read_routing_scales_up_at_the_threshold_and_never_refuses() {
        // Nothing running: slot 0 (the wake IS the first scale-up).
        assert_eq!(route_read(&[None, None, None], 4), 0);
        // Headroom on a running replica: least-loaded wins.
        assert_eq!(route_read(&[Some(3), Some(1), None], 4), 1);
        // Every running replica at the threshold: wake the first free slot.
        assert_eq!(route_read(&[Some(4), Some(5), None], 4), 2);
        assert_eq!(route_read(&[None, Some(4), None], 4), 0);
        // Pool maxed out: the least-loaded absorbs the overflow.
        assert_eq!(route_read(&[Some(9), Some(4), Some(6)], 4), 1);
        // Ties break toward the lowest index (stable, no flapping).
        assert_eq!(route_read(&[Some(2), Some(2)], 4), 0);
    }

    // -------- k8s_park_decision: the idle scale-to-zero gate --------

    #[test]
    fn k8s_park_gate_decision_matrix() {
        let w = Duration::from_secs(300); // the idle window
        let long = Duration::from_secs(301);
        let short = Duration::from_secs(299);
        let gate = k8s_park_decision;
        // The plain park: leader for a while, slot proxied traffic
        // (Running), zero sessions, idle a full window.
        assert_eq!(
            gate(true, &Phase::Running, 0, long, long, w),
            ParkDecision::Park
        );
        // Exactly at the window counts as elapsed (>=, not >).
        assert_eq!(gate(true, &Phase::Running, 0, w, w, w), ParkDecision::Park);
        // Standby ticks never park anything: the leader owns the lifecycle.
        assert_eq!(
            gate(false, &Phase::Running, 0, long, long, w),
            ParkDecision::Skip
        );
        // Live sessions, or a client seen inside the window, keep it up.
        assert_eq!(
            gate(true, &Phase::Running, 3, long, long, w),
            ParkDecision::Skip
        );
        assert_eq!(
            gate(true, &Phase::Running, 0, short, long, w),
            ParkDecision::Skip
        );
        // A NEW leader gets no instant park: sessions that survived the
        // failover on the DEPOSED instance (k8s demote severs nothing, and
        // they are invisible to this instance's clocks) drain for one idle
        // window first.
        assert_eq!(
            gate(true, &Phase::Running, 0, long, short, w),
            ParkDecision::Skip
        );
        // Restart / helm-upgrade / failed-wake residue: the slot never saw
        // a proxied connection (remote slots boot Stopped) — park only if
        // the workload probes as actually up, so the caller must probe.
        assert_eq!(
            gate(true, &Phase::Stopped, 0, long, long, w),
            ParkDecision::ProbeThenPark
        );
        assert_eq!(
            gate(true, &Phase::Starting, 0, long, long, w),
            ParkDecision::ProbeThenPark
        );
        // ... and every Skip reason still applies on the probe path.
        assert_eq!(
            gate(true, &Phase::Stopped, 0, short, long, w),
            ParkDecision::Skip
        );
        assert_eq!(
            gate(true, &Phase::Stopped, 0, long, short, w),
            ParkDecision::Skip
        );
        assert_eq!(
            gate(true, &Phase::Stopped, 1, long, long, w),
            ParkDecision::Skip
        );
        assert_eq!(
            gate(false, &Phase::Stopped, 0, long, long, w),
            ParkDecision::Skip
        );
    }

    // -------- effective_health_check_ms: the wedged-writer recovery route --------

    #[test]
    fn health_check_defaults_on_only_for_lease_plus_quorum_tail_in_process_mode() {
        // The exact combination where a demote-racing-spawn fence would
        // otherwise be a PERMANENT write outage: defaults on.
        assert_eq!(
            effective_health_check_ms(0, true, true, false),
            LEASE_QUORUM_HEALTH_CHECK_MS
        );
        // An explicit flag always wins (never overridden).
        assert_eq!(effective_health_check_ms(250, true, true, false), 250);
        // Everything else stays byte-identical to the flag default (off).
        assert_eq!(effective_health_check_ms(0, false, true, false), 0);
        assert_eq!(effective_health_check_ms(0, true, false, false), 0);
        assert_eq!(effective_health_check_ms(0, false, false, false), 0);
        // k8s mode never defaults it: the kubelet's liveness probe owns
        // compute health there and the flag is refused at boot.
        assert_eq!(effective_health_check_ms(0, true, true, true), 0);
    }

    // -------- parse_health_response --------

    #[test]
    fn health_parse_accepts_200_and_names_the_503_cause() {
        assert!(parse_health_response(b"HTTP/1.1 200 OK\r\ncontent-length: 3\r\n\r\nok\n").is_ok());
        let err = parse_health_response(
            b"HTTP/1.1 503 Service Unavailable\r\ncontent-length: 60\r\n\r\n\
              tail unhealthy: quorum tail is POISONED (superseded)\n",
        )
        .unwrap_err();
        assert!(err.contains("503"), "status carried: {err}");
        assert!(err.contains("POISONED"), "cause carried: {err}");
        assert!(parse_health_response(b"").is_err());
        assert!(parse_health_response(b"garbage\r\n\r\n").is_err());
    }
}
