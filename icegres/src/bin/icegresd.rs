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

use std::collections::HashMap;
use std::path::PathBuf;
use std::process::Stdio;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
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

#[derive(Parser)]
#[command(
    name = "icegresd",
    version,
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
    Serve(ServeArgs),
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
}

fn default_status_file() -> PathBuf {
    std::env::temp_dir().join("icegresd-status.json")
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    match Cli::parse().command {
        DCommand::Serve(args) => run_serve(args).await,
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
    spawned_at: Option<SystemTime>,
    last_exit: Option<String>,
}

/// One compute endpoint: the main one (`branch == None`, fixed port) or a
/// per-branch one (ephemeral port, `icegres serve --branch <name>`).
struct ComputeSlot {
    key: String,
    branch: Option<String>,
    /// Serializes spawn/respawn decisions for this slot.
    spawn_lock: tokio::sync::Mutex<()>,
    state: Mutex<SlotState>,
    /// Bumped on every successful spawn; a monitor task only acts while its
    /// generation is current (a newer spawn supersedes it).
    generation: AtomicU64,
    active: AtomicUsize,
    restarts: AtomicU64,
}

struct Daemon {
    args: ServeArgs,
    bin: PathBuf,
    status_file: PathBuf,
    slots: Mutex<HashMap<String, Arc<ComputeSlot>>>,
    /// Flipped to `true` exactly once, on daemon shutdown: monitor tasks
    /// then terminate AND REAP their computes (see `monitor_compute`).
    shutdown: tokio::sync::watch::Sender<bool>,
}

impl Daemon {
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
                        spawned_at: None,
                        last_exit: None,
                    }),
                    generation: AtomicU64::new(0),
                    active: AtomicUsize::new(0),
                    restarts: AtomicU64::new(0),
                })
            })
            .clone()
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
                    serde_json::json!({
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
                    })
                })
                .collect()
        };
        let doc = serde_json::json!({
            "daemon_pid": std::process::id(),
            "listen": format!("{}:{}", self.args.host, self.args.port),
            "icegres_bin": self.bin.display().to_string(),
            "updated_at_epoch_ms": epoch_ms(SystemTime::now()),
            "computes": computes,
        });
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

async fn run_serve(args: ServeArgs) -> Result<()> {
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

    let daemon = Arc::new(Daemon {
        args,
        bin,
        status_file,
        slots: Mutex::new(HashMap::new()),
        shutdown: tokio::sync::watch::channel(false).0,
    });
    daemon.write_status();

    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("failed to install SIGTERM handler")?;
    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((client, peer)) => {
                    let daemon = daemon.clone();
                    tokio::spawn(async move {
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
            slots.values().any(|s| {
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
/// database name, wake the target compute if needed, forward the original
/// startup bytes, splice.
async fn handle_client(daemon: Arc<Daemon>, mut client: TcpStream) -> Result<()> {
    let _ = client.set_nodelay(true);
    let (startup_raw, database) =
        match tokio::time::timeout(Duration::from_secs(10), read_startup(&mut client)).await {
            Ok(Ok(v)) => v,
            Ok(Err(e)) => {
                send_pg_error(&mut client, "08P01", &format!("icegresd: {e}")).await;
                return Err(e);
            }
            Err(_) => bail!("timed out waiting for the client startup message"),
        };

    let branch = match route_branch(database.as_deref()) {
        Ok(b) => b,
        Err(e) => {
            send_pg_error(&mut client, "3D000", &format!("icegresd: {e}")).await;
            return Err(e);
        }
    };
    let slot = daemon.slot(branch.as_deref());

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

    let compute_addr = format!("{}:{}", daemon.args.compute_host, port);
    let mut compute = match TcpStream::connect(&compute_addr).await {
        Ok(c) => c,
        Err(first_err) => {
            // The compute may have idle-exited in the instant between the
            // readiness check and the dial: wake once more.
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
        .write_all(&startup_raw)
        .await
        .context("failed to forward the startup message to the compute")?;

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
    daemon.write_status();
    // EOF/reset at either end just ends the session; not an icegresd error.
    if let Err(e) = res {
        info!(key = %slot.key, "splice ended: {e}");
    }
    Ok(())
}

/// Read the pgwire preamble from a fresh client connection, answering
/// SSLRequest/GSSENCRequest with `N` (plaintext only at the proxy), until
/// the protocol-3 StartupMessage arrives. Returns the raw startup bytes
/// (length prefix included, forwarded verbatim to the compute) and the
/// `database` parameter.
async fn read_startup(client: &mut TcpStream) -> Result<(Vec<u8>, Option<String>)> {
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
                let mut it = payload[4..].split(|&b| b == 0);
                while let Some(k) = it.next() {
                    if k.is_empty() {
                        break;
                    }
                    let v = it.next().unwrap_or(&[]);
                    if k == b"database" {
                        database = Some(String::from_utf8_lossy(v).into_owned());
                    }
                }
                let mut raw = Vec::with_capacity(len);
                raw.extend_from_slice(&lenb);
                raw.extend_from_slice(&payload);
                return Ok((raw, database));
            }
            _ => bail!("unsupported protocol version {code} (icegresd speaks pgwire 3.x)"),
        }
    }
}

/// Map the requested database name to a compute: `icegres` (or anything
/// without `@`, or none) -> main; `<db>@<branch>` -> the branch endpoint.
fn route_branch(database: Option<&str>) -> Result<Option<String>> {
    let Some(db) = database else { return Ok(None) };
    let Some((_, branch)) = db.split_once('@') else {
        return Ok(None);
    };
    if branch.is_empty()
        || !branch
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        bail!("invalid branch endpoint {db:?} (expected <db>@<branch>, branch = [A-Za-z0-9_-]+)");
    }
    Ok(Some(branch.to_string()))
}

/// Make sure the slot's compute is accepting connections, spawning it if
/// needed; returns (port, whether a wake was performed). Serialized per
/// slot so concurrent connections to a cold endpoint spawn exactly one
/// process.
async fn ensure_running(daemon: &Arc<Daemon>, slot: &Arc<ComputeSlot>) -> Result<(u16, bool)> {
    let _guard = slot.spawn_lock.lock().await;

    // Fast path: state says running AND the port actually accepts (the
    // probe catches a kill -9 the monitor task has not observed yet).
    let (phase, port) = {
        let st = slot.state.lock().expect("slot state lock poisoned");
        (st.phase.clone(), st.port)
    };
    if phase == Phase::Running && tcp_ready(&daemon.args.compute_host, port).await {
        return Ok((port, false));
    }

    let port = match slot.branch {
        None => daemon.args.main_port,
        Some(_) => ephemeral_port()?,
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
    loop {
        let status = tokio::select! {
            status = child.wait() => status,
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
            }

            let _guard = slot.spawn_lock.lock().await;
            if slot.generation.load(Ordering::SeqCst) != generation {
                return; // a connection re-woke the compute during backoff
            }
            match spawn_compute(&daemon, &slot, port).await {
                Ok((c, g)) => {
                    child = c;
                    generation = g;
                    started = Instant::now();
                    slot.restarts.fetch_add(1, Ordering::SeqCst);
                    daemon.write_status();
                    info!(key = %slot.key, port, restarts = slot.restarts.load(Ordering::SeqCst), "supervised restart succeeded");
                    break; // back to waiting on the new child
                }
                Err(e) => {
                    error!(key = %slot.key, "supervised restart attempt {attempts} failed: {e:#}");
                }
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
