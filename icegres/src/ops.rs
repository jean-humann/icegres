//! Operational features of `icegres serve`: scale-to-zero idle shutdown and
//! a dedicated TCP/HTTP health endpoint.
//!
//! # Scale-to-zero (`--idle-shutdown-secs`, SPEC §1 D5)
//!
//! With `--idle-shutdown-secs N` the server exits cleanly (code 0) once no
//! client connection has been open for `N` consecutive seconds (the timer
//! also starts at boot, so a server that never receives a connection shuts
//! down after `N` seconds). Because icegres computes are stateless — every
//! byte of durable state lives in the Iceberg catalog + object store (parity
//! probe D1) and a cold start is fast (parity probe D3) — exiting is safe at
//! any idle moment.
//!
//! Supervisor pattern: run icegres under any socket-activating or
//! auto-restarting supervisor and let the *supervisor* provide the
//! scale-from-zero half:
//!
//! ```text
//! # systemd (restart-on-demand flavor): the unit exits when idle and the
//! # next client connection is what wakes it up via socket activation
//! [Service]
//! ExecStart=/usr/local/bin/icegres serve --idle-shutdown-secs 300
//! Restart=on-failure          # clean idle exit (code 0) does NOT restart
//!
//! # or a shell supervisor that restarts on demand:
//! while :; do icegres serve --idle-shutdown-secs 300; done
//! ```
//!
//! The health endpoint (below) deliberately does **not** count as client
//! activity, so liveness probes never keep an idle server alive.
//!
//! # Health endpoint (`--health-port`, SPEC §1 E2)
//!
//! A minimal HTTP responder on a separate port: any TCP connection (and any
//! HTTP request path, e.g. `GET /health`) receives `HTTP/1.1 200 OK` with
//! body `ok\n`. It answers as soon as the pgwire listener is up, and it is a
//! *liveness* probe: it asserts the process is alive and accepting, not that
//! the catalog is reachable. A full readiness probe is a pgwire round trip
//! (`psql -c 'select 1'`), which is what the bench/parity harnesses use.
//! Plain TCP health checks (e.g. Kubernetes `tcpSocket`, `nc -z`) work too:
//! connect + close is enough.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result};
use datafusion::prelude::SessionContext;
use datafusion_postgres::pgwire::api::auth::noop::NoopStartupHandler;
use datafusion_postgres::pgwire::api::auth::StartupHandler;
use datafusion_postgres::pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use datafusion_postgres::pgwire::api::{ClientInfo, ErrorHandler, PgWireServerHandlers};
use datafusion_postgres::pgwire::error::PgWireError;
use datafusion_postgres::pgwire::tokio::{process_socket, TlsAcceptor};
use datafusion_postgres::DfSessionService;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{info, warn};

/// Startup handler that accepts every connection without authentication —
/// the same behavior as datafusion-postgres's stock `serve()` path (its
/// `SimpleStartupHandler` is not exported, so we declare our own).
struct AcceptAllStartupHandler;
impl NoopStartupHandler for AcceptAllStartupHandler {}

/// Error handler mirroring the stock factory's logging behavior.
struct LoggingErrorHandler;
impl ErrorHandler for LoggingErrorHandler {
    fn on_error<C>(&self, _client: &C, error: &mut PgWireError)
    where
        C: ClientInfo,
    {
        info!("Sending error: {error}");
    }
}

/// pgwire handler factory equivalent to datafusion-postgres's private
/// `HandlerFactory` (same `DfSessionService` with the default query hooks,
/// same no-op auth), used by the idle-shutdown accept loop.
struct IdleHandlerFactory {
    service: Arc<DfSessionService>,
}

impl PgWireServerHandlers for IdleHandlerFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.service.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.service.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        Arc::new(AcceptAllStartupHandler)
    }

    fn error_handler(&self) -> Arc<impl ErrorHandler> {
        Arc::new(LoggingErrorHandler)
    }
}

/// How often the idle watchdog wakes up to check the idle condition.
const IDLE_POLL: Duration = Duration::from_millis(250);

/// Serve the pgwire protocol like datafusion-postgres's `serve()`, but exit
/// cleanly (`Ok(())`) once there have been zero client connections for
/// `idle_secs` consecutive seconds. Used only when `--idle-shutdown-secs`
/// is set; the flagless path keeps the stock upstream loop.
pub async fn serve_with_idle_shutdown(
    ctx: Arc<SessionContext>,
    host: &str,
    port: u16,
    idle_secs: u64,
) -> Result<()> {
    let addr = format!("{host}:{port}");
    let listener = TcpListener::bind(&addr)
        .await
        .with_context(|| format!("failed to bind pgwire listener on {addr}"))?;
    info!(listen_addr = %addr, idle_shutdown_secs = idle_secs, "listening (scale-to-zero enabled)");

    let factory = Arc::new(IdleHandlerFactory {
        service: Arc::new(DfSessionService::new(ctx)),
    });
    let idle_window = Duration::from_secs(idle_secs);
    let active = Arc::new(AtomicUsize::new(0));
    // Instant of the last transition to the fully-idle state (boot counts).
    let idle_since = Arc::new(Mutex::new(Instant::now()));

    loop {
        tokio::select! {
            accepted = listener.accept() => match accepted {
                Ok((socket, peer)) => {
                    active.fetch_add(1, Ordering::SeqCst);
                    let factory = factory.clone();
                    let active = active.clone();
                    let idle_since = idle_since.clone();
                    tokio::spawn(async move {
                        if let Err(e) = process_socket(socket, None::<TlsAcceptor>, factory).await {
                            warn!(%peer, "error processing socket: {e}");
                        }
                        // Reset the idle clock BEFORE decrementing so the
                        // watchdog can never observe (active == 0, stale
                        // idle_since).
                        *idle_since.lock().expect("idle clock lock poisoned") = Instant::now();
                        active.fetch_sub(1, Ordering::SeqCst);
                    });
                }
                Err(e) => warn!("error accepting socket: {e}"),
            },
            _ = tokio::time::sleep(IDLE_POLL) => {
                let idle_for = idle_since.lock().expect("idle clock lock poisoned").elapsed();
                if active.load(Ordering::SeqCst) == 0 && idle_for >= idle_window {
                    info!(
                        idle_secs = idle_for.as_secs(),
                        "no client connections within the idle window; shutting down (scale-to-zero)"
                    );
                    return Ok(());
                }
            }
        }
    }
}

/// Bind a minimal HTTP liveness endpoint on `host:port` and serve it from a
/// background task. Every connection gets `200 OK` + body `ok\n` regardless
/// of the request. Binding errors are returned (loud at startup); per-
/// connection errors are logged and ignored.
pub async fn spawn_health_listener(host: &str, port: u16) -> Result<()> {
    let addr = format!("{host}:{port}");
    let listener = TcpListener::bind(&addr)
        .await
        .with_context(|| format!("failed to bind health listener on {addr}"))?;
    info!(health_addr = %addr, "health endpoint listening (HTTP 200 'ok' liveness probe)");
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((mut socket, _)) => {
                    tokio::spawn(async move {
                        // Best-effort read of the request (a plain TCP
                        // connect-and-close health check sends nothing).
                        let mut buf = [0u8; 1024];
                        let _ = socket.read(&mut buf).await;
                        let _ = socket
                            .write_all(
                                b"HTTP/1.1 200 OK\r\n\
                                  content-type: text/plain\r\n\
                                  content-length: 3\r\n\
                                  connection: close\r\n\r\nok\n",
                            )
                            .await;
                        let _ = socket.shutdown().await;
                    });
                }
                Err(e) => warn!("health listener accept error: {e}"),
            }
        }
    });
    Ok(())
}
