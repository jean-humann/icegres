//! Operational features of `icegres serve`: scale-to-zero idle shutdown,
//! a dedicated TCP/HTTP health endpoint, TLS, and SCRAM authentication.
//!
//! # TLS (`--tls-cert` / `--tls-key`, SPEC A7) and auth (`--auth-file`, A6)
//!
//! When any of `--idle-shutdown-secs`, `--tls-cert/--tls-key` or
//! `--auth-file` is set, icegres runs its own accept loop (`serve_custom`)
//! instead of datafusion-postgres's stock `serve()`:
//!
//! * TLS: certificate/key PEM files are loaded with `build_tls_acceptor`,
//!   which FAILS THE BOOT on any error — unlike upstream
//!   `serve_with_handlers`, which logs a warning and silently falls back to
//!   plaintext. Like upstream Postgres without `hostssl` rules, a TLS-enabled
//!   listener still accepts plaintext startup (clients choose via `sslmode`);
//!   use `sslmode=require`/`verify-full` on clients to guarantee encryption.
//! * Auth: with `--auth-file`, every connection must complete a
//!   SCRAM-SHA-256 exchange against `pgauth::FileAuthSource` (wrong password
//!   or unknown user → FATAL 28P01). Without it, the historical permissive
//!   startup handler is kept and `main.rs` logs a startup WARN.
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

use std::fmt::Debug;
use std::fs::File;
use std::io::BufReader;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result};
use async_trait::async_trait;
use datafusion::prelude::SessionContext;
use datafusion_postgres::pgwire::api::auth::noop::NoopStartupHandler;
use datafusion_postgres::pgwire::api::auth::sasl::scram::ScramAuth;
use datafusion_postgres::pgwire::api::auth::sasl::SASLAuthStartupHandler;
use datafusion_postgres::pgwire::api::auth::{
    AuthSource, DefaultServerParameterProvider, StartupHandler,
};
use datafusion_postgres::pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use datafusion_postgres::pgwire::api::{ClientInfo, ErrorHandler, PgWireServerHandlers};
use datafusion_postgres::pgwire::error::{PgWireError, PgWireResult};
use datafusion_postgres::pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use datafusion_postgres::pgwire::tokio::tokio_rustls::rustls;
use datafusion_postgres::pgwire::tokio::{process_socket, TlsAcceptor};
use datafusion_postgres::DfSessionService;
use futures::Sink;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{info, warn};

use crate::pgauth::FileAuthSource;

/// Startup handler that accepts every connection without authentication —
/// the same behavior as datafusion-postgres's stock `serve()` path (its
/// `SimpleStartupHandler` is not exported, so we declare our own).
struct AcceptAllStartupHandler;
impl NoopStartupHandler for AcceptAllStartupHandler {}

/// Per-connection startup handler: permissive (no `--auth-file`) or
/// SCRAM-SHA-256 against the loaded auth file. An enum because
/// `PgWireServerHandlers::startup_handler` must name one concrete type.
enum IcegresStartupHandler {
    Open(AcceptAllStartupHandler),
    Scram(SASLAuthStartupHandler<DefaultServerParameterProvider>),
}

#[async_trait]
impl StartupHandler for IcegresStartupHandler {
    async fn on_startup<C>(
        &self,
        client: &mut C,
        message: PgWireFrontendMessage,
    ) -> PgWireResult<()>
    where
        C: ClientInfo + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        match self {
            IcegresStartupHandler::Open(h) => h.on_startup(client, message).await,
            IcegresStartupHandler::Scram(h) => h.on_startup(client, message).await,
        }
    }
}

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
/// `HandlerFactory` (same `DfSessionService` with the default query hooks),
/// plus optional SCRAM auth. `startup_handler()` is invoked once per
/// connection by `process_socket`, so the SASL state machine it returns is
/// per-connection as pgwire requires.
struct IcegresHandlerFactory {
    service: Arc<DfSessionService>,
    auth: Option<Arc<FileAuthSource>>,
}

impl PgWireServerHandlers for IcegresHandlerFactory {
    fn simple_query_handler(&self) -> Arc<impl SimpleQueryHandler> {
        self.service.clone()
    }

    fn extended_query_handler(&self) -> Arc<impl ExtendedQueryHandler> {
        self.service.clone()
    }

    fn startup_handler(&self) -> Arc<impl StartupHandler> {
        match &self.auth {
            Some(source) => {
                let auth_db: Arc<dyn AuthSource> = source.clone();
                Arc::new(IcegresStartupHandler::Scram(
                    SASLAuthStartupHandler::new(
                        Arc::new(DefaultServerParameterProvider::default()),
                    )
                    .with_scram(ScramAuth::new(auth_db)),
                ))
            }
            None => Arc::new(IcegresStartupHandler::Open(AcceptAllStartupHandler)),
        }
    }

    fn error_handler(&self) -> Arc<impl ErrorHandler> {
        Arc::new(LoggingErrorHandler)
    }
}

/// Build a rustls `TlsAcceptor` from PEM cert/key paths. Unlike upstream
/// datafusion-postgres (`serve_with_handlers` logs a warning and serves
/// PLAINTEXT when TLS setup fails), any error here aborts startup —
/// misconfigured TLS must never silently downgrade to unencrypted.
pub fn build_tls_acceptor(cert_path: &str, key_path: &str) -> Result<TlsAcceptor> {
    // Same crypto provider as upstream setup_tls (pgwire ships the ring
    // feature); install_default is idempotent, ignore the AlreadyInstalled err.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let certs = rustls_pemfile::certs(&mut BufReader::new(
        File::open(cert_path)
            .with_context(|| format!("failed to open TLS certificate {cert_path}"))?,
    ))
    .collect::<std::io::Result<Vec<_>>>()
    .with_context(|| format!("failed to parse PEM certificate(s) in {cert_path}"))?;
    if certs.is_empty() {
        anyhow::bail!("no PEM certificate found in {cert_path}");
    }

    let key = rustls_pemfile::private_key(&mut BufReader::new(
        File::open(key_path).with_context(|| format!("failed to open TLS key {key_path}"))?,
    ))
    .with_context(|| format!("failed to parse PEM private key in {key_path}"))?
    .ok_or_else(|| anyhow::anyhow!("no PEM private key found in {key_path}"))?;

    let config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(certs, key)
        .context("invalid TLS certificate/key pair")?;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// How often the idle watchdog wakes up to check the idle condition.
const IDLE_POLL: Duration = Duration::from_millis(250);

/// Serve the pgwire protocol like datafusion-postgres's `serve()`, with the
/// icegres-specific extensions: optional scale-to-zero idle shutdown
/// (`idle_secs`), optional TLS (`tls`), and optional SCRAM auth (`auth`).
/// Used whenever any of those is configured; the flagless path in `main.rs`
/// keeps the stock upstream loop byte-for-byte.
///
/// With `idle_secs = Some(n)` the loop exits cleanly (`Ok(())`) once there
/// have been zero client connections for `n` consecutive seconds (boot
/// counts); with `None` it runs forever.
pub async fn serve_custom(
    ctx: Arc<SessionContext>,
    host: &str,
    port: u16,
    idle_secs: Option<u64>,
    tls: Option<TlsAcceptor>,
    auth: Option<Arc<FileAuthSource>>,
) -> Result<()> {
    let addr = format!("{host}:{port}");
    let listener = TcpListener::bind(&addr)
        .await
        .with_context(|| format!("failed to bind pgwire listener on {addr}"))?;
    info!(
        listen_addr = %addr,
        idle_shutdown_secs = idle_secs,
        tls = tls.is_some(),
        auth = auth.is_some(),
        "listening (custom accept loop)"
    );

    let factory = Arc::new(IcegresHandlerFactory {
        service: Arc::new(DfSessionService::new(ctx)),
        auth,
    });
    let idle_window = idle_secs.map(Duration::from_secs);
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
                    let tls = tls.clone();
                    tokio::spawn(async move {
                        if let Err(e) = process_socket(socket, tls, factory).await {
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
            _ = tokio::time::sleep(IDLE_POLL), if idle_window.is_some() => {
                let idle_for = idle_since.lock().expect("idle clock lock poisoned").elapsed();
                if active.load(Ordering::SeqCst) == 0
                    && idle_window.is_some_and(|w| idle_for >= w)
                {
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
