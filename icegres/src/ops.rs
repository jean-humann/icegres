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
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result};
use async_trait::async_trait;
use datafusion::common::{DFSchema, ParamValues};
use datafusion::logical_expr::{EmptyRelation, LogicalPlan};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::{
    CopyOption, CopySource, CopyTarget, Ident, Statement as SqlStatement,
};
use datafusion_postgres::arrow_pg::datatypes::arrow_schema_to_pg_fields;
use datafusion_postgres::arrow_pg::encoder::{encode_value, Encoder as ArrowPgEncoder};
use datafusion_postgres::pgwire::api::auth::noop::NoopStartupHandler;
use datafusion_postgres::pgwire::api::auth::sasl::scram::ScramAuth;
use datafusion_postgres::pgwire::api::auth::sasl::SASLAuthStartupHandler;
use datafusion_postgres::pgwire::api::auth::{
    AuthSource, DefaultServerParameterProvider, StartupHandler,
};
use datafusion_postgres::pgwire::api::portal::Format;
use datafusion_postgres::pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use datafusion_postgres::pgwire::api::results::{
    CopyCsvOptions, CopyEncoder, CopyResponse, Response,
};
use datafusion_postgres::pgwire::api::{ClientInfo, ErrorHandler, PgWireServerHandlers};
use datafusion_postgres::pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use datafusion_postgres::pgwire::messages::copy::CopyData;
use datafusion_postgres::pgwire::messages::{PgWireBackendMessage, PgWireFrontendMessage};
use datafusion_postgres::pgwire::tokio::tokio_rustls::rustls;
use datafusion_postgres::pgwire::tokio::{process_socket, TlsAcceptor};
use datafusion_postgres::{DfSessionService, QueryHook};
use futures::{Sink, StreamExt as _};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tracing::{info, warn};

use crate::pgauth::FileAuthSource;
use crate::txn::TxnRegistry;

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
    hooks: Vec<Arc<dyn QueryHook>>,
    txn_registry: Arc<TxnRegistry>,
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
        service: Arc::new(DfSessionService::new_with_hooks(ctx, hooks)),
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
                    let txn_registry = txn_registry.clone();
                    tokio::spawn(async move {
                        if let Err(e) = process_socket(socket, tls, factory).await {
                            warn!(%peer, "error processing socket: {e}");
                        }
                        // Disconnect = implicit ROLLBACK: drop any open
                        // transaction buffered for this connection (nothing
                        // was committed, so nothing needs undoing).
                        txn_registry.disconnect(&peer);
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

// ---------------------------------------------------------------------------
// COPY ... TO STDOUT (SPEC A11 lane 2: adbc_driver_postgresql reads)
// ---------------------------------------------------------------------------

/// Query hook answering `COPY (SELECT ...) TO STDOUT (FORMAT binary|text|csv)`
/// and `COPY table [(cols)] TO STDOUT ...` with a real `CopyOutResponse` +
/// PGCOPY-encoded `CopyData` stream, on BOTH protocols (libpq `PQexecParams`
/// drives COPY through the extended protocol; psql's `COPY` uses simple).
///
/// This is the read path `adbc_driver_postgresql` requires: the driver wraps
/// every result fetch in `COPY (query) TO STDOUT (FORMAT binary)` and decodes
/// the PG binary COPY framing into Arrow. Values are encoded per-field by
/// arrow-pg's `encode_value` — the exact binary encoders the normal DataRow
/// path uses — through pgwire's `CopyEncoder` (which adds the PGCOPY
/// header/row framing; the binary trailer comes from `CopyResponse::new`).
///
/// Why a `QueryHook` and not pgwire's `CopyHandler` trait: `CopyHandler`
/// (pgwire src/api/copy.rs) handles the *frontend* copy messages of `COPY
/// FROM STDIN` (CopyData/CopyDone/CopyFail). COPY TO is initiated by a
/// regular query whose response is `Response::CopyOut`; the stock
/// `NoopHandler` remains in place so `COPY FROM STDIN` fails loudly
/// ("feature not implemented") instead of hanging.
///
/// Scope (rejected loudly with 0A000 otherwise):
/// * `TO STDOUT` only (no files/programs — the server must not write disk);
/// * options: `FORMAT binary|text|csv` and CSV `HEADER` (Postgres defaults
///   otherwise: text format, tab delimiter, `\N` null);
/// * `COPY ... FROM STDIN` is out of scope (use INSERT/adbc_ingest).
///
/// Runs BEFORE TxnHook in the hook chain: inside an explicit transaction a
/// COPY TO reads the latest committed snapshot (statement-level consistency)
/// rather than the transaction's pinned view — the ADBC driver's read flow
/// (BEGIN; COPY ...; COMMIT) sees exactly one snapshot per COPY either way.
pub struct CopyOutHook;

/// The COPY output format requested in the statement options.
#[derive(Clone, Copy, PartialEq, Debug)]
enum CopyOutFormat {
    Text,
    Csv { header: bool },
    Binary,
}

/// `feature_not_supported` (0A000) for out-of-scope COPY forms.
fn copy_reject(msg: String) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        "0A000".to_string(),
        msg,
    )))
}

/// Parse a COPY statement into `(select_sql, format)` when it is a supported
/// `COPY ... TO STDOUT`; `Ok(None)` when the statement is not COPY at all.
fn translate_copy(stmt: &SqlStatement) -> PgWireResult<Option<(String, CopyOutFormat)>> {
    let SqlStatement::Copy {
        source,
        to,
        target,
        options,
        legacy_options,
        ..
    } = stmt
    else {
        return Ok(None);
    };
    if !*to {
        return Err(copy_reject(
            "COPY ... FROM is not supported; load data with INSERT or ADBC bulk ingest \
             (icegres flight-serve)"
                .to_string(),
        ));
    }
    if !matches!(target, CopyTarget::Stdout) {
        return Err(copy_reject(format!(
            "COPY TO {target} is not supported (only COPY ... TO STDOUT)"
        )));
    }
    let mut format = CopyOutFormat::Text;
    let mut header = false;
    for opt in options {
        match opt {
            CopyOption::Format(ident) => {
                format = match ident.value.to_lowercase().as_str() {
                    "text" => CopyOutFormat::Text,
                    "csv" => CopyOutFormat::Csv { header: false },
                    "binary" => CopyOutFormat::Binary,
                    other => {
                        return Err(copy_reject(format!(
                            "COPY format {other:?} is not supported (text, csv, binary)"
                        )))
                    }
                };
            }
            CopyOption::Header(h) => header = *h,
            other => {
                return Err(copy_reject(format!(
                    "COPY option {other} is not supported (FORMAT text|csv|binary, HEADER)"
                )))
            }
        }
    }
    if !legacy_options.is_empty() {
        return Err(copy_reject(
            "legacy (pre-9.0) COPY options are not supported; use COPY (...) TO STDOUT \
             (FORMAT ...)"
                .to_string(),
        ));
    }
    if header {
        match &mut format {
            CopyOutFormat::Csv { header } => *header = true,
            _ => {
                return Err(copy_reject(
                    "COPY HEADER is only supported with FORMAT csv".to_string(),
                ))
            }
        }
    }
    let select_sql = match source {
        CopySource::Query(query) => query.to_string(),
        CopySource::Table {
            table_name,
            columns,
        } => {
            let cols = if columns.is_empty() {
                "*".to_string()
            } else {
                columns
                    .iter()
                    .map(|c: &Ident| c.to_string())
                    .collect::<Vec<_>>()
                    .join(", ")
            };
            format!("SELECT {cols} FROM {table_name}")
        }
    };
    Ok(Some((select_sql, format)))
}

impl CopyOutHook {
    /// Execute the underlying SELECT and build the streaming CopyOut
    /// response (rows are encoded batch-by-batch as they arrive).
    async fn run(
        &self,
        select_sql: &str,
        format: CopyOutFormat,
        ctx: &SessionContext,
    ) -> PgWireResult<Response> {
        let df = ctx
            .sql(select_sql)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        let stream = df
            .execute_stream()
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        let arrow_schema = stream.schema();
        let pg_fields = Arc::new(arrow_schema_to_pg_fields(
            &arrow_schema,
            &Format::UnifiedText,
            None,
        )?);
        let ncols = pg_fields.len();

        let mut encoder = match format {
            CopyOutFormat::Binary => CopyEncoder::new_binary(pg_fields.clone()),
            CopyOutFormat::Text => CopyEncoder::new_text(pg_fields.clone(), Default::default()),
            CopyOutFormat::Csv { .. } => {
                CopyEncoder::new_csv(pg_fields.clone(), CopyCsvOptions::default())
            }
        };
        // CSV HEADER: one leading CopyData with the column-name row.
        let head = if let CopyOutFormat::Csv { header: true } = format {
            let mut line = pg_fields
                .iter()
                .map(|f| f.name().replace('"', "\"\""))
                .collect::<Vec<_>>()
                .join(",");
            line.push('\n');
            Some(Ok(CopyData::new(bytes_from(line.into_bytes()))))
        } else {
            None
        };

        // Row stream: encode each batch as it arrives; any encode/scan error
        // surfaces as CopyFail via pgwire's send_copy_out_response.
        let fields = pg_fields.clone();
        let emitted = Arc::new(AtomicBool::new(false));
        let emitted_rows = emitted.clone();
        let rows = stream
            .map(move |batch_res| -> Vec<PgWireResult<CopyData>> {
                let batch = match batch_res {
                    Ok(batch) => batch,
                    Err(e) => return vec![Err(PgWireError::ApiError(Box::new(e)))],
                };
                let schema = batch.schema();
                let mut out = Vec::with_capacity(batch.num_rows());
                for row in 0..batch.num_rows() {
                    for (i, arr) in batch.columns().iter().enumerate() {
                        if let Err(e) =
                            encode_value(&mut encoder, arr, row, schema.field(i), &fields[i])
                        {
                            out.push(Err(e));
                            return out;
                        }
                    }
                    emitted_rows.store(true, Ordering::Relaxed);
                    out.push(Ok(ArrowPgEncoder::take_row(&mut encoder)));
                }
                out
            })
            .flat_map(futures::stream::iter);
        // Zero-row binary COPY still needs the PGCOPY header (take_copy only
        // writes it with the first row); the trailer is appended by
        // CopyResponse::new. Evaluated lazily AFTER the row stream finishes.
        let tail = futures::stream::iter(std::iter::once(())).filter_map(move |()| {
            let need_header = format == CopyOutFormat::Binary && !emitted.load(Ordering::Relaxed);
            futures::future::ready(need_header.then(|| {
                let mut header = Vec::with_capacity(19);
                header.extend_from_slice(b"PGCOPY\n\xFF\r\n\x00");
                header.extend_from_slice(&[0u8; 8]); // flags + extension len
                Ok(CopyData::new(bytes_from(header)))
            }))
        });
        let data_stream = futures::stream::iter(head).chain(rows).chain(tail);

        let format_code: i8 = if format == CopyOutFormat::Binary {
            1
        } else {
            0
        };
        Ok(Response::CopyOut(CopyResponse::new(
            format_code,
            ncols,
            data_stream,
        )))
    }
}

/// `bytes::Bytes` via prost's re-export (same crate version pgwire links).
fn bytes_from(v: Vec<u8>) -> prost::bytes::Bytes {
    prost::bytes::Bytes::from(v)
}

#[async_trait]
impl QueryHook for CopyOutHook {
    async fn handle_simple_query(
        &self,
        statement: &SqlStatement,
        session_context: &SessionContext,
        _client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        let (sql, format) = match translate_copy(statement) {
            Ok(Some(parsed)) => parsed,
            Ok(None) => return None,
            Err(e) => return Some(Err(e)),
        };
        Some(self.run(&sql, format, session_context).await)
    }

    async fn handle_extended_parse_query(
        &self,
        sql: &SqlStatement,
        _session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        match translate_copy(sql) {
            Ok(Some(_)) => {
                // Placeholder plan: DataFusion cannot plan COPY TO STDOUT;
                // execution happens in handle_extended_query. Describe on the
                // portal reports no columns — the CopyOutResponse carries the
                // real column count/formats, which is what libpq consumes.
                Some(Ok(LogicalPlan::EmptyRelation(EmptyRelation {
                    produce_one_row: false,
                    schema: Arc::new(DFSchema::empty()),
                })))
            }
            Ok(None) => None,
            Err(e) => Some(Err(e)),
        }
    }

    async fn handle_extended_query(
        &self,
        statement: &SqlStatement,
        _logical_plan: &LogicalPlan,
        params: &ParamValues,
        session_context: &SessionContext,
        _client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        let (sql, format) = match translate_copy(statement) {
            Ok(Some(parsed)) => parsed,
            Ok(None) => return None,
            Err(e) => return Some(Err(e)),
        };
        let has_params = match params {
            ParamValues::List(l) => !l.is_empty(),
            ParamValues::Map(m) => !m.is_empty(),
        };
        if has_params {
            return Some(Err(copy_reject(
                "parameterized COPY ($n bind values) is not supported; inline the values"
                    .to_string(),
            )));
        }
        Some(self.run(&sql, format, session_context).await)
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
