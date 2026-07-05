//! icegres — a Postgres wire endpoint over an Iceberg lakehouse.
//!
//! Connects to an Iceberg REST catalog (Lakekeeper), exposes its namespaces
//! and tables through DataFusion, and serves them over the Postgres wire
//! protocol via datafusion-postgres.

mod cache;
mod context;
mod dml;
mod ops;
mod overwrite;
mod pgauth;
mod scan;
mod seed;
mod txn;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, Context as _, Result};
use clap::{Args, Parser, Subcommand};
use datafusion_postgres::auth::AuthManager;
use datafusion_postgres::datafusion_pg_catalog::pg_catalog::setup_pg_catalog;
use datafusion_postgres::hooks::set_show::SetShowHook;
use datafusion_postgres::QueryHook;
use iceberg::Catalog;
use tracing::{info, warn};

use crate::overwrite::OverwriteEngine;
use crate::txn::{TxnHook, TxnRegistry};

/// Connection options for the Iceberg REST catalog and its object store.
#[derive(Args, Clone, Debug)]
pub struct CatalogOpts {
    /// Iceberg REST catalog base URI (Lakekeeper serves it under /catalog).
    #[arg(
        long,
        env = "ICEGRES_CATALOG_URI",
        default_value = "http://127.0.0.1:8181/catalog"
    )]
    pub catalog_uri: String,

    /// Warehouse name registered in the REST catalog.
    #[arg(long, env = "ICEGRES_WAREHOUSE", default_value = "lakehouse")]
    pub warehouse: String,

    /// S3-compatible endpoint holding the table data (RustFS).
    #[arg(
        long,
        env = "ICEGRES_S3_ENDPOINT",
        default_value = "http://127.0.0.1:9000"
    )]
    pub s3_endpoint: String,

    /// S3 access key id.
    #[arg(long, env = "ICEGRES_S3_ACCESS_KEY", default_value = "rustfsadmin")]
    pub s3_access_key: String,

    /// S3 secret access key.
    #[arg(long, env = "ICEGRES_S3_SECRET_KEY", default_value = "rustfssecret")]
    pub s3_secret_key: String,

    /// S3 region.
    #[arg(long, env = "ICEGRES_S3_REGION", default_value = "us-east-1")]
    pub s3_region: String,
}

#[derive(Parser)]
#[command(
    name = "icegres",
    version,
    about = "Postgres wire endpoint over an Iceberg lakehouse"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

/// Subcommands. Note there is deliberately NO `compact` subcommand yet: the
/// pinned iceberg-rust 0.9.1 `Transaction` API has no replace-files/rewrite
/// action (only `fast_append` + metadata updates). The copy-on-write DML
/// machinery in `overwrite.rs` could carry a compaction (`replace`
/// operation) in the future; until then drop-and-reseed is the documented
/// canonicalization path (see the module docs in `seed.rs`).
#[derive(Subcommand)]
enum Command {
    /// Serve the lakehouse over the Postgres wire protocol.
    Serve {
        #[command(flatten)]
        catalog: CatalogOpts,

        /// Address to bind the pgwire listener on.
        #[arg(long, env = "ICEGRES_HOST", default_value = "0.0.0.0")]
        host: String,

        /// Port to bind the pgwire listener on.
        #[arg(long, env = "ICEGRES_PORT", default_value_t = 5439)]
        port: u16,

        /// Scale-to-zero: exit cleanly (code 0) after this many consecutive
        /// seconds with no client connections (the countdown also starts at
        /// boot). Run under a restarting/socket-activating supervisor to get
        /// scale-from-zero; see the module docs in ops.rs. Off by default.
        #[arg(long, env = "ICEGRES_IDLE_SHUTDOWN_SECS")]
        idle_shutdown_secs: Option<u64>,

        /// Serve a minimal HTTP liveness endpoint ('HTTP/1.1 200 OK', body
        /// 'ok') on this port; plain TCP connect checks work too. Liveness
        /// only — readiness is a pgwire 'select 1'. Off by default.
        #[arg(long, env = "ICEGRES_HEALTH_PORT")]
        health_port: Option<u16>,

        /// PEM certificate (chain) enabling TLS on the pgwire listener.
        /// Requires --tls-key. Any TLS setup error aborts startup (no silent
        /// plaintext fallback). Like stock Postgres, plaintext startup is
        /// still accepted — clients opt in with sslmode=require/verify-full.
        /// Dev certs: infra/scripts/gen-dev-cert.sh.
        #[arg(long, env = "ICEGRES_TLS_CERT", requires = "tls_key")]
        tls_cert: Option<String>,

        /// PEM private key (PKCS#8/RSA/SEC1) for --tls-cert.
        #[arg(long, env = "ICEGRES_TLS_KEY", requires = "tls_cert")]
        tls_key: Option<String>,

        /// Require SCRAM-SHA-256 authentication against this credentials
        /// file ('user:password' per line, '#' comments; protect it like
        /// .pgpass). Wrong password or unknown user is rejected (28P01).
        /// Without this flag the server stays permissive and logs a WARN.
        #[arg(long, env = "ICEGRES_AUTH_FILE")]
        auth_file: Option<PathBuf>,

        /// Enforce per-table primary keys (SPEC B5): tables declaring the
        /// 'icegres.primary-key' property get NOT NULL + uniqueness checks
        /// on INSERT and PK-assigning UPDATE (sqlstates 23502/23505),
        /// validated against the very snapshot each commit anchors to. Off
        /// by default: enforcement reads the key columns of every live data
        /// file per write.
        #[arg(
            long,
            env = "ICEGRES_ENFORCE_PK",
            num_args = 0..=1,
            default_missing_value = "true",
            default_value = "false",
            value_parser = clap::builder::BoolishValueParser::new()
        )]
        enforce_pk: bool,
    },
    /// Create and populate the demo namespace/tables (idempotent).
    Seed {
        #[command(flatten)]
        catalog: CatalogOpts,
    },
    /// Execute a single SQL statement locally (no server) and print results.
    Sql {
        #[command(flatten)]
        catalog: CatalogOpts,

        /// The SQL statement to execute.
        #[arg(short = 'e', long = "execute")]
        query: String,

        /// Enforce per-table primary keys for this statement (same semantics
        /// as `icegres serve --enforce-pk`).
        #[arg(
            long,
            env = "ICEGRES_ENFORCE_PK",
            num_args = 0..=1,
            default_missing_value = "true",
            default_value = "false",
            value_parser = clap::builder::BoolishValueParser::new()
        )]
        enforce_pk: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();
    match cli.command {
        Command::Serve {
            catalog,
            host,
            port,
            idle_shutdown_secs,
            health_port,
            tls_cert,
            tls_key,
            auth_file,
            enforce_pk,
        } => {
            let serve_opts = ServeOpts {
                idle_shutdown_secs,
                health_port,
                tls_cert,
                tls_key,
                auth_file,
                enforce_pk,
            };
            run_serve(&catalog, &host, port, serve_opts).await
        }
        Command::Seed { catalog } => seed::run(&catalog).await,
        Command::Sql {
            catalog,
            query,
            enforce_pk,
        } => run_sql(&catalog, &query, enforce_pk).await,
    }
}

/// Server-only options for `icegres serve` (kept separate from `CatalogOpts`).
struct ServeOpts {
    idle_shutdown_secs: Option<u64>,
    health_port: Option<u16>,
    tls_cert: Option<String>,
    tls_key: Option<String>,
    auth_file: Option<PathBuf>,
    enforce_pk: bool,
}

async fn run_serve(opts: &CatalogOpts, host: &str, port: u16, serve_opts: ServeOpts) -> Result<()> {
    // Fail fast on TLS/auth misconfiguration BEFORE touching the catalog:
    // a server asked to be secure must never come up insecure.
    let tls = match (&serve_opts.tls_cert, &serve_opts.tls_key) {
        (Some(cert), Some(key)) => {
            let acceptor = ops::build_tls_acceptor(cert, key)?;
            info!(cert = %cert, key = %key, "TLS enabled on the pgwire listener (plaintext startup still accepted; clients enforce with sslmode=require)");
            Some(acceptor)
        }
        (None, None) => None,
        // clap `requires` already enforces this; keep a hard error for
        // programmatic callers.
        _ => bail!("--tls-cert and --tls-key must be provided together"),
    };
    let auth = match &serve_opts.auth_file {
        Some(path) => {
            let source = Arc::new(pgauth::FileAuthSource::load(path)?);
            info!(
                auth_file = %path.display(),
                users = source.user_count(),
                "SCRAM-SHA-256 authentication enabled"
            );
            Some(source)
        }
        None => {
            warn!(
                "authentication is DISABLED — any user/password is accepted; \
                 pass --auth-file (env ICEGRES_AUTH_FILE) to require SCRAM-SHA-256 credentials"
            );
            None
        }
    };

    info!(
        catalog_uri = %opts.catalog_uri,
        warehouse = %opts.warehouse,
        s3_endpoint = %opts.s3_endpoint,
        "connecting to Iceberg REST catalog"
    );
    let catalog = context::connect_catalog(opts).await?;
    let ctx = context::build_session_context(catalog.clone()).await?;

    setup_pg_catalog(
        &ctx,
        context::CATALOG_NAME,
        Arc::new(AuthManager::default()),
    )
    .map_err(|e| anyhow::anyhow!("failed to set up pg_catalog emulation: {e}"))?;

    if let Some(hp) = serve_opts.health_port {
        ops::spawn_health_listener(host, hp).await?;
    }

    if serve_opts.enforce_pk {
        info!(
            "PK enforcement is ON (--enforce-pk): tables with the '{}' property get \
             NOT NULL + uniqueness checks on writes",
            overwrite::PK_PROPERTY
        );
    }
    let engine =
        Arc::new(OverwriteEngine::connect(catalog.clone(), opts, serve_opts.enforce_pk).await?);
    let txn_registry = Arc::new(TxnRegistry::new());
    let hooks = query_hooks(engine, txn_registry.clone(), catalog);

    info!(listen_addr = %format!("{host}:{port}"), "starting pgwire server");
    // Always the icegres accept loop (ops.rs): it is byte-for-byte the
    // upstream loop when no TLS/auth/idle-shutdown is configured, PLUS the
    // per-connection cleanup that drops an open transaction when its socket
    // closes (disconnect = implicit ROLLBACK; without this, abandoned
    // transaction buffers would leak).
    ops::serve_custom(
        Arc::new(ctx),
        host,
        port,
        serve_opts.idle_shutdown_secs,
        tls,
        auth,
        hooks,
        txn_registry,
    )
    .await
    .context("pgwire server failed")?;
    Ok(())
}

/// The icegres query-hook chain, in order:
/// 1. [`TxnHook`] — BEGIN/COMMIT/ROLLBACK and, while a transaction is open,
///    EVERY statement on that connection (buffered writes, pinned reads);
///    also PK-enforced autocommit INSERT. Replaces the upstream
///    `TransactionStatementHook`, whose BEGIN/COMMIT were accepted but
///    non-transactional.
/// 2. `SetShowHook` — upstream SET/SHOW handling.
/// 3. [`dml::DmlHook`] — autocommit copy-on-write UPDATE/DELETE.
fn query_hooks(
    engine: Arc<OverwriteEngine>,
    registry: Arc<TxnRegistry>,
    catalog: Arc<dyn Catalog>,
) -> Vec<Arc<dyn QueryHook>> {
    vec![
        Arc::new(TxnHook::new(registry, engine.clone(), catalog)),
        Arc::new(SetShowHook),
        Arc::new(dml::DmlHook::new(engine)),
    ]
}

async fn run_sql(opts: &CatalogOpts, query: &str, enforce_pk: bool) -> Result<()> {
    let catalog = context::connect_catalog(opts).await?;
    // UPDATE/DELETE take the same copy-on-write path as the server's wire
    // handler; everything else goes through DataFusion unchanged.
    if let Some(dml_stmt) = dml::parse_single_dml(query)? {
        let engine = OverwriteEngine::connect(catalog, opts, enforce_pk).await?;
        let outcome = engine.execute(&dml_stmt.0).await?;
        println!("{} {}", dml_stmt.1, outcome.rows);
        return Ok(());
    }
    let ctx = context::build_session_context(catalog).await?;
    let df = ctx
        .sql(query)
        .await
        .with_context(|| format!("failed to plan query: {query}"))?;
    df.show().await.context("failed to execute query")?;
    Ok(())
}
