//! icegres — a Postgres wire endpoint over an Iceberg lakehouse.
//!
//! Connects to an Iceberg REST catalog (Lakekeeper), exposes its namespaces
//! and tables through DataFusion, and serves them over the Postgres wire
//! protocol via datafusion-postgres.

mod cache;
mod context;
mod ops;
mod pgauth;
mod scan;
mod seed;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{bail, Context as _, Result};
use clap::{Args, Parser, Subcommand};
use datafusion_postgres::auth::AuthManager;
use datafusion_postgres::datafusion_pg_catalog::pg_catalog::setup_pg_catalog;
use datafusion_postgres::{serve, ServerOptions};
use tracing::{info, warn};

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

/// Subcommands. Note there is deliberately NO `compact` subcommand: the
/// pinned iceberg-rust 0.9.1 `Transaction` API has no replace-files/rewrite
/// action (only `fast_append` + metadata updates), so small files cannot be
/// rewritten safely; see the module docs in `seed.rs` for the full rationale
/// and the drop-and-reseed alternative.
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
        } => {
            let serve_opts = ServeOpts {
                idle_shutdown_secs,
                health_port,
                tls_cert,
                tls_key,
                auth_file,
            };
            run_serve(&catalog, &host, port, serve_opts).await
        }
        Command::Seed { catalog } => seed::run(&catalog).await,
        Command::Sql { catalog, query } => run_sql(&catalog, &query).await,
    }
}

/// Server-only options for `icegres serve` (kept separate from `CatalogOpts`).
struct ServeOpts {
    idle_shutdown_secs: Option<u64>,
    health_port: Option<u16>,
    tls_cert: Option<String>,
    tls_key: Option<String>,
    auth_file: Option<PathBuf>,
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
    let ctx = context::build_session_context(catalog).await?;

    setup_pg_catalog(
        &ctx,
        context::CATALOG_NAME,
        Arc::new(AuthManager::default()),
    )
    .map_err(|e| anyhow::anyhow!("failed to set up pg_catalog emulation: {e}"))?;

    if let Some(hp) = serve_opts.health_port {
        ops::spawn_health_listener(host, hp).await?;
    }

    info!(listen_addr = %format!("{host}:{port}"), "starting pgwire server");
    if serve_opts.idle_shutdown_secs.is_some() || tls.is_some() || auth.is_some() {
        // Custom accept loop (ops.rs): idle shutdown, TLS, and/or SCRAM auth.
        ops::serve_custom(
            Arc::new(ctx),
            host,
            port,
            serve_opts.idle_shutdown_secs,
            tls,
            auth,
        )
        .await
        .context("pgwire server (custom accept loop) failed")?;
    } else {
        // Default path: the stock datafusion-postgres loop, byte-for-byte
        // unchanged behavior.
        let server_options = ServerOptions::new()
            .with_host(host.to_string())
            .with_port(port);
        serve(Arc::new(ctx), &server_options)
            .await
            .context("pgwire server failed")?;
    }
    Ok(())
}

async fn run_sql(opts: &CatalogOpts, query: &str) -> Result<()> {
    let catalog = context::connect_catalog(opts).await?;
    let ctx = context::build_session_context(catalog).await?;
    let df = ctx
        .sql(query)
        .await
        .with_context(|| format!("failed to plan query: {query}"))?;
    df.show().await.context("failed to execute query")?;
    Ok(())
}
