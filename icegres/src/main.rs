//! icegres — a Postgres wire endpoint over an Iceberg lakehouse.
//!
//! Connects to an Iceberg REST catalog (Lakekeeper), exposes its namespaces
//! and tables through DataFusion, and serves them over the Postgres wire
//! protocol via datafusion-postgres.

mod cache;
mod context;
mod ops;
mod scan;
mod seed;

use std::sync::Arc;

use anyhow::{Context as _, Result};
use clap::{Args, Parser, Subcommand};
use datafusion_postgres::auth::AuthManager;
use datafusion_postgres::datafusion_pg_catalog::pg_catalog::setup_pg_catalog;
use datafusion_postgres::{serve, ServerOptions};
use tracing::info;

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
        } => run_serve(&catalog, &host, port, idle_shutdown_secs, health_port).await,
        Command::Seed { catalog } => seed::run(&catalog).await,
        Command::Sql { catalog, query } => run_sql(&catalog, &query).await,
    }
}

async fn run_serve(
    opts: &CatalogOpts,
    host: &str,
    port: u16,
    idle_shutdown_secs: Option<u64>,
    health_port: Option<u16>,
) -> Result<()> {
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

    if let Some(hp) = health_port {
        ops::spawn_health_listener(host, hp).await?;
    }

    info!(listen_addr = %format!("{host}:{port}"), "starting pgwire server");
    match idle_shutdown_secs {
        // Scale-to-zero path: our own accept loop with an idle watchdog
        // (see ops.rs). Exits cleanly after the idle window.
        Some(idle_secs) => ops::serve_with_idle_shutdown(Arc::new(ctx), host, port, idle_secs)
            .await
            .context("pgwire server (idle-shutdown mode) failed")?,
        // Default path: the stock datafusion-postgres loop, byte-for-byte
        // unchanged behavior.
        None => {
            let server_options = ServerOptions::new()
                .with_host(host.to_string())
                .with_port(port);
            serve(Arc::new(ctx), &server_options)
                .await
                .context("pgwire server failed")?;
        }
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
