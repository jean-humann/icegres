//! icegres — a Postgres wire endpoint over an Iceberg lakehouse.
//!
//! Connects to an Iceberg REST catalog (Lakekeeper), exposes its namespaces
//! and tables through DataFusion, and serves them over the Postgres wire
//! protocol via datafusion-postgres.

mod authz;
mod branch;
mod buffer;
mod cache;
mod compat;
mod context;
mod dml;
mod flight;
mod freshness;
mod keyed;
mod maintain;
mod metrics;
mod ops;
mod overwrite;
mod peer;
/// SCRAM authentication backend — the managed add-on (behind the `managed`
/// feature). The open-source build carries no auth backend.
#[cfg(feature = "managed")]
mod pgauth;
mod plancache;
/// Consensus-class durable tail (`--tail-quorum`): the proposer/acceptor
/// protocol adapted from Neon's safekeeper (see the module docs and NOTICE).
mod quorum;
mod scan;
mod seed;
/// Shared low-level segment/frame machinery (factored from `tail.rs`; also
/// compiled into the `icekeeperd` binary).
mod segment;
mod tail;
mod tail_pg;
mod tail_quorum;
/// Open tail read API (TailSnapshot/TailSubscribe over Arrow Flight) —
/// roadmap-v2 P1; see docs/open-tail-protocol.md.
mod tailapi;
mod timing;
mod traced;
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
    version = env!("ICEGRES_LONG_VERSION"),
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

        /// Enforce Lakekeeper-style authorization (ReBAC) from this policy
        /// file. Grants of read/write/drop/own on warehouse/namespace/table
        /// entities (inherited down the hierarchy) gate every SQL statement;
        /// a denied statement returns SQLSTATE 42501. Pair with --auth-file so
        /// principals are authenticated, not client-asserted. Without this
        /// flag authorization is OPEN (any authenticated user, all tables).
        #[arg(long, env = "ICEGRES_AUTHZ_FILE")]
        authz_file: Option<PathBuf>,

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

        /// Serve a zero-copy BRANCH of the lakehouse (Neon's branch-per-
        /// endpoint model, SPEC D6): all reads pin to the head of this
        /// Iceberg snapshot ref and all writes (INSERT/UPDATE/DELETE/
        /// transactions) commit to it with assert-ref-snapshot-id on the
        /// branch — never touching `main` or any other branch. The branch
        /// must already exist on each table you touch (`icegres branch
        /// create <table> <name>`); reading a table without the ref fails
        /// loudly. Default: main.
        #[arg(long, env = "ICEGRES_BRANCH", default_value = "main")]
        branch: String,

        /// Bounded-staleness reads: with N > 0, scans serve the cached
        /// table snapshot with NO per-scan catalog round trip (reclaiming
        /// the ~2-3 ms freshness check that dominates read latency) while
        /// ONE background task polls the catalog every N ms (tables
        /// refreshed concurrently, up to 8 in flight) and swaps the cached
        /// snapshot on change. TRADE-OFF: commits from OTHER writers become
        /// visible within ~N ms plus one refresh round trip instead of
        /// immediately; a slow table delays only ITSELF — its refresh is
        /// retry-free, bounded by a per-table timeout of min(4*N, 2000) ms,
        /// and retried on the next pass, never holding up other tables'
        /// freshness. That is why the default is 0 (exact freshness,
        /// semantics unchanged) and enabling it logs a WARN. THIS server's
        /// own writes stay read-your-own-writes exact (synchronous
        /// invalidation on every local write path). During a catalog outage
        /// reads keep serving the last refreshed snapshot (set
        /// ICEGRES_STALE_READ_ON_CATALOG_ERROR=0 to fail loudly instead);
        /// worst-case age is the icegres_freshness_age_ms gauge on /metrics
        /// (sampled at refresher pass start, so a healthy value reads ~N).
        /// Also enables the physical-plan cache for repeated statements
        /// (see icegres/src/freshness.rs and icegres/src/plancache.rs).
        #[arg(long, env = "ICEGRES_FRESHNESS_MS", default_value_t = 0)]
        freshness_ms: u64,

        /// Moonlink-style buffered writes: with N > 0, INSERTs acknowledge
        /// after appending to an in-memory buffer and a background task
        /// group-commits it to Iceberg every N ms (or at the row threshold,
        /// env ICEGRES_WRITE_BUFFER_MAX_ROWS). Reads on THIS server union
        /// the buffer with the committed table, so read-your-writes holds
        /// locally and same-server cross-connection freshness is instant;
        /// OTHER servers/readers see the rows at the commit cadence (<= N
        /// ms after ack). TRADE-OFF: an unclean kill loses up to N ms of
        /// acked-but-uncommitted writes (--tail-dir closes that window) —
        /// that is why the default is 0 (fully synchronous, semantics
        /// unchanged) and enabling it logs a WARN. See icegres/src/buffer.rs
        /// for the full semantics.
        #[arg(long, env = "ICEGRES_WRITE_BUFFER_MS", default_value_t = 0)]
        write_buffer_ms: u64,

        /// Durable local tail for buffered writes (requires
        /// --write-buffer-ms > 0): every buffered INSERT is appended to an
        /// fsync'd per-table WAL under this directory BEFORE its ack, and
        /// acked-but-uncommitted rows are replayed into the buffer on the
        /// next boot with the same directory — an unclean kill (SIGKILL,
        /// power loss) of the process loses NOTHING. Honest scope: the tail
        /// is THIS node's disk, so losing the node or the disk still loses
        /// un-flushed acked rows (see icegres/src/tail.rs). Off by default.
        #[arg(long, env = "ICEGRES_TAIL_DIR")]
        tail_dir: Option<PathBuf>,

        /// Durable Postgres-backed tail for buffered writes (requires
        /// --write-buffer-ms > 0; mutually exclusive with --tail-dir):
        /// every buffered INSERT is committed to a frames table in this
        /// Postgres database (schema `icegres_tail`, auto-created) BEFORE
        /// its ack, and acked-but-uncommitted rows are replayed into the
        /// buffer on the next boot with the same URL. Unlike --tail-dir,
        /// the tail SURVIVES LOSING THIS NODE: durability = the tail
        /// database's own fsync/replication (the natural target is a
        /// dedicated database on the instance already backing Lakekeeper).
        /// A tail-database outage blocks buffered writes (statement
        /// errors — backpressure, never silent loss). One server process
        /// per tail (session advisory lock); TLS URLs are not yet
        /// supported. See icegres/src/tail_pg.rs. Off by default.
        #[arg(long, env = "ICEGRES_TAIL_URL", conflicts_with = "tail_dir")]
        tail_url: Option<String>,

        /// Quorum-replicated durable tail for buffered writes (requires
        /// --write-buffer-ms > 0; mutually exclusive with --tail-dir /
        /// --tail-url): exactly three comma-separated `host:port` addresses
        /// of `icekeeperd` acceptors. Every buffered INSERT's record is
        /// fsynced by 2 of the 3 acceptors BEFORE its ack (Neon
        /// SafeKeeper's consensus, adapted — see NOTICE), so acked rows
        /// survive an unclean kill, losing this NODE, or losing ANY SINGLE
        /// acceptor. Two live acceptors = writes proceed; one live =
        /// statement errors (backpressure, never silent loss). A competing
        /// icegres on the same quorum fences this one (its INSERTs fail
        /// with "superseded by a newer server"). The quorum-ack timeout is
        /// tunable via ICEGRES_TAIL_QUORUM_TIMEOUT_MS (default 10000, min
        /// 1000): a stalled append first attempts one internal re-election,
        /// then poisons the tail on a second timeout. Trusted network only
        /// (no TLS/auth between proposer and acceptors yet). See
        /// icegres/src/tail_quorum.rs. Off by default.
        #[arg(
            long,
            env = "ICEGRES_TAIL_QUORUM",
            conflicts_with_all = ["tail_dir", "tail_url"]
        )]
        tail_quorum: Option<String>,

        /// Open tail read API (roadmap-v2 P1, docs/open-tail-protocol.md):
        /// serve TailSnapshot/TailSubscribe over Arrow Flight on this port,
        /// exposing the buffer's un-flushed rows (with their tail sequences
        /// and the watermark-property exclusion rule) to peer icegres
        /// computes (--peer-tail) and ANY external Arrow Flight client.
        /// Read-only; requires --write-buffer-ms > 0 AND a durable tail
        /// (--tail-dir/--tail-url/--tail-quorum). Auth rides --auth-file
        /// (Flight basic-auth handshake). Off by default (no listener).
        #[arg(long, env = "ICEGRES_TAIL_API_PORT")]
        tail_api_port: Option<u16>,

        /// Fleet overlays (roadmap-v2 P1): comma-separated `host:port` tail
        /// APIs of buffering PEER computes to mirror. Scans on this server
        /// union each peer's un-flushed rows with committed data under the
        /// same exactly-once watermark rule the local buffer uses, so this
        /// reader sees a peer's acked writes within the event bound instead
        /// of the commit cadence. Best-effort: if a peer dies or goes
        /// silent past the serving bound, its mirror drops out of reads and
        /// they fall back to commit-cadence freshness (WARN once; per-peer
        /// gauge icegres_peer_tail_age_ms). Read-side only — the
        /// single-buffering-writer-per-table model is unchanged. Peers
        /// secured with --auth-file need ICEGRES_PEER_TAIL_USER /
        /// ICEGRES_PEER_TAIL_PASSWORD (one identity for every peer): the
        /// subscriber runs the Flight basic-auth handshake per connection.
        /// Off by default.
        #[arg(long, env = "ICEGRES_PEER_TAILS", value_delimiter = ',')]
        peer_tail: Vec<String>,

        /// Acknowledge running an UNAUTHENTICATED listener on a non-loopback
        /// interface. Without this, binding a public address (e.g. 0.0.0.0)
        /// while `--auth-file` is unset is refused at startup (secure by
        /// default). Loopback binds and authenticated servers are unaffected.
        #[arg(long, env = "ICEGRES_INSECURE", num_args = 0..=1,
              default_missing_value = "true", default_value = "false",
              value_parser = clap::builder::BoolishValueParser::new())]
        insecure: bool,
    },
    /// Serve the lakehouse over Arrow Flight SQL (gRPC) — the ADBC
    /// first-class endpoint (SPEC A11). Same engine wiring as `serve`
    /// (snapshot-aware caches, copy-on-write DML engine), Arrow end to end:
    /// queries, catalog metadata (GetObjects), prepared statements,
    /// INSERT/UPDATE/DELETE, and bulk ingest (one Iceberg commit per
    /// `adbc_ingest` stream). See icegres/src/flight.rs for the surface.
    FlightServe {
        #[command(flatten)]
        catalog: CatalogOpts,

        /// Address to bind the gRPC listener on.
        #[arg(long, env = "ICEGRES_FLIGHT_HOST", default_value = "0.0.0.0")]
        host: String,

        /// Port to bind the gRPC listener on.
        #[arg(long, env = "ICEGRES_FLIGHT_PORT", default_value_t = 50051)]
        port: u16,

        /// Require the Flight SQL basic-auth handshake against this
        /// credentials file (same 'user:password' format and env var as
        /// `icegres serve --auth-file`). Basic auth sends the password
        /// itself — terminate TLS in front of this listener (grpc+tls) or
        /// keep it on a trusted network. Without this flag the endpoint is
        /// permissive and logs a WARN.
        #[arg(long, env = "ICEGRES_AUTH_FILE")]
        auth_file: Option<PathBuf>,

        /// Enforce Lakekeeper-style ReBAC authorization on the Flight SQL
        /// endpoint from this policy file (managed add-on; same format and
        /// semantics as `icegres serve --authz-file`). Every data RPC is gated
        /// by the same policy the pgwire path enforces. Requires --auth-file
        /// (an unauthenticated endpoint has no principal to authorize).
        #[arg(long, env = "ICEGRES_AUTHZ_FILE")]
        authz_file: Option<PathBuf>,

        /// PEM certificate (chain) enabling in-process TLS on the Flight SQL
        /// listener (requires --tls-key). Terminates TLS with the same rustls
        /// stack as pgwire, so basic-auth credentials are no longer sent in
        /// cleartext without a front proxy. Any TLS setup error aborts startup.
        #[arg(long, env = "ICEGRES_FLIGHT_TLS_CERT", requires = "tls_key")]
        tls_cert: Option<String>,

        /// PEM private key (PKCS#8/RSA/SEC1) for --tls-cert.
        #[arg(long, env = "ICEGRES_FLIGHT_TLS_KEY", requires = "tls_cert")]
        tls_key: Option<String>,

        /// Bounded-staleness reads on the Flight SQL endpoint — same
        /// semantics, trade-offs, and default (0 = exact freshness,
        /// byte-identical) as `icegres serve --freshness-ms`. With N > 0,
        /// scans serve the cached snapshot with NO per-scan catalog round
        /// trip and the physical-plan cache activates for repeated
        /// statements, which is what takes the small-query Flight p50 from
        /// ~half the historical latency (plan-once tickets alone) down to
        /// single-digit milliseconds.
        #[arg(long, env = "ICEGRES_FRESHNESS_MS", default_value_t = 0)]
        freshness_ms: u64,

        /// Acknowledge running an UNAUTHENTICATED Flight listener on a
        /// non-loopback interface (see `icegres serve --insecure`).
        #[arg(long, env = "ICEGRES_INSECURE", num_args = 0..=1,
              default_missing_value = "true", default_value = "false",
              value_parser = clap::builder::BoolishValueParser::new())]
        insecure: bool,
    },
    /// Create and populate the demo namespace/tables (idempotent).
    Seed {
        #[command(flatten)]
        catalog: CatalogOpts,
    },
    /// Manage zero-copy branches (named Iceberg snapshot refs, SPEC D6).
    Branch {
        #[command(subcommand)]
        cmd: BranchCmd,
    },
    /// Table lifecycle maintenance (snapshot expiry). No `compact` yet — see
    /// the note above `Command`.
    Maintain {
        #[command(subcommand)]
        cmd: MaintainCmd,
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

/// `icegres branch` subcommands. A branch is a named snapshot ref in table
/// metadata: creating/dropping one is a pure metadata commit — zero data
/// copied. See icegres/src/branch.rs for the full model.
#[derive(Subcommand)]
enum BranchCmd {
    /// Create branch <name> on <table> (zero-copy fork of main's head, or
    /// of --at-snapshot).
    Create {
        #[command(flatten)]
        catalog: CatalogOpts,
        /// Target table: <table> or <namespace>.<table>.
        table: String,
        /// Branch name to create (must not exist yet).
        name: String,
        /// Fork from this snapshot id instead of the current main head.
        #[arg(long)]
        at_snapshot: Option<i64>,
    },
    /// List all snapshot refs (branches/tags) of <table>.
    List {
        #[command(flatten)]
        catalog: CatalogOpts,
        /// Target table: <table> or <namespace>.<table>.
        table: String,
    },
    /// Drop branch <name> from <table> (removes only the ref; snapshots
    /// stay time-travel-readable until expiry).
    Drop {
        #[command(flatten)]
        catalog: CatalogOpts,
        /// Target table: <table> or <namespace>.<table>.
        table: String,
        /// Branch name to drop (`main` is refused).
        name: String,
    },
    /// Create branch <name> on EVERY table in the catalog as ONE atomic
    /// multi-table transaction — a consistent-or-nothing whole-lakehouse
    /// cut: each table's request pins main to the head captured at load, so
    /// a concurrent commit (or an already-existing branch) fails the whole
    /// command with nothing applied; retry it. Requires a catalog
    /// implementing transactions/commit, e.g. Lakekeeper.
    CreateAll {
        #[command(flatten)]
        catalog: CatalogOpts,
        /// Branch name to create on every table (must not exist anywhere).
        name: String,
    },
    /// Drop branch <name> from every table that has it as ONE atomic
    /// multi-table transaction (`main` is refused; tables without the ref
    /// are skipped; errors if no table has it).
    DropAll {
        #[command(flatten)]
        catalog: CatalogOpts,
        /// Branch name to drop everywhere (`main` is refused).
        name: String,
    },
}

/// `icegres maintain` subcommands.
#[derive(Subcommand)]
enum MaintainCmd {
    /// Expire old snapshots of <table>, keeping the newest --keep by commit
    /// time plus every snapshot still reachable from a branch/tag ref.
    /// Metadata-only and safe on a live endpoint (anchored commit).
    ExpireSnapshots {
        #[command(flatten)]
        catalog: CatalogOpts,
        /// Target table: <table> or <namespace>.<table>.
        table: String,
        /// Keep this many of the newest snapshots (referenced refs are always
        /// kept regardless of age).
        #[arg(long, default_value_t = 10)]
        keep: usize,
    },
    /// Orphan-file GC: list the table's storage prefix, subtract every file
    /// still reachable from ANY retained snapshot/ref (plus metadata JSONs
    /// and statistics files), and report — or with --execute, delete — the
    /// rest. Dry-run by default. Unknown-age or unrecognized objects are
    /// never deleted; an unreadable manifest — or a recorded file path
    /// outside the listed bucket, whose liveness cannot be verified —
    /// aborts the whole run.
    RemoveOrphans {
        #[command(flatten)]
        catalog: CatalogOpts,
        /// Target table: <table> or <namespace>.<table>.
        table: String,
        /// Only objects last modified more than this many hours ago are
        /// eligible — the grace window is THE guard for files written by
        /// commits (ours or a foreign writer's) still in flight. A fixed
        /// 15-minute clock-skew allowance is added on top of it. Values
        /// under 1 combined with --execute are refused unless
        /// --unsafe-grace is also passed.
        #[arg(long, default_value_t = 72)]
        older_than_hours: u64,
        /// Actually delete the orphans (default: dry run, nothing deleted).
        /// Before deleting, a tiny probe object is written under the
        /// table's metadata/ prefix and stat'ed to verify the object-store
        /// clock agrees with ours (aborts beyond the 15-minute allowance).
        #[arg(long)]
        execute: bool,
        /// Allow --execute with --older-than-hours < 1 and drop the
        /// 15-minute clock-skew allowance from the cutoff. ONLY for
        /// quiescent tables (e.g. tests): concurrent writers WILL lose
        /// in-flight files.
        #[arg(long)]
        unsafe_grace: bool,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    // Log format: ICEGRES_LOG_FORMAT=json emits structured JSON lines (for log
    // shippers/aggregators); anything else keeps the human-readable format.
    let env_filter = || {
        tracing_subscriber::EnvFilter::try_from_default_env()
            .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"))
    };
    if std::env::var("ICEGRES_LOG_FORMAT").as_deref() == Ok("json") {
        tracing_subscriber::fmt()
            .json()
            .with_env_filter(env_filter())
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(env_filter())
            .init();
    }

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
            authz_file,
            branch,
            freshness_ms,
            enforce_pk,
            write_buffer_ms,
            tail_dir,
            tail_url,
            tail_quorum,
            tail_api_port,
            peer_tail,
            insecure,
        } => {
            let serve_opts = ServeOpts {
                idle_shutdown_secs,
                health_port,
                tls_cert,
                tls_key,
                auth_file,
                authz_file,
                branch,
                freshness_ms,
                enforce_pk,
                write_buffer_ms,
                tail_dir,
                tail_url,
                tail_quorum,
                tail_api_port,
                peer_tail,
                insecure,
            };
            run_serve(&catalog, &host, port, serve_opts).await
        }
        Command::FlightServe {
            catalog,
            host,
            port,
            auth_file,
            authz_file,
            tls_cert,
            tls_key,
            freshness_ms,
            insecure,
        } => {
            // Flight authorization needs an authenticated principal: reject
            // --authz-file without --auth-file rather than trusting an
            // anonymous connection (a silent authz bypass otherwise).
            if authz_file.is_some() && auth_file.is_none() {
                bail!(
                    "--authz-file on flight-serve requires --auth-file: the Flight endpoint has \
                     no authenticated principal to authorize without it"
                );
            }
            enforce_secure_default(&host, auth_file.is_some(), insecure)?;
            let authorizer = build_authorizer(&authz_file, auth_file.is_some())?;
            // clap `requires` guarantees cert and key arrive together.
            let tls = tls_cert.zip(tls_key);
            flight::run(
                &catalog,
                &host,
                port,
                auth_file,
                authorizer,
                tls,
                freshness_ms,
            )
            .await
        }
        Command::Seed { catalog } => seed::run(&catalog).await,
        Command::Branch { cmd } => match cmd {
            BranchCmd::Create {
                catalog,
                table,
                name,
                at_snapshot,
            } => branch::create(&catalog, &table, &name, at_snapshot).await,
            BranchCmd::List { catalog, table } => branch::list(&catalog, &table).await,
            BranchCmd::Drop {
                catalog,
                table,
                name,
            } => branch::drop(&catalog, &table, &name).await,
            BranchCmd::CreateAll { catalog, name } => branch::create_all(&catalog, &name).await,
            BranchCmd::DropAll { catalog, name } => branch::drop_all(&catalog, &name).await,
        },
        Command::Maintain { cmd } => match cmd {
            MaintainCmd::ExpireSnapshots {
                catalog,
                table,
                keep,
            } => maintain::expire_snapshots(&catalog, &table, keep).await,
            MaintainCmd::RemoveOrphans {
                catalog,
                table,
                older_than_hours,
                execute,
                unsafe_grace,
            } => {
                maintain::remove_orphans(&catalog, &table, older_than_hours, execute, unsafe_grace)
                    .await
            }
        },
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
    authz_file: Option<PathBuf>,
    branch: String,
    freshness_ms: u64,
    enforce_pk: bool,
    write_buffer_ms: u64,
    tail_dir: Option<PathBuf>,
    tail_url: Option<String>,
    tail_quorum: Option<String>,
    tail_api_port: Option<u16>,
    peer_tail: Vec<String>,
    insecure: bool,
}

/// True for a loopback / localhost bind address — safe to run open (dev).
fn is_loopback_host(host: &str) -> bool {
    matches!(host, "127.0.0.1" | "::1" | "localhost")
        || host
            .parse::<std::net::IpAddr>()
            .is_ok_and(|ip| ip.is_loopback())
}

/// Secure-by-default guard: refuse to bind a NON-loopback interface with
/// authentication disabled unless the operator explicitly opts into insecure
/// exposure with `--insecure`. Prevents the "authenticate nobody, exposed on
/// 0.0.0.0" default footgun (production-readiness audit #14).
fn enforce_secure_default(host: &str, auth_present: bool, insecure: bool) -> Result<()> {
    if auth_present || insecure || is_loopback_host(host) {
        return Ok(());
    }
    bail!(
        "refusing to bind {host} (a non-loopback interface) with authentication DISABLED: \
         any client on the network could connect as any user. Pass --auth-file to require \
         credentials, bind 127.0.0.1 for local-only use, or pass --insecure (env \
         ICEGRES_INSECURE=1) to acknowledge an intentionally open, exposed listener."
    );
}

/// Build the ReBAC authorizer from `--authz-file` (managed add-on). `None` =
/// open (any authenticated principal, all tables). Shared by `serve` and
/// `flight-serve` so both wire protocols enforce the identical policy.
fn build_authorizer(
    authz_file: &Option<PathBuf>,
    auth_present: bool,
) -> Result<Option<authz::SharedAuthorizer>> {
    let Some(path) = authz_file else {
        return Ok(None);
    };
    #[cfg(feature = "managed")]
    {
        let a = Arc::new(authz::FileAuthorizer::load(path)?);
        if !auth_present {
            warn!(
                "authorization is ENABLED but authentication is NOT (--auth-file unset): \
                 principals are CLIENT-ASSERTED and spoofable. Pair --authz-file with \
                 --auth-file in production."
            );
        }
        info!(
            authz_file = %path.display(),
            grants = a.grant_count(),
            "ReBAC authorization enabled (managed add-on; warehouse->namespace->table \
             grants, SQLSTATE 42501 on deny)"
        );
        Ok(Some(a as authz::SharedAuthorizer))
    }
    #[cfg(not(feature = "managed"))]
    {
        let _ = (path, auth_present);
        bail!(
            "--authz-file is a managed add-on: this open-source build was compiled \
             without the `managed` feature. Rebuild with --features managed, or omit \
             --authz-file to run open (any authenticated user, all tables)."
        )
    }
}

async fn run_serve(opts: &CatalogOpts, host: &str, port: u16, serve_opts: ServeOpts) -> Result<()> {
    // Fail fast BEFORE touching the catalog. --tail-dir only means something
    // in buffered mode: with the synchronous default every INSERT already IS
    // an Iceberg commit before its ack, so a durable tail nothing writes to
    // would silently promise durability it never provides.
    if serve_opts.tail_dir.is_some() && serve_opts.write_buffer_ms == 0 {
        bail!(
            "--tail-dir requires buffered writes (--write-buffer-ms N with N > 0): the \
             synchronous default commits every INSERT before its ack, so the durable tail \
             would be a no-op. Set --write-buffer-ms, or drop --tail-dir."
        );
    }
    if serve_opts.tail_url.is_some() && serve_opts.write_buffer_ms == 0 {
        bail!(
            "--tail-url requires buffered writes (--write-buffer-ms N with N > 0): the \
             synchronous default commits every INSERT before its ack, so the durable tail \
             would be a no-op. Set --write-buffer-ms, or drop --tail-url."
        );
    }
    if serve_opts.tail_quorum.is_some() && serve_opts.write_buffer_ms == 0 {
        bail!(
            "--tail-quorum requires buffered writes (--write-buffer-ms N with N > 0): the \
             synchronous default commits every INSERT before its ack, so the durable tail \
             would be a no-op. Set --write-buffer-ms, or drop --tail-quorum."
        );
    }
    // The open tail API serves the buffer's window WITH per-op tail
    // sequences: it needs buffered mode AND a durable tail to exist at all.
    if serve_opts.tail_api_port.is_some()
        && (serve_opts.write_buffer_ms == 0
            || (serve_opts.tail_dir.is_none()
                && serve_opts.tail_url.is_none()
                && serve_opts.tail_quorum.is_none()))
    {
        bail!(
            "--tail-api-port requires buffered writes with a durable tail \
             (--write-buffer-ms N > 0 plus one of --tail-dir/--tail-url/--tail-quorum): \
             the tail API serves the buffer's un-flushed window keyed by durable tail \
             sequences, neither of which exists otherwise."
        );
    }
    // clap's conflicts_with already refuses the pairs; keep a hard error for
    // programmatic callers (one process writes ONE tail).
    if [
        serve_opts.tail_dir.is_some(),
        serve_opts.tail_url.is_some(),
        serve_opts.tail_quorum.is_some(),
    ]
    .iter()
    .filter(|&&set| set)
    .count()
        > 1
    {
        bail!(
            "--tail-dir, --tail-url, and --tail-quorum are mutually exclusive: a server \
             writes ONE tail"
        );
    }
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
    let auth: Option<Arc<dyn datafusion_postgres::pgwire::api::auth::AuthSource>> =
        match &serve_opts.auth_file {
            Some(path) => {
                #[cfg(feature = "managed")]
                {
                    let source = Arc::new(pgauth::FileAuthSource::load(path)?);
                    info!(
                        auth_file = %path.display(),
                        users = source.user_count(),
                        "SCRAM-SHA-256 authentication enabled (managed add-on)"
                    );
                    Some(source as Arc<dyn datafusion_postgres::pgwire::api::auth::AuthSource>)
                }
                #[cfg(not(feature = "managed"))]
                {
                    let _ = path;
                    bail!(
                        "--auth-file is a managed add-on: this open-source build was compiled \
                         without the `managed` feature. Rebuild with --features managed, or omit \
                         --auth-file to run open."
                    );
                }
            }
            None => {
                warn!(
                    "authentication is DISABLED — any user/password is accepted; \
                     pass --auth-file (env ICEGRES_AUTH_FILE) to require SCRAM-SHA-256 credentials"
                );
                None
            }
        };
    // Secure-by-default: don't expose an unauthenticated listener on a
    // non-loopback interface unless the operator opts in with --insecure.
    enforce_secure_default(host, auth.is_some(), serve_opts.insecure)?;

    // Zero-copy branch serving (--branch, SPEC D6): `None` = main = the
    // historical read/write paths byte-for-byte; `Some(name)` pins reads to
    // the branch head and routes every write to the branch ref.
    let branch: Option<String> = if serve_opts.branch == iceberg::spec::MAIN_BRANCH {
        None
    } else {
        info!(
            branch = %serve_opts.branch,
            "serving BRANCH {:?}: reads pin to the branch head, writes commit to the \
             branch ref (main and other branches are untouched); tables without this \
             branch fail loudly",
            serve_opts.branch
        );
        Some(serve_opts.branch.clone())
    };

    info!(
        catalog_uri = %opts.catalog_uri,
        warehouse = %opts.warehouse,
        s3_endpoint = %opts.s3_endpoint,
        "connecting to Iceberg REST catalog"
    );
    let catalog = context::connect_catalog(opts).await?;
    let engine = Arc::new(
        OverwriteEngine::connect(catalog.clone(), opts, serve_opts.enforce_pk, branch.clone())
            .await?,
    );

    // Moonlink-style buffered write mode (--write-buffer-ms, buffer.rs).
    // Default 0 = fully synchronous, current semantics unchanged. With
    // --tail-dir, a durable local WAL (tail.rs) closes the unclean-kill
    // loss window: fsync before every buffered ack, replay at boot.
    let write_buffer = if serve_opts.write_buffer_ms > 0 {
        let tail_store: Option<Arc<dyn tail::TailStore>> = match (
            &serve_opts.tail_dir,
            &serve_opts.tail_url,
            &serve_opts.tail_quorum,
        ) {
            (Some(dir), None, None) => Some(Arc::new(tail::LocalWal::open(dir)?)),
            // PgTail::open connects, takes the one-writer advisory
            // lock, and ensures the schema — an unreachable/locked
            // tail database fails startup loudly right here.
            (None, Some(url), None) => Some(Arc::new(tail_pg::PgTail::open(url)?)),
            // QuorumTail::open runs the full election + recovery — an
            // unreachable/unvotable quorum fails startup loudly right
            // here.
            (None, None, Some(spec)) => {
                let addrs: Vec<String> = spec
                    .split(',')
                    .map(|s| s.trim().to_string())
                    .filter(|s| !s.is_empty())
                    .collect();
                Some(Arc::new(tail_quorum::QuorumTail::open(&addrs)?))
            }
            (None, None, None) => None,
            _ => unreachable!("refused above"),
        };
        let buf = Arc::new(buffer::WriteBuffer::new(
            catalog.clone(),
            engine.clone(),
            serve_opts.write_buffer_ms,
            tail_store,
        ));
        if let Some(dir) = &serve_opts.tail_dir {
            warn!(
                write_buffer_ms = serve_opts.write_buffer_ms,
                max_rows = buf.max_rows(),
                tail_dir = %dir.display(),
                "write buffering is ENABLED with a durable local tail: INSERTs fsync to \
                 the tail BEFORE their ack and un-flushed rows replay on the next boot \
                 with the same --tail-dir, so an unclean kill of this process loses \
                 NOTHING. Durability is THIS node's disk — losing the node or the disk \
                 still loses acked-but-uncommitted rows. Other servers/readers see \
                 buffered rows only at the commit cadence; reads on this server union \
                 the buffer (read-your-writes holds locally)."
            );
        } else if let Some(spec) = &serve_opts.tail_quorum {
            warn!(
                write_buffer_ms = serve_opts.write_buffer_ms,
                max_rows = buf.max_rows(),
                tail_quorum = %spec,
                "write buffering is ENABLED with a durable quorum tail (--tail-quorum): \
                 INSERTs are fsynced by 2 of 3 icekeeperd acceptors BEFORE their ack and \
                 un-flushed rows replay on the next boot against the same quorum, so an \
                 unclean kill, losing this NODE, or losing ANY SINGLE acceptor — \
                 including this one — loses NOTHING (quorum consensus, adapted from \
                 Neon's safekeeper). Fewer than 2 live acceptors BLOCKS buffered writes \
                 (statement errors — backpressure, never silent loss). Other \
                 servers/readers see buffered rows only at the commit cadence; reads on \
                 this server union the buffer (read-your-writes holds locally)."
            );
        } else if serve_opts.tail_url.is_some() {
            warn!(
                write_buffer_ms = serve_opts.write_buffer_ms,
                max_rows = buf.max_rows(),
                "write buffering is ENABLED with a durable Postgres tail (--tail-url): \
                 INSERTs commit to the tail database BEFORE their ack and un-flushed \
                 rows replay on the next boot against the same tail, so an unclean kill \
                 — or losing this NODE entirely — loses NOTHING; durability = the tail \
                 database's own fsync/replication. A tail-database outage BLOCKS \
                 buffered writes (statement errors — backpressure, never silent loss). \
                 Other servers/readers see buffered rows only at the commit cadence; \
                 reads on this server union the buffer (read-your-writes holds locally)."
            );
        } else {
            warn!(
                write_buffer_ms = serve_opts.write_buffer_ms,
                max_rows = buf.max_rows(),
                "write buffering is ENABLED: INSERTs acknowledge BEFORE their Iceberg commit; \
                 an UNCLEAN kill loses up to {} ms of acked-but-uncommitted writes; other \
                 servers/readers see buffered rows only at the commit cadence. Reads on this \
                 server union the buffer (read-your-writes holds locally).",
                serve_opts.write_buffer_ms
            );
        }
        // Recover acked rows a previous process failed to commit: into
        // pending BEFORE the flusher starts (and before the listener opens),
        // so the normal cadence drains them like any other buffered rows.
        buf.replay_tail().await?;
        buf.spawn_flusher();
        Some(buf)
    } else {
        None
    };

    // Bounded-staleness reads (--freshness-ms, freshness.rs). Default 0 =
    // exact freshness, byte-identical semantics and code path.
    if serve_opts.freshness_ms > 0 {
        warn!(
            freshness_ms = serve_opts.freshness_ms,
            "bounded-staleness reads are ENABLED (--freshness-ms): scans serve the cached \
             snapshot with NO per-scan catalog check; commits from OTHER writers become \
             visible within ~{} ms plus one refresh round trip — a slow table delays only \
             itself (retry-free per-table refresh timeout min(4*N, 2000) ms; the next pass \
             retries), never other tables (exact freshness is the default, --freshness-ms \
             0). THIS server's own writes remain read-your-own-writes exact via synchronous \
             invalidation. During a catalog outage reads keep serving the last refreshed \
             snapshot (ICEGRES_STALE_READ_ON_CATALOG_ERROR=0 fails loudly instead) — \
             worst-case age is the icegres_freshness_age_ms gauge on /metrics (sampled at \
             refresher pass start; healthy ~N).",
            serve_opts.freshness_ms
        );
    }

    // Fleet overlays (--peer-tail, peer.rs): the mirror registry is threaded
    // into every table provider; the subscriber tasks are spawned below.
    let peer_mirrors: Option<Arc<peer::PeerMirrors>> = {
        let peers: Vec<String> = serve_opts
            .peer_tail
            .iter()
            .map(|p| p.trim().to_string())
            .filter(|p| !p.is_empty())
            .collect();
        if peers.is_empty() {
            None
        } else {
            warn!(
                peers = %peers.join(","),
                "peer tail overlays are ENABLED (--peer-tail): scans union each peer's \
                 un-flushed tail window with committed data (event-bound freshness for \
                 peer writes). Best-effort: a dead or silent peer drops out of reads \
                 (commit-cadence fallback; per-peer gauge icegres_peer_tail_age_ms, \
                 worst-case icegres_peer_tail_age_max_ms)."
            );
            // With bounded-staleness reads, watermark-covered mirror items
            // must outlive the staleness window: retention = max(30 s, 4×
            // the freshness bound), or a stale committed snapshot could
            // miss rows the mirror already GC'd (see peer.rs).
            let mirror_gc = peer::effective_mirror_gc(serve_opts.freshness_ms);
            if serve_opts.freshness_ms > 0 {
                warn!(
                    freshness_ms = serve_opts.freshness_ms,
                    mirror_gc_ms = mirror_gc.as_millis() as u64,
                    "--peer-tail with --freshness-ms: watermark-covered peer-mirror \
                     items are retained for max(30 s, 4× the freshness bound) = {} ms, \
                     so a bounded-stale reader never misses rows that left the mirror \
                     before its committed snapshot caught up (memory cost only).",
                    mirror_gc.as_millis()
                );
            }
            Some(Arc::new(peer::PeerMirrors::with_gc(mirror_gc)))
        }
    };

    let ctx = Arc::new(
        context::build_session_context_with_peers(
            catalog.clone(),
            None,
            write_buffer.clone(),
            branch.clone(),
            serve_opts.freshness_ms,
            peer_mirrors.clone(),
        )
        .await?,
    );
    if let Some(mirrors) = &peer_mirrors {
        let peers: Vec<String> = serve_opts
            .peer_tail
            .iter()
            .map(|p| p.trim().to_string())
            .filter(|p| !p.is_empty())
            .collect();
        peer::spawn_peer_tails(peers, mirrors.clone());
    }

    // Spawn the per-server refresher AFTER the context registered every
    // table with the freshness registry (tables created later register
    // lazily and are picked up on the refresher's next pass).
    if serve_opts.freshness_ms > 0 {
        freshness::spawn_refresher(std::time::Duration::from_millis(serve_opts.freshness_ms));
    }

    setup_pg_catalog(
        &ctx,
        context::CATALOG_NAME,
        Arc::new(AuthManager::default()),
    )
    .map_err(|e| anyhow::anyhow!("failed to set up pg_catalog emulation: {e}"))?;
    compat::register_compat_udfs(&ctx);
    compat::install_coherent_pg_catalog(&ctx, context::CATALOG_NAME).await?;

    if let Some(hp) = serve_opts.health_port {
        ops::spawn_health_listener(host, hp, catalog.clone()).await?;
    }

    if serve_opts.enforce_pk {
        info!(
            "PK enforcement is ON (--enforce-pk): tables with the '{}' property get \
             NOT NULL + uniqueness checks on writes",
            overwrite::PK_PROPERTY
        );
    }
    // Lakekeeper-style ReBAC authorization (--authz-file). None = open (any
    // authenticated principal, all tables); Some = every statement gated.
    let authorizer = build_authorizer(&serve_opts.authz_file, serve_opts.auth_file.is_some())?;

    let txn_registry = Arc::new(TxnRegistry::new());
    // Keep a handle for the graceful-shutdown flush before the buffer is moved
    // into the hook chain.
    let shutdown_buffer = write_buffer.clone();

    // Open tail read API (--tail-api-port, docs/open-tail-protocol.md): a
    // read-only Flight listener inside THIS process — the only one holding
    // the overlay state. Bind failures abort startup here.
    if let Some(tail_port) = serve_opts.tail_api_port {
        let buffer = write_buffer
            .clone()
            .expect("validated: --tail-api-port requires buffered mode");
        flight::spawn_tail_api(
            ctx.clone(),
            engine.clone(),
            buffer,
            host,
            tail_port,
            serve_opts.auth_file.clone(),
            authorizer.clone(),
        )
        .await?;
    }

    let hooks = query_hooks(
        engine,
        txn_registry.clone(),
        catalog,
        write_buffer,
        serve_opts.enforce_pk,
        authorizer,
        serve_opts.freshness_ms > 0,
    );

    info!(listen_addr = %format!("{host}:{port}"), "starting pgwire server");
    // Always the icegres accept loop (ops.rs): it is byte-for-byte the
    // upstream loop when no TLS/auth/idle-shutdown is configured, PLUS the
    // per-connection cleanup that drops an open transaction when its socket
    // closes (disconnect = implicit ROLLBACK; without this, abandoned
    // transaction buffers would leak).
    ops::serve_custom(
        ctx,
        host,
        port,
        serve_opts.idle_shutdown_secs,
        tls,
        auth,
        hooks,
        txn_registry,
        shutdown_buffer,
    )
    .await
    .context("pgwire server failed")?;
    Ok(())
}

/// The icegres query-hook chain, in order:
/// 1. [`buffer::BufferHook`] (only with `--write-buffer-ms > 0`) — buffered
///    autocommit INSERT ack, keyed tail UPDATE/DELETE ack (Phase 2: exact-PK
///    statements on `icegres.tail-upsert` tables with a durable tail), and
///    ordering fences (flush before non-keyed UPDATE/DELETE/BEGIN/DDL). Must
///    run first so a fence flush happens before the fenced statement's own
///    handler; it defers to TxnHook for any connection with an open
///    transaction.
/// 2. [`compat::CompatHook`] — ORM/driver pg_catalog compatibility rewrites
///    (SPEC A8). Must run before TxnHook: ORMs reflect inside a
///    driver-opened transaction, and the rewritten introspection SQL reads
///    static catalog tables only.
/// 3. [`ops::CopyOutHook`] — `COPY ... TO STDOUT` (SPEC A11 lane 2, the
///    adbc_driver_postgresql read path). Runs before TxnHook: a COPY inside
///    an explicit transaction reads the latest committed snapshot
///    (statement-level consistency; see the hook's docs).
/// 4. [`TxnHook`] — BEGIN/COMMIT/ROLLBACK and, while a transaction is open,
///    EVERY statement on that connection (buffered writes, pinned reads);
///    also PK-enforced autocommit INSERT. Replaces the upstream
///    `TransactionStatementHook`, whose BEGIN/COMMIT were accepted but
///    non-transactional.
/// 5. `SetShowHook` — upstream SET/SHOW handling.
/// 6. [`dml::DmlHook`] — autocommit copy-on-write UPDATE/DELETE.
/// 7. [`compat::InsertTagHook`] — fall-through: plain autocommit INSERTs get
///    a proper `INSERT 0 n` command tag on the extended protocol (SPEC A9,
///    JDBC `executeUpdate()`); every specialized INSERT path above ran first.
/// 8. [`plancache::PlanCacheHook`] (only with `--freshness-ms > 0`) —
///    physical-plan cache for repeated simple-protocol SELECT shapes
///    (plancache.rs). After every specialized hook (it must only see plain
///    autocommit SELECTs) and before the timing hook (so timing measures
///    the cached path when both are enabled).
/// 9. [`timing::TimingHook`] — diagnostic per-stage read timing, active only
///    with `ICEGRES_QUERY_TIMING=1`. LAST so it only ever sees plain SELECTs
///    that would fall through to the default handler.
#[allow(clippy::too_many_arguments)]
fn query_hooks(
    engine: Arc<OverwriteEngine>,
    registry: Arc<TxnRegistry>,
    catalog: Arc<dyn Catalog>,
    write_buffer: Option<Arc<buffer::WriteBuffer>>,
    enforce_pk: bool,
    authorizer: Option<authz::SharedAuthorizer>,
    plan_cache: bool,
) -> Vec<Arc<dyn QueryHook>> {
    let mut hooks: Vec<Arc<dyn QueryHook>> = Vec::with_capacity(8);
    // Observe-only: count every wire statement (falls through, never changes
    // behavior). First so it sees all statements including denied ones.
    hooks.push(Arc::new(metrics::MetricsHook));
    // 0. AuthzHook runs FIRST so an unauthorized statement is rejected (42501)
    //    before any rewrite, buffering, or planning touches it.
    if let Some(a) = authorizer {
        hooks.push(Arc::new(authz::AuthzHook::new(
            a,
            context::DEFAULT_SCHEMA.to_string(),
        )));
    }
    if let Some(buf) = &write_buffer {
        hooks.push(Arc::new(buffer::BufferHook::new(
            buf.clone(),
            registry.clone(),
            enforce_pk,
        )));
    }
    hooks.push(Arc::new(compat::CompatHook));
    hooks.push(Arc::new(ops::CopyOutHook));
    // The buffer handle lets COMMIT serialize against in-flight keyed
    // read-modify-writes on keyed-activated tables (buffer.rs, fix L1).
    hooks.push(Arc::new(TxnHook::new(
        registry,
        engine.clone(),
        catalog,
        write_buffer,
    )));
    hooks.push(Arc::new(SetShowHook));
    hooks.push(Arc::new(dml::DmlHook::new(engine)));
    hooks.push(Arc::new(compat::InsertTagHook));
    // Physical-plan cache (plancache.rs); only registered in freshness mode
    // (--freshness-ms > 0), where a cache hit is sound without a per-scan
    // catalog check. Keeps the default-mode hook chain untouched.
    if plan_cache {
        hooks.push(Arc::new(plancache::PlanCacheHook::new()));
    }
    // Diagnostic per-stage timing (timing.rs); inert unless
    // ICEGRES_QUERY_TIMING=1. Must stay last.
    hooks.push(Arc::new(timing::TimingHook::new()));
    hooks
}

async fn run_sql(opts: &CatalogOpts, query: &str, enforce_pk: bool) -> Result<()> {
    let catalog = context::connect_catalog(opts).await?;
    // UPDATE/DELETE take the same copy-on-write path as the server's wire
    // handler; everything else goes through DataFusion unchanged.
    if let Some(dml_stmt) = dml::parse_single_dml(query)? {
        let engine = OverwriteEngine::connect(catalog, opts, enforce_pk, None).await?;
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
