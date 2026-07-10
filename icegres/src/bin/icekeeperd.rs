//! icekeeperd — the quorum-tail acceptor daemon (roadmap §3 backend 3): one
//! lightweight process per replica of the consensus-class durable tail.
//! Three of these (on independent nodes/disks) plus `icegres serve
//! --tail-quorum h:p,h:p,h:p` give buffered writes durability that survives
//! losing ANY single node — including the icegres compute itself.
//!
//! The consensus protocol (terms, single-shot voting, term-history
//! reconciliation, divergence truncation) is adapted from neondatabase/neon
//! safekeeper (Apache-2.0) — see NOTICE and `src/quorum/`. This binary is a
//! thin shell: CLI + listener around `quorum::acceptor`, which fsyncs every
//! appended record before acknowledging it and persists every vote before
//! casting it.
//!
//! Honest scope (see docs/limitations.md): static 3-node membership, no
//! TLS/authentication between proposer and acceptors (trusted network
//! segment only), no acceptor-to-acceptor gossip (the proposer drives all
//! catch-up), no S3 offload of acceptor segments.

// The consensus modules are shared source files with the `icegres` binary
// (this crate has no lib target); the proposer half is unused here.
#[allow(dead_code)]
#[path = "../quorum/mod.rs"]
mod quorum;
#[allow(dead_code)]
#[path = "../segment.rs"]
mod segment;

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context as _, Result};
use clap::{Parser, Subcommand};
use tracing::info;

use quorum::acceptor::WalStore as _;

#[derive(Parser)]
#[command(
    name = "icekeeperd",
    version = env!("ICEGRES_LONG_VERSION"),
    about = "icegres quorum-tail acceptor: fsyncs replicated tail records and votes \
             in proposer elections (adapted from Neon's safekeeper)"
)]
struct Cli {
    #[command(subcommand)]
    command: KCommand,
}

#[derive(Subcommand)]
enum KCommand {
    /// Serve one acceptor over TCP.
    Serve {
        /// Address to bind on. Plain TCP, no TLS/auth — bind a loopback or
        /// trusted-segment interface only.
        #[arg(long, env = "ICEKEEPER_HOST", default_value = "127.0.0.1")]
        host: String,

        /// Port to bind on.
        #[arg(long, env = "ICEKEEPER_PORT")]
        port: u16,

        /// Data directory (control file + log segments). One directory per
        /// acceptor, exclusively flocked.
        #[arg(long, env = "ICEKEEPER_DATA_DIR")]
        data_dir: PathBuf,

        /// Diagnostic node id, pinned into the data dir on first start
        /// (a mismatch later is refused).
        #[arg(long, env = "ICEKEEPER_NODE_ID", default_value_t = 0)]
        node_id: u64,
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

    let KCommand::Serve {
        host,
        port,
        data_dir,
        node_id,
    } = Cli::parse().command;

    let acceptor = quorum::acceptor::open_dir(&data_dir, node_id)?;
    let flush = acceptor.wal.flush_lsn();
    let term = acceptor.state.term;
    let tail_id = acceptor.state.tail_id.clone();
    let shared: quorum::acceptor::SharedAcceptor = Arc::new(tokio::sync::Mutex::new(acceptor));

    let addr = format!("{host}:{port}");
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("cannot bind icekeeperd listener on {addr}"))?;
    info!(
        listen_addr = %addr,
        data_dir = %data_dir.display(),
        node_id,
        term,
        flush_lsn = flush,
        tail_id = tail_id.as_deref().unwrap_or("(unadopted)"),
        "icekeeperd acceptor ready (fsync-before-ack; votes persist before casting)"
    );

    let mut sigterm = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
        .context("failed to install SIGTERM handler")?;
    tokio::select! {
        res = quorum::acceptor::serve(listener, shared) => res,
        _ = tokio::signal::ctrl_c() => {
            info!("icekeeperd shutting down (ctrl-c)");
            Ok(())
        }
        _ = sigterm.recv() => {
            info!("icekeeperd shutting down (SIGTERM)");
            Ok(())
        }
    }
}
