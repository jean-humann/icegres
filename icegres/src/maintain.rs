//! `icegres maintain` — table lifecycle maintenance over Iceberg metadata.
//!
//! Long-lived Iceberg tables accumulate a snapshot per commit forever: every
//! INSERT/UPDATE/DELETE/branch op adds one, and nothing ever removes them
//! automatically. Unbounded, that makes `$snapshots` (and the metadata JSON
//! the catalog loads on every table open) grow without limit. Expiring old
//! snapshots is the standard remedy — a metadata-only commit that drops
//! snapshots older than a retention window while keeping the newest N and
//! every snapshot still reachable from a branch/tag ref.
//!
//! ```text
//! icegres maintain expire-snapshots demo.trips            # keep newest 10
//! icegres maintain expire-snapshots demo.trips --keep 50  # keep newest 50
//! ```
//!
//! This is metadata-only: the expired snapshots' data and manifest files are
//! left in object storage (a separate orphan-file GC reclaims them). It is
//! safe to run against a live serving endpoint — the commit is anchored with
//! `assert-table-uuid` and `assert-ref-snapshot-id main=<head>`, so a write
//! landing concurrently makes the expire conflict and no-op rather than
//! stranding a ref on a missing snapshot.

use anyhow::{Context as _, Result};

use crate::branch::parse_table;
use crate::context;
use crate::overwrite::OverwriteEngine;
use crate::CatalogOpts;

/// `icegres maintain expire-snapshots <table> [--keep N]`
pub async fn expire_snapshots(opts: &CatalogOpts, table: &str, keep_last: usize) -> Result<()> {
    let ident = parse_table(table)?;
    let catalog = context::connect_catalog(opts).await?;
    let engine = OverwriteEngine::connect(catalog, opts, false, None).await?;
    let removed = engine
        .expire_snapshots(&ident, keep_last)
        .await
        .with_context(|| format!("failed to expire snapshots on {ident}"))?;
    if removed == 0 {
        println!(
            "expired 0 snapshots on {ident} (nothing older than the newest {keep_last} + \
             referenced refs)"
        );
    } else {
        println!(
            "expired {removed} snapshot(s) on {ident} (kept newest {keep_last} + all \
             ref-referenced; data files reclaimed by orphan GC)"
        );
    }
    Ok(())
}
