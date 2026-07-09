//! `icegres branch` — Neon-style zero-copy branches over Iceberg snapshot
//! refs (SPEC D6).
//!
//! An Iceberg *branch* is a named snapshot ref in table metadata. Creating
//! one is a pure metadata commit (`set-snapshot-ref` + an
//! `assert-ref-snapshot-id <name>=null` requirement so creation is atomic):
//! no data file, manifest, or manifest list is copied — the branch shares
//! every byte with its source snapshot. `icegres serve --branch <name>`
//! then pins all reads to the branch head and routes all writes to the
//! branch ref, which is exactly Neon's branch-per-endpoint model with the
//! single copy of the data living in the lake:
//!
//! ```text
//! icegres branch create demo.trips dev      # zero-copy fork of main's head
//! icegres serve --port 5439                 # endpoint on main
//! icegres serve --port 5440 --branch dev    # endpoint on dev
//! icegres branch drop demo.trips dev        # drop the ref (snapshots stay)
//! ```
//!
//! Writes on each endpoint carry `assert-ref-snapshot-id` on their OWN
//! branch, so the two endpoints never conflict with each other; history
//! stays shared below the fork point and diverges above it. Dropping a
//! branch removes only the ref — its snapshots remain in metadata (still
//! time-travel-readable via `table@snapshot_id`) until snapshot expiry.

use anyhow::{bail, Context as _, Result};
use iceberg::TableIdent;

use crate::context::{self, DEFAULT_SCHEMA};
use crate::overwrite::OverwriteEngine;
use crate::CatalogOpts;

/// Parse `table` as `namespace.table` (or bare `table` in the default
/// namespace). Matches the identifier rules the SQL layer uses.
pub(crate) fn parse_table(table: &str) -> Result<TableIdent> {
    let parts: Vec<&str> = table.split('.').collect();
    let (ns, name) = match parts.as_slice() {
        [name] => (DEFAULT_SCHEMA, *name),
        [ns, name] => (*ns, *name),
        _ => bail!("table must be <table> or <namespace>.<table>, got {table:?}"),
    };
    if ns.is_empty() || name.is_empty() {
        bail!("table must be <table> or <namespace>.<table>, got {table:?}");
    }
    TableIdent::from_strs([ns, name]).map_err(|e| anyhow::anyhow!("bad table identifier: {e}"))
}

/// Validate a branch name: catalog-safe, no whitespace/quotes, non-empty.
fn validate_name(name: &str) -> Result<()> {
    if name.is_empty()
        || !name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '_' | '-' | '.'))
    {
        bail!(
            "branch name {name:?} is invalid: use ASCII letters, digits, '_', '-' or '.' \
             (no spaces or quotes)"
        );
    }
    Ok(())
}

async fn engine(opts: &CatalogOpts) -> Result<OverwriteEngine> {
    let catalog = context::connect_catalog(opts).await?;
    OverwriteEngine::connect(catalog, opts, false, None).await
}

/// `icegres branch create <table> <name> [--at-snapshot id]`
pub async fn create(
    opts: &CatalogOpts,
    table: &str,
    name: &str,
    at_snapshot: Option<i64>,
) -> Result<()> {
    validate_name(name)?;
    let ident = parse_table(table)?;
    let src = engine(opts)
        .await?
        .create_branch(&ident, name, at_snapshot)
        .await
        .with_context(|| format!("failed to create branch {name:?} on {ident}"))?;
    println!("created branch {name} on {ident} at snapshot {src} (zero-copy snapshot ref)");
    Ok(())
}

/// `icegres branch list <table>` — one `name<TAB>snapshot_id<TAB>type` row
/// per ref, `main` first.
pub async fn list(opts: &CatalogOpts, table: &str) -> Result<()> {
    let ident = parse_table(table)?;
    let refs = engine(opts)
        .await?
        .list_refs(&ident)
        .await
        .with_context(|| format!("failed to list branches of {ident}"))?;
    if refs.is_empty() {
        println!("(no snapshot refs — table {ident} has no snapshot yet)");
        return Ok(());
    }
    for (name, snapshot_id, kind) in refs {
        println!("{name}\t{snapshot_id}\t{kind}");
    }
    Ok(())
}

/// `icegres branch create-all <name>` — whole-lakehouse branch: set the ref
/// on EVERY table in the catalog in ONE atomic multi-table transaction
/// (Iceberg REST `transactions/commit`; requires a catalog that implements
/// it, e.g. Lakekeeper). Each table carries the
/// `assert-ref-snapshot-id <name>=null` creation guard AND pins `main` to
/// the head captured when the table was loaded, so the command succeeds
/// only if every captured head was still current at commit time: the cut is
/// consistent-or-nothing — it can never show half of a concurrent commit,
/// and any race (or an already-existing branch) fails the whole command
/// with nothing applied (just retry). Tables without a snapshot cannot
/// hold a ref and are skipped with a loud warning.
pub async fn create_all(opts: &CatalogOpts, name: &str) -> Result<()> {
    validate_name(name)?;
    let (branched, skipped) = engine(opts)
        .await?
        .create_branch_all(name)
        .await
        .with_context(|| format!("failed to create whole-lakehouse branch {name:?}"))?;
    for (ident, src) in &branched {
        println!("created branch {name} on {ident} at snapshot {src}");
    }
    for ident in &skipped {
        eprintln!(
            "WARNING: skipped {ident} — it has no snapshot yet, and an Iceberg branch \
             ref must point at one (write to it, then `icegres branch create {ident} \
             {name}` to add it)"
        );
    }
    println!(
        "created branch {name} on {} table(s) in ONE atomic transaction (zero-copy \
         snapshot refs; consistent-or-nothing cross-table cut — every table's main \
         head was pinned as captured; {} snapshot-less table(s) skipped)",
        branched.len(),
        skipped.len()
    );
    Ok(())
}

/// `icegres branch drop-all <name>` — remove the ref from every table that
/// has it in ONE atomic multi-table transaction (`main` is refused; tables
/// without the ref are skipped; errors if no table has it).
pub async fn drop_all(opts: &CatalogOpts, name: &str) -> Result<()> {
    validate_name(name)?;
    let (dropped, skipped) = engine(opts)
        .await?
        .drop_branch_all(name)
        .await
        .with_context(|| format!("failed to drop whole-lakehouse branch {name:?}"))?;
    for (ident, head) in &dropped {
        println!("dropped branch {name} on {ident} (was at snapshot {head})");
    }
    println!(
        "dropped branch {name} from {} table(s) in ONE atomic transaction ({skipped} \
         table(s) without the ref skipped; snapshots stay time-travel-readable until \
         expiry)",
        dropped.len()
    );
    Ok(())
}

/// `icegres branch drop <table> <name>`
pub async fn drop(opts: &CatalogOpts, table: &str, name: &str) -> Result<()> {
    validate_name(name)?;
    let ident = parse_table(table)?;
    let head = engine(opts)
        .await?
        .drop_branch(&ident, name)
        .await
        .with_context(|| format!("failed to drop branch {name:?} on {ident}"))?;
    println!(
        "dropped branch {name} on {ident} (was at snapshot {head}; snapshots stay \
         time-travel-readable until expiry)"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_table_defaults_namespace() {
        let ident = parse_table("trips").unwrap();
        assert_eq!(ident.namespace().to_url_string(), DEFAULT_SCHEMA);
        assert_eq!(ident.name(), "trips");
        let ident = parse_table("other.t").unwrap();
        assert_eq!(ident.namespace().to_url_string(), "other");
    }

    #[test]
    fn parse_table_rejects_bad_forms() {
        assert!(parse_table("a.b.c").is_err());
        assert!(parse_table("").is_err());
        assert!(parse_table(".t").is_err());
        assert!(parse_table("ns.").is_err());
    }

    #[test]
    fn validate_name_rules() {
        assert!(validate_name("dev").is_ok());
        assert!(validate_name("feature-x_1.2").is_ok());
        assert!(validate_name("").is_err());
        assert!(validate_name("has space").is_err());
        assert!(validate_name("q\"uote").is_err());
    }
}
