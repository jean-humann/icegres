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

use std::collections::HashSet;

use anyhow::{bail, Context as _, Result};
use iceberg::spec::TableMetadata;
use iceberg::TableIdent;

use crate::context::{self, DEFAULT_SCHEMA};
use crate::overwrite::{FastForwardMove, OverwriteEngine};
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

// ---------------------------------------------------------------------------
// `icegres branch diff` / `icegres branch merge` (roadmap-v2 P5)
// ---------------------------------------------------------------------------
//
// Both commands are METADATA-ONLY: one `load_table` per table answers
// everything (refs, snapshot lineage, summaries, schemas) — no data file is
// ever read, so a whole-lakehouse diff costs one catalog round trip per
// table. The fork point of two refs is their snapshots' common ancestor,
// found by walking `parent-snapshot-id` chains: `branch create-all` pins
// every table's branch to the exact head it forked, so the walk terminates
// at that snapshot unless history has been expired out from under it (then
// the fork is reported as unknown and the pair as diverged — honest, not
// guessed). Row deltas come from snapshot SUMMARY properties
// (`added-records`/`deleted-records`) and are labeled summary-reported:
// they are what the committing engines recorded, not a recount.

/// Iceberg snapshot-summary property naming the rows a snapshot added.
const SUMMARY_ADDED_RECORDS: &str = "added-records";
/// Iceberg snapshot-summary property naming the rows a snapshot deleted.
const SUMMARY_DELETED_RECORDS: &str = "deleted-records";

/// How one table's two refs relate across the fork point.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DiffStatus {
    /// Both refs point at the same snapshot.
    Unchanged,
    /// Only `a` moved since the fork (`b`'s head IS the fork point).
    AdvancedA,
    /// Only `b` moved since the fork (`a`'s head IS the fork point).
    AdvancedB,
    /// Both moved (or the fork point is no longer in metadata): a
    /// fast-forward in either direction is impossible.
    Diverged,
    /// The ref exists only on side `a` (created relative to `b`).
    OnlyA,
    /// The ref exists only on side `b` (dropped relative to `b`).
    OnlyB,
    /// Neither ref exists (a snapshot-less or never-branched table).
    NoRef,
}

impl DiffStatus {
    /// Stable machine-readable label (used by `--json` and tests).
    fn label(self) -> &'static str {
        match self {
            DiffStatus::Unchanged => "unchanged",
            DiffStatus::AdvancedA => "advanced-a",
            DiffStatus::AdvancedB => "advanced-b",
            DiffStatus::Diverged => "diverged",
            DiffStatus::OnlyA => "created",
            DiffStatus::OnlyB => "dropped",
            DiffStatus::NoRef => "no-ref",
        }
    }
}

/// One snapshot on a side's exclusive lineage (head down to, excluding, the
/// fork point), with its summary-reported row deltas.
#[derive(Debug, Clone)]
struct SnapshotInfo {
    id: i64,
    timestamp_ms: i64,
    operation: String,
    added_records: Option<u64>,
    deleted_records: Option<u64>,
}

/// Aggregate of one side's exclusive snapshots.
#[derive(Debug, Clone, Default)]
struct SideDelta {
    snapshots: Vec<SnapshotInfo>,
    rows_added: u64,
    rows_deleted: u64,
    /// Snapshots whose summary lacked the row-count properties (their rows
    /// are NOT in the totals — surfaced so the numbers stay honest).
    summary_missing: usize,
}

/// Top-level column changes between the two heads' schemas, matched by
/// Iceberg field id (a rename keeps its id; add/drop mint/retire one).
#[derive(Debug, Clone, Default)]
struct SchemaDiff {
    a_schema_id: i32,
    b_schema_id: i32,
    /// `(name, type)` present on `a` only.
    added: Vec<(String, String)>,
    /// `(name, type)` present on `b` only.
    dropped: Vec<(String, String)>,
    /// `(b_name, a_name)` — same field id, renamed.
    renamed: Vec<(String, String)>,
    /// `(name, b_type, a_type)` — same field id, type changed.
    retyped: Vec<(String, String, String)>,
}

impl SchemaDiff {
    fn is_empty(&self) -> bool {
        self.added.is_empty()
            && self.dropped.is_empty()
            && self.renamed.is_empty()
            && self.retyped.is_empty()
            && self.a_schema_id == self.b_schema_id
    }
}

/// Everything `diff`/`merge` know about one table, from one metadata read.
#[derive(Debug, Clone)]
struct TableCmp {
    ident: TableIdent,
    uuid: uuid::Uuid,
    a_head: Option<i64>,
    b_head: Option<i64>,
    /// Common ancestor of the two heads (when both exist and the lineage
    /// still reaches it — expired history makes this `None`).
    fork: Option<i64>,
    status: DiffStatus,
    a_side: SideDelta,
    b_side: SideDelta,
    schema: SchemaDiff,
    /// `icegres.*` table properties worth surfacing (PK/keyed-tail settings
    /// — table-scoped in Iceberg, i.e. SHARED by both sides; watermark
    /// bookkeeping properties are filtered out as noise).
    notable_props: Vec<(String, String)>,
}

/// Walk `head`'s parent chain (inclusive) through `parent_of`, stopping at
/// the root or where metadata no longer knows the snapshot (expired
/// history). `parent_of(id)` answers `None` when `id` itself is unknown,
/// `Some(parent)` otherwise. A defensive seen-set guards against cyclic
/// metadata.
fn lineage_of(head: i64, parent_of: &impl Fn(i64) -> Option<Option<i64>>) -> Vec<i64> {
    let mut out = Vec::new();
    let mut seen = HashSet::new();
    let mut cur = Some(head);
    while let Some(id) = cur {
        if !seen.insert(id) {
            break; // cyclic metadata: never loop
        }
        let Some(parent) = parent_of(id) else {
            break; // snapshot expired out of metadata: chain ends here
        };
        out.push(id);
        cur = parent;
    }
    out
}

/// Fork point of two heads: the first snapshot on `b`'s ancestry that is
/// also on `a`'s (both chains inclusive of their heads). `None` when the
/// chains share nothing that metadata still knows.
fn fork_point(
    a_head: i64,
    b_head: i64,
    parent_of: &impl Fn(i64) -> Option<Option<i64>>,
) -> Option<i64> {
    let a_ancestry: HashSet<i64> = lineage_of(a_head, parent_of).into_iter().collect();
    lineage_of(b_head, parent_of)
        .into_iter()
        .find(|id| a_ancestry.contains(id))
}

/// Classify two heads against their fork point (pure; unit-tested).
fn classify(a_head: Option<i64>, b_head: Option<i64>, fork: Option<i64>) -> DiffStatus {
    match (a_head, b_head) {
        (None, None) => DiffStatus::NoRef,
        (Some(_), None) => DiffStatus::OnlyA,
        (None, Some(_)) => DiffStatus::OnlyB,
        (Some(a), Some(b)) if a == b => DiffStatus::Unchanged,
        (Some(a), Some(b)) => {
            if fork == Some(b) {
                DiffStatus::AdvancedA
            } else if fork == Some(a) {
                DiffStatus::AdvancedB
            } else {
                DiffStatus::Diverged
            }
        }
    }
}

/// The side's exclusive snapshots: from `head` down to (excluding) `fork`.
fn side_delta(metadata: &TableMetadata, head: Option<i64>, fork: Option<i64>) -> SideDelta {
    let mut delta = SideDelta::default();
    let Some(head) = head else {
        return delta;
    };
    let parent_of = |id: i64| metadata.snapshot_by_id(id).map(|s| s.parent_snapshot_id());
    for id in lineage_of(head, &parent_of) {
        if Some(id) == fork {
            break;
        }
        let snapshot = metadata
            .snapshot_by_id(id)
            .expect("lineage_of only yields ids known to the metadata");
        let summary = snapshot.summary();
        let prop =
            |key: &str| -> Option<u64> { summary.additional_properties.get(key)?.parse().ok() };
        let added = prop(SUMMARY_ADDED_RECORDS);
        let deleted = prop(SUMMARY_DELETED_RECORDS);
        if added.is_none() && deleted.is_none() {
            delta.summary_missing += 1;
        }
        delta.rows_added += added.unwrap_or(0);
        delta.rows_deleted += deleted.unwrap_or(0);
        delta.snapshots.push(SnapshotInfo {
            id,
            timestamp_ms: snapshot.timestamp_ms(),
            operation: format!("{:?}", summary.operation).to_ascii_lowercase(),
            added_records: added,
            deleted_records: deleted,
        });
    }
    delta
}

/// Top-level columns of the schema a head snapshot wrote, as
/// `(field_id, name, type)` (falls back to the current schema when the
/// snapshot carries no schema id, per the Iceberg spec's v1 allowance).
fn head_schema(metadata: &TableMetadata, head: i64) -> Vec<(i32, String, String)> {
    let schema = metadata
        .snapshot_by_id(head)
        .and_then(|s| s.schema_id())
        .and_then(|id| metadata.schema_by_id(id))
        .unwrap_or_else(|| metadata.current_schema());
    schema
        .as_struct()
        .fields()
        .iter()
        .map(|f| (f.id, f.name.clone(), f.field_type.to_string()))
        .collect()
}

/// Schema id of a head snapshot (same fallback as [`head_schema`]).
fn head_schema_id(metadata: &TableMetadata, head: i64) -> i32 {
    metadata
        .snapshot_by_id(head)
        .and_then(|s| s.schema_id())
        .unwrap_or_else(|| metadata.current_schema_id())
}

/// Column add/drop/rename/retype between the two heads' schemas, matched by
/// field id.
fn schema_diff(metadata: &TableMetadata, a_head: i64, b_head: i64) -> SchemaDiff {
    let a_fields = head_schema(metadata, a_head);
    let b_fields = head_schema(metadata, b_head);
    let mut diff = SchemaDiff {
        a_schema_id: head_schema_id(metadata, a_head),
        b_schema_id: head_schema_id(metadata, b_head),
        ..SchemaDiff::default()
    };
    for (id, a_name, a_type) in &a_fields {
        match b_fields.iter().find(|(b_id, _, _)| b_id == id) {
            None => diff.added.push((a_name.clone(), a_type.clone())),
            Some((_, b_name, b_type)) => {
                if b_name != a_name {
                    diff.renamed.push((b_name.clone(), a_name.clone()));
                }
                if b_type != a_type {
                    diff.retyped
                        .push((a_name.clone(), b_type.clone(), a_type.clone()));
                }
            }
        }
    }
    for (id, b_name, b_type) in &b_fields {
        if !a_fields.iter().any(|(a_id, _, _)| a_id == id) {
            diff.dropped.push((b_name.clone(), b_type.clone()));
        }
    }
    diff
}

/// `icegres.*` table properties worth surfacing next to a diff (PK and
/// keyed-tail settings change merge/write behavior). Watermark bookkeeping
/// (`icegres.tail-seq.*`) is noise and filtered. NOTE: Iceberg table
/// properties are TABLE-scoped, not branch-scoped — both sides of a diff
/// share them; they are context, not a delta.
fn notable_properties(metadata: &TableMetadata) -> Vec<(String, String)> {
    let mut props: Vec<(String, String)> = metadata
        .properties()
        .iter()
        .filter(|(k, _)| k.starts_with("icegres.") && !k.starts_with("icegres.tail-seq."))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect();
    props.sort();
    props
}

/// Compare refs `a` and `b` of one table from its metadata (one read).
fn compare_table(metadata: &TableMetadata, ident: &TableIdent, a: &str, b: &str) -> TableCmp {
    let a_head = metadata.snapshot_for_ref(a).map(|s| s.snapshot_id());
    let b_head = metadata.snapshot_for_ref(b).map(|s| s.snapshot_id());
    let parent_of = |id: i64| metadata.snapshot_by_id(id).map(|s| s.parent_snapshot_id());
    let fork = match (a_head, b_head) {
        (Some(ah), Some(bh)) => fork_point(ah, bh, &parent_of),
        _ => None,
    };
    let status = classify(a_head, b_head, fork);
    let schema = match (a_head, b_head) {
        (Some(ah), Some(bh)) if ah != bh => schema_diff(metadata, ah, bh),
        _ => SchemaDiff::default(),
    };
    TableCmp {
        ident: ident.clone(),
        uuid: metadata.uuid(),
        a_head,
        b_head,
        fork,
        status,
        a_side: side_delta(metadata, a_head, fork),
        b_side: side_delta(metadata, b_head, fork),
        schema,
        notable_props: notable_properties(metadata),
    }
}

/// Load and compare every table in scope (`--table` narrows to one).
async fn compare_all(
    engine: &OverwriteEngine,
    a: &str,
    b: &str,
    table: Option<&str>,
) -> Result<Vec<TableCmp>> {
    let idents = match table {
        Some(t) => vec![parse_table(t)?],
        None => engine.list_all_tables().await?,
    };
    anyhow::ensure!(!idents.is_empty(), "the catalog has no tables to compare");
    let mut cmps = Vec::with_capacity(idents.len());
    for ident in &idents {
        let table = engine
            .catalog()
            .load_table(ident)
            .await
            .map_err(|e| anyhow::anyhow!("failed to load table {ident}: {e}"))?;
        cmps.push(compare_table(table.metadata(), ident, a, b));
    }
    // Every ref named on the command line must exist SOMEWHERE, or the
    // command is comparing a typo against reality.
    for (name, present) in [
        (a, cmps.iter().any(|c| c.a_head.is_some())),
        (b, cmps.iter().any(|c| c.b_head.is_some())),
    ] {
        if !present {
            bail!("ref {name:?} does not exist on any table in scope");
        }
    }
    Ok(cmps)
}

/// Render one side's aggregate as a compact human fragment.
fn fmt_side(side: &SideDelta) -> String {
    let mut s = format!(
        "+{} snapshot{}, rows +{}/-{}",
        side.snapshots.len(),
        if side.snapshots.len() == 1 { "" } else { "s" },
        side.rows_added,
        side.rows_deleted
    );
    if side.summary_missing > 0 {
        s.push_str(&format!(
            " ({} snapshot(s) without summary counts)",
            side.summary_missing
        ));
    }
    s
}

fn fmt_head(head: Option<i64>) -> String {
    head.map(|h| h.to_string()).unwrap_or_else(|| "-".into())
}

/// JSON shape of one side (heads/deltas; lineage only in detail mode).
fn side_json(head: Option<i64>, side: &SideDelta, detail: bool) -> serde_json::Value {
    let mut v = serde_json::json!({
        "head": head,
        "snapshots": side.snapshots.len(),
        "rows_added": side.rows_added,
        "rows_deleted": side.rows_deleted,
        "rows_source": "summary-reported",
        "snapshots_without_summary_counts": side.summary_missing,
    });
    if detail {
        v["lineage"] = serde_json::Value::Array(
            side.snapshots
                .iter()
                .map(|s| {
                    serde_json::json!({
                        "snapshot_id": s.id,
                        "timestamp_ms": s.timestamp_ms,
                        "operation": s.operation,
                        "added_records": s.added_records,
                        "deleted_records": s.deleted_records,
                    })
                })
                .collect(),
        );
    }
    v
}

fn schema_json(schema: &SchemaDiff) -> serde_json::Value {
    serde_json::json!({
        "a_schema_id": schema.a_schema_id,
        "b_schema_id": schema.b_schema_id,
        "added": schema.added.iter()
            .map(|(n, t)| serde_json::json!({"column": n, "type": t})).collect::<Vec<_>>(),
        "dropped": schema.dropped.iter()
            .map(|(n, t)| serde_json::json!({"column": n, "type": t})).collect::<Vec<_>>(),
        "renamed": schema.renamed.iter()
            .map(|(from, to)| serde_json::json!({"from": from, "to": to})).collect::<Vec<_>>(),
        "retyped": schema.retyped.iter()
            .map(|(n, from, to)| serde_json::json!({"column": n, "from": from, "to": to}))
            .collect::<Vec<_>>(),
    })
}

/// `icegres branch diff <a> <b> [--table ns.t] [--json]` — metadata-only
/// per-table comparison of two refs (see the section comment above).
pub async fn diff(
    opts: &CatalogOpts,
    a: &str,
    b: &str,
    table: Option<&str>,
    json: bool,
) -> Result<()> {
    validate_name(a)?;
    validate_name(b)?;
    anyhow::ensure!(a != b, "diffing a ref against itself is always empty");
    let engine = engine(opts).await?;
    let detail = table.is_some();
    let cmps = compare_all(&engine, a, b, table).await?;

    if json {
        let tables: Vec<serde_json::Value> = cmps
            .iter()
            .map(|c| {
                let mut v = serde_json::json!({
                    "table": c.ident.to_string(),
                    "status": c.status.label(),
                    "fork_snapshot_id": c.fork,
                    "a": side_json(c.a_head, &c.a_side, detail),
                    "b": side_json(c.b_head, &c.b_side, detail),
                    "notable_properties": c.notable_props.iter().cloned()
                        .collect::<std::collections::BTreeMap<String, String>>(),
                });
                if !c.schema.is_empty() {
                    v["schema"] = schema_json(&c.schema);
                }
                v
            })
            .collect();
        println!(
            "{}",
            serde_json::to_string_pretty(&serde_json::json!({
                "a": a,
                "b": b,
                "tables": tables,
            }))?
        );
        return Ok(());
    }

    println!("branch diff {a} (a) vs {b} (b) — metadata-only; row deltas are summary-reported");
    for c in &cmps {
        match c.status {
            DiffStatus::Unchanged => {
                println!("{}\tunchanged\thead {}", c.ident, fmt_head(c.a_head));
            }
            DiffStatus::NoRef => {
                println!(
                    "{}\tno-ref\tneither ref exists (unbranched or snapshot-less)",
                    c.ident
                );
            }
            DiffStatus::OnlyA => {
                println!(
                    "{}\tcreated\tonly on {a}: head {} ({})",
                    c.ident,
                    fmt_head(c.a_head),
                    fmt_side(&c.a_side)
                );
            }
            DiffStatus::OnlyB => {
                println!(
                    "{}\tdropped\tonly on {b}: head {} ({})",
                    c.ident,
                    fmt_head(c.b_head),
                    fmt_side(&c.b_side)
                );
            }
            DiffStatus::AdvancedA | DiffStatus::AdvancedB | DiffStatus::Diverged => {
                let fork = c
                    .fork
                    .map(|f| f.to_string())
                    .unwrap_or_else(|| "unknown (history expired)".into());
                println!(
                    "{}\t{}\tfork {fork} | a {}: {} | b {}: {}",
                    c.ident,
                    c.status.label(),
                    fmt_head(c.a_head),
                    fmt_side(&c.a_side),
                    fmt_head(c.b_head),
                    fmt_side(&c.b_side)
                );
            }
        }
        if !c.schema.is_empty() {
            let s = &c.schema;
            let mut parts: Vec<String> = Vec::new();
            for (n, t) in &s.added {
                parts.push(format!("+{n} {t}"));
            }
            for (n, t) in &s.dropped {
                parts.push(format!("-{n} {t}"));
            }
            for (from, to) in &s.renamed {
                parts.push(format!("{from}->{to}"));
            }
            for (n, from, to) in &s.retyped {
                parts.push(format!("{n}: {from}->{to}"));
            }
            println!(
                "  schema: a id {} vs b id {}: {}",
                s.a_schema_id,
                s.b_schema_id,
                parts.join(", ")
            );
        }
        if detail {
            if !c.notable_props.is_empty() {
                let props: Vec<String> = c
                    .notable_props
                    .iter()
                    .map(|(k, v)| format!("{k}={v}"))
                    .collect();
                println!(
                    "  properties (table-scoped, shared by both sides): {}",
                    props.join(", ")
                );
            }
            for (label, side) in [("a", &c.a_side), ("b", &c.b_side)] {
                for s in &side.snapshots {
                    println!(
                        "  {label} snapshot {} at {} ({}, +{}/-{} rows)",
                        s.id,
                        s.timestamp_ms,
                        s.operation,
                        s.added_records.map_or("?".into(), |n| n.to_string()),
                        s.deleted_records.map_or("?".into(), |n| n.to_string()),
                    );
                }
            }
        }
    }
    Ok(())
}

/// What `merge` decided for one table.
enum MergePlan {
    /// `to` still sits at the fork point: fast-forward it to `from`'s head.
    FastForward(FastForwardMove),
    /// Nothing to do (heads equal, `to` already ahead, or no `from` ref).
    Skip(TableIdent, String),
    /// The table cannot fast-forward; carries the human conflict report.
    Conflict(TableIdent, String),
}

/// Plan the merge from a comparison (pure; unit-tested). `from` maps to
/// side `a`, `to` to side `b`.
fn plan_merge(c: &TableCmp) -> MergePlan {
    match c.status {
        DiffStatus::AdvancedA => MergePlan::FastForward(FastForwardMove {
            ident: c.ident.clone(),
            uuid: c.uuid,
            from_head: c.a_head.expect("advanced-a implies both heads"),
            to_head: c.b_head.expect("advanced-a implies both heads"),
        }),
        DiffStatus::Unchanged => MergePlan::Skip(c.ident.clone(), "already up to date".into()),
        DiffStatus::AdvancedB => MergePlan::Skip(
            c.ident.clone(),
            "target is ahead of the source (nothing to merge)".into(),
        ),
        DiffStatus::NoRef | DiffStatus::OnlyB => MergePlan::Skip(
            c.ident.clone(),
            "source ref absent on this table (nothing to merge)".into(),
        ),
        DiffStatus::OnlyA => MergePlan::Conflict(
            c.ident.clone(),
            format!(
                "target ref is missing on this table (source head {}); create it first \
                 (icegres branch create {} <to> --at-snapshot <fork>) or re-branch",
                fmt_head(c.a_head),
                c.ident
            ),
        ),
        DiffStatus::Diverged => MergePlan::Conflict(
            c.ident.clone(),
            format!(
                "diverged: fork {}, source head {} ({}), target head {} ({}); a \
                 fast-forward is impossible — rebase by re-branching from the target \
                 (drop the source branch and `icegres branch create-all` again), or \
                 narrow with --table to merge the clean tables",
                c.fork
                    .map(|f| f.to_string())
                    .unwrap_or_else(|| "unknown (history expired)".into()),
                fmt_head(c.a_head),
                fmt_side(&c.a_side),
                fmt_head(c.b_head),
                fmt_side(&c.b_side),
            ),
        ),
    }
}

/// `icegres branch merge <from> <to> [--table ns.t] [--execute]` —
/// fast-forward-only merge (see the section comment above; three-way row
/// merges are refused by design: the operator rebases by re-branching).
/// Dry-run by default; `--execute` commits the whole eligible set in ONE
/// atomic multi-table transaction with the observed to/from heads pinned
/// as requirements, so a racing foreign commit aborts it cleanly with
/// nothing applied. ANY conflicted table refuses the whole run (narrow
/// with `--table` to merge the clean ones).
pub async fn merge(
    opts: &CatalogOpts,
    from: &str,
    to: &str,
    table: Option<&str>,
    execute: bool,
) -> Result<()> {
    validate_name(from)?;
    validate_name(to)?;
    anyhow::ensure!(from != to, "merging a ref into itself is a no-op");
    let engine = engine(opts).await?;
    let cmps = compare_all(&engine, from, to, table).await?;

    let mut moves: Vec<FastForwardMove> = Vec::new();
    let mut conflicts: Vec<(TableIdent, String)> = Vec::new();
    for c in &cmps {
        match plan_merge(c) {
            MergePlan::FastForward(mv) => {
                println!(
                    "{}: fast-forward {to} {} -> {} ({})",
                    mv.ident,
                    mv.to_head,
                    mv.from_head,
                    fmt_side(&c.a_side)
                );
                moves.push(mv);
            }
            MergePlan::Skip(ident, why) => println!("{ident}: skip — {why}"),
            MergePlan::Conflict(ident, report) => {
                println!("{ident}: CONFLICT — {report}");
                conflicts.push((ident, report));
            }
        }
    }
    if !conflicts.is_empty() {
        bail!(
            "refusing the merge: {} table(s) cannot fast-forward (icegres never \
             three-way-merges rows — rebase by re-branching, or narrow with --table); \
             nothing was applied",
            conflicts.len()
        );
    }
    if moves.is_empty() {
        println!("nothing to fast-forward: {to} already contains {from} everywhere in scope");
        return Ok(());
    }
    if !execute {
        println!(
            "dry run: {} table(s) would fast-forward atomically; nothing was committed \
             (re-run with --execute)",
            moves.len()
        );
        return Ok(());
    }
    engine.merge_fast_forward_all(from, to, &moves).await?;
    println!(
        "merged {from} into {to}: {} table(s) fast-forwarded in ONE atomic commit \
         (observed heads pinned; zero-copy ref moves). Operational rule: quiesce \
         writers committing to {to} before merging into it, as with any Iceberg \
         ref surgery.",
        moves.len()
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

    /// Build a parent-lookup closure from `(id, parent)` pairs; ids absent
    /// from the list model snapshots expired out of metadata.
    fn parents(pairs: &[(i64, Option<i64>)]) -> impl Fn(i64) -> Option<Option<i64>> + '_ {
        move |id| pairs.iter().find(|(i, _)| *i == id).map(|(_, p)| *p)
    }

    #[test]
    fn fork_point_walks_to_the_common_ancestor() {
        // 1 -> 2 -> 3 (fork) -> 4a -> 5a  and  3 -> 4b
        let p = parents(&[
            (1, None),
            (2, Some(1)),
            (3, Some(2)),
            (40, Some(3)),
            (50, Some(40)),
            (41, Some(3)),
        ]);
        assert_eq!(fork_point(50, 41, &p), Some(3));
        assert_eq!(fork_point(41, 50, &p), Some(3));
        // One head IS the fork.
        assert_eq!(fork_point(50, 3, &p), Some(3));
        assert_eq!(fork_point(3, 41, &p), Some(3));
        // Same head.
        assert_eq!(fork_point(50, 50, &p), Some(50));
    }

    #[test]
    fn fork_point_expired_history_is_none() {
        // b's chain is truncated (its parent 99 is not in metadata anymore).
        let p = parents(&[(1, None), (2, Some(1)), (7, Some(99))]);
        assert_eq!(fork_point(2, 7, &p), None);
        // A cyclic (corrupt) chain terminates instead of looping.
        let cyclic = parents(&[(1, Some(2)), (2, Some(1)), (3, None)]);
        assert_eq!(fork_point(1, 3, &cyclic), None);
    }

    #[test]
    fn classify_matrix() {
        use DiffStatus::*;
        assert_eq!(classify(None, None, None), NoRef);
        assert_eq!(classify(Some(5), None, None), OnlyA);
        assert_eq!(classify(None, Some(5), None), OnlyB);
        assert_eq!(classify(Some(5), Some(5), Some(5)), Unchanged);
        // a moved, b at the fork -> fast-forwardable.
        assert_eq!(classify(Some(9), Some(3), Some(3)), AdvancedA);
        // b moved, a at the fork.
        assert_eq!(classify(Some(3), Some(9), Some(3)), AdvancedB);
        // both moved.
        assert_eq!(classify(Some(9), Some(8), Some(3)), Diverged);
        // fork unknown (expired history) -> diverged, never guessed.
        assert_eq!(classify(Some(9), Some(8), None), Diverged);
    }

    fn cmp_with(status: DiffStatus, a: Option<i64>, b: Option<i64>, fork: Option<i64>) -> TableCmp {
        TableCmp {
            ident: parse_table("demo.t").unwrap(),
            uuid: uuid::Uuid::nil(),
            a_head: a,
            b_head: b,
            fork,
            status,
            a_side: SideDelta::default(),
            b_side: SideDelta::default(),
            schema: SchemaDiff::default(),
            notable_props: Vec::new(),
        }
    }

    #[test]
    fn merge_plan_eligibility_matrix() {
        // Fast-forward iff to's head == fork (advanced-a): the move pins
        // both observed heads.
        match plan_merge(&cmp_with(DiffStatus::AdvancedA, Some(9), Some(3), Some(3))) {
            MergePlan::FastForward(mv) => {
                assert_eq!(mv.from_head, 9);
                assert_eq!(mv.to_head, 3);
            }
            _ => panic!("advanced-a must fast-forward"),
        }
        assert!(matches!(
            plan_merge(&cmp_with(DiffStatus::Unchanged, Some(3), Some(3), Some(3))),
            MergePlan::Skip(..)
        ));
        assert!(matches!(
            plan_merge(&cmp_with(DiffStatus::AdvancedB, Some(3), Some(9), Some(3))),
            MergePlan::Skip(..)
        ));
        assert!(matches!(
            plan_merge(&cmp_with(DiffStatus::OnlyB, None, Some(9), None)),
            MergePlan::Skip(..)
        ));
        assert!(matches!(
            plan_merge(&cmp_with(DiffStatus::NoRef, None, None, None)),
            MergePlan::Skip(..)
        ));
        // Divergence and a missing target ref refuse (no three-way merge).
        assert!(matches!(
            plan_merge(&cmp_with(DiffStatus::Diverged, Some(9), Some(8), Some(3))),
            MergePlan::Conflict(..)
        ));
        assert!(matches!(
            plan_merge(&cmp_with(DiffStatus::OnlyA, Some(9), None, None)),
            MergePlan::Conflict(..)
        ));
    }
}
