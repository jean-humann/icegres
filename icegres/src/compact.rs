//! `icegres maintain compact` — bin-pack small-file compaction (roadmap-v2
//! §P2, re-scoped: docs/p2-matrix-bump-scope.md).
//!
//! Long-lived tables written one small commit at a time (per-statement
//! INSERTs, foreign micro-batch writers) fragment into many under-sized
//! Parquet files, and every extra file costs a scan an object-store round
//! trip. Compaction rewrites each partition's under-target files into
//! ~target-size files as ONE `replace` snapshot — the ROW SET is identical,
//! only the physical layout changes:
//!
//! ```text
//! icegres maintain compact --table demo.trips                # dry run (plan)
//! icegres maintain compact --table demo.trips --execute      # rewrite+commit
//! icegres maintain compact --table demo.trips --target-file-mb 256 \
//!     --min-input-files 4 --execute
//! ```
//!
//! iceberg-rust 0.10.0 has no rewrite-files transaction action, so the
//! snapshot is produced by the same hand-built machinery every icegres
//! write uses (overwrite.rs): manifests holding a compacted input (or a
//! stale DELETED entry — spec: those live only in the snapshot that deleted
//! them) are rewritten with EXISTING (kept) + DELETED (compacted, original
//! sequence numbers) entries, fully-live untouched manifests are carried
//! as-is, one ADDED manifest names the combined outputs, and the
//! `Operation::Replace` snapshot is committed over the Iceberg REST
//! protocol. Snapshot lineage is preserved: the old
//! files stay reachable through every pre-compact snapshot (time travel
//! keeps working) until `expire-snapshots` + `remove-orphans` reclaim them
//! — the orphan GC's live set includes DELETED manifest entries, so a
//! compacted-away file is NEVER an orphan while any retained snapshot still
//! names it.
//!
//! # Safety rails (each fails closed; nothing is ever rewritten in place)
//!
//! * **Dry-run by default.** Without `--execute` the plan is printed
//!   (candidates per partition, projected outputs) and NOTHING is written.
//! * **First-committer-wins.** The commit carries `assert-table-uuid` +
//!   `assert-ref-snapshot-id main=<head the plan was computed against>`.
//!   Any writer landing in between — a foreign engine, a concurrent icegres
//!   DML, a buffered server's flush — makes the catalog answer 409 and the
//!   compact aborts cleanly with nothing changed (re-run it). There is NO
//!   retry: retrying automatically would re-plan against rows the operator
//!   never saw. This anchor is also the buffer coordination: a serving
//!   endpoint's in-flight flush of the same table either lands first (we
//!   409 and abort) or lands after (it anchors to OUR new head) — the two
//!   commits can never interleave, and buffered-but-unflushed rows live
//!   outside every snapshot, out of compaction's reach by construction.
//! * **Delete manifests refuse loudly.** A table bearing merge-on-read
//!   deletes (foreign-written deletion vectors / position deletes) is
//!   refused: icegres cannot apply those deletes (upstream iceberg-rust has
//!   no puffin-DV read path), so rewriting data files under them would
//!   corrupt row semantics.
//! * **Partitioned tables refuse loudly.** The planner never combines
//!   files across partitions, but the icegres write stack is
//!   unpartitioned-only (see overwrite.rs), so rewriting partitioned
//!   layouts is refused rather than guessed at. Same for historical
//!   partition-spec ids and non-v2 format versions.
//! * **Schema-divergent files refuse loudly.** The rewrite reads Parquet
//!   raw and aligns each batch to the current schema by position +
//!   case-insensitive name (overwrite::align_batch — same-schema type
//!   widening only), with NO field-id projection. On a file physically
//!   written under an older schema version that mapping could silently
//!   resurrect a dropped column's values under a re-added name of the
//!   same shape. Two rails close that, both fail-closed and both running
//!   strictly before anything is staged. The GUARANTEE is per data FILE:
//!   every candidate input's Parquet FOOTER is read (no row group is
//!   fetched) and each column's embedded field id (`PARQUET:field_id`) is
//!   verified against the current schema — a mismatched OR missing field
//!   id refuses the whole run. A manifest stamped with a non-current
//!   schema id also refuses, but only as a cheap FAST PATH: a manifest's
//!   schema id records the manifest WRITER's schema, not the schema its
//!   listed files were encoded under, and any post-evolution manifest
//!   rewrite (Spark rewrite_manifests, foreign copy-on-write DML,
//!   icegres's own m0 carrying untouched files as EXISTING) re-stamps
//!   old-schema files under the current id — so the manifest check alone
//!   is NOT sufficient. On refusal: rewrite the old files under the
//!   current schema (full-table rewrite), or wait for field-id-aware
//!   compaction.
//! * **Row-count identity is asserted** before the commit is posted: the
//!   rewritten outputs must carry exactly the input row count, or the run
//!   aborts with every new file left as an unreferenced orphan (standard
//!   Iceberg failed-commit semantics — harmless, reclaimed by GC age-out).
//!
//! Setting `ICEGRES_COMPACT_INJECT_CONFLICT=1` (test-only knob, same
//! pattern as `ICEGRES_DML_INJECT_CONFLICT`) corrupts the commit's
//! `assert-ref-snapshot-id` requirement so the catalog rejects it with 409,
//! proving the clean conflict abort end to end (icegres/tests/e2e.sh).

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Result};
use arrow::datatypes::{Field as ArrowField, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use datafusion::parquet::arrow::PARQUET_FIELD_ID_META_KEY;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::{
    DataFile, DataFileFormat, FormatVersion, ManifestContentType, ManifestFile, ManifestListWriter,
    Operation, Snapshot, SnapshotReference, SnapshotRetention, Summary, MAIN_BRANCH,
    UNASSIGNED_SEQUENCE_NUMBER,
};
use iceberg::writer::IcebergWriter as _;
use iceberg::{TableRequirement, TableUpdate};
use iceberg_catalog_rest::CommitTableRequest;
use uuid::Uuid;

use crate::branch::parse_table;
use crate::context;
use crate::overwrite::{self, CommitOutcome, OverwriteEngine, PreparedCommit};
use crate::CatalogOpts;

/// One live data file offered to the planner.
#[derive(Debug, Clone)]
pub struct CompactCandidate {
    /// Object-store path of the data file (plan printing / debugging).
    pub path: String,
    pub size_bytes: u64,
    pub record_count: u64,
    /// Canonical partition key (`""` on unpartitioned tables). Files with
    /// different keys are NEVER combined — bin-pack must respect the
    /// partition spec.
    pub partition: String,
}

/// One planned rewrite group: a partition's under-target files, to be
/// stream-read and rewritten together into ~target-size output file(s).
#[derive(Debug)]
pub struct CompactGroup {
    pub partition: String,
    /// Indices into the planner's input slice, in input order.
    pub inputs: Vec<usize>,
    pub total_bytes: u64,
    pub total_records: u64,
}

impl CompactGroup {
    /// Projected output file count: the rolling writer splits at the
    /// target size, so ~ceil(total/target) files come out (approximate —
    /// re-encoding changes the byte size; never less than 1).
    pub fn projected_outputs(&self, target_bytes: u64) -> u64 {
        self.total_bytes.div_ceil(target_bytes).max(1)
    }
}

/// The pure bin-pack planner. Selection: a file is a candidate iff its
/// size is UNDER `target_bytes` (files at or above target are already
/// well-sized and are never touched). Grouping: candidates are grouped by
/// partition key, in first-seen order, and a partition is planned only
/// when it has at least `min_input_files` candidates — rewriting fewer
/// than 2 files cannot reduce the file count, and the threshold lets
/// operators demand a bigger payoff per rewrite.
pub fn plan_compaction(
    files: &[CompactCandidate],
    target_bytes: u64,
    min_input_files: usize,
) -> Vec<CompactGroup> {
    // First-seen partition order keeps plans deterministic for a given
    // manifest walk (and for the unit tests).
    let mut groups: Vec<CompactGroup> = Vec::new();
    for (idx, f) in files.iter().enumerate() {
        if f.size_bytes >= target_bytes {
            continue;
        }
        let group = match groups.iter_mut().find(|g| g.partition == f.partition) {
            Some(g) => g,
            None => {
                groups.push(CompactGroup {
                    partition: f.partition.clone(),
                    inputs: Vec::new(),
                    total_bytes: 0,
                    total_records: 0,
                });
                groups.last_mut().expect("just pushed")
            }
        };
        group.inputs.push(idx);
        group.total_bytes += f.size_bytes;
        group.total_records += f.record_count;
    }
    groups.retain(|g| g.inputs.len() >= min_input_files);
    groups
}

/// One live manifest entry, with everything a rewritten manifest needs to
/// carry it forward (EXISTING) or record its removal (DELETED) with the
/// ORIGINAL sequence numbers — exactly what prepare_commit records.
struct LiveEntry {
    manifest_idx: usize,
    data_file: DataFile,
    snapshot_id: i64,
    data_seq: i64,
    file_seq: Option<i64>,
}

/// `icegres maintain compact --table <t> [--target-file-mb N]
/// [--min-input-files N] [--execute]`
///
/// Dry-run by default: prints the plan and rewrites nothing. See the
/// module docs for the exact semantics and safety rails.
pub async fn run(
    opts: &CatalogOpts,
    table: &str,
    target_file_mb: u64,
    min_input_files: usize,
    execute: bool,
) -> Result<()> {
    anyhow::ensure!(
        target_file_mb >= 1,
        "--target-file-mb must be at least 1 (got {target_file_mb})"
    );
    // Rewriting fewer than 2 files cannot reduce the file count — it would
    // be pure churn (one new file per old file). Refuse loudly.
    anyhow::ensure!(
        min_input_files >= 2,
        "--min-input-files must be at least 2 (got {min_input_files}): rewriting fewer \
         than 2 files cannot reduce the file count"
    );
    let target_bytes = target_file_mb.saturating_mul(1024 * 1024);

    let ident = parse_table(table)?;
    let catalog = context::connect_catalog(opts).await?;
    let engine = OverwriteEngine::connect(catalog.clone(), opts, false, None).await?;

    // The plan and the commit anchor come from ONE metadata load: the
    // commit's `assert-ref-snapshot-id main=<this head>` requirement makes
    // any interleaving writer a clean 409 abort (first-committer-wins).
    let tbl = catalog
        .load_table(&ident)
        .await
        .map_err(|e| anyhow!("failed to load table {ident}: {e}"))?;
    let metadata = tbl.metadata();

    // ---- Guard rails (fail closed, mirroring the write engine's). ----
    if metadata.format_version() != FormatVersion::V2 {
        bail!(
            "refusing to compact {ident}: compaction requires an Iceberg format v2 table \
             (found {:?}); nothing was rewritten",
            metadata.format_version()
        );
    }
    if !metadata.default_partition_spec().is_unpartitioned() {
        bail!(
            "refusing to compact {ident}: the table is partitioned and the icegres write \
             stack is unpartitioned-only (see overwrite.rs) — rewriting partitioned \
             layouts is not supported yet; nothing was rewritten"
        );
    }
    let Some(head) = metadata.current_snapshot() else {
        println!("table {ident} has no snapshot yet; nothing to compact");
        return Ok(());
    };
    let head_id = head.snapshot_id();
    let file_io = tbl.file_io();

    let manifest_list = overwrite::load_manifest_list(file_io, head, &tbl.metadata_ref())
        .await
        .map_err(|e| anyhow!("failed to load manifest list of {ident}: {e}"))?;
    for mf in manifest_list.entries() {
        // SAFETY: merge-on-read deletes (foreign-written deletion vectors /
        // position deletes) cannot be applied by icegres — compacting data
        // files underneath them would corrupt row semantics. Refuse loudly.
        if mf.content != ManifestContentType::Data {
            bail!(
                "refusing to compact {ident}: the table has delete manifests \
                 (merge-on-read deletes written by a foreign engine), and icegres cannot \
                 apply those deletes — compacting under them would corrupt row semantics. \
                 Nothing was rewritten"
            );
        }
        // SAFETY: entries copied into a rewritten manifest are interpreted
        // under the manifest's partition spec; a historical spec id means
        // layouts this build cannot re-encode faithfully.
        if mf.partition_spec_id != metadata.default_partition_spec_id() {
            bail!(
                "refusing to compact {ident}: manifest {} was written under partition \
                 spec {} (current default is {}) — historical partition specs are not \
                 supported; nothing was rewritten",
                mf.manifest_path,
                mf.partition_spec_id,
                metadata.default_partition_spec_id()
            );
        }
    }

    // ---- Walk every manifest, collecting live entries. ----
    let mut live: Vec<LiveEntry> = Vec::new();
    // Per manifest: (live entry count, carries a stale DELETED entry).
    let mut manifest_stats: Vec<(usize, bool)> = vec![(0, false); manifest_list.entries().len()];
    for (manifest_idx, manifest_file) in manifest_list.entries().iter().enumerate() {
        let manifest = manifest_file.load_manifest(file_io).await.map_err(|e| {
            anyhow!(
                "failed to load manifest {}: {e} — an unreadable manifest must never \
                 drive a rewrite; nothing was rewritten",
                manifest_file.manifest_path
            )
        })?;
        // SAFETY: the manifest-level schema FAST PATH — divergence is only
        // visible here (the manifest LIST entry carries no schema id), so
        // this rail cannot live with the list-level guards above. It is a
        // pre-filter only: the per-file field-id verification below the
        // planner is what actually guarantees the inputs' write schema.
        ensure_manifest_schema_current(
            &ident.to_string(),
            &manifest_file.manifest_path,
            manifest.metadata().schema_id(),
            metadata.current_schema_id(),
        )?;
        let (entries, _meta) = manifest.into_parts();
        for entry in &entries {
            if !entry.is_alive() {
                // A DELETED entry from an earlier snapshot: not part of the
                // live row set (its manifest gets rewritten without it,
                // exactly as prepare_commit does — spec: DELETED entries
                // live only in the snapshot that deleted them, and leaving
                // them behind would pin long-replaced files in the orphan
                // GC's live set forever).
                manifest_stats[manifest_idx].1 = true;
                continue;
            }
            manifest_stats[manifest_idx].0 += 1;
            live.push(LiveEntry {
                manifest_idx,
                data_file: entry.data_file().clone(),
                snapshot_id: entry
                    .snapshot_id()
                    .unwrap_or(manifest_file.added_snapshot_id),
                data_seq: entry
                    .sequence_number()
                    .unwrap_or(manifest_file.sequence_number),
                file_seq: entry.file_sequence_number,
            });
        }
    }

    // ---- Plan. ----
    let candidates: Vec<CompactCandidate> = live
        .iter()
        .map(|e| CompactCandidate {
            path: e.data_file.file_path().to_string(),
            size_bytes: e.data_file.file_size_in_bytes(),
            record_count: e.data_file.record_count(),
            partition: partition_key(e.data_file.partition()),
        })
        .collect();
    let groups = plan_compaction(&candidates, target_bytes, min_input_files);

    let live_files = live.len();
    let partitions: HashSet<&str> = candidates.iter().map(|c| c.partition.as_str()).collect();
    println!(
        "table {ident}: {live_files} live data file(s) across {} partition(s) at snapshot \
         {head_id}",
        partitions.len().max(1)
    );
    if groups.is_empty() {
        println!(
            "nothing to compact (no partition has {min_input_files}+ data files under \
             {target_file_mb} MiB)"
        );
        return Ok(());
    }

    // ---- Per-file schema verification (fail closed — the GUARANTEE). ----
    // The manifest-level fast path above only proves who WROTE each
    // manifest: any post-evolution manifest rewrite re-stamps old-schema
    // files under the current schema id. So, before anything is staged —
    // and before the plan is even printed, exactly like the other refusal
    // rails — read every candidate input's Parquet FOOTER (two ranged
    // reads, no row group is fetched or decoded) and verify each column's
    // embedded field id against the table's current schema. Both dry runs
    // and --execute refuse here.
    let schema = metadata.current_schema();
    let arrow_target: ArrowSchemaRef = Arc::new(
        schema_to_arrow_schema(schema).map_err(|e| anyhow!("schema conversion failed: {e}"))?,
    );
    for g in &groups {
        for idx in &g.inputs {
            let data_file = &live[*idx].data_file;
            if data_file.file_format() != DataFileFormat::Parquet {
                bail!(
                    "refusing to compact {ident}: data file {} is not Parquet ({:?}), so \
                     its write schema cannot be verified; nothing was rewritten",
                    data_file.file_path(),
                    data_file.file_format()
                );
            }
            let file_schema =
                overwrite::read_parquet_arrow_schema(file_io, data_file.file_path()).await?;
            ensure_file_schema_current(
                &ident.to_string(),
                data_file.file_path(),
                &file_schema,
                &arrow_target,
            )?;
        }
    }

    for g in &groups {
        let label = if g.partition.is_empty() {
            "<unpartitioned>"
        } else {
            g.partition.as_str()
        };
        println!(
            "partition {label}: {} file(s), {} bytes, {} row(s) -> ~{} output file(s) \
             (target {target_file_mb} MiB)",
            g.inputs.len(),
            g.total_bytes,
            g.total_records,
            g.projected_outputs(target_bytes)
        );
        for idx in g.inputs.iter().take(20) {
            println!(
                "  {} ({} bytes)",
                candidates[*idx].path, candidates[*idx].size_bytes
            );
        }
        if g.inputs.len() > 20 {
            println!("  ... and {} more", g.inputs.len() - 20);
        }
    }
    let input_files: usize = groups.iter().map(|g| g.inputs.len()).sum();
    let input_bytes: u64 = groups.iter().map(|g| g.total_bytes).sum();
    let input_records: u64 = groups.iter().map(|g| g.total_records).sum();
    if !execute {
        println!(
            "plan: rewrite {input_files} file(s) totaling {input_bytes} bytes \
             ({input_records} row(s)) (DRY RUN — nothing rewritten; re-run with \
             --execute to compact)"
        );
        return Ok(());
    }

    // ---- Execute: stream-read the inputs, rewrite combined outputs. ----
    let commit_uuid = Uuid::new_v4();
    let mut added_files: Vec<DataFile> = Vec::new();
    for g in &groups {
        // One rolling writer per partition group; files are read one at a
        // time (peak memory = one input file's decoded batches, the same
        // bound as the DML engine). Every input's Parquet footer was
        // verified to carry exactly the current schema's field ids (the
        // per-file guard above refused anything else — the manifest-level
        // check alone would not be enough), so align_batch's position+name
        // mapping is sound here: it only casts physical types (e.g.
        // same-schema widening) onto the canonical field-id-annotated
        // Arrow shape.
        let mut writer =
            overwrite::new_compact_data_writer(&tbl, &commit_uuid, target_bytes as usize).await?;
        for idx in &g.inputs {
            let batches = overwrite::read_parquet_file(file_io, &live[*idx].data_file).await?;
            for batch in batches {
                let aligned = overwrite::align_batch(&batch, &arrow_target)?;
                writer
                    .write(aligned)
                    .await
                    .map_err(|e| anyhow!("failed to write compacted rows: {e}"))?;
            }
        }
        added_files.extend(
            writer
                .close()
                .await
                .map_err(|e| anyhow!("failed to close compaction data writer: {e}"))?,
        );
    }
    // SAFETY: row-count identity, asserted BEFORE anything is committed. On
    // failure the new files stay unreferenced (harmless orphans, reclaimed
    // by GC age-out) and the table is untouched.
    let output_records: u64 = added_files.iter().map(|f| f.record_count()).sum();
    if output_records != input_records {
        bail!(
            "row-count mismatch compacting {ident}: {input_records} row(s) in, \
             {output_records} out — refusing to commit (nothing was changed; the \
             partial output files are unreferenced orphans)"
        );
    }

    // ---- Snapshot production (mirrors overwrite.rs prepare_commit). ----
    let compacted: HashSet<usize> = groups
        .iter()
        .flat_map(|g| g.inputs.iter().copied())
        .collect();
    // A manifest is REWRITTEN when it holds a compacted input or a stale
    // DELETED entry (prepare_commit's scan-path rule); fully-live untouched
    // manifests are carried as-is, and manifests left with nothing alive
    // are dropped from the new list entirely.
    let mut touched_manifests: HashSet<usize> = compacted
        .iter()
        .map(|idx| live[*idx].manifest_idx)
        .collect();
    for (i, (_, has_stale)) in manifest_stats.iter().enumerate() {
        if *has_stale {
            touched_manifests.insert(i);
        }
    }
    let snapshot_id = overwrite::generate_unique_snapshot_id(&tbl);
    let next_seq = metadata.next_sequence_number();
    let meta_dir = format!("{}/metadata", metadata.location());

    // Untouched, fully-live manifests are carried into the new manifest
    // list as-is; the touched ones collapse into ONE rewritten manifest
    // holding EXISTING (kept) + DELETED (compacted) entries with their
    // original sequence numbers, plus one ADDED manifest for the outputs.
    let mut new_manifests: Vec<ManifestFile> = manifest_list
        .entries()
        .iter()
        .enumerate()
        .filter(|(i, _)| !touched_manifests.contains(i) && manifest_stats[*i].0 > 0)
        .map(|(_, mf)| mf.clone())
        .collect();
    let mut m0 = overwrite::new_manifest_writer(
        &tbl,
        snapshot_id,
        &format!("{meta_dir}/{commit_uuid}-m0.avro"),
    )?;
    let (mut kept_files, mut kept_records, mut kept_bytes) = (0u64, 0u64, 0u64);
    for (idx, entry) in live.iter().enumerate() {
        if !touched_manifests.contains(&entry.manifest_idx) {
            // Carried untouched with its manifest: count it for the exact
            // totals (every live entry was visited in the walk above).
            kept_files += 1;
            kept_records += entry.data_file.record_count();
            kept_bytes += entry.data_file.file_size_in_bytes();
            continue;
        }
        if compacted.contains(&idx) {
            m0.add_delete_file(
                entry.data_file.clone(),
                entry.data_seq,
                entry.file_seq.or(Some(entry.data_seq)),
            )
            .map_err(|e| anyhow!("failed to add deleted file entry: {e}"))?;
        } else {
            kept_files += 1;
            kept_records += entry.data_file.record_count();
            kept_bytes += entry.data_file.file_size_in_bytes();
            m0.add_existing_file(
                entry.data_file.clone(),
                entry.snapshot_id,
                entry.data_seq,
                entry.file_seq.or(Some(entry.data_seq)),
            )
            .map_err(|e| anyhow!("failed to add existing file entry: {e}"))?;
        }
    }
    new_manifests.push(
        m0.write_manifest_file()
            .await
            .map_err(|e| anyhow!("failed to write rewritten manifest: {e}"))?,
    );
    let mut added_bytes = 0u64;
    if !added_files.is_empty() {
        let mut m1 = overwrite::new_manifest_writer(
            &tbl,
            snapshot_id,
            &format!("{meta_dir}/{commit_uuid}-m1.avro"),
        )?;
        for df in &added_files {
            added_bytes += df.file_size_in_bytes();
            m1.add_file(df.clone(), UNASSIGNED_SEQUENCE_NUMBER)
                .map_err(|e| anyhow!("failed to add new file entry: {e}"))?;
        }
        new_manifests.push(
            m1.write_manifest_file()
                .await
                .map_err(|e| anyhow!("failed to write added manifest: {e}"))?,
        );
    }

    let manifest_list_path = format!("{meta_dir}/snap-{snapshot_id}-0-{commit_uuid}.avro");
    let mut list_writer = ManifestListWriter::v2(
        file_io
            .new_output(&manifest_list_path)
            .map_err(|e| anyhow!("failed to open manifest list output: {e}"))?
            .writer()
            .await
            .map_err(|e| anyhow!("failed to open manifest list writer: {e}"))?,
        snapshot_id,
        Some(head_id),
        next_seq,
    );
    list_writer
        .add_manifests(new_manifests.into_iter())
        .map_err(|e| anyhow!("failed to append manifests to manifest list: {e}"))?;
    list_writer
        .close()
        .await
        .map_err(|e| anyhow!("failed to write manifest list: {e}"))?;

    // Snapshot summary: a `replace` snapshot (row set identical, layout
    // changed). Totals are EXACT — every live entry was visited above.
    let mut props: HashMap<String, String> = HashMap::new();
    props.insert("added-data-files".into(), added_files.len().to_string());
    props.insert("added-records".into(), output_records.to_string());
    props.insert("added-files-size".into(), added_bytes.to_string());
    props.insert("deleted-data-files".into(), input_files.to_string());
    props.insert("deleted-records".into(), input_records.to_string());
    props.insert("removed-files-size".into(), input_bytes.to_string());
    props.insert(
        "total-data-files".into(),
        (kept_files + added_files.len() as u64).to_string(),
    );
    props.insert(
        "total-records".into(),
        (kept_records + output_records).to_string(),
    );
    props.insert(
        "total-files-size".into(),
        (kept_bytes + added_bytes).to_string(),
    );
    props.insert("total-delete-files".into(), "0".into());
    props.insert("total-position-deletes".into(), "0".into());
    props.insert("total-equality-deletes".into(), "0".into());
    props.insert("changed-partition-count".into(), "1".into());

    let snapshot = Snapshot::builder()
        .with_snapshot_id(snapshot_id)
        .with_parent_snapshot_id(Some(head_id))
        .with_sequence_number(next_seq)
        .with_timestamp_ms(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0),
        )
        .with_manifest_list(manifest_list_path)
        .with_summary(Summary {
            operation: Operation::Replace,
            additional_properties: props,
        })
        .with_schema_id(metadata.current_schema_id())
        .build();

    let mut request = CommitTableRequest {
        identifier: Some(ident.clone()),
        requirements: vec![
            TableRequirement::UuidMatch {
                uuid: metadata.uuid(),
            },
            // First-committer-wins: main must still point at the snapshot
            // the plan was computed against, or the catalog answers 409.
            TableRequirement::RefSnapshotIdMatch {
                r#ref: MAIN_BRANCH.to_string(),
                snapshot_id: Some(head_id),
            },
        ],
        updates: vec![
            TableUpdate::AddSnapshot { snapshot },
            TableUpdate::SetSnapshotRef {
                ref_name: MAIN_BRANCH.to_string(),
                reference: SnapshotReference::new(
                    snapshot_id,
                    SnapshotRetention::branch(None, None, None),
                ),
            },
        ],
    };
    // Test-only conflict injection: corrupt the ref requirement so the
    // server's optimistic-concurrency check rejects the commit for real
    // and the clean-abort path is e2e-provable.
    if std::env::var_os("ICEGRES_COMPACT_INJECT_CONFLICT").is_some() {
        overwrite::corrupt_ref_requirement(&mut request);
        tracing::warn!(
            "ICEGRES_COMPACT_INJECT_CONFLICT set (test-only): sabotaging \
             assert-ref-snapshot-id to force a 409"
        );
    }
    let prepared = PreparedCommit::for_maintenance(request, snapshot_id);
    match engine.post_prepared(&ident, &prepared).await? {
        CommitOutcome::Committed => {
            println!(
                "compacted {input_files} file(s) totaling {input_bytes} bytes into {} \
                 file(s) totaling {added_bytes} bytes on {ident} (snapshot {snapshot_id}, \
                 replace; row count unchanged at {input_records} in the rewritten set). \
                 Old files stay time-travel-readable until snapshot expiry; reclaim them \
                 with: icegres maintain expire-snapshots {ident} && icegres maintain \
                 remove-orphans {ident}",
                added_files.len()
            );
            Ok(())
        }
        CommitOutcome::Conflict(msg) => bail!(
            "another writer committed to {ident} while compacting (the table moved past \
             snapshot {head_id}, which this plan was computed against) — aborting with \
             NOTHING changed (first-committer-wins; the staged output files are \
             unreferenced orphans). Re-run the compact: {msg}"
        ),
    }
}

/// Fail-closed schema FAST PATH for one loaded manifest: refuse the whole
/// run when its schema id diverges from the table's current one. This
/// catches simple divergence (a schema evolved after the head's manifests
/// were written) before the planner even runs, but it is NOT the
/// guarantee: a manifest's schema id records the schema of whoever WROTE
/// the manifest, not the schema its listed data files were physically
/// encoded under, and any post-evolution manifest rewrite (Spark
/// rewrite_manifests, foreign copy-on-write DML, icegres's own m0 carrying
/// untouched files as EXISTING entries) re-stamps old-schema files under
/// the current id. The per-file Parquet field-id verification
/// ([`ensure_file_schema_current`], run on every candidate input) is what
/// actually closes the dropped-column-resurrection hazard. Runs during the
/// manifest walk, strictly before any output is staged or committed.
fn ensure_manifest_schema_current(
    table: &str,
    manifest_path: &str,
    manifest_schema_id: i32,
    current_schema_id: i32,
) -> Result<()> {
    if manifest_schema_id != current_schema_id {
        bail!(
            "refusing to compact {table}: manifest {manifest_path} was written under \
             schema {manifest_schema_id} (current schema is {current_schema_id}) — \
             compaction is not field-id-aware (batches are aligned to the current \
             schema by position and name), so rewriting files from a divergent schema \
             version could silently corrupt values. Rewrite the old files under the \
             current schema (full-table rewrite) or wait for field-id-aware \
             compaction; nothing was rewritten"
        );
    }
    Ok(())
}

/// Fail-closed schema GUARANTEE for one candidate input file: verify that
/// the file's Parquet-embedded field ids (`PARQUET:field_id` in the Arrow
/// field metadata — embedded by every Iceberg Parquet writer: icegres's
/// own iceberg-rust stack, pyiceberg, Spark) match the table's current
/// schema column for column. The rewrite aligns batches by position +
/// case-insensitive name (overwrite::align_batch), so a
/// dropped-then-re-added column of the same name and shape passes every
/// structural check while carrying the OLD values — only the physical
/// field id betrays the file's true write schema, and a manifest's schema
/// id cannot (it records the manifest writer's schema). Missing or
/// unparseable field-id metadata refuses too: the write schema cannot be
/// verified, so it is never trusted. Runs on every group input strictly
/// before any output is staged.
fn ensure_file_schema_current(
    table: &str,
    file_path: &str,
    file_schema: &ArrowSchema,
    target: &ArrowSchemaRef,
) -> Result<()> {
    const WORKAROUND: &str = "Rewrite the old files under the current schema (full-table \
                              rewrite) or wait for field-id-aware compaction; nothing was \
                              rewritten";
    if file_schema.fields().len() != target.fields().len() {
        bail!(
            "refusing to compact {table}: data file {file_path} has {} column(s) but the \
             table's current schema has {} — the file was physically written under a \
             different schema version. {WORKAROUND}",
            file_schema.fields().len(),
            target.fields().len()
        );
    }
    for (i, target_field) in target.fields().iter().enumerate() {
        let file_field = file_schema.field(i);
        // The canonical Arrow conversion of an Iceberg schema annotates
        // every field id; anything else is an upstream bug — refuse
        // rather than guess.
        let Some(expected) = parquet_field_id(target_field) else {
            bail!(
                "refusing to compact {table}: the current schema's column {:?} carries \
                 no field id in its Arrow conversion, so data files cannot be verified \
                 against it; nothing was rewritten",
                target_field.name()
            );
        };
        if !file_field.name().eq_ignore_ascii_case(target_field.name()) {
            bail!(
                "refusing to compact {table}: data file {file_path} column {i} is named \
                 {:?} but the table's current schema names it {:?} — the file was \
                 physically written under a different schema version. {WORKAROUND}",
                file_field.name(),
                target_field.name()
            );
        }
        match parquet_field_id(file_field) {
            None => bail!(
                "refusing to compact {table}: data file {file_path} column {:?} \
                 (position {i}) carries no Parquet field-id metadata, so the schema it \
                 was physically written under cannot be verified against the current \
                 schema (field id {expected} expected). {WORKAROUND}",
                file_field.name()
            ),
            Some(found) if found != expected => bail!(
                "refusing to compact {table}: data file {file_path} column {:?} \
                 (position {i}) was physically written with Parquet field id {found}, \
                 but the table's current schema assigns field id {expected} — the file \
                 predates a schema evolution (e.g. a dropped-then-re-added column), and \
                 rewriting it by position and name could silently resurrect stale \
                 values under the current field id. {WORKAROUND}",
                file_field.name()
            ),
            Some(_) => {}
        }
    }
    Ok(())
}

/// The `PARQUET:field_id` annotation of one Arrow field (None when the
/// key is absent or unparseable — the caller refuses either way).
fn parquet_field_id(field: &ArrowField) -> Option<i32> {
    field
        .metadata()
        .get(PARQUET_FIELD_ID_META_KEY)?
        .parse()
        .ok()
}

/// Canonical partition key of a data file's partition struct — the
/// planner's grouping key. Unpartitioned tables (the only kind the
/// executor accepts today) always produce the empty struct, i.e. ONE
/// group; the Debug rendering keeps the planner honest about never mixing
/// partitions if the executor ever grows partitioned support.
fn partition_key(partition: &iceberg::spec::Struct) -> String {
    if partition.fields().is_empty() {
        String::new()
    } else {
        format!("{partition:?}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn f(path: &str, size_bytes: u64, record_count: u64, partition: &str) -> CompactCandidate {
        CompactCandidate {
            path: path.to_string(),
            size_bytes,
            record_count,
            partition: partition.to_string(),
        }
    }

    const MB: u64 = 1024 * 1024;

    #[test]
    fn planner_selects_only_under_target_files() {
        let files = [
            f("a", MB, 10, ""),
            f("b", 200 * MB, 1000, ""), // at/over target: never touched
            f("c", 2 * MB, 20, ""),
            f("d", 128 * MB, 500, ""), // exactly at target: never touched
        ];
        let groups = plan_compaction(&files, 128 * MB, 2);
        assert_eq!(groups.len(), 1);
        assert_eq!(groups[0].inputs, vec![0, 2]);
        assert_eq!(groups[0].total_bytes, 3 * MB);
        assert_eq!(groups[0].total_records, 30);
    }

    #[test]
    fn planner_honors_min_input_files_threshold() {
        let files = [f("a", MB, 10, ""), f("b", MB, 10, "")];
        // Two candidates meet a threshold of 2 ...
        assert_eq!(plan_compaction(&files, 128 * MB, 2).len(), 1);
        // ... but not of 3: rewriting below the operator's payoff bar is
        // skipped entirely.
        assert!(plan_compaction(&files, 128 * MB, 3).is_empty());
        // A single small file is never a plan (nothing to combine).
        assert!(plan_compaction(&files[..1], 128 * MB, 2).is_empty());
    }

    #[test]
    fn planner_never_combines_across_partitions() {
        let files = [
            f("a", MB, 1, "city=paris"),
            f("b", MB, 1, "city=lyon"),
            f("c", MB, 1, "city=paris"),
            f("d", MB, 1, "city=lyon"),
            f("e", MB, 1, "city=rome"), // alone in its partition: skipped
        ];
        let groups = plan_compaction(&files, 128 * MB, 2);
        assert_eq!(groups.len(), 2);
        // First-seen partition order, inputs in input order.
        assert_eq!(groups[0].partition, "city=paris");
        assert_eq!(groups[0].inputs, vec![0, 2]);
        assert_eq!(groups[1].partition, "city=lyon");
        assert_eq!(groups[1].inputs, vec![1, 3]);
        assert!(groups.iter().all(|g| g.partition != "city=rome"));
    }

    #[test]
    fn planner_empty_when_all_files_are_well_sized() {
        let files = [f("a", 512 * MB, 1, ""), f("b", 300 * MB, 1, "")];
        assert!(plan_compaction(&files, 128 * MB, 2).is_empty());
        assert!(plan_compaction(&[], 128 * MB, 2).is_empty());
    }

    #[test]
    fn schema_guard_refuses_divergent_manifest_schema() {
        let err = ensure_manifest_schema_current("demo.t", "s3://b/metadata/m0.avro", 0, 1)
            .expect_err("divergent schema id must refuse");
        let msg = err.to_string();
        assert!(msg.contains("refusing to compact demo.t"), "{msg}");
        assert!(msg.contains("s3://b/metadata/m0.avro"), "{msg}");
        assert!(msg.contains("schema 0"), "{msg}");
        assert!(msg.contains("current schema is 1"), "{msg}");
        assert!(msg.contains("nothing was rewritten"), "{msg}");
    }

    #[test]
    fn schema_guard_passes_matching_manifest_schema() {
        // Matching ids (any value, not just 0) must not refuse.
        ensure_manifest_schema_current("demo.t", "s3://b/metadata/m0.avro", 0, 0).unwrap();
        ensure_manifest_schema_current("demo.t", "s3://b/metadata/m1.avro", 3, 3).unwrap();
    }

    /// Arrow schema of a (possibly old) data FILE: columns with optional
    /// `PARQUET:field_id` metadata, exactly what
    /// overwrite::read_parquet_arrow_schema surfaces from a footer.
    fn arrow_file_schema(cols: &[(&str, arrow::datatypes::DataType, Option<i32>)]) -> ArrowSchema {
        ArrowSchema::new(
            cols.iter()
                .map(|(name, dt, id)| {
                    let field = ArrowField::new(*name, dt.clone(), true);
                    match id {
                        Some(id) => field.with_metadata(HashMap::from([(
                            PARQUET_FIELD_ID_META_KEY.to_string(),
                            id.to_string(),
                        )])),
                        None => field,
                    }
                })
                .collect::<Vec<_>>(),
        )
    }

    /// The post-evolution CURRENT schema: `v` was dropped and re-added, so
    /// it keeps its name, position and type but carries a NEW field id (3,
    /// not the original 2). Built through the real Iceberg->Arrow
    /// conversion, which also proves that conversion annotates field ids.
    fn evolved_target() -> ArrowSchemaRef {
        use iceberg::spec::{NestedField, PrimitiveType, Type};
        let schema = iceberg::spec::Schema::builder()
            .with_schema_id(1)
            .with_fields(vec![
                NestedField::optional(1, "id", Type::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(3, "v", Type::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .expect("iceberg schema builds");
        Arc::new(schema_to_arrow_schema(&schema).expect("arrow conversion"))
    }

    #[test]
    fn file_guard_passes_matching_field_ids() {
        use arrow::datatypes::DataType;
        let target = evolved_target();
        let file = arrow_file_schema(&[
            ("id", DataType::Int64, Some(1)),
            ("v", DataType::Utf8, Some(3)),
        ]);
        ensure_file_schema_current("demo.t", "s3://b/data/f.parquet", &file, &target)
            .expect("matching field ids must pass");
        // Case-insensitive names are fine (align_batch's own rule).
        let file = arrow_file_schema(&[
            ("ID", DataType::Int64, Some(1)),
            ("V", DataType::Utf8, Some(3)),
        ]);
        ensure_file_schema_current("demo.t", "s3://b/data/f.parquet", &file, &target)
            .expect("case-insensitive name match must pass");
    }

    #[test]
    fn file_guard_refuses_mismatched_field_id() {
        use arrow::datatypes::DataType;
        // The laundered dropped-then-re-added column: same name, same
        // position, same type — OLD field id. A manifest re-stamped to the
        // current schema id sails past the manifest fast path; only this
        // per-file check catches it.
        let target = evolved_target();
        let file = arrow_file_schema(&[
            ("id", DataType::Int64, Some(1)),
            ("v", DataType::Utf8, Some(2)),
        ]);
        let msg = ensure_file_schema_current("demo.t", "s3://b/data/old.parquet", &file, &target)
            .expect_err("stale field id must refuse")
            .to_string();
        assert!(msg.contains("refusing to compact demo.t"), "{msg}");
        assert!(msg.contains("s3://b/data/old.parquet"), "{msg}");
        assert!(msg.contains("\"v\""), "{msg}");
        assert!(msg.contains("Parquet field id 2"), "{msg}");
        assert!(msg.contains("assigns field id 3"), "{msg}");
        assert!(msg.contains("nothing was rewritten"), "{msg}");
    }

    #[test]
    fn file_guard_refuses_missing_field_id_metadata() {
        use arrow::datatypes::DataType;
        // No field-id metadata at all: the write schema is unverifiable,
        // so the guard fails closed instead of trusting position + name.
        let target = evolved_target();
        let file = arrow_file_schema(&[
            ("id", DataType::Int64, Some(1)),
            ("v", DataType::Utf8, None),
        ]);
        let msg = ensure_file_schema_current("demo.t", "s3://b/data/bare.parquet", &file, &target)
            .expect_err("missing field-id metadata must refuse")
            .to_string();
        assert!(msg.contains("refusing to compact demo.t"), "{msg}");
        assert!(msg.contains("s3://b/data/bare.parquet"), "{msg}");
        assert!(msg.contains("no Parquet field-id metadata"), "{msg}");
        assert!(msg.contains("field id 3 expected"), "{msg}");
        assert!(msg.contains("nothing was rewritten"), "{msg}");
    }

    #[test]
    fn file_guard_refuses_structural_divergence() {
        use arrow::datatypes::DataType;
        let target = evolved_target();
        // Column count drift (e.g. a file predating an added column).
        let file = arrow_file_schema(&[("id", DataType::Int64, Some(1))]);
        let msg = ensure_file_schema_current("demo.t", "s3://b/data/f.parquet", &file, &target)
            .expect_err("column count divergence must refuse")
            .to_string();
        assert!(msg.contains("1 column(s)"), "{msg}");
        assert!(msg.contains("nothing was rewritten"), "{msg}");
        // Renamed column at the same position.
        let file = arrow_file_schema(&[
            ("id", DataType::Int64, Some(1)),
            ("w", DataType::Utf8, Some(3)),
        ]);
        let msg = ensure_file_schema_current("demo.t", "s3://b/data/f.parquet", &file, &target)
            .expect_err("column name divergence must refuse")
            .to_string();
        assert!(msg.contains("\"w\""), "{msg}");
        assert!(msg.contains("\"v\""), "{msg}");
        assert!(msg.contains("nothing was rewritten"), "{msg}");
    }

    /// End to end over REAL Parquet footers: write fixtures with the
    /// arrow/parquet writer (the same stack the engine uses), read their
    /// schemas back through overwrite::read_parquet_arrow_schema (footer
    /// only), and run the guard — proving field ids round-trip through
    /// physical Parquet bytes exactly as the rail assumes.
    #[tokio::test]
    async fn file_guard_reads_field_ids_from_real_parquet_footers() {
        use arrow::array::{ArrayRef, Int64Array, StringArray};
        use arrow::record_batch::RecordBatch;
        use datafusion::parquet::arrow::ArrowWriter;

        let dir = std::env::temp_dir().join(format!("icegres-file-guard-{}", Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("create fixture dir");
        let write = |name: &str, schema: ArrowSchema| -> String {
            let path = dir.join(name);
            let schema = Arc::new(schema);
            let columns: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from(vec![1, 2])),
                Arc::new(StringArray::from(vec!["a", "b"])),
            ];
            let batch = RecordBatch::try_new(schema.clone(), columns).expect("fixture batch");
            let file = std::fs::File::create(&path).expect("create fixture file");
            let mut writer = ArrowWriter::try_new(file, schema, None).expect("fixture writer");
            writer.write(&batch).expect("write fixture");
            writer.close().expect("close fixture");
            path.to_string_lossy().into_owned()
        };
        use arrow::datatypes::DataType;
        let current = write(
            "current.parquet",
            arrow_file_schema(&[
                ("id", DataType::Int64, Some(1)),
                ("v", DataType::Utf8, Some(3)),
            ]),
        );
        let stale = write(
            "stale.parquet",
            arrow_file_schema(&[
                ("id", DataType::Int64, Some(1)),
                ("v", DataType::Utf8, Some(2)),
            ]),
        );
        let bare = write(
            "bare.parquet",
            arrow_file_schema(&[("id", DataType::Int64, None), ("v", DataType::Utf8, None)]),
        );

        let file_io = iceberg::io::FileIO::new_with_fs();
        let target = evolved_target();

        let schema = overwrite::read_parquet_arrow_schema(&file_io, &current)
            .await
            .expect("footer-only read of the current-schema fixture");
        ensure_file_schema_current("demo.t", &current, &schema, &target)
            .expect("matching physical field ids must pass");

        let schema = overwrite::read_parquet_arrow_schema(&file_io, &stale)
            .await
            .expect("footer-only read of the stale-schema fixture");
        let msg = ensure_file_schema_current("demo.t", &stale, &schema, &target)
            .expect_err("stale physical field id must refuse")
            .to_string();
        assert!(msg.contains("Parquet field id 2"), "{msg}");
        assert!(msg.contains("assigns field id 3"), "{msg}");
        assert!(msg.contains("nothing was rewritten"), "{msg}");

        let schema = overwrite::read_parquet_arrow_schema(&file_io, &bare)
            .await
            .expect("footer-only read of the id-less fixture");
        let msg = ensure_file_schema_current("demo.t", &bare, &schema, &target)
            .expect_err("id-less physical file must refuse")
            .to_string();
        assert!(msg.contains("no Parquet field-id metadata"), "{msg}");
        assert!(msg.contains("nothing was rewritten"), "{msg}");

        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn projected_outputs_round_up_and_never_hit_zero() {
        let g = CompactGroup {
            partition: String::new(),
            inputs: vec![0, 1],
            total_bytes: 3 * MB,
            total_records: 2,
        };
        // 3 MiB under a 128 MiB target: one output.
        assert_eq!(g.projected_outputs(128 * MB), 1);
        // 3 MiB under a 2 MiB target: two outputs.
        assert_eq!(g.projected_outputs(2 * MB), 2);
        // Degenerate zero-byte group still projects one file.
        let empty = CompactGroup {
            partition: String::new(),
            inputs: vec![0, 1],
            total_bytes: 0,
            total_records: 0,
        };
        assert_eq!(empty.projected_outputs(128 * MB), 1);
    }
}
