//! Copy-on-write UPDATE/DELETE over Iceberg (SPEC B2/B3), zero data
//! replication: a DML statement becomes ONE new Iceberg snapshot that
//! reuses every untouched data file and rewrites only the files that
//! actually contain matching rows.
//!
//! # Why hand-rolled snapshot production
//!
//! The pinned iceberg-rust 0.9.1 `Transaction` API exposes only
//! `fast_append` — there is no overwrite/rewrite-files action, and
//! `TableCommit`'s constructor is `pub(crate)`, so `Catalog::update_table`
//! cannot be fed a hand-built commit. All the *building blocks* are public,
//! however: `ManifestWriterBuilder` (ADDED/EXISTING/DELETED entries),
//! `ManifestListWriter`, `Snapshot`/`TableUpdate`/`TableRequirement`, and
//! the data-file writer stack. This module assembles them exactly the way
//! `Transaction`'s own `SnapshotProducer` does (see iceberg-0.9.1
//! `src/transaction/snapshot.rs`) and commits the result through the
//! Iceberg REST protocol (`POST /v1/{prefix}/namespaces/{ns}/tables/{tbl}`,
//! which Lakekeeper implements in full, including snapshot refs).
//!
//! # Algorithm (per attempt)
//!
//! 1. `load_table` — fresh metadata; the current `main` snapshot id becomes
//!    the optimistic-concurrency anchor.
//! 2. For every live data file (from the manifest list): read it, evaluate
//!    the DML predicate against its rows with DataFusion, and classify the
//!    file as **kept** (0 matches — the common case, zero-copy), **removed**
//!    (DELETE matched every row), or **rewritten** (matched rows deleted /
//!    updated, survivors preserved byte-for-value).
//! 3. Rewritten survivor rows are written to new Parquet file(s) with the
//!    standard iceberg-rust writer stack (same one the INSERT path uses).
//! 4. A new manifest records ADDED (new files), EXISTING (kept files, with
//!    their original snapshot ids/sequence numbers) and DELETED (removed +
//!    rewritten sources) entries; manifests whose files are all kept are
//!    carried into the new manifest list untouched.
//! 5. The new snapshot (summary operation `overwrite`, or `delete` when no
//!    file was added) is committed with requirements
//!    `assert-table-uuid` + `assert-ref-snapshot-id main=<anchor>`.
//!
//! # Atomicity, durability, concurrency
//!
//! The catalog commit is the *only* mutation: readers see either the old
//! snapshot or the new one, never an intermediate state. If the commit
//! loses an optimistic-concurrency race (HTTP 409 because another writer
//! moved `main` — e.g. a concurrent INSERT), the whole computation is
//! **recomputed from fresh metadata and retried** (bounded by
//! [`MAX_COMMIT_ATTEMPTS`]). Parquet/Avro files written by a failed or
//! abandoned attempt are unreferenced by any snapshot and therefore
//! harmless orphans (standard Iceberg semantics; removable by any orphan
//! file cleanup). Time travel keeps working: the pre-DML snapshot and its
//! files are untouched — a copy-on-write rewrite never mutates or deletes
//! committed objects.
//!
//! Setting `ICEGRES_DML_INJECT_CONFLICT=1` (test-only knob) deliberately
//! corrupts the first attempt's `assert-ref-snapshot-id` requirement so the
//! catalog rejects it with 409, proving the server-side check and the
//! refresh-and-retry path end to end (used by icegres/tests/e2e.sh).
//!
//! # Bounds & limitations (fail loudly, never wrong)
//!
//! * Format v2, unpartitioned tables, Parquet data files, no delete
//!   manifests — anything else is rejected before any write.
//! * Every live data file is read once per DML statement (no min/max stat
//!   pruning yet — an optimization, not a correctness issue), one file at a
//!   time, so peak memory is one data file's decoded batches.
//! * Predicates/assignment values must be self-contained row expressions:
//!   subqueries are rejected (they would otherwise be evaluated per-file
//!   and yield wrong answers).

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, bail, Context as _, Result};
use arrow::array::{Array, AsArray, RecordBatch};
use arrow::compute::{cast_with_options, CastOptions};
use arrow::datatypes::{Int64Type, SchemaRef as ArrowSchemaRef};
use datafusion::datasource::MemTable;
use datafusion::parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg::arrow::{schema_to_arrow_schema, FieldMatchMode};
use iceberg::spec::{
    DataFile, DataFileFormat, FormatVersion, ManifestContentType, ManifestFile, ManifestListWriter,
    ManifestWriter, ManifestWriterBuilder, Operation, Snapshot, SnapshotReference,
    SnapshotRetention, Summary, MAIN_BRANCH, UNASSIGNED_SEQUENCE_NUMBER,
};
use iceberg::table::Table;
use iceberg::writer::base_writer::data_file_writer::DataFileWriterBuilder;
use iceberg::writer::file_writer::location_generator::{
    DefaultFileNameGenerator, DefaultLocationGenerator,
};
use iceberg::writer::file_writer::rolling_writer::RollingFileWriterBuilder;
use iceberg::writer::file_writer::ParquetWriterBuilder;
use iceberg::writer::{IcebergWriter, IcebergWriterBuilder};
use iceberg::{Catalog, TableIdent, TableRequirement, TableUpdate};
use iceberg_catalog_rest::CommitTableRequest;
use uuid::Uuid;

use crate::context::CATALOG_NAME;
use crate::CatalogOpts;

/// Upper bound on optimistic-concurrency attempts (initial try + retries
/// after 409 conflicts). Each retry recomputes from fresh table metadata.
pub const MAX_COMMIT_ATTEMPTS: u32 = 3;

/// What a DML statement does to matched rows.
#[derive(Debug, Clone)]
pub enum DmlKind {
    /// Remove matched rows.
    Delete,
    /// Rewrite matched rows: `(column, value-expression SQL)` pairs.
    Update { assignments: Vec<(String, String)> },
}

/// A validated, engine-ready UPDATE/DELETE statement.
#[derive(Debug, Clone)]
pub struct DmlStatement {
    pub kind: DmlKind,
    /// Iceberg namespace (single level, e.g. "demo").
    pub namespace: String,
    /// Table name within the namespace.
    pub table: String,
    /// Optional table alias from the original statement (needed so a
    /// predicate like `t.fare > 5` keeps resolving).
    pub alias: Option<String>,
    /// WHERE clause SQL (absent = all rows match).
    pub predicate: Option<String>,
}

/// Result of a committed (or no-op) DML statement.
#[derive(Debug)]
pub struct DmlOutcome {
    /// Rows deleted/updated.
    pub rows: u64,
    /// Commit attempts consumed (1 = no conflict).
    pub attempts: u32,
    /// New snapshot id; `None` when nothing matched (no commit was made).
    pub snapshot_id: Option<i64>,
}

/// Executes copy-on-write DML against one Iceberg REST catalog.
pub struct OverwriteEngine {
    catalog: Arc<dyn Catalog>,
    http: reqwest::Client,
    /// REST base, e.g. `http://127.0.0.1:8181/catalog`.
    catalog_uri: String,
    /// Catalog path prefix from `GET /v1/config` (may be empty).
    prefix: String,
}

impl OverwriteEngine {
    /// Build an engine over an already-connected catalog, resolving the REST
    /// path prefix from `GET /v1/config?warehouse=...` (same handshake the
    /// REST catalog client performs).
    pub async fn connect(catalog: Arc<dyn Catalog>, opts: &CatalogOpts) -> Result<Self> {
        let http = reqwest::Client::new();
        let config_url = format!(
            "{}/v1/config?warehouse={}",
            opts.catalog_uri.trim_end_matches('/'),
            urlencode(&opts.warehouse)
        );
        let config: serde_json::Value = http
            .get(&config_url)
            .send()
            .await
            .with_context(|| format!("failed to fetch catalog config from {config_url}"))?
            .error_for_status()
            .with_context(|| format!("catalog config request rejected ({config_url})"))?
            .json()
            .await
            .context("catalog config response is not JSON")?;
        let prefix = config
            .pointer("/overrides/prefix")
            .or_else(|| config.pointer("/defaults/prefix"))
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();
        Ok(Self {
            catalog,
            http,
            catalog_uri: opts.catalog_uri.trim_end_matches('/').to_string(),
            prefix,
        })
    }

    /// Execute a DML statement: classify/rewrite data files, produce an
    /// overwrite snapshot, and commit it via the REST catalog with bounded
    /// optimistic-concurrency retries.
    pub async fn execute(&self, stmt: &DmlStatement) -> Result<DmlOutcome> {
        let ident = TableIdent::from_strs([stmt.namespace.as_str(), stmt.table.as_str()])
            .map_err(|e| anyhow!("bad table identifier: {e}"))?;
        let mut conflicts: Vec<String> = Vec::new();
        for attempt in 1..=MAX_COMMIT_ATTEMPTS {
            let table = self
                .catalog
                .load_table(&ident)
                .await
                .map_err(|e| anyhow!("failed to load table {ident}: {e}"))?;
            let prepared = prepare_overwrite(&table, stmt)
                .await
                .with_context(|| format!("DML against {ident} failed"))?;
            let Some(mut prepared) = prepared else {
                // Nothing matched: Postgres semantics — succeed with 0 rows,
                // commit nothing.
                return Ok(DmlOutcome {
                    rows: 0,
                    attempts: attempt,
                    snapshot_id: None,
                });
            };
            // Test-only conflict injection: corrupt the ref requirement on
            // the FIRST attempt so the server's optimistic-concurrency check
            // rejects it and the retry path is exercised for real.
            if attempt == 1 && std::env::var_os("ICEGRES_DML_INJECT_CONFLICT").is_some() {
                corrupt_ref_requirement(&mut prepared.request);
                tracing::warn!(
                    "ICEGRES_DML_INJECT_CONFLICT set: sabotaging attempt 1's \
                     assert-ref-snapshot-id to force a 409"
                );
            }
            match self
                .post_commit(&stmt.namespace, &stmt.table, &prepared.request)
                .await?
            {
                CommitOutcome::Committed => {
                    tracing::info!(
                        table = %ident,
                        rows = prepared.rows,
                        snapshot_id = prepared.snapshot_id,
                        attempt,
                        "DML committed (copy-on-write overwrite snapshot)"
                    );
                    return Ok(DmlOutcome {
                        rows: prepared.rows,
                        attempts: attempt,
                        snapshot_id: Some(prepared.snapshot_id),
                    });
                }
                CommitOutcome::Conflict(msg) => {
                    tracing::warn!(
                        table = %ident,
                        attempt,
                        "commit conflict (409), refreshing metadata and retrying: {msg}"
                    );
                    conflicts.push(msg);
                }
            }
        }
        bail!(
            "DML on {ident} lost the optimistic-concurrency race {MAX_COMMIT_ATTEMPTS} times; \
             giving up (no partial effects were committed). Conflicts: {}",
            conflicts.join(" | ")
        )
    }

    /// POST the commit to the REST catalog. 2xx = committed, 409 = conflict
    /// (caller retries), anything else = hard error.
    async fn post_commit(
        &self,
        namespace: &str,
        table: &str,
        request: &CommitTableRequest,
    ) -> Result<CommitOutcome> {
        let prefix_seg = if self.prefix.is_empty() {
            String::new()
        } else {
            format!("/{}", urlencode(&self.prefix))
        };
        let url = format!(
            "{}/v1{}/namespaces/{}/tables/{}",
            self.catalog_uri,
            prefix_seg,
            urlencode(namespace),
            urlencode(table)
        );
        let resp = self
            .http
            .post(&url)
            .json(request)
            .send()
            .await
            .with_context(|| format!("commit POST to {url} failed"))?;
        let status = resp.status();
        if status.is_success() {
            return Ok(CommitOutcome::Committed);
        }
        let body = resp.text().await.unwrap_or_default();
        if status == reqwest::StatusCode::CONFLICT {
            return Ok(CommitOutcome::Conflict(body));
        }
        bail!("catalog rejected commit ({status}) at {url}: {body}")
    }
}

enum CommitOutcome {
    Committed,
    Conflict(String),
}

/// A fully-prepared commit: all data/metadata files are already durable in
/// object storage; only the atomic catalog POST remains.
struct PreparedCommit {
    request: CommitTableRequest,
    rows: u64,
    snapshot_id: i64,
}

/// Classification of one live data file against the DML predicate.
enum FileFate {
    /// No row matched: reuse the file as-is (zero-copy).
    Keep,
    /// Every row matched a DELETE: drop the file, nothing to rewrite.
    Remove { matched: u64 },
    /// Some rows matched: survivors/updated rows for the replacement file.
    Rewrite {
        matched: u64,
        batches: Vec<RecordBatch>,
    },
}

/// Compute the overwrite snapshot for `stmt` against the table's current
/// metadata and stage every file it needs. Returns `None` when no row
/// matched (nothing to commit).
async fn prepare_overwrite(table: &Table, stmt: &DmlStatement) -> Result<Option<PreparedCommit>> {
    let metadata = table.metadata();

    // ---- Guard rails: reject unsupported table shapes loudly. ----
    if metadata.format_version() != FormatVersion::V2 {
        bail!(
            "UPDATE/DELETE support requires an Iceberg format v2 table (found {:?})",
            metadata.format_version()
        );
    }
    if !metadata.default_partition_spec().is_unpartitioned() {
        bail!("UPDATE/DELETE on partitioned tables is not supported yet");
    }
    let Some(current_snapshot) = metadata.current_snapshot() else {
        // Empty table (no snapshot): nothing can match.
        return Ok(None);
    };

    let file_io = table.file_io();
    let schema = metadata.current_schema();
    let arrow_target: ArrowSchemaRef = Arc::new(
        schema_to_arrow_schema(schema).map_err(|e| anyhow!("schema conversion failed: {e}"))?,
    );
    let sql = DmlSql::new(
        stmt,
        schema.as_struct().fields().iter().map(|f| f.name.as_str()),
    )?;

    // Ephemeral evaluation context: each data file's rows are registered
    // under the statement's own table name so the predicate/assignment SQL
    // evaluates unchanged, one file at a time.
    let eval_ctx = SessionContext::new_with_config(
        SessionConfig::new().with_default_catalog_and_schema(CATALOG_NAME, &stmt.namespace),
    );

    let manifest_list = current_snapshot
        .load_manifest_list(file_io, &table.metadata_ref())
        .await
        .map_err(|e| anyhow!("failed to load manifest list: {e}"))?;
    for mf in manifest_list.entries() {
        if mf.content != ManifestContentType::Data {
            bail!(
                "table has delete manifests (merge-on-read); UPDATE/DELETE via icegres \
                 supports copy-on-write tables only"
            );
        }
    }

    // Lazily-built writer for replacement/updated rows.
    let mut data_writer: Option<_> = None;
    let commit_uuid = Uuid::new_v4();

    let mut matched_rows: u64 = 0;
    // Manifests whose files are all kept: carried forward untouched.
    let mut carried: Vec<ManifestFile> = Vec::new();
    // (data_file, snapshot_id, data_seq, file_seq) for kept files from
    // manifests that must be rewritten.
    let mut existing: Vec<(DataFile, i64, i64, Option<i64>)> = Vec::new();
    // Same, for files removed by this snapshot (recorded as DELETED).
    let mut deleted: Vec<(DataFile, i64, Option<i64>)> = Vec::new();
    // Summary bookkeeping. Kept-file stats are accumulated so post-DML
    // totals are computed exactly from the final live set (previous
    // snapshots' summaries are not trusted: iceberg-rust 0.9.1 fast_append
    // itself writes non-cumulative totals).
    let (mut removed_files, mut removed_records, mut removed_bytes) = (0u64, 0u64, 0u64);
    let (mut kept_files, mut kept_records, mut kept_bytes) = (0u64, 0u64, 0u64);

    for manifest_file in manifest_list.entries() {
        let manifest = manifest_file.load_manifest(file_io).await.map_err(|e| {
            anyhow!(
                "failed to load manifest {}: {e}",
                manifest_file.manifest_path
            )
        })?;
        let (entries, _meta) = manifest.into_parts();

        let mut rewrite_manifest = false;
        // (entry index, fate) for live entries of this manifest.
        let mut fates: Vec<(usize, FileFate)> = Vec::new();
        for (idx, entry) in entries.iter().enumerate() {
            if !entry.is_alive() {
                // Entry already deleted by an earlier snapshot: drop it from
                // the new manifest (spec: DELETED entries live only in the
                // snapshot that deleted them).
                rewrite_manifest = true;
                continue;
            }
            let fate = classify_file(&eval_ctx, file_io, entry.data_file(), &sql, stmt).await?;
            match &fate {
                FileFate::Keep => {
                    kept_files += 1;
                    kept_records += entry.data_file().record_count();
                    kept_bytes += entry.data_file().file_size_in_bytes();
                }
                FileFate::Remove { matched } => {
                    matched_rows += matched;
                    rewrite_manifest = true;
                }
                FileFate::Rewrite { matched, .. } => {
                    matched_rows += matched;
                    rewrite_manifest = true;
                }
            }
            fates.push((idx, fate));
        }

        if !rewrite_manifest {
            carried.push(manifest_file.clone());
            continue;
        }
        for (idx, fate) in fates {
            let entry = &entries[idx];
            let data_seq = entry
                .sequence_number()
                .unwrap_or(manifest_file.sequence_number);
            let file_seq = entry.file_sequence_number;
            match fate {
                FileFate::Keep => {
                    existing.push((
                        entry.data_file().clone(),
                        entry
                            .snapshot_id()
                            .unwrap_or(manifest_file.added_snapshot_id),
                        data_seq,
                        file_seq,
                    ));
                }
                FileFate::Remove { .. } | FileFate::Rewrite { .. } => {
                    removed_files += 1;
                    removed_records += entry.data_file().record_count();
                    removed_bytes += entry.data_file().file_size_in_bytes();
                    deleted.push((entry.data_file().clone(), data_seq, file_seq));
                    if let FileFate::Rewrite { batches, .. } = fate {
                        let writer = match data_writer.as_mut() {
                            Some(w) => w,
                            None => {
                                data_writer = Some(new_data_writer(table, &commit_uuid).await?);
                                data_writer.as_mut().expect("just set")
                            }
                        };
                        for batch in batches {
                            let aligned = align_batch(&batch, &arrow_target)?;
                            writer
                                .write(aligned)
                                .await
                                .map_err(|e| anyhow!("failed to write replacement rows: {e}"))?;
                        }
                    }
                }
            }
        }
    }

    if matched_rows == 0 {
        return Ok(None);
    }

    let added_files: Vec<DataFile> = match data_writer.as_mut() {
        Some(w) => w
            .close()
            .await
            .map_err(|e| anyhow!("failed to close replacement data file writer: {e}"))?,
        None => Vec::new(),
    };

    // ---- Snapshot production (mirrors iceberg-0.9.1 SnapshotProducer). ----
    let snapshot_id = generate_unique_snapshot_id(table);
    let next_seq = metadata.next_sequence_number();
    let meta_dir = format!("{}/metadata", metadata.location());

    // One rewritten manifest holding EXISTING (kept) + DELETED (removed)
    // entries from every touched source manifest...
    let mut new_manifests: Vec<ManifestFile> = carried;
    if !existing.is_empty() || !deleted.is_empty() {
        let mut writer = new_manifest_writer(
            table,
            snapshot_id,
            &format!("{meta_dir}/{commit_uuid}-m0.avro"),
        )?;
        for (df, snap, seq, fseq) in existing {
            writer
                .add_existing_file(df, snap, seq, fseq.or(Some(seq)))
                .map_err(|e| anyhow!("failed to add existing file entry: {e}"))?;
        }
        for (df, seq, fseq) in deleted {
            writer
                .add_delete_file(df, seq, fseq.or(Some(seq)))
                .map_err(|e| anyhow!("failed to add deleted file entry: {e}"))?;
        }
        new_manifests.push(
            writer
                .write_manifest_file()
                .await
                .map_err(|e| anyhow!("failed to write rewritten manifest: {e}"))?,
        );
    }
    // ... plus one manifest of ADDED replacement files.
    let (mut added_records, mut added_bytes) = (0u64, 0u64);
    if !added_files.is_empty() {
        let mut writer = new_manifest_writer(
            table,
            snapshot_id,
            &format!("{meta_dir}/{commit_uuid}-m1.avro"),
        )?;
        for df in &added_files {
            added_records += df.record_count();
            added_bytes += df.file_size_in_bytes();
            writer
                .add_file(df.clone(), UNASSIGNED_SEQUENCE_NUMBER)
                .map_err(|e| anyhow!("failed to add new file entry: {e}"))?;
        }
        new_manifests.push(
            writer
                .write_manifest_file()
                .await
                .map_err(|e| anyhow!("failed to write added manifest: {e}"))?,
        );
    }

    let manifest_list_path = format!("{meta_dir}/snap-{snapshot_id}-0-{commit_uuid}.avro");
    let mut list_writer = ManifestListWriter::v2(
        file_io
            .new_output(&manifest_list_path)
            .map_err(|e| anyhow!("failed to open manifest list output: {e}"))?,
        snapshot_id,
        metadata.current_snapshot_id(),
        next_seq,
    );
    let added_manifest_count = new_manifests.len();
    list_writer
        .add_manifests(new_manifests.into_iter())
        .map_err(|e| anyhow!("failed to append manifests to manifest list: {e}"))?;
    list_writer
        .close()
        .await
        .map_err(|e| anyhow!("failed to write manifest list: {e}"))?;

    // Snapshot summary. added/deleted counts are file-level (Iceberg spec
    // semantics — a rewritten file counts all its records on both sides);
    // totals are EXACT, recomputed from the final live set (kept + added),
    // every member of which was visited above.
    let operation = if added_files.is_empty() {
        Operation::Delete
    } else {
        Operation::Overwrite
    };
    let mut props: HashMap<String, String> = HashMap::new();
    props.insert("added-data-files".into(), added_files.len().to_string());
    props.insert("added-records".into(), added_records.to_string());
    props.insert("added-files-size".into(), added_bytes.to_string());
    props.insert("deleted-data-files".into(), removed_files.to_string());
    props.insert("deleted-records".into(), removed_records.to_string());
    props.insert("removed-files-size".into(), removed_bytes.to_string());
    props.insert(
        "total-data-files".into(),
        (kept_files + added_files.len() as u64).to_string(),
    );
    props.insert(
        "total-records".into(),
        (kept_records + added_records).to_string(),
    );
    props.insert(
        "total-files-size".into(),
        (kept_bytes + added_bytes).to_string(),
    );
    props.insert("total-delete-files".into(), "0".into());
    props.insert("total-position-deletes".into(), "0".into());
    props.insert("total-equality-deletes".into(), "0".into());
    props.insert("changed-partition-count".into(), "1".into());
    let _ = added_manifest_count; // manifest count is derivable; not summarized

    let snapshot = Snapshot::builder()
        .with_snapshot_id(snapshot_id)
        .with_parent_snapshot_id(metadata.current_snapshot_id())
        .with_sequence_number(next_seq)
        .with_timestamp_ms(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0),
        )
        .with_manifest_list(manifest_list_path)
        .with_summary(Summary {
            operation,
            additional_properties: props,
        })
        .with_schema_id(metadata.current_schema_id())
        .build();

    let request = CommitTableRequest {
        identifier: Some(table.identifier().clone()),
        requirements: vec![
            TableRequirement::UuidMatch {
                uuid: metadata.uuid(),
            },
            // Optimistic concurrency: `main` must still point where we
            // started, otherwise the catalog answers 409 and we recompute.
            TableRequirement::RefSnapshotIdMatch {
                r#ref: MAIN_BRANCH.to_string(),
                snapshot_id: metadata.current_snapshot_id(),
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

    Ok(Some(PreparedCommit {
        request,
        rows: matched_rows,
        snapshot_id,
    }))
}

/// Read one data file and decide its fate under the DML statement.
async fn classify_file(
    eval_ctx: &SessionContext,
    file_io: &iceberg::io::FileIO,
    data_file: &DataFile,
    sql: &DmlSql,
    stmt: &DmlStatement,
) -> Result<FileFate> {
    if data_file.file_format() != DataFileFormat::Parquet {
        bail!(
            "data file {} is not Parquet ({:?}); unsupported",
            data_file.file_path(),
            data_file.file_format()
        );
    }
    let bytes = file_io
        .new_input(data_file.file_path())
        .map_err(|e| anyhow!("bad data file path {}: {e}", data_file.file_path()))?
        .read()
        .await
        .map_err(|e| anyhow!("failed to read data file {}: {e}", data_file.file_path()))?;
    let reader = ParquetRecordBatchReaderBuilder::try_new(bytes)
        .with_context(|| format!("failed to open Parquet file {}", data_file.file_path()))?
        .build()
        .context("failed to build Parquet reader")?;
    let batches: Vec<RecordBatch> = reader
        .collect::<std::result::Result<_, _>>()
        .with_context(|| format!("failed to decode Parquet file {}", data_file.file_path()))?;
    let file_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
    if file_rows == 0 {
        return Ok(FileFate::Keep);
    }
    let batch_schema = batches[0].schema();

    // Evaluate against the file's rows registered under the real table name.
    let table_ref =
        datafusion::sql::TableReference::partial(stmt.namespace.as_str(), stmt.table.as_str());
    let mem = MemTable::try_new(batch_schema, vec![batches])
        .map_err(|e| anyhow!("failed to build in-memory eval table: {e}"))?;
    let _ = eval_ctx.deregister_table(table_ref.clone());
    eval_ctx
        .register_table(table_ref.clone(), Arc::new(mem))
        .map_err(|e| anyhow!("failed to register eval table: {e}"))?;
    let result = evaluate_file(eval_ctx, sql, file_rows).await;
    let _ = eval_ctx.deregister_table(table_ref);
    result
}

async fn evaluate_file(ctx: &SessionContext, sql: &DmlSql, file_rows: u64) -> Result<FileFate> {
    let matched = match &sql.count_matched {
        None => file_rows, // no WHERE clause: everything matches
        Some(count_sql) => {
            let batches = ctx
                .sql(count_sql)
                .await
                .map_err(|e| anyhow!("failed to plan DML predicate ({count_sql}): {e}"))?
                .collect()
                .await
                .map_err(|e| anyhow!("failed to evaluate DML predicate: {e}"))?;
            batches
                .first()
                .map(|b| b.column(0).as_primitive::<Int64Type>().value(0) as u64)
                .unwrap_or(0)
        }
    };
    if matched == 0 {
        return Ok(FileFate::Keep);
    }
    match &sql.rewrite {
        // DELETE matching the whole file: drop it, no replacement rows.
        None if matched == file_rows => Ok(FileFate::Remove { matched }),
        None => {
            // DELETE: survivors are rows where the predicate is not TRUE.
            let survivors_sql = sql
                .survivors
                .as_ref()
                .expect("survivors SQL exists for predicated DELETE");
            let batches = ctx
                .sql(survivors_sql)
                .await
                .map_err(|e| anyhow!("failed to plan DELETE survivors ({survivors_sql}): {e}"))?
                .collect()
                .await
                .map_err(|e| anyhow!("failed to compute DELETE survivors: {e}"))?;
            let survivor_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
            if survivor_rows + matched != file_rows {
                bail!(
                    "DELETE row accounting mismatch: {file_rows} rows, {matched} matched, \
                     {survivor_rows} survivors — refusing to commit"
                );
            }
            Ok(FileFate::Rewrite { matched, batches })
        }
        Some(rewrite_sql) => {
            // UPDATE: all rows survive; matched ones get new values.
            let batches = ctx
                .sql(rewrite_sql)
                .await
                .map_err(|e| anyhow!("failed to plan UPDATE rewrite ({rewrite_sql}): {e}"))?
                .collect()
                .await
                .map_err(|e| anyhow!("failed to compute UPDATE rewrite: {e}"))?;
            let rewritten_rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
            if rewritten_rows != file_rows {
                bail!(
                    "UPDATE row accounting mismatch: {file_rows} rows in, {rewritten_rows} out \
                     — refusing to commit"
                );
            }
            Ok(FileFate::Rewrite { matched, batches })
        }
    }
}

/// Pre-rendered SQL for per-file evaluation.
#[derive(Debug)]
struct DmlSql {
    /// `SELECT count(*) ... WHERE (pred)`; `None` when there is no WHERE.
    count_matched: Option<String>,
    /// DELETE survivors query (`None` for UPDATE or unpredicated DELETE).
    survivors: Option<String>,
    /// UPDATE projection query (`None` for DELETE).
    rewrite: Option<String>,
}

impl DmlSql {
    fn new<'a>(stmt: &DmlStatement, columns: impl Iterator<Item = &'a str>) -> Result<Self> {
        let columns: Vec<&str> = columns.collect();
        let from = match &stmt.alias {
            Some(alias) => format!(
                "{}.{} AS {}",
                quote_ident(&stmt.namespace),
                quote_ident(&stmt.table),
                quote_ident(alias)
            ),
            None => format!(
                "{}.{}",
                quote_ident(&stmt.namespace),
                quote_ident(&stmt.table)
            ),
        };
        let count_matched = stmt
            .predicate
            .as_ref()
            .map(|p| format!("SELECT count(*) FROM {from} WHERE ({p})"));
        let (survivors, rewrite) = match &stmt.kind {
            DmlKind::Delete => {
                let cols = columns
                    .iter()
                    .map(|c| quote_ident(c))
                    .collect::<Vec<_>>()
                    .join(", ");
                // `IS DISTINCT FROM TRUE`: rows where the predicate is FALSE
                // *or NULL* survive a DELETE (Postgres semantics).
                let survivors = stmt.predicate.as_ref().map(|p| {
                    format!("SELECT {cols} FROM {from} WHERE ({p}) IS DISTINCT FROM TRUE")
                });
                (survivors, None)
            }
            DmlKind::Update { assignments } => {
                let assigned: HashMap<&str, &str> = assignments
                    .iter()
                    .map(|(c, v)| (c.as_str(), v.as_str()))
                    .collect();
                for (col, _) in assignments {
                    if !columns.iter().any(|c| c == col) {
                        bail!(
                            "column \"{col}\" of relation \"{}\" does not exist",
                            stmt.table
                        );
                    }
                }
                let projection = columns
                    .iter()
                    .map(|c| {
                        let q = quote_ident(c);
                        match (assigned.get(*c), &stmt.predicate) {
                            (Some(expr), Some(p)) => {
                                // Predicate NULL/FALSE keeps the old value —
                                // exactly Postgres UPDATE semantics.
                                format!("CASE WHEN ({p}) THEN ({expr}) ELSE {q} END AS {q}")
                            }
                            (Some(expr), None) => format!("({expr}) AS {q}"),
                            (None, _) => q,
                        }
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                (None, Some(format!("SELECT {projection} FROM {from}")))
            }
        };
        Ok(Self {
            count_matched,
            survivors,
            rewrite,
        })
    }
}

/// Cast a DataFusion output batch onto the table's canonical Arrow schema
/// (field-id annotated, exact Iceberg types) so the Parquet writer receives
/// exactly what an INSERT would produce. Fails loudly on any incompatible
/// column (no silent coercion to a wrong shape).
fn align_batch(batch: &RecordBatch, target: &ArrowSchemaRef) -> Result<RecordBatch> {
    if batch.num_columns() != target.fields().len() {
        bail!(
            "rewritten batch has {} columns, table has {}",
            batch.num_columns(),
            target.fields().len()
        );
    }
    let mut columns = Vec::with_capacity(batch.num_columns());
    for (i, field) in target.fields().iter().enumerate() {
        let src = batch.column(i);
        let src_name = batch.schema().field(i).name().clone();
        if !src_name.eq_ignore_ascii_case(field.name()) {
            bail!(
                "rewritten column {i} is named {src_name:?}, expected {:?}",
                field.name()
            );
        }
        let col: Arc<dyn Array> = if src.data_type() == field.data_type() {
            src.clone()
        } else {
            cast_with_options(
                src,
                field.data_type(),
                &CastOptions {
                    safe: false,
                    format_options: Default::default(),
                },
            )
            .with_context(|| {
                format!(
                    "cannot cast column {:?} from {} to {}",
                    field.name(),
                    src.data_type(),
                    field.data_type()
                )
            })?
        };
        columns.push(col);
    }
    RecordBatch::try_new(target.clone(), columns)
        .map_err(|e| anyhow!("rewritten rows do not fit the table schema (nullability/type): {e}"))
}

/// The standard iceberg-rust data-file writer stack (identical to the one
/// iceberg-datafusion's INSERT path builds): rolling Parquet writer with
/// default target file size, writing under the table's data location.
async fn new_data_writer(
    table: &Table,
    commit_uuid: &Uuid,
) -> Result<impl IcebergWriter<arrow::array::RecordBatch, Vec<DataFile>>> {
    let parquet_builder = ParquetWriterBuilder::new_with_match_mode(
        datafusion::parquet::file::properties::WriterProperties::default(),
        table.metadata().current_schema().clone(),
        FieldMatchMode::Name,
    );
    let location_generator = DefaultLocationGenerator::new(table.metadata().clone())
        .map_err(|e| anyhow!("failed to build location generator: {e}"))?;
    let file_name_generator =
        DefaultFileNameGenerator::new(format!("dml-{commit_uuid}"), None, DataFileFormat::Parquet);
    let rolling = RollingFileWriterBuilder::new_with_default_file_size(
        parquet_builder,
        table.file_io().clone(),
        location_generator,
        file_name_generator,
    );
    DataFileWriterBuilder::new(rolling)
        .build(None)
        .await
        .map_err(|e| anyhow!("failed to build data file writer: {e}"))
}

fn new_manifest_writer(table: &Table, snapshot_id: i64, path: &str) -> Result<ManifestWriter> {
    let output = table
        .file_io()
        .new_output(path)
        .map_err(|e| anyhow!("failed to open manifest output {path}: {e}"))?;
    Ok(ManifestWriterBuilder::new(
        output,
        Some(snapshot_id),
        None,
        table.metadata().current_schema().clone(),
        table.metadata().default_partition_spec().as_ref().clone(),
    )
    .build_v2_data())
}

/// Random snapshot id not colliding with any existing one (same scheme as
/// iceberg-rust's `SnapshotProducer`).
fn generate_unique_snapshot_id(table: &Table) -> i64 {
    loop {
        let (lhs, rhs) = Uuid::new_v4().as_u64_pair();
        let id = ((lhs ^ rhs) as i64).abs();
        if !table.metadata().snapshots().any(|s| s.snapshot_id() == id) {
            return id;
        }
    }
}

/// Sabotage the `assert-ref-snapshot-id` requirement so the catalog is
/// guaranteed to reject the commit with 409 (test-only; see module docs).
fn corrupt_ref_requirement(request: &mut CommitTableRequest) {
    for req in &mut request.requirements {
        if let TableRequirement::RefSnapshotIdMatch { snapshot_id, .. } = req {
            *snapshot_id = Some(snapshot_id.map(|id| id.wrapping_add(1)).unwrap_or(1));
        }
    }
}

/// Double-quote (and escape) a SQL identifier.
fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

/// Minimal percent-encoding for URL path segments / query values.
fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn stmt(kind: DmlKind, predicate: Option<&str>) -> DmlStatement {
        DmlStatement {
            kind,
            namespace: "demo".into(),
            table: "trips".into(),
            alias: None,
            predicate: predicate.map(str::to_string),
        }
    }

    #[test]
    fn delete_sql_uses_is_distinct_from_true_for_survivors() {
        let s = stmt(DmlKind::Delete, Some("trip_id = 7"));
        let sql = DmlSql::new(&s, ["trip_id", "city"].into_iter()).unwrap();
        assert_eq!(
            sql.count_matched.as_deref(),
            Some("SELECT count(*) FROM \"demo\".\"trips\" WHERE (trip_id = 7)")
        );
        assert_eq!(
            sql.survivors.as_deref(),
            Some(
                "SELECT \"trip_id\", \"city\" FROM \"demo\".\"trips\" \
                 WHERE (trip_id = 7) IS DISTINCT FROM TRUE"
            )
        );
        assert!(sql.rewrite.is_none());
    }

    #[test]
    fn update_sql_wraps_assignments_in_case_when() {
        let s = stmt(
            DmlKind::Update {
                assignments: vec![("fare".into(), "99.9".into())],
            },
            Some("trip_id = 7"),
        );
        let sql = DmlSql::new(&s, ["trip_id", "fare"].into_iter()).unwrap();
        assert_eq!(
            sql.rewrite.as_deref(),
            Some(
                "SELECT \"trip_id\", \
                 CASE WHEN (trip_id = 7) THEN (99.9) ELSE \"fare\" END AS \"fare\" \
                 FROM \"demo\".\"trips\""
            )
        );
        assert!(sql.survivors.is_none());
    }

    #[test]
    fn update_rejects_unknown_column() {
        let s = stmt(
            DmlKind::Update {
                assignments: vec![("nope".into(), "1".into())],
            },
            None,
        );
        let err = DmlSql::new(&s, ["trip_id"].into_iter()).unwrap_err();
        assert!(err.to_string().contains("does not exist"));
    }

    #[test]
    fn unpredicated_update_has_no_count_query() {
        let s = stmt(
            DmlKind::Update {
                assignments: vec![("fare".into(), "0".into())],
            },
            None,
        );
        let sql = DmlSql::new(&s, ["fare"].into_iter()).unwrap();
        assert!(sql.count_matched.is_none());
        assert_eq!(
            sql.rewrite.as_deref(),
            Some("SELECT (0) AS \"fare\" FROM \"demo\".\"trips\"")
        );
    }

    #[test]
    fn alias_is_preserved_in_from_clause() {
        let s = DmlStatement {
            kind: DmlKind::Delete,
            namespace: "demo".into(),
            table: "trips".into(),
            alias: Some("t".into()),
            predicate: Some("t.trip_id = 7".into()),
        };
        let sql = DmlSql::new(&s, ["trip_id"].into_iter()).unwrap();
        assert!(sql
            .count_matched
            .as_deref()
            .unwrap()
            .contains("FROM \"demo\".\"trips\" AS \"t\""));
    }

    #[test]
    fn urlencode_escapes_non_unreserved() {
        assert_eq!(urlencode("demo"), "demo");
        assert_eq!(urlencode("a b/c"), "a%20b%2Fc");
    }
}
