//! Copy-on-write write engine over Iceberg (SPEC B2/B3/B4/B5), zero data
//! replication: a sequence of buffered operations against one table becomes
//! ONE new Iceberg snapshot that reuses every untouched data file and
//! rewrites only the files that actually contain affected rows.
//!
//! Three entry points share the same snapshot-production core
//! ([`prepare_commit`]):
//!
//! * [`OverwriteEngine::execute`] — one autocommit UPDATE/DELETE
//!   (B2/B3), anchored at the current `main` snapshot with bounded
//!   refresh-and-retry on optimistic-concurrency conflicts;
//! * [`OverwriteEngine::insert_enforced`] — one autocommit INSERT under
//!   `--enforce-pk` (B5): uniqueness is validated against the same snapshot
//!   the commit is anchored to, and a 409 retry re-validates against fresh
//!   metadata, so two racing INSERTs of the same key can never both land;
//! * [`OverwriteEngine::commit_pinned`] — an explicit transaction's COMMIT
//!   (B4): the whole buffered op list (appends + UPDATE/DELETE, in statement
//!   order) is applied as one snapshot anchored at the snapshot pinned at
//!   BEGIN. NO retry: if `main` moved since BEGIN the commit aborts with a
//!   serialization failure (first-committer-wins snapshot isolation) —
//!   retrying silently would invalidate the row counts already reported to
//!   the client at statement time.
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
//! # Algorithm (per prepared commit)
//!
//! 1. Caller supplies a freshly-loaded table; its current `main` snapshot id
//!    becomes the optimistic-concurrency anchor.
//! 2. If the op list contains any UPDATE/DELETE (or a PK must be enforced),
//!    every live data file is read once and the DML ops are folded over its
//!    rows in statement order; the file is classified **kept** (no op
//!    matched — the common case, zero-copy), **removed** (nothing survived),
//!    or **rewritten**. Append-only op lists skip the file scan entirely.
//! 3. Buffered append batches are themselves folded through every DML op
//!    that came *after* them in the transaction, then written (together with
//!    rewritten survivors) to new Parquet file(s) with the standard
//!    iceberg-rust writer stack.
//! 4. A new manifest records ADDED/EXISTING/DELETED entries; manifests whose
//!    files are all kept are carried into the new manifest list untouched.
//! 5. The new snapshot (`append`, `overwrite`, or `delete`) is committed
//!    with requirements `assert-table-uuid` + `assert-ref-snapshot-id
//!    main=<anchor>`.
//!
//! # Atomicity, durability, concurrency
//!
//! The catalog commit is the *only* mutation: readers see either the old
//! snapshot or the new one, never an intermediate state — this is what makes
//! a multi-statement transaction's COMMIT atomic per table. Parquet/Avro
//! files written by a failed or abandoned attempt are unreferenced by any
//! snapshot and therefore harmless orphans (standard Iceberg semantics).
//! Time travel keeps working: pre-commit snapshots and their files are never
//! mutated.
//!
//! Setting `ICEGRES_DML_INJECT_CONFLICT=1` (test-only knob) deliberately
//! corrupts the first attempt's `assert-ref-snapshot-id` requirement in
//! [`OverwriteEngine::execute`] so the catalog rejects it with 409, proving
//! the server-side check and the refresh-and-retry path end to end (used by
//! icegres/tests/e2e.sh).
//!
//! # Primary-key enforcement (SPEC B5, opt-in via `--enforce-pk`)
//!
//! A table declares its key with the table property
//! `icegres.primary-key = "col[,col...]"`. When enforcement is on and a
//! commit appends rows or rewrites a PK column, the FINAL row set's key
//! columns (kept files are read key-columns-only) are checked for NULLs and
//! duplicates before the commit is posted; violations abort with Postgres
//! sqlstates 23502/23505. Because the check runs against the very snapshot
//! the commit is anchored to, a concurrent writer cannot sneak a duplicate
//! past it: either the check saw their rows, or the commit 409s and the
//! retry re-checks. Enforcement is off by default — it makes every INSERT
//! read the key columns of every live data file.
//!
//! # Bounds & limitations (fail loudly, never wrong)
//!
//! * Format v2, unpartitioned tables, Parquet data files, no delete
//!   manifests — anything else is rejected before any write.
//! * With DML ops (or PK enforcement) every live data file is read once per
//!   commit, one file at a time, so peak memory is one data file's decoded
//!   batches (plus the buffered transaction rows and, under enforcement,
//!   the table's key columns).
//! * Predicates/assignment values must be self-contained row expressions:
//!   subqueries are rejected (they would otherwise be evaluated per-file
//!   and yield wrong answers).

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, OnceLock};
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
/// after 409 conflicts) for AUTOCOMMIT statements. Each retry recomputes
/// from fresh table metadata. Explicit transactions never retry (see
/// [`OverwriteEngine::commit_pinned`]).
pub const MAX_COMMIT_ATTEMPTS: u32 = 3;

/// Table property naming the enforced primary-key columns (comma-separated).
pub const PK_PROPERTY: &str = "icegres.primary-key";

/// The Iceberg REST capability identifier for the multi-table transaction
/// endpoint, exactly as advertised in `GET /v1/config`'s `endpoints` array
/// (spec form: `"<HTTP verb> <path template>"`).
const TXN_COMMIT_ENDPOINT: &str = "POST /v1/{prefix}/transactions/commit";

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

/// One buffered operation against a table, in statement order.
#[derive(Debug, Clone)]
pub enum TableOp {
    /// Rows appended by INSERT (already aligned to the table Arrow schema).
    Append(Vec<RecordBatch>),
    /// A buffered UPDATE/DELETE.
    Dml(DmlStatement),
}

/// A constraint violation (opt-in PK enforcement), carrying the Postgres
/// sqlstate the wire layer should answer with (23502/23505).
#[derive(Debug)]
pub struct ConstraintViolation {
    pub sqlstate: &'static str,
    pub message: String,
}

impl std::fmt::Display for ConstraintViolation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}
impl std::error::Error for ConstraintViolation {}

/// A transaction COMMIT lost the optimistic-concurrency race (another
/// writer moved `main` after BEGIN). Maps to Postgres sqlstate 40001
/// (serialization_failure) on the wire; the client should retry the whole
/// transaction.
#[derive(Debug)]
pub struct CommitConflict {
    pub message: String,
}

impl std::fmt::Display for CommitConflict {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.message)
    }
}
impl std::error::Error for CommitConflict {}

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

/// Executes copy-on-write commits against one Iceberg REST catalog.
pub struct OverwriteEngine {
    catalog: Arc<dyn Catalog>,
    http: reqwest::Client,
    /// REST base, e.g. `http://127.0.0.1:8181/catalog`.
    catalog_uri: String,
    /// Catalog path prefix from `GET /v1/config` (may be empty).
    prefix: String,
    /// Opt-in PK enforcement (`--enforce-pk`); when off, `pk_columns`
    /// always answers `None` and no enforcement work happens anywhere.
    enforce_pk: bool,
    /// Snapshot ref (branch) every commit anchors to and publishes on
    /// (`--branch`, SPEC D6). Default `main` — byte-for-byte the historical
    /// behavior. A non-`main` branch must already exist on the target table
    /// (`icegres branch create`); commits carry
    /// `assert-ref-snapshot-id <branch>=<head>` so two servers on different
    /// branches of the same table never conflict with each other.
    branch: String,
    /// Whether the catalog implements the multi-table transaction endpoint
    /// (`POST /v1/{prefix}/transactions/commit`). Seeded from the config
    /// response's `endpoints` capability list when the catalog advertises
    /// one (Lakekeeper does); otherwise resolved on first use by an explicit
    /// DATA-FREE probe ([`Self::probe_txn_endpoint`]) — never learned from a
    /// real commit's response, so a commit-level 404 (e.g. a missing table)
    /// or a transient routing failure can never poison this cache. Unset =
    /// unknown yet.
    txn_endpoint: OnceLock<bool>,
}

impl OverwriteEngine {
    /// Build an engine over an already-connected catalog, resolving the REST
    /// path prefix from `GET /v1/config?warehouse=...` (same handshake the
    /// REST catalog client performs). `branch = None` means `main`.
    pub async fn connect(
        catalog: Arc<dyn Catalog>,
        opts: &CatalogOpts,
        enforce_pk: bool,
        branch: Option<String>,
    ) -> Result<Self> {
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
        // Multi-table transaction capability. Test-only knob
        // ICEGRES_TXN_DISABLE_ATOMIC forces the "catalog lacks the endpoint"
        // path so the ordered fallback (and strict-mode refusal) stays
        // e2e-provable against a catalog that DOES support it.
        let txn_endpoint = OnceLock::new();
        if std::env::var_os("ICEGRES_TXN_DISABLE_ATOMIC").is_some() {
            tracing::warn!(
                "ICEGRES_TXN_DISABLE_ATOMIC set (test-only): treating the catalog as if it \
                 lacked {TXN_COMMIT_ENDPOINT:?}"
            );
            let _ = txn_endpoint.set(false);
        } else if let Some(endpoints) = config.get("endpoints").and_then(|v| v.as_array()) {
            // The catalog advertises its capabilities: trust the list.
            let supported = endpoints
                .iter()
                .any(|e| e.as_str() == Some(TXN_COMMIT_ENDPOINT));
            let _ = txn_endpoint.set(supported);
        }
        Ok(Self {
            catalog,
            http,
            catalog_uri: opts.catalog_uri.trim_end_matches('/').to_string(),
            prefix,
            enforce_pk,
            branch: branch.unwrap_or_else(|| MAIN_BRANCH.to_string()),
            txn_endpoint,
        })
    }

    /// Whether `--enforce-pk` is on for this engine.
    pub fn enforce_pk(&self) -> bool {
        self.enforce_pk
    }

    /// The snapshot ref this engine commits to (`main` by default).
    pub fn branch(&self) -> &str {
        &self.branch
    }

    /// Whether this engine writes to the `main` branch (the default mode,
    /// where autocommit INSERTs without PK enforcement may use the stock
    /// fast_append path).
    pub fn is_main_branch(&self) -> bool {
        self.branch == MAIN_BRANCH
    }

    /// The enforced PK columns of `table`, or `None` when enforcement is off
    /// or the table declares no `icegres.primary-key` property. Unknown
    /// columns in the property are a loud error, never silently ignored.
    pub fn pk_columns(&self, table: &Table) -> Result<Option<Vec<String>>> {
        if !self.enforce_pk {
            return Ok(None);
        }
        pk_columns_of(table)
    }

    /// Execute one AUTOCOMMIT DML statement: classify/rewrite data files,
    /// produce an overwrite snapshot, and commit it via the REST catalog
    /// with bounded optimistic-concurrency retries.
    pub async fn execute(&self, stmt: &DmlStatement) -> Result<DmlOutcome> {
        let ident = TableIdent::from_strs([stmt.namespace.as_str(), stmt.table.as_str()])
            .map_err(|e| anyhow!("bad table identifier: {e}"))?;
        let ops = [TableOp::Dml(stmt.clone())];
        let mut conflicts: Vec<String> = Vec::new();
        for attempt in 1..=MAX_COMMIT_ATTEMPTS {
            let table = self
                .catalog
                .load_table(&ident)
                .await
                .map_err(|e| anyhow!("failed to load table {ident}: {e}"))?;
            let pk = self.pk_columns(&table)?;
            let prepared = prepare_commit(&table, &ops, pk.as_deref(), &self.branch, None)
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
            let rows = prepared.rows_by_op[0];
            match self
                .post_commit(&stmt.namespace, &stmt.table, &prepared.request)
                .await?
            {
                CommitOutcome::Committed => {
                    tracing::info!(
                        table = %ident,
                        rows,
                        snapshot_id = prepared.snapshot_id,
                        attempt,
                        "DML committed (copy-on-write overwrite snapshot)"
                    );
                    return Ok(DmlOutcome {
                        rows,
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

    /// Commit one AUTOCOMMIT INSERT under PK enforcement. The uniqueness
    /// check runs against the same snapshot the commit is anchored to; a 409
    /// retry reloads fresh metadata and re-checks, so racing duplicate
    /// INSERTs cannot both land (one commits, the other sees the committed
    /// row on retry and fails with 23505).
    pub async fn insert_enforced(
        &self,
        ident: &TableIdent,
        batches: Vec<RecordBatch>,
    ) -> Result<DmlOutcome> {
        let rows: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
        let ops = [TableOp::Append(batches)];
        let mut conflicts: Vec<String> = Vec::new();
        for attempt in 1..=MAX_COMMIT_ATTEMPTS {
            let table = self
                .catalog
                .load_table(ident)
                .await
                .map_err(|e| anyhow!("failed to load table {ident}: {e}"))?;
            let pk = self.pk_columns(&table)?;
            let prepared = prepare_commit(&table, &ops, pk.as_deref(), &self.branch, None)
                .await
                .with_context(|| format!("INSERT into {ident} failed"))?;
            let Some(prepared) = prepared else {
                return Ok(DmlOutcome {
                    rows: 0,
                    attempts: attempt,
                    snapshot_id: None,
                });
            };
            match self
                .post_commit(
                    &ident.namespace().to_url_string(),
                    ident.name(),
                    &prepared.request,
                )
                .await?
            {
                CommitOutcome::Committed => {
                    return Ok(DmlOutcome {
                        rows,
                        attempts: attempt,
                        snapshot_id: Some(prepared.snapshot_id),
                    });
                }
                CommitOutcome::Conflict(msg) => {
                    tracing::warn!(
                        table = %ident,
                        attempt,
                        "INSERT commit conflict (409), re-validating against fresh metadata: {msg}"
                    );
                    conflicts.push(msg);
                }
            }
        }
        bail!(
            "INSERT into {ident} lost the optimistic-concurrency race {MAX_COMMIT_ATTEMPTS} \
             times; giving up (no partial effects were committed). Conflicts: {}",
            conflicts.join(" | ")
        )
    }

    /// Commit a transaction's buffered op list for one table as ONE snapshot
    /// anchored at the snapshot pinned at BEGIN (`expected_head`, the head of
    /// this engine's branch at pin time).
    ///
    /// NO retry on conflict: statement-level results (row counts, reads)
    /// were computed against the pin, so if the branch moved the only honest
    /// outcome is a serialization failure ([`CommitConflict`], wire sqlstate
    /// 40001) — exactly what Postgres REPEATABLE READ does. Returns the new
    /// snapshot id, or `None` when the ops net out to no change.
    pub async fn commit_pinned(
        &self,
        ident: &TableIdent,
        expected_head: Option<i64>,
        ops: &[TableOp],
    ) -> Result<Option<i64>> {
        // Fresh metadata for correct sequence numbers / uuid; the pin only
        // anchors the ref requirement. If the branch already moved, abort
        // cheaply before doing any work (the POSTed requirement is the real
        // guard).
        let table = self
            .catalog
            .load_table(ident)
            .await
            .map_err(|e| anyhow!("failed to load table {ident}: {e}"))?;
        let fresh_head = branch_head(table.metadata(), &self.branch)?.map(|s| s.snapshot_id());
        if fresh_head != expected_head {
            let show = |s: Option<i64>| {
                s.map(|id| id.to_string())
                    .unwrap_or_else(|| "<none>".to_string())
            };
            return Err(anyhow!(CommitConflict {
                message: format!(
                    "could not serialize access due to concurrent update: table {ident} \
                     (branch {}) moved from snapshot {} (pinned at BEGIN) to {}; retry \
                     the transaction",
                    self.branch,
                    show(expected_head),
                    show(fresh_head)
                ),
            }));
        }
        let pk = self.pk_columns(&table)?;
        let prepared = prepare_commit(&table, ops, pk.as_deref(), &self.branch, None)
            .await
            .with_context(|| format!("transaction COMMIT against {ident} failed"))?;
        let Some(prepared) = prepared else {
            return Ok(None);
        };
        match self
            .post_commit(
                &ident.namespace().to_url_string(),
                ident.name(),
                &prepared.request,
            )
            .await?
        {
            CommitOutcome::Committed => {
                tracing::info!(
                    table = %ident,
                    snapshot_id = prepared.snapshot_id,
                    ops = ops.len(),
                    "transaction committed (single composed snapshot)"
                );
                Ok(Some(prepared.snapshot_id))
            }
            CommitOutcome::Conflict(msg) => Err(anyhow!(CommitConflict {
                message: format!(
                    "could not serialize access due to concurrent update: catalog rejected \
                     the commit for {ident} (409): {msg}; retry the transaction"
                ),
            })),
        }
    }

    /// Commit a transaction's buffered op lists for SEVERAL tables as ONE
    /// atomic multi-table catalog transaction
    /// (`POST /v1/{prefix}/transactions/commit`, Iceberg REST spec;
    /// implemented by Lakekeeper). Per table the request carries exactly the
    /// requirements/updates [`commit_pinned`] would post — including the
    /// `assert-ref-snapshot-id <branch>=<pin>` anchor — so the semantics are
    /// the single-table semantics, made all-or-nothing across tables:
    ///
    /// * every table commits, or NONE does (the catalog checks all
    ///   requirements against freshly-loaded metadata before touching
    ///   anything, and applies all pointer swaps in one transaction);
    /// * any conflict (a branch moved since its pin) is a
    ///   [`CommitConflict`] → wire sqlstate 40001, retryable because
    ///   nothing was applied;
    /// * NO retry, same reason as [`commit_pinned`]: statement-time row
    ///   counts were computed against the pins.
    ///
    /// Returns [`MultiTableCommit::Unsupported`] — with nothing applied AND
    /// nothing staged — when the catalog does not implement the endpoint
    /// (known from the config capability list, or resolved here by a
    /// data-free probe BEFORE any table is prepared); the caller falls back
    /// to the ordered per-table path (or refuses, in strict mode) without a
    /// single data file having been written twice.
    pub async fn commit_pinned_multi(
        &self,
        tables: &[(&TableIdent, Option<i64>, &[TableOp])],
    ) -> Result<MultiTableCommit> {
        // Resolve the endpoint capability BEFORE staging (writing Parquet
        // for) any table: when unknown this costs one data-free probe, so an
        // unsupported catalog is discovered — and strict mode can refuse —
        // with zero staging work done.
        if !self.probe_txn_endpoint().await? {
            return Ok(MultiTableCommit::Unsupported);
        }
        // Prepare every table first (all data/manifest files durable before
        // any catalog mutation), with the same cheap pre-check as
        // commit_pinned so a stale pin aborts before staging N tables.
        let mut prepared_all: Vec<Option<PreparedCommit>> = Vec::with_capacity(tables.len());
        for (ident, expected_head, ops) in tables {
            let table = self
                .catalog
                .load_table(ident)
                .await
                .map_err(|e| anyhow!("failed to load table {ident}: {e}"))?;
            let fresh_head = branch_head(table.metadata(), &self.branch)?.map(|s| s.snapshot_id());
            if fresh_head != *expected_head {
                let show = |s: Option<i64>| {
                    s.map(|id| id.to_string())
                        .unwrap_or_else(|| "<none>".to_string())
                };
                return Err(anyhow!(CommitConflict {
                    message: format!(
                        "could not serialize access due to concurrent update: table {ident} \
                         (branch {}) moved from snapshot {} (pinned at BEGIN) to {}; nothing \
                         was applied — retry the transaction",
                        self.branch,
                        show(*expected_head),
                        show(fresh_head)
                    ),
                }));
            }
            let pk = self.pk_columns(&table)?;
            let prepared = prepare_commit(&table, ops, pk.as_deref(), &self.branch, None)
                .await
                .with_context(|| format!("transaction COMMIT against {ident} failed"))?;
            prepared_all.push(prepared);
        }
        let live: Vec<&PreparedCommit> = prepared_all.iter().flatten().collect();
        if live.is_empty() {
            // Every table's ops net out to no change: nothing to commit.
            return Ok(MultiTableCommit::Committed);
        }
        let requests: Vec<&CommitTableRequest> = live.iter().map(|p| &p.request).collect();
        match self.post_transaction(&requests).await? {
            TxnCommitOutcome::Committed => {
                let snapshot_ids: Vec<i64> = live.iter().map(|p| p.snapshot_id).collect();
                tracing::info!(
                    tables = tables.len(),
                    committed = live.len(),
                    snapshot_ids = ?snapshot_ids,
                    "transaction committed atomically via transactions/commit \
                     (one all-or-nothing multi-table catalog commit)"
                );
                Ok(MultiTableCommit::Committed)
            }
            TxnCommitOutcome::Conflict(msg) => Err(anyhow!(CommitConflict {
                message: format!(
                    "could not serialize access due to concurrent update: catalog rejected \
                     the multi-table transaction commit (409): {msg}; nothing was applied — \
                     retry the transaction"
                ),
            })),
            TxnCommitOutcome::Unsupported => Ok(MultiTableCommit::Unsupported),
        }
    }

    /// POST an externally-prepared commit (see [`prepare_commit`]) for
    /// `ident`. Used by the write buffer's group-commit flusher, which
    /// needs the prepare/post split so it can tag in-flight rows with the
    /// new snapshot id between the two steps (buffer.rs).
    pub async fn post_prepared(
        &self,
        ident: &TableIdent,
        prepared: &PreparedCommit,
    ) -> Result<CommitOutcome> {
        self.post_commit(
            &ident.namespace().to_url_string(),
            ident.name(),
            &prepared.request,
        )
        .await
    }

    /// Create branch `name` on `ident` pointing at `at_snapshot` (must exist
    /// in the table's metadata) or, when `None`, at the current `main` head.
    /// Zero-copy: the commit only adds a snapshot ref — no data or metadata
    /// file is rewritten. The `assert-ref-snapshot-id <name>=null`
    /// requirement makes the create atomic: if the ref already exists the
    /// catalog answers 409 and this fails loudly. When forking from `main`'s
    /// head (no `at_snapshot`), the request additionally anchors
    /// `assert-ref-snapshot-id main=<head>` so the branch is guaranteed to
    /// fork the head that was actually read — a concurrent commit yields a
    /// clean 409 (retry) instead of silently branching a superseded state.
    /// An explicit `at_snapshot` carries no main anchor: the user chose the
    /// snapshot, main is free to move. Returns the snapshot id the new
    /// branch points at.
    pub async fn create_branch(
        &self,
        ident: &TableIdent,
        name: &str,
        at_snapshot: Option<i64>,
    ) -> Result<i64> {
        anyhow::ensure!(
            name != MAIN_BRANCH,
            "branch {MAIN_BRANCH:?} always exists; pick another name"
        );
        let table = self
            .catalog
            .load_table(ident)
            .await
            .map_err(|e| anyhow!("failed to load table {ident}: {e}"))?;
        let metadata = table.metadata();
        let src = match at_snapshot {
            Some(id) => {
                anyhow::ensure!(
                    metadata.snapshot_by_id(id).is_some(),
                    "snapshot {id} does not exist in table {ident}"
                );
                id
            }
            None => metadata.current_snapshot_id().ok_or_else(|| {
                anyhow!("table {ident} has no snapshot yet; write to it before branching")
            })?,
        };
        let request =
            set_branch_ref_request(ident, metadata.uuid(), name, src, at_snapshot.is_none());
        match self
            .post_commit(&ident.namespace().to_url_string(), ident.name(), &request)
            .await?
        {
            CommitOutcome::Committed => Ok(src),
            CommitOutcome::Conflict(msg) => bail!(
                "branch {name:?} already exists on {ident}, or main moved concurrently \
                 (nothing was applied — retry the create): {msg}"
            ),
        }
    }

    /// Drop branch `name` from `ident`. Zero-copy and non-destructive: only
    /// the ref is removed; the snapshots it pointed at stay in table
    /// metadata (time travel keeps working) until snapshot expiry. Anchored
    /// with `assert-ref-snapshot-id <name>=<head>` so a concurrent commit to
    /// the branch is never silently discarded. Returns the head the branch
    /// pointed at when dropped.
    pub async fn drop_branch(&self, ident: &TableIdent, name: &str) -> Result<i64> {
        anyhow::ensure!(
            name != MAIN_BRANCH,
            "refusing to drop {MAIN_BRANCH:?} — it is the table's default branch"
        );
        let table = self
            .catalog
            .load_table(ident)
            .await
            .map_err(|e| anyhow!("failed to load table {ident}: {e}"))?;
        let head = table
            .metadata()
            .snapshot_for_ref(name)
            .ok_or_else(|| anyhow!("branch {name:?} does not exist on table {ident}"))?
            .snapshot_id();
        let request = remove_branch_ref_request(ident, table.metadata().uuid(), name, head);
        match self
            .post_commit(&ident.namespace().to_url_string(), ident.name(), &request)
            .await?
        {
            CommitOutcome::Committed => Ok(head),
            CommitOutcome::Conflict(msg) => bail!(
                "branch {name:?} on {ident} moved while dropping it (concurrent commit?): {msg}"
            ),
        }
    }

    /// Every table in the catalog — NESTED namespaces included: the Iceberg
    /// REST `list_namespaces` answers one level per call, so this walks the
    /// namespace tree depth-first (`list_namespaces(Some(parent))` per
    /// namespace), accumulating tables at every level. A visited set guards
    /// (defensively) against a catalog answering cyclic or duplicated
    /// listings; the result is sorted by qualified name for deterministic
    /// whole-lakehouse operations.
    pub async fn list_all_tables(&self) -> Result<Vec<TableIdent>> {
        let mut out: Vec<TableIdent> = Vec::new();
        let mut visited: HashSet<iceberg::NamespaceIdent> = HashSet::new();
        let mut stack: Vec<iceberg::NamespaceIdent> = self
            .catalog
            .list_namespaces(None)
            .await
            .map_err(|e| anyhow!("failed to list namespaces: {e}"))?;
        while let Some(ns) = stack.pop() {
            if !visited.insert(ns.clone()) {
                // Defensive: a misbehaving catalog repeating a namespace (or
                // cycling) must not loop or double-count.
                continue;
            }
            let tables = self
                .catalog
                .list_tables(&ns)
                .await
                .map_err(|e| anyhow!("failed to list tables of namespace {ns:?}: {e}"))?;
            out.extend(tables);
            let children = self
                .catalog
                .list_namespaces(Some(&ns))
                .await
                .map_err(|e| anyhow!("failed to list child namespaces of {ns:?}: {e}"))?;
            stack.extend(children);
        }
        out.sort_by_key(|i| i.to_string());
        Ok(out)
    }

    /// `icegres branch create-all`: create branch `name` on EVERY table in
    /// the catalog in ONE atomic multi-table transaction — a
    /// consistent-or-nothing cross-table cut of the whole lakehouse. Each
    /// table carries the `assert-ref-snapshot-id <name>=null` creation guard
    /// AND an `assert-ref-snapshot-id main=<head captured at load>` anchor:
    /// the commit succeeds only if every captured `main` head is STILL
    /// current at commit time, so the cut can never show half of a
    /// concurrent (even atomic multi-table) commit that landed between the
    /// per-table loads — any such race is a clean 409 with nothing applied
    /// (retry the create-all). A table without a snapshot cannot hold a
    /// ref at all (Iceberg refs point at snapshots) and is SKIPPED — the cut
    /// covers every table that has history; skipped tables are returned so
    /// the caller can warn loudly. Requires a catalog that implements
    /// `transactions/commit`; without it the command errors cleanly rather
    /// than applying a partial cut. Returns the branched
    /// `(table, snapshot_id)` pairs plus the skipped (snapshot-less) tables.
    #[allow(clippy::type_complexity)]
    pub async fn create_branch_all(
        &self,
        name: &str,
    ) -> Result<(Vec<(TableIdent, i64)>, Vec<TableIdent>)> {
        anyhow::ensure!(
            name != MAIN_BRANCH,
            "branch {MAIN_BRANCH:?} always exists; pick another name"
        );
        let idents = self.list_all_tables().await?;
        anyhow::ensure!(!idents.is_empty(), "the catalog has no tables to branch");
        let mut requests: Vec<CommitTableRequest> = Vec::with_capacity(idents.len());
        let mut branched: Vec<(TableIdent, i64)> = Vec::with_capacity(idents.len());
        let mut skipped: Vec<TableIdent> = Vec::new();
        for ident in &idents {
            let table = self
                .catalog
                .load_table(ident)
                .await
                .map_err(|e| anyhow!("failed to load table {ident}: {e}"))?;
            let metadata = table.metadata();
            let Some(src) = metadata.current_snapshot_id() else {
                skipped.push(ident.clone());
                continue;
            };
            // Branch source is always main's head here: anchor main to the
            // captured head so a torn cross-table cut is impossible.
            requests.push(set_branch_ref_request(
                ident,
                metadata.uuid(),
                name,
                src,
                true,
            ));
            branched.push((ident.clone(), src));
        }
        anyhow::ensure!(
            !branched.is_empty(),
            "no table in the catalog has a snapshot yet, so branch {name:?} cannot point \
             anywhere (write to at least one table first)"
        );
        let refs: Vec<&CommitTableRequest> = requests.iter().collect();
        match self.post_transaction(&refs).await? {
            TxnCommitOutcome::Committed => Ok((branched, skipped)),
            TxnCommitOutcome::Conflict(msg) => bail!(
                "branch {name:?} already exists on at least one table, or a table's main \
                 head moved between load and commit (the per-table \
                 `assert-ref-snapshot-id main=<captured head>` anchors reject a torn \
                 cross-table cut) — the whole-lakehouse create is all-or-nothing and \
                 NOTHING was applied; retry the create-all: {msg}"
            ),
            TxnCommitOutcome::Unsupported => bail!(
                "the catalog does not implement the multi-table transaction endpoint \
                 ({TXN_COMMIT_ENDPOINT:?}), so a whole-lakehouse branch cannot be created \
                 atomically; NOTHING was applied. Create branches per table instead: \
                 icegres branch create <table> {name}"
            ),
        }
    }

    /// `icegres branch drop-all`: remove branch `name` from every table that
    /// has it, in ONE atomic multi-table transaction (each removal anchored
    /// with `assert-ref-snapshot-id <name>=<head>`, so a concurrent commit
    /// to the branch aborts the whole request with nothing applied).
    /// Tables without the ref are skipped; if NO table has it, errors.
    /// Returns the dropped `(table, head)` pairs plus the skipped count.
    pub async fn drop_branch_all(&self, name: &str) -> Result<(Vec<(TableIdent, i64)>, usize)> {
        anyhow::ensure!(
            name != MAIN_BRANCH,
            "refusing to drop {MAIN_BRANCH:?} — it is every table's default branch"
        );
        let idents = self.list_all_tables().await?;
        let mut requests: Vec<CommitTableRequest> = Vec::new();
        let mut dropped: Vec<(TableIdent, i64)> = Vec::new();
        let mut skipped = 0usize;
        for ident in &idents {
            let table = self
                .catalog
                .load_table(ident)
                .await
                .map_err(|e| anyhow!("failed to load table {ident}: {e}"))?;
            let Some(head) = table
                .metadata()
                .snapshot_for_ref(name)
                .map(|s| s.snapshot_id())
            else {
                skipped += 1;
                continue;
            };
            requests.push(remove_branch_ref_request(
                ident,
                table.metadata().uuid(),
                name,
                head,
            ));
            dropped.push((ident.clone(), head));
        }
        anyhow::ensure!(
            !dropped.is_empty(),
            "branch {name:?} does not exist on any table in the catalog"
        );
        let refs: Vec<&CommitTableRequest> = requests.iter().collect();
        match self.post_transaction(&refs).await? {
            TxnCommitOutcome::Committed => Ok((dropped, skipped)),
            TxnCommitOutcome::Conflict(msg) => bail!(
                "branch {name:?} moved on at least one table while dropping it (concurrent \
                 commit?) — the whole-lakehouse drop is all-or-nothing and NOTHING was \
                 applied: {msg}"
            ),
            TxnCommitOutcome::Unsupported => bail!(
                "the catalog does not implement the multi-table transaction endpoint \
                 ({TXN_COMMIT_ENDPOINT:?}), so a whole-lakehouse branch drop cannot be \
                 applied atomically; NOTHING was applied. Drop branches per table instead: \
                 icegres branch drop <table> {name}"
            ),
        }
    }

    /// Expire old snapshots of `ident`, keeping the newest `keep_last` by
    /// commit timestamp plus every snapshot that is still reachable from a
    /// branch/tag ref (so time-travel over live refs and the current head
    /// never break). Metadata-only: the removed snapshots' data/manifest
    /// files are left in object storage — a separate orphan-file GC reclaims
    /// them — but they drop out of table metadata so `$snapshots` shrinks and
    /// metadata stops growing unbounded. Anchored with `assert-table-uuid`
    /// and a `assert-ref-snapshot-id main=<head>` guard so a snapshot written
    /// concurrently is never expired out from under the writer. Returns the
    /// number of snapshots removed.
    pub async fn expire_snapshots(&self, ident: &TableIdent, keep_last: usize) -> Result<usize> {
        let table = self
            .catalog
            .load_table(ident)
            .await
            .map_err(|e| anyhow!("failed to load table {ident}: {e}"))?;
        let metadata = table.metadata();

        // Snapshots still reachable from a ref must always be kept, regardless
        // of age — expiring them would strand the branch/tag on a missing
        // snapshot. The current head is ref-referenced by `main`, so this
        // also protects it.
        let referenced: HashSet<i64> = self
            .list_refs(ident)
            .await?
            .into_iter()
            .map(|(_, id, _)| id)
            .collect();

        // Newest-first by commit time; ties broken by id for determinism.
        let mut ordered: Vec<(i64, i64)> = metadata
            .snapshots()
            .map(|s| (s.timestamp_ms(), s.snapshot_id()))
            .collect();
        ordered.sort_by(|a, b| b.0.cmp(&a.0).then_with(|| b.1.cmp(&a.1)));

        // Keep the newest `keep_last` plus everything a ref points at.
        let mut keep: HashSet<i64> = referenced.clone();
        for (_, id) in ordered.iter().take(keep_last) {
            keep.insert(*id);
        }

        let expire: Vec<i64> = ordered
            .iter()
            .map(|(_, id)| *id)
            .filter(|id| !keep.contains(id))
            .collect();
        if expire.is_empty() {
            return Ok(0);
        }

        let head = metadata.current_snapshot_id();
        let mut requirements = vec![TableRequirement::UuidMatch {
            uuid: metadata.uuid(),
        }];
        // If the table has a main head, pin it so a concurrent append (which
        // moves main) forces us to reload and recompute rather than expiring a
        // stale set.
        if let Some(head) = head {
            requirements.push(TableRequirement::RefSnapshotIdMatch {
                r#ref: MAIN_BRANCH.to_string(),
                snapshot_id: Some(head),
            });
        }
        let request = CommitTableRequest {
            identifier: Some(ident.clone()),
            requirements,
            updates: vec![TableUpdate::RemoveSnapshots {
                snapshot_ids: expire.clone(),
            }],
        };
        match self
            .post_commit(&ident.namespace().to_url_string(), ident.name(), &request)
            .await?
        {
            CommitOutcome::Committed => Ok(expire.len()),
            CommitOutcome::Conflict(msg) => bail!(
                "table {ident} changed while expiring snapshots (concurrent commit?); \
                 retry: {msg}"
            ),
        }
    }

    /// List every snapshot ref (branch/tag) of `ident` as
    /// `(name, snapshot_id, type)` tuples, `main` first then sorted by name.
    /// Read through the raw REST metadata because iceberg-rust 0.9.1 exposes
    /// no public accessor for the full refs map.
    pub async fn list_refs(&self, ident: &TableIdent) -> Result<Vec<(String, i64, String)>> {
        let prefix_seg = if self.prefix.is_empty() {
            String::new()
        } else {
            format!("/{}", urlencode(&self.prefix))
        };
        let url = format!(
            "{}/v1{}/namespaces/{}/tables/{}",
            self.catalog_uri,
            prefix_seg,
            urlencode(&ident.namespace().to_url_string()),
            urlencode(ident.name())
        );
        let body: serde_json::Value = self
            .http
            .get(&url)
            .send()
            .await
            .with_context(|| format!("failed to load table metadata from {url}"))?
            .error_for_status()
            .with_context(|| format!("catalog rejected metadata request for {ident}"))?
            .json()
            .await
            .context("table metadata response is not JSON")?;
        let refs = body
            .pointer("/metadata/refs")
            .and_then(|v| v.as_object())
            .cloned()
            .unwrap_or_default();
        let mut out: Vec<(String, i64, String)> = refs
            .into_iter()
            .filter_map(|(name, r)| {
                let id = r.get("snapshot-id").and_then(|v| v.as_i64())?;
                let kind = r
                    .get("type")
                    .and_then(|v| v.as_str())
                    .unwrap_or("branch")
                    .to_string();
                Some((name, id, kind))
            })
            .collect();
        out.sort_by(|a, b| {
            (a.0 != MAIN_BRANCH)
                .cmp(&(b.0 != MAIN_BRANCH))
                .then_with(|| a.0.cmp(&b.0))
        });
        Ok(out)
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

    /// The multi-table transaction endpoint URL for this catalog.
    fn transactions_commit_url(&self) -> String {
        let prefix_seg = if self.prefix.is_empty() {
            String::new()
        } else {
            format!("/{}", urlencode(&self.prefix))
        };
        format!("{}/v1{}/transactions/commit", self.catalog_uri, prefix_seg)
    }

    /// Resolve the multi-table transaction capability, running an explicit
    /// DATA-FREE probe when it is not already known (config advertisement or
    /// a previous probe): POST one empty, identifier-less table change to
    /// the transactions endpoint. A catalog that implements the endpoint
    /// answers that body with a request-level validation error — live-
    /// verified against Lakekeeper 0.13.1, which answers 400
    /// `TableIdentifierRequiredForCommitTransaction` (the spec-prescribed
    /// shape requires an identifier per change, so no catalog can commit
    /// anything from it) — while a catalog WITHOUT the endpoint answers
    /// 404/405/501 at routing level. Interpretation is
    /// [`txn_endpoint_capability`]; an indeterminate answer (5xx, a catalog
    /// restart) or a network error bails WITHOUT caching, so the next
    /// attempt re-probes. This is the ONLY place the capability is learned
    /// off the wire: a real commit's 404 (e.g. a missing table) can never
    /// be misread as "endpoint unsupported" and poison the cache.
    async fn probe_txn_endpoint(&self) -> Result<bool> {
        if let Some(known) = self.txn_endpoint.get() {
            return Ok(*known);
        }
        let url = self.transactions_commit_url();
        let probe_change = CommitTableRequest {
            identifier: None,
            requirements: Vec::new(),
            updates: Vec::new(),
        };
        let resp = self
            .http
            .post(&url)
            .json(&transaction_request_body(&[&probe_change]))
            .send()
            .await
            .with_context(|| {
                format!("multi-table transaction capability probe POST to {url} failed")
            })?;
        let status = resp.status();
        match txn_endpoint_capability(status) {
            Some(supported) => {
                let _ = self.txn_endpoint.set(supported);
                if supported {
                    tracing::debug!(
                        status = %status,
                        "capability probe: catalog implements {TXN_COMMIT_ENDPOINT:?}"
                    );
                } else {
                    tracing::warn!(
                        status = %status,
                        "capability probe: catalog does not implement \
                         {TXN_COMMIT_ENDPOINT:?}; falling back to per-table commits"
                    );
                }
                Ok(supported)
            }
            None => {
                let body = resp.text().await.unwrap_or_default();
                bail!(
                    "could not determine whether the catalog implements \
                     {TXN_COMMIT_ENDPOINT:?}: the data-free capability probe at {url} \
                     answered {status}: {body} — nothing was cached, the next attempt \
                     re-probes"
                )
            }
        }
    }

    /// POST one multi-table transaction (`{"table-changes": [...]}`) to
    /// `POST /v1/{prefix}/transactions/commit`. The endpoint capability is
    /// resolved FIRST ([`Self::probe_txn_endpoint`]: cached answer, or one
    /// data-free probe) — the real commit's own response NEVER teaches
    /// "unsupported", so once support is established a 404 here is what it
    /// is at commit level (e.g. a table vanished): a hard error, exactly
    /// like every other non-409 rejection. All-or-nothing on the server:
    /// 2xx = every change committed; 409 = conflict (a requirement failed
    /// or the catalog CAS lost) with NOTHING applied. Skips the wire
    /// entirely when support is already known to be absent.
    async fn post_transaction(&self, requests: &[&CommitTableRequest]) -> Result<TxnCommitOutcome> {
        if !self.probe_txn_endpoint().await? {
            return Ok(TxnCommitOutcome::Unsupported);
        }
        let url = self.transactions_commit_url();
        let resp = self
            .http
            .post(&url)
            .json(&transaction_request_body(requests))
            .send()
            .await
            .with_context(|| format!("transaction commit POST to {url} failed"))?;
        let status = resp.status();
        if status.is_success() {
            return Ok(TxnCommitOutcome::Committed);
        }
        let body = resp.text().await.unwrap_or_default();
        if status == reqwest::StatusCode::CONFLICT {
            return Ok(TxnCommitOutcome::Conflict(body));
        }
        // Endpoint support was established above, so ANY other status —
        // including 404 (a table named in the transaction does not exist) —
        // is a commit-level failure of THIS transaction (nothing applied),
        // never a capability signal.
        bail!("catalog rejected transaction commit ({status}) at {url}: {body}")
    }
}

/// Interpret the status a DATA-FREE capability probe (one empty,
/// identifier-less table change) got from `POST
/// /v1/{prefix}/transactions/commit`:
///
/// * request-level validation errors (400/422) — and, defensively, 409 or a
///   2xx — mean the endpoint EXISTS: the request was routed to a handler
///   that understood it (`Some(true)`);
/// * routing-level 404/405/501 mean the endpoint is NOT implemented
///   (`Some(false)`);
/// * anything else (5xx during a catalog restart, auth hiccups, ...) is
///   indeterminate: `None` — the caller must NOT cache a capability from
///   it.
fn txn_endpoint_capability(status: reqwest::StatusCode) -> Option<bool> {
    use reqwest::StatusCode;
    if status.is_success() || status == StatusCode::CONFLICT {
        return Some(true);
    }
    match status {
        StatusCode::BAD_REQUEST | StatusCode::UNPROCESSABLE_ENTITY => Some(true),
        StatusCode::NOT_FOUND | StatusCode::METHOD_NOT_ALLOWED | StatusCode::NOT_IMPLEMENTED => {
            Some(false)
        }
        _ => None,
    }
}

/// The wire body of a multi-table transaction commit
/// (`CommitTransactionRequest` in the Iceberg REST spec): the prepared
/// per-table [`CommitTableRequest`]s — each REQUIRED to carry its
/// `identifier` — wrapped under `"table-changes"`.
fn transaction_request_body(requests: &[&CommitTableRequest]) -> serde_json::Value {
    serde_json::json!({ "table-changes": requests })
}

/// The zero-copy branch-create commit for one table: set ref `name` to
/// snapshot `src`, guarded by `assert-ref-snapshot-id <name>=null` (the ref
/// must not already exist) + `assert-table-uuid`. With `anchor_main` (used
/// whenever the branch source IS main's freshly-read head: single-table
/// creates without `--at-snapshot`, and every table of `create_branch_all`)
/// the request additionally asserts `assert-ref-snapshot-id main=<src>`, so
/// the commit succeeds only if the captured head is still current — for
/// create-all this is what makes the cross-table cut consistent-or-nothing.
/// Callers branching from an explicit user-chosen snapshot pass `false`
/// (main is free to move). Shared by [`OverwriteEngine::create_branch`]
/// (single-table POST) and [`OverwriteEngine::create_branch_all`] (one
/// atomic multi-table transaction).
fn set_branch_ref_request(
    ident: &TableIdent,
    uuid: Uuid,
    name: &str,
    src: i64,
    anchor_main: bool,
) -> CommitTableRequest {
    let mut requirements = vec![
        TableRequirement::UuidMatch { uuid },
        // null snapshot-id = "the ref must not already exist".
        TableRequirement::RefSnapshotIdMatch {
            r#ref: name.to_string(),
            snapshot_id: None,
        },
    ];
    if anchor_main {
        // The branch forks main's head as read: pin main to it, so a
        // concurrent commit between load and this commit is a 409, never a
        // silently stale (or, across tables, torn) fork point.
        requirements.push(TableRequirement::RefSnapshotIdMatch {
            r#ref: MAIN_BRANCH.to_string(),
            snapshot_id: Some(src),
        });
    }
    CommitTableRequest {
        identifier: Some(ident.clone()),
        requirements,
        updates: vec![TableUpdate::SetSnapshotRef {
            ref_name: name.to_string(),
            reference: SnapshotReference::new(src, SnapshotRetention::branch(None, None, None)),
        }],
    }
}

/// The branch-drop commit for one table: remove ref `name`, anchored at its
/// current `head` so a concurrent commit to the branch is never silently
/// discarded. Shared by [`OverwriteEngine::drop_branch`] and
/// [`OverwriteEngine::drop_branch_all`].
fn remove_branch_ref_request(
    ident: &TableIdent,
    uuid: Uuid,
    name: &str,
    head: i64,
) -> CommitTableRequest {
    CommitTableRequest {
        identifier: Some(ident.clone()),
        requirements: vec![
            TableRequirement::UuidMatch { uuid },
            TableRequirement::RefSnapshotIdMatch {
                r#ref: name.to_string(),
                snapshot_id: Some(head),
            },
        ],
        updates: vec![TableUpdate::RemoveSnapshotRef {
            ref_name: name.to_string(),
        }],
    }
}

/// Result of POSTing a prepared commit: accepted, or rejected with 409
/// (optimistic-concurrency conflict; the caller decides whether to retry).
pub enum CommitOutcome {
    Committed,
    Conflict(String),
}

/// Result of POSTing a multi-table transaction to the catalog. All three
/// non-committed outcomes mean NOTHING was applied (the endpoint is
/// all-or-nothing on the server).
enum TxnCommitOutcome {
    /// Every table change committed atomically.
    Committed,
    /// 409: a per-table requirement failed, or the catalog lost its own CAS
    /// even after server-side retries. The whole transaction rolled back.
    Conflict(String),
    /// The catalog does not implement the endpoint; the caller must fall
    /// back (ordered per-table commits, or a clean refusal).
    Unsupported,
}

/// Result of [`OverwriteEngine::commit_pinned_multi`].
pub enum MultiTableCommit {
    /// All tables committed in ONE atomic catalog transaction (tables whose
    /// ops net out to no change contribute nothing, per Postgres semantics).
    Committed,
    /// The catalog lacks `transactions/commit`; nothing was applied. The
    /// caller decides: ordered per-table fallback, or strict refusal.
    Unsupported,
}

/// A fully-prepared commit: all data/metadata files are already durable in
/// object storage; only the atomic catalog POST remains.
pub struct PreparedCommit {
    request: CommitTableRequest,
    /// Rows affected per op (matched rows for DML ops, appended rows for
    /// Append ops), aligned with the input op list.
    rows_by_op: Vec<u64>,
    snapshot_id: i64,
}

impl PreparedCommit {
    /// The snapshot id this commit will publish if the catalog accepts it
    /// (known BEFORE the POST — the write buffer tags in-flight rows with
    /// it so union reads can dedupe exactly; see buffer.rs).
    pub fn snapshot_id(&self) -> i64 {
        self.snapshot_id
    }
}

/// Parse and validate the `icegres.primary-key` table property.
pub fn pk_columns_of(table: &Table) -> Result<Option<Vec<String>>> {
    let Some(raw) = table.metadata().properties().get(PK_PROPERTY) else {
        return Ok(None);
    };
    let cols: Vec<String> = raw
        .split(',')
        .map(str::trim)
        .filter(|c| !c.is_empty())
        .map(str::to_string)
        .collect();
    if cols.is_empty() {
        return Ok(None);
    }
    let schema = table.metadata().current_schema();
    for c in &cols {
        if !schema.as_struct().fields().iter().any(|f| &f.name == c) {
            bail!(
                "table property {PK_PROPERTY}={raw:?} names column {c:?} which does not \
                 exist in table {}",
                table.identifier()
            );
        }
    }
    Ok(Some(cols))
}

/// Apply one DML statement to in-memory rows: returns `(matched, rows_out)`.
/// Row-accounting invariants abort (never mis-commit) on any mismatch.
/// Shared by the per-file commit path and the transaction hook's effective-
/// state maintenance, so statement-time answers and COMMIT-time snapshots
/// are computed by the same code.
pub async fn apply_dml_to_batches(
    stmt: &DmlStatement,
    columns: &[String],
    batches: Vec<RecordBatch>,
) -> Result<(u64, Vec<RecordBatch>)> {
    let rows_in: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
    if rows_in == 0 {
        return Ok((0, batches));
    }
    let sql = DmlSql::new(stmt, columns.iter().map(String::as_str))?;
    let ctx = SessionContext::new_with_config(
        SessionConfig::new().with_default_catalog_and_schema(CATALOG_NAME, &stmt.namespace),
    );
    let schema = batches[0].schema();
    let table_ref =
        datafusion::sql::TableReference::partial(stmt.namespace.as_str(), stmt.table.as_str());
    let mem = MemTable::try_new(schema, vec![batches.clone()])
        .map_err(|e| anyhow!("failed to build in-memory eval table: {e}"))?;
    ctx.register_table(table_ref, Arc::new(mem))
        .map_err(|e| anyhow!("failed to register eval table: {e}"))?;
    match evaluate_rows(&ctx, &sql, rows_in).await? {
        FileFate::Keep => Ok((0, batches)),
        FileFate::Remove { matched } => Ok((matched, Vec::new())),
        FileFate::Rewrite { matched, batches } => Ok((matched, batches)),
    }
}

/// Classification of one file's (or buffered batch set's) rows against a
/// DML statement.
enum FileFate {
    /// No row matched: reuse as-is (zero-copy).
    Keep,
    /// Nothing survived (DELETE matched every row).
    Remove { matched: u64 },
    /// Some rows matched: the surviving/updated row set.
    Rewrite {
        matched: u64,
        batches: Vec<RecordBatch>,
    },
}

/// Fold every DML op in `ops` (skipping ops before `first_op`) over `rows`.
/// Returns (changed, final rows, matched counts accumulated into
/// `rows_by_op`).
async fn fold_dml_ops(
    ops: &[TableOp],
    first_op: usize,
    columns: &[String],
    mut rows: Vec<RecordBatch>,
    rows_by_op: &mut [u64],
) -> Result<(bool, Vec<RecordBatch>)> {
    let mut changed = false;
    for (i, op) in ops.iter().enumerate().skip(first_op) {
        let TableOp::Dml(stmt) = op else { continue };
        let (matched, out) = apply_dml_to_batches(stmt, columns, rows).await?;
        if matched > 0 {
            changed = true;
        }
        rows_by_op[i] += matched;
        rows = out;
    }
    Ok((changed, rows))
}

/// Resolve the head snapshot of `branch` in `metadata`. `main` falls back to
/// the current snapshot (identical by construction — the builder keeps the
/// `main` ref and `current-snapshot-id` in lockstep, and an empty table has
/// neither); any other branch must exist as a snapshot ref.
pub fn branch_head<'a>(
    metadata: &'a iceberg::spec::TableMetadata,
    branch: &str,
) -> Result<Option<&'a iceberg::spec::SnapshotRef>> {
    if branch == MAIN_BRANCH {
        return Ok(metadata.current_snapshot());
    }
    match metadata.snapshot_for_ref(branch) {
        Some(s) => Ok(Some(s)),
        None => bail!(
            "branch {branch:?} does not exist on this table — create it first with \
             `icegres branch create <table> {branch}`"
        ),
    }
}

/// Compute ONE snapshot applying `ops` (in order) against the head of
/// `branch` in the table's current metadata and stage every file it needs.
/// Returns `None` when the ops net out to no change (nothing to commit).
/// `pk` = enforced key columns; violations return [`ConstraintViolation`]
/// before anything is posted. The produced commit asserts
/// `assert-ref-snapshot-id <branch>=<head>` and publishes the new snapshot
/// on `branch` only — snapshots reachable from other refs are untouched
/// (zero-copy branch isolation, SPEC D6). `extra_properties` (if non-empty)
/// become a `set-properties` update in the SAME atomic commit — the durable
/// tail records its drained-sequence watermark this way (`buffer.rs` /
/// `tail.rs`); all other callers pass `None`.
pub async fn prepare_commit(
    table: &Table,
    ops: &[TableOp],
    pk: Option<&[String]>,
    branch: &str,
    extra_properties: Option<&HashMap<String, String>>,
) -> Result<Option<PreparedCommit>> {
    let metadata = table.metadata();

    // ---- Guard rails: reject unsupported table shapes loudly. ----
    if metadata.format_version() != FormatVersion::V2 {
        bail!(
            "writes through icegres require an Iceberg format v2 table (found {:?})",
            metadata.format_version()
        );
    }
    if !metadata.default_partition_spec().is_unpartitioned() {
        bail!("writes on partitioned tables are not supported yet");
    }
    let head = branch_head(metadata, branch)
        .with_context(|| format!("cannot commit to table {}", table.identifier()))?;
    let head_id = head.map(|s| s.snapshot_id());

    let file_io = table.file_io();
    let schema = metadata.current_schema();
    let arrow_target: ArrowSchemaRef = Arc::new(
        schema_to_arrow_schema(schema).map_err(|e| anyhow!("schema conversion failed: {e}"))?,
    );
    let columns: Vec<String> = schema
        .as_struct()
        .fields()
        .iter()
        .map(|f| f.name.clone())
        .collect();
    // Validate every DML op eagerly (unknown assignment columns etc.).
    for op in ops {
        if let TableOp::Dml(stmt) = op {
            DmlSql::new(stmt, columns.iter().map(String::as_str))?;
        }
    }
    let has_dml = ops.iter().any(|op| matches!(op, TableOp::Dml(_)));
    // Existing files must be scanned when DML can touch them, or when the
    // final key set must be assembled for PK enforcement.
    let need_file_scan = has_dml || pk.is_some();

    let mut rows_by_op = vec![0u64; ops.len()];
    let commit_uuid = Uuid::new_v4();
    // Lazily-built writer for replacement/appended rows.
    let mut data_writer: Option<_> = None;

    // Manifests whose files are all kept: carried forward untouched.
    let mut carried: Vec<ManifestFile> = Vec::new();
    // (data_file, snapshot_id, data_seq, file_seq) for kept files from
    // manifests that must be rewritten.
    let mut existing: Vec<(DataFile, i64, i64, Option<i64>)> = Vec::new();
    // Same, for files removed by this snapshot (recorded as DELETED).
    let mut deleted: Vec<(DataFile, i64, Option<i64>)> = Vec::new();
    // Summary bookkeeping: exact totals recomputed from the final live set
    // (previous snapshots' summaries are not trusted: iceberg-rust 0.9.1
    // fast_append itself writes non-cumulative totals).
    let (mut removed_files, mut removed_records, mut removed_bytes) = (0u64, 0u64, 0u64);
    let (mut kept_files, mut kept_records, mut kept_bytes) = (0u64, 0u64, 0u64);
    let mut any_file_changed = false;
    // PK columns of every FINAL row (kept + rewritten + appended).
    let mut pk_rows: Vec<RecordBatch> = Vec::new();

    if let Some(current_snapshot) = head {
        let manifest_list = current_snapshot
            .load_manifest_list(file_io, &table.metadata_ref())
            .await
            .map_err(|e| anyhow!("failed to load manifest list: {e}"))?;
        for mf in manifest_list.entries() {
            if mf.content != ManifestContentType::Data {
                bail!(
                    "table has delete manifests (merge-on-read); writes via icegres \
                     support copy-on-write tables only"
                );
            }
        }

        for manifest_file in manifest_list.entries() {
            if !need_file_scan {
                // Append-only commit: every existing manifest is carried.
                carried.push(manifest_file.clone());
                continue;
            }
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
                    // Entry already deleted by an earlier snapshot: drop it
                    // from the new manifest (spec: DELETED entries live only
                    // in the snapshot that deleted them).
                    rewrite_manifest = true;
                    continue;
                }
                let batches = read_parquet_file(file_io, entry.data_file()).await?;
                let rows_in: u64 = batches.iter().map(|b| b.num_rows() as u64).sum();
                let fate = if has_dml {
                    // RecordBatch clones share Arc'd buffers — cheap; the
                    // original stays available for PK collection on Keep.
                    let (changed, out) =
                        fold_dml_ops(ops, 0, &columns, batches.clone(), &mut rows_by_op).await?;
                    let rows_out: u64 = out.iter().map(|b| b.num_rows() as u64).sum();
                    if !changed {
                        FileFate::Keep
                    } else if rows_out == 0 {
                        FileFate::Remove {
                            matched: rows_in, // informational only
                        }
                    } else {
                        FileFate::Rewrite {
                            matched: rows_in.saturating_sub(rows_out),
                            batches: out,
                        }
                    }
                } else {
                    FileFate::Keep
                };
                if let Some(pk_cols) = pk {
                    // Final rows of this file feed the PK check.
                    match &fate {
                        FileFate::Keep if !batches.is_empty() => {
                            pk_rows.push(project_columns(&batches, pk_cols)?);
                        }
                        FileFate::Rewrite { batches, .. } if !batches.is_empty() => {
                            pk_rows.push(project_columns(batches, pk_cols)?);
                        }
                        _ => {}
                    }
                }
                match &fate {
                    FileFate::Keep => {
                        kept_files += 1;
                        kept_records += entry.data_file().record_count();
                        kept_bytes += entry.data_file().file_size_in_bytes();
                    }
                    FileFate::Remove { .. } | FileFate::Rewrite { .. } => {
                        any_file_changed = true;
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
                                writer.write(aligned).await.map_err(|e| {
                                    anyhow!("failed to write replacement rows: {e}")
                                })?;
                            }
                        }
                    }
                }
            }
        }
    }

    // ---- Buffered appends, folded through every LATER DML op. ----
    let mut appended_rows: u64 = 0;
    for (i, op) in ops.iter().enumerate() {
        let TableOp::Append(batches) = op else {
            continue;
        };
        rows_by_op[i] = batches.iter().map(|b| b.num_rows() as u64).sum();
        let (_, out) = fold_dml_ops(ops, i + 1, &columns, batches.clone(), &mut rows_by_op).await?;
        let rows_out: u64 = out.iter().map(|b| b.num_rows() as u64).sum();
        if rows_out == 0 {
            continue;
        }
        appended_rows += rows_out;
        let aligned: Vec<RecordBatch> = out
            .iter()
            .map(|b| align_batch(b, &arrow_target))
            .collect::<Result<_>>()?;
        if let Some(pk_cols) = pk {
            pk_rows.push(project_columns(&aligned, pk_cols)?);
        }
        let writer = match data_writer.as_mut() {
            Some(w) => w,
            None => {
                data_writer = Some(new_data_writer(table, &commit_uuid).await?);
                data_writer.as_mut().expect("just set")
            }
        };
        for batch in aligned {
            writer
                .write(batch)
                .await
                .map_err(|e| anyhow!("failed to write appended rows: {e}"))?;
        }
    }

    if !any_file_changed && appended_rows == 0 {
        // Net no-op: nothing to commit.
        return Ok(None);
    }

    // ---- PK enforcement over the FINAL row set. ----
    if let Some(pk_cols) = pk {
        check_pk(pk_cols, &pk_rows, table.identifier().name()).await?;
    }

    let added_files: Vec<DataFile> = match data_writer.as_mut() {
        Some(w) => w
            .close()
            .await
            .map_err(|e| anyhow!("failed to close data file writer: {e}"))?,
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
    // ... plus one manifest of ADDED files (rewritten survivors + appends).
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
        head_id,
        next_seq,
    );
    list_writer
        .add_manifests(new_manifests.into_iter())
        .map_err(|e| anyhow!("failed to append manifests to manifest list: {e}"))?;
    list_writer
        .close()
        .await
        .map_err(|e| anyhow!("failed to write manifest list: {e}"))?;

    // Snapshot summary. added/deleted counts are file-level (Iceberg spec
    // semantics — a rewritten file counts all its records on both sides);
    // totals are EXACT, recomputed from the final live set, every member of
    // which was visited above (kept files carried in untouched manifests
    // when !need_file_scan are counted from their manifest stats: for the
    // append fast path kept_* stay 0 and totals fall back to file-level
    // accounting below).
    let operation = if !any_file_changed {
        Operation::Append
    } else if added_files.is_empty() {
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
    if need_file_scan {
        // Every live file was visited: exact totals.
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
    }
    props.insert("total-delete-files".into(), "0".into());
    props.insert("total-position-deletes".into(), "0".into());
    props.insert("total-equality-deletes".into(), "0".into());
    props.insert("changed-partition-count".into(), "1".into());

    let snapshot = Snapshot::builder()
        .with_snapshot_id(snapshot_id)
        .with_parent_snapshot_id(head_id)
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

    let mut updates = vec![
        TableUpdate::AddSnapshot { snapshot },
        TableUpdate::SetSnapshotRef {
            ref_name: branch.to_string(),
            reference: SnapshotReference::new(
                snapshot_id,
                SnapshotRetention::branch(None, None, None),
            ),
        },
    ];
    if let Some(props) = extra_properties {
        if !props.is_empty() {
            updates.push(TableUpdate::SetProperties {
                updates: props.clone(),
            });
        }
    }
    let request = CommitTableRequest {
        identifier: Some(table.identifier().clone()),
        requirements: vec![
            TableRequirement::UuidMatch {
                uuid: metadata.uuid(),
            },
            // Optimistic concurrency: the target BRANCH ref must still
            // point where we started, otherwise the catalog answers 409.
            // Commits to other branches of the same table do not conflict.
            TableRequirement::RefSnapshotIdMatch {
                r#ref: branch.to_string(),
                snapshot_id: head_id,
            },
        ],
        updates,
    };

    Ok(Some(PreparedCommit {
        request,
        rows_by_op,
        snapshot_id,
    }))
}

/// Read one Parquet data file fully into record batches.
async fn read_parquet_file(
    file_io: &iceberg::io::FileIO,
    data_file: &DataFile,
) -> Result<Vec<RecordBatch>> {
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
    reader
        .collect::<std::result::Result<_, _>>()
        .with_context(|| format!("failed to decode Parquet file {}", data_file.file_path()))
}

/// Concatenate `batches` projected onto `cols` (by name) into one batch.
/// Callers must not pass an empty batch list (no schema to project from).
fn project_columns(batches: &[RecordBatch], cols: &[String]) -> Result<RecordBatch> {
    let Some(first) = batches.first() else {
        bail!("cannot project PK columns out of an empty batch set");
    };
    let indices: Vec<usize> = cols
        .iter()
        .map(|c| {
            first
                .schema()
                .fields()
                .iter()
                .position(|f| f.name().eq_ignore_ascii_case(c))
                .ok_or_else(|| anyhow!("PK column {c:?} missing from row batch"))
        })
        .collect::<Result<_>>()?;
    let projected: Vec<RecordBatch> = batches
        .iter()
        .map(|b| {
            b.project(&indices)
                .map_err(|e| anyhow!("projection failed: {e}"))
        })
        .collect::<Result<_>>()?;
    arrow::compute::concat_batches(&projected[0].schema(), &projected)
        .map_err(|e| anyhow!("failed to concatenate PK batches: {e}"))
}

/// Enforce NOT NULL + uniqueness over the assembled final key rows.
/// Violations return [`ConstraintViolation`] with the standard sqlstate.
pub async fn check_pk(pk_cols: &[String], pk_rows: &[RecordBatch], table: &str) -> Result<()> {
    let nonempty: Vec<RecordBatch> = pk_rows
        .iter()
        .filter(|b| b.num_rows() > 0)
        .cloned()
        .collect();
    if nonempty.is_empty() {
        return Ok(());
    }
    let ctx = SessionContext::new();
    let mem = MemTable::try_new(nonempty[0].schema(), vec![nonempty])
        .map_err(|e| anyhow!("failed to build PK check table: {e}"))?;
    ctx.register_table("__icegres_pk", Arc::new(mem))
        .map_err(|e| anyhow!("failed to register PK check table: {e}"))?;
    let cols_sql = pk_cols
        .iter()
        .map(|c| quote_ident(c))
        .collect::<Vec<_>>()
        .join(", ");
    let null_pred = pk_cols
        .iter()
        .map(|c| format!("{} IS NULL", quote_ident(c)))
        .collect::<Vec<_>>()
        .join(" OR ");

    let nulls = count_query(
        &ctx,
        &format!("SELECT count(*) FROM __icegres_pk WHERE {null_pred}"),
    )
    .await?;
    if nulls > 0 {
        return Err(anyhow!(ConstraintViolation {
            sqlstate: "23502",
            message: format!(
                "null value in column(s) ({}) of relation \"{table}\" violates not-null \
                 constraint (primary key, {nulls} row(s))",
                pk_cols.join(", ")
            ),
        }));
    }
    let dup_sql = format!(
        "SELECT {cols_sql}, count(*) AS n FROM __icegres_pk \
         GROUP BY {cols_sql} HAVING count(*) > 1 LIMIT 1"
    );
    let dups = ctx
        .sql(&dup_sql)
        .await
        .map_err(|e| anyhow!("failed to plan PK duplicate check: {e}"))?
        .collect()
        .await
        .map_err(|e| anyhow!("failed to run PK duplicate check: {e}"))?;
    let dup_rows: usize = dups.iter().map(|b| b.num_rows()).sum();
    if dup_rows > 0 {
        let sample = format_first_row(&dups, pk_cols.len());
        return Err(anyhow!(ConstraintViolation {
            sqlstate: "23505",
            message: format!(
                "duplicate key value violates unique constraint \"{table}_pkey\": \
                 key ({}) = ({sample}) is not unique",
                pk_cols.join(", ")
            ),
        }));
    }
    Ok(())
}

async fn count_query(ctx: &SessionContext, sql: &str) -> Result<u64> {
    let batches = ctx
        .sql(sql)
        .await
        .map_err(|e| anyhow!("failed to plan ({sql}): {e}"))?
        .collect()
        .await
        .map_err(|e| anyhow!("failed to evaluate ({sql}): {e}"))?;
    Ok(batches
        .first()
        .map(|b| b.column(0).as_primitive::<Int64Type>().value(0) as u64)
        .unwrap_or(0))
}

/// Render the first `ncols` columns of the first row for error messages.
fn format_first_row(batches: &[RecordBatch], ncols: usize) -> String {
    for b in batches {
        if b.num_rows() == 0 {
            continue;
        }
        let mut parts = Vec::with_capacity(ncols);
        for c in 0..ncols.min(b.num_columns()) {
            let col = b.column(c);
            let display = arrow::util::display::array_value_to_string(col, 0)
                .unwrap_or_else(|_| "?".to_string());
            parts.push(display);
        }
        return parts.join(", ");
    }
    "?".to_string()
}

async fn evaluate_rows(ctx: &SessionContext, sql: &DmlSql, rows_in: u64) -> Result<FileFate> {
    let matched = match &sql.count_matched {
        None => rows_in, // no WHERE clause: everything matches
        Some(count_sql) => count_query(ctx, count_sql)
            .await
            .context("failed to evaluate DML predicate")?,
    };
    if matched == 0 {
        return Ok(FileFate::Keep);
    }
    match &sql.rewrite {
        // DELETE matching every row: drop it, no replacement rows.
        None if matched == rows_in => Ok(FileFate::Remove { matched }),
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
            if survivor_rows + matched != rows_in {
                bail!(
                    "DELETE row accounting mismatch: {rows_in} rows, {matched} matched, \
                     {survivor_rows} survivors — refusing to commit"
                );
            }
            if survivor_rows == 0 {
                return Ok(FileFate::Remove { matched });
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
            if rewritten_rows != rows_in {
                bail!(
                    "UPDATE row accounting mismatch: {rows_in} rows in, {rewritten_rows} out \
                     — refusing to commit"
                );
            }
            Ok(FileFate::Rewrite { matched, batches })
        }
    }
}

/// Pre-rendered SQL for per-batch-set evaluation.
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
pub fn align_batch(batch: &RecordBatch, target: &ArrowSchemaRef) -> Result<RecordBatch> {
    if batch.num_columns() != target.fields().len() {
        bail!(
            "row batch has {} columns, table has {}",
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
                "row batch column {i} is named {src_name:?}, expected {:?}",
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
        .map_err(|e| anyhow!("rows do not fit the table schema (nullability/type): {e}"))
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
pub fn quote_ident(ident: &str) -> String {
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
    use arrow::array::{Float64Array, Int64Array};
    use arrow::datatypes::{DataType, Field, Schema};

    #[test]
    fn multi_table_transaction_body_wraps_per_table_requirements() {
        // N per-table CommitTableRequests (with their assert-ref-snapshot-id
        // pins) become ONE {"table-changes": [...]} wire body, identifiers
        // included — the exact CommitTransactionRequest shape Lakekeeper's
        // POST /v1/{prefix}/transactions/commit expects.
        let r1 = CommitTableRequest {
            identifier: Some(TableIdent::from_strs(["ns", "t1"]).unwrap()),
            requirements: vec![TableRequirement::RefSnapshotIdMatch {
                r#ref: "main".to_string(),
                snapshot_id: Some(123),
            }],
            updates: vec![],
        };
        let r2 = CommitTableRequest {
            identifier: Some(TableIdent::from_strs(["ns", "t2"]).unwrap()),
            requirements: vec![TableRequirement::RefSnapshotIdMatch {
                r#ref: "main".to_string(),
                snapshot_id: None,
            }],
            updates: vec![],
        };
        let body = transaction_request_body(&[&r1, &r2]);
        let changes = body
            .get("table-changes")
            .and_then(|v| v.as_array())
            .expect("body must carry a table-changes array");
        assert_eq!(changes.len(), 2);
        let str_at = |i: usize, ptr: &str| {
            changes[i]
                .pointer(ptr)
                .and_then(|v| v.as_str())
                .map(str::to_string)
        };
        assert_eq!(str_at(0, "/identifier/name").as_deref(), Some("t1"));
        assert_eq!(str_at(0, "/identifier/namespace/0").as_deref(), Some("ns"));
        assert_eq!(str_at(1, "/identifier/name").as_deref(), Some("t2"));
        assert_eq!(
            str_at(0, "/requirements/0/type").as_deref(),
            Some("assert-ref-snapshot-id")
        );
        assert_eq!(str_at(0, "/requirements/0/ref").as_deref(), Some("main"));
        assert_eq!(
            changes[0]
                .pointer("/requirements/0/snapshot-id")
                .and_then(|v| v.as_i64()),
            Some(123)
        );
        // A None pin ("ref must not exist") serializes as an EXPLICIT null,
        // never omitted — omitting it would change the assertion's meaning.
        assert!(changes[1]
            .pointer("/requirements/0/snapshot-id")
            .is_some_and(|v| v.is_null()));
    }

    #[test]
    fn txn_endpoint_capability_interpretation() {
        use reqwest::StatusCode;
        // The endpoint answered the data-free probe: it exists. 400/422 are
        // the expected request-level validation answers (Lakekeeper 0.13.1:
        // 400 TableIdentifierRequiredForCommitTransaction); 2xx/409 also
        // prove a handler is behind the route.
        for s in [
            StatusCode::OK,
            StatusCode::NO_CONTENT,
            StatusCode::BAD_REQUEST,
            StatusCode::CONFLICT,
            StatusCode::UNPROCESSABLE_ENTITY,
        ] {
            assert_eq!(txn_endpoint_capability(s), Some(true), "{s}");
        }
        // Routing-level "no such endpoint" answers.
        for s in [
            StatusCode::NOT_FOUND,
            StatusCode::METHOD_NOT_ALLOWED,
            StatusCode::NOT_IMPLEMENTED,
        ] {
            assert_eq!(txn_endpoint_capability(s), Some(false), "{s}");
        }
        // Indeterminate (catalog restart, gateway, auth): must NOT be
        // cached either way — the caller re-probes next time.
        for s in [
            StatusCode::INTERNAL_SERVER_ERROR,
            StatusCode::BAD_GATEWAY,
            StatusCode::SERVICE_UNAVAILABLE,
            StatusCode::GATEWAY_TIMEOUT,
            StatusCode::UNAUTHORIZED,
            StatusCode::TOO_MANY_REQUESTS,
        ] {
            assert_eq!(txn_endpoint_capability(s), None, "{s}");
        }
    }

    #[test]
    fn branch_ref_request_builders_guard_and_anchor() {
        let ident = TableIdent::from_strs(["demo", "trips"]).unwrap();
        let uuid = Uuid::new_v4();

        // create (forking main's head): uuid match + "ref must not exist"
        // guard + main pinned to the captured head + set-snapshot-ref.
        let create = serde_json::to_value(set_branch_ref_request(&ident, uuid, "dev", 42, true))
            .expect("create request serializes");
        assert_eq!(
            create
                .pointer("/requirements/0/type")
                .and_then(|v| v.as_str()),
            Some("assert-table-uuid")
        );
        assert_eq!(
            create
                .pointer("/requirements/1/type")
                .and_then(|v| v.as_str()),
            Some("assert-ref-snapshot-id")
        );
        assert_eq!(
            create
                .pointer("/requirements/1/ref")
                .and_then(|v| v.as_str()),
            Some("dev")
        );
        assert!(create
            .pointer("/requirements/1/snapshot-id")
            .is_some_and(|v| v.is_null()));
        // The main anchor: assert-ref-snapshot-id main=<src>, making the
        // fork point (and, across tables in create-all, the whole cut)
        // consistent-or-nothing.
        assert_eq!(
            create
                .pointer("/requirements/2/type")
                .and_then(|v| v.as_str()),
            Some("assert-ref-snapshot-id")
        );
        assert_eq!(
            create
                .pointer("/requirements/2/ref")
                .and_then(|v| v.as_str()),
            Some("main")
        );
        assert_eq!(
            create
                .pointer("/requirements/2/snapshot-id")
                .and_then(|v| v.as_i64()),
            Some(42)
        );
        assert_eq!(
            create.pointer("/updates/0/action").and_then(|v| v.as_str()),
            Some("set-snapshot-ref")
        );
        assert_eq!(
            create
                .pointer("/updates/0/ref-name")
                .and_then(|v| v.as_str()),
            Some("dev")
        );
        assert_eq!(
            create
                .pointer("/updates/0/snapshot-id")
                .and_then(|v| v.as_i64()),
            Some(42)
        );

        // create from an explicit --at-snapshot: the user chose the fork
        // point, so NO main anchor is carried (main is free to move).
        let create_at =
            serde_json::to_value(set_branch_ref_request(&ident, uuid, "dev", 42, false))
                .expect("at-snapshot create request serializes");
        assert_eq!(
            create_at
                .pointer("/requirements")
                .and_then(|v| v.as_array())
                .map(Vec::len),
            Some(2)
        );
        assert!(create_at.pointer("/requirements/2").is_none());

        // drop: anchored at the current head + remove-snapshot-ref.
        let drop = serde_json::to_value(remove_branch_ref_request(&ident, uuid, "dev", 42))
            .expect("drop request serializes");
        assert_eq!(
            drop.pointer("/requirements/1/snapshot-id")
                .and_then(|v| v.as_i64()),
            Some(42)
        );
        assert_eq!(
            drop.pointer("/updates/0/action").and_then(|v| v.as_str()),
            Some("remove-snapshot-ref")
        );
        assert_eq!(
            drop.pointer("/updates/0/ref-name").and_then(|v| v.as_str()),
            Some("dev")
        );
    }

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

    fn rows(ids: &[i64], fares: &[f64]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("trip_id", DataType::Int64, true),
            Field::new("fare", DataType::Float64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(Float64Array::from(fares.to_vec())),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn apply_dml_delete_removes_matched_rows() {
        let cols = vec!["trip_id".to_string(), "fare".to_string()];
        let s = stmt(DmlKind::Delete, Some("trip_id = 2"));
        let (matched, out) =
            apply_dml_to_batches(&s, &cols, vec![rows(&[1, 2, 3], &[1.0, 2.0, 3.0])])
                .await
                .unwrap();
        assert_eq!(matched, 1);
        let n: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(n, 2);
    }

    #[tokio::test]
    async fn apply_dml_update_keeps_row_count_and_changes_values() {
        let cols = vec!["trip_id".to_string(), "fare".to_string()];
        let s = stmt(
            DmlKind::Update {
                assignments: vec![("fare".into(), "99.0".into())],
            },
            Some("trip_id = 1"),
        );
        let (matched, out) = apply_dml_to_batches(&s, &cols, vec![rows(&[1, 2], &[1.0, 2.0])])
            .await
            .unwrap();
        assert_eq!(matched, 1);
        let n: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(n, 2);
    }

    #[tokio::test]
    async fn apply_dml_no_match_keeps_batches_untouched() {
        let cols = vec!["trip_id".to_string(), "fare".to_string()];
        let s = stmt(DmlKind::Delete, Some("trip_id = 999"));
        let input = vec![rows(&[1, 2], &[1.0, 2.0])];
        let (matched, out) = apply_dml_to_batches(&s, &cols, input.clone())
            .await
            .unwrap();
        assert_eq!(matched, 0);
        assert_eq!(out.len(), input.len());
    }

    #[tokio::test]
    async fn check_pk_rejects_duplicates_with_23505() {
        let batch = rows(&[1, 2, 2], &[1.0, 2.0, 3.0]).project(&[0]).unwrap();
        let err = check_pk(&["trip_id".to_string()], &[batch], "trips")
            .await
            .unwrap_err();
        let v = err
            .downcast_ref::<ConstraintViolation>()
            .expect("typed violation");
        assert_eq!(v.sqlstate, "23505");
        assert!(v.message.contains("trips_pkey"));
        assert!(v.message.contains("(2)"));
    }

    #[tokio::test]
    async fn check_pk_rejects_nulls_with_23502() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "trip_id",
            DataType::Int64,
            true,
        )]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(Int64Array::from(vec![Some(1), None]))],
        )
        .unwrap();
        let err = check_pk(&["trip_id".to_string()], &[batch], "trips")
            .await
            .unwrap_err();
        let v = err
            .downcast_ref::<ConstraintViolation>()
            .expect("typed violation");
        assert_eq!(v.sqlstate, "23502");
    }

    #[tokio::test]
    async fn check_pk_accepts_unique_keys() {
        let batch = rows(&[1, 2, 3], &[1.0, 2.0, 3.0]).project(&[0]).unwrap();
        check_pk(&["trip_id".to_string()], &[batch], "trips")
            .await
            .unwrap();
    }
}
