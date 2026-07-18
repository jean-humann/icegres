//! Object-store IO tuning for Iceberg scans.
//!
//! iceberg-datafusion's `IcebergTableScan` executes the underlying
//! `iceberg::scan::TableScan` with its default concurrency limits, which are
//! all `num_cpus` (= 4 on this box). The scan work is not CPU-bound: with
//! append-only commits `demo.trips` is spread over hundreds of tiny Parquet
//! files, and a full scan issues one ~1 ms GET to RustFS per file, four at a
//! time. That serialized IO — not DataFusion operators — measured as almost
//! the entire aggregate/join p50 (SessionConfig knobs like
//! `target_partitions`, `batch_size`, `coalesce_batches`, and the
//! `repartition_*` flags all moved nothing beyond noise).
//!
//! [`TunedIcebergScan`] is a drop-in replacement execution plan built from
//! the upstream scan's public getters (table, snapshot, projection,
//! predicate, limit). It runs the *same* `TableScan`, but with
//! `with_concurrency_limit(N)` so data-file/manifest GETs against the local
//! object store overlap. `N` comes from `ICEGRES_SCAN_CONCURRENCY`
//! (default [`DEFAULT_SCAN_CONCURRENCY`]; `0` disables the wrapper and
//! falls back to upstream behavior).
//!
//! # Table statistics for the optimizer
//!
//! The upstream scan reports no statistics, so DataFusion plans every join
//! blind — `JoinSelection` cannot pick the smaller build side. The tuned scan
//! feeds the snapshot's live row count to `partition_statistics`: the
//! manifest *list* entries already carry `added/existing_rows_count`, so the
//! count needs ONE small object GET per snapshot (not a manifest walk),
//! cached per `(table uuid, snapshot id)` — any commit changes the snapshot
//! id and naturally misses to a fresh entry. Tables with delete manifests or
//! missing counts honestly report no statistics rather than an overcount.
//! `ICEGRES_TABLE_STATS=0` disables the lookup entirely.

use std::any::Any;
use std::collections::HashMap;
use std::pin::Pin;
use std::sync::{Arc, Mutex as StdMutex, OnceLock};

use datafusion::arrow::array::RecordBatch;
use datafusion::common::stats::Precision;
use datafusion::common::Statistics;
use datafusion::error::Result as DFResult;
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use futures::{Stream, TryStreamExt};
use iceberg::expr::Predicate;
use iceberg::spec::{ManifestContentType, ManifestFile};
use iceberg::table::Table;
use iceberg_datafusion::physical_plan::IcebergTableScan;
use iceberg_datafusion::to_datafusion_error;
use tracing::warn;

/// Default IO concurrency for Iceberg scans (manifest files, manifest
/// entries, and data files). Tuned on the 4-core bench box against local
/// RustFS; see bench/SCORECARD.md.
pub const DEFAULT_SCAN_CONCURRENCY: usize = 32;

/// Default Parquet reader batch size. iceberg-rust leaves `batch_size: None`,
/// which falls back to parquet's 1024-row default — ~8x more, smaller batches
/// than DataFusion's execution batch size on the 5M-row path. Matching 8192
/// cuts per-batch overhead on large scans; tables under one batch (the tiny
/// demo tables) are unaffected. `ICEGRES_SCAN_BATCH_SIZE` overrides; `0`
/// leaves the reader default.
pub const DEFAULT_SCAN_BATCH_SIZE: usize = 8192;

/// Scan IO concurrency from `ICEGRES_SCAN_CONCURRENCY` (parsed once).
/// `0` disables [`tune`] entirely.
fn scan_concurrency() -> usize {
    static CONCURRENCY: OnceLock<usize> = OnceLock::new();
    *CONCURRENCY.get_or_init(|| match std::env::var("ICEGRES_SCAN_CONCURRENCY") {
        Ok(raw) => raw.trim().parse().unwrap_or_else(|_| {
            warn!(
                value = %raw,
                default = DEFAULT_SCAN_CONCURRENCY,
                "invalid ICEGRES_SCAN_CONCURRENCY; using default"
            );
            DEFAULT_SCAN_CONCURRENCY
        }),
        Err(_) => DEFAULT_SCAN_CONCURRENCY,
    })
}

/// Parquet reader batch size from `ICEGRES_SCAN_BATCH_SIZE` (parsed once).
/// `0` leaves iceberg-rust's reader default (1024).
fn scan_batch_size() -> usize {
    static BATCH: OnceLock<usize> = OnceLock::new();
    *BATCH.get_or_init(|| match std::env::var("ICEGRES_SCAN_BATCH_SIZE") {
        Ok(raw) => raw.trim().parse().unwrap_or_else(|_| {
            warn!(
                value = %raw,
                default = DEFAULT_SCAN_BATCH_SIZE,
                "invalid ICEGRES_SCAN_BATCH_SIZE; using default"
            );
            DEFAULT_SCAN_BATCH_SIZE
        }),
        Err(_) => DEFAULT_SCAN_BATCH_SIZE,
    })
}

/// Parquet page-index row selection from `ICEGRES_SCAN_ROW_SELECTION`
/// (parsed once; default OFF). When enabled the reader consults the Parquet
/// page index to skip non-matching data pages inside surviving row groups.
///
/// Default off because iceberg-rust's Parquet writer does not emit a column
/// (page) index, so the reader errors ("Parquet file metadata does not
/// contain a column index") on icegres-written data files when this is on.
/// The flag stays available for datasets whose files *do* carry a page index;
/// wiring the writer to emit one is a separate change.
fn row_selection_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| match std::env::var("ICEGRES_SCAN_ROW_SELECTION") {
        Ok(raw) => matches!(raw.trim(), "1" | "true" | "on" | "yes"),
        Err(_) => false,
    })
}

/// Whether the tuned scan feeds table statistics to the optimizer
/// (`ICEGRES_TABLE_STATS`; default ON, `0`/`false`/`off`/`no` disables).
fn table_stats_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| match std::env::var("ICEGRES_TABLE_STATS") {
        Ok(raw) => !matches!(raw.trim(), "0" | "false" | "off" | "no"),
        Err(_) => true,
    })
}

/// Live row count from manifest-LIST entries: Σ (added + existing) over data
/// manifests. `None` when the count would be unsound or unknowable — any
/// delete manifest present (merge-on-read: the sum would overcount without
/// applying deletes) or any entry missing its row-count fields. Pure, so it
/// is unit-tested without object storage.
fn sum_live_rows(entries: &[ManifestFile]) -> Option<u64> {
    let mut total: u64 = 0;
    for entry in entries {
        if entry.content != ManifestContentType::Data {
            return None;
        }
        total = total
            .checked_add(entry.added_rows_count?)?
            .checked_add(entry.existing_rows_count?)?;
    }
    Some(total)
}

/// Per-`(table uuid, snapshot id)` row-count cache. Snapshot-keyed, so any
/// commit misses to a fresh entry and stale snapshots age out via the crude
/// clear-on-cap bound (entries are two integers; the cap exists only so an
/// endless snapshot churn cannot grow the map forever).
type StatsKey = (uuid::Uuid, i64);
const STATS_CACHE_CAP: usize = 1024;
fn stats_cache() -> &'static StdMutex<HashMap<StatsKey, Option<u64>>> {
    static CACHE: OnceLock<StdMutex<HashMap<StatsKey, Option<u64>>>> = OnceLock::new();
    CACHE.get_or_init(|| StdMutex::new(HashMap::new()))
}

/// The scanned snapshot's live row count, from the manifest list (one small
/// object GET per snapshot, then cached). `None` when statistics are
/// disabled, the table is empty of snapshots, the count is unsound
/// ([`sum_live_rows`]), or the manifest list is unreadable — a read error is
/// NOT cached, so a transient object-store blip does not pin "unknown" for
/// the snapshot's lifetime.
async fn snapshot_row_count(table: &Table, snapshot_id: Option<i64>) -> Option<u64> {
    let metadata = table.metadata();
    let snapshot = match snapshot_id {
        Some(id) => metadata.snapshot_by_id(id)?,
        None => metadata.current_snapshot()?,
    };
    let key: StatsKey = (metadata.uuid(), snapshot.snapshot_id());
    if let Some(cached) = crate::freshness::recover("scan stats cache", stats_cache().lock())
        .get(&key)
        .copied()
    {
        return cached;
    }
    let manifest_list = snapshot
        .load_manifest_list(table.file_io(), &table.metadata_ref())
        .await
        .ok()?; // transient read failure: report unknown, do not cache
    let count = sum_live_rows(manifest_list.entries());
    let mut cache = crate::freshness::recover("scan stats cache", stats_cache().lock());
    if cache.len() >= STATS_CACHE_CAP {
        cache.clear();
    }
    cache.insert(key, count);
    count
}

/// If `plan` is an upstream [`IcebergTableScan`], replace it with a
/// [`TunedIcebergScan`] running the same scan at the configured IO
/// concurrency and carrying the snapshot's row count as optimizer
/// statistics. Any other plan (metadata tables, inserts, ...) is returned
/// unchanged.
pub async fn tune(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let concurrency = scan_concurrency();
    if concurrency == 0 {
        return plan;
    }
    match plan.as_any().downcast_ref::<IcebergTableScan>() {
        Some(scan) => {
            let row_count = if table_stats_enabled() {
                snapshot_row_count(scan.table(), scan.snapshot_id()).await
            } else {
                None
            };
            Arc::new(TunedIcebergScan::from_upstream(
                scan,
                concurrency,
                row_count,
            ))
        }
        None => plan,
    }
}

/// Executes the same table scan as the wrapped upstream plan, with explicit
/// IO concurrency limits. Mirrors `IcebergTableScan::execute` (Apache-2.0).
#[derive(Debug)]
pub struct TunedIcebergScan {
    table: Table,
    snapshot_id: Option<i64>,
    projection: Option<Vec<String>>,
    predicate: Option<Predicate>,
    limit: Option<usize>,
    concurrency: usize,
    /// Live row count of the scanned snapshot (manifest-list sum), when
    /// known. Feeds `partition_statistics` so `JoinSelection` can pick the
    /// smaller build side instead of planning blind.
    row_count: Option<u64>,
    plan_properties: PlanProperties,
}

impl TunedIcebergScan {
    fn from_upstream(scan: &IcebergTableScan, concurrency: usize, row_count: Option<u64>) -> Self {
        let plan_properties = PlanProperties::new(
            EquivalenceProperties::new(scan.schema()),
            Partitioning::UnknownPartitioning(1),
            EmissionType::Incremental,
            Boundedness::Bounded,
        );
        Self {
            table: scan.table().clone(),
            snapshot_id: scan.snapshot_id(),
            projection: scan.projection().map(<[String]>::to_vec),
            predicate: scan.predicates().cloned(),
            limit: scan.limit(),
            concurrency,
            row_count,
            plan_properties,
        }
    }

    /// Statistics for the whole scan: the snapshot's row count, always
    /// reported INEXACT — deliberately, even for an unfiltered unlimited scan
    /// where the manifest sum IS the row count. `Precision::Exact` would let
    /// DataFusion's `AggregateStatistics` rule answer an ungrouped `COUNT(*)`
    /// straight from these statistics without executing the scan, making
    /// table METADATA result-bearing: a deployment with lost/unreadable data
    /// files would answer counts happily (gutting `icegres verify`'s
    /// count-probe durability checks and the suites' count assertions), and a
    /// foreign writer's dishonest manifest counts would become wrong query
    /// results. Inexact keeps statistics purely advisory — `JoinSelection`
    /// reads estimates via `get_value()` either way, so the build-side win is
    /// unaffected. Column statistics stay unknown.
    fn stats(&self) -> Statistics {
        let mut stats = Statistics::new_unknown(&self.schema());
        if let Some(n) = self.row_count.and_then(|n| usize::try_from(n).ok()) {
            stats.num_rows = Precision::Inexact(n);
        }
        stats
    }
}

async fn get_batch_stream(
    table: Table,
    snapshot_id: Option<i64>,
    projection: Option<Vec<String>>,
    predicate: Option<Predicate>,
    concurrency: usize,
) -> DFResult<Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>>> {
    let scan_builder = match snapshot_id {
        Some(snapshot_id) => table.scan().snapshot_id(snapshot_id),
        None => table.scan(),
    };
    let mut scan_builder = match projection {
        Some(columns) => scan_builder.select(columns),
        None => scan_builder.select_all(),
    };
    if let Some(predicate) = predicate {
        scan_builder = scan_builder.with_filter(predicate);
    }
    scan_builder = scan_builder.with_concurrency_limit(concurrency);
    let batch_size = scan_batch_size();
    if batch_size != 0 {
        scan_builder = scan_builder.with_batch_size(Some(batch_size));
    }
    if row_selection_enabled() {
        scan_builder = scan_builder.with_row_selection_enabled(true);
    }
    let table_scan = scan_builder.build().map_err(to_datafusion_error)?;
    let stream = table_scan
        .to_arrow()
        .await
        .map_err(to_datafusion_error)?
        .map_err(to_datafusion_error);
    Ok(Box::pin(stream))
}

impl ExecutionPlan for TunedIcebergScan {
    fn name(&self) -> &str {
        "TunedIcebergScan"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan + 'static>> {
        vec![]
    }

    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn properties(&self) -> &PlanProperties {
        &self.plan_properties
    }

    fn partition_statistics(&self, _partition: Option<usize>) -> DFResult<Statistics> {
        // One partition (UnknownPartitioning(1)): partition 0's statistics
        // and the whole plan's statistics are the same thing.
        Ok(self.stats())
    }

    fn execute(
        &self,
        _partition: usize,
        _context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let fut = get_batch_stream(
            self.table.clone(),
            self.snapshot_id,
            self.projection.clone(),
            self.predicate.clone(),
            self.concurrency,
        );
        let stream = futures::stream::once(fut).try_flatten();

        // Same limit semantics as the upstream IcebergTableScan.
        let limited: Pin<Box<dyn Stream<Item = DFResult<RecordBatch>> + Send>> =
            if let Some(limit) = self.limit {
                let mut remaining = limit;
                Box::pin(stream.try_filter_map(move |batch| {
                    futures::future::ready(if remaining == 0 {
                        Ok(None)
                    } else if batch.num_rows() <= remaining {
                        remaining -= batch.num_rows();
                        Ok(Some(batch))
                    } else {
                        let limited_batch = batch.slice(0, remaining);
                        remaining = 0;
                        Ok(Some(limited_batch))
                    })
                }))
            } else {
                Box::pin(stream)
            };

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema(),
            limited,
        )))
    }
}

impl DisplayAs for TunedIcebergScan {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "TunedIcebergScan concurrency={} rows={} projection:[{}] predicate:[{}]",
            self.concurrency,
            self.row_count
                .map_or("unknown".to_string(), |n| n.to_string()),
            self.projection
                .as_deref()
                .map_or(String::new(), |v| v.join(",")),
            self.predicate
                .as_ref()
                .map_or(String::new(), |p| format!("{p}")),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest(
        content: ManifestContentType,
        added: Option<u64>,
        existing: Option<u64>,
    ) -> ManifestFile {
        ManifestFile {
            manifest_path: "m.avro".into(),
            manifest_length: 1,
            partition_spec_id: 0,
            content,
            sequence_number: 1,
            min_sequence_number: 1,
            added_snapshot_id: 1,
            added_files_count: Some(1),
            existing_files_count: Some(0),
            deleted_files_count: Some(0),
            added_rows_count: added,
            existing_rows_count: existing,
            deleted_rows_count: Some(0),
            partitions: None,
            key_metadata: None,
            first_row_id: None,
        }
    }

    #[test]
    fn sums_added_and_existing_rows_across_data_manifests() {
        let entries = vec![
            manifest(ManifestContentType::Data, Some(100), Some(0)),
            manifest(ManifestContentType::Data, Some(50), Some(25)),
        ];
        assert_eq!(sum_live_rows(&entries), Some(175));
        assert_eq!(sum_live_rows(&[]), Some(0));
    }

    #[test]
    fn refuses_delete_manifests_and_missing_counts() {
        // A delete manifest means merge-on-read: the plain sum would
        // overcount, so the count must be refused, not guessed.
        let mor = vec![
            manifest(ManifestContentType::Data, Some(100), Some(0)),
            manifest(ManifestContentType::Deletes, Some(5), Some(0)),
        ];
        assert_eq!(sum_live_rows(&mor), None);
        // Missing row-count fields: unknowable, refused.
        let missing = vec![manifest(ManifestContentType::Data, None, Some(0))];
        assert_eq!(sum_live_rows(&missing), None);
    }
}
