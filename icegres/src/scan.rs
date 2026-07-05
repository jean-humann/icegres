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

use std::any::Any;
use std::pin::Pin;
use std::sync::{Arc, OnceLock};

use datafusion::arrow::array::RecordBatch;
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
use iceberg::table::Table;
use iceberg_datafusion::physical_plan::IcebergTableScan;
use iceberg_datafusion::to_datafusion_error;
use tracing::warn;

/// Default IO concurrency for Iceberg scans (manifest files, manifest
/// entries, and data files). Tuned on the 4-core bench box against local
/// RustFS; see bench/SCORECARD.md.
pub const DEFAULT_SCAN_CONCURRENCY: usize = 32;

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

/// If `plan` is an upstream [`IcebergTableScan`], replace it with a
/// [`TunedIcebergScan`] running the same scan at the configured IO
/// concurrency. Any other plan (metadata tables, inserts, ...) is returned
/// unchanged.
pub fn tune(plan: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
    let concurrency = scan_concurrency();
    if concurrency == 0 {
        return plan;
    }
    match plan.as_any().downcast_ref::<IcebergTableScan>() {
        Some(scan) => Arc::new(TunedIcebergScan::from_upstream(scan, concurrency)),
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
    plan_properties: PlanProperties,
}

impl TunedIcebergScan {
    fn from_upstream(scan: &IcebergTableScan, concurrency: usize) -> Self {
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
            plan_properties,
        }
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
    let table_scan = scan_builder
        .with_concurrency_limit(concurrency)
        .build()
        .map_err(to_datafusion_error)?;
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
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "TunedIcebergScan concurrency={} projection:[{}] predicate:[{}]",
            self.concurrency,
            self.projection
                .as_deref()
                .map_or(String::new(), |v| v.join(",")),
            self.predicate
                .as_ref()
                .map_or(String::new(), |p| format!("{p}")),
        )
    }
}
