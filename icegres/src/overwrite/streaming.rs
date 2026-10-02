//! Ranged Parquet reads and deterministic batch evaluation for copy-on-write DML.
//! Prefix replay can retain two compressed row groups. These buffers, the key
//! set and writer buffers are outside the decoded batch working set measured here.

use super::*;
use std::ops::Range;
use std::sync::atomic::{AtomicU64, Ordering};

use arrow::array::BooleanArray;
use arrow::compute::filter_record_batch;
use datafusion::common::{
    tree_node::{TreeNode, TreeNodeRecursion},
    DFSchema, ScalarValue, TableReference,
};
use datafusion::logical_expr::{Expr, Operator, Volatility};
use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use datafusion::parquet::arrow::async_reader::AsyncFileReader;
use datafusion::parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use datafusion::parquet::errors::{ParquetError, Result as ParquetResult};
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::physical_expr::PhysicalExpr;
use futures::{future::BoxFuture, TryStreamExt};
use iceberg::expr::{Predicate, Reference};
use iceberg::io::FileRead;
use iceberg::spec::Datum;
use prost::bytes::Bytes;

pub(super) const BATCH_ROWS: usize = 8192;

#[derive(Default, Debug)]
pub(super) struct ScanStats {
    pub files: AtomicU64,
    pub pruned_files: AtomicU64,
    pub pk_only_files: AtomicU64,
    pub streamed_files: AtomicU64,
    pub fallback_files: AtomicU64,
    pub read_requests: AtomicU64,
    pub read_bytes: AtomicU64,
    pub prefix_read_bytes: AtomicU64,
    /// Logical Arrow bytes concurrently held by batch evaluation, excluding
    /// Parquet page/row-group buffers, writer buffers and retained PK columns.
    pub peak_batch_bytes: AtomicU64,
    pub output_rows: AtomicU64,
}

impl ScanStats {
    fn observe(&self, bytes: usize) {
        self.peak_batch_bytes
            .fetch_max(bytes as u64, Ordering::Relaxed);
    }
}

#[derive(Clone)]
struct RangeReader {
    reader: Arc<dyn FileRead>,
    metadata: Arc<ParquetMetaData>,
    stats: Arc<ScanStats>,
    prefix: bool,
}

impl AsyncFileReader for RangeReader {
    fn get_bytes(&mut self, range: Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        Box::pin(async move {
            self.stats.read_requests.fetch_add(1, Ordering::Relaxed);
            let bytes = self
                .reader
                .read(range)
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))?;
            self.stats
                .read_bytes
                .fetch_add(bytes.len() as u64, Ordering::Relaxed);
            if self.prefix {
                self.stats
                    .prefix_read_bytes
                    .fetch_add(bytes.len() as u64, Ordering::Relaxed);
            }
            Ok(bytes)
        })
    }

    fn get_metadata<'a>(
        &'a mut self,
        _options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, ParquetResult<Arc<ParquetMetaData>>> {
        Box::pin(async move { Ok(self.metadata.clone()) })
    }
}

pub(super) struct ParquetSource {
    reader: RangeReader,
    metadata: ArrowReaderMetadata,
}

impl ParquetSource {
    pub async fn open(
        file_io: &iceberg::io::FileIO,
        file: &DataFile,
        stats: Arc<ScanStats>,
    ) -> Result<Self> {
        anyhow::ensure!(
            file.file_format() == DataFileFormat::Parquet,
            "unsupported non-Parquet data file {}",
            file.file_path()
        );
        let reader: Arc<dyn FileRead> = file_io.new_input(file.file_path())?.reader().await?.into();
        let size = file.file_size_in_bytes();
        anyhow::ensure!(
            size >= FOOTER_SIZE as u64,
            "Parquet file is smaller than its footer"
        );
        let read = |range| {
            let reader = reader.clone();
            let stats = stats.clone();
            async move {
                stats.read_requests.fetch_add(1, Ordering::Relaxed);
                let bytes = reader.read(range).await?;
                stats
                    .read_bytes
                    .fetch_add(bytes.len() as u64, Ordering::Relaxed);
                Ok::<_, anyhow::Error>(bytes)
            }
        };
        let tail = read(size - FOOTER_SIZE as u64..size).await?;
        let tail: [u8; FOOTER_SIZE] = tail.as_ref().try_into().context("short Parquet footer")?;
        let len = FooterTail::try_new(&tail)?.metadata_length() as u64;
        anyhow::ensure!(
            len <= size - FOOTER_SIZE as u64,
            "invalid Parquet metadata length"
        );
        let bytes = read(size - FOOTER_SIZE as u64 - len..size - FOOTER_SIZE as u64).await?;
        let metadata = Arc::new(ParquetMetaDataReader::decode_metadata(&bytes)?);
        let arrow_metadata =
            ArrowReaderMetadata::try_new(metadata.clone(), ArrowReaderOptions::default())?;
        Ok(Self {
            reader: RangeReader {
                reader,
                metadata,
                stats,
                prefix: false,
            },
            metadata: arrow_metadata,
        })
    }

    pub fn schema(&self) -> &ArrowSchemaRef {
        self.metadata.schema()
    }

    pub fn stream(
        &self,
        columns: Option<&[usize]>,
        limit: Option<usize>,
        prefix: bool,
    ) -> Result<impl futures::Stream<Item = ParquetResult<RecordBatch>> + Send + Unpin> {
        let mut reader = self.reader.clone();
        reader.prefix = prefix;
        let mut builder =
            ParquetRecordBatchStreamBuilder::new_with_metadata(reader, self.metadata.clone())
                .with_batch_size(BATCH_ROWS);
        if let Some(columns) = columns {
            let projection =
                ProjectionMask::roots(builder.parquet_schema(), columns.iter().copied());
            builder = builder.with_projection(projection);
        }
        if let Some(limit) = limit {
            builder = builder.with_limit(limit);
        }
        Ok(builder.build()?)
    }
}

/// Expressions are compiled once. Unknown expression forms, volatile/stable
/// functions and window/aggregate semantics retain the existing file evaluator.
pub(super) struct BatchDml {
    pub op_index: usize,
    predicate: Option<Arc<dyn PhysicalExpr>>,
    logical_predicate: Option<Expr>,
    update: Option<Vec<Arc<dyn PhysicalExpr>>>,
}

fn row_local(expr: &Expr) -> bool {
    let mut safe = true;
    let result = expr.apply(|e| {
        let supported = match e {
            Expr::Column(_)
            | Expr::Literal(_, _)
            | Expr::Alias(_)
            | Expr::BinaryExpr(_)
            | Expr::Like(_)
            | Expr::SimilarTo(_)
            | Expr::Not(_)
            | Expr::IsNotNull(_)
            | Expr::IsNull(_)
            | Expr::IsTrue(_)
            | Expr::IsFalse(_)
            | Expr::IsUnknown(_)
            | Expr::IsNotTrue(_)
            | Expr::IsNotFalse(_)
            | Expr::IsNotUnknown(_)
            | Expr::Negative(_)
            | Expr::Between(_)
            | Expr::Case(_)
            | Expr::Cast(_)
            | Expr::TryCast(_)
            | Expr::InList(_) => true,
            Expr::ScalarFunction(f) => f.func.signature().volatility == Volatility::Immutable,
            _ => false,
        };
        if !supported {
            safe = false;
            return Ok(TreeNodeRecursion::Stop);
        }
        Ok(TreeNodeRecursion::Continue)
    });
    result.is_ok() && safe
}

impl BatchDml {
    fn compile(
        op_index: usize,
        stmt: &DmlStatement,
        target: &ArrowSchemaRef,
    ) -> Result<Option<Self>> {
        let ctx = SessionContext::new();
        let qualifier = stmt
            .alias
            .as_ref()
            .map(|a| TableReference::bare(a.clone()))
            .unwrap_or_else(|| TableReference::partial(stmt.namespace.clone(), stmt.table.clone()));
        let schema = DFSchema::try_from_qualified_schema(qualifier, target)?;
        let logical_predicate = stmt
            .predicate
            .as_ref()
            .map(|p| ctx.parse_sql_expr(p, &schema))
            .transpose()?;
        if logical_predicate.as_ref().is_some_and(|p| !row_local(p)) {
            return Ok(None);
        }
        let predicate = logical_predicate
            .as_ref()
            .map(|p| ctx.create_physical_expr(p.clone(), &schema))
            .transpose()?;
        let update = match &stmt.kind {
            DmlKind::Delete => None,
            DmlKind::Update { assignments } => {
                let mut projection = Vec::new();
                for field in target.fields() {
                    let name = quote_ident(field.name());
                    let sql = match assignments
                        .iter()
                        .rev()
                        .find(|(column, _)| column == field.name())
                    {
                        Some((_, value)) => match &stmt.predicate {
                            Some(predicate) => {
                                format!("CASE WHEN ({predicate}) THEN ({value}) ELSE {name} END")
                            }
                            None => value.clone(),
                        },
                        None => name,
                    };
                    let expr = ctx.parse_sql_expr(&sql, &schema)?;
                    if !row_local(&expr) {
                        return Ok(None);
                    }
                    projection.push(ctx.create_physical_expr(expr, &schema)?);
                }
                Some(projection)
            }
        };
        Ok(Some(Self {
            op_index,
            predicate,
            logical_predicate,
            update,
        }))
    }

    fn apply(&self, input: &RecordBatch, target: &ArrowSchemaRef) -> Result<(u64, RecordBatch)> {
        if input.num_rows() == 0 {
            return Ok((0, input.clone()));
        }
        let selected = match &self.predicate {
            None => BooleanArray::from(vec![true; input.num_rows()]),
            Some(expr) => {
                let values = expr.evaluate(input)?.into_array(input.num_rows())?;
                let values = values
                    .as_any()
                    .downcast_ref::<BooleanArray>()
                    .context("DML predicate must evaluate to boolean")?;
                BooleanArray::from_iter(values.iter().map(|value| Some(value.unwrap_or(false))))
            }
        };
        let matched = selected.true_count() as u64;
        if matched == 0 {
            return Ok((0, input.clone()));
        }
        let Some(projection) = &self.update else {
            let keep = BooleanArray::from_iter(selected.values().iter().map(|v| Some(!v)));
            return Ok((matched, filter_record_batch(input, &keep)?));
        };
        let arrays = projection
            .iter()
            .zip(target.fields())
            .map(|(expr, field)| {
                let values = expr.evaluate(input)?.into_array(input.num_rows())?;
                // Match align_batch's strict cast behavior after DataFusion coercion.
                Ok(cast_with_options(
                    &values,
                    field.data_type(),
                    &CastOptions {
                        safe: false,
                        ..Default::default()
                    },
                )?)
            })
            .collect::<Result<Vec<_>>>()?;
        Ok((matched, RecordBatch::try_new(target.clone(), arrays)?))
    }
}

pub(super) fn compile_ops(ops: &[TableOp], target: &ArrowSchemaRef) -> Option<Vec<BatchDml>> {
    ops.iter()
        .enumerate()
        .filter_map(|(i, op)| match op {
            TableOp::Dml(stmt) => Some(BatchDml::compile(i, stmt, target).ok().flatten()),
            _ => None,
        })
        .collect()
}

pub(super) async fn read_keys(
    source: &ParquetSource,
    keys: &[String],
    target: &ArrowSchemaRef,
    output: &mut Vec<RecordBatch>,
    stats: &ScanStats,
) -> Result<()> {
    let indices = keys
        .iter()
        .map(|key| source.schema().index_of(key))
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let canonical = Arc::new(
        target.project(
            &keys
                .iter()
                .map(|key| target.index_of(key))
                .collect::<std::result::Result<Vec<_>, _>>()?,
        )?,
    );
    let mut stream = source.stream(Some(&indices), None, false)?;
    while let Some(batch) = stream.try_next().await? {
        stats.observe(batch.get_array_memory_size());
        if batch.num_rows() != 0 {
            // ProjectionMask keeps physical order; a composite key may declare
            // a different order, shared with rewritten and appended key rows.
            output.push(align_batch(&project_columns(&[batch], keys)?, &canonical)?);
        }
    }
    Ok(())
}

/// Keep only one input/output batch. Upon the first match, reread the unchanged
/// prefix without reevaluating expressions. No-match files create no output.
#[allow(clippy::too_many_arguments)]
pub(super) async fn rewrite_file<W: IcebergWriter<RecordBatch, Vec<DataFile>>>(
    source: &ParquetSource,
    programs: &[BatchDml],
    target: &ArrowSchemaRef,
    writer: &mut W,
    pk: Option<&[String]>,
    pk_rows: &mut Vec<RecordBatch>,
    rows_by_op: &mut [u64],
    stats: &ScanStats,
) -> Result<bool> {
    let mut stream = source.stream(None, None, false)?;
    let mut changed = false;
    let mut prefix_rows = 0usize;
    while let Some(batch) = stream.try_next().await? {
        let input_bytes = batch.get_array_memory_size();
        let mut output = batch.clone();
        let mut batch_changed = false;
        for program in programs {
            let before_bytes = output.get_array_memory_size();
            let (matched, after) = program.apply(&output, target)?;
            stats.observe(input_bytes + before_bytes + after.get_array_memory_size());
            rows_by_op[program.op_index] += matched;
            batch_changed |= matched != 0;
            output = after;
        }
        if let Some(keys) = pk {
            if output.num_rows() != 0 {
                pk_rows.push(project_columns(&[output.clone()], keys)?);
            }
        }
        if batch_changed && !changed {
            if prefix_rows != 0 {
                let mut prefix = source.stream(None, Some(prefix_rows), true)?;
                while let Some(previous) = prefix.try_next().await? {
                    stats.observe(
                        input_bytes
                            + output.get_array_memory_size()
                            + previous.get_array_memory_size(),
                    );
                    stats
                        .output_rows
                        .fetch_add(previous.num_rows() as u64, Ordering::Relaxed);
                    writer.write(align_batch(&previous, target)?).await?;
                }
            }
            changed = true;
        }
        if changed && output.num_rows() != 0 {
            stats
                .output_rows
                .fetch_add(output.num_rows() as u64, Ordering::Relaxed);
            writer.write(align_batch(&output, target)?).await?;
        }
        prefix_rows += batch.num_rows();
    }
    Ok(changed)
}

fn literal_for(value: &ScalarValue, dtype: &DataType) -> Option<Datum> {
    let integer = match value {
        ScalarValue::Int64(Some(v)) => Some(*v),
        ScalarValue::Int32(Some(v)) => Some(i64::from(*v)),
        _ => None,
    };
    match dtype {
        DataType::Int64 => Some(Datum::long(integer?)),
        DataType::Int32 => Some(Datum::int(i32::try_from(integer?).ok()?)),
        DataType::Boolean => match value {
            ScalarValue::Boolean(Some(v)) => Some(Datum::bool(*v)),
            _ => None,
        },
        DataType::Date32 => match value {
            ScalarValue::Date32(Some(v)) => Some(Datum::date(*v)),
            _ => None,
        },
        _ => None,
    }
}

fn prune_predicate(expr: &Expr, schema: &ArrowSchema) -> Option<Predicate> {
    match expr {
        Expr::BinaryExpr(binary) if binary.op == Operator::And => {
            match (
                prune_predicate(&binary.left, schema),
                prune_predicate(&binary.right, schema),
            ) {
                (Some(a), Some(b)) => Some(a.and(b)),
                (a, b) => a.or(b),
            }
        }
        Expr::BinaryExpr(binary) if binary.op == Operator::Or => {
            Some(prune_predicate(&binary.left, schema)?.or(prune_predicate(&binary.right, schema)?))
        }
        Expr::BinaryExpr(binary) => {
            let (column, literal, reversed) = match (binary.left.as_ref(), binary.right.as_ref()) {
                (Expr::Column(column), Expr::Literal(value, _)) => (column, value, false),
                (Expr::Literal(value, _), Expr::Column(column)) => (column, value, true),
                _ => return None,
            };
            let field = schema.field_with_name(&column.name).ok()?;
            let value = literal_for(literal, field.data_type())?;
            let reference = Reference::new(column.name.clone());
            Some(match (binary.op, reversed) {
                (Operator::Eq, _) => reference.equal_to(value),
                (Operator::Lt, false) | (Operator::Gt, true) => reference.less_than(value),
                (Operator::LtEq, false) | (Operator::GtEq, true) => {
                    reference.less_than_or_equal_to(value)
                }
                (Operator::Gt, false) | (Operator::Lt, true) => reference.greater_than(value),
                (Operator::GtEq, false) | (Operator::LtEq, true) => {
                    reference.greater_than_or_equal_to(value)
                }
                _ => return None,
            })
        }
        Expr::IsNull(inner) | Expr::IsNotNull(inner) => {
            let Expr::Column(column) = inner.as_ref() else {
                return None;
            };
            let field = schema.field_with_name(&column.name).ok()?;
            if !matches!(
                field.data_type(),
                DataType::Int32 | DataType::Int64 | DataType::Boolean | DataType::Date32
            ) {
                return None;
            }
            let reference = Reference::new(column.name.clone());
            Some(if matches!(expr, Expr::IsNull(_)) {
                reference.is_null()
            } else {
                reference.is_not_null()
            })
        }
        _ => None,
    }
}

pub(super) async fn candidate_files(
    table: &Table,
    head: Option<i64>,
    ops: &[TableOp],
    programs: Option<&[BatchDml]>,
    schema: &ArrowSchema,
) -> Result<Option<HashSet<String>>> {
    // Composed operations can change predicate columns. Their original file
    // bounds cannot determine whether later operations will match.
    if ops.len() != 1 {
        return Ok(None);
    }
    let Some([program]) = programs else {
        return Ok(None);
    };
    let Some(predicate) = program
        .logical_predicate
        .as_ref()
        .and_then(|p| prune_predicate(p, schema))
    else {
        return Ok(None);
    };
    let Some(head) = head else {
        return Ok(None);
    };
    let scan = match table
        .scan()
        .snapshot_id(head)
        .with_filter(predicate)
        .select_empty()
        .build()
    {
        Ok(scan) => scan,
        // A schema-only change may leave a snapshot with an older schema.
        // Binding against it is optional optimization, never permission to skip.
        Err(_) => return Ok(None),
    };
    let paths = scan
        .plan_files()
        .await?
        .map_ok(|task| task.data_file_path)
        .try_collect()
        .await?;
    Ok(Some(paths))
}
