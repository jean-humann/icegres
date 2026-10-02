//! ORM/driver compatibility shims (SPEC A8).
//!
//! Real ORMs (SQLAlchemy) and drivers issue pg_catalog introspection SQL
//! that the upstream datafusion-pg-catalog emulation cannot plan verbatim:
//!
//! 1. `select version()` resolves to DataFusion's builtin `version()` UDF,
//!    whose string ("Apache DataFusion x.y.z, ...") SQLAlchemy refuses to
//!    parse. [`register_compat_udfs`] shadows it with a Postgres-shaped
//!    version string (major.minor kept in sync with the `server_version`
//!    startup parameter pgwire sends).
//! 2. `pg_get_indexdef(oid, n, bool)` does not exist in the emulation; a
//!    NULL-returning stub is registered so index introspection plans.
//! 3. `pg_index.indkey` / `indclass` / `indoption` and
//!    `pg_constraint.conkey` / `confkey` are stored as TEXT
//!    (`"1 2"` / `"[1, 2]"`), so `unnest(...)` / `generate_subscripts(...)`
//!    over them fail at planning. [`CompatHook`] rewrites them to
//!    `unnest(string_to_array(...))` forms (`generate_subscripts(x, 1)`
//!    becomes `unnest(generate_series(1, array_length(...)))`, preserving
//!    ordinality).
//! 4. Correlated scalar subqueries NESTED inside expressions (e.g. the
//!    collation CASE in SQLAlchemy's get_columns) fail with "correlated
//!    scalar subquery must be aggregated"; the hook wraps their single
//!    projected column in `max(...)` (a no-op for the at-most-one-row
//!    lookups these queries perform). Top-level projection subqueries are
//!    already NULLed upstream by `RemoveSubqueryFromProjection`.
//! 5. `pg_class.reloptions` is missing from the static table; projection
//!    references are replaced with `CAST(NULL AS TEXT) AS reloptions`.
//! 6. The static `pg_type.typnamespace` carries stock Postgres namespace
//!    oids that never match the dynamic `pg_namespace`'s minted oids, so
//!    `JOIN pg_namespace` over types returns zero rows — which is Npgsql's
//!    connect-time type-loading query (Power BI / Excel). The coherent
//!    snapshot re-materializes `pg_type` with `typnamespace` rewritten to
//!    the snapshot's real `pg_catalog` namespace oid.
//! 7. SQLAlchemy schema reflection uses a custom LIKE escape. The hook
//!    translates only the pattern to Arrow's backslash convention, preserving
//!    literal backslashes and prepared parameters. Its nested identity and
//!    collation expressions also need post-order catalog cast normalization
//!    and JSON construction for metadata values.
//!
//! The hook intercepts ONLY plain `SELECT` statements whose table references
//! all live in `pg_catalog` AND that one of the rewrites actually changed —
//! every other statement flows to the later hooks / default handler
//! untouched, so the data-query hot path never pays for this. Because the
//! rewritten statements touch static catalog tables only, executing them on
//! the shared (non-transaction-pinned) context is safe even while the
//! connection has an open transaction.

use std::any::Any;
use std::collections::HashMap;
use std::ops::ControlFlow;
use std::sync::Arc;

use async_trait::async_trait;
use datafusion::arrow::array::{Array, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::{DataType, SchemaRef};
use datafusion::catalog::{CatalogProviderList, SchemaProvider, Session, TableProvider};
use datafusion::common::{ParamValues, ScalarValue};
use datafusion::datasource::MemTable;
use datafusion::error::DataFusionError;
use datafusion::logical_expr::{ColumnarValue, LogicalPlan, ScalarUDF, TableType, Volatility};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{create_udf, SessionContext};
use datafusion::sql::sqlparser::ast::{
    BinaryOperator, Expr, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, ObjectName,
    Query, SelectItem, SetExpr, Statement, Value, Visit, VisitMut, Visitor, VisitorMut,
};
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;
use datafusion_postgres::arrow_pg::datatypes::df as pgdf;
use datafusion_postgres::pgwire::api::portal::Format;
use datafusion_postgres::pgwire::api::results::{Response, Tag};
use datafusion_postgres::pgwire::api::ClientInfo;
use datafusion_postgres::pgwire::error::{PgWireError, PgWireResult};
use datafusion_postgres::pgwire::types::format::FormatOptions;
use datafusion_postgres::QueryHook;

/// Postgres-shaped `version()` string. The `16.6` major.minor matches the
/// `server_version` startup parameter reported by pgwire, so drivers that
/// read either source agree on the emulated server version.
fn version_string() -> String {
    format!(
        "PostgreSQL 16.6 (icegres {}) on x86_64-pc-linux-gnu, Apache DataFusion 52.5.0",
        env!("CARGO_PKG_VERSION")
    )
}

/// Register the compatibility UDFs on the session (serve path, after
/// `setup_pg_catalog`): a Postgres-parseable `version()` (shadows the
/// DataFusion builtin) and a NULL-returning `pg_get_indexdef` stub.
pub fn register_compat_udfs(ctx: &SessionContext) {
    let version = version_string();
    ctx.register_udf(create_udf(
        "version",
        vec![],
        DataType::Utf8,
        Volatility::Stable,
        Arc::new(move |_args: &[ColumnarValue]| {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some(
                version.clone(),
            ))))
        }),
    ));
    // Index definitions have no meaning over Iceberg tables (and the static
    // pg_index rows never join to user tables), so NULL is always correct.
    ctx.register_udf(ScalarUDF::new_from_impl(ConstStub::new(
        "pg_get_indexdef",
        ScalarValue::Utf8(None),
    )));
    // Everything the emulated catalog exposes is on the search path.
    ctx.register_udf(ScalarUDF::new_from_impl(ConstStub::new(
        "pg_type_is_visible",
        ScalarValue::Boolean(Some(true)),
    )));
    // Iceberg has no PostgreSQL serial/identity sequences, and the emulated
    // collations belong to the visible pg_catalog namespace.
    ctx.register_udf(ScalarUDF::new_from_impl(ConstStub::new(
        "pg_get_serial_sequence",
        ScalarValue::Utf8(None),
    )));
    ctx.register_udf(ScalarUDF::new_from_impl(ConstStub::new(
        "pg_collation_is_visible",
        ScalarValue::Boolean(Some(true)),
    )));
    ctx.register_udf(create_udf(
        "icegres_catalog_like_pattern",
        vec![DataType::Utf8, DataType::Utf8],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|args| {
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let patterns = arrays[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Execution("LIKE pattern must be text".into()))?;
            let escapes = arrays[1]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| DataFusionError::Execution("LIKE escape must be text".into()))?;
            let values = patterns
                .iter()
                .zip(escapes.iter())
                .map(|(pattern, escape)| match (pattern, escape) {
                    (Some(pattern), Some(escape)) => {
                        normalize_like_pattern(pattern, escape).map(Some)
                    }
                    _ => Ok(None),
                })
                .collect::<Result<Vec<_>, DataFusionError>>()?;
            if args.iter().all(|v| matches!(v, ColumnarValue::Scalar(_))) {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                    values.into_iter().next().flatten(),
                )))
            } else {
                Ok(ColumnarValue::Array(Arc::new(StringArray::from(values))))
            }
        }),
    ));
    ctx.register_udf(ScalarUDF::new_from_impl(CatalogJsonObject {
        signature: datafusion::logical_expr::Signature::variadic_any(Volatility::Immutable),
    }));
    // SQLAlchemy 2.1 asks for the default PostgreSQL access method while
    // reflecting table options. This is an emulation constant; session
    // settings such as search_path must never be answered from shared state.
    ctx.register_udf(create_udf(
        "icegres_catalog_setting",
        vec![DataType::Utf8],
        DataType::Utf8,
        Volatility::Immutable,
        Arc::new(|args| {
            let arrays = ColumnarValue::values_to_arrays(args)?;
            let names = arrays[0]
                .as_any()
                .downcast_ref::<StringArray>()
                .ok_or_else(|| {
                    DataFusionError::Execution("catalog setting name must be text".into())
                })?;
            let values = names
                .iter()
                .map(|name| match name {
                    Some("default_table_access_method") => Ok(Some("heap".to_owned())),
                    None => Ok(None),
                    Some(name) => Err(DataFusionError::Execution(format!(
                        "current_setting({name:?}) is not supported by catalog compatibility"
                    ))),
                })
                .collect::<Result<Vec<_>, _>>()?;
            if matches!(args[0], ColumnarValue::Scalar(_)) {
                Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                    values.into_iter().next().flatten(),
                )))
            } else {
                Ok(ColumnarValue::Array(Arc::new(StringArray::from(values))))
            }
        }),
    ));
}

/// Convert PostgreSQL LIKE escaping to Arrow's backslash escaping. A quoted
/// ordinary character loses its escape; literal backslashes remain literal.
/// Empty ESCAPE disables escaping. Never change the value being matched.
fn normalize_like_pattern(pattern: &str, escape: &str) -> Result<String, DataFusionError> {
    let mut escape_chars = escape.chars();
    let escape = escape_chars.next();
    if escape_chars.next().is_some() {
        return Err(DataFusionError::Execution(
            "LIKE escape must be empty or one character".into(),
        ));
    }
    let mut normalized = String::with_capacity(pattern.len());
    let mut chars = pattern.chars();
    while let Some(ch) = chars.next() {
        if Some(ch) == escape {
            let quoted = chars.next().ok_or_else(|| {
                DataFusionError::Execution("LIKE pattern must not end with escape character".into())
            })?;
            if matches!(quoted, '%' | '_' | '\\') {
                normalized.push('\\');
            }
            normalized.push(quoted);
        } else {
            if ch == '\\' {
                normalized.push('\\');
            }
            normalized.push(ch);
        }
    }
    Ok(normalized)
}

/// JSON construction for catalog reflection. Keep every argument in the plan,
/// including placeholders in unreachable identity/collation CASE branches.
/// Arrow's JSON writer preserves numbers, booleans, strings and nulls instead
/// of converting all values to strings.
#[derive(Debug, PartialEq, Eq, Hash)]
struct CatalogJsonObject {
    signature: datafusion::logical_expr::Signature,
}

impl datafusion::logical_expr::ScalarUDFImpl for CatalogJsonObject {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn name(&self) -> &str {
        "icegres_catalog_json_object"
    }
    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> Result<DataType, DataFusionError> {
        Ok(DataType::Utf8)
    }
    fn invoke_with_args(
        &self,
        args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> Result<ColumnarValue, DataFusionError> {
        use datafusion::arrow::datatypes::{Field, Schema};
        use datafusion::arrow::json::writer::{JsonArray, WriterBuilder};

        if !args.args.len().is_multiple_of(2) {
            return Err(DataFusionError::Execution(
                "json_build_object requires alternating keys and values".into(),
            ));
        }
        if args.args.is_empty() {
            return Ok(ColumnarValue::Scalar(ScalarValue::Utf8(Some("{}".into()))));
        }
        let arrays = ColumnarValue::values_to_arrays(&args.args)?;
        let values = arrays
            .iter()
            .skip(1)
            .step_by(2)
            .cloned()
            .collect::<Vec<_>>();
        let fields = values
            .iter()
            .enumerate()
            .map(|(i, value)| Field::new(i.to_string(), value.data_type().clone(), true))
            .collect::<Vec<_>>();
        let batch = RecordBatch::try_new(Arc::new(Schema::new(fields)), values)?;
        let mut writer = WriterBuilder::new()
            .with_explicit_nulls(true)
            .build::<_, JsonArray>(Vec::new());
        writer.write(&batch)?;
        writer.finish()?;
        let rows: Vec<serde_json::Map<String, serde_json::Value>> =
            serde_json::from_slice(&writer.into_inner())
                .map_err(|e| DataFusionError::Execution(e.to_string()))?;
        let keys = arrays
            .iter()
            .step_by(2)
            .map(|array| cast(array, &DataType::Utf8))
            .collect::<Result<Vec<_>, _>>()?;
        let mut results = Vec::with_capacity(rows.len());
        for (row_index, mut row) in rows.into_iter().enumerate() {
            let mut object = serde_json::Map::new();
            for (key_index, keys) in keys.iter().enumerate() {
                let keys = keys.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
                    DataFusionError::Execution("json_build_object keys must be text".into())
                })?;
                if keys.is_null(row_index) {
                    return Err(DataFusionError::Execution(
                        "json_build_object keys must not be null".into(),
                    ));
                }
                object.insert(
                    keys.value(row_index).to_owned(),
                    row.remove(&key_index.to_string())
                        .unwrap_or(serde_json::Value::Null),
                );
            }
            results.push(serde_json::Value::Object(object).to_string());
        }
        if args
            .args
            .iter()
            .all(|v| matches!(v, ColumnarValue::Scalar(_)))
        {
            Ok(ColumnarValue::Scalar(ScalarValue::Utf8(
                results.into_iter().next(),
            )))
        } else {
            Ok(ColumnarValue::Array(Arc::new(StringArray::from(results))))
        }
    }
}

/// A pg_catalog function stub: accepts any arguments, always returns the
/// same constant. Used for functions whose real answer is meaningless over
/// an Iceberg catalog but whose absence breaks ORM introspection planning.
#[derive(Debug, PartialEq, Eq, Hash)]
struct ConstStub {
    name: &'static str,
    value: ScalarValue,
    signature: datafusion::logical_expr::Signature,
}

impl ConstStub {
    fn new(name: &'static str, value: ScalarValue) -> Self {
        Self {
            name,
            value,
            signature: datafusion::logical_expr::Signature::variadic_any(Volatility::Stable),
        }
    }
}

impl datafusion::logical_expr::ScalarUDFImpl for ConstStub {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn name(&self) -> &str {
        self.name
    }
    fn signature(&self) -> &datafusion::logical_expr::Signature {
        &self.signature
    }
    fn return_type(&self, _arg_types: &[DataType]) -> datafusion::error::Result<DataType> {
        Ok(self.value.data_type())
    }
    fn invoke_with_args(
        &self,
        _args: datafusion::logical_expr::ScalarFunctionArgs,
    ) -> datafusion::error::Result<ColumnarValue> {
        Ok(ColumnarValue::Scalar(self.value.clone()))
    }
}

// ---------------------------------------------------------------------------
// Coherent pg_class / pg_namespace / pg_attribute (oid drift fix)
// ---------------------------------------------------------------------------

/// The dynamic pg_catalog tables whose rows must agree on oids, in the ONE
/// materialization order that yields a consistent snapshot with upstream's
/// swap-cache behavior: each table's builder REPLACES the shared oid cache
/// with only the keys it produced, so
///   pg_attribute (allocates/reuses table oids)
///   -> pg_class   (reuses those table oids, allocates schema oids)
///   -> pg_namespace (reuses pg_class's schema oids)
/// always agrees within a generation. Scanned independently (upstream
/// behavior), any interleaving that runs pg_attribute between pg_class and
/// pg_namespace reallocates the schema oids and `pg_class.relnamespace =
/// pg_namespace.oid` joins silently return zero rows — which is exactly the
/// join every ORM's reflection queries perform.
const COHERENT_TABLES: [&str; 3] = ["pg_attribute", "pg_class", "pg_namespace"];

/// Static-table oid patch riding the same snapshot: upstream's `pg_type` is a
/// static table whose `typnamespace` column carries STOCK Postgres namespace
/// oids (11, 13283, …), while the dynamic `pg_namespace` mints its own oids —
/// so `pg_type JOIN pg_namespace ON ns.oid = typnamespace` silently returns
/// zero rows. That join is exactly Npgsql's connect-time type-loading query
/// (the driver under Power BI / Excel): with zero types loaded, every typed
/// read and every parameter bind fails. Each generation therefore also
/// re-materializes `pg_type` with `typnamespace` rewritten to the snapshot's
/// actual `pg_catalog` namespace oid. Honest scope: the static export also
/// carries information_schema helper types (typnamespace 13283), which this
/// collapses into `pg_catalog` alongside the real catalog types — correct
/// for oid-keyed type loaders (Npgsql's is) and for the oid-equality joins
/// reflection performs; a name+namespace-keyed lookup would now see those
/// helper types under `pg_catalog` too, which no probed client does.
const PATCHED_PG_TYPE: &str = "pg_type";

/// All tables served from the coherent snapshot: the trio plus the patched
/// static `pg_type`.
const SERVED_TABLES: [&str; 4] = ["pg_attribute", "pg_class", "pg_namespace", "pg_type"];

#[derive(Debug)]
struct Generation {
    fingerprint: String,
    batches: HashMap<&'static str, Vec<RecordBatch>>,
}

/// Shared materialization cache: one coherent snapshot of the trio, rebuilt
/// only when the set of catalogs/schemas/tables changes. As a bonus over
/// upstream, oids are now STABLE across queries on a running server (ORMs
/// carry oids from one introspection statement into the next).
#[derive(Debug)]
struct TrioCache {
    catalog_list: Arc<dyn CatalogProviderList>,
    upstream: HashMap<&'static str, Arc<dyn TableProvider>>,
    generation: tokio::sync::Mutex<Option<Generation>>,
}

impl TrioCache {
    fn fingerprint(&self) -> String {
        let mut parts = Vec::new();
        for c in self.catalog_list.catalog_names() {
            if let Some(cat) = self.catalog_list.catalog(&c) {
                for s in cat.schema_names() {
                    if let Some(schema) = cat.schema(&s) {
                        let mut ts = schema.table_names();
                        ts.sort();
                        parts.push(format!("{c}.{s}:{}", ts.join(",")));
                    }
                }
            }
        }
        parts.sort();
        parts.join(";")
    }

    /// Return the cached batches for `name`, rebuilding the whole trio (in
    /// [`COHERENT_TABLES`] order, under the lock, so nothing else interleaves
    /// with upstream's shared oid cache) when the table set changed.
    async fn batches_for(
        &self,
        name: &'static str,
        session: &dyn Session,
    ) -> Result<Vec<RecordBatch>, DataFusionError> {
        let fp = self.fingerprint();
        let mut guard = self.generation.lock().await;
        if guard.as_ref().map(|g| g.fingerprint == fp) != Some(true) {
            let mut batches = HashMap::new();
            for t in COHERENT_TABLES {
                let provider = self.upstream.get(t).expect("trio provider present");
                let plan = provider.scan(session, None, &[], None).await?;
                let collected =
                    datafusion::physical_plan::collect(plan, session.task_ctx()).await?;
                batches.insert(t, collected);
            }
            // pg_type is optional in the snapshot: when the upstream
            // emulation lacks it (a future matrix bump), the wrapper never
            // serves it and the WARN at install time already fired. A patch
            // FAILURE must not poison the generation either — the trio is
            // load-bearing for every ORM's reflection, and a shim for one
            // driver family degrading it to a total pg_catalog outage would
            // invert the priority. Serve pg_type unpatched instead, loudly.
            if let Some(provider) = self.upstream.get(PATCHED_PG_TYPE) {
                let plan = provider.scan(session, None, &[], None).await?;
                let collected =
                    datafusion::physical_plan::collect(plan, session.task_ctx()).await?;
                let ns_batches = batches
                    .get("pg_namespace")
                    .expect("pg_namespace just built");
                let patched = match pg_catalog_namespace_oid(ns_batches)
                    .and_then(|ns_oid| patch_typnamespace(collected.clone(), ns_oid))
                {
                    Ok(patched) => patched,
                    Err(e) => {
                        tracing::warn!(
                            "pg_type namespace-oid patch failed ({e}); serving pg_type \
                             unpatched — Npgsql-family type loading will see stock oids"
                        );
                        collected
                    }
                };
                batches.insert(PATCHED_PG_TYPE, patched);
            }
            *guard = Some(Generation {
                fingerprint: fp,
                batches,
            });
        }
        Ok(guard
            .as_ref()
            .expect("generation just ensured")
            .batches
            .get(name)
            .expect("trio table present")
            .clone())
    }
}

/// Find the `pg_catalog` namespace oid inside the freshly built
/// `pg_namespace` batches (oid column read via cast so the emulation's
/// concrete integer type never matters). Takes the FIRST matching row —
/// correct while the server registers exactly one catalog (`context.rs`
/// builds a single `icegres` catalog, so one pg_catalog schema exists).
fn pg_catalog_namespace_oid(batches: &[RecordBatch]) -> Result<i64, DataFusionError> {
    for batch in batches {
        let (Ok(name_idx), Ok(oid_idx)) = (
            batch.schema().index_of("nspname"),
            batch.schema().index_of("oid"),
        ) else {
            continue;
        };
        let names = cast(batch.column(name_idx), &DataType::Utf8)?;
        let names = names
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("just cast to Utf8");
        let oids = cast(batch.column(oid_idx), &DataType::Int64)?;
        let oids = oids
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("just cast to Int64");
        for i in 0..batch.num_rows() {
            if !names.is_null(i) && names.value(i) == "pg_catalog" && !oids.is_null(i) {
                return Ok(oids.value(i));
            }
        }
    }
    Err(DataFusionError::Internal(
        "pg_namespace has no pg_catalog row to anchor pg_type.typnamespace".into(),
    ))
}

/// Rewrite every `typnamespace` value in the pg_type batches to `ns_oid`,
/// preserving the column's original arrow type.
fn patch_typnamespace(
    batches: Vec<RecordBatch>,
    ns_oid: i64,
) -> Result<Vec<RecordBatch>, DataFusionError> {
    batches
        .into_iter()
        .map(|batch| {
            let Ok(idx) = batch.schema().index_of("typnamespace") else {
                return Ok(batch);
            };
            let original_type = batch.column(idx).data_type().clone();
            let filled = Int64Array::from(vec![ns_oid; batch.num_rows()]);
            let column = cast(&(Arc::new(filled) as Arc<dyn Array>), &original_type)?;
            let mut columns = batch.columns().to_vec();
            columns[idx] = column;
            RecordBatch::try_new(batch.schema(), columns).map_err(DataFusionError::from)
        })
        .collect()
}

/// One of the trio tables, served from the coherent snapshot.
#[derive(Debug)]
struct CoherentPgTable {
    name: &'static str,
    schema: SchemaRef,
    cache: Arc<TrioCache>,
}

#[async_trait]
impl TableProvider for CoherentPgTable {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[datafusion::logical_expr::Expr],
        limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>, DataFusionError> {
        let batches = self.cache.batches_for(self.name, state).await?;
        MemTable::try_new(self.schema.clone(), vec![batches])?
            .scan(state, projection, filters, limit)
            .await
    }
}

/// `pg_catalog` schema wrapper: delegates everything to the upstream
/// emulation except the trio and the patched `pg_type`, which serve the
/// coherent snapshot.
#[derive(Debug)]
struct CoherentPgCatalog {
    upstream: Arc<dyn SchemaProvider>,
    trio: HashMap<&'static str, Arc<CoherentPgTable>>,
}

#[async_trait]
impl SchemaProvider for CoherentPgCatalog {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn table_names(&self) -> Vec<String> {
        self.upstream.table_names()
    }
    async fn table(&self, name: &str) -> Result<Option<Arc<dyn TableProvider>>, DataFusionError> {
        if let Some(t) = self.trio.get(name) {
            return Ok(Some(t.clone() as Arc<dyn TableProvider>));
        }
        self.upstream.table(name).await
    }
    fn table_exist(&self, name: &str) -> bool {
        self.upstream.table_exist(name)
    }
}

/// Replace the `pg_catalog` schema registered by `setup_pg_catalog` with the
/// coherent wrapper. Must run after `setup_pg_catalog`.
pub async fn install_coherent_pg_catalog(
    ctx: &SessionContext,
    catalog_name: &str,
) -> anyhow::Result<()> {
    let catalog = ctx
        .catalog(catalog_name)
        .ok_or_else(|| anyhow::anyhow!("catalog {catalog_name} not registered"))?;
    let upstream = catalog
        .schema("pg_catalog")
        .ok_or_else(|| anyhow::anyhow!("pg_catalog schema not registered"))?;
    let mut upstream_trio: HashMap<&'static str, Arc<dyn TableProvider>> = HashMap::new();
    for name in COHERENT_TABLES {
        let provider = upstream
            .table(name)
            .await
            .map_err(|e| anyhow::anyhow!("failed to fetch pg_catalog.{name}: {e}"))?
            .ok_or_else(|| anyhow::anyhow!("pg_catalog.{name} missing from the emulation"))?;
        upstream_trio.insert(name, provider);
    }
    // The trio is load-bearing (ORM reflection breaks without it) and stays a
    // hard boot requirement; the pg_type patch degrades gracefully — if a
    // future dependency-matrix bump drops or renames the static table, serve
    // whatever upstream serves and say so, rather than turning a
    // compatibility shim into a boot failure.
    match upstream.table(PATCHED_PG_TYPE).await {
        Ok(Some(provider)) => {
            upstream_trio.insert(PATCHED_PG_TYPE, provider);
        }
        Ok(None) => tracing::warn!(
            "pg_catalog.{PATCHED_PG_TYPE} missing from the emulation — serving it \
             unpatched; Npgsql-family type loading will see stock namespace oids"
        ),
        Err(e) => tracing::warn!(
            "failed to fetch pg_catalog.{PATCHED_PG_TYPE} ({e}) — serving it \
             unpatched; Npgsql-family type loading will see stock namespace oids"
        ),
    }
    let cache = Arc::new(TrioCache {
        catalog_list: ctx.state().catalog_list().clone(),
        upstream: upstream_trio.clone(),
        generation: tokio::sync::Mutex::new(None),
    });
    let trio = SERVED_TABLES
        .into_iter()
        .filter(|name| upstream_trio.contains_key(name))
        .map(|name| {
            (
                name,
                Arc::new(CoherentPgTable {
                    name,
                    schema: upstream_trio[name].schema(),
                    cache: cache.clone(),
                }),
            )
        })
        .collect();
    catalog
        .register_schema("pg_catalog", Arc::new(CoherentPgCatalog { upstream, trio }))
        .map_err(|e| anyhow::anyhow!("failed to re-register pg_catalog: {e}"))?;
    Ok(())
}

// ---------------------------------------------------------------------------
// AST rewriting
// ---------------------------------------------------------------------------

/// Parse an SQL expression snippet built by the rewriter. The inputs are
/// constructed from already-parsed expressions, so failure is a rewriter
/// bug; `None` makes the caller leave the statement untouched.
fn parse_expr_snippet(sql: &str) -> Option<Expr> {
    Parser::new(&PostgreSqlDialect {})
        .try_with_sql(sql)
        .ok()?
        .parse_expr()
        .ok()
}

/// `string_to_array` form for a TEXT-encoded vector column, or `None` if the
/// column is not one of the known vector columns.
///
/// * `indkey`/`indclass`/`indoption` (int2vector/oidvector) render as
///   space-separated numbers (`"1 2"`).
/// * `conkey`/`confkey` (smallint[]) render as `"[1, 2]"` (may be empty, so
///   `nullif` keeps the cast row-free instead of failing on `''`).
fn vector_to_array_sql(col: &Expr) -> Option<String> {
    let last = match col {
        Expr::Identifier(id) => id.value.to_lowercase(),
        Expr::CompoundIdentifier(ids) => ids.last()?.value.to_lowercase(),
        _ => return None,
    };
    match last.as_str() {
        "indkey" | "indclass" | "indoption" => Some(format!("string_to_array({col}, ' ')")),
        "conkey" | "confkey" => Some(format!(
            "string_to_array(nullif(btrim({col}, '[]'), ''), ', ')"
        )),
        _ => None,
    }
}

/// True if `name`'s last segment equals `func` (matches both `unnest(..)`
/// and `pg_catalog.unnest(..)` spellings).
fn func_is(name: &ObjectName, func: &str) -> bool {
    name.0
        .last()
        .and_then(|p| p.as_ident())
        .is_some_and(|id| id.value.eq_ignore_ascii_case(func))
}

/// Nth unnamed argument of a function call, if any.
fn nth_arg(args: &FunctionArguments, n: usize) -> Option<&Expr> {
    if let FunctionArguments::List(list) = args {
        if let Some(FunctionArg::Unnamed(FunctionArgExpr::Expr(e))) = list.args.get(n) {
            return Some(e);
        }
    }
    None
}

fn first_arg(args: &FunctionArguments) -> Option<&Expr> {
    nth_arg(args, 0)
}

fn second_arg(args: &FunctionArguments) -> Option<&Expr> {
    nth_arg(args, 1)
}

/// True if the expression is an identifier whose last segment is `name`.
fn ident_ends_with(expr: &Expr, name: &str) -> bool {
    match expr {
        Expr::Identifier(id) => id.value.eq_ignore_ascii_case(name),
        Expr::CompoundIdentifier(ids) => ids
            .last()
            .is_some_and(|id| id.value.eq_ignore_ascii_case(name)),
        _ => false,
    }
}

/// If the expression is a cast to REGCLASS (either `CAST(x AS REGCLASS)` or
/// `x::regclass`) or a bare placeholder (upstream's RemoveUnsupportedTypes
/// strips regclass casts before this hook runs, leaving `$n`), return the
/// inner expression's SQL text.
fn regclass_cast_inner(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Cast {
            expr: inner,
            data_type,
            ..
        } if data_type.to_string().to_lowercase().ends_with("regclass") => Some(inner.to_string()),
        Expr::Value(v) if matches!(v.value, Value::Placeholder(_)) => Some(expr.to_string()),
        _ => None,
    }
}

/// Aggregates whose presence means a scalar subquery already plans.
const AGGREGATES: &[&str] = &[
    "max",
    "min",
    "count",
    "sum",
    "avg",
    "bool_and",
    "bool_or",
    "array_agg",
    "string_agg",
];

fn is_aggregate_call(expr: &Expr) -> bool {
    if let Expr::Function(f) = expr {
        AGGREGATES.iter().any(|a| func_is(&f.name, a))
    } else {
        false
    }
}

/// The rewriting visitor. Sets `changed` when any rule fired.
struct CompatRewriter {
    changed: bool,
}

impl CompatRewriter {
    /// Rule 4: wrap the single projected column of a scalar subquery in
    /// `max(...)` unless it already is an aggregate. Runs for every
    /// `Expr::Subquery` the visitor reaches — top-level projection
    /// subqueries were already replaced with NULL upstream, so this fires
    /// only for the nested (CASE/WHERE) ones DataFusion cannot plan
    /// un-aggregated when correlated.
    fn wrap_scalar_subquery(&mut self, q: &mut Query) {
        if let SetExpr::Select(select) = q.body.as_mut() {
            let grouped = match &select.group_by {
                GroupByExpr::Expressions(exprs, mods) => !exprs.is_empty() || !mods.is_empty(),
                GroupByExpr::All(_) => true,
            };
            if select.projection.len() != 1 || grouped {
                return;
            }
            let (expr, alias) = match &mut select.projection[0] {
                SelectItem::UnnamedExpr(e) => (e, None),
                SelectItem::ExprWithAlias { expr, alias } => (expr, Some(alias.clone())),
                _ => return,
            };
            if is_aggregate_call(expr) {
                return;
            }
            if let Some(wrapped) = parse_expr_snippet(&format!("max({expr})")) {
                select.projection[0] = match alias {
                    Some(alias) => SelectItem::ExprWithAlias {
                        expr: wrapped,
                        alias,
                    },
                    None => SelectItem::UnnamedExpr(wrapped),
                };
                self.changed = true;
            }
        }
    }
}

impl VisitorMut for CompatRewriter {
    type Break = ();

    fn pre_visit_query(&mut self, query: &mut Query) -> ControlFlow<Self::Break> {
        // Rule 5: pg_class.reloptions does not exist in the static catalog.
        if let SetExpr::Select(select) = query.body.as_mut() {
            for item in &mut select.projection {
                if let SelectItem::UnnamedExpr(Expr::CompoundIdentifier(ids)) = item {
                    if ids
                        .last()
                        .is_some_and(|id| id.value.eq_ignore_ascii_case("reloptions"))
                    {
                        if let Some(null_expr) = parse_expr_snippet("CAST(NULL AS TEXT)") {
                            let alias = ids.last().expect("checked above").clone();
                            *item = SelectItem::ExprWithAlias {
                                expr: null_expr,
                                alias,
                            };
                            self.changed = true;
                        }
                    }
                }
            }
        }
        ControlFlow::Continue(())
    }

    fn pre_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        // Rule 7: parenthesis-less keyword functions Postgres reserves —
        // CURRENT_CATALOG / CURRENT_SCHEMA always mean the function, never a
        // column. sqlparser's PostgreSqlDialect parses CURRENT_CATALOG as an
        // argument-less `Expr::Function` and CURRENT_SCHEMA as a plain
        // identifier; both fail DataFusion name resolution ("No field named
        // current_catalog"). pgjdbc's Connection.getCatalog() — called by
        // every DatabaseMetaData method — issues exactly
        // `select current_catalog`. Both AST shapes are rewritten to the
        // parenthesized spellings the engine resolves. (CURRENT_ROLE /
        // CURRENT_USER are NOT mapped here: the session user lives in the
        // wire session, not the shared SessionContext this hook executes on
        // — upstream answers `current_user` on its own path.)
        let keyword_fn = |name: &str| match name {
            "current_catalog" => Some("current_database()"),
            "current_schema" => Some("current_schema()"),
            _ => None,
        };
        let replacement = match expr {
            Expr::Identifier(id) => keyword_fn(&id.value.to_lowercase()),
            Expr::Function(f) if matches!(f.args, FunctionArguments::None) => f
                .name
                .0
                .last()
                .and_then(|p| p.as_ident())
                .and_then(|id| keyword_fn(&id.value.to_lowercase())),
            _ => None,
        };
        if let Some(snippet) = replacement {
            if let Some(new_expr) = parse_expr_snippet(snippet) {
                *expr = new_expr;
                self.changed = true;
                return ControlFlow::Continue(());
            }
        }
        // Rule 6: `<...>.classoid = CAST(x AS REGCLASS)` (SQLAlchemy's
        // comment lookups against pg_description). DataFusion cannot cast
        // to REGCLASS and infers the parameter as numeric (22P02 for
        // 'pg_catalog.pg_class'). The emulated pg_description is permanently
        // empty, so the comparison never matches anyway: rewrite it to an
        // always-false predicate that KEEPS the operand expression (extended-
        // protocol placeholders must survive, now inferred as text).
        if let Expr::BinaryOp { left, op, right } = expr {
            if matches!(op, BinaryOperator::Eq) {
                let class_cast = |a: &Expr, b: &Expr| {
                    ident_ends_with(a, "classoid")
                        .then(|| regclass_cast_inner(b))
                        .flatten()
                };
                if let Some(inner) = class_cast(left, right).or_else(|| class_cast(right, left)) {
                    if let Some(new_expr) =
                        parse_expr_snippet(&format!("length(CAST(({inner}) AS TEXT)) < 0"))
                    {
                        *expr = new_expr;
                        self.changed = true;
                        return ControlFlow::Continue(());
                    }
                }
            }
        }
        match expr {
            Expr::Like {
                pattern,
                escape_char,
                ..
            }
            | Expr::ILike {
                pattern,
                escape_char,
                ..
            } => {
                if let Some(escape) = escape_char.as_ref() {
                    if !matches!(escape, Value::SingleQuotedString(s) if s == "\\") {
                        if let Some(normalized) = parse_expr_snippet(&format!(
                            "icegres_catalog_like_pattern(({pattern}), {escape})"
                        )) {
                            **pattern = normalized;
                            *escape_char = Some(Value::SingleQuotedString("\\".into()));
                            self.changed = true;
                        }
                    }
                }
            }
            Expr::Function(f) => {
                let internal_name = if func_is(&f.name, "json_build_object") {
                    Some("icegres_catalog_json_object")
                } else if func_is(&f.name, "current_setting") {
                    Some("icegres_catalog_setting")
                } else {
                    None
                };
                if let Some(internal_name) = internal_name {
                    f.name = ObjectName(vec![
                        datafusion::sql::sqlparser::ast::ObjectNamePart::Identifier(
                            datafusion::sql::sqlparser::ast::Ident::new(internal_name),
                        ),
                    ]);
                    self.changed = true;
                }
                // Rule 3a: unnest(<text vector col>).
                if func_is(&f.name, "unnest") {
                    if let Some(arr) = first_arg(&f.args).and_then(vector_to_array_sql) {
                        if let Some(new_expr) = parse_expr_snippet(&format!("unnest({arr})")) {
                            *expr = new_expr;
                            self.changed = true;
                        }
                    }
                } else if func_is(&f.name, "generate_subscripts") {
                    // Rule 3b: generate_subscripts(<text vector col>, dim) —
                    // emulated ordinality; DataFusion zips parallel unnests
                    // in lockstep, so this pairs correctly with rule 3a.
                    // The dim argument (always 1 for these one-dimensional
                    // vectors) is kept in the expression as `1 + 0 * dim`:
                    // clients on the extended protocol pass it as a
                    // placeholder ($n), and dropping it would misalign every
                    // later parameter of the statement.
                    if let Some(arr) = first_arg(&f.args).and_then(vector_to_array_sql) {
                        let dim = second_arg(&f.args)
                            .map(|d| d.to_string())
                            .unwrap_or_else(|| "1".to_string());
                        if let Some(new_expr) = parse_expr_snippet(&format!(
                            "unnest(generate_series(1 + 0 * ({dim}), array_length({arr}, 1)))"
                        )) {
                            *expr = new_expr;
                            self.changed = true;
                        }
                    }
                }
            }
            Expr::Subquery(q) => self.wrap_scalar_subquery(q),
            _ => {}
        }
        ControlFlow::Continue(())
    }

    fn post_visit_expr(&mut self, expr: &mut Expr) -> ControlFlow<Self::Break> {
        // The upstream unsupported-type rule visits BEFORE children. Replacing
        // an outer OID cast with its inner REGCLASS cast skips the new root,
        // leaving nested SQLAlchemy identity expressions unplannable. Finish
        // its existing catalog-only normalization in post-order; retain all
        // operands and placeholders. User-table queries never enter this hook.
        if let Expr::Cast {
            expr: inner,
            data_type,
            ..
        } = expr
        {
            if matches!(
                data_type.to_string().to_ascii_lowercase().as_str(),
                "regclass" | "pg_catalog.regclass" | "oid" | "pg_catalog.oid"
            ) {
                *expr = *inner.clone();
                self.changed = true;
            }
        }
        ControlFlow::Continue(())
    }
}

/// Immutable pre-check: does this statement reference ONLY `pg_catalog`
/// tables (and at least one)? Statements touching user tables are never
/// intercepted, keeping transaction-pinned reads on their normal path.
struct PgCatalogOnly {
    seen: usize,
    all_pg_catalog: bool,
}

impl Visitor for PgCatalogOnly {
    type Break = ();

    fn pre_visit_relation(&mut self, relation: &ObjectName) -> ControlFlow<Self::Break> {
        self.seen += 1;
        let qualifier_ok = relation.0.len() >= 2
            && relation.0[relation.0.len() - 2]
                .as_ident()
                .is_some_and(|id| id.value.eq_ignore_ascii_case("pg_catalog"));
        if !qualifier_ok {
            // One non-pg_catalog relation is enough: eligibility is
            // `seen == 0 || all_pg_catalog`, and both terms are now settled
            // (seen > 0, all_pg_catalog = false), so walking the rest of the
            // AST cannot change the answer. Stop here.
            self.all_pg_catalog = false;
            return ControlFlow::Break(());
        }
        ControlFlow::Continue(())
    }
}

/// True if this SELECT is eligible for compat rewriting: it references
/// ONLY `pg_catalog` tables, or no tables at all (`select current_catalog`,
/// `select 1`...). Either way it reads no user data, so running a rewritten
/// copy on the shared (non-transaction-pinned) context is safe. Statements
/// touching user tables are never intercepted, keeping transaction-pinned
/// reads on their normal path.
fn is_rewrite_eligible_query(stmt: &Statement) -> bool {
    if !matches!(stmt, Statement::Query(_)) {
        return false;
    }
    let mut v = PgCatalogOnly {
        seen: 0,
        all_pg_catalog: true,
    };
    let _ = Visit::visit(stmt, &mut v);
    v.seen == 0 || v.all_pg_catalog
}

/// Rewrite `stmt` if it is a pg_catalog-only (or table-less) SELECT that any
/// rule changed.
fn rewrite(stmt: &Statement) -> Option<Statement> {
    if !is_rewrite_eligible_query(stmt) {
        return None;
    }
    let mut rewritten = stmt.clone();
    let mut visitor = CompatRewriter { changed: false };
    let _ = VisitMut::visit(&mut rewritten, &mut visitor);
    visitor.changed.then_some(rewritten)
}

// ---------------------------------------------------------------------------
// The hook
// ---------------------------------------------------------------------------

/// Strip inferred data types from every placeholder in the plan (subqueries
/// included).
///
/// Rationale: upstream datafusion-postgres deserializes extended-protocol
/// parameters against `ordered_param_types(..)`, which sorts the `$n` keys
/// LEXICOGRAPHICALLY — `$10` before `$2` — so any statement with ten or more
/// parameters (SQLAlchemy's index-introspection query has eleven) pairs
/// values with the wrong types and fails with 22P02 "invalid digit found in
/// string". With no inferred types every parameter deserializes as text
/// (order-insensitive), each value binds to its `$n` positionally, and
/// DataFusion's coercion handles the rest. Applied only to the pg_catalog
/// statements this hook intercepts.
fn strip_placeholder_types(plan: LogicalPlan) -> Result<LogicalPlan, DataFusionError> {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    use datafusion::logical_expr::Expr as DfExpr;
    plan.transform_down_with_subqueries(|node| {
        node.map_expressions(|expr| {
            expr.transform(|e| {
                if let DfExpr::Placeholder(p) = &e {
                    if p.field.is_some() {
                        let mut p = p.clone();
                        p.field = None;
                        return Ok(Transformed::yes(DfExpr::Placeholder(p)));
                    }
                }
                Ok(Transformed::no(e))
            })
        })
    })
    .map(|t| t.data)
}

/// The pinned pg_attribute provider exposes no defaults, identity columns or
/// nondefault collations (pg_attribute.rs:156-163). DataFusion cannot decorrelate
/// scalar subqueries nested in SQLAlchemy's CASE projections. After binding,
/// discard only branches whose exact, resolved catalog guard cannot be true.
/// A NULL-extended outer-join row cannot make these guards true either. Keep
/// placeholders through parsing/binding, and do not substitute catalog columns
/// generally: that would change NULL semantics in outer joins.
fn fold_catalog_metadata_cases(plan: LogicalPlan) -> Result<LogicalPlan, DataFusionError> {
    use datafusion::common::tree_node::{Transformed, TreeNode};
    use datafusion::logical_expr::expr_rewriter::NamePreserver;
    use datafusion::logical_expr::{Expr as DfExpr, ExprSchemable, Operator};

    fn attribute(expr: &DfExpr, name: &str) -> bool {
        matches!(expr, DfExpr::Column(column) if column.name == name &&
            column.relation.as_ref().is_some_and(|relation|
                relation.schema() == Some("pg_catalog") && relation.table() == "pg_attribute"))
    }
    fn impossible(expr: &DfExpr) -> bool {
        if attribute(expr, "atthasdef") {
            return true;
        }
        let DfExpr::BinaryExpr(binary) = expr else {
            return false;
        };
        if binary.op == Operator::And {
            return impossible(&binary.left) || impossible(&binary.right);
        }
        if binary.op != Operator::NotEq {
            return false;
        }
        fn literal(expr: &DfExpr) -> Option<ScalarValue> {
            match expr {
                DfExpr::Literal(value, _) => Some(value.clone()),
                DfExpr::Cast(cast) => literal(&cast.expr)?.cast_to(&cast.data_type).ok(),
                _ => None,
            }
        }
        let comparison = |column: &DfExpr, value: &DfExpr| {
            let Some(value) = literal(value) else {
                return false;
            };
            if attribute(column, "attidentity") {
                return matches!(&value, ScalarValue::Utf8(Some(v)) | ScalarValue::Utf8View(Some(v)) |
                    ScalarValue::LargeUtf8(Some(v)) if v.is_empty());
            }
            if attribute(column, "attcollation") {
                return matches!(
                    value,
                    ScalarValue::Int16(Some(0))
                        | ScalarValue::Int32(Some(0))
                        | ScalarValue::Int64(Some(0))
                ) || matches!(&value, ScalarValue::Utf8(Some(v)) | ScalarValue::Utf8View(Some(v)) |
                        ScalarValue::LargeUtf8(Some(v)) if v == "0");
            }
            false
        };
        comparison(&binary.left, &binary.right) || comparison(&binary.right, &binary.left)
    }

    plan.transform_up_with_subqueries(|node| {
        let LogicalPlan::Projection(projection) = &node else {
            return Ok(Transformed::no(node));
        };
        let schema = projection.input.schema().clone();
        node.map_expressions(|expr| {
            let saved_name = NamePreserver::new_for_projection().save(&expr);
            let mut transformed = expr.transform_up(|expr| {
                if let DfExpr::Case(case) = &expr {
                    if case.expr.is_none()
                        && case
                            .when_then_expr
                            .iter()
                            .all(|(guard, _)| impossible(guard))
                    {
                        let data_type = expr.get_type(&schema)?;
                        let replacement = match &case.else_expr {
                            Some(otherwise) if !matches!(otherwise.as_ref(), DfExpr::Literal(value, _) if value.is_null()) => (*otherwise.clone()).cast_to(&data_type, &schema)?,
                            Some(_) | None => DfExpr::Literal(
                                ScalarValue::try_new_null(&data_type)?,
                                None,
                            ),
                        };
                        return Ok(Transformed::yes(replacement));
                    }
                }
                Ok(Transformed::no(expr))
            })?;
            transformed.data = saved_name.restore(transformed.data);
            Ok(transformed)
        })
    })
    .map(|transformed| transformed.data)
}

/// Query hook applying the pg_catalog compatibility rewrites. Must run
/// BEFORE [`crate::txn::TxnHook`]: ORMs reflect inside a driver-opened
/// transaction, and TxnHook would otherwise route the un-rewritten SQL into
/// the transaction view where it fails identically.
pub struct CompatHook;

impl CompatHook {
    /// Execute a rewritten statement on the shared context and encode it
    /// eagerly (same rationale as `TxnHook::txn_select`: errors must surface
    /// before the hook returns, not mid-stream).
    async fn run(
        &self,
        rewritten: &Statement,
        ctx: &SessionContext,
        params: Option<&ParamValues>,
        client: &(dyn ClientInfo + Send + Sync),
    ) -> PgWireResult<Response> {
        let df_stmt = datafusion::sql::parser::Statement::Statement(Box::new(rewritten.clone()));
        let mut plan = ctx
            .state()
            .statement_to_plan(df_stmt)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        if let Some(p) = params {
            // Values arrive as text (see strip_placeholder_types); drop the
            // inferred placeholder types so substitution never type-checks
            // a text value against a mis-paired inferred type.
            plan = strip_placeholder_types(plan)
                .and_then(|plan| plan.replace_params_with_values(p))
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        }
        plan = fold_catalog_metadata_cases(plan).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        let df = ctx
            .execute_logical_plan(plan)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        let arrow_schema = Arc::new(df.schema().as_arrow().clone());
        let mut batches = df
            .collect()
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        if batches.is_empty() {
            batches.push(RecordBatch::new_empty(arrow_schema));
        }
        let mem_df = ctx
            .read_batches(batches)
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        let format_options = Arc::new(FormatOptions::from_client_metadata(client.metadata()));
        let resp =
            pgdf::encode_dataframe(mem_df, &Format::UnifiedText, Some(format_options)).await?;
        Ok(Response::Query(resp))
    }
}

#[async_trait]
impl QueryHook for CompatHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        let rewritten = rewrite(statement)?;
        tracing::debug!(sql = %rewritten, "compat: rewrote pg_catalog query (simple)");
        Some(self.run(&rewritten, session_context, None, client).await)
    }

    async fn handle_extended_parse_query(
        &self,
        sql: &Statement,
        session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        let rewritten = rewrite(sql)?;
        tracing::debug!(sql = %rewritten, "compat: rewrote pg_catalog query (extended parse)");
        let df_stmt = datafusion::sql::parser::Statement::Statement(Box::new(rewritten));
        Some(
            session_context
                .state()
                .statement_to_plan(df_stmt)
                .await
                .and_then(strip_placeholder_types)
                .map_err(|e| PgWireError::ApiError(Box::new(e))),
        )
    }

    async fn handle_extended_query(
        &self,
        statement: &Statement,
        _logical_plan: &LogicalPlan,
        params: &ParamValues,
        session_context: &SessionContext,
        client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        // Re-derive the rewrite (statements are cheap to clone-rewrite);
        // the plan passed in is the one this hook produced at parse time,
        // but planning from the statement again keeps this path independent
        // of hook-vs-default parse ordering.
        let rewritten = rewrite(statement)?;
        Some(
            self.run(&rewritten, session_context, Some(params), client)
                .await,
        )
    }
}

// ---------------------------------------------------------------------------
// Extended-protocol INSERT command tag (SPEC A9, pgjdbc executeUpdate)
// ---------------------------------------------------------------------------

/// Give plain autocommit INSERTs a proper `INSERT 0 <n>` command tag on the
/// extended query protocol.
///
/// Upstream datafusion-postgres executes the DataFusion DML plan and answers
/// with the plan's output — a one-row `count` ROWSET (RowDescription +
/// DataRow). The simple-protocol path converts that into a command tag, but
/// the extended path streams it as a result set, and JDBC's
/// `executeUpdate()` rejects any result set with "A result was returned when
/// none was expected". This hook executes the same plan and answers with
/// `Response::Execution(Tag INSERT 0 n)` instead.
///
/// Registration order (main.rs `query_hooks`) makes this a fall-through
/// handler: BufferHook (buffered ack), TxnHook (in-transaction RYOW +
/// PK-enforced/branched autocommit) and DmlHook all run first, so the only
/// INSERTs reaching this hook are plain autocommit appends on the main
/// branch — exactly the ones the upstream default path used to answer with a
/// rowset.
pub struct InsertTagHook;

impl InsertTagHook {
    async fn run(
        &self,
        stmt: &Statement,
        ctx: &SessionContext,
        params: &ParamValues,
    ) -> PgWireResult<Response> {
        let api_err = |e: DataFusionError| PgWireError::ApiError(Box::new(e));
        let df_stmt = datafusion::sql::parser::Statement::Statement(Box::new(stmt.clone()));
        let mut plan = ctx
            .state()
            .statement_to_plan(df_stmt)
            .await
            .map_err(api_err)?;
        let has_params = match params {
            ParamValues::List(l) => !l.is_empty(),
            ParamValues::Map(m) => !m.is_empty(),
        };
        if has_params {
            plan = plan.replace_params_with_values(params).map_err(api_err)?;
        }
        let df = ctx.execute_logical_plan(plan).await.map_err(api_err)?;
        let batches = df.collect().await.map_err(api_err)?;
        // A DataFusion DML plan yields a single UInt64 `count` column.
        let mut rows: u64 = 0;
        for batch in &batches {
            if let Some(col) = batch.columns().first() {
                if let Some(counts) = col
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::UInt64Array>()
                {
                    rows += counts.iter().flatten().sum::<u64>();
                }
            }
        }
        Ok(Response::Execution(
            Tag::new("INSERT").with_oid(0).with_rows(rows as usize),
        ))
    }
}

/// Build the empty-schema stand-in plan for an INSERT (see
/// `InsertTagHook::handle_extended_parse_query`): an `EmptyRelation`, wrapped
/// in a `Filter` carrying every `$n` placeholder of the real plan (with its
/// inferred type) inside a boolean no-op predicate, so
/// `get_parameter_types()` still reports them. Falls back to the real plan
/// (upstream describe behavior) if the wrapper cannot be built.
fn describe_shaped_insert_plan(real: &LogicalPlan) -> LogicalPlan {
    use datafusion::common::DFSchema;
    use datafusion::logical_expr::expr::Placeholder;
    use datafusion::logical_expr::{EmptyRelation, Expr as DfExpr, Filter as DfFilter};

    let empty = LogicalPlan::EmptyRelation(EmptyRelation {
        produce_one_row: false,
        schema: Arc::new(DFSchema::empty()),
    });
    let Ok(param_types) = real.get_parameter_types() else {
        return real.clone();
    };
    if param_types.is_empty() {
        return empty;
    }
    let mut params: Vec<_> = param_types.into_iter().collect();
    params.sort_by(|a, b| a.0.cmp(&b.0));
    let mut predicate: Option<DfExpr> = None;
    for (id, data_type) in params {
        let ph = DfExpr::Placeholder(Placeholder {
            id,
            field: data_type
                .map(|dt| Arc::new(datafusion::arrow::datatypes::Field::new("", dt, true))),
        });
        let leaf = DfExpr::IsNull(Box::new(ph));
        predicate = Some(match predicate {
            Some(p) => p.and(leaf),
            None => leaf,
        });
    }
    match DfFilter::try_new(
        predicate.expect("params is non-empty"),
        Arc::new(empty.clone()),
    ) {
        Ok(filter) => LogicalPlan::Filter(filter),
        // Extremely defensive: if the wrapper does not type-check, fall back
        // to upstream behavior (correct Bind types, JDBC-unfriendly
        // describe) rather than break the statement.
        Err(_) => real.clone(),
    }
}

#[async_trait]
impl QueryHook for InsertTagHook {
    async fn handle_simple_query(
        &self,
        _statement: &Statement,
        _session_context: &SessionContext,
        _client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        // The upstream simple-query path already answers `INSERT 0 n`.
        None
    }

    async fn handle_extended_parse_query(
        &self,
        sql: &Statement,
        session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        if !matches!(sql, Statement::Insert(_)) {
            return None;
        }
        // Plan the real INSERT (so the $n placeholder types are inferred
        // exactly like the default path would), then hand the framework a
        // DESCRIBE-shaped stand-in: same placeholders, EMPTY output schema.
        // The framework derives two things from this plan — the parameter
        // types for Bind (must match the real INSERT) and the portal's
        // result schema for Describe (must be empty so pgwire answers
        // NoData: the real DML plan's one-column `count` schema makes
        // Describe(portal) promise a RowDescription, and JDBC's
        // executeUpdate() rejects any statement that describes as returning
        // rows before Execute even runs). Execution never touches this
        // stand-in — handle_extended_query re-plans from the statement.
        let df_stmt = datafusion::sql::parser::Statement::Statement(Box::new(sql.clone()));
        let real = match session_context.state().statement_to_plan(df_stmt).await {
            Ok(plan) => plan,
            Err(e) => return Some(Err(PgWireError::ApiError(Box::new(e)))),
        };
        Some(Ok(describe_shaped_insert_plan(&real)))
    }

    async fn handle_extended_query(
        &self,
        statement: &Statement,
        _logical_plan: &LogicalPlan,
        params: &ParamValues,
        session_context: &SessionContext,
        _client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        if !matches!(statement, Statement::Insert(_)) {
            return None;
        }
        Some(self.run(statement, session_context, params).await)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    fn parse(sql: &str) -> Statement {
        Parser::new(&PostgreSqlDialect {})
            .try_with_sql(sql)
            .unwrap()
            .parse_statement()
            .unwrap()
    }

    #[test]
    fn rewrites_unnest_of_indkey() {
        let stmt = parse("SELECT unnest(pg_catalog.pg_index.indkey) FROM pg_catalog.pg_index");
        let out = rewrite(&stmt).expect("should rewrite").to_string();
        assert!(
            out.contains("unnest(string_to_array(pg_catalog.pg_index.indkey, ' '))"),
            "got: {out}"
        );
    }

    #[test]
    fn rewrites_generate_subscripts_to_ordinality_emulation() {
        let stmt = parse(
            "SELECT generate_subscripts(pg_catalog.pg_index.indkey, 1) FROM pg_catalog.pg_index",
        );
        let out = rewrite(&stmt).expect("should rewrite").to_string();
        assert!(
            out.contains("unnest(generate_series(1 + 0 * (1), array_length(string_to_array("),
            "got: {out}"
        );
    }

    #[test]
    fn rewrites_conkey_with_btrim() {
        let stmt =
            parse("SELECT unnest(pg_catalog.pg_constraint.conkey) FROM pg_catalog.pg_constraint");
        let out = rewrite(&stmt).expect("should rewrite").to_string();
        assert!(out.contains("btrim("), "got: {out}");
        assert!(out.contains("nullif("), "got: {out}");
    }

    #[test]
    fn wraps_nested_correlated_subquery_in_max() {
        let stmt = parse(
            "SELECT CASE WHEN (SELECT pg_catalog.pg_type.typcollation FROM pg_catalog.pg_type \
             WHERE pg_catalog.pg_type.oid = pg_catalog.pg_attribute.atttypid) != 0 THEN 1 END \
             FROM pg_catalog.pg_attribute",
        );
        let out = rewrite(&stmt).expect("should rewrite").to_string();
        assert!(
            out.contains("SELECT max(pg_catalog.pg_type.typcollation)"),
            "got: {out}"
        );
    }

    #[test]
    fn aliases_missing_reloptions_as_null() {
        let stmt = parse("SELECT pg_catalog.pg_class.reloptions FROM pg_catalog.pg_class");
        let out = rewrite(&stmt).expect("should rewrite").to_string();
        assert!(
            out.contains("CAST(NULL AS TEXT) AS reloptions"),
            "got: {out}"
        );
    }

    #[test]
    fn leaves_user_table_queries_alone() {
        for sql in [
            "SELECT * FROM demo.trips",
            "SELECT unnest(x) FROM demo.trips",
            // mixed pg_catalog + user table: not intercepted
            "SELECT t.city FROM demo.trips t JOIN pg_catalog.pg_class c ON true",
            // pg_catalog-only but nothing to rewrite
            "SELECT relname FROM pg_catalog.pg_class",
            "INSERT INTO demo.trips (trip_id) VALUES (1)",
        ] {
            assert!(rewrite(&parse(sql)).is_none(), "must not rewrite: {sql}");
        }
    }

    #[test]
    fn already_aggregated_subquery_untouched() {
        let stmt = parse(
            "SELECT CASE WHEN (SELECT max(pg_catalog.pg_type.typcollation) FROM \
             pg_catalog.pg_type WHERE pg_catalog.pg_type.oid = 1) != 0 THEN 1 END \
             FROM pg_catalog.pg_attribute",
        );
        assert!(rewrite(&stmt).is_none());
    }

    #[test]
    fn rewrites_keyword_identifiers_without_tables() {
        // pgjdbc Connection.getCatalog()
        let out = rewrite(&parse("select current_catalog"))
            .expect("should rewrite")
            .to_string();
        assert!(
            out.to_lowercase().contains("current_database()"),
            "got: {out}"
        );
        let out = rewrite(&parse("SELECT CURRENT_SCHEMA"))
            .expect("should rewrite")
            .to_string();
        assert!(
            out.to_lowercase().contains("current_schema()"),
            "got: {out}"
        );
        // current_role / current_user are session-scoped (see rule 7 note):
        // deliberately NOT intercepted.
        assert!(rewrite(&parse("select current_user")).is_none());
        assert!(rewrite(&parse("select current_role")).is_none());
    }

    #[test]
    fn keyword_identifiers_rewritten_inside_pg_catalog_queries_only() {
        // pg_catalog-only query: still rewritten.
        let out = rewrite(&parse(
            "SELECT current_catalog, relname FROM pg_catalog.pg_class",
        ))
        .expect("should rewrite")
        .to_string();
        assert!(
            out.to_lowercase().contains("current_database()"),
            "got: {out}"
        );
        // User-table query: untouched (normal read path, txn pinning intact).
        assert!(rewrite(&parse("SELECT current_catalog FROM demo.trips")).is_none());
    }

    #[test]
    fn table_less_select_without_keywords_untouched() {
        assert!(rewrite(&parse("select 1")).is_none());
        assert!(rewrite(&parse("select version()")).is_none());
    }

    #[test]
    fn version_string_is_postgres_shaped() {
        let v = version_string();
        assert!(v.starts_with("PostgreSQL 16.6 "), "got: {v}");
    }

    #[test]
    fn like_escape_translation_preserves_literals_and_rejects_invalid_patterns() {
        for (pattern, escape, expected) in [
            ("pg/_%", "/", "pg\\_%"),
            ("a//b", "/", "a/b"),
            ("/a", "/", "a"),
            (r"a\b/%/_", "/", r"a\\b\%\_"),
            (r"a\b_%", "", r"a\\b_%"),
            ("a💠_b💠%", "💠", r"a\_b\%"),
        ] {
            assert_eq!(normalize_like_pattern(pattern, escape).unwrap(), expected);
        }
        assert!(normalize_like_pattern("ends/", "/").is_err());
        assert!(normalize_like_pattern("a", "ab").is_err());
        assert!(normalize_like_pattern("a", "💠x").is_err());
        assert!(rewrite(&parse("SELECT city LIKE 'x/%' ESCAPE '/' FROM demo.trips")).is_none());
        assert!(rewrite(&parse("SELECT 'CAST(x AS REGCLASS)' AS untouched")).is_none());
        assert!(rewrite(&parse(
            "SELECT json_build_object('k', city) FROM demo.trips"
        ))
        .is_none());
    }

    async fn catalog_test_context() -> SessionContext {
        use datafusion::catalog::MemorySchemaProvider;
        use datafusion_postgres::auth::AuthManager;
        use datafusion_postgres::datafusion_pg_catalog::pg_catalog::setup_pg_catalog;

        let ctx = SessionContext::new();
        let demo = Arc::new(MemorySchemaProvider::new());
        let schema = Arc::new(Schema::new(vec![
            Field::new("trip_id", DataType::Int64, false),
            Field::new("city", DataType::Utf8, true),
        ]));
        demo.register_table(
            "trips".into(),
            Arc::new(
                MemTable::try_new(schema.clone(), vec![vec![RecordBatch::new_empty(schema)]])
                    .unwrap(),
            ),
        )
        .unwrap();
        ctx.catalog("datafusion")
            .unwrap()
            .register_schema("demo", demo)
            .unwrap();
        setup_pg_catalog(&ctx, "datafusion", Arc::new(AuthManager::default())).unwrap();
        register_compat_udfs(&ctx);
        install_coherent_pg_catalog(&ctx, "datafusion")
            .await
            .unwrap();
        ctx
    }

    async fn run_catalog_query(
        ctx: &SessionContext,
        sql: &str,
        params: Option<ParamValues>,
    ) -> Vec<RecordBatch> {
        use datafusion_postgres::datafusion_pg_catalog::sql::PostgresCompatibilityParser;
        // Use the same upstream parser/rewrite ordering as the wire server.
        let statement = PostgresCompatibilityParser::new()
            .parse(sql)
            .unwrap()
            .remove(0);
        let rewritten = rewrite(&statement).expect("compatibility rewrite must intercept");
        let mut plan = ctx
            .state()
            .statement_to_plan(datafusion::sql::parser::Statement::Statement(Box::new(
                rewritten,
            )))
            .await
            .unwrap();
        if let Some(params) = params {
            if let ParamValues::List(values) = &params {
                assert_eq!(
                    plan.get_parameter_types().unwrap().len(),
                    values.len(),
                    "every parameter must survive metadata rewriting"
                );
            }
            plan = strip_placeholder_types(plan)
                .unwrap()
                .replace_params_with_values(&params)
                .unwrap();
        }
        let plan = fold_catalog_metadata_cases(plan).unwrap();
        let df = ctx.execute_logical_plan(plan).await.unwrap();
        let optimized = df.clone().into_optimized_plan().unwrap();
        df.collect()
            .await
            .unwrap_or_else(|error| panic!("{error}\n{}", optimized.display_indent()))
    }

    #[tokio::test]
    async fn sqlalchemy_schema_escape_executes_with_bound_pattern() {
        let ctx = catalog_test_context().await;
        let batches = run_catalog_query(
            &ctx,
            "SELECT pg_catalog.pg_namespace.nspname FROM pg_catalog.pg_namespace \
             WHERE pg_catalog.pg_namespace.nspname NOT LIKE $1 || '%' ESCAPE '/' \
             ORDER BY pg_catalog.pg_namespace.nspname",
            Some(ParamValues::List(vec![ScalarValue::Utf8(Some(
                "pg/_".into(),
            ))
            .into()])),
        )
        .await;
        let names = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .flatten()
            })
            .collect::<Vec<_>>();
        assert!(names.contains(&"demo"));
        assert!(!names.iter().any(|name| name.starts_with("pg_")));
        let batches = run_catalog_query(
            &ctx,
            r"SELECT 'a\b_%' LIKE 'a\b/_/%' ESCAPE '/',
                      'PG_Name' ILIKE 'pg/_%' ESCAPE '/',
                      NULL LIKE 'pg/_%' ESCAPE '/',
                      'a\b' LIKE 'a\b' ESCAPE ''",
            None,
        )
        .await;
        use datafusion::arrow::array::BooleanArray;
        for index in [0, 1, 3] {
            assert!(batches[0]
                .column(index)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .value(0));
        }
        assert!(batches[0].column(2).is_null(0));
    }

    #[tokio::test]
    async fn sqlalchemy_columns_reflection_executes_simple_and_bound_queries() {
        // Captured from SQLAlchemy's PostgreSQL 16 reflection compiler, with
        // nested identity/default/collation CASE expressions and 21 bindings.
        let fixture: serde_json::Value =
            serde_json::from_str(include_str!("../tests/fixtures/sqlalchemy2_columns.json"))
                .unwrap();
        let ctx = catalog_test_context().await;
        // Verify the provider invariant used by the post-bind CASE rewrite,
        // rather than assuming similarly named columns are constant.
        let metadata = ctx
            .sql(
                "SELECT atthasdef, attidentity, attcollation \
            FROM pg_catalog.pg_attribute",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert!(metadata.iter().map(RecordBatch::num_rows).sum::<usize>() > 0);
        for batch in metadata {
            use datafusion::arrow::array::BooleanArray;
            assert!(batch
                .column(0)
                .as_any()
                .downcast_ref::<BooleanArray>()
                .unwrap()
                .iter()
                .all(|value| value == Some(false)));
            assert!(batch
                .column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .iter()
                .all(|value| value == Some("")));
            assert!(batch
                .column(2)
                .as_any()
                .downcast_ref::<Int32Array>()
                .unwrap()
                .iter()
                .all(|value| value == Some(0)));
        }
        let params = fixture["params"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| {
                // pgwire's compat hook deliberately decodes all placeholders as
                // text, preventing upstream's $10-before-$2 type ordering bug.
                ScalarValue::Utf8(Some(
                    v.as_str()
                        .map(str::to_owned)
                        .unwrap_or_else(|| v.to_string()),
                ))
                .into()
            })
            .collect::<Vec<_>>();
        for (sql, params) in [
            (fixture["literal_sql"].as_str().unwrap(), None),
            (
                fixture["sql"].as_str().unwrap(),
                Some(ParamValues::List(params)),
            ),
        ] {
            let batches = run_catalog_query(&ctx, sql, params).await;
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 2);
            let names = batches
                .iter()
                .flat_map(|batch| {
                    batch
                        .column(0)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .unwrap()
                        .iter()
                        .flatten()
                })
                .collect::<Vec<_>>();
            assert_eq!(names, ["trip_id", "city"]);
            for batch in batches {
                for column in ["default", "identity_options", "collation"] {
                    let col = batch.column_by_name(column).unwrap();
                    assert_eq!(col.null_count(), batch.num_rows(), "{column}");
                }
            }
        }
    }

    #[tokio::test]
    async fn metadata_case_folding_uses_resolved_catalog_fields_and_preserves_nulls() {
        let ctx = catalog_test_context().await;
        for sql in [
            "SELECT CASE WHEN a.atthasdef THEN 'yes' ELSE 'no' END FROM pg_catalog.pg_attribute AS a",
            "SELECT CASE WHEN pg_attribute.atthasdef THEN 'yes' ELSE 'no' END \
             FROM (SELECT true AS atthasdef) AS pg_attribute",
            "SELECT CASE WHEN pg_catalog.pg_attribute.attidentity IS NULL THEN 'null' ELSE 'value' END \
             FROM pg_catalog.pg_attribute",
        ] {
            let plan = ctx.sql(sql).await.unwrap().logical_plan().clone();
            assert_eq!(fold_catalog_metadata_cases(plan.clone()).unwrap(), plan, "{sql}");
        }
        let plan = ctx
            .sql(
                "SELECT CASE WHEN pg_catalog.pg_attribute.atthasdef THEN 1 ELSE NULL END AS d \
            FROM (VALUES (1)) AS seed(n) LEFT JOIN pg_catalog.pg_attribute ON false",
            )
            .await
            .unwrap()
            .logical_plan()
            .clone();
        let folded = fold_catalog_metadata_cases(plan.clone()).unwrap();
        assert_eq!(folded.schema(), plan.schema());
        let original = ctx
            .execute_logical_plan(plan)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let rewritten = ctx
            .execute_logical_plan(folded)
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(original, rewritten);
        assert_eq!(rewritten[0].column(0).null_count(), 1);
        let plan = ctx
            .sql(
                "SELECT CASE WHEN pg_catalog.pg_attribute.atthasdef \
            THEN CAST(1 AS BIGINT) ELSE CAST(2 AS SMALLINT) END AS typed \
            FROM pg_catalog.pg_attribute",
            )
            .await
            .unwrap()
            .logical_plan()
            .clone();
        let folded = fold_catalog_metadata_cases(plan.clone()).unwrap();
        assert_eq!(folded.schema(), plan.schema());
        assert_eq!(
            ctx.execute_logical_plan(plan)
                .await
                .unwrap()
                .collect()
                .await
                .unwrap(),
            ctx.execute_logical_plan(folded)
                .await
                .unwrap()
                .collect()
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn catalog_json_preserves_value_types_and_bound_key_order() {
        let ctx = catalog_test_context().await;
        let batches = run_catalog_query(
            &ctx,
            "SELECT json_build_object($1, 7, $2, true, $3, NULL, $4, 'a\"b')",
            Some(ParamValues::List(
                ["number", "boolean", "null", "string"]
                    .into_iter()
                    .map(|s| ScalarValue::Utf8(Some(s.into())).into())
                    .collect(),
            )),
        )
        .await;
        let value = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0);
        let object: serde_json::Value = serde_json::from_str(value).unwrap();
        assert_eq!(
            object,
            serde_json::json!({"number": 7, "boolean": true, "null": null, "string": "a\"b"})
        );
        let batches = run_catalog_query(
            &ctx,
            "SELECT json_build_object('number', n, 'null', NULL) \
             FROM (VALUES (1), (2)) AS values_table(n)",
            None,
        )
        .await;
        let objects = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .iter()
                    .flatten()
            })
            .map(|s| serde_json::from_str::<serde_json::Value>(s).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            objects,
            vec![
                serde_json::json!({"number": 1, "null": null}),
                serde_json::json!({"number": 2, "null": null})
            ]
        );
    }

    #[tokio::test]
    async fn sqlalchemy_table_options_reflection_executes_simple_and_bound_queries() {
        let fixture: serde_json::Value = serde_json::from_str(include_str!(
            "../tests/fixtures/sqlalchemy2_table_options.json"
        ))
        .unwrap();
        let ctx = catalog_test_context().await;
        let params = fixture["params"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| ScalarValue::Utf8(Some(v.as_str().unwrap().to_owned())).into())
            .collect::<Vec<_>>();
        for (sql, params) in [
            (fixture["literal_sql"].as_str().unwrap(), None),
            (
                fixture["sql"].as_str().unwrap(),
                Some(ParamValues::List(params)),
            ),
        ] {
            let batches = run_catalog_query(&ctx, sql, params).await;
            assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
            let batch = &batches[0];
            assert_eq!(
                batch
                    .column_by_name("relname")
                    .unwrap()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(0),
                "trips"
            );
            for column in [
                "reloptions",
                "relhasoids",
                "access_method_name",
                "tablespace_name",
                "parent_table_names",
            ] {
                assert!(
                    ScalarValue::try_from_array(batch.column_by_name(column).unwrap(), 0)
                        .unwrap()
                        .is_null(),
                    "{column}"
                );
            }
        }
    }

    #[tokio::test]
    async fn catalog_setting_keeps_session_settings_out_of_shared_emulation() {
        let ctx = catalog_test_context().await;
        let batches = run_catalog_query(
            &ctx,
            "SELECT current_setting($1)",
            Some(ParamValues::List(vec![ScalarValue::Utf8(Some(
                "default_table_access_method".into(),
            ))
            .into()])),
        )
        .await;
        assert_eq!(
            batches[0]
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "heap"
        );
        let null = run_catalog_query(&ctx, "SELECT current_setting(NULL)", None).await;
        assert!(null[0].column(0).is_null(0));
        let unknown = ctx
            .sql("SELECT icegres_catalog_setting('search_path')")
            .await
            .unwrap()
            .collect()
            .await;
        assert!(unknown.is_err());
        assert!(rewrite(&parse(
            "SELECT current_setting('default_table_access_method') FROM demo.trips"
        ))
        .is_none());
        assert!(rewrite(&parse("SELECT 'current_setting(''search_path'')'")).is_none());
    }

    // -- pg_type oid patch helpers ------------------------------------------

    use datafusion::arrow::array::Int32Array;
    use datafusion::arrow::datatypes::{Field, Schema};

    fn ns_batch(names: &[&str], oids: &[i32]) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int32, false),
            Field::new("nspname", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(oids.to_vec())),
                Arc::new(StringArray::from(names.to_vec())),
            ],
        )
        .unwrap()
    }

    #[test]
    fn namespace_oid_found_via_cast() {
        // oid stored as Int32 upstream; the helper reads it type-agnostically.
        let oid =
            pg_catalog_namespace_oid(&[ns_batch(&["demo", "pg_catalog"], &[101, 202])]).unwrap();
        assert_eq!(oid, 202);
    }

    #[test]
    fn namespace_oid_missing_pg_catalog_errors() {
        assert!(pg_catalog_namespace_oid(&[ns_batch(&["demo"], &[101])]).is_err());
    }

    #[test]
    fn namespace_oid_reads_large_utf8_names() {
        // The helper's cast-based reads must absorb alternate string/int
        // widths, not just the Int32/Utf8 the pinned emulation uses today.
        use datafusion::arrow::array::LargeStringArray;
        let schema = Arc::new(Schema::new(vec![
            Field::new("oid", DataType::Int64, false),
            Field::new("nspname", DataType::LargeUtf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![7, 42])),
                Arc::new(LargeStringArray::from(vec!["demo", "pg_catalog"])),
            ],
        )
        .unwrap();
        assert_eq!(pg_catalog_namespace_oid(&[batch]).unwrap(), 42);
    }

    #[test]
    fn patch_rewrites_typnamespace_and_preserves_type() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("typname", DataType::Utf8, false),
            Field::new("typnamespace", DataType::Int32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from(vec!["int8", "text"])),
                Arc::new(Int32Array::from(vec![11, 13283])),
            ],
        )
        .unwrap();
        let patched = patch_typnamespace(vec![batch], 202).unwrap();
        let col = patched[0].column(1);
        // Type preserved (Int32, not the helper's Int64 working type)...
        assert_eq!(col.data_type(), &DataType::Int32);
        // ...every row rewritten to the coherent namespace oid...
        let col = col.as_any().downcast_ref::<Int32Array>().unwrap();
        assert_eq!(col.values(), &[202, 202]);
        // ...and other columns untouched.
        let names = patched[0]
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(names.value(0), "int8");
    }

    #[test]
    fn patch_passes_through_batches_without_typnamespace() {
        let schema = Arc::new(Schema::new(vec![Field::new(
            "unrelated",
            DataType::Int32,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(Int32Array::from(vec![7]))]).unwrap();
        let out = patch_typnamespace(vec![batch.clone()], 202).unwrap();
        assert_eq!(out[0], batch);
    }
}
