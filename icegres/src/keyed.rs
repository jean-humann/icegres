//! Keyed PK upserts on the durable tail — roadmap Phase 2
//! (docs/sota-roadmap.md §4) helpers.
//!
//! A table opts in with `icegres.tail-upsert = "true"` **and** a declared
//! `icegres.primary-key`, on a server running buffered writes
//! (`--write-buffer-ms > 0`) with a durable tail (`--tail-dir`/`--tail-url`).
//! On such a table, an autocommit `UPDATE ... WHERE <exact PK equality>`
//! (SET values literal) or `DELETE ... WHERE <exact PK equality>` skips the
//! synchronous COW commit: the current row is resolved through the same
//! union view a scan sees, the replacement (or deletion) is fsync'd to the
//! tail as ONE keyed frame, and the statement acks — the flusher later
//! coalesces every keyed op of the window into ONE copy-on-write commit.
//! Everything that does not fit this shape falls back to the unchanged
//! fence-then-synchronous path (never rejected here).
//!
//! This module holds the statement-shape detection, the canonical key
//! encoding (arrow-row over the PK columns — one code path for single and
//! composite keys), the SQL-literal rendering the flusher's coalesced
//! `DELETE ... WHERE <keys>` op needs, and [`KeySuppressExec`] — the
//! execution-plan wrapper that hides committed rows whose key was updated or
//! deleted in the buffer window (the keyed analogue of the union overlay;
//! see `buffer.rs` for the layered overlay semantics and `cache.rs` for the
//! plan shape).

use std::any::Any;
use std::collections::HashSet;
use std::sync::Arc;

use anyhow::{anyhow, bail, Result};
use arrow::array::{ArrayRef, BooleanArray, RecordBatch, StringArray};
use arrow::compute::{cast_with_options, filter_record_batch, CastOptions};
use arrow::datatypes::{DataType, Schema as ArrowSchema, SchemaRef as ArrowSchemaRef};
use arrow::row::{RowConverter, SortField};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::{SendableRecordBatchStream, TaskContext};
use datafusion::physical_expr::EquivalenceProperties;
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, Partitioning, PlanProperties,
};
use datafusion::sql::sqlparser::ast::{
    self, BinaryOperator, Delete, Expr, Statement, UnaryOperator, Value,
};
use futures::StreamExt;
use iceberg::TableIdent;

use crate::overwrite::{quote_ident, DmlStatement};

/// Table property opting a table into keyed tail upserts (with a declared
/// `icegres.primary-key`, a running write buffer, and a durable tail).
pub const TAIL_UPSERT_PROPERTY: &str = "icegres.tail-upsert";

/// Whether `raw` is a truthy property value (`true`, case-insensitive).
pub fn property_is_true(raw: Option<&String>) -> bool {
    raw.is_some_and(|v| v.trim().eq_ignore_ascii_case("true"))
}

/// PK column types the keyed path supports. Bounded by what
/// [`render_keys_predicate`] can render as an unambiguous SQL literal for
/// the flusher's coalesced delete; anything else falls back to the
/// synchronous path (documented in docs/limitations.md). Covers the Arrow
/// types the Iceberg primitives int/long/string/boolean/date map to.
pub fn key_type_supported(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Boolean
            | DataType::Date32
    )
}

// ---------------------------------------------------------------------------
// Canonical key encoding (arrow-row): one representation for single and
// composite keys, shared by the buffer's keyed map, overlay suppression,
// and the committed-scan filter.
// ---------------------------------------------------------------------------

/// Resolve `pk_cols` to column indices in `schema` (canonical names first,
/// case-insensitive fallback — same rule as `overwrite::project_columns`).
pub fn pk_indices(schema: &ArrowSchema, pk_cols: &[String]) -> Result<Vec<usize>> {
    pk_cols
        .iter()
        .map(|c| {
            schema
                .fields()
                .iter()
                .position(|f| f.name().eq_ignore_ascii_case(c))
                .ok_or_else(|| anyhow!("PK column {c:?} missing from batch schema"))
        })
        .collect()
}

/// Row-encode the PK columns of `batch` into one stable byte key per row.
/// The encoding is arrow-row over the columns' exact (canonical) types, so
/// keys derived from a scan batch, a buffered upsert row, and a casted
/// literal all compare equal exactly when the SQL values are equal.
pub fn encode_batch_keys(batch: &RecordBatch, pk_cols: &[String]) -> Result<Vec<Vec<u8>>> {
    let indices = pk_indices(batch.schema_ref(), pk_cols)?;
    let cols: Vec<ArrayRef> = indices.iter().map(|&i| batch.column(i).clone()).collect();
    encode_key_columns(&cols)
}

/// Row-encode already-extracted key columns.
pub fn encode_key_columns(cols: &[ArrayRef]) -> Result<Vec<Vec<u8>>> {
    let fields: Vec<SortField> = cols
        .iter()
        .map(|c| SortField::new(c.data_type().clone()))
        .collect();
    let converter =
        RowConverter::new(fields).map_err(|e| anyhow!("key row-converter failed: {e}"))?;
    let rows = converter
        .convert_columns(cols)
        .map_err(|e| anyhow!("key row-encoding failed: {e}"))?;
    Ok(rows.iter().map(|r| r.data().to_vec()).collect())
}

/// Drop every row of `batch` whose PK key is in `keys`. Returns the
/// (possibly empty) surviving batch.
pub fn suppress_batch(
    batch: &RecordBatch,
    pk_cols: &[String],
    keys: &HashSet<Vec<u8>>,
) -> Result<RecordBatch> {
    if keys.is_empty() || batch.num_rows() == 0 {
        return Ok(batch.clone());
    }
    let encoded = encode_batch_keys(batch, pk_cols)?;
    let mask = BooleanArray::from_iter(encoded.iter().map(|k| Some(!keys.contains(k))));
    filter_record_batch(batch, &mask).map_err(|e| anyhow!("key suppression filter failed: {e}"))
}

// ---------------------------------------------------------------------------
// Statement-shape detection: exact PK-equality UPDATE/DELETE with literals.
// ---------------------------------------------------------------------------

/// A literal scalar from the WHERE clause (rendered/cast later against the
/// canonical PK column types).
#[derive(Debug, Clone, PartialEq)]
pub enum ScalarLit {
    /// Unquoted SQL number (possibly negative), kept as its source text.
    Number(String),
    /// Single-quoted SQL string.
    Str(String),
    Bool(bool),
}

/// A WHERE-clause column reference: the normalized name plus whether the
/// source identifier was quoted. Quoted identifiers are case-sensitive in
/// Postgres, so keyed routing matches them against the PK columns only on
/// an EXACT-case match ([`key_col_matches`]) — `"ID" = 5` on a table whose
/// PK is `id` falls back to the synchronous path (which errors or resolves
/// authoritatively) instead of silently map-hitting the wrong column.
#[derive(Debug, Clone, PartialEq)]
pub struct KeyCol {
    pub name: String,
    pub quoted: bool,
}

/// Whether a WHERE-clause column reference binds PK column `pk`: quoted
/// identifiers must match exactly; unquoted ones match case-insensitively
/// (the same resolution rule as [`pk_indices`]).
pub fn key_col_matches(col: &KeyCol, pk: &str) -> bool {
    if col.quoted {
        col.name == pk
    } else {
        col.name.eq_ignore_ascii_case(pk)
    }
}

/// An UPDATE/DELETE that *may* route to the keyed tail: single plain table,
/// no rejected clauses (delegated to `dml::translate`'s checks), WHERE is an
/// AND-chain of `col = <literal>`, and (for UPDATE) every SET value is a
/// literal. Activation against the actual table (property, PK declaration,
/// PK types) happens later in `buffer.rs` — this is pure statement shape.
pub struct KeyedCandidate {
    pub dml: DmlStatement,
    pub tag: &'static str,
    pub ident: TableIdent,
    /// Normalized `(column, literal)` equality pairs, in WHERE order.
    pub eq: Vec<(KeyCol, ScalarLit)>,
    /// Normalized columns assigned by SET (empty for DELETE).
    pub assigned: Vec<String>,
}

/// The identifier-qualification scope of one statement: which qualifiers a
/// compound column reference (`t.id`, `demo.t.id`) may legally carry.
struct QualScope<'a> {
    namespace: &'a str,
    table: &'a str,
    alias: Option<&'a str>,
}

/// Parse `stmt` into a [`KeyedCandidate`]. `None` = not keyed-shaped (the
/// caller falls back to the fence-then-synchronous path; nothing is ever
/// rejected here).
pub fn parse_keyed_candidate(stmt: &Statement) -> Option<KeyedCandidate> {
    // Reuse the DML translator's scope checks (RETURNING/joins/subqueries/
    // ... all make it error → not a keyed candidate; DmlHook will produce
    // the real rejection on the fallback path).
    let (dml, tag) = crate::dml::translate(stmt).ok()??;
    let selection = match stmt {
        Statement::Update(ast::Update { selection, .. }) => selection.as_ref()?,
        Statement::Delete(Delete { selection, .. }) => selection.as_ref()?,
        _ => return None,
    };
    let scope = QualScope {
        namespace: &dml.namespace,
        table: &dml.table,
        alias: dml.alias.as_deref(),
    };
    let mut conjuncts = Vec::new();
    split_conjuncts(selection, &mut conjuncts);
    let mut eq = Vec::with_capacity(conjuncts.len());
    for c in conjuncts {
        eq.push(eq_pair(c, &scope)?);
    }
    if eq.is_empty() {
        return None;
    }
    let assigned = match stmt {
        Statement::Update(ast::Update { assignments, .. }) => {
            let mut cols = Vec::with_capacity(assignments.len());
            for a in assignments {
                if !is_literal_expr(&a.value) {
                    return None; // non-literal SET value: synchronous path
                }
                let ast::AssignmentTarget::ColumnName(name) = &a.target else {
                    return None;
                };
                let last = name.0.last()?;
                cols.push(normalize_ident(last.as_ident()?));
            }
            cols
        }
        _ => Vec::new(),
    };
    let ident = TableIdent::from_strs([dml.namespace.as_str(), dml.table.as_str()]).ok()?;
    Some(KeyedCandidate {
        dml,
        tag,
        ident,
        eq,
        assigned,
    })
}

/// Flatten an AND-chain (`a AND b AND c`, arbitrarily nested) into its
/// conjuncts. Parenthesized expressions unwrap.
fn split_conjuncts<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    match expr {
        Expr::BinaryOp {
            left,
            op: BinaryOperator::And,
            right,
        } => {
            split_conjuncts(left, out);
            split_conjuncts(right, out);
        }
        Expr::Nested(inner) => split_conjuncts(inner, out),
        other => out.push(other),
    }
}

/// `<column> = <literal>` (either order), else `None`.
fn eq_pair(expr: &Expr, scope: &QualScope<'_>) -> Option<(KeyCol, ScalarLit)> {
    let Expr::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    } = expr
    else {
        return None;
    };
    if let (Some(col), Some(lit)) = (column_ref(left, scope), scalar_lit(right)) {
        return Some((col, lit));
    }
    if let (Some(col), Some(lit)) = (column_ref(right, scope), scalar_lit(left)) {
        return Some((col, lit));
    }
    None
}

/// A plain (possibly qualified) column reference, with its quoted-ness.
/// Compound identifiers route ONLY when the qualifier names the statement's
/// own table — the alias when one is set (Postgres forbids the table name
/// once aliased), else the table (optionally namespace-qualified). A
/// foreign qualifier (`x.id` on `demo.t`) returns `None`, so the statement
/// falls back to the synchronous path, which resolves or errors
/// authoritatively instead of the keyed path silently map-hitting.
fn column_ref(expr: &Expr, scope: &QualScope<'_>) -> Option<KeyCol> {
    match expr {
        Expr::Identifier(id) => Some(KeyCol {
            name: normalize_ident(id),
            quoted: id.quote_style.is_some(),
        }),
        Expr::CompoundIdentifier(ids) => {
            let (quals, col) = ids.split_at(ids.len().checked_sub(1)?);
            let col = col.first()?;
            let qual_ok = match (scope.alias, quals) {
                (Some(alias), [q]) => normalize_ident(q) == alias,
                (None, [q]) => normalize_ident(q) == scope.table,
                (None, [ns, q]) => {
                    normalize_ident(ns) == scope.namespace && normalize_ident(q) == scope.table
                }
                _ => false,
            };
            if !qual_ok {
                return None;
            }
            Some(KeyCol {
                name: normalize_ident(col),
                quoted: col.quote_style.is_some(),
            })
        }
        Expr::Nested(inner) => column_ref(inner, scope),
        _ => None,
    }
}

/// Postgres identifier folding (same rule as dml.rs).
fn normalize_ident(ident: &ast::Ident) -> String {
    if ident.quote_style.is_some() {
        ident.value.clone()
    } else {
        ident.value.to_lowercase()
    }
}

/// A key-usable literal: number (possibly negated), string, or boolean.
fn scalar_lit(expr: &Expr) -> Option<ScalarLit> {
    match expr {
        Expr::Value(v) => match &v.value {
            Value::Number(n, _) => Some(ScalarLit::Number(n.clone())),
            Value::SingleQuotedString(s) => Some(ScalarLit::Str(s.clone())),
            Value::Boolean(b) => Some(ScalarLit::Bool(*b)),
            _ => None,
        },
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => match scalar_lit(expr)? {
            ScalarLit::Number(n) => Some(ScalarLit::Number(format!("-{n}"))),
            _ => None,
        },
        Expr::Nested(inner) => scalar_lit(inner),
        _ => None,
    }
}

/// Whether a SET value is a plain literal (incl. NULL and negated numbers).
fn is_literal_expr(expr: &Expr) -> bool {
    match expr {
        Expr::Value(_) => true,
        Expr::UnaryOp {
            op: UnaryOperator::Minus,
            expr,
        } => is_literal_expr(expr),
        Expr::Nested(inner) => is_literal_expr(inner),
        _ => false,
    }
}

// ---------------------------------------------------------------------------
// Literal <-> Arrow bridging.
// ---------------------------------------------------------------------------

/// Cast the candidate's WHERE literals onto the canonical PK column types,
/// producing a ONE-ROW batch in `pk_cols` order (schema = the canonical
/// fields, so keys encoded from it compare equal to keys encoded from scan
/// rows). Errors on any value the column type cannot represent — the caller
/// treats that as "not keyed" and falls back.
///
/// The literal→key derivation is deliberately TIGHTER than what arrow's
/// cast accepts, because the cast is more permissive than the engine's own
/// comparison coercion and a too-permissive key would map-hit rows the
/// synchronous path would never match (mis-execute, not just mis-route):
///
/// * only same-category pairings are attempted at all — numbers onto
///   integer columns, strings onto string/integer/date columns, booleans
///   onto boolean columns; everything else (e.g. `bool_pk = 1`,
///   `str_pk = 5`) errors → sync fallback;
/// * any pairing that needs a real cast must survive a ROUND-TRIP: the
///   cast value rendered back must equal the source text exactly, so
///   `int_pk = '05'` (casts to 5, which sync does not match) errors →
///   sync fallback, while `int_pk = '5'` routes.
pub fn literals_to_key_batch(
    eq: &[(KeyCol, ScalarLit)],
    pk_cols: &[String],
    canonical: &ArrowSchemaRef,
) -> Result<RecordBatch> {
    let indices = pk_indices(canonical, pk_cols)?;
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(pk_cols.len());
    let mut fields = Vec::with_capacity(pk_cols.len());
    for (pk, &idx) in pk_cols.iter().zip(&indices) {
        let field = canonical.field(idx);
        let lit = eq
            .iter()
            .find(|(c, _)| key_col_matches(c, pk))
            .map(|(_, l)| l)
            .ok_or_else(|| anyhow!("no literal for PK column {pk:?}"))?;
        type DT = DataType;
        let (raw, round_trip): (ArrayRef, Option<&str>) = match (lit, field.data_type()) {
            (ScalarLit::Bool(b), DT::Boolean) => (Arc::new(BooleanArray::from(vec![*b])), None),
            (ScalarLit::Number(s), DT::Int8 | DT::Int16 | DT::Int32 | DT::Int64) => {
                (Arc::new(StringArray::from(vec![s.as_str()])), Some(s))
            }
            (ScalarLit::Str(s), DT::Int8 | DT::Int16 | DT::Int32 | DT::Int64 | DT::Date32) => {
                (Arc::new(StringArray::from(vec![s.as_str()])), Some(s))
            }
            (ScalarLit::Str(s), DT::Utf8 | DT::LargeUtf8) => {
                (Arc::new(StringArray::from(vec![s.as_str()])), None)
            }
            (lit, other) => bail!(
                "key literal {lit:?} does not pair with PK column {pk:?} of type {other} \
                 (keyed routing is conservative; the synchronous path is authoritative)"
            ),
        };
        let col = if raw.data_type() == field.data_type() {
            raw
        } else {
            cast_with_options(
                &raw,
                field.data_type(),
                &CastOptions {
                    safe: false,
                    format_options: Default::default(),
                },
            )
            .map_err(|e| anyhow!("cannot cast key literal for {pk:?}: {e}"))?
        };
        if let Some(src) = round_trip {
            let rendered = arrow::util::display::array_value_to_string(&col, 0)
                .map_err(|e| anyhow!("cannot render cast key literal for {pk:?}: {e}"))?;
            anyhow::ensure!(
                rendered == src,
                "key literal {src:?} does not survive a cast round-trip onto {} \
                 (renders back as {rendered:?}); the synchronous path is authoritative",
                field.data_type()
            );
        }
        cols.push(col);
        fields.push(field.clone());
    }
    RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), cols)
        .map_err(|e| anyhow!("cannot build key batch: {e}"))
}

/// Project the PK columns of `batch` (one canonical-schema row set) into a
/// key-rows batch with a FRESH schema (fields cloned, no schema-level
/// metadata) — the same shape [`literals_to_key_batch`] builds, so key-row
/// batches from either source concatenate cleanly at flush time.
pub fn project_key_rows(batch: &RecordBatch, pk_cols: &[String]) -> Result<RecordBatch> {
    let indices = pk_indices(batch.schema_ref(), pk_cols)?;
    let fields: Vec<_> = indices
        .iter()
        .map(|&i| batch.schema_ref().field(i).clone())
        .collect();
    let cols: Vec<ArrayRef> = indices.iter().map(|&i| batch.column(i).clone()).collect();
    RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), cols)
        .map_err(|e| anyhow!("cannot project PK columns: {e}"))
}

/// Re-align a key-only batch (a replayed tail Delete frame — self-described
/// schema) onto the canonical PK column types, matching columns by name.
/// Fails loudly when a column is missing or its values no longer fit the
/// (possibly evolved) canonical type.
pub fn align_key_batch(
    batch: &RecordBatch,
    canonical: &ArrowSchemaRef,
    pk_cols: &[String],
) -> Result<RecordBatch> {
    let can_idx = pk_indices(canonical, pk_cols)?;
    let src_idx = pk_indices(batch.schema_ref(), pk_cols)?;
    let mut fields = Vec::with_capacity(pk_cols.len());
    let mut cols: Vec<ArrayRef> = Vec::with_capacity(pk_cols.len());
    for (&ci, &si) in can_idx.iter().zip(&src_idx) {
        let field = canonical.field(ci);
        let src = batch.column(si);
        let col = if src.data_type() == field.data_type() {
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
            .map_err(|e| anyhow!("cannot align key column {:?}: {e}", field.name()))?
        };
        fields.push(field.clone());
        cols.push(col);
    }
    RecordBatch::try_new(Arc::new(ArrowSchema::new(fields)), cols)
        .map_err(|e| anyhow!("cannot build aligned key batch: {e}"))
}

/// Render one value of `col` at `row` as an unambiguous SQL literal.
fn render_sql_literal(col: &ArrayRef, row: usize) -> Result<String> {
    if col.is_null(row) {
        // Keyed entries are only ever created from `pk = <literal>` matches,
        // which never match NULL — reaching here is a bug upstream.
        bail!("keyed op holds a NULL key component");
    }
    let text = arrow::util::display::array_value_to_string(col, row)
        .map_err(|e| anyhow!("cannot render key value: {e}"))?;
    Ok(match col.data_type() {
        DataType::Int8 | DataType::Int16 | DataType::Int32 | DataType::Int64 => text,
        DataType::Boolean => text.to_uppercase(),
        DataType::Utf8 | DataType::LargeUtf8 => format!("'{}'", text.replace('\'', "''")),
        DataType::Date32 => format!("DATE '{text}'"),
        other => bail!("unsupported keyed PK literal type {other} (activation bug)"),
    })
}

/// Render the flusher's coalesced key predicate over a batch of key rows
/// (columns = PK columns in canonical types): `"pk" IN (v, ...)` for a
/// single-column key, an OR of per-row AND conjunctions for composite keys.
/// Never empty — the caller skips the op when there are no keys.
pub fn render_keys_predicate(keys: &RecordBatch) -> Result<String> {
    anyhow::ensure!(keys.num_rows() > 0, "empty keyed predicate");
    let names: Vec<String> = keys
        .schema_ref()
        .fields()
        .iter()
        .map(|f| quote_ident(f.name()))
        .collect();
    if keys.num_columns() == 1 {
        let col = keys.column(0);
        let vals: Vec<String> = (0..keys.num_rows())
            .map(|r| render_sql_literal(col, r))
            .collect::<Result<_>>()?;
        return Ok(format!("{} IN ({})", names[0], vals.join(", ")));
    }
    let mut rows = Vec::with_capacity(keys.num_rows());
    for r in 0..keys.num_rows() {
        let conj: Vec<String> = keys
            .columns()
            .iter()
            .zip(&names)
            .map(|(col, name)| Ok(format!("{name} = {}", render_sql_literal(col, r)?)))
            .collect::<Result<_>>()?;
        rows.push(format!("({})", conj.join(" AND ")));
    }
    Ok(format!("({})", rows.join(" OR ")))
}

// ---------------------------------------------------------------------------
// KeySuppressExec: hide committed rows whose key is buffered as updated or
// deleted. Wraps the committed scan child; the buffered (MemTable) side is
// filtered eagerly with `suppress_batch` before planning.
// ---------------------------------------------------------------------------

/// Execution plan dropping input rows whose PK-row is in a key set, then
/// (optionally) projecting away the PK columns a widened scan added purely
/// for the filter (`output_indices` into the INPUT schema — done here, not
/// with a ProjectionExec, so field-id metadata survives verbatim).
#[derive(Debug)]
pub struct KeySuppressExec {
    input: Arc<dyn ExecutionPlan>,
    pk_indices: Vec<usize>,
    keys: Arc<HashSet<Vec<u8>>>,
    output_indices: Option<Vec<usize>>,
    schema: ArrowSchemaRef,
    plan_properties: Arc<PlanProperties>,
}

impl KeySuppressExec {
    pub fn try_new(
        input: Arc<dyn ExecutionPlan>,
        pk_cols: &[String],
        keys: Arc<HashSet<Vec<u8>>>,
        output_indices: Option<Vec<usize>>,
    ) -> DFResult<Self> {
        let input_schema = input.schema();
        let pk_idx =
            pk_indices(&input_schema, pk_cols).map_err(|e| DataFusionError::External(e.into()))?;
        let schema: ArrowSchemaRef = match &output_indices {
            Some(idx) => Arc::new(input_schema.project(idx)?),
            None => input_schema,
        };
        let plan_properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(schema.clone()),
            Partitioning::UnknownPartitioning(
                input.properties().output_partitioning().partition_count(),
            ),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Ok(Self {
            input,
            pk_indices: pk_idx,
            keys,
            output_indices,
            schema,
            plan_properties,
        })
    }
}

impl DisplayAs for KeySuppressExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "KeySuppressExec keys={}", self.keys.len())
    }
}

impl ExecutionPlan for KeySuppressExec {
    fn name(&self) -> &str {
        "KeySuppressExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.plan_properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan + 'static>> {
        vec![&self.input]
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let input = children
            .pop()
            .ok_or_else(|| DataFusionError::Internal("KeySuppressExec expects one child".into()))?;
        Ok(Arc::new(Self {
            input,
            pk_indices: self.pk_indices.clone(),
            keys: self.keys.clone(),
            output_indices: self.output_indices.clone(),
            schema: self.schema.clone(),
            plan_properties: self.plan_properties.clone(),
        }))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DFResult<SendableRecordBatchStream> {
        let input = self.input.execute(partition, context)?;
        let pk_idx = self.pk_indices.clone();
        let keys = self.keys.clone();
        let output_indices = self.output_indices.clone();
        let stream = input.map(move |res| {
            res.and_then(|batch| {
                suppress_by_indices(&batch, &pk_idx, &keys, output_indices.as_deref())
                    .map_err(|e| DataFusionError::External(e.into()))
            })
        });
        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}

/// The per-batch body of [`KeySuppressExec`]: filter by key membership,
/// then project down to the requested output columns.
fn suppress_by_indices(
    batch: &RecordBatch,
    pk_indices: &[usize],
    keys: &HashSet<Vec<u8>>,
    output_indices: Option<&[usize]>,
) -> Result<RecordBatch> {
    let cols: Vec<ArrayRef> = pk_indices
        .iter()
        .map(|&i| batch.column(i).clone())
        .collect();
    let encoded = encode_key_columns(&cols)?;
    let mask = BooleanArray::from_iter(encoded.iter().map(|k| Some(!keys.contains(k))));
    let filtered = filter_record_batch(batch, &mask)
        .map_err(|e| anyhow!("key suppression filter failed: {e}"))?;
    match output_indices {
        Some(idx) => filtered
            .project(idx)
            .map_err(|e| anyhow!("key suppression projection failed: {e}")),
        None => Ok(filtered),
    }
}

// ---------------------------------------------------------------------------
// Unit tests.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Date32Array, Int64Array};
    use arrow::datatypes::Field;
    use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    fn parse(sql: &str) -> Statement {
        Parser::parse_sql(&PostgreSqlDialect {}, sql)
            .unwrap()
            .remove(0)
    }

    fn schema_i64_str() -> ArrowSchemaRef {
        Arc::new(ArrowSchema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("region", DataType::Utf8, false),
            Field::new("val", DataType::Utf8, true),
        ]))
    }

    fn batch(ids: &[i64], regions: &[&str], vals: &[&str]) -> RecordBatch {
        RecordBatch::try_new(
            schema_i64_str(),
            vec![
                Arc::new(Int64Array::from(ids.to_vec())),
                Arc::new(StringArray::from(regions.to_vec())),
                Arc::new(StringArray::from(vals.to_vec())),
            ],
        )
        .unwrap()
    }

    fn unquoted(name: &str) -> KeyCol {
        KeyCol {
            name: name.to_string(),
            quoted: false,
        }
    }

    // Exact-PK-equality shapes route; everything else falls back (None).
    #[test]
    fn candidate_parsing_matrix() {
        // Single-key UPDATE with a literal SET.
        let c = parse_keyed_candidate(&parse("UPDATE demo.t SET val = 'x' WHERE id = 7")).unwrap();
        assert_eq!(c.tag, "UPDATE");
        assert_eq!(c.eq, vec![(unquoted("id"), ScalarLit::Number("7".into()))]);
        assert_eq!(c.assigned, vec!["val".to_string()]);
        // Reversed equality + negative number + AND chain (composite).
        let c = parse_keyed_candidate(&parse("DELETE FROM demo.t WHERE -3 = id AND region = 'eu'"))
            .unwrap();
        assert_eq!(
            c.eq,
            vec![
                (unquoted("id"), ScalarLit::Number("-3".into())),
                (unquoted("region"), ScalarLit::Str("eu".into()))
            ]
        );
        // Fallbacks: non-equality, non-literal, OR, no WHERE, expression SET,
        // RETURNING (translate rejects), subquery (translate rejects).
        for sql in [
            "UPDATE demo.t SET val = 'x' WHERE id > 7",
            "UPDATE demo.t SET val = 'x' WHERE id = other_col",
            "UPDATE demo.t SET val = 'x' WHERE id = 7 OR id = 8",
            "UPDATE demo.t SET val = 'x'",
            "UPDATE demo.t SET val = val || 'x' WHERE id = 7",
            "UPDATE demo.t SET val = 'x' WHERE id = 7 RETURNING val",
            "DELETE FROM demo.t WHERE id IN (SELECT id FROM demo.t)",
            "DELETE FROM demo.t WHERE id = 7 AND val LIKE 'a%'",
        ] {
            assert!(
                parse_keyed_candidate(&parse(sql)).is_none(),
                "must fall back: {sql}"
            );
        }
    }

    // FIX (S1): compound identifiers route only when the qualifier names
    // the statement's own table (the alias once one is set), and quoted
    // identifiers carry their quoted-ness so activation can require an
    // exact-case PK match. Everything divergent falls back.
    #[test]
    fn qualified_and_quoted_identifier_routing() {
        // Wrong qualifier: sync would error "x.id not found" — never route.
        assert!(parse_keyed_candidate(&parse("DELETE FROM demo.t WHERE x.id = 5")).is_none());
        // Table-name qualifier routes.
        let c = parse_keyed_candidate(&parse("DELETE FROM demo.t WHERE t.id = 5")).unwrap();
        assert_eq!(c.eq, vec![(unquoted("id"), ScalarLit::Number("5".into()))]);
        // Namespace-qualified table routes; a foreign namespace does not.
        assert!(parse_keyed_candidate(&parse("DELETE FROM demo.t WHERE demo.t.id = 5")).is_some());
        assert!(parse_keyed_candidate(&parse("DELETE FROM demo.t WHERE other.t.id = 5")).is_none());
        // With an alias, ONLY the alias qualifies (Postgres rule).
        assert!(parse_keyed_candidate(&parse("DELETE FROM demo.t AS a WHERE a.id = 5")).is_some());
        assert!(parse_keyed_candidate(&parse("DELETE FROM demo.t AS a WHERE t.id = 5")).is_none());
        // Quoted identifier: parsed verbatim with quoted=true; matching is
        // exact-case only, so `"ID"` never binds PK column `id`.
        let c = parse_keyed_candidate(&parse("DELETE FROM demo.t WHERE \"ID\" = 5")).unwrap();
        let (col, _) = &c.eq[0];
        assert!(col.quoted && col.name == "ID");
        assert!(
            !key_col_matches(col, "id"),
            "quoted \"ID\" must not bind pk id"
        );
        assert!(key_col_matches(col, "ID"));
        // Unquoted stays case-insensitive.
        assert!(key_col_matches(&unquoted("id"), "ID"));
    }

    // FIX (S1): the literal→key derivation is tighter than arrow's cast —
    // divergent literals ('05' onto int, bool_pk = 1, number onto string)
    // error so the caller falls back to the synchronous path; exact
    // round-trips still route.
    #[test]
    fn key_derivation_round_trip_rejects_divergent_literals() {
        let pk = vec!["id".to_string()];
        let sch = schema_i64_str();
        let key = |lit: ScalarLit| literals_to_key_batch(&[(unquoted("id"), lit)], &pk, &sch);
        // int_pk = '05' map-hits nothing in sync: must NOT derive key 5.
        assert!(key(ScalarLit::Str("05".into())).is_err());
        assert!(key(ScalarLit::Number("05".into())).is_err());
        // Exact round-trips route.
        assert!(key(ScalarLit::Str("5".into())).is_ok());
        assert!(key(ScalarLit::Number("5".into())).is_ok());
        assert!(key(ScalarLit::Number("-5".into())).is_ok());
        // Category mismatches never route.
        assert!(key(ScalarLit::Bool(true)).is_err());
        // str_pk = 5 (number onto a string column): sync coercion is the
        // authority — fall back.
        let pk_r = vec!["region".to_string()];
        assert!(literals_to_key_batch(
            &[(unquoted("region"), ScalarLit::Number("5".into()))],
            &pk_r,
            &sch
        )
        .is_err());
        assert!(literals_to_key_batch(
            &[(unquoted("region"), ScalarLit::Str("eu".into()))],
            &pk_r,
            &sch
        )
        .is_ok());
        // bool_pk = 1 falls back; bool_pk = true routes.
        let bool_schema: ArrowSchemaRef = Arc::new(ArrowSchema::new(vec![Field::new(
            "flag",
            DataType::Boolean,
            false,
        )]));
        let pk_b = vec!["flag".to_string()];
        assert!(literals_to_key_batch(
            &[(unquoted("flag"), ScalarLit::Number("1".into()))],
            &pk_b,
            &bool_schema
        )
        .is_err());
        assert!(literals_to_key_batch(
            &[(unquoted("flag"), ScalarLit::Bool(true))],
            &pk_b,
            &bool_schema
        )
        .is_ok());
        // Dates: the canonical rendering routes, a non-canonical spelling
        // (valid to the cast, divergent from sync) falls back.
        let date_schema: ArrowSchemaRef = Arc::new(ArrowSchema::new(vec![Field::new(
            "d",
            DataType::Date32,
            false,
        )]));
        let pk_d = vec!["d".to_string()];
        assert!(literals_to_key_batch(
            &[(unquoted("d"), ScalarLit::Str("2022-01-08".into()))],
            &pk_d,
            &date_schema
        )
        .is_ok());
        assert!(literals_to_key_batch(
            &[(unquoted("d"), ScalarLit::Str("2022-1-8".into()))],
            &pk_d,
            &date_schema
        )
        .is_err());
    }

    // Composite keys: literals cast onto canonical types encode equal to
    // batch rows with the same values, and differ otherwise.
    #[test]
    fn composite_key_encoding_matches_batch_rows() {
        let pk = vec!["id".to_string(), "region".to_string()];
        let rows = batch(&[1, 2], &["eu", "us"], &["a", "b"]);
        let keys = encode_batch_keys(&rows, &pk).unwrap();
        assert_eq!(keys.len(), 2);
        let lit = literals_to_key_batch(
            &[
                (unquoted("region"), ScalarLit::Str("us".into())),
                (unquoted("id"), ScalarLit::Number("2".into())),
            ],
            &pk,
            &schema_i64_str(),
        )
        .unwrap();
        let lit_key = encode_batch_keys(&lit, &pk).unwrap().remove(0);
        assert_eq!(lit_key, keys[1], "literal (2,'us') == row (2,'us')");
        assert_ne!(lit_key, keys[0]);
    }

    // Suppression: rows whose key is in the set disappear; others survive.
    #[test]
    fn suppress_batch_hides_keyed_rows() {
        let pk = vec!["id".to_string()];
        let rows = batch(&[1, 2, 3], &["a", "b", "c"], &["x", "y", "z"]);
        let keys: HashSet<Vec<u8>> = encode_batch_keys(&batch(&[2], &["b"], &["y"]), &pk)
            .unwrap()
            .into_iter()
            .collect();
        let out = suppress_batch(&rows, &pk, &keys).unwrap();
        assert_eq!(out.num_rows(), 2);
        let ids: Vec<i64> = out
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec();
        assert_eq!(ids, vec![1, 3]);
        // Empty key set: untouched.
        let out = suppress_batch(&rows, &pk, &HashSet::new()).unwrap();
        assert_eq!(out.num_rows(), 3);
    }

    // Literal rendering: IN-list for single keys, OR-of-ANDs for composite,
    // quoting and escaping for strings, DATE prefix for dates.
    #[test]
    fn predicate_rendering() {
        let pk = ["id".to_string()];
        let single = literals_to_key_batch(
            &[(unquoted("id"), ScalarLit::Number("5".into()))],
            &pk,
            &schema_i64_str(),
        )
        .unwrap();
        assert_eq!(render_keys_predicate(&single).unwrap(), "\"id\" IN (5)");

        let pk2 = vec!["id".to_string(), "region".to_string()];
        let comp = batch(&[1, 2], &["e'u", "us"], &["a", "b"]);
        let comp_keys = comp.project(&[0, 1]).unwrap();
        assert_eq!(
            render_keys_predicate(&comp_keys).unwrap(),
            "((\"id\" = 1 AND \"region\" = 'e''u') OR (\"id\" = 2 AND \"region\" = 'us'))"
        );
        let _ = pk2;

        // Dates render with the DATE keyword.
        let date_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "d",
            DataType::Date32,
            false,
        )]));
        let d = RecordBatch::try_new(
            date_schema,
            vec![Arc::new(Date32Array::from(vec![19_000]))], // 2022-01-08
        )
        .unwrap();
        let rendered = render_keys_predicate(&d).unwrap();
        assert!(
            rendered.starts_with("\"d\" IN (DATE '"),
            "unexpected: {rendered}"
        );
    }

    // RMW against a mock view: the same evaluator the sync engine uses
    // (apply_dml_to_batches) applied to the resolved current row produces
    // the replacement row for the tail upsert.
    #[test]
    fn rmw_produces_replacement_row() {
        let stmt = parse("UPDATE demo.t SET val = 'new' WHERE id = 2");
        let cand = parse_keyed_candidate(&stmt).unwrap();
        let current = batch(&[2], &["eu"], &["old"]);
        let columns = vec!["id".to_string(), "region".to_string(), "val".to_string()];
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let (matched, out) = rt
            .block_on(crate::overwrite::apply_dml_to_batches(
                &cand.dml,
                &columns,
                vec![current],
            ))
            .unwrap();
        assert_eq!(matched, 1);
        let total: usize = out.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total, 1);
        let vals = out[0]
            .column(2)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(vals.value(0), "new");
        // The key columns are untouched (keyed path forbids SET on PK).
        let ids = out[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(ids.value(0), 2);
    }

    // Supported key types are exactly the renderable ones.
    #[test]
    fn key_type_support_matrix() {
        for dt in [
            DataType::Int32,
            DataType::Int64,
            DataType::Utf8,
            DataType::Boolean,
            DataType::Date32,
        ] {
            assert!(key_type_supported(&dt), "{dt} must be supported");
        }
        for dt in [
            DataType::Float64,
            DataType::Timestamp(arrow::datatypes::TimeUnit::Microsecond, None),
            DataType::Binary,
        ] {
            assert!(!key_type_supported(&dt), "{dt} must fall back");
        }
    }
}
