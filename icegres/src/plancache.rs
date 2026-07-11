//! Physical-plan cache for repeated simple-protocol SELECT shapes
//! (registered only with `--freshness-ms > 0`).
//!
//! # What upstream already caches (investigated, not duplicated)
//!
//! datafusion-postgres 0.15 caches exactly one thing: the **logical plan of
//! an extended-protocol prepared statement** (`DfSessionService`'s
//! `Statement = (String, Option<(sqlparser Statement, LogicalPlan)>)`,
//! built once at Parse time). Every Execute still runs
//! `replace_params_with_values` → `optimize` → physical planning. The
//! **simple protocol** (what psql/psycopg2 send by default) caches nothing:
//! each statement pays pg-compat parse → `ctx.sql` (DataFusion parse +
//! logical plan) → optimize + physical plan → execute. This module adds the
//! missing layer — a bounded LRU from statement shape to the **physical
//! plan** — for the simple protocol only. Extended-protocol executes are
//! left to the upstream logical-plan cache: their bound parameter values
//! are baked into the physical plan at optimize time, so a physical plan
//! cannot be soundly reused across different parameter values (see below).
//!
//! # Cache key and soundness
//!
//! Key: the statement's normalized SQL text (the parsed AST re-rendered, so
//! whitespace/comment variants collapse) plus the session-relevant planning
//! state — default catalog, default schema (search path), and time zone.
//! Key and plan come from ONE `SessionState` snapshot per statement (the
//! same snapshot both derives the key and plans on a miss), so a concurrent
//! global `SET` can never poison an entry under a mismatched key.
//! Value: the DataFusion physical plan. Physical plans are NOT universally
//! re-executable (`RepartitionExec` consumes its per-instance channel state
//! on first execution), so every hit rebuilds the plan's internal nodes via
//! [`reset_plan`] — microseconds of allocation, reusing the leaf scans that
//! hold the expensive plan-time pruning work.
//!
//! An Iceberg physical plan bakes in the exact data-file list of the
//! snapshot it was planned against (manifest pruning happens at plan time).
//! A hit is therefore only sound when every referenced table is provably at
//! the same metadata version the plan scanned, **without a catalog round
//! trip** (a per-hit catalog check would re-add the cost the freshness
//! refresher just removed). That is why the cache is active only in
//! freshness mode: each entry records `(table, metadata-version)` for every
//! scanned table, and a hit requires each table's `CachingTableProvider` to
//! be currently *fresh* at exactly that version
//! ([`CachingTableProvider::plan_cache_version`]). Invalidation therefore
//! rides the same paths as the freshness refresher — a foreign commit swaps
//! the version on the next poll, a local write invalidates synchronously,
//! and a DDL fence (`deregister_table`) removes the table from the
//! freshness registry — any of which makes version validation fail and the
//! entry lazily replaced. Consequently the same statement's literal values
//! are part of the key too: plan-time file pruning makes a plan for
//! `trip_id = 5` unsound for `trip_id = 6`, so only *repeated identical*
//! statements hit.
//!
//! Excluded from caching (the statement still executes normally, it just
//! re-plans every time):
//!
//! * **Overlay-bearing tables** (`--write-buffer-ms > 0`): the buffered/
//!   keyed overlay is per-scan state unioned into the plan at plan time — a
//!   cached plan would bake a stale overlay in and serve vanished/duplicate
//!   rows. The scope calls this the overlay trap; exclusion is the chosen
//!   remedy ([`crate::cache::plan_cache_eligible`]).
//! * **Time-travel (`table@snapshot`), metadata (`table$type`), pg_catalog
//!   and information_schema tables**: anything whose scan source is not a
//!   [`CachingTableProvider`] — cheap and rare per the scope.
//! * **Non-immutable expressions** (`now()`, `current_timestamp`,
//!   `random()`, ...): `now()` and friends are const-folded to the
//!   statement's own plan time during optimization, so replaying the plan
//!   would replay a stale clock. Anything whose scalar-function volatility
//!   is not `Immutable` (or a placeholder/config variable) opts the
//!   statement out.
//! * **Table-less statements** (`select 1`, driver health probes): correct
//!   to cache but pointless (planning them is microseconds) — skipped so
//!   they don't churn the LRU.
//!
//! # Divergences from the default handler (when this hook handles a SELECT)
//!
//! Same pipeline, same statement_timeout treatment (applied to the planning
//! phase, exactly like upstream `do_query`), same streaming row encoding.
//! One diagnostic exception: with `ICEGRES_QUERY_TIMING=1` rows are
//! buffered (not streamed) so per-stage timings can be logged — the same
//! divergence timing.rs documents.

use std::collections::HashMap;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::common::ParamValues;
use datafusion::datasource::source_as_provider;
use datafusion::execution::session_state::SessionState;
use datafusion::execution::TaskContext;
use datafusion::logical_expr::{Expr, LogicalPlan, Volatility};
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::Statement as SqlStatement;
use datafusion_postgres::arrow_pg::datatypes::{arrow_schema_to_pg_fields, encode_recordbatch};
use datafusion_postgres::datafusion_pg_catalog::sql::PostgresCompatibilityParser;
use datafusion_postgres::pgwire::api::portal::Format;
use datafusion_postgres::pgwire::api::results::{QueryResponse, Response};
use datafusion_postgres::pgwire::api::ClientInfo;
use datafusion_postgres::pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use datafusion_postgres::pgwire::messages::data::DataRow;
use datafusion_postgres::pgwire::types::format::FormatOptions;
use datafusion_postgres::QueryHook;
use futures::StreamExt;

use crate::cache::{CachingTableProvider, MetadataVersion};
use crate::metrics::metrics;
use crate::timing;

/// Default LRU capacity; override with `ICEGRES_PLAN_CACHE_ENTRIES`
/// (`0` disables the cache — every SELECT falls through to the default
/// handler unchanged).
const DEFAULT_CAPACITY: usize = 256;

/// Cache key: normalized statement text plus the session state that affects
/// name resolution and planning. The session context is process-shared (one
/// `SessionContext` serves every connection), so the key MUST be derived
/// from the very [`SessionState`] snapshot that plans the statement — one
/// snapshot per statement, taken once in [`PlanCacheHook::run`]. Deriving
/// the key from a separate `ctx.state()` clone would race a concurrent
/// global `SET` (e.g. `datafusion.catalog.default_schema`) between key
/// derivation and planning, permanently filing a plan under a key that
/// describes different planning state.
#[derive(Clone, PartialEq, Eq, Hash, Debug)]
struct PlanKey {
    catalog: String,
    schema: String,
    timezone: String,
    sql: String,
}

impl PlanKey {
    /// Derive the key from `state` — the SAME snapshot the caller plans
    /// with (see the type docs for why they must not diverge).
    fn from_state(state: &SessionState, sql: String) -> Self {
        let options = state.config_options();
        Self {
            catalog: options.catalog.default_catalog.clone(),
            schema: options.catalog.default_schema.clone(),
            timezone: format!("{:?}", options.execution.time_zone),
            sql,
        }
    }
}

struct Entry {
    plan: Arc<dyn ExecutionPlan>,
    schema: ArrowSchemaRef,
    /// Every table the plan scans, at the metadata version the plan was
    /// built against: `(freshness registry key, version)`.
    tables: Vec<(String, MetadataVersion)>,
    last_used: u64,
}

/// Bounded LRU of physical plans.
struct PlanCache {
    entries: StdMutex<(HashMap<PlanKey, Entry>, u64)>,
    capacity: usize,
}

impl PlanCache {
    fn new(capacity: usize) -> Self {
        Self {
            entries: StdMutex::new((HashMap::new(), 0)),
            capacity,
        }
    }

    /// Look up `key`, validating every referenced table's current version
    /// through `resolve` (production: [`current_version`]). A version
    /// mismatch or vanished table removes the entry (miss) — the caller's
    /// re-plan re-inserts the fresh plan under the same key.
    fn lookup_with(
        &self,
        key: &PlanKey,
        resolve: impl Fn(&str) -> Option<MetadataVersion>,
    ) -> Option<(Arc<dyn ExecutionPlan>, ArrowSchemaRef)> {
        // M3: recover a poisoned lock instead of panicking — the cache is
        // shared across every connection, and one panic under the lock
        // must not turn every later SELECT into a panic. Worst case after
        // an unwind mid-insert is a stale/missing entry, which the version
        // validation below already treats as a miss.
        let mut guard = crate::freshness::recover("plan cache", self.entries.lock());
        let (map, clock) = &mut *guard;
        let entry = map.get_mut(key)?;
        let valid = entry
            .tables
            .iter()
            .all(|(table, version)| resolve(table).as_ref() == Some(version));
        if !valid {
            map.remove(key);
            return None;
        }
        *clock += 1;
        entry.last_used = *clock;
        Some((entry.plan.clone(), entry.schema.clone()))
    }

    fn lookup(&self, key: &PlanKey) -> Option<(Arc<dyn ExecutionPlan>, ArrowSchemaRef)> {
        self.lookup_with(key, current_version)
    }

    fn insert(
        &self,
        key: PlanKey,
        plan: Arc<dyn ExecutionPlan>,
        schema: ArrowSchemaRef,
        tables: Vec<(String, MetadataVersion)>,
    ) {
        // M3: see lookup_with — recover, never panic-cascade.
        let mut guard = crate::freshness::recover("plan cache", self.entries.lock());
        let (map, clock) = &mut *guard;
        *clock += 1;
        map.insert(
            key,
            Entry {
                plan,
                schema,
                tables,
                last_used: *clock,
            },
        );
        // Evict least-recently-used entries beyond the cap.
        while map.len() > self.capacity {
            if let Some(lru) = map
                .iter()
                .min_by_key(|(_, e)| e.last_used)
                .map(|(k, _)| k.clone())
            {
                map.remove(&lru);
            } else {
                break;
            }
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.lock().expect("plan cache poisoned").0.len()
    }
}

/// The current cacheable metadata version of the table registered under
/// `key` in the freshness registry: `Some` only when the table's provider
/// is live, freshness-managed, currently fresh, and overlay-free
/// ([`CachingTableProvider::plan_cache_version`]).
fn current_version(key: &str) -> Option<MetadataVersion> {
    crate::freshness::provider(key)?.plan_cache_version()
}

/// Why a planned statement cannot be cached (kept for tests/debugging).
#[derive(Debug, PartialEq)]
enum Uncacheable {
    /// A scalar function that is not `Immutable` (now(), random(), ...), a
    /// placeholder, or a config variable.
    NonImmutableExpr,
    /// A scan source that is not a freshness-managed, overlay-free
    /// [`CachingTableProvider`] (metadata/time-travel/pg_catalog tables,
    /// buffered tables, default mode).
    IneligibleTable,
}

/// Walk the (pre-optimization) logical plan — including subquery plans —
/// and either collect every scanned table's `(registry key, current
/// version)` or report why the statement must not be cached.
fn analyze(plan: &LogicalPlan) -> Result<Vec<(String, MetadataVersion)>, Uncacheable> {
    let mut tables: HashMap<String, MetadataVersion> = HashMap::new();
    let mut blocker: Option<Uncacheable> = None;
    plan.apply_with_subqueries(|node| {
        node.apply_expressions(|expr| {
            expr.apply(|e| {
                if expr_blocks_caching(e) {
                    blocker = Some(Uncacheable::NonImmutableExpr);
                    return Ok(TreeNodeRecursion::Stop);
                }
                Ok(TreeNodeRecursion::Continue)
            })
        })?;
        if blocker.is_some() {
            return Ok(TreeNodeRecursion::Stop);
        }
        if let LogicalPlan::TableScan(scan) = node {
            let version = source_as_provider(&scan.source).ok().and_then(|provider| {
                provider
                    .as_any()
                    .downcast_ref::<CachingTableProvider>()
                    .map(|caching| (caching.table_key(), caching.plan_cache_version()))
            });
            match version {
                Some((key, Some(version))) => {
                    tables.insert(key, version);
                }
                _ => {
                    blocker = Some(Uncacheable::IneligibleTable);
                    return Ok(TreeNodeRecursion::Stop);
                }
            }
        }
        Ok(TreeNodeRecursion::Continue)
    })
    .expect("plan walk is infallible");
    match blocker {
        Some(reason) => Err(reason),
        None => Ok(tables.into_iter().collect()),
    }
}

/// True for expression nodes that make a plan unsound to replay: scalar
/// functions that are not `Immutable` (`now()` et al. are const-folded to
/// plan time during optimization; `random()` is defensively excluded with
/// them), placeholders, and config variables.
fn expr_blocks_caching(expr: &Expr) -> bool {
    match expr {
        Expr::ScalarFunction(f) => f.func.signature().volatility != Volatility::Immutable,
        Expr::Placeholder(_) | Expr::ScalarVariable(..) => true,
        _ => false,
    }
}

/// Replicates upstream `client::get_statement_timeout` (pub(crate) there):
/// the per-session statement timeout SET/SHOW stores in client metadata.
fn statement_timeout(client: &(dyn ClientInfo + Send + Sync)) -> Option<Duration> {
    client
        .metadata()
        .get("statement_timeout_ms")
        .and_then(|s| s.parse::<u64>().ok())
        .map(Duration::from_millis)
}

fn timeout_error() -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        "57014".to_string(),
        "canceling statement due to statement timeout".to_string(),
    )))
}

/// Query hook implementing the plan cache. Handles plain simple-protocol
/// `SELECT`s (every specialized hook has already had its chance); extended
/// protocol falls through to upstream's prepared-statement logical-plan
/// cache (see the module docs).
pub struct PlanCacheHook {
    cache: PlanCache,
    /// Same compatibility parser the wire handler uses — only exercised in
    /// `ICEGRES_QUERY_TIMING=1` mode to measure the real parse cost of the
    /// stage breakdown (timing.rs).
    parser: PostgresCompatibilityParser,
}

impl PlanCacheHook {
    pub fn new() -> Self {
        // L5: an unparseable override WARNs and falls back, matching the
        // other knobs (ICEGRES_SCAN_CONCURRENCY & co.) — never a silent
        // default.
        let capacity = match std::env::var("ICEGRES_PLAN_CACHE_ENTRIES") {
            Ok(raw) => raw.trim().parse::<usize>().unwrap_or_else(|_| {
                tracing::warn!(
                    value = %raw,
                    default = DEFAULT_CAPACITY,
                    "invalid ICEGRES_PLAN_CACHE_ENTRIES; using default"
                );
                DEFAULT_CAPACITY
            }),
            Err(_) => DEFAULT_CAPACITY,
        };
        Self {
            cache: PlanCache::new(capacity),
            parser: PostgresCompatibilityParser::new(),
        }
    }

    async fn run(
        &self,
        statement: &SqlStatement,
        ctx: &SessionContext,
        client: &mut (dyn ClientInfo + Send + Sync),
    ) -> PgWireResult<Response> {
        let total = Instant::now();
        let record_stages = timing::enabled();
        let sql = statement.to_string();
        // ONE SessionState snapshot for the whole statement: the cache key
        // is derived from the SAME state that plans on a miss, so a
        // concurrent global SET (the SessionContext is process-shared —
        // e.g. `SET datafusion.catalog.default_schema` from another
        // connection) between key derivation and planning can never file a
        // plan under a key describing different planning state (which would
        // permanently poison that key's entry). Locked by the
        // `plan_key_and_planning_share_one_state_snapshot` test.
        let state = ctx.state();
        let key = PlanKey::from_state(&state, sql.clone());

        // HIT: every referenced table is fresh at the planned version.
        let lookup_started = Instant::now();
        if let Some((plan, schema)) = self.cache.lookup(&key) {
            // Rebuild the plan's internal nodes so per-instance execution
            // state starts fresh (see [`reset_plan`]) — the expensive leaf
            // scans (pruned file lists) are reused as-is.
            let plan = reset_plan(plan).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            metrics()
                .plan_cache_hits_total
                .fetch_add(1, Ordering::Relaxed);
            if record_stages {
                timing::record("plan_cache_hit", lookup_started.elapsed());
            }
            return respond(plan, schema, state.task_ctx(), client, record_stages, total).await;
        }
        metrics()
            .plan_cache_misses_total
            .fetch_add(1, Ordering::Relaxed);

        // MISS: run the default handler's pipeline (statement_timeout on the
        // planning phase, exactly like upstream do_query), then cache the
        // physical plan if the statement shape is sound to replay.
        if record_stages {
            // Measure the stage-(b) pg-compat parse the wire handler already
            // paid (same approach as timing.rs, so before/after per-stage
            // breakdowns stay comparable).
            let t = Instant::now();
            self.parser
                .parse(&sql)
                .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
            timing::record("parse", t.elapsed());
        }
        let t = Instant::now();
        // Plan with the SAME state snapshot the key was derived from (the
        // equivalent of `ctx.sql` for a read-only statement, minus the
        // fresh state clone `ctx.sql` would take).
        let logical_result = match statement_timeout(client) {
            Some(limit) => tokio::time::timeout(limit, state.create_logical_plan(&sql))
                .await
                .map_err(|_| timeout_error())?,
            None => state.create_logical_plan(&sql).await,
        };
        let logical = logical_result.map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        if record_stages {
            timing::record("plan_logical", t.elapsed());
        }

        // Capture cacheability + versions BEFORE physical planning …
        let tables_before = analyze(&logical);
        let t = Instant::now();
        let plan = state
            .create_physical_plan(&logical)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        if record_stages {
            timing::record("plan_physical", t.elapsed());
        }
        let schema = plan.schema();
        if let Ok(tables) = tables_before {
            // … and require them UNCHANGED after: the physical plan scanned
            // the providers' cached snapshots, so `before == after == fresh`
            // proves the entry's versions are the ones baked into the plan
            // (a write or refresh racing the planning window skips caching).
            // Table-less statements (`select 1`, health probes) are not
            // cached: planning them is already cheap and they would churn
            // the LRU and the hit/miss counters for no measurable win.
            let unchanged = tables
                .iter()
                .all(|(table, version)| current_version(table).as_ref() == Some(version));
            if unchanged && !tables.is_empty() && plan_safe_to_cache(&plan) {
                self.cache.insert(key, plan.clone(), schema.clone(), tables);
            }
        }
        respond(plan, schema, state.task_ctx(), client, record_stages, total).await
    }
}

/// Rebuild every internal node of a physical plan via `with_new_children`
/// so per-plan-instance execution state starts fresh. DataFusion physical
/// plans are not universally re-executable: `RepartitionExec` (present in
/// almost any multi-partition plan) builds its partition channels once per
/// instance and CONSUMES them on first execution — executing the same
/// instance again panics with "partition not used yet". Reconstructing the
/// internal nodes (microseconds, no IO) gives each execution fresh state,
/// while the LEAF nodes — the Iceberg/parquet scans holding the expensive
/// plan-time manifest pruning and file lists — are reused as-is (they build
/// fresh streams on every `execute` call).
fn reset_plan(plan: Arc<dyn ExecutionPlan>) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
    let children = plan.children();
    if children.is_empty() {
        return Ok(plan);
    }
    let rebuilt = children
        .into_iter()
        .map(|child| reset_plan(Arc::clone(child)))
        .collect::<datafusion::error::Result<Vec<_>>>()?;
    plan.with_new_children(rebuilt)
}

/// Reject the rare node kinds whose execution state [`reset_plan`] cannot
/// safely rebuild: recursive-CTE nodes share a work table ACROSS nodes of
/// the same plan, so reconstructing them independently could tear that link.
fn plan_safe_to_cache(plan: &Arc<dyn ExecutionPlan>) -> bool {
    if matches!(plan.name(), "RecursiveQueryExec" | "WorkTableExec") {
        return false;
    }
    plan.children()
        .iter()
        .all(|child| plan_safe_to_cache(child))
}

/// Execute `plan` and encode rows for the wire. Streaming (identical shape
/// to arrow-pg's `encode_dataframe`) in normal operation; collect-then-
/// encode when `ICEGRES_QUERY_TIMING=1` so per-stage timings can be logged
/// (same diagnostic divergence as timing.rs).
async fn respond(
    plan: Arc<dyn ExecutionPlan>,
    schema: ArrowSchemaRef,
    task_ctx: Arc<TaskContext>,
    client: &mut (dyn ClientInfo + Send + Sync),
    record_stages: bool,
    total: Instant,
) -> PgWireResult<Response> {
    let format_options = Arc::new(FormatOptions::from_client_metadata(client.metadata()));
    let fields = Arc::new(arrow_schema_to_pg_fields(
        &schema,
        &Format::UnifiedText,
        Some(format_options),
    )?);
    if record_stages {
        let t = Instant::now();
        let batches = datafusion::physical_plan::collect(plan, task_ctx)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        timing::record("execute_collect", t.elapsed());
        let t = Instant::now();
        let mut rows: Vec<PgWireResult<DataRow>> = Vec::new();
        for batch in batches {
            rows.extend(encode_recordbatch(fields.clone(), batch));
        }
        timing::record("encode", t.elapsed());
        timing::record("total", total.elapsed());
        return Ok(Response::Query(QueryResponse::new(
            fields,
            futures::stream::iter(rows),
        )));
    }
    let stream = execute_stream(plan, task_ctx).map_err(|e| PgWireError::ApiError(Box::new(e)))?;
    let fields_ref = fields.clone();
    let pg_row_stream = stream
        .map(move |batch| {
            let rows: Box<dyn Iterator<Item = PgWireResult<DataRow>> + Send + Sync> = match batch {
                Ok(batch) => encode_recordbatch(fields_ref.clone(), batch),
                Err(e) => Box::new(std::iter::once(Err(PgWireError::ApiError(e.into())))),
            };
            futures::stream::iter(rows)
        })
        .flatten();
    Ok(Response::Query(QueryResponse::new(fields, pg_row_stream)))
}

#[async_trait]
impl QueryHook for PlanCacheHook {
    async fn handle_simple_query(
        &self,
        statement: &SqlStatement,
        session_context: &SessionContext,
        client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        if self.cache.capacity == 0 || !matches!(statement, SqlStatement::Query(_)) {
            return None;
        }
        Some(self.run(statement, session_context, client).await)
    }

    async fn handle_extended_parse_query(
        &self,
        _sql: &SqlStatement,
        _session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        // Upstream already caches the prepared statement's logical plan;
        // physical plans cannot be reused across parameter values.
        None
    }

    async fn handle_extended_query(
        &self,
        _statement: &SqlStatement,
        _logical_plan: &LogicalPlan,
        _params: &ParamValues,
        _session_context: &SessionContext,
        _client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::arrow::array::Int64Array;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::arrow::record_batch::RecordBatch;
    use datafusion::datasource::MemTable;
    use datafusion::physical_plan::empty::EmptyExec;

    fn key(sql: &str, schema: &str) -> PlanKey {
        PlanKey {
            catalog: "icegres".into(),
            schema: schema.into(),
            timezone: "utc".into(),
            sql: sql.into(),
        }
    }

    fn dummy_entry() -> (Arc<dyn ExecutionPlan>, ArrowSchemaRef) {
        let schema: ArrowSchemaRef =
            Arc::new(Schema::new(vec![Field::new("x", DataType::Int64, false)]));
        (Arc::new(EmptyExec::new(schema.clone())), schema)
    }

    #[test]
    fn hit_and_miss_on_statement_fingerprint_and_session_state() {
        let cache = PlanCache::new(8);
        let (plan, schema) = dummy_entry();
        let version: MetadataVersion = (Some("v1".into()), Some(1));
        cache.insert(
            key("select * from t where id = 5", "demo"),
            plan,
            schema,
            vec![("demo\u{1f}t".into(), version.clone())],
        );
        let resolve = |k: &str| (k == "demo\u{1f}t").then(|| version.clone());
        // Same fingerprint + same session state: hit.
        assert!(cache
            .lookup_with(&key("select * from t where id = 5", "demo"), resolve)
            .is_some());
        // Different literal (different fingerprint): miss.
        assert!(cache
            .lookup_with(&key("select * from t where id = 6", "demo"), resolve)
            .is_none());
        // Same SQL, different search path: miss (resolves other tables).
        assert!(cache
            .lookup_with(&key("select * from t where id = 5", "other"), resolve)
            .is_none());
    }

    #[test]
    fn snapshot_bump_invalidates_and_evicts_the_entry() {
        let cache = PlanCache::new(8);
        let (plan, schema) = dummy_entry();
        let v1: MetadataVersion = (Some("v1".into()), Some(1));
        let v2: MetadataVersion = (Some("v2".into()), Some(2));
        let k = key("select count(*) from t", "demo");
        cache.insert(k.clone(), plan, schema, vec![("demo\u{1f}t".into(), v1)]);
        // Table committed → current version moved to v2: entry invalid AND
        // removed, so the next miss re-inserts the fresh plan.
        assert!(cache.lookup_with(&k, |_| Some(v2.clone())).is_none());
        assert_eq!(cache.len(), 0);
        // Table dropped (deregistered from the freshness registry): miss.
        let (plan, schema) = dummy_entry();
        let v1: MetadataVersion = (Some("v1".into()), Some(1));
        cache.insert(k.clone(), plan, schema, vec![("demo\u{1f}t".into(), v1)]);
        assert!(cache.lookup_with(&k, |_| None).is_none());
    }

    #[test]
    fn lru_stays_bounded_and_evicts_least_recently_used() {
        let cache = PlanCache::new(4);
        for i in 0..10 {
            let (plan, schema) = dummy_entry();
            cache.insert(key(&format!("select {i}"), "demo"), plan, schema, vec![]);
        }
        assert_eq!(cache.len(), 4);
        // The four newest survive; older ones were evicted.
        let hit = |sql: &str| cache.lookup_with(&key(sql, "demo"), |_| None).is_some();
        assert!(hit("select 9") && hit("select 6"));
        assert!(!hit("select 0") && !hit("select 5"));
    }

    async fn ctx_with_memtable() -> SessionContext {
        let ctx = SessionContext::new();
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1i64, 2, 3]))],
        )
        .unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("t", Arc::new(table)).unwrap();
        ctx
    }

    #[tokio::test]
    async fn plan_key_and_planning_share_one_state_snapshot() {
        // F2 contract, asserted by construction/API: `run` takes ONE
        // SessionState snapshot and derives BOTH the cache key and the plan
        // from it, so a concurrent global SET between the two cannot file a
        // plan under the wrong schema key. This test replays the race
        // against the same API: snapshot, then mutate the shared context,
        // then show the snapshot still keys AND plans under its own
        // (pre-SET) schema while the mutated context does neither.
        let ctx = ctx_with_memtable().await; // registers `t` under `public`
        let state = ctx.state();
        let key = PlanKey::from_state(&state, "select id from t".into());
        assert_eq!(key.schema, "public");

        // A racing SET on the process-shared context AFTER the snapshot.
        ctx.sql("set datafusion.catalog.default_schema = 'elsewhere'")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        // A fresh snapshot now keys differently …
        let key_after = PlanKey::from_state(&ctx.state(), "select id from t".into());
        assert_eq!(key_after.schema, "elsewhere");
        assert_ne!(key.schema, key_after.schema);
        // … and no longer resolves `t` (proving planning follows the state
        // the key describes) …
        assert!(
            ctx.state()
                .create_logical_plan("select id from t")
                .await
                .is_err(),
            "the mutated context must not resolve t under schema 'elsewhere'"
        );
        // … while the captured snapshot still plans under exactly the
        // schema its key records: key and plan cannot diverge.
        assert!(state.create_logical_plan("select id from t").await.is_ok());
    }

    #[tokio::test]
    async fn analyze_rejects_non_caching_table_providers() {
        // A MemTable stands in for every non-CachingTableProvider source
        // (pg_catalog, metadata `$` tables, time-travel pins, buffered
        // overlays surface the same way): not cacheable.
        let ctx = ctx_with_memtable().await;
        let plan = ctx
            .sql("select id from t where id = 1")
            .await
            .unwrap()
            .into_parts()
            .1;
        assert_eq!(analyze(&plan), Err(Uncacheable::IneligibleTable));
    }

    #[tokio::test]
    async fn analyze_rejects_non_immutable_expressions() {
        let ctx = SessionContext::new();
        for sql in ["select now()", "select random()", "select current_date"] {
            let plan = ctx.sql(sql).await.unwrap().into_parts().1;
            assert_eq!(
                analyze(&plan),
                Err(Uncacheable::NonImmutableExpr),
                "{sql} must not be cacheable"
            );
        }
    }

    #[tokio::test]
    async fn analyze_rejects_non_immutable_exprs_inside_subqueries() {
        let ctx = ctx_with_memtable().await;
        let plan = ctx
            .sql("select id from t where id in (select cast(random() * 3 as bigint))")
            .await
            .unwrap()
            .into_parts()
            .1;
        assert!(analyze(&plan).is_err());
    }

    #[tokio::test]
    async fn analyze_accepts_tableless_immutable_statements() {
        let ctx = SessionContext::new();
        let plan = ctx.sql("select 1 + 1").await.unwrap().into_parts().1;
        assert_eq!(analyze(&plan), Ok(vec![]));
    }

    #[tokio::test]
    async fn reset_plan_makes_a_cached_plan_re_executable() {
        // A multi-partition aggregate contains RepartitionExec, whose
        // channel state is consumed by the first execution — the exact
        // shape a cached point-lookup/aggregate plan replays.
        let ctx = SessionContext::new_with_config(
            datafusion::prelude::SessionConfig::new().with_target_partitions(4),
        );
        let schema = Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1i64, 2, 3, 4]))],
        )
        .unwrap();
        let table = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("t", Arc::new(table)).unwrap();
        let df = ctx
            .sql("select id % 2 as k, count(*) from t group by k")
            .await
            .unwrap();
        let (state, logical) = df.into_parts();
        let plan = state.create_physical_plan(&logical).await.unwrap();
        assert!(
            format!(
                "{}",
                datafusion::physical_plan::displayable(plan.as_ref()).indent(false)
            )
            .contains("RepartitionExec"),
            "test plan must contain the stateful node under test"
        );
        assert!(plan_safe_to_cache(&plan));
        // Execute the cached instance three times, resetting each time —
        // without reset_plan the second execution panics ("partition not
        // used yet").
        for _ in 0..3 {
            let fresh = reset_plan(plan.clone()).unwrap();
            let batches = datafusion::physical_plan::collect(fresh, state.task_ctx())
                .await
                .unwrap();
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            assert_eq!(rows, 2);
        }
    }
}
