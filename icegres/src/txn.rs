//! Explicit transactions on the wire (SPEC B4) and opt-in PK-enforced
//! INSERT (SPEC B5): BEGIN/COMMIT/ROLLBACK with real semantics over an
//! Iceberg lakehouse, without a write-ahead log or data replication.
//!
//! # Semantics (explicit and default-safe)
//!
//! * **Snapshot isolation, pinned per table at first touch.** The first
//!   statement in a transaction that reads or writes a table pins that
//!   table's current snapshot (the same machinery as time-travel reads,
//!   SPEC D4). All subsequent reads in the transaction see exactly that
//!   snapshot — concurrent committers are invisible until COMMIT/ROLLBACK
//!   (Postgres REPEATABLE READ pins at first statement, not at BEGIN; same
//!   model here, per table).
//! * **Read your own writes.** INSERT/UPDATE/DELETE inside the transaction
//!   are buffered in the session; reads resolve tables to the pinned
//!   snapshot overlaid with the buffered effects (a union with the append
//!   buffer, or a materialized effective-row state once the transaction has
//!   run UPDATE/DELETE — see "costs" below).
//! * **COMMIT = one Iceberg commit per table.** The buffered op list is
//!   applied in statement order and posted as a single snapshot with an
//!   `assert-ref-snapshot-id main=<pin>` requirement. If any other writer
//!   moved `main` since the pin, the catalog answers 409 and the whole
//!   transaction aborts with sqlstate 40001 (serialization_failure) —
//!   first-committer-wins, NO silent retry: statement-time row counts were
//!   computed against the pin and retrying against different data would
//!   make them lies.
//! * **ROLLBACK discards the buffer.** Nothing was written to the catalog
//!   before COMMIT, so rollback is trivially complete. A statement error
//!   inside a transaction aborts it (25P01 until COMMIT/ROLLBACK, exactly
//!   like Postgres; COMMIT after a failure rolls back and answers
//!   `ROLLBACK`).
//!
//! # Honest limitations
//!
//! * **Multi-table transactions are atomic when the catalog implements the
//!   Iceberg REST multi-table transaction endpoint**
//!   (`POST /v1/{prefix}/transactions/commit` — Lakekeeper does): the whole
//!   COMMIT becomes ONE all-or-nothing catalog request carrying every
//!   table's pins; a conflict is a clean 40001 with nothing applied. On a
//!   catalog WITHOUT the endpoint (probed once with a data-free request
//!   BEFORE any data file is staged, then cached), a transaction
//!   touching N tables falls back to N commits in deterministic (sorted)
//!   order after re-validating every pin. If commit k fails after k-1
//!   succeeded, the error (40003) says exactly which tables committed and
//!   which did not — no silent partial state. Single-table transactions
//!   (the common case) always use the per-table path and are fully atomic.
//! * **In-transaction SELECT is served on the simple query protocol**
//!   (psql, psycopg2). Extended-protocol SELECT inside a transaction is
//!   rejected loudly: the hook API cannot see the portal's requested result
//!   format, and answering a binary-format portal with text rows would be
//!   silent corruption — the one thing this codebase never does.
//! * **DDL and non-DML statements inside a transaction are rejected**
//!   (0A000), never half-applied.
//!
//! # Costs (bounded, documented)
//!
//! * Each table touched costs one `load_table` at pin time.
//! * A transaction that only INSERTs holds its rows in memory and unions
//!   them with the pinned snapshot for reads — no table materialization.
//! * The first UPDATE/DELETE *inside a transaction* materializes that
//!   table's effective rows in session memory (pinned rows + buffered
//!   appends) so subsequent reads and row counts are exact; peak memory is
//!   the table's decoded size. Autocommit UPDATE/DELETE (the overwhelmingly
//!   common path) does NOT do this — it streams file-by-file as before.
//!
//! # PK enforcement (SPEC B5, `--enforce-pk`)
//!
//! With `--enforce-pk` (env `ICEGRES_ENFORCE_PK=1`) and a table property
//! `icegres.primary-key=col[,col...]`, INSERTs (autocommit and buffered)
//! and PK-assigning UPDATEs are checked for NULL keys (23502) and duplicate
//! keys (23505) against the snapshot the commit anchors to; the COMMIT-time
//! check re-runs inside the anchored commit, so races either see each
//! other or 409. Off by default: enforcement reads the key columns of every
//! live data file per write.

use std::collections::HashMap;
use std::fmt::Debug;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex as StdMutex};

use anyhow::{anyhow, bail, Context as _, Result};
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef as ArrowSchemaRef;
use async_trait::async_trait;
use datafusion::catalog::{
    CatalogProvider, CatalogProviderList, SchemaProvider, Session, TableProvider,
};
use datafusion::common::ParamValues;
use datafusion::datasource::{MemTable, TableType};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::execution::SessionStateBuilder;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, LogicalPlan, TableProviderFilterPushDown, WriteOp};
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::{DataFrame, SessionContext};
use datafusion::sql::sqlparser::ast::{ObjectName, ObjectNamePart, Statement, TableObject};
use datafusion::sql::TableReference;
use datafusion_postgres::arrow_pg::datatypes::df as pgdf;
use datafusion_postgres::pgwire::api::portal::Format;
use datafusion_postgres::pgwire::api::results::{Response, Tag};
use datafusion_postgres::pgwire::api::ClientInfo;
use datafusion_postgres::pgwire::error::{ErrorInfo, PgWireError, PgWireResult};
use datafusion_postgres::pgwire::types::format::FormatOptions;
use datafusion_postgres::QueryHook;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::MAIN_BRANCH;
use iceberg::table::Table;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use iceberg_datafusion::IcebergStaticTableProvider;

use crate::context::{CATALOG_NAME, DEFAULT_SCHEMA};
use crate::dml;
use crate::overwrite::{
    align_batch, apply_dml_to_batches, check_pk, pk_columns_of, quote_ident, DmlKind,
    MultiTableCommit, OverwriteEngine, TableOp,
};

// ---------------------------------------------------------------------------
// Session state
// ---------------------------------------------------------------------------

/// Per-connection transaction sessions, keyed by peer address. The accept
/// loop (ops.rs) removes a connection's entry when its socket closes, so an
/// abandoned transaction can never leak or bleed into a later connection
/// that reuses the same peer address.
#[derive(Default)]
pub struct TxnRegistry {
    sessions: StdMutex<HashMap<SocketAddr, Arc<tokio::sync::Mutex<TxnSession>>>>,
    /// Count of open sessions, kept in step with `sessions` (only ever mutated
    /// while the map lock is held). Lets the per-statement `active`/`get`
    /// lookups skip the mutex entirely on the overwhelmingly common path where
    /// no transaction is open anywhere — the whole qps workload is autocommit
    /// reads, so this removes the map lock from every wire statement.
    open: AtomicUsize,
}

impl TxnRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    /// Whether `addr` has an open transaction (used by the write-buffer
    /// hook, which must not touch statements owned by this hook).
    pub fn active(&self, addr: SocketAddr) -> bool {
        if self.open.load(Ordering::Acquire) == 0 {
            return false;
        }
        self.sessions
            .lock()
            .expect("txn registry lock poisoned")
            .contains_key(&addr)
    }

    fn get(&self, addr: SocketAddr) -> Option<Arc<tokio::sync::Mutex<TxnSession>>> {
        if self.open.load(Ordering::Acquire) == 0 {
            return None;
        }
        self.sessions
            .lock()
            .expect("txn registry lock poisoned")
            .get(&addr)
            .cloned()
    }

    /// Insert a fresh session; answers `false` when one already exists
    /// (nested BEGIN).
    fn begin(&self, addr: SocketAddr) -> bool {
        let mut map = self.sessions.lock().expect("txn registry lock poisoned");
        if map.contains_key(&addr) {
            return false;
        }
        map.insert(addr, Arc::new(tokio::sync::Mutex::new(TxnSession::new())));
        self.open.store(map.len(), Ordering::Release);
        true
    }

    fn take(&self, addr: SocketAddr) -> Option<Arc<tokio::sync::Mutex<TxnSession>>> {
        let mut map = self.sessions.lock().expect("txn registry lock poisoned");
        let taken = map.remove(&addr);
        self.open.store(map.len(), Ordering::Release);
        taken
    }

    /// Connection closed: drop any open transaction (implicit rollback —
    /// nothing was committed, so nothing needs undoing).
    pub fn disconnect(&self, addr: &SocketAddr) {
        let mut map = self.sessions.lock().expect("txn registry lock poisoned");
        map.remove(addr);
        self.open.store(map.len(), Ordering::Release);
    }
}

/// One open transaction.
struct TxnSession {
    /// A statement failed: everything except COMMIT/ROLLBACK answers 25P01,
    /// and COMMIT rolls back.
    aborted: bool,
    tables: HashMap<TableIdent, TxnTable>,
}

impl TxnSession {
    fn new() -> Self {
        Self {
            aborted: false,
            tables: HashMap::new(),
        }
    }
}

/// Per-table transaction state: the pinned snapshot plus buffered ops.
struct TxnTable {
    /// Table as loaded at first touch.
    pinned: Table,
    /// The pinned snapshot id: the head of the serving branch (`--branch`;
    /// `main` by default) at first touch. COMMIT anchors its
    /// `assert-ref-snapshot-id` requirement here.
    pin_snapshot: Option<i64>,
    /// Read provider for the pinned snapshot (reused across statements).
    pinned_provider: Arc<IcebergStaticTableProvider>,
    /// Pinned Arrow schema (field-id annotated) — buffered rows are aligned
    /// to it so reads and the COMMIT writer see identical shapes.
    schema: ArrowSchemaRef,
    columns: Vec<String>,
    /// Buffered operations in statement order.
    ops: Vec<TableOp>,
    /// Effective rows (pin + ops applied), maintained eagerly once the
    /// transaction has run UPDATE/DELETE on this table. `None` while the
    /// transaction is append-only (reads use the union overlay instead).
    materialized: Option<Vec<RecordBatch>>,
}

impl TxnTable {
    /// Pin `ident` at the current head of `branch` (`main` = the table's
    /// current snapshot, identical to the historical behavior).
    async fn pin(catalog: &Arc<dyn Catalog>, ident: &TableIdent, branch: &str) -> Result<Self> {
        let table = catalog
            .load_table(ident)
            .await
            .map_err(|e| anyhow!("failed to load table {ident}: {e}"))?;
        let pin_snapshot = crate::overwrite::branch_head(table.metadata(), branch)
            .with_context(|| format!("cannot pin table {ident}"))?
            .map(|s| s.snapshot_id());
        let schema: ArrowSchemaRef = Arc::new(
            schema_to_arrow_schema(table.metadata().current_schema())
                .map_err(|e| anyhow!("schema conversion failed for {ident}: {e}"))?,
        );
        let columns = table
            .metadata()
            .current_schema()
            .as_struct()
            .fields()
            .iter()
            .map(|f| f.name.clone())
            .collect();
        let pinned_provider = match pin_snapshot {
            // Branch endpoint: the read view is the BRANCH head, not main's.
            Some(head) if branch != MAIN_BRANCH => Arc::new(
                IcebergStaticTableProvider::try_new_from_table_snapshot(table.clone(), head)
                    .await
                    .map_err(|e| {
                        anyhow!("failed to build pinned provider for {ident}@{head}: {e}")
                    })?,
            ),
            _ => Arc::new(
                IcebergStaticTableProvider::try_new_from_table(table.clone())
                    .await
                    .map_err(|e| anyhow!("failed to build pinned provider for {ident}: {e}"))?,
            ),
        };
        Ok(Self {
            pinned: table,
            pin_snapshot,
            pinned_provider,
            schema,
            columns,
            ops: Vec::new(),
            materialized: None,
        })
    }

    /// All buffered append batches (cheap Arc-sharing clones).
    fn append_batches(&self) -> Vec<RecordBatch> {
        self.ops
            .iter()
            .filter_map(|op| match op {
                TableOp::Append(b) => Some(b.iter().cloned()),
                // Pre-written files never occur in a transaction's buffered ops
                // (streaming ingest is an autocommit fast-append path).
                TableOp::Dml(_) | TableOp::AppendFiles(_) => None,
            })
            .flatten()
            .collect()
    }

    /// The provider transaction reads should see for this table right now.
    fn effective_provider(&self) -> Result<Arc<dyn TableProvider>> {
        if let Some(rows) = &self.materialized {
            let mem = MemTable::try_new(self.schema.clone(), vec![rows.clone()])
                .map_err(|e| anyhow!("failed to build effective-state table: {e}"))?;
            return Ok(Arc::new(mem));
        }
        let pinned: Arc<dyn TableProvider> = Arc::new(TunedProvider {
            inner: self.pinned_provider.clone(),
        });
        if self.ops.is_empty() {
            return Ok(pinned);
        }
        let appends = self.append_batches();
        let mem = MemTable::try_new(self.schema.clone(), vec![appends])
            .map_err(|e| anyhow!("failed to build append-buffer table: {e}"))?;
        Ok(Arc::new(UnionProvider {
            schema: self.schema.clone(),
            parts: vec![pinned, Arc::new(mem)],
        }))
    }

    /// Materialize effective rows (pinned snapshot + buffered appends).
    /// Only called before applying the first UPDATE/DELETE; afterwards the
    /// state is maintained eagerly by `txn_dml`.
    async fn materialize(&mut self) -> Result<()> {
        if self.materialized.is_some() {
            return Ok(());
        }
        debug_assert!(
            self.ops.iter().all(|op| matches!(op, TableOp::Append(_))),
            "materialize must run before the first DML op is buffered"
        );
        let ctx = SessionContext::new();
        ctx.register_table("__icegres_pin", self.pinned_provider.clone())
            .map_err(|e| anyhow!("failed to register pinned table: {e}"))?;
        let batches = ctx
            .sql("SELECT * FROM __icegres_pin")
            .await
            .map_err(|e| anyhow!("failed to plan pinned scan: {e}"))?
            .collect()
            .await
            .map_err(|e| anyhow!("failed to materialize pinned rows: {e}"))?;
        let mut rows: Vec<RecordBatch> = batches
            .iter()
            .map(|b| align_batch(b, &self.schema))
            .collect::<Result<_>>()?;
        rows.extend(self.append_batches());
        self.materialized = Some(rows);
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Read providers for the transaction view
// ---------------------------------------------------------------------------

/// Wraps the pinned static provider so its scans get the same IO-concurrency
/// tuning (scan.rs) as normal reads.
#[derive(Debug)]
struct TunedProvider {
    inner: Arc<IcebergStaticTableProvider>,
}

#[async_trait]
impl TableProvider for TunedProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn schema(&self) -> ArrowSchemaRef {
        self.inner.schema()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let plan = self.inner.scan(state, projection, filters, limit).await?;
        Ok(crate::scan::tune(plan).await)
    }
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

/// Union of the pinned snapshot and the in-memory append buffer: scans both
/// with the same projection and unions the streams. Filters are reported
/// Inexact, so DataFusion re-applies them above the union — correctness
/// never depends on either child's pushdown.
#[derive(Debug)]
struct UnionProvider {
    schema: ArrowSchemaRef,
    parts: Vec<Arc<dyn TableProvider>>,
}

#[async_trait]
impl TableProvider for UnionProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }
    fn table_type(&self) -> TableType {
        TableType::Base
    }
    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let mut children = Vec::with_capacity(self.parts.len());
        for part in &self.parts {
            // No per-child limit: a limit hint applied before the union
            // would be sound (union only concatenates), but passing None
            // keeps the semantics trivially obvious.
            children
                .push(crate::scan::tune(part.scan(state, projection, filters, None).await?).await);
        }
        UnionExec::try_new(children)
    }
    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }
}

// ---------------------------------------------------------------------------
// Transaction-scoped catalog wiring
// ---------------------------------------------------------------------------

/// Catalog list serving the shared catalogs, with the `icegres` catalog
/// wrapped so table lookups resolve through the transaction session
/// (pin-on-first-touch + buffered-op overlay).
struct TxnCatalogList {
    inner: Arc<dyn CatalogProviderList>,
    catalog: Arc<dyn Catalog>,
    /// Serving branch (`main` by default): tables pin at ITS head.
    branch: String,
    sess: Arc<tokio::sync::Mutex<TxnSession>>,
}

impl Debug for TxnCatalogList {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxnCatalogList").finish_non_exhaustive()
    }
}

impl CatalogProviderList for TxnCatalogList {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn register_catalog(
        &self,
        _name: String,
        _catalog: Arc<dyn CatalogProvider>,
    ) -> Option<Arc<dyn CatalogProvider>> {
        None // registration inside a transaction view is not supported
    }
    fn catalog_names(&self) -> Vec<String> {
        self.inner.catalog_names()
    }
    fn catalog(&self, name: &str) -> Option<Arc<dyn CatalogProvider>> {
        let inner = self.inner.catalog(name)?;
        if name == CATALOG_NAME {
            Some(Arc::new(TxnCatalogProvider {
                inner,
                catalog: self.catalog.clone(),
                branch: self.branch.clone(),
                sess: self.sess.clone(),
            }))
        } else {
            Some(inner)
        }
    }
}

struct TxnCatalogProvider {
    inner: Arc<dyn CatalogProvider>,
    catalog: Arc<dyn Catalog>,
    branch: String,
    sess: Arc<tokio::sync::Mutex<TxnSession>>,
}

impl Debug for TxnCatalogProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxnCatalogProvider").finish_non_exhaustive()
    }
}

impl CatalogProvider for TxnCatalogProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn schema_names(&self) -> Vec<String> {
        self.inner.schema_names()
    }
    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let inner = self.inner.schema(name)?;
        if name == "pg_catalog" || name == "information_schema" {
            return Some(inner);
        }
        Some(Arc::new(TxnSchemaProvider {
            inner,
            namespace: NamespaceIdent::new(name.to_string()),
            catalog: self.catalog.clone(),
            branch: self.branch.clone(),
            sess: self.sess.clone(),
        }))
    }
}

struct TxnSchemaProvider {
    inner: Arc<dyn SchemaProvider>,
    namespace: NamespaceIdent,
    catalog: Arc<dyn Catalog>,
    branch: String,
    sess: Arc<tokio::sync::Mutex<TxnSession>>,
}

impl Debug for TxnSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxnSchemaProvider")
            .field("namespace", &self.namespace)
            .finish_non_exhaustive()
    }
}

#[async_trait]
impl SchemaProvider for TxnSchemaProvider {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn table_names(&self) -> Vec<String> {
        self.inner.table_names()
    }
    fn table_exist(&self, name: &str) -> bool {
        self.inner.table_exist(name)
    }
    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        // Metadata tables and explicit time-travel refs stay point-in-time
        // by construction — pass through.
        if name.contains('$') || name.contains('@') {
            return self.inner.table(name).await;
        }
        if !self.inner.table_exist(name) {
            return Ok(None);
        }
        let ident = TableIdent::new(self.namespace.clone(), name.to_string());
        let mut sess = self.sess.lock().await;
        if !sess.tables.contains_key(&ident) {
            let pinned = TxnTable::pin(&self.catalog, &ident, &self.branch)
                .await
                .map_err(|e| DataFusionError::External(e.into()))?;
            sess.tables.insert(ident.clone(), pinned);
        }
        let entry = sess.tables.get(&ident).expect("just inserted");
        let provider = entry
            .effective_provider()
            .map_err(|e| DataFusionError::External(e.into()))?;
        Ok(Some(provider))
    }
    fn register_table(
        &self,
        _name: String,
        _table: Arc<dyn TableProvider>,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        Err(DataFusionError::Plan(
            "creating tables inside a transaction block is not supported".to_string(),
        ))
    }
    fn deregister_table(&self, _name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        Err(DataFusionError::Plan(
            "dropping tables inside a transaction block is not supported".to_string(),
        ))
    }
}

// ---------------------------------------------------------------------------
// The hook
// ---------------------------------------------------------------------------

/// Query hook implementing BEGIN/COMMIT/ROLLBACK (B4) and PK-enforced
/// INSERT (B5). Must be FIRST in the hook chain: when a transaction is
/// active it owns every statement on the connection.
pub struct TxnHook {
    registry: Arc<TxnRegistry>,
    engine: Arc<OverwriteEngine>,
    catalog: Arc<dyn Catalog>,
    /// Buffered-write mode (`--write-buffer-ms`), when enabled: COMMIT
    /// serializes against in-flight keyed read-modify-writes on every
    /// keyed-activated touched table via the buffer's per-table
    /// keyed-serial locks (buffer.rs, fix L1) — without this, a keyed
    /// statement's union read could predate this COMMIT and its stale
    /// full-row image would clobber the committed change at flush time.
    buffer: Option<Arc<crate::buffer::WriteBuffer>>,
    /// When true, a multi-table COMMIT that cannot be applied atomically
    /// (the catalog lacks the `transactions/commit` endpoint) is refused
    /// (nothing applied) instead of being applied best-effort per-table.
    /// Set via `ICEGRES_TXN_STRICT=true`. On a catalog WITH the endpoint
    /// strict mode never refuses: atomicity is provided by the single
    /// all-or-nothing catalog transaction.
    strict: bool,
}

impl TxnHook {
    pub fn new(
        registry: Arc<TxnRegistry>,
        engine: Arc<OverwriteEngine>,
        catalog: Arc<dyn Catalog>,
        buffer: Option<Arc<crate::buffer::WriteBuffer>>,
    ) -> Self {
        Self {
            registry,
            engine,
            catalog,
            buffer,
            strict: strict_txn_mode(),
        }
    }

    /// A transaction-scoped session context: same UDFs/config/optimizer as
    /// the shared context, but table resolution goes through the session.
    fn txn_ctx(
        &self,
        shared: &SessionContext,
        sess: Arc<tokio::sync::Mutex<TxnSession>>,
    ) -> SessionContext {
        let state = shared.state();
        let list = Arc::new(TxnCatalogList {
            inner: state.catalog_list().clone(),
            catalog: self.catalog.clone(),
            branch: self.engine.branch().to_string(),
            sess,
        });
        SessionContext::new_with_state(
            SessionStateBuilder::new_from_existing(state)
                .with_catalog_list(list)
                .build(),
        )
    }

    fn begin(&self, addr: SocketAddr) -> PgWireResult<Response> {
        if self.registry.begin(addr) {
            Ok(Response::TransactionStart(Tag::new("BEGIN")))
        } else {
            // Postgres behavior: warn and stay in the current transaction.
            tracing::warn!(peer = %addr, "BEGIN inside a transaction block — ignored");
            Ok(Response::Execution(Tag::new("BEGIN")))
        }
    }

    async fn rollback(&self, addr: SocketAddr) -> PgWireResult<Response> {
        let _ = self.registry.take(addr);
        Ok(Response::TransactionEnd(Tag::new("ROLLBACK")))
    }

    async fn commit(&self, addr: SocketAddr) -> PgWireResult<Response> {
        let Some(sess_arc) = self.registry.take(addr) else {
            // COMMIT with no open transaction: Postgres warns and answers
            // COMMIT; there is nothing to apply.
            return Ok(Response::TransactionEnd(Tag::new("COMMIT")));
        };
        let sess = sess_arc.lock().await;
        if sess.aborted {
            // Failed transaction: COMMIT rolls back (Postgres semantics).
            return Ok(Response::TransactionEnd(Tag::new("ROLLBACK")));
        }
        // Deterministic per-table order; skip untouched/no-op tables.
        let mut idents: Vec<&TableIdent> = sess
            .tables
            .iter()
            .filter(|(_, t)| !t.ops.is_empty())
            .map(|(ident, _)| ident)
            .collect();
        idents.sort_by_key(|i| i.to_string());

        // L1(c): serialize this COMMIT against in-flight keyed
        // read-modify-writes on every KEYED-ACTIVATED touched table, held
        // across the whole commit. Activation comes from the PIN-TIME
        // metadata (zero extra catalog calls; a property change since the
        // pin moves the metadata and conflicts the pin's CAS anyway).
        // Locks are acquired in the already-sorted ident order, so
        // concurrent multi-table COMMITs cannot deadlock; non-activated
        // tables acquire nothing.
        let mut _keyed_serial_guards = Vec::new();
        if let Some(buffer) = &self.buffer {
            let keyed_locks: Vec<_> = idents
                .iter()
                .filter(|ident| {
                    crate::keyed::property_is_true(
                        sess.tables[**ident]
                            .pinned
                            .metadata()
                            .properties()
                            .get(crate::keyed::TAIL_UPSERT_PROPERTY),
                    )
                })
                .map(|ident| buffer.keyed_serial_lock(ident))
                .collect();
            for lock in keyed_locks {
                _keyed_serial_guards.push(lock.lock_owned().await);
            }
        }

        // Multi-table transaction: try ONE atomic all-or-nothing catalog
        // commit (POST /v1/{prefix}/transactions/commit, Iceberg REST spec;
        // Lakekeeper implements it). Per table it carries exactly the
        // requirements/updates the per-table path would post, so semantics
        // are unchanged except across tables: every table commits or none
        // does — a conflict is a clean 40001 with nothing applied, and the
        // 40003 partial-apply outcome below becomes unreachable. Single-
        // table transactions never take this path (byte-identical behavior).
        if idents.len() > 1 {
            let batch: Vec<(&TableIdent, Option<i64>, &[TableOp])> = idents
                .iter()
                .map(|ident| {
                    let t = &sess.tables[*ident];
                    (*ident, t.pin_snapshot, t.ops.as_slice())
                })
                .collect();
            match self.engine.commit_pinned_multi(&batch).await {
                Ok(MultiTableCommit::Committed) => {
                    return Ok(Response::TransactionEnd(Tag::new("COMMIT")));
                }
                Ok(MultiTableCommit::Unsupported) => {
                    // Catalog lacks the endpoint. The capability was
                    // resolved by a DATA-FREE probe before any table was
                    // prepared, so nothing was applied AND nothing was
                    // staged: strict mode refuses below having touched
                    // nothing; otherwise fall through to the documented
                    // ordered per-table path, which stages each table
                    // exactly once (no double staging).
                }
                Err(e) => {
                    // All-or-nothing: NOTHING was applied. Preserve the
                    // underlying sqlstate (40001 serialization_failure for
                    // conflicts) — retrying the whole transaction is safe.
                    let base = dml::engine_error(&e);
                    let msg = format!(
                        "COMMIT failed, transaction rolled back (no changes were \
                         applied): {}",
                        err_message(&base)
                    );
                    return Err(with_message(base, msg));
                }
            }

            // Strict mode: without the multi-table transaction endpoint a
            // COMMIT spanning >1 table cannot be applied atomically. Rather
            // than risk a partial apply, refuse before touching anything —
            // literally: the missing capability was learned from the
            // data-free probe, so no data file was written and no catalog
            // state changed. The whole transaction rolls back with a clear
            // feature_not_supported (0A000) error.
            if self.strict {
                let names: Vec<String> = idents.iter().map(|i| i.to_string()).collect();
                return Err(user_err(
                    "0A000",
                    &format!(
                        "strict transaction mode (ICEGRES_TXN_STRICT): COMMIT touches {} \
                         tables [{}] but this catalog does not implement the multi-table \
                         transaction endpoint (POST /v1/{{prefix}}/transactions/commit), so \
                         the COMMIT cannot be applied atomically across tables; transaction \
                         rolled back (nothing applied). Commit one table per transaction, \
                         use a catalog with multi-table transactions (e.g. Lakekeeper), or \
                         unset ICEGRES_TXN_STRICT to allow best-effort ordered multi-table \
                         commits.",
                        idents.len(),
                        names.join(", ")
                    ),
                ));
            }
        }

        let mut committed: Vec<String> = Vec::new();
        for (k, ident) in idents.iter().enumerate() {
            let t = &sess.tables[*ident];
            match self
                .engine
                .commit_pinned(ident, t.pin_snapshot, &t.ops)
                .await
            {
                Ok(_) => committed.push(ident.to_string()),
                Err(e) => {
                    let remaining: Vec<String> =
                        idents[k + 1..].iter().map(|i| i.to_string()).collect();
                    let base = dml::engine_error(&e);
                    if committed.is_empty() {
                        // Nothing applied yet: a true rollback. Preserve the
                        // underlying sqlstate (e.g. 40001 serialization_failure)
                        // so retry logic keyed on it still works — retrying is
                        // safe because no table changed.
                        let msg = format!(
                            "COMMIT failed, transaction rolled back (no changes were \
                             applied): {}",
                            err_message(&base)
                        );
                        return Err(with_message(base, msg));
                    }
                    // Some tables committed, then one failed: the outcome is
                    // NOT a rollback. Report SQLSTATE 40003
                    // (statement_completion_unknown) so a client does not
                    // blindly retry the whole COMMIT — retrying would re-apply
                    // the already-committed tables. The client must reconcile
                    // per-table state instead.
                    let msg = format!(
                        "COMMIT PARTIALLY APPLIED: table(s) [{}] committed before table \
                         {ident} failed: {}; table(s) [{}] were NOT committed. Iceberg \
                         REST commits are per-table — multi-table transactions are \
                         best-effort ordered (see icegres docs). Do NOT blindly retry \
                         this COMMIT; reconcile per-table state, or set \
                         ICEGRES_TXN_STRICT=true to refuse non-atomic multi-table commits \
                         up front.",
                        committed.join(", "),
                        err_message(&base),
                        remaining.join(", ")
                    );
                    return Err(user_err("40003", &msg));
                }
            }
        }
        Ok(Response::TransactionEnd(Tag::new("COMMIT")))
    }

    /// INSERT inside a transaction: evaluate its source against the
    /// transaction view (read-your-own-writes for INSERT..SELECT), buffer
    /// the rows, answer `INSERT 0 n`.
    async fn txn_insert(
        &self,
        stmt: &Statement,
        shared: &SessionContext,
        sess_arc: Arc<tokio::sync::Mutex<TxnSession>>,
        params: Option<&ParamValues>,
    ) -> Result<Response> {
        let ctx = self.txn_ctx(shared, sess_arc.clone());
        let (ident, batches) = plan_insert_rows(&ctx, stmt, params).await?;
        // Planning pinned the target table (provider resolution); align the
        // rows to its pinned schema so every later consumer agrees.
        let mut sess = sess_arc.lock().await;
        let entry = sess
            .tables
            .get_mut(&ident)
            .ok_or_else(|| anyhow!("INSERT target {ident} was not pinned during planning"))?;
        let aligned: Vec<RecordBatch> = batches
            .iter()
            .map(|b| align_batch(b, &entry.schema))
            .collect::<Result<_>>()?;
        let rows: usize = aligned.iter().map(|b| b.num_rows()).sum();

        // Statement-time PK feedback (the COMMIT-time check inside the
        // anchored commit is the authoritative one).
        if let Some(pk_cols) = self.engine.pk_columns(&entry.pinned)? {
            check_insert_pk(entry, &aligned, &pk_cols).await?;
        }

        if let Some(m) = entry.materialized.as_mut() {
            m.extend(aligned.iter().cloned());
        }
        entry.ops.push(TableOp::Append(aligned));
        Ok(Response::Execution(
            Tag::new("INSERT").with_oid(0).with_rows(rows),
        ))
    }

    /// UPDATE/DELETE inside a transaction: apply to the materialized
    /// effective state (exact row counts, validates the statement fully),
    /// buffer the op for COMMIT.
    async fn txn_dml(
        &self,
        stmt: &Statement,
        sess_arc: Arc<tokio::sync::Mutex<TxnSession>>,
    ) -> Result<Response> {
        let (dml_stmt, tag) =
            dml::translate(stmt)?.ok_or_else(|| anyhow!("statement is not UPDATE/DELETE"))?;
        let ident = TableIdent::from_strs([dml_stmt.namespace.as_str(), dml_stmt.table.as_str()])
            .map_err(|e| anyhow!("bad table identifier: {e}"))?;
        let mut sess = sess_arc.lock().await;
        if !sess.tables.contains_key(&ident) {
            let pinned = TxnTable::pin(&self.catalog, &ident, self.engine.branch()).await?;
            sess.tables.insert(ident.clone(), pinned);
        }
        let entry = sess.tables.get_mut(&ident).expect("just pinned");
        entry.materialize().await?;
        let rows_in = entry
            .materialized
            .as_ref()
            .expect("materialized above")
            .clone();
        let (matched, out) = apply_dml_to_batches(&dml_stmt, &entry.columns, rows_in).await?;
        let aligned: Vec<RecordBatch> = out
            .iter()
            .map(|b| align_batch(b, &entry.schema))
            .collect::<Result<_>>()?;

        // PK feedback when an UPDATE rewrites key columns: the effective
        // state holds EVERY row of the table, so this check is global.
        if let DmlKind::Update { assignments } = &dml_stmt.kind {
            if let Some(pk_cols) = self.engine.pk_columns(&entry.pinned)? {
                let touches_pk = assignments
                    .iter()
                    .any(|(c, _)| pk_cols.iter().any(|p| p == c));
                if touches_pk && !aligned.is_empty() {
                    let keys = project_pk(&aligned, &pk_cols)?;
                    check_pk(&pk_cols, &keys, ident.name()).await?;
                }
            }
        }

        entry.materialized = Some(aligned);
        entry.ops.push(TableOp::Dml(dml_stmt));
        Ok(Response::Execution(
            Tag::new(tag).with_rows(matched as usize),
        ))
    }

    /// SELECT (or EXPLAIN) inside a transaction, served from the
    /// transaction view. Simple protocol only (text results).
    ///
    /// The result is collected EAGERLY: `encode_dataframe` otherwise defers
    /// execution into the response stream, where a runtime error (e.g.
    /// divide by zero) would surface only while pgwire streams rows — after
    /// this hook returned Ok — and the session would miss its
    /// transaction-aborting failure. Result-set memory is bounded by what
    /// the client asked for.
    async fn txn_select(
        &self,
        stmt: &Statement,
        shared: &SessionContext,
        sess_arc: Arc<tokio::sync::Mutex<TxnSession>>,
        client: &(dyn ClientInfo + Send + Sync),
    ) -> PgWireResult<Response> {
        let ctx = self.txn_ctx(shared, sess_arc);
        let df = ctx
            .sql(&stmt.to_string())
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        let arrow_schema = Arc::new(df.schema().as_arrow().clone());
        let mut batches = df
            .collect()
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        if batches.is_empty() {
            // Zero-batch result: keep the schema so RowDescription is right.
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

    /// Autocommit INSERT under `--enforce-pk` and/or `--branch`: routed
    /// through the engine's anchored-commit path when the target declares a
    /// PK (check-then-commit, re-validated on 409 retry) or when this server
    /// serves a non-`main` branch (the stock fast_append path would commit
    /// to `main`). Otherwise answers `None` so the default handler runs.
    ///
    /// `ICEGRES_QUERY_TIMING=1` additionally routes PLAIN autocommit INSERTs
    /// (no PK, `main` branch, no write buffer) through this path: the stock
    /// fast_append commit runs fused inside iceberg-datafusion's execution
    /// plan and cannot be timed per stage, while this path produces an
    /// equivalent append snapshot through prepare_commit/post_commit, whose
    /// stages ARE recorded (timing.rs module docs — same diagnostic-mode
    /// divergence contract as the read-path TimingHook). Zero cost when
    /// unset: [`crate::timing::enabled`] is one cached bool load.
    async fn autocommit_insert(
        &self,
        stmt: &Statement,
        shared: &SessionContext,
        params: Option<&ParamValues>,
    ) -> Option<PgWireResult<Response>> {
        let ident = match insert_target(stmt) {
            Ok(ident) => ident,
            Err(e) => return Some(Err(user_err("0A000", &format!("{e:#}")))),
        };
        let table = match self.catalog.load_table(&ident).await {
            Ok(t) => t,
            // Unknown table etc.: let the default path produce its usual
            // error (in branch mode it fails on the same missing table, so
            // nothing can leak onto main).
            Err(_) => return None,
        };
        match pk_columns_of(&table) {
            Ok(Some(_)) => {}
            // No PK declared: on main the stock fast_append path is fine;
            // on a branch every INSERT must still go through the engine.
            // Diagnostic timing mode keeps the engine path so the sync
            // commit budget is observable (see the method docs).
            Ok(None) if self.engine.is_main_branch() && !crate::timing::enabled() => return None,
            Ok(None) => {}
            Err(e) => return Some(Err(user_err("XX000", &format!("{e:#}")))),
        }
        let result: Result<Response> = async {
            let plan_started = crate::timing::enabled().then(std::time::Instant::now);
            let (plan_ident, batches) = plan_insert_rows(shared, stmt, params).await?;
            if let Some(t) = plan_started {
                crate::timing::record("insert_plan", t.elapsed());
            }
            anyhow::ensure!(
                plan_ident == ident,
                "INSERT target resolution mismatch: {plan_ident} vs {ident}"
            );
            let target: ArrowSchemaRef = Arc::new(
                schema_to_arrow_schema(table.metadata().current_schema())
                    .map_err(|e| anyhow!("schema conversion failed: {e}"))?,
            );
            let aligned: Vec<RecordBatch> = batches
                .iter()
                .map(|b| align_batch(b, &target))
                .collect::<Result<_>>()?;
            // The table loaded above (for the routing decision + schema)
            // anchors attempt 1 — killing the redundant per-statement
            // double `load_table`; retries still reload fresh metadata.
            let outcome = self
                .engine
                .insert_enforced(&ident, aligned, Some(table))
                .await?;
            Ok(Response::Execution(
                Tag::new("INSERT")
                    .with_oid(0)
                    .with_rows(outcome.rows as usize),
            ))
        }
        .await;
        Some(result.map_err(|e| dml::engine_error(&e)))
    }

    /// Dispatch a statement while a transaction is active. `Err` marks the
    /// session aborted (caller responsibility handled here).
    async fn in_txn(
        &self,
        stmt: &Statement,
        shared: &SessionContext,
        sess_arc: Arc<tokio::sync::Mutex<TxnSession>>,
        client: &(dyn ClientInfo + Send + Sync),
        params: Option<&ParamValues>,
        extended: bool,
    ) -> Option<PgWireResult<Response>> {
        // SET/SHOW pass through to the SetShow hook (session settings are
        // not transactional here — documented deviation, matching upstream).
        if matches!(
            stmt,
            Statement::Set { .. } | Statement::ShowVariable { .. } | Statement::ShowStatus { .. }
        ) {
            return None;
        }
        let result: PgWireResult<Response> = match stmt {
            Statement::Insert(_) => self
                .txn_insert(stmt, shared, sess_arc.clone(), params)
                .await
                .map_err(|e| dml::engine_error(&e)),
            Statement::Update { .. } | Statement::Delete(_) => {
                let has_params = params.is_some_and(|p| match p {
                    ParamValues::List(l) => !l.is_empty(),
                    ParamValues::Map(m) => !m.is_empty(),
                });
                if has_params {
                    Err(user_err(
                        "0A000",
                        "parameterized UPDATE/DELETE ($n bind values) is not supported yet; \
                         inline the values",
                    ))
                } else {
                    self.txn_dml(stmt, sess_arc.clone())
                        .await
                        .map_err(|e| dml::engine_error(&e))
                }
            }
            Statement::Query(_) | Statement::Explain { .. } => {
                if extended {
                    Err(user_err(
                        "0A000",
                        "SELECT inside an explicit transaction is supported on the simple \
                         query protocol only (the extended-protocol hook cannot see the \
                         portal's result format, and answering a binary portal with text \
                         would corrupt it)",
                    ))
                } else {
                    self.txn_select(stmt, shared, sess_arc.clone(), client)
                        .await
                }
            }
            other => Err(user_err(
                "0A000",
                &format!(
                    "statement is not supported inside a transaction block: {}",
                    statement_kind(other)
                ),
            )),
        };
        if result.is_err() {
            sess_arc.lock().await.aborted = true;
        }
        Some(result)
    }
}

impl TxnHook {
    /// Belt-and-braces: if pgwire marked the connection's transaction as
    /// failed through an error path this hook never saw (e.g. an error
    /// surfacing while rows were being streamed to the client), propagate
    /// that into the session so 25P02 blocking and COMMIT-rolls-back apply.
    async fn sync_aborted_from_wire(
        &self,
        addr: SocketAddr,
        client: &(dyn ClientInfo + Send + Sync),
    ) {
        use datafusion_postgres::pgwire::messages::response::TransactionStatus;
        if client.transaction_status() == TransactionStatus::Error {
            if let Some(sess) = self.registry.get(addr) {
                sess.lock().await.aborted = true;
            }
        }
    }
}

#[async_trait]
impl QueryHook for TxnHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        let addr = client.socket_addr();
        self.sync_aborted_from_wire(addr, client).await;
        match statement {
            Statement::StartTransaction { .. } => {
                if let Some(sess) = self.registry.get(addr) {
                    if sess.lock().await.aborted {
                        return Some(Err(aborted_err()));
                    }
                }
                Some(self.begin(addr))
            }
            Statement::Commit { .. } => Some(self.commit(addr).await),
            Statement::Rollback { .. } => Some(self.rollback(addr).await),
            _ => match self.registry.get(addr) {
                Some(sess_arc) => {
                    if sess_arc.lock().await.aborted {
                        return Some(Err(aborted_err()));
                    }
                    self.in_txn(statement, session_context, sess_arc, client, None, false)
                        .await
                }
                None => {
                    // ICEGRES_QUERY_TIMING routes plain INSERTs through the
                    // engine too, so the write budget is observable
                    // (autocommit_insert docs); one cached bool when unset.
                    if (self.engine.enforce_pk()
                        || !self.engine.is_main_branch()
                        || crate::timing::enabled())
                        && matches!(statement, Statement::Insert(_))
                    {
                        self.autocommit_insert(statement, session_context, None)
                            .await
                    } else {
                        None
                    }
                }
            },
        }
    }

    async fn handle_extended_parse_query(
        &self,
        sql: &Statement,
        _session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        if matches!(
            sql,
            Statement::StartTransaction { .. }
                | Statement::Commit { .. }
                | Statement::Rollback { .. }
        ) {
            // Dummy plan; execution is handled by handle_extended_query.
            return Some(Ok(LogicalPlan::EmptyRelation(
                datafusion::logical_expr::EmptyRelation {
                    produce_one_row: false,
                    schema: Arc::new(datafusion::common::DFSchema::empty()),
                },
            )));
        }
        None
    }

    async fn handle_extended_query(
        &self,
        statement: &Statement,
        _logical_plan: &LogicalPlan,
        params: &ParamValues,
        session_context: &SessionContext,
        client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        let addr = client.socket_addr();
        self.sync_aborted_from_wire(addr, client).await;
        match statement {
            Statement::StartTransaction { .. } => {
                if let Some(sess) = self.registry.get(addr) {
                    if sess.lock().await.aborted {
                        return Some(Err(aborted_err()));
                    }
                }
                Some(self.begin(addr))
            }
            Statement::Commit { .. } => Some(self.commit(addr).await),
            Statement::Rollback { .. } => Some(self.rollback(addr).await),
            _ => match self.registry.get(addr) {
                Some(sess_arc) => {
                    if sess_arc.lock().await.aborted {
                        return Some(Err(aborted_err()));
                    }
                    self.in_txn(
                        statement,
                        session_context,
                        sess_arc,
                        client,
                        Some(params),
                        true,
                    )
                    .await
                }
                None => {
                    // Same timing-mode routing as handle_simple_query.
                    if (self.engine.enforce_pk()
                        || !self.engine.is_main_branch()
                        || crate::timing::enabled())
                        && matches!(statement, Statement::Insert(_))
                    {
                        self.autocommit_insert(statement, session_context, Some(params))
                            .await
                    } else {
                        None
                    }
                }
            },
        }
    }
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Plan an INSERT through DataFusion (column-list reordering, NULL-filling
/// of omitted nullable columns, type coercion — identical rules to the
/// stock INSERT path) and execute ONLY its input, returning the target
/// table plus the rows to append. Also used by the write-buffer hook
/// (buffer.rs) so buffered and synchronous INSERTs shape rows identically.
pub(crate) async fn plan_insert_rows(
    ctx: &SessionContext,
    stmt: &Statement,
    params: Option<&ParamValues>,
) -> Result<(TableIdent, Vec<RecordBatch>)> {
    let df_stmt = datafusion::sql::parser::Statement::Statement(Box::new(stmt.clone()));
    let mut plan = ctx
        .state()
        .statement_to_plan(df_stmt)
        .await
        .map_err(|e| anyhow!("failed to plan INSERT: {e}"))?;
    if let Some(p) = params {
        plan = plan
            .replace_params_with_values(p)
            .map_err(|e| anyhow!("failed to bind INSERT parameters: {e}"))?;
    }
    let LogicalPlan::Dml(dml_plan) = plan else {
        bail!("unexpected logical plan for INSERT statement");
    };
    match &dml_plan.op {
        WriteOp::Insert(InsertOp::Append) => {}
        other => bail!("INSERT mode {other} is not supported"),
    }
    let ident = table_ref_to_ident(&dml_plan.table_name)?;
    let df = DataFrame::new(ctx.state(), dml_plan.input.as_ref().clone());
    let batches = df
        .collect()
        .await
        .map_err(|e| anyhow!("failed to evaluate INSERT rows: {e}"))?;
    Ok((ident, batches))
}

/// Resolve a planner table reference to an Iceberg table identity, applying
/// the session's default catalog/schema.
fn table_ref_to_ident(table_ref: &TableReference) -> Result<TableIdent> {
    if let Some(catalog) = table_ref.catalog() {
        anyhow::ensure!(
            catalog == CATALOG_NAME,
            "unknown catalog {catalog:?} (only {CATALOG_NAME:?} is served)"
        );
    }
    let schema = table_ref.schema().unwrap_or(DEFAULT_SCHEMA);
    TableIdent::from_strs([schema, table_ref.table()])
        .map_err(|e| anyhow!("bad table identifier: {e}"))
}

/// Extract the target table identity from an INSERT AST (Postgres identifier
/// folding, default namespace).
pub(crate) fn insert_target(stmt: &Statement) -> Result<TableIdent> {
    let Statement::Insert(insert) = stmt else {
        bail!("not an INSERT statement");
    };
    let TableObject::TableName(name) = &insert.table else {
        bail!("INSERT into a table function is not supported");
    };
    object_name_to_ident(name)
}

fn object_name_to_ident(name: &ObjectName) -> Result<TableIdent> {
    let mut parts: Vec<String> = Vec::new();
    for part in &name.0 {
        let ObjectNamePart::Identifier(ident) = part else {
            bail!("unsupported table name part in {name}");
        };
        parts.push(if ident.quote_style.is_some() {
            ident.value.clone()
        } else {
            ident.value.to_lowercase()
        });
    }
    let (namespace, table) = match parts.len() {
        1 => (DEFAULT_SCHEMA.to_string(), parts.pop().expect("len 1")),
        2 => {
            let t = parts.pop().expect("len 2");
            (parts.pop().expect("len 2"), t)
        }
        3 => {
            anyhow::ensure!(
                parts[0] == CATALOG_NAME,
                "unknown catalog {:?} (only {CATALOG_NAME:?} is served)",
                parts[0]
            );
            let t = parts.pop().expect("len 3");
            (parts.pop().expect("len 3"), t)
        }
        n => bail!("table name with {n} parts is not supported"),
    };
    TableIdent::from_strs([namespace.as_str(), table.as_str()])
        .map_err(|e| anyhow!("bad table identifier: {e}"))
}

/// Statement-time PK check for a buffered INSERT: new keys must be non-NULL,
/// unique among themselves, and absent from the transaction's effective view
/// of the table.
async fn check_insert_pk(
    entry: &TxnTable,
    new_rows: &[RecordBatch],
    pk_cols: &[String],
) -> Result<()> {
    // NULLs + duplicates WITHIN the new rows.
    let new_keys = project_pk(new_rows, pk_cols)?;
    check_pk(pk_cols, &new_keys, entry.pinned.identifier().name()).await?;

    // Collision with the effective (pin + buffer) view: key-columns-only
    // anti-join through DataFusion, so the pinned side reads only the key
    // columns from Parquet.
    let ctx = SessionContext::new();
    ctx.register_table("__icegres_cur", entry.effective_provider()?)
        .map_err(|e| anyhow!("failed to register effective table: {e}"))?;
    let mem = MemTable::try_new(new_keys[0].schema(), vec![new_keys.clone()])
        .map_err(|e| anyhow!("failed to build new-keys table: {e}"))?;
    ctx.register_table("__icegres_new", Arc::new(mem))
        .map_err(|e| anyhow!("failed to register new-keys table: {e}"))?;
    let join = pk_cols
        .iter()
        .map(|c| format!("c.{col} = n.{col}", col = quote_ident(c)))
        .collect::<Vec<_>>()
        .join(" AND ");
    let sql = format!("SELECT count(*) FROM __icegres_new n JOIN __icegres_cur c ON {join}");
    let batches = ctx
        .sql(&sql)
        .await
        .map_err(|e| anyhow!("failed to plan PK collision check: {e}"))?
        .collect()
        .await
        .map_err(|e| anyhow!("failed to run PK collision check: {e}"))?;
    let collisions = batches
        .first()
        .map(|b| {
            use arrow::array::AsArray;
            use arrow::datatypes::Int64Type;
            b.column(0).as_primitive::<Int64Type>().value(0)
        })
        .unwrap_or(0);
    if collisions > 0 {
        return Err(anyhow!(crate::overwrite::ConstraintViolation {
            sqlstate: "23505",
            message: format!(
                "duplicate key value violates unique constraint \"{}_pkey\": {collisions} \
                 inserted row(s) collide with existing key(s) ({})",
                entry.pinned.identifier().name(),
                pk_cols.join(", ")
            ),
        }));
    }
    Ok(())
}

/// Project key columns (by name) out of row batches, one concatenated batch.
fn project_pk(batches: &[RecordBatch], pk_cols: &[String]) -> Result<Vec<RecordBatch>> {
    let nonempty: Vec<&RecordBatch> = batches.iter().filter(|b| b.num_rows() > 0).collect();
    let Some(first) = nonempty.first() else {
        return Ok(Vec::new());
    };
    let indices: Vec<usize> = pk_cols
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
    nonempty
        .iter()
        .map(|b| {
            b.project(&indices)
                .map_err(|e| anyhow!("PK projection failed: {e}"))
        })
        .collect()
}

fn user_err(code: &str, msg: &str) -> PgWireError {
    PgWireError::UserError(Box::new(ErrorInfo::new(
        "ERROR".to_string(),
        code.to_string(),
        msg.to_string(),
    )))
}

/// Whether strict transaction mode is enabled (`ICEGRES_TXN_STRICT` truthy).
/// In strict mode a COMMIT spanning more than one table is refused (before
/// any table is written) unless the catalog can apply it atomically via the
/// multi-table transaction endpoint — every COMMIT stays all-or-nothing.
fn strict_txn_mode() -> bool {
    matches!(
        std::env::var("ICEGRES_TXN_STRICT").as_deref(),
        Ok("1") | Ok("true") | Ok("TRUE") | Ok("yes") | Ok("on")
    )
}

fn aborted_err() -> PgWireError {
    user_err(
        "25P02",
        "current transaction is aborted, commands ignored until end of transaction block",
    )
}

/// Extract the human message out of a UserError (for recomposition).
fn err_message(e: &PgWireError) -> String {
    match e {
        PgWireError::UserError(info) => info.message.clone(),
        other => other.to_string(),
    }
}

/// Rebuild a UserError with the same sqlstate but a new message.
fn with_message(e: PgWireError, msg: String) -> PgWireError {
    let code = match &e {
        PgWireError::UserError(info) => info.code.clone(),
        _ => "XX000".to_string(),
    };
    user_err(&code, &msg)
}

/// Terse statement-kind label for error messages (never echoes user data).
fn statement_kind(stmt: &Statement) -> &'static str {
    match stmt {
        Statement::CreateTable { .. } => "CREATE TABLE",
        Statement::Drop { .. } => "DROP",
        Statement::AlterTable { .. } => "ALTER TABLE",
        Statement::CreateView { .. } => "CREATE VIEW",
        Statement::Copy { .. } => "COPY",
        Statement::Truncate { .. } => "TRUNCATE",
        _ => "this statement type",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
    use datafusion::sql::sqlparser::parser::Parser;

    fn parse(sql: &str) -> Statement {
        Parser::parse_sql(&PostgreSqlDialect {}, sql)
            .unwrap()
            .remove(0)
    }

    #[test]
    fn insert_target_folds_and_defaults_namespace() {
        let ident = insert_target(&parse("INSERT INTO Trips VALUES (1)")).unwrap();
        assert_eq!(ident.namespace().to_url_string(), DEFAULT_SCHEMA);
        assert_eq!(ident.name(), "trips");

        let ident = insert_target(&parse("INSERT INTO demo.\"MyTable\" VALUES (1)")).unwrap();
        assert_eq!(ident.name(), "MyTable");
    }

    #[test]
    fn insert_target_rejects_wrong_catalog() {
        let err = insert_target(&parse("INSERT INTO other.demo.trips VALUES (1)")).unwrap_err();
        assert!(err.to_string().contains("unknown catalog"));
    }

    #[test]
    fn registry_begin_take_disconnect() {
        let reg = TxnRegistry::new();
        let addr: SocketAddr = "127.0.0.1:5432".parse().unwrap();
        assert!(reg.begin(addr));
        assert!(
            !reg.begin(addr),
            "nested BEGIN must not replace the session"
        );
        assert!(reg.get(addr).is_some());
        assert!(reg.take(addr).is_some());
        assert!(reg.get(addr).is_none());
        assert!(reg.begin(addr));
        reg.disconnect(&addr);
        assert!(reg.get(addr).is_none());
    }

    #[test]
    fn table_ref_resolution() {
        let ident = table_ref_to_ident(&TableReference::bare("trips")).unwrap();
        assert_eq!(ident.namespace().to_url_string(), DEFAULT_SCHEMA);
        let ident = table_ref_to_ident(&TableReference::partial("demo", "t")).unwrap();
        assert_eq!(ident.name(), "t");
        let err = table_ref_to_ident(&TableReference::full("nope", "demo", "t")).unwrap_err();
        assert!(err.to_string().contains("unknown catalog"));
    }
}
