//! Moonlink-style buffered write mode (`--write-buffer-ms N`, opt-in).
//!
//! With `N > 0`, autocommit INSERTs acknowledge after appending their rows
//! to an in-memory per-table buffer; a background task group-commits the
//! buffer to Iceberg every `N` ms (or earlier when the row threshold
//! [`max_rows`](WriteBuffer) is hit). Every read on THIS server unions the
//! committed table scan with the buffer (the Moonlink union-read pattern —
//! see [`overlay`](WriteBuffer::overlay) and `cache.rs`), so
//! read-your-writes holds locally and cross-connection freshness on the
//! same server is instant.
//!
//! # Semantics (explicit and default-safe)
//!
//! * **Default OFF (`--write-buffer-ms 0`)**: current fully-synchronous
//!   semantics, byte-for-byte unchanged — every INSERT is an Iceberg commit
//!   before the ack.
//! * **Durability trade (why the default is 0)**: an acked-but-unflushed
//!   INSERT lives only in this process's memory. An UNCLEAN kill loses up
//!   to `N` ms of acked writes (plus whatever a slow/failing catalog delays
//!   past the cadence). `main.rs` prints a WARN at startup when the mode is
//!   enabled. Rows the flusher HAS committed are exactly as durable as any
//!   synchronous write.
//! * **Cross-SERVER freshness = commit cadence**: other icegres computes
//!   (and any external Iceberg reader) see buffered rows only once the
//!   flusher commits them — at most ~`N` ms after the ack. Only reads on
//!   the buffering server itself get the instant union view.
//! * **Group commit**: one flush = ONE Iceberg snapshot per table for every
//!   row buffered since the last flush (metadata pressure is 1 snapshot per
//!   `N` ms per table instead of 1 per INSERT).
//! * **Ordering fences**: statements whose semantics depend on committed
//!   state — autocommit UPDATE/DELETE, `BEGIN` (transactions pin
//!   snapshots), PK-enforced INSERT, and any DDL — force a synchronous
//!   flush FIRST, then run on their normal path. So `INSERT ...; UPDATE
//!   ...` on one connection behaves exactly as in synchronous mode, just
//!   with the INSERT's commit deferred to the fence.
//! * **Time travel excluded by design**: `table@snapshot` reads are
//!   point-in-time views of committed history and never see the buffer.
//!
//! # Union-read correctness (no duplicates, no lost rows)
//!
//! The hazard in any union-read design is the flush race: a scan must see
//! each buffered row EXACTLY once, whether the flusher is idle, mid-commit,
//! or just finished. The protocol here:
//!
//! 1. Buffered rows live in `pending` (no snapshot tag).
//! 2. The flusher PREPARES the commit (data files + manifest staged, new
//!    snapshot id `S` chosen), then — BEFORE posting to the catalog — moves
//!    the rows from `pending` to a `flushed(S)` generation.
//! 3. A scan first loads the table's current committed metadata, THEN takes
//!    the overlay under the buffer lock: all `pending` rows plus every
//!    `flushed(S)` generation whose `S` is NOT in that metadata
//!    (`snapshot_by_id(S).is_none()`).
//!
//! Whatever the interleaving, correctness holds: if the scan's metadata
//! predates commit `S`, the committed scan lacks the rows and the overlay
//! supplies them (as `pending` or as an unseen `flushed(S)`); if the
//! metadata includes `S`, the committed scan has the rows and the
//! `flushed(S)` generation is excluded. A failed/conflicted post moves the
//! generation back to the FRONT of `pending` (order preserved) and the
//! commit is retried from fresh metadata; `S` never enters the catalog, so
//! no scan could have excluded those rows. Flushed generations are garbage-
//! collected once they are older than [`FLUSHED_GC`] — by then every
//! in-flight scan that loaded pre-`S` metadata has long since taken its
//! overlay (the load and the overlay take are adjacent awaits in one scan
//! call), and any NEW scan's metadata contains `S`, excluding the
//! generation anyway. Buffer memory is therefore bounded by rows inserted
//! in the last `FLUSHED_GC` window.

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Result};
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef as ArrowSchemaRef;
use async_trait::async_trait;
use datafusion::common::ParamValues;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::Statement;
use datafusion_postgres::pgwire::api::results::{Response, Tag};
use datafusion_postgres::pgwire::api::ClientInfo;
use datafusion_postgres::pgwire::error::PgWireResult;
use datafusion_postgres::QueryHook;
use iceberg::arrow::schema_to_arrow_schema;
use iceberg::spec::TableMetadata;
use iceberg::{Catalog, TableIdent};

use crate::dml;
use crate::overwrite::{
    align_batch, prepare_commit, CommitOutcome, OverwriteEngine, TableOp, MAX_COMMIT_ATTEMPTS,
};
use crate::txn::{insert_target, plan_insert_rows, TxnRegistry};

/// Flush early once a table has this many pending buffered rows (bounds
/// buffer memory under a hot writer regardless of the flush cadence).
/// Overridable via `ICEGRES_WRITE_BUFFER_MAX_ROWS`.
const DEFAULT_MAX_ROWS: usize = 50_000;

/// Retain committed `flushed(S)` generations this long before garbage
/// collection (see the union-read correctness protocol in the module docs).
const FLUSHED_GC: Duration = Duration::from_secs(30);

/// One committed-but-possibly-not-yet-observed flush generation.
struct FlushedGen {
    snapshot_id: i64,
    batches: Vec<RecordBatch>,
    committed_at: Instant,
}

/// Per-table buffer state.
struct TableBuf {
    /// Canonical (field-id annotated) Arrow schema every buffered batch is
    /// aligned to — identical to what the committed scan produces.
    schema: ArrowSchemaRef,
    /// Acked rows not yet handed to a catalog commit, in insert order.
    pending: Vec<RecordBatch>,
    pending_rows: usize,
    /// Committed generations kept until every scan can see them (GC'd).
    flushed: Vec<FlushedGen>,
}

/// The overlay a scan must union with its committed data (see cache.rs).
pub struct Overlay {
    pub schema: ArrowSchemaRef,
    pub batches: Vec<RecordBatch>,
}

/// Shared in-memory write buffer + its flush machinery. Constructed once
/// per server when `--write-buffer-ms > 0`; `spawn_flusher` starts the
/// background group-commit task.
pub struct WriteBuffer {
    catalog: Arc<dyn Catalog>,
    engine: Arc<OverwriteEngine>,
    interval: Duration,
    max_rows: usize,
    tables: StdMutex<HashMap<TableIdent, TableBuf>>,
    /// Serializes flushes (background cadence vs. forced fences).
    flush_lock: tokio::sync::Mutex<()>,
    /// Wakes the flusher early when `max_rows` is hit.
    kick: tokio::sync::Notify,
}

impl WriteBuffer {
    pub fn new(catalog: Arc<dyn Catalog>, engine: Arc<OverwriteEngine>, interval_ms: u64) -> Self {
        let max_rows = std::env::var("ICEGRES_WRITE_BUFFER_MAX_ROWS")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(DEFAULT_MAX_ROWS);
        Self {
            catalog,
            engine,
            interval: Duration::from_millis(interval_ms),
            max_rows,
            tables: StdMutex::new(HashMap::new()),
            flush_lock: tokio::sync::Mutex::new(()),
            kick: tokio::sync::Notify::new(),
        }
    }

    pub fn max_rows(&self) -> usize {
        self.max_rows
    }

    /// Start the background group-commit task: flush every `interval`, or
    /// immediately when kicked by a threshold-crossing insert. A flush
    /// failure (catalog down, ...) is logged loudly and the rows STAY
    /// buffered — they retry on the next tick and remain readable through
    /// the union view meanwhile.
    pub fn spawn_flusher(self: &Arc<Self>) {
        let buf = self.clone();
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = tokio::time::sleep(buf.interval) => {}
                    _ = buf.kick.notified() => {}
                }
                if let Err(e) = buf.flush_now().await {
                    tracing::error!(
                        "write-buffer flush FAILED (rows stay buffered and readable on this \
                         server; retrying next tick): {e:#}"
                    );
                }
            }
        });
    }

    /// Buffer aligned `batches` for `ident`; the INSERT is acked as soon as
    /// this returns. Returns the number of buffered rows.
    async fn buffer_insert(&self, ident: &TableIdent, batches: Vec<RecordBatch>) -> Result<usize> {
        // First touch of a table: capture its canonical Arrow schema (one
        // catalog load); afterwards inserts are pure in-memory appends.
        let need_schema = {
            let tables = self.tables.lock().expect("write-buffer lock poisoned");
            !tables.contains_key(ident)
        };
        let schema = if need_schema {
            let table = self
                .catalog
                .load_table(ident)
                .await
                .map_err(|e| anyhow!("failed to load table {ident}: {e}"))?;
            Some(Arc::new(
                schema_to_arrow_schema(table.metadata().current_schema())
                    .map_err(|e| anyhow!("schema conversion failed for {ident}: {e}"))?,
            ))
        } else {
            None
        };
        let mut tables = self.tables.lock().expect("write-buffer lock poisoned");
        if !tables.contains_key(ident) {
            // Entries are never removed, so reaching here implies this call
            // loaded the schema above (a racing first insert may have won
            // the map entry, in which case contains_key is now true).
            let schema =
                schema.ok_or_else(|| anyhow!("write-buffer schema for {ident} disappeared"))?;
            tables.insert(
                ident.clone(),
                TableBuf {
                    schema,
                    pending: Vec::new(),
                    pending_rows: 0,
                    flushed: Vec::new(),
                },
            );
        }
        let entry = tables.get_mut(ident).expect("just ensured present");
        let aligned: Vec<RecordBatch> = batches
            .iter()
            .map(|b| align_batch(b, &entry.schema))
            .collect::<Result<_>>()?;
        let rows: usize = aligned.iter().map(|b| b.num_rows()).sum();
        entry.pending.extend(aligned);
        entry.pending_rows += rows;
        if entry.pending_rows >= self.max_rows {
            self.kick.notify_one();
        }
        Ok(rows)
    }

    /// The union overlay for one table against the committed metadata a
    /// scan just loaded: all pending rows plus committed generations that
    /// metadata cannot see yet. `None` when there is nothing to add
    /// (fast path — scans are unchanged when the buffer is idle).
    pub fn overlay(&self, ident: &TableIdent, metadata: &TableMetadata) -> Option<Overlay> {
        let tables = self.tables.lock().expect("write-buffer lock poisoned");
        let entry = tables.get(ident)?;
        let mut batches: Vec<RecordBatch> = entry.pending.clone();
        for flushed_gen in &entry.flushed {
            if metadata.snapshot_by_id(flushed_gen.snapshot_id).is_none() {
                batches.extend(flushed_gen.batches.iter().cloned());
            }
        }
        if batches.is_empty() {
            return None;
        }
        Some(Overlay {
            schema: entry.schema.clone(),
            batches,
        })
    }

    /// Synchronously flush every table's pending rows (ordering fence /
    /// background tick body). Serialized by `flush_lock`.
    pub async fn flush_now(&self) -> Result<()> {
        let _guard = self.flush_lock.lock().await;
        self.gc_flushed();
        let idents: Vec<TableIdent> = {
            let tables = self.tables.lock().expect("write-buffer lock poisoned");
            tables
                .iter()
                .filter(|(_, t)| !t.pending.is_empty())
                .map(|(ident, _)| ident.clone())
                .collect()
        };
        let mut first_err: Option<anyhow::Error> = None;
        for ident in idents {
            if let Err(e) = self.flush_table(&ident).await {
                tracing::error!(table = %ident, "write-buffer flush failed: {e:#}");
                first_err.get_or_insert(e);
            }
        }
        match first_err {
            None => Ok(()),
            Some(e) => Err(e),
        }
    }

    /// Whether any pending rows exist (cheap check for the fence path).
    pub fn has_pending(&self) -> bool {
        let tables = self.tables.lock().expect("write-buffer lock poisoned");
        tables.values().any(|t| !t.pending.is_empty())
    }

    /// Group-commit one table's pending rows as ONE snapshot, with bounded
    /// optimistic-concurrency retries (fresh metadata per attempt, exactly
    /// like autocommit INSERT).
    async fn flush_table(&self, ident: &TableIdent) -> Result<()> {
        let mut conflicts: Vec<String> = Vec::new();
        for attempt in 1..=MAX_COMMIT_ATTEMPTS {
            // Snapshot the current pending prefix WITHOUT removing it: the
            // rows must stay readable through the union view while the
            // commit is in flight. New inserts append behind the prefix.
            let (batches, n_batches) = {
                let tables = self.tables.lock().expect("write-buffer lock poisoned");
                let Some(entry) = tables.get(ident) else {
                    return Ok(());
                };
                (entry.pending.clone(), entry.pending.len())
            };
            if n_batches == 0 {
                return Ok(());
            }
            let table = self
                .catalog
                .load_table(ident)
                .await
                .map_err(|e| anyhow!("failed to load table {ident}: {e}"))?;
            let pk = self.engine.pk_columns(&table)?;
            let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
            let ops = [TableOp::Append(batches)];
            let Some(prepared) = prepare_commit(&table, &ops, pk.as_deref(), self.engine.branch())
                .await
                .map_err(|e| anyhow!("buffered flush of {ident} failed to prepare: {e:#}"))?
            else {
                // Zero net rows (shouldn't happen for a non-empty append
                // list, but handle it: drop the prefix, nothing to commit).
                self.drop_pending_prefix(ident, n_batches);
                return Ok(());
            };
            let snapshot_id = prepared.snapshot_id();
            // Tag the prefix as flushed(S) BEFORE posting: see the module
            // docs for why this ordering makes the union race-free.
            self.move_pending_to_flushed(ident, n_batches, snapshot_id);
            match self.engine.post_prepared(ident, &prepared).await {
                Ok(CommitOutcome::Committed) => {
                    tracing::debug!(
                        table = %ident,
                        rows,
                        snapshot_id,
                        attempt,
                        "write-buffer flushed (group commit)"
                    );
                    return Ok(());
                }
                Ok(CommitOutcome::Conflict(msg)) => {
                    tracing::warn!(
                        table = %ident,
                        attempt,
                        "buffered flush conflict (409), retrying from fresh metadata: {msg}"
                    );
                    self.move_flushed_back_to_pending(ident, snapshot_id);
                    conflicts.push(msg);
                }
                Err(e) => {
                    self.move_flushed_back_to_pending(ident, snapshot_id);
                    return Err(e);
                }
            }
        }
        Err(anyhow!(
            "buffered flush of {ident} lost the optimistic-concurrency race \
             {MAX_COMMIT_ATTEMPTS} times; rows stay buffered for the next tick. Conflicts: {}",
            conflicts.join(" | ")
        ))
    }

    fn drop_pending_prefix(&self, ident: &TableIdent, n_batches: usize) {
        let mut tables = self.tables.lock().expect("write-buffer lock poisoned");
        if let Some(entry) = tables.get_mut(ident) {
            let dropped: Vec<RecordBatch> = entry.pending.drain(..n_batches).collect();
            entry.pending_rows -= dropped.iter().map(|b| b.num_rows()).sum::<usize>();
        }
    }

    fn move_pending_to_flushed(&self, ident: &TableIdent, n_batches: usize, snapshot_id: i64) {
        let mut tables = self.tables.lock().expect("write-buffer lock poisoned");
        if let Some(entry) = tables.get_mut(ident) {
            let batches: Vec<RecordBatch> = entry.pending.drain(..n_batches).collect();
            entry.pending_rows -= batches.iter().map(|b| b.num_rows()).sum::<usize>();
            entry.flushed.push(FlushedGen {
                snapshot_id,
                batches,
                committed_at: Instant::now(),
            });
        }
    }

    fn move_flushed_back_to_pending(&self, ident: &TableIdent, snapshot_id: i64) {
        let mut tables = self.tables.lock().expect("write-buffer lock poisoned");
        if let Some(entry) = tables.get_mut(ident) {
            let Some(pos) = entry
                .flushed
                .iter()
                .position(|g| g.snapshot_id == snapshot_id)
            else {
                return;
            };
            let flushed_gen = entry.flushed.remove(pos);
            entry.pending_rows += flushed_gen
                .batches
                .iter()
                .map(|b| b.num_rows())
                .sum::<usize>();
            // These were the OLDEST rows: restore insert order at the front.
            let mut restored = flushed_gen.batches;
            restored.extend(entry.pending.drain(..));
            entry.pending = restored;
        }
    }

    /// Drop flushed generations old enough that no scan can still need them
    /// (module docs: the metadata-load -> overlay-take window inside one
    /// scan is microseconds; FLUSHED_GC is 30 s).
    fn gc_flushed(&self) {
        let mut tables = self.tables.lock().expect("write-buffer lock poisoned");
        for entry in tables.values_mut() {
            entry
                .flushed
                .retain(|g| g.committed_at.elapsed() < FLUSHED_GC);
        }
    }
}

// ---------------------------------------------------------------------------
// The hook
// ---------------------------------------------------------------------------

/// Query hook for buffered write mode. Must be FIRST in the hook chain:
///
/// * autocommit INSERT (no open transaction, PK enforcement off) is acked
///   from the buffer;
/// * ordering fences (UPDATE/DELETE, BEGIN, DDL, PK-enforced INSERT) flush
///   synchronously and fall through (`None`) to their normal handler;
/// * everything else (SELECT, SET/SHOW, COMMIT/ROLLBACK, statements on a
///   connection with an open transaction) passes through untouched — reads
///   get buffer visibility via the union in `cache.rs`, transactions flushed
///   at BEGIN and then run on committed state (snapshot isolation).
pub struct BufferHook {
    buffer: Arc<WriteBuffer>,
    registry: Arc<TxnRegistry>,
    /// With `--enforce-pk`, INSERTs must keep their check-then-commit path:
    /// buffering is bypassed (flush-fence first so checks see acked rows).
    enforce_pk: bool,
}

impl BufferHook {
    pub fn new(buffer: Arc<WriteBuffer>, registry: Arc<TxnRegistry>, enforce_pk: bool) -> Self {
        Self {
            buffer,
            registry,
            enforce_pk,
        }
    }

    /// Plan the INSERT (identical rules to the stock path: column-list
    /// reordering, NULL filling, coercion), buffer the rows, ack.
    async fn buffered_insert(
        &self,
        stmt: &Statement,
        shared: &SessionContext,
        params: Option<&ParamValues>,
    ) -> Result<Response> {
        // Target must be a real Iceberg table (planning through the shared
        // context also validates this — insert_target is the cheap check).
        let ident = insert_target(stmt)?;
        let (plan_ident, batches) = plan_insert_rows(shared, stmt, params).await?;
        anyhow::ensure!(
            plan_ident == ident,
            "INSERT target resolution mismatch: {plan_ident} vs {ident}"
        );
        let rows = self.buffer.buffer_insert(&ident, batches).await?;
        Ok(Response::Execution(
            Tag::new("INSERT").with_oid(0).with_rows(rows),
        ))
    }

    /// Shared dispatch for both wire protocols. `None` = fall through.
    async fn dispatch(
        &self,
        stmt: &Statement,
        shared: &SessionContext,
        client: &(dyn ClientInfo + Send + Sync),
        params: Option<&ParamValues>,
    ) -> Option<PgWireResult<Response>> {
        // A connection with an open transaction is owned by TxnHook.
        if self.registry.active(client.socket_addr()) {
            return None;
        }
        match stmt {
            Statement::Insert(_) if !self.enforce_pk => Some(
                self.buffered_insert(stmt, shared, params)
                    .await
                    .map_err(|e| dml::engine_error(&e)),
            ),
            // Pass-throughs that need no fence: reads see the buffer via
            // the union view; SET/SHOW are session-local; COMMIT/ROLLBACK
            // without a transaction are warnings/no-ops.
            Statement::Query(_)
            | Statement::Explain { .. }
            | Statement::Set { .. }
            | Statement::ShowVariable { .. }
            | Statement::ShowStatus { .. }
            | Statement::Commit { .. }
            | Statement::Rollback { .. } => None,
            // Everything else — UPDATE/DELETE, BEGIN, DDL, PK-enforced
            // INSERT, COPY, ... — is an ordering fence: acked rows must be
            // committed before it runs on its normal path.
            _ => {
                if self.buffer.has_pending() {
                    if let Err(e) = self.buffer.flush_now().await {
                        return Some(Err(dml::engine_error(&anyhow!(
                            "write-buffer flush (required before this statement) failed: {e:#}"
                        ))));
                    }
                }
                None
            }
        }
    }
}

#[async_trait]
impl QueryHook for BufferHook {
    async fn handle_simple_query(
        &self,
        statement: &Statement,
        session_context: &SessionContext,
        client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        self.dispatch(statement, session_context, client, None)
            .await
    }

    async fn handle_extended_parse_query(
        &self,
        _sql: &Statement,
        _session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        // The default planner plans INSERT fine; execution is intercepted in
        // handle_extended_query (same pattern as TxnHook::autocommit_insert).
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
        self.dispatch(statement, session_context, client, Some(params))
            .await
    }
}
