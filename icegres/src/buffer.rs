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
//! * **Durable tail (`--tail-dir`, opt-in, closes the unclean-kill window)**:
//!   with a [`TailStore`] attached, every buffered INSERT is fsync'd to a
//!   per-table WAL BEFORE its ack (a tail failure is the statement's error —
//!   no silent downgrade), replayed into `pending` on the next boot, and
//!   truncated when the flush commit lands. One STATEMENT is one tail frame
//!   (all its batches in a single fsync'd append), so a failed statement can
//!   never leave a replayable partial prefix. Each flush commit records the
//!   highest drained tail sequence as a table property namespaced by the
//!   tail's persistent identity (`icegres.tail-seq.<tail-id>`, see
//!   [`TAIL_SEQ_PROPERTY_PREFIX`](crate::tail::TAIL_SEQ_PROPERTY_PREFIX)) in
//!   the SAME atomic commit, plus a best-effort local sidecar; boot replay
//!   drops frames at or below `max(property, sidecar)` — exactly-once across
//!   crashes, reconcilable from the lake alone even when a foreign writer
//!   drops the property. Honest scope: durability is this NODE's disk, not
//!   node-loss durability (see `tail.rs`). Tail appends and the flush
//!   snapshot both run under the buffer lock, so per-table pending order ==
//!   tail sequence order — the invariant the watermark depends on.
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

use anyhow::{anyhow, Context as _, Result};
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
use crate::tail::{drop_stale_frames, effective_watermark, parse_watermark_property, TailStore};
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
    /// Highest durable-tail sequence reflected in this buffer (None when no
    /// tail is attached). Because tail appends happen under the same lock
    /// that orders `pending`, this is exactly the max sequence of any
    /// pending-snapshot taken now — the flush commit's watermark.
    tail_high: Option<u64>,
}

/// The overlay a scan must union with its committed data (see cache.rs).
pub struct Overlay {
    pub schema: ArrowSchemaRef,
    pub batches: Vec<RecordBatch>,
}

/// The pure in-memory buffer bookkeeping — the union-read state machine
/// (pending rows, flushed-but-maybe-unobserved generations) with NO catalog
/// or engine dependency, so its correctness (the flush race is the one truly
/// subtle property here) is unit-testable offline. `WriteBuffer` owns one of
/// these and adds the catalog I/O (schema load, group commit) around it.
#[derive(Default)]
struct BufferState {
    tables: StdMutex<HashMap<TableIdent, TableBuf>>,
}

impl BufferState {
    fn contains(&self, ident: &TableIdent) -> bool {
        self.tables
            .lock()
            .expect("write-buffer lock poisoned")
            .contains_key(ident)
    }

    /// Align `batches` to the table's canonical schema and append them to its
    /// pending buffer, creating the entry from `schema_if_first` on first
    /// touch. With a `tail`, the aligned batches are durably appended to it
    /// FIRST (fsync before return) — a tail error leaves `pending` untouched
    /// and becomes the statement's error, so nothing is ever acked from
    /// memory alone. Running the tail append under the buffer lock keeps
    /// pending order == tail sequence order (the watermark invariant; the
    /// fsync is the same latency the client is paying for durability anyway).
    /// Returns `(rows_appended, pending_rows_total)`.
    fn append(
        &self,
        ident: &TableIdent,
        schema_if_first: Option<ArrowSchemaRef>,
        batches: &[RecordBatch],
        tail: Option<&dyn TailStore>,
    ) -> Result<(usize, usize)> {
        let mut tables = self.tables.lock().expect("write-buffer lock poisoned");
        if !tables.contains_key(ident) {
            // Entries are never removed, so reaching here implies the caller
            // loaded the schema (a racing first insert may have won the entry,
            // in which case contains_key is now true and we skip this).
            let schema = schema_if_first
                .ok_or_else(|| anyhow!("write-buffer schema for {ident} disappeared"))?;
            tables.insert(
                ident.clone(),
                TableBuf {
                    schema,
                    pending: Vec::new(),
                    pending_rows: 0,
                    flushed: Vec::new(),
                    tail_high: None,
                },
            );
        }
        let entry = tables.get_mut(ident).expect("just ensured present");
        let aligned: Vec<RecordBatch> = batches
            .iter()
            .map(|b| align_batch(b, &entry.schema))
            .collect::<Result<_>>()?;
        if let Some(tail) = tail {
            // ONE append for the whole statement (all batches in one frame,
            // one fsync, one sequence): all-or-nothing by construction — a
            // failure leaves NO durable frame (tail.rs rolls back partial
            // bytes), so rows of a failed statement can never replay.
            if !aligned.is_empty() {
                entry.tail_high = Some(tail.append(ident, &aligned)?);
            }
        }
        let rows: usize = aligned.iter().map(|b| b.num_rows()).sum();
        entry.pending.extend(aligned);
        entry.pending_rows += rows;
        Ok((rows, entry.pending_rows))
    }

    /// Record that tail frames up to `seq` are reflected in this buffer
    /// (boot replay pushes recovered rows via [`append`](Self::append) with
    /// no tail — they are already durable — then notes their sequences here
    /// so the next flush commit's watermark covers them).
    fn note_tail_high(&self, ident: &TableIdent, seq: u64) {
        let mut tables = self.tables.lock().expect("write-buffer lock poisoned");
        if let Some(entry) = tables.get_mut(ident) {
            entry.tail_high = Some(entry.tail_high.map_or(seq, |h| h.max(seq)));
        }
    }

    fn has_pending(&self) -> bool {
        let tables = self.tables.lock().expect("write-buffer lock poisoned");
        tables.values().any(|t| !t.pending.is_empty())
    }

    /// Idents with pending rows (flush work list).
    fn pending_idents(&self) -> Vec<TableIdent> {
        let tables = self.tables.lock().expect("write-buffer lock poisoned");
        tables
            .iter()
            .filter(|(_, t)| !t.pending.is_empty())
            .map(|(ident, _)| ident.clone())
            .collect()
    }

    /// Snapshot the current pending prefix WITHOUT removing it (rows stay
    /// readable through the union while the commit is in flight). Returns
    /// `(batches, batch_count, tail_mark)` where `tail_mark` is the highest
    /// durable-tail sequence the snapshot covers (the commit's watermark;
    /// `None` without a tail) — taken under the same lock as the batches,
    /// so it is exact for this snapshot.
    fn snapshot_pending(&self, ident: &TableIdent) -> (Vec<RecordBatch>, usize, Option<u64>) {
        let tables = self.tables.lock().expect("write-buffer lock poisoned");
        match tables.get(ident) {
            Some(entry) => (entry.pending.clone(), entry.pending.len(), entry.tail_high),
            None => (Vec::new(), 0, None),
        }
    }

    /// The union overlay for one table: all pending rows plus every committed
    /// generation the scan's metadata cannot see yet. `is_committed(S)` is the
    /// caller's snapshot-membership test (real code: does the just-loaded
    /// metadata contain `S`?). `None` when there is nothing to add.
    fn overlay_with(
        &self,
        ident: &TableIdent,
        is_committed: impl Fn(i64) -> bool,
    ) -> Option<Overlay> {
        let tables = self.tables.lock().expect("write-buffer lock poisoned");
        let entry = tables.get(ident)?;
        let mut batches: Vec<RecordBatch> = entry.pending.clone();
        for flushed_gen in &entry.flushed {
            if !is_committed(flushed_gen.snapshot_id) {
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
            restored.append(&mut entry.pending);
            entry.pending = restored;
        }
    }

    /// Drop flushed generations for which `keep` returns false.
    fn retain_flushed(&self, keep: impl Fn(&FlushedGen) -> bool) {
        let mut tables = self.tables.lock().expect("write-buffer lock poisoned");
        for entry in tables.values_mut() {
            entry.flushed.retain(&keep);
        }
    }
}

/// Shared in-memory write buffer + its flush machinery. Constructed once
/// per server when `--write-buffer-ms > 0`; `spawn_flusher` starts the
/// background group-commit task.
pub struct WriteBuffer {
    catalog: Arc<dyn Catalog>,
    engine: Arc<OverwriteEngine>,
    interval: Duration,
    max_rows: usize,
    state: BufferState,
    /// Durable tail (`--tail-dir`): acked rows are fsync'd here before the
    /// ack and replayed at boot. `None` = today's in-memory-only semantics.
    tail: Option<Arc<dyn TailStore>>,
    /// Serializes flushes (background cadence vs. forced fences).
    flush_lock: tokio::sync::Mutex<()>,
    /// Wakes the flusher early when `max_rows` is hit.
    kick: tokio::sync::Notify,
}

impl WriteBuffer {
    pub fn new(
        catalog: Arc<dyn Catalog>,
        engine: Arc<OverwriteEngine>,
        interval_ms: u64,
        tail: Option<Arc<dyn TailStore>>,
    ) -> Self {
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
            state: BufferState::default(),
            tail,
            flush_lock: tokio::sync::Mutex::new(()),
            kick: tokio::sync::Notify::new(),
        }
    }

    pub fn max_rows(&self) -> usize {
        self.max_rows
    }

    /// Whether a durable tail is attached (shutdown messaging honesty:
    /// a failed shutdown flush is not lossy when the tail replays).
    pub fn tail_enabled(&self) -> bool {
        self.tail.is_some()
    }

    /// Boot-time recovery (`--tail-dir`): replay every surviving tail frame,
    /// drop those at or below each table's committed watermark (the
    /// `icegres.tail-seq.<tail-id>` property, belt-and-braced with the local
    /// sidecar), and push the survivors into `pending` for the normal
    /// flusher to drain. Call once, after catalog/engine init and BEFORE
    /// `spawn_flusher`/the listener. Fails loudly (aborting startup) rather
    /// than silently dropping acked rows — if a tailed table was dropped
    /// since the crash, remove its `<ns>.<table>` directory from the tail
    /// dir to acknowledge the loss.
    pub async fn replay_tail(&self) -> Result<()> {
        let Some(tail) = &self.tail else {
            return Ok(());
        };
        let replayed = tail.replay()?;
        if replayed.is_empty() {
            tracing::info!("durable tail is empty; nothing to replay");
            return Ok(());
        }
        let (mut recovered_rows, mut recovered_tables) = (0usize, 0usize);
        for table_tail in replayed {
            let ident = table_tail.ident;
            let table = match self.catalog.load_table(&ident).await {
                Ok(t) => t,
                // A frameless tail dir holds no acked rows, so a table that
                // no longer loads (dropped since?) costs nothing: WARN and
                // move on. With frames present, abort loudly as ever.
                Err(e) if table_tail.frames.is_empty() => {
                    tracing::warn!(
                        table = %ident,
                        "tail replay: cannot load the table behind a FRAMELESS tail \
                         dir (dropped since the crash?); skipping it — no acked rows \
                         are at stake: {e}"
                    );
                    // The sidecar alone (no catalog needed) must still floor
                    // the sequence: if the table is merely UNLOADABLE (not
                    // gone) and later takes appends, numbering restarting at
                    // 1 UNDER the committed watermark would make the next
                    // crash-replay drop those acked rows as already covered.
                    apply_sidecar_seq_floor(tail.as_ref(), &ident, table_tail.sidecar_watermark)?;
                    continue;
                }
                Err(e) => {
                    return Err(anyhow!(
                        "tail replay: cannot load table {ident} (its tail holds acked \
                         rows; if the table was dropped, delete its directory under \
                         --tail-dir to acknowledge losing them): {e}"
                    ));
                }
            };
            // Watermark = max(own namespaced property, local sidecar): the
            // sidecar survives a foreign writer dropping the property; the
            // property survives a crash before the sidecar write.
            let watermark = effective_watermark(
                &ident,
                table
                    .metadata()
                    .properties()
                    .get(tail.watermark_property())
                    .map(String::as_str),
                table_tail.sidecar_watermark,
            );
            // Sequence floor for EVERY table dir — crucially including the
            // frameless ones a full truncate + restart leaves behind:
            // without it, numbering restarts at 1 UNDER the persisted
            // watermark and the NEXT crash-replay silently drops the new
            // acked rows as "already covered".
            if let Some(w) = watermark {
                tail.ensure_seq_floor(&ident, w + 1)?;
            }
            let (survivors, dropped) = drop_stale_frames(table_tail.frames, watermark);
            if dropped > 0 {
                tracing::info!(
                    table = %ident,
                    dropped,
                    watermark = watermark.unwrap_or_default(),
                    "tail replay: dropped frames already covered by the committed \
                     watermark (crash landed between commit and truncate)"
                );
                // Best-effort disk cleanup of the covered frames.
                if let Some(w) = watermark {
                    if let Err(e) = tail.truncate(&ident, w) {
                        tracing::warn!(
                            table = %ident,
                            upto_seq = w,
                            "tail truncate of stale frames failed (harmless; the \
                             watermark keeps replay exact): {e:#}"
                        );
                    }
                }
            }
            let Some((max_seq, _)) = survivors.last() else {
                continue;
            };
            let max_seq = *max_seq;
            let schema = Arc::new(
                schema_to_arrow_schema(table.metadata().current_schema())
                    .map_err(|e| anyhow!("schema conversion failed for {ident}: {e}"))?,
            );
            let batches: Vec<RecordBatch> = survivors.into_iter().flat_map(|(_, b)| b).collect();
            // No tail here: the rows are already durable. align_batch inside
            // fails loudly if the table's schema evolved past the frames.
            let (rows, _) = self
                .state
                .append(&ident, Some(schema), &batches, None)
                .with_context(|| format!("tail replay: cannot re-buffer rows for {ident}"))?;
            self.state.note_tail_high(&ident, max_seq);
            recovered_rows += rows;
            recovered_tables += 1;
        }
        tracing::info!(
            rows = recovered_rows,
            tables = recovered_tables,
            "recovered {recovered_rows} rows for {recovered_tables} tables from the \
             durable tail; the flusher will commit them"
        );
        Ok(())
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
        let schema_if_first = if self.state.contains(ident) {
            None
        } else {
            let table = self
                .catalog
                .load_table(ident)
                .await
                .map_err(|e| anyhow!("failed to load table {ident}: {e}"))?;
            Some(Arc::new(
                schema_to_arrow_schema(table.metadata().current_schema())
                    .map_err(|e| anyhow!("schema conversion failed for {ident}: {e}"))?,
            ))
        };
        // With a tail: durable append FIRST, then pending, then the ack
        // (a tail error is this statement's error — never a silent
        // downgrade to non-durable buffering).
        let (rows, pending_total) =
            self.state
                .append(ident, schema_if_first, &batches, self.tail.as_deref())?;
        if pending_total >= self.max_rows {
            self.kick.notify_one();
        }
        Ok(rows)
    }

    /// The union overlay for one table against the committed metadata a
    /// scan just loaded: all pending rows plus committed generations that
    /// metadata cannot see yet. `None` when there is nothing to add
    /// (fast path — scans are unchanged when the buffer is idle).
    pub fn overlay(&self, ident: &TableIdent, metadata: &TableMetadata) -> Option<Overlay> {
        // A generation `S` is "committed" (and so already in the scan's data)
        // exactly when the just-loaded metadata contains it.
        self.state
            .overlay_with(ident, |s| metadata.snapshot_by_id(s).is_some())
    }

    /// Synchronously flush every table's pending rows (ordering fence /
    /// background tick body). Serialized by `flush_lock`.
    pub async fn flush_now(&self) -> Result<()> {
        let _guard = self.flush_lock.lock().await;
        self.gc_flushed();
        let idents = self.state.pending_idents();
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
        self.state.has_pending()
    }

    /// Group-commit one table's pending rows as ONE snapshot, with bounded
    /// optimistic-concurrency retries (fresh metadata per attempt, exactly
    /// like autocommit INSERT).
    async fn flush_table(&self, ident: &TableIdent) -> Result<()> {
        // Rotate the tail ONCE per flush, before the commit is built: new
        // appends land in a fresh segment, so a successful commit can delete
        // whole covered segments instead of head-truncating a live file.
        // Retries reuse the rotation; on failure the segments simply stay
        // until a later successful flush covers them.
        if let Some(tail) = &self.tail {
            tail.rotate(ident)?;
        }
        let mut conflicts: Vec<String> = Vec::new();
        for attempt in 1..=MAX_COMMIT_ATTEMPTS {
            // Snapshot the current pending prefix WITHOUT removing it: the
            // rows must stay readable through the union view while the
            // commit is in flight. New inserts append behind the prefix.
            // `tail_mark` is the generation's exact watermark (taken under
            // the same lock, see snapshot_pending).
            let (batches, n_batches, tail_mark) = self.state.snapshot_pending(ident);
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
            // Record the drained tail sequence in the SAME atomic commit
            // (the exactly-once watermark boot replay filters against),
            // under THIS tail's namespaced key only. Never regress a
            // previously stamped value: after a full truncate + restart the
            // in-memory mark starts over from the floor, and a lower stamp
            // would re-open the double-apply window the watermark closes.
            let tail_props = match (&self.tail, tail_mark) {
                (Some(tail), Some(mark)) => {
                    let key = tail.watermark_property();
                    let prev = parse_watermark_property(
                        ident,
                        table.metadata().properties().get(key).map(String::as_str),
                    );
                    // Prepare-time already-committed guard: when the stamp
                    // in the metadata just loaded already covers this whole
                    // generation, an EARLIER flush of these same rows
                    // committed but its outcome came back ambiguous (POST
                    // error + failed disambiguation reload re-queued it).
                    // Re-posting would double-apply the rows. Run the
                    // Committed-arm bookkeeping instead: park the rows as a
                    // flushed generation tagged with a snapshot this
                    // metadata already contains (new scans exclude it;
                    // in-flight ones still see the rows until GC), then
                    // sidecar + truncate the covered tail frames — the
                    // reload-failure residual heals on the next flush.
                    if generation_already_committed(prev, mark) {
                        tracing::warn!(
                            table = %ident,
                            rows,
                            tail_mark = mark,
                            committed_watermark = prev.unwrap_or_default(),
                            "flush generation is already covered by the committed \
                             tail watermark (an earlier ambiguous flush LANDED); \
                             skipping the post instead of double-applying"
                        );
                        let seen_snapshot = table
                            .metadata()
                            .snapshot_for_ref(self.engine.branch())
                            .map(|s| s.snapshot_id())
                            .or_else(|| table.metadata().current_snapshot_id());
                        match seen_snapshot {
                            Some(s) => self.state.move_pending_to_flushed(ident, n_batches, s),
                            // No snapshot in the metadata at all (cannot
                            // happen when the watermark was stamped by a
                            // snapshot commit, but stay safe): the rows are
                            // committed — drop them outright.
                            None => self.state.drop_pending_prefix(ident, n_batches),
                        }
                        tail_truncate_covered(self.tail.as_deref(), ident, tail_mark);
                        return Ok(());
                    }
                    let stamped = prev.map_or(mark, |p| p.max(mark));
                    Some(HashMap::from([(key.to_string(), stamped.to_string())]))
                }
                _ => None,
            };
            let Some(prepared) = prepare_commit(
                &table,
                &ops,
                pk.as_deref(),
                self.engine.branch(),
                tail_props.as_ref(),
            )
            .await
            .map_err(|e| anyhow!("buffered flush of {ident} failed to prepare: {e:#}"))?
            else {
                // Zero net rows (shouldn't happen for a non-empty append
                // list, but handle it: drop the prefix, nothing to commit).
                // The covered tail frames also net zero rows on any future
                // replay, so forgetting them without a commit is safe.
                self.state.drop_pending_prefix(ident, n_batches);
                tail_truncate_covered(self.tail.as_deref(), ident, tail_mark);
                return Ok(());
            };
            let snapshot_id = prepared.snapshot_id();
            // Tag the prefix as flushed(S) BEFORE posting: see the module
            // docs for why this ordering makes the union race-free.
            self.state
                .move_pending_to_flushed(ident, n_batches, snapshot_id);
            match self.engine.post_prepared(ident, &prepared).await {
                Ok(CommitOutcome::Committed) => {
                    // The commit carries the watermark, so the covered tail
                    // segments are dead weight from here on.
                    tail_truncate_covered(self.tail.as_deref(), ident, tail_mark);
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
                    self.state.move_flushed_back_to_pending(ident, snapshot_id);
                    conflicts.push(msg);
                }
                Err(e) => {
                    // Ambiguous outcome: a transport error / 5xx can follow
                    // a commit the catalog actually APPLIED, and re-queueing
                    // then double-applies the generation on the next tick.
                    // The tail watermark makes this detectable while alive:
                    // reload the metadata once, and if our own namespaced
                    // key already covers this generation's mark, the commit
                    // landed — treat it exactly as the Committed arm (the
                    // flushed(S) bookkeeping with the known snapshot id is
                    // already in place; just truncate, no re-queue). The
                    // mark cannot come from an OLDER commit: this
                    // generation holds at least one seq consumed after the
                    // last stamped watermark, and flush_lock serializes any
                    // newer one.
                    if let (Some(tail), Some(mark)) = (&self.tail, tail_mark) {
                        match self.catalog.load_table(ident).await {
                            Ok(fresh) => {
                                let seen = parse_watermark_property(
                                    ident,
                                    fresh
                                        .metadata()
                                        .properties()
                                        .get(tail.watermark_property())
                                        .map(String::as_str),
                                );
                                if seen.is_some_and(|s| s >= mark) {
                                    tracing::warn!(
                                        table = %ident,
                                        rows,
                                        snapshot_id,
                                        tail_mark = mark,
                                        "flush POST errored but the committed tail \
                                         watermark covers this generation: the commit \
                                         LANDED — treating it as committed (no \
                                         re-queue, no double-apply): {e:#}"
                                    );
                                    tail_truncate_covered(self.tail.as_deref(), ident, tail_mark);
                                    return Ok(());
                                }
                            }
                            Err(load_err) => tracing::warn!(
                                table = %ident,
                                "cannot reload metadata to disambiguate a failed flush \
                                 POST; re-queueing the generation (the prepare-time \
                                 already-committed guard resolves it on the next \
                                 flush): {load_err}"
                            ),
                        }
                    }
                    self.state.move_flushed_back_to_pending(ident, snapshot_id);
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

    /// Drop flushed generations old enough that no scan can still need them
    /// (module docs: the metadata-load -> overlay-take window inside one
    /// scan is microseconds; FLUSHED_GC is 30 s).
    fn gc_flushed(&self) {
        self.state
            .retain_flushed(|g| g.committed_at.elapsed() < FLUSHED_GC);
    }
}

/// Floor a table's next tail sequence from its watermark SIDECAR alone —
/// the boot-replay path for a FRAMELESS tail dir whose table failed to
/// load from the catalog (needs no catalog: the sidecar is a local file).
/// Without it, a full truncate + restart + transiently unloadable table
/// would restart numbering at 1 UNDER the committed watermark, and the
/// next crash-replay would silently drop freshly acked rows as covered.
fn apply_sidecar_seq_floor(
    tail: &dyn TailStore,
    ident: &TableIdent,
    sidecar: Option<u64>,
) -> Result<()> {
    match sidecar {
        Some(w) => tail.ensure_seq_floor(ident, w + 1),
        None => Ok(()),
    }
}

/// Whether the watermark already stamped in table metadata (`prev`) covers
/// a generation about to be posted (whose highest tail seq is `mark`).
/// Sequence numbers are only ever consumed ABOVE the last stamped
/// watermark (the boot floor plus the never-regress stamp guarantee it),
/// so full coverage can only mean an earlier flush of exactly these rows
/// COMMITTED but its outcome came back ambiguous and the generation was
/// re-queued — posting it again would double-apply.
fn generation_already_committed(prev: Option<u64>, mark: u64) -> bool {
    prev.is_some_and(|p| p >= mark)
}

/// After a flush generation is safely committed (or netted out to zero
/// rows), record the covered watermark in the local sidecar (best-effort;
/// the second gate against a foreign writer dropping the table property)
/// and forget its tail frames. A truncate failure only leaks segments —
/// the committed watermark keeps replay exactly-once regardless — so it is
/// a WARN, never a flush failure.
fn tail_truncate_covered(tail: Option<&dyn TailStore>, ident: &TableIdent, mark: Option<u64>) {
    let (Some(tail), Some(upto_seq)) = (tail, mark) else {
        return;
    };
    tail.record_watermark(ident, upto_seq);
    if let Err(e) = tail.truncate(ident, upto_seq) {
        tracing::warn!(
            table = %ident,
            upto_seq,
            "tail truncate after commit failed (segments leak until a later flush \
             covers them; replay stays exact via the committed watermark): {e:#}"
        );
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

// ---------------------------------------------------------------------------
// Unit tests — the union-read state machine (BufferState).
//
// These exercise the one genuinely subtle property of buffered mode: a scan
// must see every buffered row EXACTLY once regardless of where the flusher is
// (idle, mid-commit, just-conflicted). BufferState has no catalog/engine
// dependency, so the whole flush race is testable offline with hand-built
// batches and a closure standing in for the "is this snapshot in the metadata
// the scan just loaded?" test.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema};

    fn schema() -> ArrowSchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn ident() -> TableIdent {
        TableIdent::from_strs(["demo", "t"]).unwrap()
    }

    fn batch(sch: &ArrowSchemaRef, vals: &[i64]) -> RecordBatch {
        RecordBatch::try_new(sch.clone(), vec![Arc::new(Int64Array::from(vals.to_vec()))]).unwrap()
    }

    /// Flatten an overlay's batches into the id column, in order.
    fn ids(ov: &Overlay) -> Vec<i64> {
        let mut out = Vec::new();
        for b in &ov.batches {
            let col = b
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .expect("id column is Int64");
            out.extend(col.values().iter().copied());
        }
        out
    }

    // Overlay of pending rows always includes them: nothing is committed yet.
    #[test]
    fn overlay_sees_all_pending() {
        let st = BufferState::default();
        let sch = schema();
        let (rows, total) = st
            .append(
                &ident(),
                Some(sch.clone()),
                &[batch(&sch, &[1, 2, 3])],
                None,
            )
            .unwrap();
        assert_eq!((rows, total), (3, 3));
        assert!(st.has_pending());
        let ov = st
            .overlay_with(&ident(), |_| false)
            .expect("pending rows present");
        assert_eq!(ids(&ov), vec![1, 2, 3]);
    }

    // The flush race: once the prefix is tagged flushed(S), a scan whose
    // metadata already contains S must NOT re-add those rows (the committed
    // scan has them) — but a scan whose metadata predates S MUST see them via
    // the flushed generation, so no row is ever lost mid-commit.
    #[test]
    fn flushed_generation_excluded_iff_metadata_sees_it() {
        let st = BufferState::default();
        let sch = schema();
        st.append(&ident(), Some(sch.clone()), &[batch(&sch, &[1, 2])], None)
            .unwrap();
        let (_batches, n, _) = st.snapshot_pending(&ident());
        assert_eq!(n, 1); // one batch
        let s: i64 = 4242;
        st.move_pending_to_flushed(&ident(), n, s);
        // No pending rows left; the rows now live only in flushed(S).
        assert!(!st.has_pending());
        // Scan whose metadata already contains S: committed scan has the rows,
        // so the overlay must add NOTHING.
        assert!(st.overlay_with(&ident(), |x| x == s).is_none());
        // Scan whose metadata predates S: the committed scan lacks the rows,
        // so the overlay must supply them from the flushed generation.
        let ov = st
            .overlay_with(&ident(), |_| false)
            .expect("unseen flushed gen");
        assert_eq!(ids(&ov), vec![1, 2]);
    }

    // A conflicted/failed post moves the flushed generation BACK to the FRONT
    // of pending, preserving insert order relative to rows appended meanwhile,
    // and never enters the catalog — so the rows stay readable throughout.
    #[test]
    fn conflict_restores_pending_order_at_front() {
        let st = BufferState::default();
        let sch = schema();
        // Buffer A=[1], B=[2] -> pending [A,B].
        st.append(&ident(), Some(sch.clone()), &[batch(&sch, &[1])], None)
            .unwrap();
        st.append(&ident(), None, &[batch(&sch, &[2])], None)
            .unwrap();
        let (_b, n, _) = st.snapshot_pending(&ident());
        assert_eq!(n, 2);
        let s: i64 = 99;
        st.move_pending_to_flushed(&ident(), n, s);
        // A new insert C=[3] lands while the commit is "in flight".
        st.append(&ident(), None, &[batch(&sch, &[3])], None)
            .unwrap();
        // Commit conflicts: restore the flushed prefix to the front.
        st.move_flushed_back_to_pending(&ident(), s);
        // Row accounting and order: [1,2] restored ahead of [3].
        let ov = st.overlay_with(&ident(), |_| false).expect("rows present");
        assert_eq!(ids(&ov), vec![1, 2, 3]);
        // And with the (now-abandoned) S considered committed, the flushed gen
        // is gone from flushed (it was moved back), so all three are pending.
        let ov2 = st.overlay_with(&ident(), |x| x == s).expect("rows present");
        assert_eq!(ids(&ov2), vec![1, 2, 3]);
    }

    // Dropping the committed prefix (the zero-net-rows / success bookkeeping)
    // removes exactly those rows and their row count.
    #[test]
    fn drop_prefix_removes_committed_rows() {
        let st = BufferState::default();
        let sch = schema();
        st.append(
            &ident(),
            Some(sch.clone()),
            &[batch(&sch, &[1, 2, 3])],
            None,
        )
        .unwrap();
        let (_b, n, _) = st.snapshot_pending(&ident());
        st.drop_pending_prefix(&ident(), n);
        assert!(!st.has_pending());
        assert!(st.overlay_with(&ident(), |_| false).is_none());
    }

    // GC drops flushed generations by predicate (real code: older than
    // FLUSHED_GC). A kept generation still overlays for pre-S scans.
    #[test]
    fn retain_flushed_by_predicate() {
        let st = BufferState::default();
        let sch = schema();
        st.append(&ident(), Some(sch.clone()), &[batch(&sch, &[7])], None)
            .unwrap();
        let (_b, n, _) = st.snapshot_pending(&ident());
        let s: i64 = 5;
        st.move_pending_to_flushed(&ident(), n, s);
        // Keep-everything predicate: generation survives, still overlays.
        st.retain_flushed(|_| true);
        assert_eq!(ids(&st.overlay_with(&ident(), |_| false).unwrap()), vec![7]);
        // Drop-everything predicate (stands in for "too old"): gone.
        st.retain_flushed(|_| false);
        assert!(st.overlay_with(&ident(), |_| false).is_none());
    }

    // pending_idents reports only tables with pending rows; moving to flushed
    // clears a table from the flush work list.
    #[test]
    fn pending_idents_tracks_flush_worklist() {
        let st = BufferState::default();
        let sch = schema();
        assert!(st.pending_idents().is_empty());
        st.append(&ident(), Some(sch.clone()), &[batch(&sch, &[1])], None)
            .unwrap();
        assert_eq!(st.pending_idents(), vec![ident()]);
        let (_b, n, _) = st.snapshot_pending(&ident());
        st.move_pending_to_flushed(&ident(), n, 1);
        assert!(st.pending_idents().is_empty()); // pending drained into flushed
    }

    // -----------------------------------------------------------------------
    // Durable-tail wiring (mock TailStore): the insert path appends durably
    // BEFORE rows enter pending, the flush snapshot carries the exact
    // watermark, and a committed flush truncates at that watermark.
    // -----------------------------------------------------------------------

    #[derive(Default)]
    struct MockTail {
        next_seq: std::sync::atomic::AtomicU64,
        fail_appends: std::sync::atomic::AtomicBool,
        /// (table, seq, batch_count, total_rows) per STATEMENT append.
        appends: StdMutex<Vec<(TableIdent, u64, usize, usize)>>,
        truncates: StdMutex<Vec<(TableIdent, u64)>>,
        watermarks: StdMutex<Vec<(TableIdent, u64)>>,
    }

    impl TailStore for MockTail {
        fn append(&self, table: &TableIdent, batches: &[RecordBatch]) -> Result<u64> {
            if self.fail_appends.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(anyhow!("mock tail: disk on fire"));
            }
            let seq = self
                .next_seq
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1;
            self.appends.lock().unwrap().push((
                table.clone(),
                seq,
                batches.len(),
                batches.iter().map(|b| b.num_rows()).sum(),
            ));
            Ok(seq)
        }

        fn replay(&self) -> Result<Vec<crate::tail::ReplayedTable>> {
            Ok(Vec::new())
        }

        fn truncate(&self, table: &TableIdent, upto_seq: u64) -> Result<()> {
            self.truncates
                .lock()
                .unwrap()
                .push((table.clone(), upto_seq));
            Ok(())
        }

        fn ensure_seq_floor(&self, _table: &TableIdent, floor: u64) -> Result<()> {
            let cur = self.next_seq.load(std::sync::atomic::Ordering::SeqCst);
            // next_seq holds "last handed out"; the floor is the NEXT seq.
            self.next_seq.store(
                cur.max(floor.saturating_sub(1)),
                std::sync::atomic::Ordering::SeqCst,
            );
            Ok(())
        }

        fn watermark_property(&self) -> &str {
            "icegres.tail-seq.mock-tail-id"
        }

        fn record_watermark(&self, table: &TableIdent, seq: u64) {
            self.watermarks.lock().unwrap().push((table.clone(), seq));
        }
    }

    // Insert path: each STATEMENT is durably appended as one frame (all its
    // batches, one seq) before it becomes pending, and the snapshot's tail
    // mark is the exact highest seq.
    #[test]
    fn tail_appends_precede_pending_and_mark_is_exact() {
        let st = BufferState::default();
        let sch = schema();
        let tail = MockTail::default();
        // A 2-batch statement: ONE tail append covering both batches.
        st.append(
            &ident(),
            Some(sch.clone()),
            &[batch(&sch, &[1, 2]), batch(&sch, &[3])],
            Some(&tail),
        )
        .unwrap();
        st.append(&ident(), None, &[batch(&sch, &[4])], Some(&tail))
            .unwrap();
        assert_eq!(
            *tail.appends.lock().unwrap(),
            vec![(ident(), 1, 2, 3), (ident(), 2, 1, 1)]
        );
        let (_batches, n, mark) = st.snapshot_pending(&ident());
        assert_eq!((n, mark), (3, Some(2)));
    }

    // A tail append failure is the statement's failure: nothing enters
    // pending, so nothing could be acked from memory alone.
    #[test]
    fn tail_failure_keeps_rows_out_of_pending() {
        let st = BufferState::default();
        let sch = schema();
        let tail = MockTail::default();
        tail.fail_appends
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let err = st
            .append(
                &ident(),
                Some(sch.clone()),
                &[batch(&sch, &[1])],
                Some(&tail),
            )
            .unwrap_err();
        assert!(err.to_string().contains("disk on fire"));
        assert!(!st.has_pending());
        assert!(tail.appends.lock().unwrap().is_empty());
        // The mark never moved either: no watermark could cover the failure.
        assert_eq!(st.snapshot_pending(&ident()).2, None);
    }

    // The flush success path truncates at exactly the generation's mark
    // (tail_truncate_covered is the function flush_table calls), and a
    // markless / tailless flush truncates nothing.
    #[test]
    fn flush_success_truncates_at_generation_mark() {
        let st = BufferState::default();
        let sch = schema();
        let tail = MockTail::default();
        // Two statements = two frames (seqs 1 and 2).
        st.append(
            &ident(),
            Some(sch.clone()),
            &[batch(&sch, &[1])],
            Some(&tail),
        )
        .unwrap();
        st.append(&ident(), None, &[batch(&sch, &[2])], Some(&tail))
            .unwrap();
        let (_batches, n, mark) = st.snapshot_pending(&ident());
        assert_eq!((n, mark), (2, Some(2)));
        // ... prepare + post succeed (mocked away), then:
        st.move_pending_to_flushed(&ident(), n, 7);
        tail_truncate_covered(Some(&tail), &ident(), mark);
        assert_eq!(*tail.truncates.lock().unwrap(), vec![(ident(), 2)]);
        // The covered watermark was recorded in the sidecar (second gate)
        // before the truncation.
        assert_eq!(*tail.watermarks.lock().unwrap(), vec![(ident(), 2)]);
        // No tail / no mark: no truncation attempted.
        tail_truncate_covered(None, &ident(), Some(9));
        tail_truncate_covered(Some(&tail), &ident(), None);
        assert_eq!(tail.truncates.lock().unwrap().len(), 1);
    }

    // Boot replay bookkeeping: recovered rows enter pending with no tail
    // re-append, and note_tail_high makes the next flush's watermark cover
    // their (already durable) sequences.
    #[test]
    fn replayed_rows_carry_their_recovered_sequences() {
        let st = BufferState::default();
        let sch = schema();
        st.append(&ident(), Some(sch.clone()), &[batch(&sch, &[5])], None)
            .unwrap();
        st.note_tail_high(&ident(), 41);
        let (_batches, n, mark) = st.snapshot_pending(&ident());
        assert_eq!((n, mark), (1, Some(41)));
        // note_tail_high never regresses the mark.
        st.note_tail_high(&ident(), 12);
        assert_eq!(st.snapshot_pending(&ident()).2, Some(41));
    }

    // FIX (r3-1): a FRAMELESS tail dir whose table cannot load from the
    // catalog still floors the sequence from the sidecar alone — the next
    // append lands ABOVE the committed watermark, never under it (where
    // the next crash-replay would drop it as already covered).
    #[test]
    fn frameless_unloadable_table_floors_from_sidecar() {
        let sch = schema();
        let tail = MockTail::default();
        // Sidecar 7 → floor 8: the next handed-out seq clears the watermark.
        apply_sidecar_seq_floor(&tail, &ident(), Some(7)).unwrap();
        assert_eq!(tail.append(&ident(), &[batch(&sch, &[1])]).unwrap(), 8);
        // No sidecar: nothing to floor from, and no error either.
        let bare = MockTail::default();
        apply_sidecar_seq_floor(&bare, &ident(), None).unwrap();
        assert_eq!(bare.append(&ident(), &[batch(&sch, &[1])]).unwrap(), 1);
    }

    // FIX (r3-5): the prepare-time already-committed guard fires exactly
    // when the stamped watermark covers the generation's whole mark —
    // boundary: equality fires (all seqs covered), one below does not
    // (this generation holds at least one uncovered seq), and an absent
    // property never fires.
    #[test]
    fn already_committed_guard_boundary() {
        assert!(!generation_already_committed(None, 1));
        assert!(!generation_already_committed(Some(6), 7));
        assert!(generation_already_committed(Some(7), 7));
        assert!(generation_already_committed(Some(9), 7));
    }

    // Row-count accounting survives a move-out then move-back round trip.
    #[test]
    fn row_accounting_survives_roundtrip() {
        let st = BufferState::default();
        let sch = schema();
        let (_r, total0) = st
            .append(
                &ident(),
                Some(sch.clone()),
                &[batch(&sch, &[1, 2, 3])],
                None,
            )
            .unwrap();
        assert_eq!(total0, 3);
        let (_b, n, _) = st.snapshot_pending(&ident());
        st.move_pending_to_flushed(&ident(), n, 1);
        // Appending after the move must not corrupt the count.
        let (_r2, total1) = st
            .append(&ident(), None, &[batch(&sch, &[4])], None)
            .unwrap();
        assert_eq!(total1, 1); // only the new row is pending
        st.move_flushed_back_to_pending(&ident(), 1);
        // Back to 4 pending rows, in order.
        let ov = st.overlay_with(&ident(), |_| false).unwrap();
        assert_eq!(ids(&ov), vec![1, 2, 3, 4]);
    }
}
