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
//!   node-loss durability (see `tail.rs`). Tail appends are STAGED (frame
//!   written, sequence assigned) and the flush snapshot taken under the
//!   buffer lock, so per-table pending order == tail sequence order and
//!   `tail_high` is exact for any snapshot — the invariant the watermark
//!   depends on; the durability WAIT (fsync / round trip) runs after the
//!   lock drops, so concurrent statements coalesce into the local WAL's
//!   group fsync instead of serializing behind one another (tail.rs,
//!   "Group fsync"). A statement that fails at the WAIT stage errors AND
//!   its routed rows are removed from the window (exact failure; see
//!   [`BufferState::unroute_failed`]) unless a flush snapshot claimed them
//!   first — that narrow window keeps the old disclosed ambiguity and
//!   WARNs loudly. During the staging window the rows are transiently
//!   visible to same-server union reads (acceptable for buffered mode —
//!   its reads are already ahead of the lake by design).
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
//! * **Keyed tail upserts (Phase 2, docs/sota-roadmap.md §4)**: a table
//!   with `icegres.primary-key` + `icegres.tail-upsert = "true"` (and a
//!   durable tail attached) additionally acks exact-PK-equality autocommit
//!   UPDATE/DELETE from the tail instead of fencing: the statement resolves
//!   the key's current row through the union view, fsyncs ONE keyed frame,
//!   and records the op in a per-table keyed map (per-key last-writer-wins
//!   within the window). **Tail sequence order is the total order for a
//!   key**: a plain INSERT on a keyed-activated table routes each row whose
//!   key currently has a keyed-map entry into a NEWER upsert entry (live
//!   path and boot replay run the SAME routing inside
//!   [`BufferState::append`]), so a delete-then-reinsert of one key in one
//!   window leaves the row present with the inserted values. The flusher
//!   coalesces the map into the SAME window commit as pending inserts
//!   (`[Append(pending), Delete(keys), Append(replacements)]` — pending
//!   appends fold through the later delete, and by the routing above such
//!   an append is always OLDER than the keyed op that folds it away), and
//!   the overlay gains key suppression: committed rows and older buffered
//!   layers whose key has a newer keyed op are hidden (cache.rs applies the
//!   committed-side filter via `KeySuppressExec`). Anything not exactly
//!   keyed-shaped falls back to the fence path — which, on a
//!   keyed-activated table, runs its flush + synchronous execution under
//!   the SAME per-table keyed-serial lock a keyed read-modify-write holds
//!   across its union-read → tail-frame window (and an explicit-txn COMMIT
//!   touching keyed-activated tables acquires those locks in sorted ident
//!   order), so a committed synchronous write can never be clobbered by a
//!   keyed statement's stale full-row image. Lock ORDER (deadlock-critical):
//!   keyed-serial → `flush_lock` → the tables mutex; a keyed-serial lock is
//!   never acquired while holding either of the others.
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

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context as _, Result};
use arrow::array::{BooleanArray, RecordBatch};
use arrow::compute::filter_record_batch;
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
use crate::keyed;
use crate::overwrite::{
    align_batch, apply_dml_to_batches, pk_columns_of, pk_columns_of_metadata, prepare_commit,
    quote_ident, CommitOutcome, DmlKind, DmlStatement, OverwriteEngine, TableOp,
    MAX_COMMIT_ATTEMPTS,
};
use crate::tail::{
    drop_stale_frames, effective_watermark, parse_watermark_property, TailOp, TailOpKind, TailStore,
};
use crate::txn::{insert_target, plan_insert_rows, TxnRegistry};

/// Flush early once a table has this many pending buffered rows (bounds
/// buffer memory under a hot writer regardless of the flush cadence).
/// Overridable via `ICEGRES_WRITE_BUFFER_MAX_ROWS`.
const DEFAULT_MAX_ROWS: usize = 50_000;

/// Retain committed `flushed(S)` generations this long before garbage
/// collection (see the union-read correctness protocol in the module docs).
const FLUSHED_GC: Duration = Duration::from_secs(30);

/// What a buffered keyed op does to its key (Phase 2, keyed tail upserts).
#[derive(Clone)]
pub(crate) enum KeyedKind {
    /// The key's full replacement row (one row, canonical schema).
    Upsert(RecordBatch),
    /// The key is deleted.
    Delete,
}

/// One buffered keyed op: the key's PK values (one-row batch of the PK
/// columns in canonical types — the flusher's SQL-literal source and the
/// tail delete payload) plus what happens to the key.
#[derive(Clone)]
pub(crate) struct KeyedOp {
    key_row: RecordBatch,
    kind: KeyedKind,
}

/// A keyed map entry: the op plus a per-table monotonic stamp. The stamp is
/// what makes flush snapshots drain EXACTLY what they committed — a key
/// overwritten after the snapshot keeps its newer entry pending
/// (last-writer-wins), and a conflicted flush merges back without ever
/// clobbering a later write.
struct KeyedEntry {
    stamp: u64,
    op: KeyedOp,
}

/// Per-table keyed state (present once the first keyed op lands).
struct KeyedState {
    /// The declared `icegres.primary-key` columns (canonical names).
    pk_cols: Vec<String>,
    /// Encoded PK -> latest op (per-key last-writer-wins within the window).
    entries: HashMap<Vec<u8>, KeyedEntry>,
    next_stamp: u64,
    /// Stamps BELOW this were taken by a flush snapshot (every snapshot
    /// covers the whole live map, so this is simply `next_stamp` at the
    /// last snapshot). A claimed stamp belongs to an in-flight (or
    /// completed) commit — a failing statement may no longer unroute it
    /// (see [`BufferState::unroute_failed`]).
    claimed_below: u64,
}

/// How a flushed generation's commit is recognizable in scan metadata —
/// the overlay excludes the generation exactly when the metadata a scan
/// just loaded provably contains its rows.
#[derive(Clone, Copy, PartialEq, Debug)]
enum GenCommit {
    /// The normal case: the generation was committed as this snapshot id;
    /// metadata containing the snapshot contains the rows.
    Snapshot(i64),
    /// The prepare-time already-committed guard's case (an earlier
    /// AMBIGUOUS flush of these rows actually landed, snapshot id unknown):
    /// the rows are committed at-or-before tail watermark `mark`, so
    /// metadata whose OWN tail watermark property is `>= mark` contains
    /// them — exact, with no snapshot-id guess that could postdate the
    /// real commit and double-count rows for scans of intermediate
    /// snapshots.
    CoveredByWatermark(u64),
}

/// One committed-but-possibly-not-yet-observed flush generation.
struct FlushedGen {
    commit: GenCommit,
    batches: Vec<RecordBatch>,
    /// The keyed ops this generation committed (key, stamp, op) — scans
    /// whose metadata predates the commit still need their suppression and
    /// upsert rows; a conflicted post merges them back stamp-aware.
    keyed: Vec<(Vec<u8>, u64, KeyedOp)>,
    committed_at: Instant,
}

/// One pending append batch. `id` ties the batch to the statement that
/// routed it (the wait-failure unroute, [`BufferState::unroute_failed`]);
/// `claimed` is set the moment a flush snapshot takes the batch — a
/// claimed batch belongs to an in-flight (or completed) commit and may no
/// longer be removed by its failing statement, only drained/moved by the
/// flush bookkeeping. Because every snapshot claims the WHOLE pending vec
/// and new batches append behind it, claimed batches always form a prefix
/// — which is what keeps the flush's `drain(..n_batches)` positional
/// accounting exact in the presence of unroutes (they only ever remove
/// unclaimed batches, i.e. batches strictly behind any claimed prefix).
struct PendingBatch {
    id: u64,
    claimed: bool,
    batch: RecordBatch,
}

/// Per-table buffer state.
struct TableBuf {
    /// Canonical (field-id annotated) Arrow schema every buffered batch is
    /// aligned to — identical to what the committed scan produces.
    schema: ArrowSchemaRef,
    /// Acked rows not yet handed to a catalog commit, in insert order.
    pending: Vec<PendingBatch>,
    pending_rows: usize,
    /// Monotonic id source for [`PendingBatch::id`].
    next_batch_id: u64,
    /// Keyed tail ops of the current window (Phase 2); `None` until the
    /// first keyed op touches the table.
    keyed: Option<KeyedState>,
    /// Committed generations kept until every scan can see them (GC'd).
    flushed: Vec<FlushedGen>,
    /// Highest durable-tail sequence reflected in this buffer (None when no
    /// tail is attached). Because tail appends happen under the same lock
    /// that orders `pending`, this is exactly the max sequence of any
    /// pending-snapshot taken now — the flush commit's watermark.
    tail_high: Option<u64>,
}

/// Keyed suppression a scan must apply to its committed data (and that the
/// overlay's own rows were already filtered by, layer-aware): every key
/// with a buffered update or delete hides its committed row.
pub struct OverlaySuppress {
    pub pk_cols: Vec<String>,
    pub keys: Arc<HashSet<Vec<u8>>>,
}

/// The overlay a scan must union with its committed data (see cache.rs).
/// `batches` are ready to serve as-is (keyed suppression between buffer
/// layers is already applied); `suppress` — when present — must additionally
/// filter the committed scan (cache.rs wraps it in a `KeySuppressExec`).
pub struct Overlay {
    pub schema: ArrowSchemaRef,
    pub batches: Vec<RecordBatch>,
    pub suppress: Option<OverlaySuppress>,
}

/// A consistent snapshot of one table's pending work (append prefix +
/// keyed ops + the tail watermark), taken under one lock acquisition.
struct PendingSnapshot {
    batches: Vec<RecordBatch>,
    n_batches: usize,
    /// `(key, stamp, op)` sorted by stamp (arrival order).
    keyed: Vec<(Vec<u8>, u64, KeyedOp)>,
    tail_mark: Option<u64>,
}

impl PendingSnapshot {
    fn is_empty(&self) -> bool {
        self.n_batches == 0 && self.keyed.is_empty()
    }
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
    /// touch. With a `tail`, the aligned batches are STAGED onto it (frame
    /// written, sequence assigned) under the buffer lock — a staging error
    /// leaves `pending` untouched and becomes the statement's error — then
    /// the durability wait ([`StagedAppend::wait_durable`], the fsync /
    /// round trip) runs AFTER the lock is dropped, so concurrent statements
    /// coalesce into the tail's group fsync instead of serializing behind
    /// one another. Nothing is ever acked from memory alone: this returns
    /// (and the caller acks) only after the wait succeeds. Staging + the
    /// window bookkeeping run under ONE lock acquisition, which keeps
    /// pending order == tail sequence order AND tail_high exact for any
    /// flush snapshot (the watermark invariant). Error-path semantics: a
    /// failure at the WAIT stage (a dying disk) errors the statement AND
    /// removes its routed rows from the window ([`unroute_failed`]
    /// (Self::unroute_failed)) — exact failure — UNLESS a flush snapshot
    /// already claimed them (narrow window: the failure is then AMBIGUOUS,
    /// the rows may still commit, and this WARNs loudly with the tail
    /// sequence). During the staging window (between routing and the wait)
    /// the rows are transiently visible to same-server union reads —
    /// acceptable for buffered mode, whose reads are already ahead of the
    /// lake. The frame itself never replays and its sequence is never
    /// reused (tail.rs burned-sequence rule), so replay stays exact either
    /// way. Returns `(rows_appended, pending_rows_total)`.
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
                    next_batch_id: 1,
                    keyed: None,
                    flushed: Vec::new(),
                    tail_high: None,
                },
            );
        }
        let entry = tables.get_mut(ident).expect("just ensured present");
        // ICEGRES_QUERY_TIMING tail-ack budget: `buffer_append` = the whole
        // statement append (align + staged tail append + window bookkeeping
        // + the durability wait; the tail backend logs its own
        // encode/durability split), `buffer_route` = the in-memory
        // bookkeeping alone. One cached bool load when unset (timing.rs).
        let timing = crate::timing::enabled();
        let append_started = timing.then(std::time::Instant::now);
        let aligned: Vec<RecordBatch> = batches
            .iter()
            .map(|b| align_batch(b, &entry.schema))
            .collect::<Result<_>>()?;
        let staged = match tail {
            // ONE frame for the whole statement (all batches, one fsync,
            // one sequence): all-or-nothing by construction — a staging
            // failure leaves NO frame (tail.rs rolls back partial bytes),
            // so rows of a failed statement can never replay. The frame
            // stays a plain Append even when rows are routed into upsert
            // entries below: boot replay pushes Append frames back through
            // THIS function in sequence order, so the same routing rebuilds
            // the identical split.
            Some(tail) if !aligned.is_empty() => {
                let staged = tail.append_staged(ident, TailOpKind::Append, &aligned)?;
                entry.tail_high = Some(staged.seq());
                Some(staged)
            }
            _ => None,
        };
        let rows: usize = aligned.iter().map(|b| b.num_rows()).sum();
        let t = timing.then(std::time::Instant::now);
        let routed = route_appends(entry, aligned)?;
        if let Some(t) = t {
            crate::timing::record("buffer_route", t.elapsed());
        }
        let pending_rows = entry.pending_rows;
        // Durability wait OUTSIDE the buffer lock (the group-fsync win).
        drop(tables);
        if let Some(staged) = staged {
            let seq = staged.seq();
            if let Err(e) = staged.wait_durable() {
                self.unroute_failed(ident, seq, routed);
                return Err(e);
            }
        }
        if let Some(t) = append_started {
            crate::timing::record("buffer_append", t.elapsed());
        }
        Ok((rows, pending_rows))
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
        tables.values().any(|t| {
            !t.pending.is_empty() || t.keyed.as_ref().is_some_and(|k| !k.entries.is_empty())
        })
    }

    /// Idents with pending rows or keyed ops (flush work list).
    fn pending_idents(&self) -> Vec<TableIdent> {
        let tables = self.tables.lock().expect("write-buffer lock poisoned");
        tables
            .iter()
            .filter(|(_, t)| {
                !t.pending.is_empty() || t.keyed.as_ref().is_some_and(|k| !k.entries.is_empty())
            })
            .map(|(ident, _)| ident.clone())
            .collect()
    }

    /// The current keyed op for `key`: `Some(Some(row))` = buffered upsert
    /// (row = the key's current value), `Some(None)` = buffered delete,
    /// `None` = no keyed entry (resolve through the union view instead).
    /// Test-only view of the raw map; production reads go through
    /// [`keyed_current`](Self::keyed_current), which adds the pk-cols
    /// declaration check.
    #[cfg(test)]
    fn keyed_lookup(&self, ident: &TableIdent, key: &[u8]) -> Option<Option<RecordBatch>> {
        let tables = self.tables.lock().expect("write-buffer lock poisoned");
        let entry = tables.get(ident)?;
        let keyed = entry.keyed.as_ref()?;
        keyed.entries.get(key).map(|e| match &e.op.kind {
            KeyedKind::Upsert(row) => Some(row.clone()),
            KeyedKind::Delete => None,
        })
    }

    /// The key's current version when it is PROVABLY resolvable from the
    /// live keyed map (the keyed-map RMW shortcut, write-latency scope
    /// item 3): `Some(Some(row))` = current row, `Some(None)` = currently
    /// deleted, `None` = not resolvable here — read through the union view.
    ///
    /// Only the LIVE map qualifies, and that is load-bearing: a map entry
    /// is strictly newer than any pending append of its key (the routing
    /// invariant, [`route_appends`]), and every serialized sync write on a
    /// keyed table drains the map FIRST (`try_serialized_sync_dml` fences
    /// via flush under L1 before committing), so a live entry can never be
    /// staler than a local sync commit. A keyed op retained in a FLUSHED
    /// generation has neither property — a sync write may have committed a
    /// newer value for the key after the generation's commit while it is
    /// still window-retained, and a re-inserted key may sit in `pending` or
    /// a newer generation's batches (a duplicate only the engine read is
    /// honest about) — so window-retained generations were deliberately
    /// NOT made a fast-path source; their keys resolve through the union
    /// view, which composes all of this correctly.
    ///
    /// A `pk_cols` mismatch with the buffered keyed state declines: the
    /// map's keys were encoded under a different declaration and equal
    /// bytes would not prove an equal logical key (the caller's
    /// keyed_write later fails loudly on the change, exactly as before).
    fn keyed_current(
        &self,
        ident: &TableIdent,
        key: &[u8],
        pk_cols: &[String],
    ) -> Option<Option<RecordBatch>> {
        let tables = self.tables.lock().expect("write-buffer lock poisoned");
        let entry = tables.get(ident)?;
        let keyed = entry.keyed.as_ref()?;
        if keyed.pk_cols != pk_cols {
            return None;
        }
        keyed.entries.get(key).map(|e| match &e.op.kind {
            KeyedKind::Upsert(row) => Some(row.clone()),
            KeyedKind::Delete => None,
        })
    }

    /// Record one keyed op: durable tail append FIRST (under this lock, so
    /// keyed seq order == map order — the watermark invariant), then the
    /// map insert (per-key last-writer-wins). Creates the table entry /
    /// keyed state on first touch. Returns `(keyed_entries, pending_rows)`
    /// for the flush-kick heuristic.
    #[allow(clippy::too_many_arguments)]
    fn keyed_write(
        &self,
        ident: &TableIdent,
        schema_if_first: Option<ArrowSchemaRef>,
        pk_cols: &[String],
        key: Vec<u8>,
        op: KeyedOp,
        tail: Option<&dyn TailStore>,
    ) -> Result<(usize, usize)> {
        let mut tables = self.tables.lock().expect("write-buffer lock poisoned");
        if !tables.contains_key(ident) {
            let schema = schema_if_first
                .ok_or_else(|| anyhow!("write-buffer schema for {ident} disappeared"))?;
            tables.insert(
                ident.clone(),
                TableBuf {
                    schema,
                    pending: Vec::new(),
                    pending_rows: 0,
                    next_batch_id: 1,
                    keyed: None,
                    flushed: Vec::new(),
                    tail_high: None,
                },
            );
        }
        let entry = tables.get_mut(ident).expect("just ensured present");
        if entry.keyed.is_none() {
            entry.keyed = Some(KeyedState {
                pk_cols: pk_cols.to_vec(),
                entries: HashMap::new(),
                next_stamp: 1,
                claimed_below: 1,
            });
        }
        // S3: a pk_cols mismatch is only fatal while keys encoded under the
        // OLD declaration are still buffered (live entries, or flushed
        // generations whose keyed ops still overlay/merge back) — those keys
        // would no longer address the same rows. Otherwise the property
        // legitimately changed between windows: refresh and proceed instead
        // of failing keyed ops until restart.
        let old_keys_buffered = entry.keyed.as_ref().is_some_and(|k| !k.entries.is_empty())
            || entry.flushed.iter().any(|g| !g.keyed.is_empty());
        {
            let keyed = entry.keyed.as_mut().expect("just ensured present");
            if keyed.pk_cols != pk_cols {
                if old_keys_buffered {
                    // The primary-key property changed while keyed ops were
                    // pending: the buffered keys would no longer address the
                    // same rows.
                    bail_pk_changed(ident, &keyed.pk_cols, pk_cols)?;
                }
                keyed.pk_cols = pk_cols.to_vec();
            }
        }
        // ICEGRES_QUERY_TIMING: the keyed ack's durable half — tail append
        // (backend logs its own encode/durability split) + map insert.
        let write_started = crate::timing::enabled().then(std::time::Instant::now);
        // Staged like BufferState::append: frame written + seq assigned +
        // map insert under THIS lock acquisition (keyed seq order == map
        // order — the watermark invariant), the durability wait after the
        // lock drops. Same error-path semantics as append: a WAIT-stage
        // failure errors the statement AND unroutes its map entry
        // (restoring whatever earlier acked entry it displaced) — exact
        // failure — unless a flush snapshot already claimed the entry
        // (narrow window: disclosed ambiguity, WARNed loudly). The frame
        // never replays and its sequence is never reused either way.
        let staged = match tail {
            Some(tail) => {
                let (kind, frame): (TailOpKind, &RecordBatch) = match &op.kind {
                    KeyedKind::Upsert(row) => (TailOpKind::Upsert, row),
                    KeyedKind::Delete => (TailOpKind::Delete, &op.key_row),
                };
                let staged = tail.append_staged(ident, kind, std::slice::from_ref(frame))?;
                entry.tail_high = Some(staged.seq());
                Some(staged)
            }
            None => None,
        };
        let keyed = entry.keyed.as_mut().expect("just ensured present");
        let stamp = keyed.next_stamp;
        keyed.next_stamp += 1;
        let displaced = keyed.entries.insert(key.clone(), KeyedEntry { stamp, op });
        let routed = RoutedStmt {
            batch_ids: Vec::new(),
            keyed: vec![(key, stamp, displaced)],
        };
        let counts = (keyed.entries.len(), entry.pending_rows);
        drop(tables);
        if let Some(staged) = staged {
            let seq = staged.seq();
            if let Err(e) = staged.wait_durable() {
                self.unroute_failed(ident, seq, routed);
                return Err(e);
            }
        }
        if let Some(t) = write_started {
            crate::timing::record("keyed_write", t.elapsed());
        }
        Ok(counts)
    }

    /// The wait-failure unroute: a statement whose tail durability wait
    /// failed has already errored to its client — its routed rows must not
    /// silently commit. Under the buffer lock, remove EXACTLY what the
    /// statement routed (its pending batches; its keyed entries, restoring
    /// whatever earlier acked entry each displaced), making the failure
    /// exact — UNLESS a flush snapshot already claimed any of it (the
    /// narrow staging-to-failure window): then nothing is touched (partial
    /// removal would make the outcome worse than the disclosed ambiguity)
    /// and this WARNs loudly with the burned tail sequence. A key
    /// overwritten by a NEWER statement is left alone either way — the
    /// newer op supersedes the failed value regardless. Known bounded
    /// residual: if TWO statements fail their waits on the same key in one
    /// window, the later one's restore can resurrect the earlier one's
    /// failed entry (both clients errored; only reachable while the tail
    /// is already dying).
    fn unroute_failed(&self, ident: &TableIdent, seq: u64, routed: RoutedStmt) {
        let mut tables = self.tables.lock().expect("write-buffer lock poisoned");
        let Some(entry) = tables.get_mut(ident) else {
            return;
        };
        // Claim check first, all-or-nothing. A snapshot claims the WHOLE
        // window at once, so a statement's routed state is either fully
        // unclaimed or fully claimed — mixed states only arise vs. batches
        // of OTHER statements, never within one.
        let ids: HashSet<u64> = routed.batch_ids.iter().copied().collect();
        let mut present_unclaimed = 0usize;
        for pb in &entry.pending {
            if ids.contains(&pb.id) && !pb.claimed {
                present_unclaimed += 1;
            }
        }
        let batches_claimed = present_unclaimed != ids.len();
        let keyed_claimed = routed.keyed.iter().any(|(key, stamp, _)| {
            match entry.keyed.as_ref() {
                // Missing state with routed keyed ops = drained wholesale.
                None => true,
                // Claimed when a snapshot took our stamp, or the key
                // vanished from the live map entirely (only a snapshot
                // drain removes keys — an overwrite keeps the key present
                // under a newer stamp, which is NOT ambiguous: the failed
                // value was displaced and can no longer flush).
                Some(state) => *stamp < state.claimed_below || !state.entries.contains_key(key),
            }
        });
        if batches_claimed || keyed_claimed {
            tracing::warn!(
                table = %ident,
                tail_seq = seq,
                "a statement's tail durability wait FAILED after a flush \
                 snapshot claimed its rows — AMBIGUOUS failure: the client \
                 saw an error but the rows may still commit with the \
                 in-flight flush (the tail frame itself never replays and \
                 sequence {seq} is burned, so replay stays exactly-once)"
            );
            return;
        }
        // Exact removal: pending batches out, keyed entries removed or
        // rolled back to what they displaced.
        if !ids.is_empty() {
            let mut removed_rows = 0usize;
            entry.pending.retain(|pb| {
                if ids.contains(&pb.id) {
                    removed_rows += pb.batch.num_rows();
                    false
                } else {
                    true
                }
            });
            entry.pending_rows -= removed_rows;
        }
        for (key, stamp, displaced) in routed.keyed {
            let Some(state) = entry.keyed.as_mut() else {
                unreachable!("checked above: keyed state present");
            };
            match state.entries.get(&key) {
                Some(e) if e.stamp == stamp => {
                    match displaced {
                        Some(prev) => {
                            state.entries.insert(key, prev);
                        }
                        None => {
                            state.entries.remove(&key);
                        }
                    };
                }
                // A newer statement overwrote the key: the failed value
                // can no longer flush on its own — leave the newer op.
                _ => {}
            }
        }
        tracing::warn!(
            table = %ident,
            tail_seq = seq,
            "a statement's tail durability wait FAILED; its routed rows were \
             removed from the buffer window (exact failure — nothing of the \
             statement will commit; sequence {seq} is burned, never reused)"
        );
    }

    /// Snapshot the current pending prefix AND keyed map WITHOUT removing
    /// anything (rows stay readable through the union while the commit is
    /// in flight; new writes land behind/over the snapshot). The tail mark
    /// is taken under the same lock, so it is exact for this snapshot.
    /// Everything snapshotted is marked CLAIMED (batches and keyed stamps):
    /// from here on it belongs to the flush bookkeeping and a failing
    /// statement may no longer unroute it (see [`unroute_failed`]
    /// (Self::unroute_failed)).
    fn snapshot_pending(&self, ident: &TableIdent) -> PendingSnapshot {
        let mut tables = self.tables.lock().expect("write-buffer lock poisoned");
        match tables.get_mut(ident) {
            Some(entry) => {
                let mut keyed: Vec<(Vec<u8>, u64, KeyedOp)> = match &entry.keyed {
                    Some(k) => k
                        .entries
                        .iter()
                        .map(|(key, e)| (key.clone(), e.stamp, e.op.clone()))
                        .collect(),
                    None => Vec::new(),
                };
                keyed.sort_by_key(|(_, stamp, _)| *stamp);
                for pb in entry.pending.iter_mut() {
                    pb.claimed = true;
                }
                if let Some(state) = entry.keyed.as_mut() {
                    state.claimed_below = state.next_stamp;
                }
                PendingSnapshot {
                    batches: entry.pending.iter().map(|pb| pb.batch.clone()).collect(),
                    n_batches: entry.pending.len(),
                    keyed,
                    tail_mark: entry.tail_high,
                }
            }
            None => PendingSnapshot {
                batches: Vec::new(),
                n_batches: 0,
                keyed: Vec::new(),
                tail_mark: None,
            },
        }
    }

    /// The union overlay for one table: pending rows, keyed upsert rows, and
    /// every committed generation the scan's metadata cannot see yet —
    /// layered so newer keyed ops suppress older layers' rows for the same
    /// key (and a layer's own keyed ops suppress its own appended rows,
    /// exactly as the flush composes them). `is_committed` is the caller's
    /// test of whether the scan's metadata already contains a generation
    /// (by snapshot membership, or — for a generation parked by the
    /// already-committed guard — by the metadata's own tail watermark; see
    /// [`GenCommit`]). `None` when there is nothing to add or suppress.
    fn overlay_with(
        &self,
        ident: &TableIdent,
        is_committed: impl Fn(&GenCommit) -> bool,
    ) -> Result<Option<Overlay>> {
        let tables = self.tables.lock().expect("write-buffer lock poisoned");
        let Some(entry) = tables.get(ident) else {
            return Ok(None);
        };
        struct Layer {
            appends: Vec<RecordBatch>,
            upserts: Vec<RecordBatch>,
            keys: HashSet<Vec<u8>>,
        }
        let mut layers: Vec<Layer> = Vec::new();
        for flushed_gen in &entry.flushed {
            if is_committed(&flushed_gen.commit) {
                continue;
            }
            layers.push(Layer {
                appends: flushed_gen.batches.clone(),
                upserts: flushed_gen
                    .keyed
                    .iter()
                    .filter_map(|(_, _, op)| match &op.kind {
                        KeyedKind::Upsert(row) => Some(row.clone()),
                        KeyedKind::Delete => None,
                    })
                    .collect(),
                keys: flushed_gen
                    .keyed
                    .iter()
                    .map(|(k, _, _)| k.clone())
                    .collect(),
            });
        }
        layers.push(Layer {
            appends: entry.pending.iter().map(|pb| pb.batch.clone()).collect(),
            upserts: entry
                .keyed
                .iter()
                .flat_map(|k| k.entries.values())
                .filter_map(|e| match &e.op.kind {
                    KeyedKind::Upsert(row) => Some(row.clone()),
                    KeyedKind::Delete => None,
                })
                .collect(),
            keys: entry
                .keyed
                .iter()
                .flat_map(|k| k.entries.keys().cloned())
                .collect(),
        });
        // Fast path (no keyed ops anywhere): plain concatenation, no
        // suppression — byte-for-byte the pre-Phase-2 overlay.
        if layers.iter().all(|l| l.keys.is_empty()) {
            let batches: Vec<RecordBatch> = layers.into_iter().flat_map(|l| l.appends).collect();
            if batches.is_empty() {
                return Ok(None);
            }
            return Ok(Some(Overlay {
                schema: entry.schema.clone(),
                batches,
                suppress: None,
            }));
        }
        // The keyed state struct survives flushes (only its entries drain),
        // so keyed layers imply it exists — but a missing PK set must fail
        // loudly, never mis-serve.
        let pk_cols = entry
            .keyed
            .as_ref()
            .map(|k| k.pk_cols.clone())
            .ok_or_else(|| anyhow!("keyed generations without keyed state for {ident}"))?;
        // suffix_keys[i] = keys of every layer NEWER than i.
        let n = layers.len();
        let mut suffix: Vec<HashSet<Vec<u8>>> = vec![HashSet::new(); n];
        for i in (0..n.saturating_sub(1)).rev() {
            let mut s = suffix[i + 1].clone();
            s.extend(layers[i + 1].keys.iter().cloned());
            suffix[i] = s;
        }
        let mut batches: Vec<RecordBatch> = Vec::new();
        for (i, layer) in layers.iter().enumerate() {
            // A layer's own keyed ops suppress its own appends (the flush
            // composes [Append, Delete-by-key, Append-upserts], folding the
            // delete over the appends); newer layers suppress everything.
            let mut own_and_newer = suffix[i].clone();
            own_and_newer.extend(layer.keys.iter().cloned());
            for b in &layer.appends {
                let filtered = keyed::suppress_batch(b, &pk_cols, &own_and_newer)?;
                if filtered.num_rows() > 0 {
                    batches.push(filtered);
                }
            }
            for u in &layer.upserts {
                let filtered = keyed::suppress_batch(u, &pk_cols, &suffix[i])?;
                if filtered.num_rows() > 0 {
                    batches.push(filtered);
                }
            }
        }
        let mut all_keys: HashSet<Vec<u8>> = HashSet::new();
        for layer in &layers {
            all_keys.extend(layer.keys.iter().cloned());
        }
        if batches.is_empty() && all_keys.is_empty() {
            return Ok(None);
        }
        Ok(Some(Overlay {
            schema: entry.schema.clone(),
            batches,
            suppress: (!all_keys.is_empty()).then(|| OverlaySuppress {
                pk_cols,
                keys: Arc::new(all_keys),
            }),
        }))
    }

    fn drop_pending_prefix(
        &self,
        ident: &TableIdent,
        n_batches: usize,
        keyed: &[(Vec<u8>, u64, KeyedOp)],
    ) {
        let mut tables = self.tables.lock().expect("write-buffer lock poisoned");
        if let Some(entry) = tables.get_mut(ident) {
            let dropped: Vec<RecordBatch> = entry
                .pending
                .drain(..n_batches)
                .map(|pb| pb.batch)
                .collect();
            entry.pending_rows -= dropped.iter().map(|b| b.num_rows()).sum::<usize>();
            drain_keyed_snapshot(entry, keyed);
        }
    }

    fn move_pending_to_flushed(
        &self,
        ident: &TableIdent,
        n_batches: usize,
        keyed: &[(Vec<u8>, u64, KeyedOp)],
        snapshot_id: i64,
    ) {
        self.move_pending_to_flushed_gen(ident, n_batches, keyed, GenCommit::Snapshot(snapshot_id));
    }

    /// L3: park a generation the prepare-time already-committed guard
    /// proved committed by an EARLIER ambiguous flush — its snapshot id is
    /// unknown, so it is tagged with the covering tail watermark instead of
    /// a guessed (possibly too-young) snapshot that would hide the rows
    /// from scans of intermediate snapshots and double-count them for
    /// scans at the guessed head.
    fn move_pending_to_flushed_covered(
        &self,
        ident: &TableIdent,
        n_batches: usize,
        keyed: &[(Vec<u8>, u64, KeyedOp)],
        mark: u64,
    ) {
        self.move_pending_to_flushed_gen(
            ident,
            n_batches,
            keyed,
            GenCommit::CoveredByWatermark(mark),
        );
    }

    fn move_pending_to_flushed_gen(
        &self,
        ident: &TableIdent,
        n_batches: usize,
        keyed: &[(Vec<u8>, u64, KeyedOp)],
        commit: GenCommit,
    ) {
        let mut tables = self.tables.lock().expect("write-buffer lock poisoned");
        if let Some(entry) = tables.get_mut(ident) {
            let batches: Vec<RecordBatch> = entry
                .pending
                .drain(..n_batches)
                .map(|pb| pb.batch)
                .collect();
            entry.pending_rows -= batches.iter().map(|b| b.num_rows()).sum::<usize>();
            drain_keyed_snapshot(entry, keyed);
            entry.flushed.push(FlushedGen {
                commit,
                batches,
                keyed: keyed.to_vec(),
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
                .position(|g| g.commit == GenCommit::Snapshot(snapshot_id))
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
            // They stay CLAIMED (they belonged to acked statements a flush
            // took; the next snapshot re-claims everything anyway).
            let mut restored: Vec<PendingBatch> = flushed_gen
                .batches
                .into_iter()
                .map(|batch| {
                    let id = entry.next_batch_id;
                    entry.next_batch_id += 1;
                    PendingBatch {
                        id,
                        claimed: true,
                        batch,
                    }
                })
                .collect();
            restored.append(&mut entry.pending);
            entry.pending = restored;
            // Keyed merge-back, last-writer-wins: a key updated AFTER the
            // snapshot keeps its newer entry — only absent keys restore
            // (with their original stamps, still unique and older than any
            // current stamp by construction).
            if !flushed_gen.keyed.is_empty() {
                let keyed = entry
                    .keyed
                    .as_mut()
                    .expect("keyed generation implies keyed state");
                for (key, stamp, op) in flushed_gen.keyed {
                    keyed.entries.entry(key).or_insert(KeyedEntry { stamp, op });
                }
            }
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

/// What one statement routed into the buffer window — the undo record for
/// the wait-failure path ([`BufferState::unroute_failed`]): the pending
/// batch ids it pushed, and the keyed entries it created as
/// `(key, its stamp, the entry it displaced)`.
#[derive(Default)]
struct RoutedStmt {
    batch_ids: Vec<u64>,
    keyed: Vec<(Vec<u8>, u64, Option<KeyedEntry>)>,
}

/// Push one aligned batch into `pending`, tagged for unroute.
fn push_pending(entry: &mut TableBuf, routed: &mut RoutedStmt, batch: RecordBatch) {
    let id = entry.next_batch_id;
    entry.next_batch_id += 1;
    routed.batch_ids.push(id);
    entry.pending_rows += batch.num_rows();
    entry.pending.push(PendingBatch {
        id,
        claimed: false,
        batch,
    });
}

/// Route one statement's aligned append batches into the table buffer
/// (L2 — tail sequence order is the total order for keyed keys): a row
/// whose PK currently has a keyed-map entry supersedes it as a NEWER
/// Upsert entry (stamped now, i.e. at this statement's position in the
/// per-table sequence order); rows without a map entry stay plain pending
/// appends. Multi-row statements split accordingly. Tables without
/// buffered keyed ops take the plain-append fast path and pay nothing.
/// Live inserts and boot replay both land here (via
/// [`BufferState::append`]), so replay rebuilds the identical routing by
/// construction. Returns the statement's undo record ([`RoutedStmt`]).
fn route_appends(entry: &mut TableBuf, aligned: Vec<RecordBatch>) -> Result<RoutedStmt> {
    let mut routed = RoutedStmt::default();
    if entry.keyed.as_ref().is_none_or(|k| k.entries.is_empty()) {
        for batch in aligned {
            push_pending(entry, &mut routed, batch);
        }
        return Ok(routed);
    }
    for batch in aligned {
        if batch.num_rows() == 0 {
            continue;
        }
        let keyed = entry.keyed.as_mut().expect("checked above");
        let keys = keyed::encode_batch_keys(&batch, &keyed.pk_cols)?;
        let hit: Vec<bool> = keys.iter().map(|k| keyed.entries.contains_key(k)).collect();
        if hit.iter().all(|h| !h) {
            push_pending(entry, &mut routed, batch);
            continue;
        }
        let key_rows = keyed::project_key_rows(&batch, &keyed.pk_cols)?;
        for (row, key) in keys.iter().enumerate().filter(|&(r, _)| hit[r]) {
            let stamp = keyed.next_stamp;
            keyed.next_stamp += 1;
            let displaced = keyed.entries.insert(
                key.clone(),
                KeyedEntry {
                    stamp,
                    op: KeyedOp {
                        key_row: key_rows.slice(row, 1),
                        kind: KeyedKind::Upsert(batch.slice(row, 1)),
                    },
                },
            );
            routed.keyed.push((key.clone(), stamp, displaced));
        }
        if hit.iter().all(|h| *h) {
            continue;
        }
        let mask = BooleanArray::from_iter(hit.iter().map(|h| Some(!h)));
        let filtered = filter_record_batch(&batch, &mask)
            .map_err(|e| anyhow!("cannot split a routed insert batch: {e}"))?;
        // The invariant the overlay's own-keys-suppress-own-appends rule
        // and the flush's fold-through-delete rely on: a row entering
        // `pending` never carries a key with a CURRENT map entry — every
        // keyed-map entry is therefore strictly NEWER than any pending
        // append of its key.
        debug_assert!(
            keyed::encode_batch_keys(&filtered, &keyed.pk_cols)
                .map(|ks| ks.iter().all(|k| !keyed.entries.contains_key(k)))
                .unwrap_or(false),
            "a pending append must never carry a keyed-map key"
        );
        push_pending(entry, &mut routed, filtered);
    }
    Ok(routed)
}

/// Remove exactly the snapshotted keyed entries from the live map: an entry
/// is removed only when its stamp still matches — a key overwritten after
/// the snapshot keeps its NEWER entry pending (it is not covered by the
/// commit being accounted for).
fn drain_keyed_snapshot(entry: &mut TableBuf, keyed: &[(Vec<u8>, u64, KeyedOp)]) {
    if keyed.is_empty() {
        return;
    }
    let Some(state) = entry.keyed.as_mut() else {
        return;
    };
    for (key, stamp, _) in keyed {
        if state.entries.get(key).is_some_and(|e| e.stamp == *stamp) {
            state.entries.remove(key);
        }
    }
}

/// The `icegres.primary-key` property changed while keyed ops were pending
/// for the table — fail the statement loudly (the buffered keys would no
/// longer address the same rows). Cold path, factored for the error text.
fn bail_pk_changed(ident: &TableIdent, had: &[String], now: &[String]) -> Result<()> {
    Err(anyhow!(
        "the primary key of {ident} changed from {had:?} to {now:?} while keyed tail \
         ops were pending; flush the buffer (any fenced statement) before changing \
         the {} property",
        crate::overwrite::PK_PROPERTY
    ))
}

/// Per-table keyed serialization locks (L1). One lock per keyed-activated
/// table serializes every keyed read-modify-write's union-read → tail-frame
/// window against (a) other keyed statements on the table, (b) the fenced
/// synchronous UPDATE/DELETE path, and (c) explicit-txn COMMITs touching
/// the table — so a committed synchronous write can never fall between a
/// keyed statement's read and its frame and be clobbered by the stale
/// full-row image at flush.
///
/// Lock ORDER (deadlock-critical, documented in the module docs): a
/// keyed-serial lock is acquired FIRST — before `flush_lock` (the fenced
/// path flushes while holding it) and before the tables mutex; neither is
/// ever held while acquiring keyed-serial. Multi-table COMMITs acquire
/// their keyed-serial locks in sorted ident order.
#[derive(Default)]
struct KeyedSerial {
    locks: StdMutex<HashMap<TableIdent, Arc<tokio::sync::Mutex<()>>>>,
}

impl KeyedSerial {
    /// Get-or-create the table's lock (cloning the `Arc` so the caller can
    /// `lock().await` without holding the map mutex).
    fn lock_for(&self, ident: &TableIdent) -> Arc<tokio::sync::Mutex<()>> {
        let mut locks = self.locks.lock().expect("keyed-serial lock poisoned");
        locks
            .entry(ident.clone())
            .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
            .clone()
    }
}

/// One cached keyed-activation decision (S5), keyed by metadata location.
struct ActivationEntry {
    metadata_location: Option<String>,
    activated: bool,
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
    /// Per-table keyed serialization (L1; see [`KeyedSerial`]).
    keyed_serial: KeyedSerial,
    /// S5 — per-table keyed-activation cache, so the fallback path never
    /// pays a per-statement catalog `load_table` for tables that never
    /// opted in. The caching rule: an entry is recorded from every place a
    /// fresh `load_table` is otherwise unavoidable — a keyed-shaped
    /// statement's own activation load, the fenced sync path's one-time
    /// resolution, a buffered INSERT's first-touch schema load, boot
    /// replay, and every scan's snapshot check (cache.rs calls
    /// [`note_activation`](Self::note_activation) whenever it rebuilds its
    /// provider from fresh metadata, which any commit — local or foreign —
    /// triggers). It is invalidated wholesale by property-changing DDL
    /// through this server (the BufferHook fence arm); a property flipped
    /// by a FOREIGN writer is picked up by the next scan's rebuild. Staleness
    /// is fail-safe in both directions: a stale "not activated" routes to
    /// the always-correct synchronous path; a stale "activated" costs one
    /// keyed-path load that re-checks the live property and re-records.
    activation: StdMutex<HashMap<TableIdent, ActivationEntry>>,
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
            keyed_serial: KeyedSerial::default(),
            activation: StdMutex::new(HashMap::new()),
            kick: tokio::sync::Notify::new(),
        }
    }

    /// The per-table keyed serialization lock (L1; see [`KeyedSerial`]).
    /// `pub(crate)` so the transaction hook can serialize its COMMIT
    /// against in-flight keyed read-modify-writes.
    pub(crate) fn keyed_serial_lock(&self, ident: &TableIdent) -> Arc<tokio::sync::Mutex<()>> {
        self.keyed_serial.lock_for(ident)
    }

    /// Record the keyed-activation decision fresh table metadata implies
    /// (S5 — see the `activation` field for the caching rule). Cheap and
    /// idempotent: an unchanged metadata location short-circuits.
    pub fn note_activation(
        &self,
        ident: &TableIdent,
        metadata_location: Option<&str>,
        metadata: &TableMetadata,
    ) {
        let mut map = self.activation.lock().expect("activation lock poisoned");
        if let Some(entry) = map.get(ident) {
            if metadata_location.is_some()
                && entry.metadata_location.as_deref() == metadata_location
            {
                return;
            }
        }
        let activated =
            keyed::property_is_true(metadata.properties().get(keyed::TAIL_UPSERT_PROPERTY));
        map.insert(
            ident.clone(),
            ActivationEntry {
                metadata_location: metadata_location.map(str::to_string),
                activated,
            },
        );
    }

    /// The cached keyed-activation decision, if any (S5).
    fn cached_activation(&self, ident: &TableIdent) -> Option<bool> {
        self.activation
            .lock()
            .expect("activation lock poisoned")
            .get(ident)
            .map(|e| e.activated)
    }

    /// Drop every cached activation decision (S5): called by the fence arm
    /// for property-changing DDL, so an `ALTER TABLE ... tail-upsert`
    /// through this server takes effect on the very next statement.
    pub fn invalidate_activation(&self) {
        self.activation
            .lock()
            .expect("activation lock poisoned")
            .clear();
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
            // A load we pay anyway: record the activation decision (S5).
            self.note_activation(&ident, table.metadata_location(), table.metadata());
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
                // Best-effort disk cleanup of the covered frames — through
                // the SAME watermark-record-then-truncate discipline as the
                // flush path (FIX C1a). The boot path used to truncate from
                // the LAKE property alone: with no watermark record ever
                // written (the crash landed between the property stamp and
                // record_watermark), the truncate deleted the table's ONLY
                // records, the next restart replayed nothing for it, the
                // sequence floor never applied, and freshly acked rows were
                // silently dropped as already-committed. record_watermark
                // BEFORE truncate retains the table's trace; a failed
                // watermark record skips the truncate (bounded leak, zero
                // loss — same rule as the flush path).
                tail_truncate_covered(Some(tail.as_ref()), &ident, watermark);
            }
            let Some((max_seq, _)) = survivors.last() else {
                continue;
            };
            let max_seq = *max_seq;
            let schema = Arc::new(
                schema_to_arrow_schema(table.metadata().current_schema())
                    .map_err(|e| anyhow!("schema conversion failed for {ident}: {e}"))?,
            );
            // Keyed frames need the table's PK declaration to rebuild the
            // map. Read it lazily (only when a keyed frame appears) so
            // plain-append tables replay exactly as before.
            let mut pk_cols: Option<Vec<String>> = None;
            let mut rows_here = 0usize;
            // Process frames IN SEQUENCE ORDER: appends re-enter `pending`,
            // keyed ops rebuild the map last-writer-wins. No tail below:
            // the frames are already durable. align_batch fails loudly if
            // the table's schema evolved past the frames.
            for (seq, op) in survivors {
                match op {
                    TailOp::Append(batches) => {
                        let (rows, _) = self
                            .state
                            .append(&ident, Some(schema.clone()), &batches, None)
                            .with_context(|| {
                                format!("tail replay: cannot re-buffer rows for {ident}")
                            })?;
                        rows_here += rows;
                    }
                    TailOp::Upsert(_) | TailOp::Delete(_) => {
                        let pk = match &pk_cols {
                            Some(pk) => pk.clone(),
                            None => {
                                let pk = pk_columns_of(&table)?.ok_or_else(|| {
                                    anyhow!(
                                        "tail replay: {ident} holds keyed frames (acked \
                                     updates/deletes) but declares no {} property; \
                                     restore the property, or delete the table's tail \
                                     state to acknowledge losing them",
                                        crate::overwrite::PK_PROPERTY
                                    )
                                })?;
                                pk_cols = Some(pk.clone());
                                pk
                            }
                        };
                        rows_here += self
                            .replay_keyed_frame(&ident, &schema, &pk, seq, op)
                            .with_context(|| {
                                format!("tail replay: cannot re-buffer keyed op for {ident}")
                            })?;
                    }
                }
            }
            self.state.note_tail_high(&ident, max_seq);
            recovered_rows += rows_here;
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

    /// Rebuild one keyed tail frame into the keyed map (boot replay; frames
    /// arrive in sequence order, so plain map insertion IS last-writer-wins).
    /// Returns the number of keyed ops applied.
    fn replay_keyed_frame(
        &self,
        ident: &TableIdent,
        schema: &ArrowSchemaRef,
        pk_cols: &[String],
        _seq: u64,
        op: TailOp,
    ) -> Result<usize> {
        let mut applied = 0usize;
        match op {
            TailOp::Upsert(batches) => {
                for batch in batches {
                    let aligned = align_batch(&batch, schema)?;
                    let key_rows = keyed::project_key_rows(&aligned, pk_cols)?;
                    let keys = keyed::encode_batch_keys(&aligned, pk_cols)?;
                    for (row, key) in keys.into_iter().enumerate() {
                        let op = KeyedOp {
                            key_row: key_rows.slice(row, 1),
                            kind: KeyedKind::Upsert(aligned.slice(row, 1)),
                        };
                        self.state.keyed_write(
                            ident,
                            Some(schema.clone()),
                            pk_cols,
                            key,
                            op,
                            None,
                        )?;
                        applied += 1;
                    }
                }
            }
            TailOp::Delete(batches) => {
                for batch in batches {
                    // Key-only frame: re-align by name onto the canonical PK
                    // types (schema evolution fails loudly, like align_batch).
                    let key_batch = keyed::align_key_batch(&batch, schema, pk_cols)?;
                    let keys = keyed::encode_batch_keys(&key_batch, pk_cols)?;
                    for (row, key) in keys.into_iter().enumerate() {
                        let op = KeyedOp {
                            key_row: key_batch.slice(row, 1),
                            kind: KeyedKind::Delete,
                        };
                        self.state.keyed_write(
                            ident,
                            Some(schema.clone()),
                            pk_cols,
                            key,
                            op,
                            None,
                        )?;
                        applied += 1;
                    }
                }
            }
            TailOp::Append(_) => unreachable!("the caller routes append frames"),
        }
        Ok(applied)
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
            // A load we pay anyway: record the activation decision (S5).
            self.note_activation(ident, table.metadata_location(), table.metadata());
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

    /// Keyed tail path for one autocommit UPDATE/DELETE (Phase 2,
    /// docs/sota-roadmap.md §4). `Ok(None)` = not eligible — the caller
    /// falls back to the unchanged fence-then-synchronous path. Eligible =
    /// the statement is an exact-PK-equality UPDATE (literal SETs) or
    /// DELETE, AND the table opts in (`icegres.tail-upsert=true` + declared
    /// `icegres.primary-key` of renderable types), AND a durable tail is
    /// attached. The ack is: resolve the key's current row through the same
    /// union view a scan sees, apply the statement, fsync ONE keyed frame
    /// to the tail, insert into the keyed map — no Iceberg commit until the
    /// flush window closes.
    pub async fn try_keyed_dml(
        &self,
        stmt: &Statement,
        ctx: &SessionContext,
    ) -> Result<Option<Response>> {
        // Activation requires a durable tail (acked keyed ops must survive
        // an unclean kill exactly like buffered inserts).
        if self.tail.is_none() {
            return Ok(None);
        }
        let Some(cand) = keyed::parse_keyed_candidate(stmt) else {
            return Ok(None);
        };
        // S5: a cached "not activated" decision skips the catalog load
        // entirely — an exact-PK-shaped sync DML on a never-opted table
        // costs no new per-statement catalog round trip (see the
        // `activation` field for the caching rule).
        if self.cached_activation(&cand.ident) == Some(false) {
            return Ok(None);
        }
        // ICEGRES_QUERY_TIMING keyed-ack budget (cached bool when unset):
        // `keyed_gate` = the activation catalog load, `keyed_rmw_read` = the
        // current-row resolution, `keyed_apply` = folding the statement over
        // it, `keyed_write` (keyed_write fn) = durable tail append + map
        // insert, `keyed_total` = the whole eligible statement.
        let timing = crate::timing::enabled();
        let keyed_started = timing.then(std::time::Instant::now);
        // Activation gate. With `--freshness-ms` on and the table's cache
        // fresh, the gate serves the freshness-cached metadata with NO
        // catalog round trip — the same bounded-staleness contract every
        // read already rides (local writes/DDL invalidate synchronously,
        // foreign property flips land within the bound). Otherwise (default
        // mode, or a stale cache) one `load_table`, exactly as before.
        let (metadata_location, metadata) =
            match crate::freshness::provider(&crate::freshness::table_key(&cand.ident))
                .and_then(|p| p.fresh_metadata())
            {
                Some((location, metadata)) => (location, metadata),
                None => match self.catalog.load_table(&cand.ident).await {
                    Ok(t) => (t.metadata_location().map(str::to_string), t.metadata_ref()),
                    // Unknown table etc.: let the normal path produce the
                    // error.
                    Err(_) => return Ok(None),
                },
            };
        if let Some(t) = keyed_started {
            crate::timing::record("keyed_gate", t.elapsed());
        }
        self.note_activation(&cand.ident, metadata_location.as_deref(), &metadata);
        if !keyed::property_is_true(metadata.properties().get(keyed::TAIL_UPSERT_PROPERTY)) {
            return Ok(None);
        }
        // The table opted in: a broken PK declaration is now a loud error,
        // not a silent fallback.
        let Some(pk_cols) = pk_columns_of_metadata(&metadata, &cand.ident.to_string())? else {
            return Ok(None);
        };
        // WHERE must bind EVERY PK column exactly once and nothing else
        // (S1: quoted identifiers bind only on an exact-case match); SET
        // must not move the key.
        if cand.eq.len() != pk_cols.len() {
            return Ok(None);
        }
        for pk in &pk_cols {
            let bound = cand
                .eq
                .iter()
                .filter(|(c, _)| keyed::key_col_matches(c, pk))
                .count();
            if bound != 1 {
                return Ok(None);
            }
        }
        if cand
            .assigned
            .iter()
            .any(|a| pk_cols.iter().any(|p| p.eq_ignore_ascii_case(a)))
        {
            return Ok(None);
        }
        let schema = Arc::new(
            schema_to_arrow_schema(metadata.current_schema())
                .map_err(|e| anyhow!("schema conversion failed for {}: {e}", cand.ident))?,
        );
        let pk_idx = keyed::pk_indices(&schema, &pk_cols)?;
        if pk_idx
            .iter()
            .any(|&i| !keyed::key_type_supported(schema.field(i).data_type()))
        {
            return Ok(None);
        }
        // The key in canonical types. A literal the PK type cannot represent
        // falls back — the synchronous path surfaces the same cast error the
        // planner would.
        let Ok(key_batch) = keyed::literals_to_key_batch(&cand.eq, &pk_cols, &schema) else {
            return Ok(None);
        };
        let key = keyed::encode_batch_keys(&key_batch, &pk_cols)?
            .into_iter()
            .next()
            .ok_or_else(|| anyhow!("empty key batch"))?;
        // S2: validate the statement against the schema BEFORE the key
        // lookup, exactly as the sync path does at prepare time — an
        // unknown SET column or un-castable SET literal errors identically
        // whether or not the key matches (never a silent `UPDATE 0`).
        crate::overwrite::validate_dml_against_schema(&cand.dml, &schema)
            .await
            .with_context(|| format!("DML against {} failed", cand.ident))?;

        // L1: serialize this read-modify-write PER TABLE — against other
        // keyed statements AND against the fenced synchronous path / txn
        // COMMITs on keyed-activated tables (which take the same lock), so
        // nothing can commit between the union read below and the tail
        // frame and then be clobbered by our stale full-row image at flush.
        let serial = self.keyed_serial.lock_for(&cand.ident);
        let _rmw = serial.lock().await;
        let predicate = cand
            .dml
            .predicate
            .as_deref()
            .expect("keyed candidates always carry a predicate");
        // Resolve the key's current row: the keyed map is the newest layer
        // (fast path, no IO); otherwise read through the same union view a
        // scan sees (committed + pending + unseen flushed generations, with
        // keyed suppression applied by the scan path itself).
        let rmw_started = timing.then(std::time::Instant::now);
        let current: Vec<RecordBatch> =
            match self.state.keyed_current(&cand.ident, &key, &pk_cols) {
                Some(Some(row)) => vec![row],
                Some(None) => Vec::new(), // deleted earlier in this window
                None => {
                    let from = match &cand.dml.alias {
                        Some(alias) => format!(
                            "{}.{} AS {}",
                            quote_ident(&cand.dml.namespace),
                            quote_ident(&cand.dml.table),
                            quote_ident(alias)
                        ),
                        None => format!(
                            "{}.{}",
                            quote_ident(&cand.dml.namespace),
                            quote_ident(&cand.dml.table)
                        ),
                    };
                    let sql = format!("SELECT * FROM {from} WHERE ({predicate}) LIMIT 2");
                    let df = ctx.sql(&sql).await.map_err(|e| {
                        anyhow!("keyed lookup for {} failed to plan: {e}", cand.ident)
                    })?;
                    df.collect()
                        .await
                        .map_err(|e| anyhow!("keyed lookup for {} failed: {e}", cand.ident))?
                }
            };
        if let Some(t) = rmw_started {
            crate::timing::record("keyed_rmw_read", t.elapsed());
        }
        let current_rows: usize = current.iter().map(|b| b.num_rows()).sum();
        if current_rows == 0 {
            // Missing key: 0 rows affected, nothing stored (same answer the
            // synchronous path gives).
            return Ok(Some(Response::Execution(Tag::new(cand.tag).with_rows(0))));
        }
        if current_rows > 1 {
            // Duplicate keys exist (declaration != enforcement): only the
            // synchronous path is honest about multi-row effects.
            return Ok(None);
        }
        let apply_started = timing.then(std::time::Instant::now);
        let nonempty: Vec<RecordBatch> = current.into_iter().filter(|b| b.num_rows() > 0).collect();
        let current_row = align_batch(
            &arrow::compute::concat_batches(&nonempty[0].schema(), &nonempty)
                .map_err(|e| anyhow!("cannot assemble the current row: {e}"))?,
            &schema,
        )?;
        let columns: Vec<String> = schema.fields().iter().map(|f| f.name().clone()).collect();
        let kind = match &cand.dml.kind {
            DmlKind::Delete => {
                // S1: re-evaluate the ORIGINAL predicate against the
                // resolved current row exactly as the UPDATE branch does —
                // any residual divergence between key derivation and the
                // engine's comparison semantics becomes an honest DELETE 0
                // instead of deleting a row sync would not match.
                let (matched, out) =
                    apply_dml_to_batches(&cand.dml, &columns, vec![current_row.clone()]).await?;
                if matched == 0 {
                    return Ok(Some(Response::Execution(Tag::new(cand.tag).with_rows(0))));
                }
                let rows_out: usize = out.iter().map(|b| b.num_rows()).sum();
                anyhow::ensure!(
                    matched == 1 && rows_out == 0,
                    "keyed DELETE row accounting mismatch: 1 row in, {matched} matched, \
                     {rows_out} out"
                );
                KeyedKind::Delete
            }
            DmlKind::Update { .. } => {
                let (matched, out) =
                    apply_dml_to_batches(&cand.dml, &columns, vec![current_row.clone()]).await?;
                if matched == 0 {
                    // The predicate re-evaluated false on the resolved row
                    // (e.g. a cast quirk): honest 0-row answer.
                    return Ok(Some(Response::Execution(Tag::new(cand.tag).with_rows(0))));
                }
                let rows_out: usize = out.iter().map(|b| b.num_rows()).sum();
                anyhow::ensure!(
                    rows_out == 1,
                    "keyed UPDATE row accounting mismatch: 1 row in, {rows_out} out"
                );
                let replacement = align_batch(
                    &arrow::compute::concat_batches(&out[0].schema(), &out)
                        .map_err(|e| anyhow!("cannot assemble the replacement row: {e}"))?,
                    &schema,
                )?;
                KeyedKind::Upsert(replacement)
            }
        };
        if let Some(t) = apply_started {
            crate::timing::record("keyed_apply", t.elapsed());
        }
        // Durable tail append + map insert, under the buffer lock (ordering
        // vs. inserts) — the statement's ack rides on this succeeding.
        let (keyed_total, pending_rows) = self.state.keyed_write(
            &cand.ident,
            Some(schema),
            &pk_cols,
            key,
            KeyedOp {
                key_row: key_batch,
                kind,
            },
            self.tail.as_deref(),
        )?;
        if keyed_total + pending_rows >= self.max_rows {
            self.kick.notify_one();
        }
        if let Some(t) = keyed_started {
            crate::timing::record("keyed_total", t.elapsed());
        }
        Ok(Some(Response::Execution(Tag::new(cand.tag).with_rows(1))))
    }

    /// L1(b): the fenced synchronous UPDATE/DELETE arm for KEYED-ACTIVATED
    /// tables. `Ok(None)` = the table is not keyed-activated (or the
    /// statement is a shape the DML hook must reject itself) — the caller
    /// falls through to the plain fence + DmlHook path unchanged. When it
    /// IS activated, the fence flush and the synchronous execution run
    /// under the table's keyed-serial lock, so no keyed read-modify-write
    /// can slip its union read between them and later clobber this
    /// statement's committed effect with a stale full-row image.
    pub async fn try_serialized_sync_dml(&self, stmt: &Statement) -> Result<Option<Response>> {
        // Without a tail no table can be keyed-activated on this server —
        // nothing to serialize against.
        if self.tail.is_none() {
            return Ok(None);
        }
        // Unsupported shapes fall through so DmlHook produces its own
        // rejection (identical errors, nothing executed => no race).
        let Ok(Some((dml, tag))) = crate::dml::translate(stmt) else {
            return Ok(None);
        };
        let Ok(ident) = TableIdent::from_strs([dml.namespace.as_str(), dml.table.as_str()]) else {
            return Ok(None);
        };
        let activated = match self.cached_activation(&ident) {
            Some(a) => a,
            // Unknown table: resolve once (amortized — the decision is
            // cached under the caching rule on the `activation` field;
            // never-opted tables pay no per-statement load).
            None => match self.catalog.load_table(&ident).await {
                Ok(t) => {
                    self.note_activation(&ident, t.metadata_location(), t.metadata());
                    keyed::property_is_true(
                        t.metadata().properties().get(keyed::TAIL_UPSERT_PROPERTY),
                    )
                }
                Err(_) => return Ok(None),
            },
        };
        if !activated {
            return Ok(None);
        }
        // Keyed-serial FIRST, then the fence flush (which takes flush_lock)
        // — the documented lock order. Held across the synchronous commit.
        let serial = self.keyed_serial.lock_for(&ident);
        let _serial = serial.lock().await;
        if self.state.has_pending() {
            self.flush_now()
                .await
                .context("write-buffer flush (required before this statement) failed")?;
        }
        let outcome = self.engine.execute(&dml).await?;
        Ok(Some(Response::Execution(
            Tag::new(tag).with_rows(outcome.rows as usize),
        )))
    }

    /// The union overlay for one table against the committed metadata a
    /// scan just loaded: all pending rows plus committed generations that
    /// metadata cannot see yet. `None` when there is nothing to add
    /// (fast path — scans are unchanged when the buffer is idle).
    pub fn overlay(&self, ident: &TableIdent, metadata: &TableMetadata) -> Result<Option<Overlay>> {
        // L3: a watermark-tagged generation (parked by the prepare-time
        // already-committed guard) is contained in metadata exactly when
        // the metadata's OWN tail watermark property covers its mark.
        let scan_mark = self.tail.as_deref().and_then(|t| {
            parse_watermark_property(
                ident,
                metadata
                    .properties()
                    .get(t.watermark_property())
                    .map(String::as_str),
            )
        });
        // A generation `S` is "committed" (and so already in the scan's data)
        // exactly when the just-loaded metadata contains it.
        self.state.overlay_with(ident, |commit| match commit {
            GenCommit::Snapshot(s) => metadata.snapshot_by_id(*s).is_some(),
            GenCommit::CoveredByWatermark(mark) => scan_mark.is_some_and(|w| w >= *mark),
        })
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

    /// Group-commit one table's pending rows AND coalesced keyed ops as ONE
    /// snapshot, with bounded optimistic-concurrency retries (fresh metadata
    /// per attempt, exactly like autocommit INSERT). Keyed ops compose as
    /// `[Append(pending), Delete(all keyed keys), Append(upsert rows)]` —
    /// appends fold through the later delete, and by the insert-routing
    /// invariant (L2, [`route_appends`]) a pending append whose key has a
    /// keyed entry is always OLDER than that entry, so the fold only ever
    /// erases rows a later keyed op superseded — exactly as the overlay
    /// hides them (an insert acked AFTER the keyed op became the key's
    /// upsert row instead and survives); N updates to one hot row become
    /// ONE file rewrite.
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
            // Snapshot the current pending prefix + keyed map WITHOUT
            // removing anything: the rows must stay readable through the
            // union view while the commit is in flight. New inserts append
            // behind the prefix; newer keyed writes overwrite by stamp.
            // `tail_mark` is the generation's exact watermark (taken under
            // the same lock, see snapshot_pending).
            let snap = self.state.snapshot_pending(ident);
            if snap.is_empty() {
                return Ok(());
            }
            let PendingSnapshot {
                batches,
                n_batches,
                keyed: keyed_snapshot,
                tail_mark,
            } = snap;
            // LOAD-BEARING: reload the table metadata on EVERY attempt. The
            // fresh properties feed the generation_already_committed guard
            // below, and the fresh snapshot pins the commit's
            // assert-ref-snapshot-id CAS. Together these — NOT the tail's
            // one-writer lock, which is only best-effort boot-time
            // exclusion and releases with a dead tail connection — are
            // what make a flush racing a replacement writer on the same
            // tail unable to double-apply.
            let table = self
                .catalog
                .load_table(ident)
                .await
                .map_err(|e| anyhow!("failed to load table {ident}: {e}"))?;
            let pk = self.engine.pk_columns(&table)?;
            let rows: usize =
                batches.iter().map(|b| b.num_rows()).sum::<usize>() + keyed_snapshot.len();
            // Compose the window: pending inserts first, then ONE delete
            // covering every keyed key (updated AND deleted), then the
            // replacement rows of the upserts. `prepare_commit` folds each
            // Append through every LATER Dml op, so ordering is semantic.
            let mut ops: Vec<TableOp> = Vec::with_capacity(3);
            if !batches.is_empty() {
                ops.push(TableOp::Append(batches));
            }
            if !keyed_snapshot.is_empty() {
                let key_rows: Vec<RecordBatch> = keyed_snapshot
                    .iter()
                    .map(|(_, _, op)| op.key_row.clone())
                    .collect();
                let keys_batch = arrow::compute::concat_batches(&key_rows[0].schema(), &key_rows)
                    .map_err(|e| anyhow!("cannot concatenate keyed keys: {e}"))?;
                let predicate = keyed::render_keys_predicate(&keys_batch)?;
                ops.push(TableOp::Dml(DmlStatement {
                    kind: DmlKind::Delete,
                    namespace: ident.namespace().clone().inner().join("."),
                    table: ident.name().to_string(),
                    alias: None,
                    predicate: Some(predicate),
                }));
                let upserts: Vec<RecordBatch> = keyed_snapshot
                    .iter()
                    .filter_map(|(_, _, op)| match &op.kind {
                        KeyedKind::Upsert(row) => Some(row.clone()),
                        KeyedKind::Delete => None,
                    })
                    .collect();
                if !upserts.is_empty() {
                    ops.push(TableOp::Append(upserts));
                }
            }
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
                    // flushed generation tagged "covered at-or-before
                    // watermark <mark>" (L3) — NOT with a guessed snapshot
                    // id: the guard's branch head may POSTDATE the real
                    // commit, and a scan of an intermediate snapshot (own
                    // watermark < mark) must still overlay these rows while
                    // one at-or-past the real commit (watermark >= mark)
                    // must exclude them. Then sidecar + truncate the
                    // covered tail frames — the reload-failure residual
                    // heals on the next flush.
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
                        self.state.move_pending_to_flushed_covered(
                            ident,
                            n_batches,
                            &keyed_snapshot,
                            mark,
                        );
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
                // Zero net rows (e.g. a keyed delete of keys absent
                // everywhere): drop the snapshot, nothing to commit. The
                // covered tail frames also net zero rows on any future
                // replay, so forgetting them without a commit is safe.
                self.state
                    .drop_pending_prefix(ident, n_batches, &keyed_snapshot);
                tail_truncate_covered(self.tail.as_deref(), ident, tail_mark);
                return Ok(());
            };
            let snapshot_id = prepared.snapshot_id();
            // Tag the prefix as flushed(S) BEFORE posting: see the module
            // docs for why this ordering makes the union race-free.
            self.state
                .move_pending_to_flushed(ident, n_batches, &keyed_snapshot, snapshot_id);
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
/// rows), record the covered watermark in the sidecar (the second gate
/// against a foreign writer dropping the table property) and forget its
/// tail frames. Neither failure ever fails the flush (the committed
/// watermark keeps replay exactly-once regardless), but a FAILED watermark
/// record SKIPS the truncate: deleting the frames anyway could leave a
/// table — on its very first flush — with NEITHER frames NOR a watermark
/// row in the durable store, so replay would not report the table at all,
/// the `icegres.tail-seq.<id>` property floor would never be applied, and
/// post-restart sequences would restart UNDER the committed watermark (the
/// next crash-replay then silently drops those acked rows as covered).
/// Keeping the frames instead is a bounded leak with zero loss: replay
/// neutralizes them via the in-commit property watermark.
fn tail_truncate_covered(tail: Option<&dyn TailStore>, ident: &TableIdent, mark: Option<u64>) {
    let (Some(tail), Some(upto_seq)) = (tail, mark) else {
        return;
    };
    if let Err(e) = tail.record_watermark(ident, upto_seq) {
        tracing::warn!(
            table = %ident,
            upto_seq,
            "cannot record the tail watermark sidecar; SKIPPING the covered-frame \
             truncate for this generation so the table cannot vanish from the \
             durable store (frames stay until a later flush covers them; replay \
             drops them via the committed watermark — bounded leak, zero loss): {e:#}"
        );
        return;
    }
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
/// * autocommit UPDATE/DELETE by exact PK equality on an opted-in keyed
///   table (`icegres.tail-upsert`) is acked from the keyed tail;
/// * ordering fences (non-keyed UPDATE/DELETE, BEGIN, DDL, PK-enforced
///   INSERT) flush synchronously and fall through (`None`) to their normal
///   handler;
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
        // ICEGRES_QUERY_TIMING tail-ack budget: `insert_plan` = shaping the
        // rows through DataFusion, `buffer_ack_total` = the whole buffered
        // ack (plan + durable tail append + bookkeeping). Cached bool when
        // unset (timing.rs).
        let timing = crate::timing::enabled();
        let ack_started = timing.then(std::time::Instant::now);
        // Target must be a real Iceberg table (planning through the shared
        // context also validates this — insert_target is the cheap check).
        let ident = insert_target(stmt)?;
        let (plan_ident, batches) = plan_insert_rows(shared, stmt, params).await?;
        if let Some(t) = ack_started {
            crate::timing::record("insert_plan", t.elapsed());
        }
        anyhow::ensure!(
            plan_ident == ident,
            "INSERT target resolution mismatch: {plan_ident} vs {ident}"
        );
        let rows = self.buffer.buffer_insert(&ident, batches).await?;
        if let Some(t) = ack_started {
            crate::timing::record("buffer_ack_total", t.elapsed());
        }
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
            // UPDATE/DELETE: exact-PK-equality statements on an opted-in
            // table (icegres.tail-upsert + icegres.primary-key, durable
            // tail attached) ack from the keyed tail — no fence, no COW
            // commit until the flush window. Anything else on a
            // keyed-ACTIVATED table runs fence + synchronous execution
            // under the table's keyed-serial lock (L1 — a concurrent keyed
            // RMW must not clobber the committed sync write); anything else
            // again (incl. bind parameters, which the DML engine rejects
            // downstream) falls through to the fence below.
            Statement::Update { .. } | Statement::Delete(_) => {
                let has_params = params.is_some_and(|p| match p {
                    ParamValues::List(l) => !l.is_empty(),
                    ParamValues::Map(m) => !m.is_empty(),
                });
                if !has_params {
                    match self.buffer.try_keyed_dml(stmt, shared).await {
                        Ok(Some(resp)) => return Some(Ok(resp)),
                        Ok(None) => {} // not keyed-shaped: sync path below
                        Err(e) => return Some(Err(dml::engine_error(&e))),
                    }
                    match self.buffer.try_serialized_sync_dml(stmt).await {
                        Ok(Some(resp)) => return Some(Ok(resp)),
                        Ok(None) => {} // not keyed-activated: plain fence
                        Err(e) => return Some(Err(dml::engine_error(&e))),
                    }
                }
                self.fence().await
            }
            // Everything else — BEGIN, DDL, PK-enforced INSERT, COPY, ... —
            // is an ordering fence: acked rows must be committed before it
            // runs on its normal path. Property-changing DDL additionally
            // invalidates the keyed-activation cache (S5), so an
            // `ALTER TABLE ... icegres.tail-upsert` through this server is
            // honored by the very next statement.
            _ => {
                if matches!(
                    stmt,
                    Statement::AlterTable { .. }
                        | Statement::Drop { .. }
                        | Statement::CreateTable(_)
                ) {
                    self.buffer.invalidate_activation();
                }
                self.fence().await
            }
        }
    }

    /// The ordering fence: force a synchronous flush of every pending row
    /// and keyed op, then fall through (`None`) to the statement's normal
    /// handler. A flush failure is the statement's error.
    async fn fence(&self) -> Option<PgWireResult<Response>> {
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

    /// The old 3-tuple view of a pending snapshot (most tests only need
    /// the append side).
    fn snap3(st: &BufferState) -> (Vec<RecordBatch>, usize, Option<u64>) {
        let s = st.snapshot_pending(&ident());
        (s.batches, s.n_batches, s.tail_mark)
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
            .unwrap()
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
        let (_batches, n, _) = snap3(&st);
        assert_eq!(n, 1); // one batch
        let s: i64 = 4242;
        st.move_pending_to_flushed(&ident(), n, &[], s);
        // No pending rows left; the rows now live only in flushed(S).
        assert!(!st.has_pending());
        // Scan whose metadata already contains S: committed scan has the rows,
        // so the overlay must add NOTHING.
        assert!(st
            .overlay_with(&ident(), |c| *c == GenCommit::Snapshot(s))
            .unwrap()
            .is_none());
        // Scan whose metadata predates S: the committed scan lacks the rows,
        // so the overlay must supply them from the flushed generation.
        let ov = st
            .overlay_with(&ident(), |_| false)
            .unwrap()
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
        let (_b, n, _) = snap3(&st);
        assert_eq!(n, 2);
        let s: i64 = 99;
        st.move_pending_to_flushed(&ident(), n, &[], s);
        // A new insert C=[3] lands while the commit is "in flight".
        st.append(&ident(), None, &[batch(&sch, &[3])], None)
            .unwrap();
        // Commit conflicts: restore the flushed prefix to the front.
        st.move_flushed_back_to_pending(&ident(), s);
        // Row accounting and order: [1,2] restored ahead of [3].
        let ov = st
            .overlay_with(&ident(), |_| false)
            .unwrap()
            .expect("rows present");
        assert_eq!(ids(&ov), vec![1, 2, 3]);
        // And with the (now-abandoned) S considered committed, the flushed gen
        // is gone from flushed (it was moved back), so all three are pending.
        let ov2 = st
            .overlay_with(&ident(), |c| *c == GenCommit::Snapshot(s))
            .unwrap()
            .expect("rows present");
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
        let (_b, n, _) = snap3(&st);
        st.drop_pending_prefix(&ident(), n, &[]);
        assert!(!st.has_pending());
        assert!(st.overlay_with(&ident(), |_| false).unwrap().is_none());
    }

    // GC drops flushed generations by predicate (real code: older than
    // FLUSHED_GC). A kept generation still overlays for pre-S scans.
    #[test]
    fn retain_flushed_by_predicate() {
        let st = BufferState::default();
        let sch = schema();
        st.append(&ident(), Some(sch.clone()), &[batch(&sch, &[7])], None)
            .unwrap();
        let (_b, n, _) = snap3(&st);
        let s: i64 = 5;
        st.move_pending_to_flushed(&ident(), n, &[], s);
        // Keep-everything predicate: generation survives, still overlays.
        st.retain_flushed(|_| true);
        assert_eq!(
            ids(&st.overlay_with(&ident(), |_| false).unwrap().unwrap()),
            vec![7]
        );
        // Drop-everything predicate (stands in for "too old"): gone.
        st.retain_flushed(|_| false);
        assert!(st.overlay_with(&ident(), |_| false).unwrap().is_none());
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
        let (_b, n, _) = snap3(&st);
        st.move_pending_to_flushed(&ident(), n, &[], 1);
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
        fail_watermarks: std::sync::atomic::AtomicBool,
        /// (table, seq, batch_count, total_rows) per STATEMENT append.
        appends: StdMutex<Vec<(TableIdent, u64, usize, usize)>>,
        /// Op kind per append, aligned with `appends`.
        kinds: StdMutex<Vec<TailOpKind>>,
        truncates: StdMutex<Vec<(TableIdent, u64)>>,
        watermarks: StdMutex<Vec<(TableIdent, u64)>>,
        /// Interleaved watermark/truncate call log (order assertions).
        calls: StdMutex<Vec<String>>,
    }

    impl TailStore for MockTail {
        fn append(
            &self,
            table: &TableIdent,
            kind: TailOpKind,
            batches: &[RecordBatch],
        ) -> Result<u64> {
            if self.fail_appends.load(std::sync::atomic::Ordering::SeqCst) {
                return Err(anyhow!("mock tail: disk on fire"));
            }
            self.kinds.lock().unwrap().push(kind);
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
            self.calls
                .lock()
                .unwrap()
                .push(format!("truncate:{upto_seq}"));
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

        fn record_watermark(&self, table: &TableIdent, seq: u64) -> Result<()> {
            if self
                .fail_watermarks
                .load(std::sync::atomic::Ordering::SeqCst)
            {
                return Err(anyhow!("mock tail: watermark store on fire"));
            }
            self.calls.lock().unwrap().push(format!("watermark:{seq}"));
            self.watermarks.lock().unwrap().push((table.clone(), seq));
            Ok(())
        }
    }

    // FIX (C1a): the covered-frame cleanup records the watermark BEFORE the
    // truncate — the ORDER is the contract, not just both calls happening.
    // The boot replay path now runs through this same helper, so a
    // boot-time truncate can never delete a table's only frames without a
    // watermark record retaining its trace (and with it the seq floor).
    #[test]
    fn covered_cleanup_records_watermark_before_truncate() {
        let tail = MockTail::default();
        tail_truncate_covered(Some(&tail), &ident(), Some(3));
        assert_eq!(
            *tail.calls.lock().unwrap(),
            vec!["watermark:3".to_string(), "truncate:3".to_string()]
        );
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
        let (_batches, n, mark) = snap3(&st);
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
        assert_eq!(snap3(&st).2, None);
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
        let (_batches, n, mark) = snap3(&st);
        assert_eq!((n, mark), (2, Some(2)));
        // ... prepare + post succeed (mocked away), then:
        st.move_pending_to_flushed(&ident(), n, &[], 7);
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

    // FIX (phase1b-1): a FAILED watermark record must SKIP the truncate —
    // otherwise a table's first flush could delete its only frames while
    // leaving no watermark row, making the table vanish from the durable
    // store entirely (replay would never apply the property seq floor and
    // post-restart sequences would duck under the committed watermark).
    // Once the watermark records again, truncation proceeds normally.
    #[test]
    fn watermark_record_failure_skips_truncate() {
        let tail = MockTail::default();
        tail.fail_watermarks
            .store(true, std::sync::atomic::Ordering::SeqCst);
        tail_truncate_covered(Some(&tail), &ident(), Some(2));
        assert!(
            tail.truncates.lock().unwrap().is_empty(),
            "truncate must be skipped when the watermark record failed"
        );
        assert!(tail.watermarks.lock().unwrap().is_empty());
        // Recovery: the next covered flush records AND truncates.
        tail.fail_watermarks
            .store(false, std::sync::atomic::Ordering::SeqCst);
        tail_truncate_covered(Some(&tail), &ident(), Some(2));
        assert_eq!(*tail.watermarks.lock().unwrap(), vec![(ident(), 2)]);
        assert_eq!(*tail.truncates.lock().unwrap(), vec![(ident(), 2)]);
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
        let (_batches, n, mark) = snap3(&st);
        assert_eq!((n, mark), (1, Some(41)));
        // note_tail_high never regresses the mark.
        st.note_tail_high(&ident(), 12);
        assert_eq!(snap3(&st).2, Some(41));
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
        assert_eq!(
            tail.append(&ident(), TailOpKind::Append, &[batch(&sch, &[1])])
                .unwrap(),
            8
        );
        // No sidecar: nothing to floor from, and no error either.
        let bare = MockTail::default();
        apply_sidecar_seq_floor(&bare, &ident(), None).unwrap();
        assert_eq!(
            bare.append(&ident(), TailOpKind::Append, &[batch(&sch, &[1])])
                .unwrap(),
            1
        );
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
        let (_b, n, _) = snap3(&st);
        st.move_pending_to_flushed(&ident(), n, &[], 1);
        // Appending after the move must not corrupt the count.
        let (_r2, total1) = st
            .append(&ident(), None, &[batch(&sch, &[4])], None)
            .unwrap();
        assert_eq!(total1, 1); // only the new row is pending
        st.move_flushed_back_to_pending(&ident(), 1);
        // Back to 4 pending rows, in order.
        let ov = st.overlay_with(&ident(), |_| false).unwrap().unwrap();
        assert_eq!(ids(&ov), vec![1, 2, 3, 4]);
    }

    // -----------------------------------------------------------------------
    // PHASE 2 — keyed tail state: per-key last-writer-wins, layered overlay
    // suppression, stamp-exact snapshot drain/restore, replay rebuild.
    // -----------------------------------------------------------------------

    use arrow::array::StringArray;

    /// Two-column schema (id PK, val payload) for the keyed tests.
    fn kschema() -> ArrowSchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("val", DataType::Utf8, true),
        ]))
    }

    fn pk() -> Vec<String> {
        vec!["id".to_string()]
    }

    fn krow(id: i64, val: &str) -> RecordBatch {
        RecordBatch::try_new(
            kschema(),
            vec![
                Arc::new(Int64Array::from(vec![id])),
                Arc::new(StringArray::from(vec![val])),
            ],
        )
        .unwrap()
    }

    fn kbatch(rows: &[(i64, &str)]) -> RecordBatch {
        RecordBatch::try_new(
            kschema(),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
                )),
                Arc::new(StringArray::from(
                    rows.iter().map(|(_, v)| *v).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap()
    }

    fn key_row(id: i64) -> RecordBatch {
        krow(id, "").project(&[0]).unwrap()
    }

    fn key_of(id: i64) -> Vec<u8> {
        keyed::encode_batch_keys(&key_row(id), &pk())
            .unwrap()
            .remove(0)
    }

    fn write_upsert(st: &BufferState, id: i64, val: &str, tail: Option<&dyn TailStore>) {
        st.keyed_write(
            &ident(),
            Some(kschema()),
            &pk(),
            key_of(id),
            KeyedOp {
                key_row: key_row(id),
                kind: KeyedKind::Upsert(krow(id, val)),
            },
            tail,
        )
        .unwrap();
    }

    fn write_delete(st: &BufferState, id: i64, tail: Option<&dyn TailStore>) {
        st.keyed_write(
            &ident(),
            Some(kschema()),
            &pk(),
            key_of(id),
            KeyedOp {
                key_row: key_row(id),
                kind: KeyedKind::Delete,
            },
            tail,
        )
        .unwrap();
    }

    fn upserted_val(st: &BufferState, id: i64) -> String {
        let row = st
            .keyed_lookup(&ident(), &key_of(id))
            .expect("entry present")
            .expect("entry is an upsert");
        row.column(1)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap()
            .value(0)
            .to_string()
    }

    fn vals(ov: &Overlay) -> Vec<(i64, String)> {
        let mut out = Vec::new();
        for b in &ov.batches {
            let row_ids = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let vs = b.column(1).as_any().downcast_ref::<StringArray>().unwrap();
            for r in 0..b.num_rows() {
                out.push((row_ids.value(r), vs.value(r).to_string()));
            }
        }
        out.sort();
        out
    }

    // Keyed map LWW ordering: update-then-update keeps the second value,
    // update-then-delete deletes, delete-then-insert(=upsert) resurrects —
    // exactly what replay's in-seq-order rebuild produces too (boot replay
    // pushes keyed frames through this same keyed_write path in sequence
    // order).
    #[test]
    fn keyed_map_lww_ordering() {
        let st = BufferState::default();
        // update then update: second wins.
        write_upsert(&st, 1, "v1", None);
        write_upsert(&st, 1, "v2", None);
        assert_eq!(upserted_val(&st, 1), "v2");
        // update then delete: deleted.
        write_delete(&st, 1, None);
        assert!(st
            .keyed_lookup(&ident(), &key_of(1))
            .expect("entry present")
            .is_none());
        // delete then insert = upsert: the row is back.
        write_upsert(&st, 1, "v3", None);
        assert_eq!(upserted_val(&st, 1), "v3");
        // Unknown key: no entry.
        assert!(st.keyed_lookup(&ident(), &key_of(9)).is_none());
        // Keyed entries count as pending work.
        assert!(st.has_pending());
        assert_eq!(st.pending_idents(), vec![ident()]);
    }

    // Overlay suppression: a pending INSERT of a key that is then
    // keyed-deleted disappears from the overlay; a keyed upsert both adds
    // its new row AND suppresses the committed row (via `suppress`).
    #[test]
    fn overlay_suppresses_keyed_rows() {
        let st = BufferState::default();
        // Pending inserts id=1,2.
        st.append(
            &ident(),
            Some(kschema()),
            &[kbatch(&[(1, "x"), (2, "y")])],
            None,
        )
        .unwrap();
        // Keyed delete of 1 (and its committed twin), keyed update of 3.
        write_delete(&st, 1, None);
        write_upsert(&st, 3, "new", None);
        let ov = st.overlay_with(&ident(), |_| false).unwrap().unwrap();
        // Pending id=1 is hidden; id=2 survives; upsert row id=3 unions in.
        assert_eq!(
            vals(&ov),
            vec![(2, "y".to_string()), (3, "new".to_string())]
        );
        // The committed scan must suppress BOTH keyed keys.
        let sup = ov.suppress.expect("keyed suppression present");
        assert_eq!(sup.pk_cols, pk());
        assert!(sup.keys.contains(&key_of(1)));
        assert!(sup.keys.contains(&key_of(3)));
        assert!(!sup.keys.contains(&key_of(2)));
    }

    // A keyed flushed(S) generation overlays for scans whose metadata
    // predates S (rows + suppression) and vanishes once S is committed;
    // a NEWER pending keyed op suppresses the older generation's row.
    #[test]
    fn keyed_flushed_generation_layering() {
        let st = BufferState::default();
        write_upsert(&st, 1, "v1", None);
        let snap = st.snapshot_pending(&ident());
        assert_eq!(snap.keyed.len(), 1);
        let s: i64 = 77;
        st.move_pending_to_flushed(&ident(), 0, &snap.keyed, s);
        // Metadata predating S: the generation supplies row AND suppression.
        let ov = st.overlay_with(&ident(), |_| false).unwrap().unwrap();
        assert_eq!(vals(&ov), vec![(1, "v1".to_string())]);
        assert!(ov.suppress.unwrap().keys.contains(&key_of(1)));
        // Metadata containing S: nothing to add or suppress.
        assert!(st
            .overlay_with(&ident(), |c| *c == GenCommit::Snapshot(s))
            .unwrap()
            .is_none());
        // A newer pending upsert of the same key hides the gen's row.
        write_upsert(&st, 1, "v2", None);
        let ov = st.overlay_with(&ident(), |_| false).unwrap().unwrap();
        assert_eq!(vals(&ov), vec![(1, "v2".to_string())]);
    }

    // Stamp-exact drain: a key overwritten AFTER the flush snapshot keeps
    // its newer entry through the drain, and a conflict merge-back never
    // clobbers it; an un-overwritten key drains and restores cleanly.
    #[test]
    fn keyed_snapshot_drain_respects_later_writes() {
        let st = BufferState::default();
        write_upsert(&st, 1, "v1", None);
        let snap = st.snapshot_pending(&ident());
        // The key is overwritten while the commit is in flight.
        write_upsert(&st, 1, "v2", None);
        st.move_pending_to_flushed(&ident(), 0, &snap.keyed, 5);
        // The NEWER entry survived the drain.
        assert_eq!(upserted_val(&st, 1), "v2");
        // Conflict merge-back must NOT resurrect v1 over v2.
        st.move_flushed_back_to_pending(&ident(), 5);
        assert_eq!(upserted_val(&st, 1), "v2");
        // Contrast: an un-overwritten key drains away and restores on
        // conflict.
        let st2 = BufferState::default();
        write_delete(&st2, 4, None);
        let snap = st2.snapshot_pending(&ident());
        st2.move_pending_to_flushed(&ident(), 0, &snap.keyed, 6);
        assert!(st2.keyed_lookup(&ident(), &key_of(4)).is_none());
        st2.move_flushed_back_to_pending(&ident(), 6);
        assert!(st2
            .keyed_lookup(&ident(), &key_of(4))
            .expect("restored")
            .is_none());
    }

    // Keyed ops are durably appended to the tail (correct op kinds) BEFORE
    // entering the map, and share the table's tail mark with inserts.
    #[test]
    fn keyed_tail_frames_precede_map() {
        let st = BufferState::default();
        let tail = MockTail::default();
        st.append(
            &ident(),
            Some(kschema()),
            &[kbatch(&[(1, "x")])],
            Some(&tail),
        )
        .unwrap();
        write_upsert(&st, 2, "u", Some(&tail));
        write_delete(&st, 3, Some(&tail));
        assert_eq!(
            *tail.kinds.lock().unwrap(),
            vec![TailOpKind::Append, TailOpKind::Upsert, TailOpKind::Delete]
        );
        // One shared per-table mark covering all three statements.
        assert_eq!(st.snapshot_pending(&ident()).tail_mark, Some(3));
        // A failing tail keeps the op OUT of the map (nothing acked from
        // memory alone).
        tail.fail_appends
            .store(true, std::sync::atomic::Ordering::SeqCst);
        let err = st
            .keyed_write(
                &ident(),
                Some(kschema()),
                &pk(),
                key_of(9),
                KeyedOp {
                    key_row: key_row(9),
                    kind: KeyedKind::Delete,
                },
                Some(&tail),
            )
            .unwrap_err();
        assert!(err.to_string().contains("disk on fire"));
        assert!(st.keyed_lookup(&ident(), &key_of(9)).is_none());
    }

    // Composite keys flow through the same map/overlay machinery.
    #[test]
    fn composite_keys_roundtrip_through_state() {
        let comp_schema: ArrowSchemaRef = Arc::new(Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Utf8, false),
            Field::new("val", DataType::Utf8, true),
        ]));
        let comp_pk = vec!["a".to_string(), "b".to_string()];
        let row = RecordBatch::try_new(
            comp_schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["eu"])),
                Arc::new(StringArray::from(vec!["v"])),
            ],
        )
        .unwrap();
        let key_row = row.project(&[0, 1]).unwrap();
        let key = keyed::encode_batch_keys(&key_row, &comp_pk)
            .unwrap()
            .remove(0);
        let st = BufferState::default();
        st.keyed_write(
            &ident(),
            Some(comp_schema),
            &comp_pk,
            key.clone(),
            KeyedOp {
                key_row,
                kind: KeyedKind::Upsert(row),
            },
            None,
        )
        .unwrap();
        assert!(st
            .keyed_lookup(&ident(), &key)
            .expect("entry present")
            .is_some());
        let ov = st.overlay_with(&ident(), |_| false).unwrap().unwrap();
        assert_eq!(ov.batches.len(), 1);
        let sup = ov.suppress.unwrap();
        assert_eq!(sup.pk_cols, comp_pk);
        assert!(sup.keys.contains(&key));
        // A different composite value does not collide.
        let other = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("a", DataType::Int64, false),
                Field::new("b", DataType::Utf8, false),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(StringArray::from(vec!["us"])),
            ],
        )
        .unwrap();
        let other_key = keyed::encode_batch_keys(&other, &comp_pk)
            .unwrap()
            .remove(0);
        assert!(!sup.keys.contains(&other_key));
    }

    // -----------------------------------------------------------------------
    // FIX (L2): seq order is the total order for keyed keys — a plain
    // INSERT of a key with a live keyed-map entry routes into a NEWER
    // upsert entry instead of a pending append the older keyed op would
    // fold away.
    // -----------------------------------------------------------------------

    // delete-then-insert of one key in one window: the row is PRESENT with
    // the inserted values; a multi-row insert splits (map-hit rows become
    // upserts, the rest stay plain appends); the tail frame stays a plain
    // Append (replay re-routes through the same code).
    #[test]
    fn insert_after_keyed_delete_resurrects_key() {
        let st = BufferState::default();
        let tail = MockTail::default();
        write_delete(&st, 1, Some(&tail));
        // Mid-window the key is deleted.
        assert!(st
            .keyed_lookup(&ident(), &key_of(1))
            .expect("entry present")
            .is_none());
        // Plain INSERT of keys 1 (map hit -> upsert) and 2 (plain append).
        st.append(
            &ident(),
            Some(kschema()),
            &[kbatch(&[(1, "back"), (2, "new")])],
            Some(&tail),
        )
        .unwrap();
        // The statement's tail frame is a plain Append — replay routes it
        // through the same code path.
        assert_eq!(
            *tail.kinds.lock().unwrap(),
            vec![TailOpKind::Delete, TailOpKind::Append]
        );
        // The key's entry is now the newer upsert (LWW by arrival order).
        assert_eq!(upserted_val(&st, 1), "back");
        // Overlay: both rows visible, key 1 via the upsert side.
        let ov = st.overlay_with(&ident(), |_| false).unwrap().unwrap();
        assert_eq!(
            vals(&ov),
            vec![(1, "back".to_string()), (2, "new".to_string())]
        );
        // Row 2 went to pending (exactly one plain pending row).
        let snap = st.snapshot_pending(&ident());
        let pending_rows: usize = snap.batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(pending_rows, 1);
        assert_eq!(snap.keyed.len(), 1);
    }

    // insert-then-delete of one key in one window: the row is ABSENT (the
    // append is OLDER than the keyed op, so folding it away is correct).
    #[test]
    fn insert_then_keyed_delete_removes_row() {
        let st = BufferState::default();
        st.append(&ident(), Some(kschema()), &[kbatch(&[(1, "x")])], None)
            .unwrap();
        write_delete(&st, 1, None);
        let ov = st
            .overlay_with(&ident(), |_| false)
            .unwrap()
            .expect("suppression still present");
        assert!(vals(&ov).is_empty(), "the inserted row must be hidden");
        assert!(ov.suppress.expect("suppression").keys.contains(&key_of(1)));
    }

    // Replay rebuilds the identical state: pushing the same frames in
    // sequence order through the SAME entry points (keyed_write for keyed
    // frames, append for Append frames — exactly what replay_tail does)
    // yields the same overlay and the same keyed map as the live path.
    #[test]
    fn replay_routing_rebuilds_identical_state() {
        let build = |st: &BufferState| {
            write_upsert(st, 3, "u3", None);
            write_delete(st, 1, None);
            st.append(
                &ident(),
                Some(kschema()),
                &[kbatch(&[(1, "back"), (2, "plain")])],
                None,
            )
            .unwrap();
        };
        let live = BufferState::default();
        build(&live);
        let replayed = BufferState::default();
        build(&replayed);
        let ov_live = live.overlay_with(&ident(), |_| false).unwrap().unwrap();
        let ov_replay = replayed.overlay_with(&ident(), |_| false).unwrap().unwrap();
        assert_eq!(vals(&ov_live), vals(&ov_replay));
        assert_eq!(
            vals(&ov_live),
            vec![
                (1, "back".to_string()),
                (2, "plain".to_string()),
                (3, "u3".to_string())
            ]
        );
        let sup_live = ov_live.suppress.expect("suppression");
        let sup_replay = ov_replay.suppress.expect("suppression");
        assert_eq!(*sup_live.keys, *sup_replay.keys);
        assert_eq!(upserted_val(&live, 1), upserted_val(&replayed, 1));
    }

    // -----------------------------------------------------------------------
    // FIX (S3): a pk_cols mismatch is fatal only while old-key state is
    // buffered; with an empty map (and no keyed flushed generations) the
    // declaration refreshes and keyed ops keep working.
    // -----------------------------------------------------------------------
    #[test]
    fn pk_change_refreshes_when_no_keyed_state_buffered() {
        let st = BufferState::default();
        // With a live entry, a changed PK declaration fails loudly.
        write_upsert(&st, 1, "v1", None);
        let err = st
            .keyed_write(
                &ident(),
                Some(kschema()),
                &["val".to_string()],
                key_of(9),
                KeyedOp {
                    key_row: key_row(9),
                    kind: KeyedKind::Delete,
                },
                None,
            )
            .unwrap_err();
        assert!(err.to_string().contains("primary key"));
        // Drain the window (flush success bookkeeping): entries empty, but
        // the flushed generation still holds old-key ops — still fatal.
        let snap = st.snapshot_pending(&ident());
        st.move_pending_to_flushed(&ident(), 0, &snap.keyed, 5);
        let err = st
            .keyed_write(
                &ident(),
                Some(kschema()),
                &["val".to_string()],
                key_of(9),
                KeyedOp {
                    key_row: key_row(9),
                    kind: KeyedKind::Delete,
                },
                None,
            )
            .unwrap_err();
        assert!(err.to_string().contains("primary key"));
        // GC the generation: now the change is safe — pk_cols refresh and
        // the op lands under the new declaration.
        st.retain_flushed(|_| false);
        let new_pk = vec!["val".to_string()];
        let row = krow(7, "k");
        let key_row_new = row.project(&[1]).unwrap();
        let new_key = keyed::encode_batch_keys(&key_row_new, &new_pk)
            .unwrap()
            .remove(0);
        st.keyed_write(
            &ident(),
            Some(kschema()),
            &new_pk,
            new_key.clone(),
            KeyedOp {
                key_row: key_row_new,
                kind: KeyedKind::Upsert(row),
            },
            None,
        )
        .unwrap();
        assert!(st
            .keyed_lookup(&ident(), &new_key)
            .expect("entry present")
            .is_some());
        // Suppression now encodes under the NEW declaration.
        let ov = st.overlay_with(&ident(), |_| false).unwrap().unwrap();
        assert_eq!(ov.suppress.unwrap().pk_cols, new_pk);
    }

    // -----------------------------------------------------------------------
    // FIX (L3): a generation parked by the prepare-time already-committed
    // guard is tagged with the covering WATERMARK, and the overlay excludes
    // it exactly when the scan metadata's own tail watermark covers the
    // mark — no snapshot-id guess.
    // -----------------------------------------------------------------------
    #[test]
    fn covered_generation_excluded_exactly_by_watermark() {
        let st = BufferState::default();
        let sch = schema();
        st.append(&ident(), Some(sch.clone()), &[batch(&sch, &[1, 2])], None)
            .unwrap();
        let (_b, n, _) = snap3(&st);
        st.move_pending_to_flushed_covered(&ident(), n, &[], 7);
        assert!(!st.has_pending());
        let with_mark = |scan_mark: Option<u64>| {
            st.overlay_with(&ident(), |c| match c {
                GenCommit::Snapshot(_) => false,
                GenCommit::CoveredByWatermark(mark) => scan_mark.is_some_and(|w| w >= *mark),
            })
            .unwrap()
        };
        // A scan whose metadata carries NO watermark (or one below the
        // mark) predates the commit: the overlay must supply the rows.
        assert_eq!(ids(&with_mark(None).expect("rows")), vec![1, 2]);
        assert_eq!(ids(&with_mark(Some(6)).expect("rows")), vec![1, 2]);
        // At or past the mark the committed scan has the rows: excluded.
        assert!(with_mark(Some(7)).is_none());
        assert!(with_mark(Some(9)).is_none());
        // Snapshot-membership never excludes a watermark-tagged generation.
        assert_eq!(
            ids(&st
                .overlay_with(&ident(), |c| matches!(c, GenCommit::Snapshot(_)))
                .unwrap()
                .expect("rows")),
            vec![1, 2]
        );
    }

    // -----------------------------------------------------------------------
    // FIX (L1): the per-table keyed-serial lock makes a keyed
    // read-modify-write and a fenced synchronous write on the same key
    // mutually exclusive — the fenced write's effect always survives (the
    // RMW either sees it or blocks until it is visible). Modeled with the
    // real lock structure and BufferState; the "lake" is a mock cell.
    // -----------------------------------------------------------------------
    #[tokio::test]
    async fn keyed_serial_lock_serializes_rmw_against_fenced_write() {
        use std::sync::atomic::{AtomicBool, Ordering};

        let serial = KeyedSerial::default();
        // Same table -> same lock; different table -> different lock.
        let l1 = serial.lock_for(&ident());
        let l2 = serial.lock_for(&ident());
        assert!(Arc::ptr_eq(&l1, &l2));
        let other = TableIdent::from_strs(["demo", "other"]).unwrap();
        assert!(!Arc::ptr_eq(&l1, &serial.lock_for(&other)));

        // The interleaving the lock forbids: RMW reads the committed value,
        // a fenced write commits, the RMW then frames its (now stale) full
        // row. With both critical sections under the table's lock, they
        // cannot overlap and the fenced increment always survives.
        let st = Arc::new(BufferState::default());
        let committed = Arc::new(StdMutex::new(10i64)); // the "lake" row
        let in_critical = Arc::new(AtomicBool::new(false));
        let overlapped = Arc::new(AtomicBool::new(false));

        // RMW task (try_keyed_dml's shape): lock; read; dawdle; write the
        // full-row image derived from the read into the keyed map.
        let rmw = {
            let (lock, st, committed) = (l1.clone(), st.clone(), committed.clone());
            let (in_critical, overlapped) = (in_critical.clone(), overlapped.clone());
            tokio::spawn(async move {
                let _g = lock.lock().await;
                if in_critical.swap(true, Ordering::SeqCst) {
                    overlapped.store(true, Ordering::SeqCst);
                }
                let seen = *committed.lock().unwrap();
                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                write_upsert(&st, 1, &format!("v{}", seen + 100), None);
                in_critical.store(false, Ordering::SeqCst);
            })
        };
        // Give the RMW time to enter its critical section first.
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        // Fenced-sync task (try_serialized_sync_dml's shape): lock; "fence
        // flush" the keyed map into the lake; apply the sync increment.
        let fenced = {
            let (lock, st, committed) = (l1.clone(), st.clone(), committed.clone());
            let (in_critical, overlapped) = (in_critical.clone(), overlapped.clone());
            tokio::spawn(async move {
                let _g = lock.lock().await;
                if in_critical.swap(true, Ordering::SeqCst) {
                    overlapped.store(true, Ordering::SeqCst);
                }
                // Fence: drain the keyed map into the committed cell (what
                // flush_now does), THEN apply the synchronous write on top.
                let snap = st.snapshot_pending(&ident());
                if let Some((_, _, op)) = snap.keyed.first() {
                    if let KeyedKind::Upsert(row) = &op.kind {
                        let v = row
                            .column(1)
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .unwrap()
                            .value(0)
                            .trim_start_matches('v')
                            .parse::<i64>()
                            .unwrap();
                        *committed.lock().unwrap() = v;
                    }
                    st.drop_pending_prefix(&ident(), 0, &snap.keyed);
                }
                *committed.lock().unwrap() += 1;
                in_critical.store(false, Ordering::SeqCst);
            })
        };
        rmw.await.unwrap();
        fenced.await.unwrap();
        assert!(
            !overlapped.load(Ordering::SeqCst),
            "critical sections must never overlap"
        );
        // RMW entered first: it framed 10+100=110, the fence flushed it,
        // and the sync increment applied ON TOP — the fenced write's effect
        // survives (committed 111, keyed map drained).
        assert_eq!(*committed.lock().unwrap(), 111);
        assert!(st.keyed_lookup(&ident(), &key_of(1)).is_none());
    }

    // -----------------------------------------------------------------------
    // F2 — wait-failure unroute: an errored statement's rows must not
    // silently commit. Staging succeeds, the durability WAIT fails; the
    // statement's routed state must be removed exactly — unless a flush
    // snapshot claimed it first (then the disclosed ambiguity stands).
    // -----------------------------------------------------------------------

    use crate::tail::StagedAppend;

    /// A tail whose staging succeeds but whose durability WAIT fails —
    /// the F2 shape (a dying disk detected at the group fsync).
    #[derive(Default)]
    struct FailWaitTail {
        next_seq: std::sync::atomic::AtomicU64,
    }

    impl TailStore for FailWaitTail {
        fn append(
            &self,
            _table: &TableIdent,
            _kind: TailOpKind,
            _batches: &[RecordBatch],
        ) -> Result<u64> {
            unreachable!("buffered paths stage; they never call append directly")
        }

        fn append_staged(
            &self,
            _table: &TableIdent,
            _kind: TailOpKind,
            _batches: &[RecordBatch],
        ) -> Result<StagedAppend> {
            let seq = self
                .next_seq
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1;
            Ok(StagedAppend::with_waiter(
                seq,
                Box::new(|| Err(anyhow!("mock tail: fsync wait failed"))),
            ))
        }

        fn replay(&self) -> Result<Vec<crate::tail::ReplayedTable>> {
            Ok(Vec::new())
        }

        fn truncate(&self, _table: &TableIdent, _upto_seq: u64) -> Result<()> {
            Ok(())
        }

        fn ensure_seq_floor(&self, _table: &TableIdent, _floor: u64) -> Result<()> {
            Ok(())
        }

        fn watermark_property(&self) -> &str {
            "icegres.tail-seq.mock-tail-id"
        }
    }

    /// A tail whose WAIT stage first lets a flush CLAIM + drain the window
    /// (snapshot + move to flushed, exactly what flush_table does between
    /// prepare and post) and then fails — the narrow window where the
    /// unroute must stand down and keep the disclosed ambiguity.
    struct ClaimingTail {
        st: Arc<BufferState>,
        next_seq: std::sync::atomic::AtomicU64,
    }

    impl TailStore for ClaimingTail {
        fn append(
            &self,
            _table: &TableIdent,
            _kind: TailOpKind,
            _batches: &[RecordBatch],
        ) -> Result<u64> {
            unreachable!("buffered paths stage; they never call append directly")
        }

        fn append_staged(
            &self,
            table: &TableIdent,
            _kind: TailOpKind,
            _batches: &[RecordBatch],
        ) -> Result<StagedAppend> {
            let seq = self
                .next_seq
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst)
                + 1;
            let st = self.st.clone();
            let ident = table.clone();
            Ok(StagedAppend::with_waiter(
                seq,
                Box::new(move || {
                    // The waiter runs AFTER the buffer lock drops — the
                    // exact interleaving where a flush can claim the rows
                    // before the wait resolves.
                    let snap = st.snapshot_pending(&ident);
                    st.move_pending_to_flushed(&ident, snap.n_batches, &snap.keyed, 42);
                    Err(anyhow!("mock tail: died after a flush claimed the rows"))
                }),
            ))
        }

        fn replay(&self) -> Result<Vec<crate::tail::ReplayedTable>> {
            Ok(Vec::new())
        }

        fn truncate(&self, _table: &TableIdent, _upto_seq: u64) -> Result<()> {
            Ok(())
        }

        fn ensure_seq_floor(&self, _table: &TableIdent, _floor: u64) -> Result<()> {
            Ok(())
        }

        fn watermark_property(&self) -> &str {
            "icegres.tail-seq.mock-tail-id"
        }
    }

    // F2: a plain INSERT whose durability wait fails is removed from
    // `pending` exactly — earlier acked rows stay, the failed rows are gone
    // from the overlay, the snapshot, and the row accounting.
    #[test]
    fn wait_failure_unroutes_pending_rows_exactly() {
        let st = BufferState::default();
        let sch = schema();
        // An earlier acked statement (survives).
        st.append(&ident(), Some(sch.clone()), &[batch(&sch, &[1])], None)
            .unwrap();
        let tail = FailWaitTail::default();
        let err = st
            .append(&ident(), None, &[batch(&sch, &[2, 3])], Some(&tail))
            .unwrap_err();
        assert!(err.to_string().contains("fsync wait failed"));
        // Exactly the acked row remains.
        let ov = st
            .overlay_with(&ident(), |_| false)
            .unwrap()
            .expect("acked row still pending");
        assert_eq!(ids(&ov), vec![1]);
        let (_batches, n, _mark) = snap3(&st);
        assert_eq!(n, 1, "the failed statement's batch is gone");
        // pending_rows accounting shrank with it: only rows 1 and 4 count.
        let (_, total) = st
            .append(&ident(), None, &[batch(&sch, &[4])], None)
            .unwrap();
        assert_eq!(total, 2);
    }

    // F2: a keyed op whose durability wait fails is removed from the live
    // map, RESTORING the earlier acked entry it displaced (that entry must
    // still flush); a failed op on a fresh key leaves no entry at all —
    // and keyed_current never serves the failed value.
    #[test]
    fn wait_failure_unroutes_keyed_entry_and_restores_displaced() {
        let st = BufferState::default();
        write_upsert(&st, 1, "v10", None); // acked, must survive
        let tail = FailWaitTail::default();
        let err = st
            .keyed_write(
                &ident(),
                Some(kschema()),
                &pk(),
                key_of(1),
                KeyedOp {
                    key_row: key_row(1),
                    kind: KeyedKind::Upsert(krow(1, "v20")),
                },
                Some(&tail),
            )
            .unwrap_err();
        assert!(err.to_string().contains("fsync wait failed"));
        assert_eq!(
            upserted_val(&st, 1),
            "v10",
            "displaced acked entry restored"
        );
        let cur = st
            .keyed_current(&ident(), &key_of(1), &pk())
            .expect("entry present")
            .expect("upsert");
        assert_eq!(
            cur.column(1)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(0),
            "v10",
            "keyed_current must not serve the failed value"
        );
        // Fresh key: the failed op leaves no trace.
        let err = st
            .keyed_write(
                &ident(),
                None,
                &pk(),
                key_of(2),
                KeyedOp {
                    key_row: key_row(2),
                    kind: KeyedKind::Delete,
                },
                Some(&tail),
            )
            .unwrap_err();
        assert!(err.to_string().contains("fsync wait failed"));
        assert!(st.keyed_lookup(&ident(), &key_of(2)).is_none());
        assert!(st.keyed_current(&ident(), &key_of(2), &pk()).is_none());
    }

    // F2: an INSERT that route_appends split (one row into an upsert entry
    // displacing an acked keyed op, one row into pending) unroutes BOTH
    // halves on wait failure — the acked keyed op is restored, the pending
    // half is gone.
    #[test]
    fn wait_failure_unroutes_routed_insert_upserts() {
        let st = BufferState::default();
        write_upsert(&st, 1, "old", None); // acked, must survive
        let tail = FailWaitTail::default();
        let err = st
            .append(
                &ident(),
                Some(kschema()),
                &[kbatch(&[(1, "new"), (2, "plain")])],
                Some(&tail),
            )
            .unwrap_err();
        assert!(err.to_string().contains("fsync wait failed"));
        assert_eq!(
            upserted_val(&st, 1),
            "old",
            "displaced acked entry restored"
        );
        let ov = st
            .overlay_with(&ident(), |_| false)
            .unwrap()
            .expect("the acked upsert still overlays");
        assert_eq!(vals(&ov), vec![(1, "old".to_string())]);
    }

    // F2 narrow window: when a flush snapshot claimed (and drained) the
    // rows BEFORE the wait failed, the unroute stands down — the rows stay
    // in their flushed generation (they may genuinely commit; removing
    // them would corrupt the flush accounting), union reads still serve
    // them, and the generation excludes itself once metadata contains it.
    #[test]
    fn wait_failure_after_flush_claim_keeps_disclosed_ambiguity() {
        let st = Arc::new(BufferState::default());
        let sch = schema();
        let tail = ClaimingTail {
            st: st.clone(),
            next_seq: Default::default(),
        };
        let err = st
            .append(
                &ident(),
                Some(sch.clone()),
                &[batch(&sch, &[1, 2])],
                Some(&tail),
            )
            .unwrap_err();
        assert!(err.to_string().contains("died after a flush claimed"));
        // Drained into flushed(42), NOT removed.
        let (_batches, n, _mark) = snap3(&st);
        assert_eq!(n, 0, "pending was drained by the claiming flush");
        let ov = st
            .overlay_with(&ident(), |_| false)
            .unwrap()
            .expect("rows still readable through the flushed generation");
        assert_eq!(ids(&ov), vec![1, 2]);
        assert!(
            st.overlay_with(&ident(), |c| *c == GenCommit::Snapshot(42))
                .unwrap()
                .is_none(),
            "once committed metadata contains the snapshot, the rows drop out"
        );
    }

    // F2 narrow window, keyed shape: a keyed entry claimed + drained by a
    // flush before the wait failure stays with its generation (disclosed
    // ambiguity) — never resurrected into the live map, never removed from
    // the generation.
    #[test]
    fn keyed_wait_failure_after_claim_keeps_disclosed_ambiguity() {
        let st = Arc::new(BufferState::default());
        let tail = ClaimingTail {
            st: st.clone(),
            next_seq: Default::default(),
        };
        let err = st
            .keyed_write(
                &ident(),
                Some(kschema()),
                &pk(),
                key_of(1),
                KeyedOp {
                    key_row: key_row(1),
                    kind: KeyedKind::Upsert(krow(1, "v1")),
                },
                Some(&tail),
            )
            .unwrap_err();
        assert!(err.to_string().contains("died after a flush claimed"));
        assert!(
            st.keyed_lookup(&ident(), &key_of(1)).is_none(),
            "the live map entry was drained into the generation"
        );
        let ov = st
            .overlay_with(&ident(), |_| false)
            .unwrap()
            .expect("the generation still overlays the op");
        assert_eq!(vals(&ov), vec![(1, "v1".to_string())]);
    }
}
