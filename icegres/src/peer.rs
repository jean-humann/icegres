//! Peer tail mirrors (`--peer-tail`, roadmap-v2 P1 "fleet overlays"):
//! a READ compute subscribes to a buffering peer's open tail API
//! (tailapi.rs / docs/open-tail-protocol.md) and keeps an in-memory,
//! per-table mirror of the peer's tail window. Scans union the mirror with
//! committed data under the SAME exactly-once rule the local overlay uses:
//!
//! * `w` = the peer's `icegres.tail-seq.<peer-id>` property in the SCAN's
//!   own metadata (absent = -∞) — stamped by the peer's flushes in the same
//!   atomic commit as the rows, so `w >= seq` ⟺ committed data contains
//!   the op.
//! * Include mirrored append/upsert rows iff `seq > w`; suppress committed
//!   rows whose key has a mirrored keyed op with `seq > w`; among mirrored
//!   rows the newest seq per key wins (route-on-append at ingest mirrors
//!   the server's `route_appends` invariant).
//!
//! Honesty/fallback contract (scope §2): mirrors are read-side only and
//! best-effort. On disconnect the mirror is DROPPED — reads fall back to
//! commit-cadence freshness (rows are tail-durable on the peer; nothing is
//! at stake but the freshness bonus) — with ONE warn per outage (the latch
//! resets only after a session that exchanged tail RPCs, never on a bare
//! TCP connect) and the `icegres_peer_tail_age_ms` gauge tracking
//! staleness. The single-buffering-writer-per-table deployment model is
//! unchanged: a table's mirror is OWNED by the first peer to claim it —
//! ingest/drop from any other peer is refused, and a second peer claiming
//! the same table is refused with one WARN (it takes over automatically
//! once the owner's mirror drops).
//!
//! Auth: when the peer's tail API runs with `--auth-file`, set
//! `ICEGRES_PEER_TAIL_USER` / `ICEGRES_PEER_TAIL_PASSWORD` (one identity
//! for all peers, v1) — the subscriber performs the standard Flight
//! basic-auth handshake per connection and attaches the minted bearer
//! token to every tail RPC.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use anyhow::{anyhow, Context as _, Result};
use arrow::array::{ArrayRef as ArrowArrayRef, RecordBatch, StringArray, UInt64Array};
use arrow::datatypes::SchemaRef as ArrowSchemaRef;
use iceberg::spec::TableMetadata;
use iceberg::TableIdent;
use tracing::{info, warn};

use crate::buffer::{Overlay, OverlaySuppress};
use crate::keyed;
use crate::tail::parse_watermark_property;
use crate::tailapi;

/// How long a watermark-covered mirror item is retained before GC — the
/// mirror-side analogue of buffer.rs `FLUSHED_GC`: a reader whose metadata
/// is bounded-stale (freshness mode) may still need rows the peer already
/// committed; the property-watermark rule excludes them per-scan either
/// way, so retention only costs memory, never correctness. This is the
/// FLOOR; [`effective_mirror_gc`] raises it under `--freshness-ms`.
const MIRROR_GC: Duration = Duration::from_secs(30);

/// The watermark-covered retention this process must actually use: a reader
/// running `--freshness-ms S` can scan metadata up to ~S ms stale, and a
/// row absent from that stale committed snapshot AND already GC'd from the
/// mirror would silently vanish from the union — so retention is raised to
/// max([`MIRROR_GC`], 4× the freshness bound), computed once at startup
/// (main.rs WARNs with the value chosen). 4× leaves ample headroom over the
/// worst-case refresh lag (bound + one refresh round trip + timeout).
pub fn effective_mirror_gc(freshness_ms: u64) -> Duration {
    MIRROR_GC.max(Duration::from_millis(freshness_ms.saturating_mul(4)))
}

/// Serving bound on mirror staleness (the per-peer last-event age): past
/// this, scans treat the peer's mirrors as ABSENT (commit-cadence fallback,
/// one WARN per stall) and resume serving when events resume. A healthy
/// subscription delivers at least the 1 Hz liveness heartbeat
/// (tailapi::HEARTBEAT_EVERY), so a silent connection means the peer is
/// hung/partitioned even if TCP still looks alive. Protocol v1's handshake
/// carries no flush-cadence hint, so the bound is a constant: 5× the
/// heartbeat interval (comfortably above the 3× floor, low enough that a
/// hung peer stops being served long before its data could mislead).
const SERVE_AGE_BOUND: Duration = Duration::from_secs(5);

/// HTTP/2 keepalive on subscriber channels: PING every interval, drop the
/// connection when a PING goes unanswered past the timeout — so a hung peer
/// (dead host, partition) surfaces as a stream error and the mirror is
/// dropped instead of being served stale forever. TCP keepalive is layered
/// under it for middleboxes that kill idle flows without RST.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(10);
const KEEPALIVE_TIMEOUT: Duration = Duration::from_secs(5);
const TCP_KEEPALIVE: Duration = Duration::from_secs(30);

/// Reconnect backoff bounds for peer subscriber tasks.
const BACKOFF_MIN: Duration = Duration::from_millis(500);
const BACKOFF_MAX: Duration = Duration::from_secs(10);

/// How often each peer's table list is re-polled (discovery of tables that
/// start buffering after we connected).
const DISCOVERY_EVERY: Duration = Duration::from_secs(2);

/// One mirrored append item: the statement's seq, its rows, and (keyed
/// tables) the rows' encoded PKs, computed once at ingest.
struct MirrorAppend {
    seq: u64,
    batch: RecordBatch,
    keys: Option<Vec<Vec<u8>>>,
}

/// The latest mirrored keyed op for one key.
struct MirrorKeyed {
    seq: u64,
    /// `Some(row)` = upsert (canonical one-row batch); `None` = delete.
    row: Option<RecordBatch>,
}

/// One table's mirror of a peer's tail window.
struct TableMirror {
    /// The peer's full `icegres.tail-seq.<peer-id>` property key — what the
    /// exclusion rule looks up in the SCAN's metadata.
    property_key: String,
    /// The peer's canonical Arrow schema for the table.
    schema: ArrowSchemaRef,
    pk_cols: Vec<String>,
    appends: Vec<MirrorAppend>,
    keyed: HashMap<Vec<u8>, MirrorKeyed>,
    /// Highest watermark the peer reported committed (heartbeats).
    watermark: u64,
    /// `(observed_at, watermark)` history for grace-period GC.
    covered_marks: Vec<(Instant, u64)>,
    /// The peer address serving this mirror (diagnostics + the per-peer age
    /// gate; a second peer claiming the same table warns — one buffering
    /// writer per table).
    peer: String,
    /// Watermark-covered retention ([`PeerMirrors::gc_grace`], threaded in
    /// at install).
    gc_grace: Duration,
    /// Highest watermark OBSERVED in a scan's own committed metadata
    /// (recorded by [`overlay_for`](Self::overlay_for)) — the mirror
    /// analogue of buffer.rs's observed-coverage GC gate (F9): during a
    /// reader-side catalog outage the stale-read default keeps serving a
    /// frozen snapshot, and dropping items its watermark has not caught up
    /// to would make previously-visible rows VANISH from the union.
    observed_watermark: u64,
    /// When a scan last consulted this mirror (F9): with no reader inside
    /// the grace window, nothing can observe coverage — and nothing can
    /// experience a non-monotonic read either, so GC proceeds on the
    /// peer-reported watermark alone.
    last_overlay_at: Option<Instant>,
}

impl TableMirror {
    /// Ingest one wire batch (Snapshot backfill and Subscribe events use
    /// the identical format): rows grouped by contiguous `(seq, op)` runs.
    fn ingest(&mut self, batch: &RecordBatch) -> Result<()> {
        let n = batch.num_rows();
        if n == 0 {
            return Ok(());
        }
        let seq_idx = batch
            .schema()
            .index_of(tailapi::SEQ_COL)
            .map_err(|_| anyhow!("tail wire batch lacks {}", tailapi::SEQ_COL))?;
        let op_idx = batch
            .schema()
            .index_of(tailapi::OP_COL)
            .map_err(|_| anyhow!("tail wire batch lacks {}", tailapi::OP_COL))?;
        let seqs = batch
            .column(seq_idx)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .ok_or_else(|| anyhow!("{} is not UInt64", tailapi::SEQ_COL))?
            .clone();
        let ops = batch
            .column(op_idx)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| anyhow!("{} is not Utf8", tailapi::OP_COL))?
            .clone();
        // Strip the wire columns and re-anchor onto the mirror's canonical
        // schema (drops the wire header metadata, so overlay batches feed
        // cache.rs's MemTable under one schema identity).
        let data_cols: Vec<ArrowArrayRef> = (0..batch.num_columns())
            .filter(|&i| i != seq_idx && i != op_idx)
            .map(|i| batch.column(i).clone())
            .collect();
        let data = RecordBatch::try_new(self.schema.clone(), data_cols)
            .map_err(|e| anyhow!("tail wire batch does not match the header schema: {e}"))?;
        let mut start = 0usize;
        while start < n {
            let mut end = start + 1;
            while end < n
                && seqs.value(end) == seqs.value(start)
                && ops.value(end) == ops.value(start)
            {
                end += 1;
            }
            let run = data.slice(start, end - start);
            self.apply(seqs.value(start), ops.value(start), run)?;
            start = end;
        }
        Ok(())
    }

    /// Apply one `(seq, op)` run.
    fn apply(&mut self, seq: u64, op: &str, run: RecordBatch) -> Result<()> {
        match op {
            "watermark" => {
                if seq > self.watermark {
                    self.watermark = seq;
                    self.covered_marks.push((Instant::now(), seq));
                }
                self.gc();
            }
            "append" => self.apply_append(seq, run)?,
            "upsert" => {
                self.require_pk("upsert")?;
                // Wire data columns are already canonical types, so key
                // encoding compares equal to committed-scan keys.
                let keys = keyed::encode_batch_keys(&run, &self.pk_cols)?;
                for (row, key) in keys.into_iter().enumerate() {
                    self.upsert_key(key, seq, Some(run.slice(row, 1)));
                }
            }
            "delete" => {
                self.require_pk("delete")?;
                // Delete rows carry the key in the PK columns (rest null).
                let keys = keyed::encode_batch_keys(&run, &self.pk_cols)?;
                for key in keys {
                    self.upsert_key(key, seq, None);
                }
            }
            other => warn!(op = other, "unknown tail wire op ignored (newer peer?)"),
        }
        Ok(())
    }

    /// F1 consumer hardening: a keyed event arriving on a mirror whose wire
    /// header declared NO pk-cols is a PROTOCOL ERROR — with an empty key
    /// declaration `encode_batch_keys` yields zero keys and the op would be
    /// SILENTLY discarded (deleted rows served as live, updates lost, on a
    /// stream that stays healthy). Erroring here ends the subscriber task,
    /// which drops the mirror and re-snapshots with a fresh header.
    fn require_pk(&self, op: &str) -> Result<()> {
        anyhow::ensure!(
            !self.pk_cols.is_empty(),
            "protocol error: a keyed {op} event arrived but the tail header declared \
             no pk-cols — dropping the mirror to re-snapshot (the peer must serve the \
             table's declared primary key in icegres.tail.pk-cols)"
        );
        Ok(())
    }

    /// Route-on-append (the mirror-side `route_appends`): rows whose key has
    /// an OLDER, still-LIVE keyed entry supersede it as an upsert at this
    /// seq; the rest stay appends. Ordering-robust: an append older than the
    /// key's entry stays an append and is suppressed at read time by the
    /// newer entry. Watermark-covered entries never route (F10): the source
    /// drained them from its live map at the flush that stamped the
    /// watermark, so ITS `route_appends` appended the row plainly — routing
    /// against the retained-for-grace copy would fabricate an upsert that
    /// suppresses a committed row the source serves.
    fn apply_append(&mut self, seq: u64, run: RecordBatch) -> Result<()> {
        if self.pk_cols.is_empty() || self.keyed.is_empty() {
            let keys = if self.pk_cols.is_empty() {
                None
            } else {
                Some(keyed::encode_batch_keys(&run, &self.pk_cols)?)
            };
            self.appends.push(MirrorAppend {
                seq,
                batch: run,
                keys,
            });
            return Ok(());
        }
        let keys = keyed::encode_batch_keys(&run, &self.pk_cols)?;
        let routed: Vec<bool> = keys
            .iter()
            .map(|k| {
                self.keyed
                    .get(k)
                    .is_some_and(|e| e.seq < seq && e.seq > self.watermark)
            })
            .collect();
        for (row, key) in keys.iter().enumerate().filter(|&(r, _)| routed[r]) {
            self.upsert_key(key.clone(), seq, Some(run.slice(row, 1)));
        }
        if routed.iter().all(|r| *r) {
            return Ok(());
        }
        let mask = arrow::array::BooleanArray::from_iter(routed.iter().map(|r| Some(!r)));
        let rest = arrow::compute::filter_record_batch(&run, &mask)
            .map_err(|e| anyhow!("cannot split a routed mirror append: {e}"))?;
        let rest_keys: Vec<Vec<u8>> = keys
            .into_iter()
            .zip(routed)
            .filter(|(_, r)| !r)
            .map(|(k, _)| k)
            .collect();
        self.appends.push(MirrorAppend {
            seq,
            batch: rest,
            keys: Some(rest_keys),
        });
        Ok(())
    }

    /// Newest-seq-per-key wins in the keyed map.
    fn upsert_key(&mut self, key: Vec<u8>, seq: u64, row: Option<RecordBatch>) {
        match self.keyed.get(&key) {
            Some(existing) if existing.seq >= seq => {}
            _ => {
                self.keyed.insert(key, MirrorKeyed { seq, row });
            }
        }
    }

    /// Whether an OBSERVED peer watermark separates `older` from `newer`
    /// (some mark `w` with `older <= w < newer`): the two seqs then belong
    /// to different flush windows on the source, so its live map could NOT
    /// have routed one against the other. Marks are pruned only together
    /// with the items they cover (see [`gc`](Self::gc)), so a needed mark
    /// is present whenever the question can still arise.
    fn separated_by_watermark(&self, older: u64, newer: u64) -> bool {
        self.covered_marks
            .iter()
            .any(|(_, w)| older <= *w && *w < newer)
    }

    /// Drop items covered by a watermark observed at least `gc_grace` ago
    /// (a bounded-stale reader may still need younger coverage;
    /// [`effective_mirror_gc`] sizes the grace to the freshness bound).
    /// While a reader is actively scanning this mirror, coverage must
    /// additionally have been OBSERVED in the reader's own scan metadata
    /// (F9, [`observed_watermark`](Self::observed_watermark)): the
    /// stale-read-on-catalog-error default can freeze scan metadata for
    /// arbitrarily long, and age-only GC would make rows that were being
    /// served from the mirror vanish mid-outage (non-monotonic reads) —
    /// the exact hazard buffer.rs's FLUSHED_GC observed-rule closes for
    /// the local overlay.
    fn gc(&mut self) {
        let now = Instant::now();
        let aged = self
            .covered_marks
            .iter()
            .filter(|(at, _)| now.duration_since(*at) >= self.gc_grace)
            .map(|(_, w)| *w)
            .max();
        let Some(mut threshold) = aged else { return };
        let reader_active = self
            .last_overlay_at
            .is_some_and(|at| now.duration_since(at) < self.gc_grace);
        if reader_active {
            threshold = threshold.min(self.observed_watermark);
        }
        self.appends.retain(|a| a.seq > threshold);
        self.keyed.retain(|_, e| e.seq > threshold);
        self.covered_marks
            .retain(|(at, w)| *w > threshold || now.duration_since(*at) < self.gc_grace);
    }

    /// The overlay this mirror contributes to a scan whose committed
    /// metadata is `metadata` — the exactly-once rule from the module docs.
    /// Also records the scan observation (F9): the metadata's own watermark
    /// is what unblocks watermark-covered GC while readers are active.
    fn overlay_for(
        &mut self,
        ident: &TableIdent,
        metadata: &TableMetadata,
    ) -> Result<Option<Overlay>> {
        let w = parse_watermark_property(
            ident,
            metadata
                .properties()
                .get(&self.property_key)
                .map(String::as_str),
        );
        self.last_overlay_at = Some(Instant::now());
        if let Some(w) = w {
            self.observed_watermark = self.observed_watermark.max(w);
        }
        self.overlay_at(w)
    }

    /// [`overlay_for`](Self::overlay_for) at an already-resolved scan
    /// watermark `w` (`None` = the scan's metadata carries no watermark for
    /// this peer — nothing committed, include everything).
    fn overlay_at(&self, w: Option<u64>) -> Result<Option<Overlay>> {
        let newer = |seq: u64| w.is_none_or(|w| seq > w);
        // Newest mirrored append seq per key (F7): the read-time half of
        // route-on-append for OUT-OF-ORDER arrival — an upsert/delete that
        // arrives after a same-key append with a higher seq must not serve
        // its (older) row next to the append's.
        let mut append_newest: HashMap<&[u8], u64> = HashMap::new();
        if !self.keyed.is_empty() {
            for append in &self.appends {
                if let Some(keys) = &append.keys {
                    for k in keys {
                        let e = append_newest.entry(k.as_slice()).or_insert(append.seq);
                        *e = (*e).max(append.seq);
                    }
                }
            }
        }
        let mut batches: Vec<RecordBatch> = Vec::new();
        for append in &self.appends {
            if !newer(append.seq) {
                continue;
            }
            // Suppress rows whose key has a NEWER keyed op (per-key max-seq
            // wins). Keys with entries at seq <= append.seq cannot exist
            // for these rows (route-on-append superseded them at ingest).
            let suppress: std::collections::HashSet<Vec<u8>> = match &append.keys {
                None => Default::default(),
                Some(keys) => keys
                    .iter()
                    .filter(|k| self.keyed.get(*k).is_some_and(|e| e.seq > append.seq))
                    .cloned()
                    .collect(),
            };
            if suppress.is_empty() {
                if append.batch.num_rows() > 0 {
                    batches.push(append.batch.clone());
                }
            } else {
                let filtered = keyed::suppress_batch(&append.batch, &self.pk_cols, &suppress)?;
                if filtered.num_rows() > 0 {
                    batches.push(filtered);
                }
            }
        }
        let mut suppress_keys: std::collections::HashSet<Vec<u8>> = Default::default();
        for (key, entry) in &self.keyed {
            if !newer(entry.seq) {
                continue;
            }
            suppress_keys.insert(key.clone());
            if let Some(row) = &entry.row {
                // F7 (newest seq per key wins, arrival-order independent):
                // skip the row when a NEWER same-key append is mirrored and
                // no observed watermark separates the two — the source's
                // route-on-append superseded this op at ingest, so serving
                // both would duplicate the key. A watermark BETWEEN them
                // means separate flush windows (this op committed first;
                // the append is a genuine later row): both serve, matching
                // the source (declaration != enforcement).
                let superseded = append_newest.get(key.as_slice()).is_some_and(|&aseq| {
                    aseq > entry.seq && !self.separated_by_watermark(entry.seq, aseq)
                });
                if !superseded {
                    batches.push(row.clone());
                }
            }
        }
        if batches.is_empty() && suppress_keys.is_empty() {
            return Ok(None);
        }
        Ok(Some(Overlay {
            schema: self.schema.clone(),
            batches,
            suppress: (!suppress_keys.is_empty()).then(|| OverlaySuppress {
                pk_cols: self.pk_cols.clone(),
                keys: Arc::new(suppress_keys),
            }),
        }))
    }
}

/// Per-peer subscription health: drives the serving age gate and the
/// per-peer `icegres_peer_tail_age_ms` gauge.
struct PeerHealth {
    /// Instant of the last successfully applied event/heartbeat from this
    /// peer (initialized at spawn, so a never-connecting peer's age grows
    /// from boot — honest for alerting).
    last_event: Instant,
    /// Whether the age-gate WARN already fired for the current stall
    /// (reset when events resume — one WARN per outage).
    warned_stale: bool,
}

/// The shared mirror registry: one per `icegres serve` process configured
/// with `--peer-tail`, threaded into every `CachingTableProvider` (cache.rs)
/// alongside the local write buffer.
pub struct PeerMirrors {
    tables: StdMutex<HashMap<TableIdent, TableMirror>>,
    /// Per-peer health, keyed by peer address. Lock order where both are
    /// held: `tables` then `peers`, never the reverse.
    peers: StdMutex<HashMap<String, PeerHealth>>,
    /// Watermark-covered mirror retention ([`effective_mirror_gc`]).
    gc_grace: Duration,
    /// `(table, refused peer)` pairs already WARNed about (F3: one WARN per
    /// contested claim, not one per 2 s discovery pass). Entries clear when
    /// the table's mirror drops (the refused peer then takes over) or the
    /// refused peer's claim later succeeds. Leaf lock.
    contested: StdMutex<HashSet<(TableIdent, String)>>,
}

impl Default for PeerMirrors {
    fn default() -> Self {
        Self::new()
    }
}

impl PeerMirrors {
    pub fn new() -> Self {
        Self::with_gc(MIRROR_GC)
    }

    /// Registry with an explicit watermark-covered retention (main.rs passes
    /// [`effective_mirror_gc`] of the configured freshness bound).
    pub fn with_gc(gc_grace: Duration) -> Self {
        Self {
            tables: StdMutex::new(HashMap::new()),
            peers: StdMutex::new(HashMap::new()),
            gc_grace,
            contested: StdMutex::new(HashSet::new()),
        }
    }

    /// Start tracking a configured peer's health from now (spawn time).
    pub fn register_peer(&self, peer: &str) {
        self.peers
            .lock()
            .expect("peer mirror lock poisoned")
            .entry(peer.to_string())
            .or_insert(PeerHealth {
                last_event: Instant::now(),
                warned_stale: false,
            });
    }

    /// `(peer, ms since its last applied event)` for every tracked peer
    /// (the per-peer gauge sampler).
    pub fn peer_ages(&self) -> Vec<(String, u64)> {
        self.peers
            .lock()
            .expect("peer mirror lock poisoned")
            .iter()
            .map(|(peer, h)| (peer.clone(), h.last_event.elapsed().as_millis() as u64))
            .collect()
    }

    fn touch(&self, peer: &str) {
        let mut peers = self.peers.lock().expect("peer mirror lock poisoned");
        let health = peers.entry(peer.to_string()).or_insert(PeerHealth {
            last_event: Instant::now(),
            warned_stale: false,
        });
        health.last_event = Instant::now();
        if health.warned_stale {
            health.warned_stale = false;
            info!(
                peer = %peer,
                "peer tail events resumed — mirrors are being served again"
            );
        }
    }

    /// The PF2 age gate: serve this peer's mirrors only while its last
    /// applied event (a 1 Hz heartbeat at minimum) is younger than
    /// [`SERVE_AGE_BOUND`]; past it the mirror is treated as ABSENT (scans
    /// fall back to commit cadence) with ONE WARN per stall, resuming when
    /// events resume.
    fn peer_serving(&self, peer: &str) -> bool {
        let mut peers = self.peers.lock().expect("peer mirror lock poisoned");
        let Some(health) = peers.get_mut(peer) else {
            return false;
        };
        let age = health.last_event.elapsed();
        if age <= SERVE_AGE_BOUND {
            return true;
        }
        if !health.warned_stale {
            health.warned_stale = true;
            warn!(
                peer = %peer,
                age_ms = age.as_millis() as u64,
                bound_ms = SERVE_AGE_BOUND.as_millis() as u64,
                "peer tail mirror is stale (no events within the serving bound) — \
                 treating it as absent; reads fall back to commit-cadence freshness \
                 until events resume"
            );
        }
        false
    }

    /// Install a table mirror from a fresh TailSnapshot header. F3 (one
    /// buffering writer per table): the FIRST peer to claim a table owns
    /// its mirror; a claim by any OTHER peer is REFUSED (`false`, one WARN
    /// per contested pair) — never replaced, so the owner's live stream
    /// cannot be interleaved with a second seq space or cross-killed. The
    /// refused peer's discovery loop keeps re-trying cheaply and takes
    /// over as soon as the owner's mirror drops. A same-peer re-install
    /// (reconnect re-snapshot) replaces as before.
    fn install(
        &self,
        ident: TableIdent,
        peer: &str,
        property_key: String,
        schema: ArrowSchemaRef,
        pk_cols: Vec<String>,
    ) -> bool {
        let mut tables = self.tables.lock().expect("peer mirror lock poisoned");
        if let Some(existing) = tables.get(&ident) {
            if existing.peer != peer {
                let owner = existing.peer.clone();
                drop(tables);
                self.note_contested(&ident, peer, &owner);
                return false;
            }
        }
        tables.insert(
            ident.clone(),
            TableMirror {
                property_key,
                schema,
                pk_cols,
                appends: Vec::new(),
                keyed: HashMap::new(),
                watermark: 0,
                covered_marks: Vec::new(),
                peer: peer.to_string(),
                gc_grace: self.gc_grace,
                observed_watermark: 0,
                last_overlay_at: None,
            },
        );
        drop(tables);
        // A successful claim clears any earlier refusal latch for this
        // pair (a NEW contest later warns again).
        self.contested
            .lock()
            .expect("peer contested lock poisoned")
            .remove(&(ident, peer.to_string()));
        self.touch(peer);
        true
    }

    /// The owner check + one-shot WARN behind a refused claim (F3). `true`
    /// = the table is currently owned by ANOTHER peer, do not proceed.
    fn claim_refused(&self, ident: &TableIdent, peer: &str) -> bool {
        let owner = {
            let tables = self.tables.lock().expect("peer mirror lock poisoned");
            match tables.get(ident) {
                Some(m) if m.peer != peer => m.peer.clone(),
                _ => return false,
            }
        };
        self.note_contested(ident, peer, &owner);
        true
    }

    /// WARN once per contested `(table, refused peer)` pair (F3).
    fn note_contested(&self, ident: &TableIdent, peer: &str, owner: &str) {
        let mut contested = self.contested.lock().expect("peer contested lock poisoned");
        if contested.insert((ident.clone(), peer.to_string())) {
            warn!(
                table = %ident,
                owner = %owner,
                refused_peer = %peer,
                "two peers serve a tail for the same table — keeping the FIRST claim \
                 and refusing this one (the deployment model is ONE buffering writer \
                 per table); the refused subscriber takes over if the owner's mirror \
                 drops"
            );
        }
    }

    /// Ingest one wire batch into a table's mirror. Scoped to the OWNING
    /// peer (F3): a batch from any other peer's subscriber errors — ending
    /// that stale subscriber task — instead of interleaving a foreign seq
    /// space into the owner's mirror.
    fn ingest(&self, peer: &str, ident: &TableIdent, batch: &RecordBatch) -> Result<()> {
        {
            let mut tables = self.tables.lock().expect("peer mirror lock poisoned");
            let mirror = tables
                .get_mut(ident)
                .ok_or_else(|| anyhow!("no mirror installed for {ident}"))?;
            anyhow::ensure!(
                mirror.peer == peer,
                "mirror for {ident} is owned by peer {} (this subscriber's peer {peer} \
                 was refused; one buffering writer per table)",
                mirror.peer
            );
            mirror.ingest(batch)?;
        }
        self.touch(peer);
        Ok(())
    }

    /// Drop a table's mirror (disconnect → fall back to commit cadence) —
    /// only when `peer` OWNS it (F3): a refused/stale subscriber's teardown
    /// must not kill the owner's healthy mirror.
    fn drop_table(&self, ident: &TableIdent, peer: &str) {
        let mut tables = self.tables.lock().expect("peer mirror lock poisoned");
        if tables.get(ident).is_some_and(|m| m.peer == peer) {
            tables.remove(ident);
            drop(tables);
            // The table is unowned: clear its refusal latches so the next
            // contest (if any) warns afresh.
            self.contested
                .lock()
                .expect("peer contested lock poisoned")
                .retain(|(t, _)| t != ident);
        }
    }

    /// The peer overlay for one scan (cache.rs): `None` when nothing is
    /// mirrored for the table, the mirror's peer is past the serving age
    /// bound (PF2 — a hung peer must not serve unboundedly stale rows), or
    /// everything is covered by the scan's own watermark property.
    pub fn overlay(&self, ident: &TableIdent, metadata: &TableMetadata) -> Result<Option<Overlay>> {
        self.overlay_with(ident, |mirror| mirror.overlay_for(ident, metadata))
    }

    /// [`overlay`](Self::overlay)'s gate + dispatch, with the per-mirror
    /// build injectable so the age gate is unit-testable. `&mut` mirror:
    /// [`TableMirror::overlay_for`] records the scan observation (F9).
    fn overlay_with(
        &self,
        ident: &TableIdent,
        build: impl FnOnce(&mut TableMirror) -> Result<Option<Overlay>>,
    ) -> Result<Option<Overlay>> {
        let mut tables = self.tables.lock().expect("peer mirror lock poisoned");
        let Some(mirror) = tables.get_mut(ident) else {
            return Ok(None);
        };
        let peer = mirror.peer.clone();
        if !self.peer_serving(&peer) {
            return Ok(None);
        }
        build(mirror)
    }
}

// ---------------------------------------------------------------------------
// Subscriber tasks (tonic Flight client against the peer's tail API)
// ---------------------------------------------------------------------------

use arrow_flight::decode::{DecodedPayload, FlightDataDecoder};
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{HandshakeRequest, Ticket};
use futures::StreamExt;
use tonic::metadata::{Ascii, MetadataValue};
use tonic::transport::Channel;

/// Spawn the peer subscriber machinery: one task per peer address, each
/// discovering the peer's buffered tables and running a per-table
/// snapshot→subscribe loop with reconnect/backoff; plus one sampler task
/// feeding the per-peer `icegres_peer_tail_age_ms{peer=…}` gauges (and the
/// `icegres_peer_tail_age_max_ms` worst-case series).
pub fn spawn_peer_tails(peers: Vec<String>, mirrors: Arc<PeerMirrors>) {
    for peer in peers {
        mirrors.register_peer(&peer);
        let mirrors = mirrors.clone();
        tokio::spawn(async move {
            peer_loop(peer, mirrors).await;
        });
    }
    let mirrors_for_gauge = mirrors;
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        loop {
            tick.tick().await;
            crate::metrics::metrics().set_peer_tail_ages(mirrors_for_gauge.peer_ages());
        }
    });
}

/// Reconnect/WARN policy for one peer subscriber (F8): the warn latch and
/// backoff reset only after a session that actually EXCHANGED tail RPCs (a
/// successful handshake + first `Tables` response), never on a bare TCP/h2
/// connect. A persistently failing post-connect peer (auth misconfig,
/// `--peer-tail` aimed at a non-buffering Flight port, authz denial)
/// therefore backs off to [`BACKOFF_MAX`] with ONE warn per outage instead
/// of a ~500 ms WARN + reconnect storm against a production endpoint.
struct ReconnectPolicy {
    backoff: Duration,
    warned: bool,
}

impl ReconnectPolicy {
    fn new() -> Self {
        Self {
            backoff: BACKOFF_MIN,
            warned: false,
        }
    }

    /// Register one failed session. `established` = the session got at
    /// least one successful tail RPC response before failing (a NEW
    /// outage: latch and backoff reset). Returns `(warn_now, sleep_for)`.
    fn on_failure(&mut self, established: bool) -> (bool, Duration) {
        if established {
            self.warned = false;
            self.backoff = BACKOFF_MIN;
        }
        let warn_now = !self.warned;
        self.warned = true;
        let sleep_for = self.backoff;
        self.backoff = (self.backoff * 2).min(BACKOFF_MAX);
        (warn_now, sleep_for)
    }
}

/// Connect-discover-subscribe loop for one peer, forever, with backoff.
/// WARNs once per outage (see [`ReconnectPolicy`]), then logs at debug.
async fn peer_loop(peer: String, mirrors: Arc<PeerMirrors>) {
    let mut policy = ReconnectPolicy::new();
    loop {
        let mut established = false;
        let err = run_peer(&peer, &mirrors, &mut established)
            .await
            .expect_err("run_peer only returns on error");
        let (warn_now, sleep_for) = policy.on_failure(established);
        if warn_now {
            warn!(
                peer = %peer,
                "peer tail unavailable — reads fall back to commit-cadence \
                 freshness until it returns (rows are tail-durable on the \
                 peer; only the freshness bonus is lost): {err:#}"
            );
        } else {
            tracing::debug!(peer = %peer, "peer tail still unavailable: {err:#}");
        }
        tokio::time::sleep(sleep_for).await;
    }
}

/// One session's connection state: the channel plus the bearer token from
/// the Flight basic-auth handshake (F2; `None` = no credentials configured
/// — an open tail API). Cloned into every per-table subscriber task.
#[derive(Clone)]
struct TailSession {
    channel: Channel,
    bearer: Option<MetadataValue<Ascii>>,
}

impl TailSession {
    /// A DoGet request for `ticket`, carrying the bearer token when the
    /// session is authenticated.
    fn do_get_request(&self, ticket: Ticket) -> tonic::Request<Ticket> {
        let mut request = tonic::Request::new(ticket);
        if let Some(bearer) = &self.bearer {
            request
                .metadata_mut()
                .insert("authorization", bearer.clone());
        }
        request
    }
}

/// Subscriber credentials for authed peer tail APIs (F2):
/// `ICEGRES_PEER_TAIL_USER` / `ICEGRES_PEER_TAIL_PASSWORD` — ONE identity
/// for every configured peer (v1; documented in the README flag table and
/// docs/open-tail-protocol.md). `None` = connect without credentials.
fn peer_tail_credentials() -> Option<(String, String)> {
    let user = std::env::var("ICEGRES_PEER_TAIL_USER").ok()?;
    if user.is_empty() {
        return None;
    }
    let password = std::env::var("ICEGRES_PEER_TAIL_PASSWORD").unwrap_or_default();
    Some((user, password))
}

/// Perform the standard Flight basic-auth handshake (the same flow the
/// server documents for every consumer: `authorization: Basic
/// base64(user:password)` → per-boot `Bearer` token) and return the
/// `authorization` header value every subsequent tail RPC must carry.
async fn tail_handshake(
    channel: &Channel,
    user: &str,
    password: &str,
) -> Result<MetadataValue<Ascii>> {
    use base64::Engine as _;
    let mut client = tail_client(channel);
    let mut request = tonic::Request::new(futures::stream::iter([HandshakeRequest::default()]));
    let basic = base64::engine::general_purpose::STANDARD.encode(format!("{user}:{password}"));
    request.metadata_mut().insert(
        "authorization",
        format!("Basic {basic}")
            .parse()
            .map_err(|e| anyhow!("peer tail credentials are not header-safe: {e}"))?,
    );
    let response = client
        .handshake(request)
        .await
        .map_err(|e| anyhow!("peer tail handshake failed: {e}"))?;
    // The server answers with `authorization: Bearer <token>` response
    // metadata (and the raw token in the payload); reuse the header as-is.
    if let Some(bearer) = response.metadata().get("authorization") {
        return Ok(bearer.clone());
    }
    let mut stream = response.into_inner();
    let payload = stream
        .message()
        .await
        .map_err(|e| anyhow!("peer tail handshake stream failed: {e}"))?
        .ok_or_else(|| anyhow!("peer tail handshake returned no token"))?;
    let token = String::from_utf8(payload.payload.to_vec())
        .map_err(|_| anyhow!("peer tail handshake token is not UTF-8"))?;
    format!("Bearer {token}")
        .parse()
        .map_err(|e| anyhow!("peer tail handshake token is not header-safe: {e}"))
}

/// One connected session against a peer: handshake (when credentials are
/// configured), then poll its table list and keep one subscriber task per
/// table alive. Returns Err on any connection-level failure (the caller
/// backs off and reconnects); sets `established` only once a tail RPC got
/// a successful response — never on the bare transport connect (F8).
async fn run_peer(peer: &str, mirrors: &Arc<PeerMirrors>, established: &mut bool) -> Result<()> {
    let endpoint = format!("http://{peer}");
    // Keepalives (PF2): an infinite Subscribe stream on a hung/partitioned
    // peer would otherwise sit in recv() forever — HTTP/2 PINGs (also while
    // idle, which a quiet tail stream is) turn the hang into a stream error
    // so the mirror is dropped instead of served stale.
    let channel = Channel::from_shared(endpoint.clone())
        .map_err(|e| anyhow!("bad peer address {peer:?}: {e}"))?
        .connect_timeout(Duration::from_secs(5))
        .http2_keep_alive_interval(KEEPALIVE_INTERVAL)
        .keep_alive_timeout(KEEPALIVE_TIMEOUT)
        .keep_alive_while_idle(true)
        .tcp_keepalive(Some(TCP_KEEPALIVE))
        .connect()
        .await
        .with_context(|| format!("cannot connect to peer tail {peer}"))?;
    // F2: authed tail APIs need the Flight basic-auth handshake before any
    // tail RPC. Credentials come from the environment (one fleet identity).
    let bearer = match peer_tail_credentials() {
        Some((user, password)) => Some(
            tail_handshake(&channel, &user, &password)
                .await
                .with_context(|| {
                    format!(
                        "peer tail {peer} rejected the ICEGRES_PEER_TAIL_USER/\
                         ICEGRES_PEER_TAIL_PASSWORD handshake"
                    )
                })?,
        ),
        None => None,
    };
    let session = TailSession {
        channel: channel.clone(),
        bearer,
    };
    // Per-table WARN latches (F8's lesser storm): a persistently failing
    // table mirror (protocol-version mismatch, refused claim turned error)
    // is respawned every DISCOVERY_EVERY — warn on the FIRST failure, then
    // debug until a successful install resets the latch.
    let mut table_warned: HashMap<TableIdent, Arc<std::sync::atomic::AtomicBool>> = HashMap::new();
    let mut subscribers: HashMap<TableIdent, tokio::task::JoinHandle<()>> = HashMap::new();
    let mut announced = false;
    let result: Result<()> = async {
        loop {
            let tables = list_tables(&session).await?;
            // First successful tail RPC response: the session is real (F8 —
            // this, not the transport connect, resets the outage latch).
            *established = true;
            if !announced {
                announced = true;
                info!(peer = %peer, "connected to peer tail API");
            }
            for ident in tables {
                let stale = subscribers
                    .get(&ident)
                    .is_none_or(|handle| handle.is_finished());
                if stale {
                    let session = session.clone();
                    let mirrors = mirrors.clone();
                    let peer = peer.to_string();
                    let table = ident.clone();
                    let warned = table_warned
                        .entry(ident.clone())
                        .or_insert_with(|| Arc::new(std::sync::atomic::AtomicBool::new(false)))
                        .clone();
                    subscribers.insert(
                        ident,
                        tokio::spawn(async move {
                            match mirror_table(&session, &peer, &table, &mirrors, &warned).await {
                                // A refused claim (another peer owns the
                                // table) returns Ok: nothing to warn about,
                                // the next discovery pass re-checks.
                                Ok(()) => {}
                                Err(e) => {
                                    mirrors.drop_table(&table, &peer);
                                    if !warned.swap(true, std::sync::atomic::Ordering::Relaxed) {
                                        warn!(
                                            peer = %peer,
                                            table = %table,
                                            "peer tail mirror dropped (fallback to commit \
                                             cadence); will re-establish: {e:#}"
                                        );
                                    } else {
                                        tracing::debug!(
                                            peer = %peer,
                                            table = %table,
                                            "peer tail mirror still failing: {e:#}"
                                        );
                                    }
                                }
                            }
                        }),
                    );
                }
            }
            tokio::time::sleep(DISCOVERY_EVERY).await;
        }
    }
    .await;
    // Connection-level failure: stop every per-table subscriber and drop
    // their mirrors (fallback semantics; scoped — only OUR mirrors).
    for (ident, handle) in subscribers {
        handle.abort();
        mirrors.drop_table(&ident, peer);
    }
    result
}

/// Fetch the peer's buffered-table list.
async fn list_tables(session: &TailSession) -> Result<Vec<TableIdent>> {
    let mut client = tail_client(&session.channel);
    let ticket = Ticket {
        ticket: crate::tailapi::TailTicket::Tables.encode().into(),
    };
    let stream = client
        .do_get(session.do_get_request(ticket))
        .await
        .map_err(|e| anyhow!("peer Tables call failed: {e}"))?
        .into_inner();
    let mut decoder =
        FlightDataDecoder::new(stream.map(|r| r.map_err(|e| FlightError::Tonic(Box::new(e)))));
    let mut idents = Vec::new();
    while let Some(msg) = decoder.next().await {
        let msg = msg.map_err(|e| anyhow!("peer Tables stream failed: {e}"))?;
        if let DecodedPayload::RecordBatch(batch) = msg.payload {
            let ns = batch
                .column_by_name("namespace")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>().cloned())
                .ok_or_else(|| anyhow!("Tables batch lacks a namespace column"))?;
            let name = batch
                .column_by_name("table")
                .and_then(|c| c.as_any().downcast_ref::<StringArray>().cloned())
                .ok_or_else(|| anyhow!("Tables batch lacks a table column"))?;
            for i in 0..batch.num_rows() {
                let mut parts: Vec<&str> = ns.value(i).split('.').collect();
                parts.push(name.value(i));
                idents.push(TableIdent::from_strs(parts)?);
            }
        }
    }
    Ok(idents)
}

fn tail_client(channel: &Channel) -> FlightServiceClient<Channel> {
    FlightServiceClient::new(channel.clone())
        .max_decoding_message_size(64 * 1024 * 1024)
        .max_encoding_message_size(64 * 1024 * 1024)
}

/// Snapshot → install → subscribe loop for one table. Returns Ok when the
/// table is owned by another peer (a refused claim — quiet; the discovery
/// loop re-checks) and Err on any stream failure; the caller drops the
/// mirror (fallback, scoped to this peer) and the discovery loop re-spawns
/// us. `warned` is the per-table failure latch, reset once a mirror
/// installs successfully.
async fn mirror_table(
    session: &TailSession,
    peer: &str,
    table: &TableIdent,
    mirrors: &Arc<PeerMirrors>,
    warned: &std::sync::atomic::AtomicBool,
) -> Result<()> {
    // F3: don't even Snapshot a table another peer owns — the claim would
    // be refused at install anyway; this keeps the contested-retry loop to
    // one cheap in-memory check per discovery pass.
    if mirrors.claim_refused(table, peer) {
        return Ok(());
    }
    let mut client = tail_client(&session.channel);
    // 1. Snapshot.
    let ticket = Ticket {
        ticket: crate::tailapi::TailTicket::Snapshot {
            table: table.clone(),
        }
        .encode()
        .into(),
    };
    let stream = client
        .do_get(session.do_get_request(ticket))
        .await
        .map_err(|e| anyhow!("TailSnapshot({table}) failed: {e}"))?
        .into_inner();
    let mut decoder =
        FlightDataDecoder::new(stream.map(|r| r.map_err(|e| FlightError::Tonic(Box::new(e)))));
    let mut header: Option<(String, ArrowSchemaRef, Vec<String>, u64)> = None;
    let mut backlog: Vec<RecordBatch> = Vec::new();
    while let Some(msg) = decoder.next().await {
        let msg = msg.map_err(|e| anyhow!("TailSnapshot({table}) stream failed: {e}"))?;
        match msg.payload {
            DecodedPayload::Schema(schema) => {
                header = Some(parse_header(table, &schema)?);
            }
            DecodedPayload::RecordBatch(batch) => backlog.push(batch),
            DecodedPayload::None => {}
        }
    }
    let (property_key, schema, pk_cols, high) =
        header.ok_or_else(|| anyhow!("TailSnapshot({table}) carried no schema header"))?;
    if !mirrors.install(table.clone(), peer, property_key, schema.clone(), pk_cols) {
        // Lost the claim race to another peer between the check above and
        // the snapshot: quiet, the discovery loop re-checks.
        return Ok(());
    }
    for batch in &backlog {
        mirrors.ingest(peer, table, batch)?;
    }
    info!(
        peer = %peer,
        table = %table,
        high,
        items = backlog.len(),
        "peer tail mirror installed"
    );
    // A healthy install resets the per-table failure WARN latch.
    warned.store(false, std::sync::atomic::Ordering::Relaxed);
    // 2. Subscribe from the snapshot head; every decoded batch feeds the
    // mirror. The stream is infinite (heartbeats at 1 Hz); returning = error.
    let ticket = Ticket {
        ticket: crate::tailapi::TailTicket::Subscribe {
            table: table.clone(),
            from_seq: high,
        }
        .encode()
        .into(),
    };
    let stream = client
        .do_get(session.do_get_request(ticket))
        .await
        .map_err(|e| anyhow!("TailSubscribe({table}) failed: {e}"))?
        .into_inner();
    let mut decoder =
        FlightDataDecoder::new(stream.map(|r| r.map_err(|e| FlightError::Tonic(Box::new(e)))));
    while let Some(msg) = decoder.next().await {
        let msg = msg.map_err(|e| anyhow!("TailSubscribe({table}) stream failed: {e}"))?;
        if let DecodedPayload::RecordBatch(batch) = msg.payload {
            mirrors.ingest(peer, table, &batch)?;
        }
    }
    Err(anyhow!(
        "TailSubscribe({table}) stream ended (peer shutdown?)"
    ))
}

/// Extract the tail header from the wire schema's metadata and rebuild the
/// canonical schema (wire fields minus the trailing seq/op columns).
fn parse_header(
    table: &TableIdent,
    wire: &ArrowSchemaRef,
) -> Result<(String, ArrowSchemaRef, Vec<String>, u64)> {
    let meta = wire.metadata();
    let version = meta
        .get(tailapi::META_VERSION)
        .ok_or_else(|| anyhow!("tail stream for {table} lacks a version header"))?;
    anyhow::ensure!(
        version == tailapi::TAIL_PROTOCOL_VERSION,
        "tail protocol version mismatch for {table}: peer serves {version}, this build \
         speaks {}",
        tailapi::TAIL_PROTOCOL_VERSION
    );
    let property_key = meta
        .get(tailapi::META_WATERMARK_PROPERTY)
        .ok_or_else(|| anyhow!("tail stream for {table} lacks the watermark-property header"))?
        .clone();
    let high: u64 = meta
        .get(tailapi::META_HIGH)
        .and_then(|h| h.parse().ok())
        .unwrap_or(0);
    let pk_cols: Vec<String> = meta
        .get(tailapi::META_PK_COLS)
        .map(|s| {
            s.split(',')
                .filter(|c| !c.is_empty())
                .map(str::to_string)
                .collect()
        })
        .unwrap_or_default();
    let data_fields: Vec<arrow::datatypes::Field> = wire
        .fields()
        .iter()
        .filter(|f| f.name() != tailapi::SEQ_COL && f.name() != tailapi::OP_COL)
        .map(|f| f.as_ref().clone())
        .collect();
    let schema = Arc::new(arrow::datatypes::Schema::new(data_fields));
    Ok((property_key, schema, pk_cols, high))
}

/// Merge the local write-buffer overlay with the peer-mirror overlay for
/// one scan. Schemas and PK declarations must agree (they describe the same
/// table) — a mismatch fails the scan loudly rather than mis-serving.
pub fn merge_overlays(local: Option<Overlay>, peer: Option<Overlay>) -> Result<Option<Overlay>> {
    match (local, peer) {
        (None, None) => Ok(None),
        (Some(o), None) | (None, Some(o)) => Ok(Some(o)),
        (Some(local), Some(peer)) => {
            anyhow::ensure!(
                local.schema.fields() == peer.schema.fields(),
                "local overlay and peer mirror disagree on the table schema"
            );
            let mut batches = local.batches;
            batches.extend(peer.batches);
            let suppress = match (local.suppress, peer.suppress) {
                (None, None) => None,
                (Some(s), None) | (None, Some(s)) => Some(s),
                (Some(a), Some(b)) => {
                    anyhow::ensure!(
                        a.pk_cols == b.pk_cols,
                        "local overlay and peer mirror disagree on the PK declaration \
                         ({:?} vs {:?})",
                        a.pk_cols,
                        b.pk_cols
                    );
                    let mut keys: std::collections::HashSet<Vec<u8>> =
                        a.keys.iter().cloned().collect();
                    keys.extend(b.keys.iter().cloned());
                    Some(OverlaySuppress {
                        pk_cols: a.pk_cols,
                        keys: Arc::new(keys),
                    })
                }
            };
            Ok(Some(Overlay {
                schema: local.schema,
                batches,
                suppress,
            }))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::buffer::TailEventKind;
    use arrow::array::{Int64Array, StringArray as SA};
    use arrow::datatypes::{DataType, Field, Schema};

    fn ident() -> TableIdent {
        TableIdent::from_strs(["demo", "t"]).unwrap()
    }

    fn canonical() -> ArrowSchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, true),
            Field::new("val", DataType::Utf8, true),
        ]))
    }

    fn row(id: i64, val: &str) -> RecordBatch {
        rows(&[(id, val)])
    }

    fn rows(items: &[(i64, &str)]) -> RecordBatch {
        RecordBatch::try_new(
            canonical(),
            vec![
                Arc::new(Int64Array::from(
                    items.iter().map(|(i, _)| *i).collect::<Vec<_>>(),
                )) as ArrowArrayRef,
                Arc::new(SA::from(items.iter().map(|(_, v)| *v).collect::<Vec<_>>()))
                    as ArrowArrayRef,
            ],
        )
        .unwrap()
    }

    fn mirror(pk: bool) -> TableMirror {
        TableMirror {
            property_key: "icegres.tail-seq.peer".into(),
            schema: canonical(),
            pk_cols: if pk { vec!["id".into()] } else { Vec::new() },
            appends: Vec::new(),
            keyed: HashMap::new(),
            watermark: 0,
            covered_marks: Vec::new(),
            peer: "127.0.0.1:1".into(),
            gc_grace: MIRROR_GC,
            observed_watermark: 0,
            last_overlay_at: None,
        }
    }

    /// Feed a mirror through the REAL wire format (tailapi::wire_batch →
    /// ingest), proving server encode and consumer decode agree.
    fn feed(m: &mut TableMirror, seq: u64, kind: TailEventKind, batch: Option<&RecordBatch>) {
        let property_key = m.property_key.clone();
        let pk_cols = m.pk_cols.clone();
        let wire = tailapi::wire_schema(&canonical(), &ident(), &property_key, 0, &pk_cols);
        let wb = tailapi::wire_batch(&wire, seq, kind, batch).unwrap();
        m.ingest(&wb).unwrap();
    }

    fn overlay_ids(ov: &Overlay) -> Vec<(i64, String)> {
        let mut out = Vec::new();
        for b in &ov.batches {
            let ids = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let vals = b.column(1).as_any().downcast_ref::<SA>().unwrap();
            for i in 0..b.num_rows() {
                out.push((ids.value(i), vals.value(i).to_string()));
            }
        }
        out.sort();
        out
    }

    // The exactly-once rule: rows are included iff seq > the scan's
    // property watermark; absent property includes everything.
    #[test]
    fn exclusion_rule_property_watermark_vs_seq() {
        let mut m = mirror(false);
        feed(&mut m, 3, TailEventKind::Append, Some(&rows(&[(1, "a")])));
        feed(&mut m, 7, TailEventKind::Append, Some(&rows(&[(2, "b")])));
        let all = m.overlay_at(None).unwrap().unwrap();
        assert_eq!(overlay_ids(&all).len(), 2, "no watermark: include all");
        let some = m.overlay_at(Some(3)).unwrap().unwrap();
        assert_eq!(overlay_ids(&some), vec![(2, "b".into())]);
        assert!(
            m.overlay_at(Some(7)).unwrap().is_none(),
            "watermark covers the window: nothing to overlay"
        );
    }

    // Keyed ops with seq > w suppress committed rows and contribute upsert
    // rows; ops at seq <= w contribute nothing (already committed).
    #[test]
    fn keyed_suppression_follows_the_watermark() {
        let mut m = mirror(true);
        feed(&mut m, 5, TailEventKind::Upsert, Some(&row(1, "new")));
        feed(&mut m, 6, TailEventKind::Delete, Some(&row(2, "")));
        let ov = m.overlay_at(Some(4)).unwrap().unwrap();
        assert_eq!(overlay_ids(&ov), vec![(1, "new".into())]);
        let sup = ov.suppress.expect("keys suppress the committed scan");
        assert_eq!(sup.pk_cols, vec!["id".to_string()]);
        assert_eq!(sup.keys.len(), 2);
        // w = 5: only the delete (seq 6) is still un-committed.
        let ov = m.overlay_at(Some(5)).unwrap().unwrap();
        assert!(overlay_ids(&ov).is_empty());
        assert_eq!(ov.suppress.unwrap().keys.len(), 1);
        assert!(m.overlay_at(Some(6)).unwrap().is_none());
    }

    // Route-on-append (the mirror-side route_appends): an append NEWER than
    // a key's entry supersedes it; an OLDER (out-of-order) append stays and
    // is suppressed at read time. Either way each key serves exactly once.
    #[test]
    fn route_on_append_newest_seq_per_key_wins() {
        let mut m = mirror(true);
        feed(&mut m, 5, TailEventKind::Upsert, Some(&row(1, "old")));
        feed(
            &mut m,
            8,
            TailEventKind::Append,
            Some(&rows(&[(1, "reinserted"), (3, "plain")])),
        );
        let ov = m.overlay_at(None).unwrap().unwrap();
        assert_eq!(
            overlay_ids(&ov),
            vec![(1, "reinserted".into()), (3, "plain".into())]
        );
        // Out-of-order arrival: an append OLDER than the key's newest op.
        let mut m = mirror(true);
        feed(&mut m, 9, TailEventKind::Upsert, Some(&row(1, "newest")));
        feed(&mut m, 4, TailEventKind::Append, Some(&row(1, "stale")));
        let ov = m.overlay_at(None).unwrap().unwrap();
        assert_eq!(overlay_ids(&ov), vec![(1, "newest".into())]);
    }

    // F7: the inverted arrival order of the same-window race — the append
    // (an INSERT the source routed as the key's NEWEST op, seq 12) arrives
    // BEFORE the older upsert (seq 9). The mirror must still serve exactly
    // one row per key, with the newest seq winning, and suppress the
    // committed row — never a duplicate of key 1.
    #[test]
    fn out_of_order_upsert_after_newer_append_does_not_duplicate_key() {
        let mut m = mirror(true);
        feed(
            &mut m,
            12,
            TailEventKind::Append,
            Some(&row(1, "insert-new")),
        );
        feed(
            &mut m,
            9,
            TailEventKind::Upsert,
            Some(&row(1, "update-old")),
        );
        let ov = m.overlay_at(None).unwrap().unwrap();
        assert_eq!(
            overlay_ids(&ov),
            vec![(1, "insert-new".into())],
            "newest seq per key wins regardless of arrival order"
        );
        let sup = ov.suppress.expect("the keyed op still suppresses");
        assert!(sup.keys.len() == 1);
        // Same interleaving with a delete: append 12 then delete 9 — the
        // reinserted row survives, the key is suppressed exactly once.
        let mut m = mirror(true);
        feed(
            &mut m,
            12,
            TailEventKind::Append,
            Some(&row(1, "reinserted")),
        );
        feed(&mut m, 9, TailEventKind::Delete, Some(&row(1, "")));
        let ov = m.overlay_at(None).unwrap().unwrap();
        assert_eq!(overlay_ids(&ov), vec![(1, "reinserted".into())]);
    }

    // F10: a keyed entry the source already drained (covered by an observed
    // watermark) must NOT route a later same-key append into an upsert —
    // the source's live map was empty, so IT appended plainly, serving the
    // committed row AND the new append. The peer must agree for the same
    // metadata: fresh scans (w = the covering watermark) see the append
    // without suppressing the committed row.
    #[test]
    fn covered_keyed_entry_does_not_route_or_suppress_a_later_append() {
        let mut m = mirror(true);
        feed(&mut m, 5, TailEventKind::Upsert, Some(&row(1, "a")));
        feed(&mut m, 5, TailEventKind::Watermark, None); // source flushed seq 5
        feed(&mut m, 9, TailEventKind::Append, Some(&row(1, "b")));
        // Fresh metadata (w=5): the committed row holds 'a'; the overlay
        // must add ONLY the plain append 'b' and suppress nothing.
        let ov = m.overlay_at(Some(5)).unwrap().unwrap();
        assert_eq!(overlay_ids(&ov), vec![(1, "b".into())]);
        assert!(
            ov.suppress.is_none(),
            "a watermark-covered keyed entry must not suppress the committed row"
        );
        // Stale metadata (no watermark): the covered upsert still overlays
        // (its commit is invisible there) NEXT TO the append — exactly the
        // source's union view (declaration != enforcement).
        let ov = m.overlay_at(None).unwrap().unwrap();
        assert_eq!(
            overlay_ids(&ov),
            vec![(1, "a".into()), (1, "b".into())],
            "separate flush windows: both rows serve, matching the source"
        );
    }

    // F1 consumer hardening: a keyed event on a mirror whose header carried
    // no pk-cols is a PROTOCOL ERROR (ingest errs -> the subscriber task
    // drops the mirror and re-snapshots) — never a silent no-op that would
    // keep serving deleted rows as live.
    #[test]
    fn keyed_event_with_empty_pk_cols_is_a_protocol_error() {
        let mut m = mirror(false); // header declared no pk-cols
        let wire = tailapi::wire_schema(&canonical(), &ident(), "icegres.tail-seq.peer", 0, &[]);
        for kind in [TailEventKind::Upsert, TailEventKind::Delete] {
            let wb = tailapi::wire_batch(&wire, 7, kind, Some(&row(5, "x"))).unwrap();
            let err = m.ingest(&wb).unwrap_err();
            assert!(
                err.to_string().contains("protocol error"),
                "keyed op on an empty-pk mirror must error loudly, got: {err}"
            );
        }
        // Plain appends still ingest fine on a PK-less mirror.
        let wb = tailapi::wire_batch(&wire, 8, TailEventKind::Append, Some(&row(5, "x"))).unwrap();
        m.ingest(&wb).unwrap();
        assert_eq!(m.appends.len(), 1);
    }

    // F3: mirror ownership is scoped to the claiming peer — the FIRST claim
    // wins, a second peer's install is refused, its ingest/drop are inert
    // against the owner's mirror, and the refused peer takes over once the
    // owner's mirror drops.
    #[test]
    fn mirror_ops_are_scoped_to_the_owning_peer() {
        let mirrors = PeerMirrors::new();
        let (peer_a, peer_b) = ("127.0.0.1:1", "127.0.0.1:2");
        assert!(mirrors.install(
            ident(),
            peer_a,
            "icegres.tail-seq.a".into(),
            canonical(),
            Vec::new(),
        ));
        let wire_a = tailapi::wire_schema(&canonical(), &ident(), "icegres.tail-seq.a", 0, &[]);
        let wb = |seq| {
            tailapi::wire_batch(&wire_a, seq, TailEventKind::Append, Some(&row(1, "a"))).unwrap()
        };
        mirrors.ingest(peer_a, &ident(), &wb(3)).unwrap();
        // B claims the same table: refused, the owner's mirror survives.
        assert!(
            !mirrors.install(
                ident(),
                peer_b,
                "icegres.tail-seq.b".into(),
                canonical(),
                Vec::new(),
            ),
            "the FIRST claim is kept; a second peer's claim is refused"
        );
        assert!(mirrors.claim_refused(&ident(), peer_b));
        assert!(!mirrors.claim_refused(&ident(), peer_a));
        // B's ingest errors (its subscriber ends) instead of interleaving
        // its seq space into A's mirror.
        assert!(mirrors.ingest(peer_b, &ident(), &wb(9)).is_err());
        // B's teardown must not kill A's healthy mirror ...
        mirrors.drop_table(&ident(), peer_b);
        assert!(
            mirrors
                .overlay_with(&ident(), |t| t.overlay_at(None))
                .unwrap()
                .is_some(),
            "the owner's mirror survives a refused peer's drop"
        );
        // ... while A's own drop removes it, and B can then take over.
        mirrors.drop_table(&ident(), peer_a);
        assert!(mirrors
            .overlay_with(&ident(), |t| t.overlay_at(None))
            .unwrap()
            .is_none());
        assert!(mirrors.install(
            ident(),
            peer_b,
            "icegres.tail-seq.b".into(),
            canonical(),
            Vec::new(),
        ));
    }

    // F8: the reconnect policy warns once per outage and escalates backoff;
    // only a session that actually exchanged tail RPCs (established) resets
    // the latch and backoff — a bare TCP connect does not.
    #[test]
    fn reconnect_policy_latches_until_a_session_is_established() {
        let mut p = ReconnectPolicy::new();
        // Persistent post-connect failure (never established): one warn,
        // then silence, with backoff escalating to the max.
        let (warn0, sleep0) = p.on_failure(false);
        assert!(warn0, "first failure of an outage warns");
        assert_eq!(sleep0, BACKOFF_MIN);
        let mut last_sleep = sleep0;
        for _ in 0..8 {
            let (warn, sleep) = p.on_failure(false);
            assert!(!warn, "the same outage never re-warns");
            assert!(sleep >= last_sleep, "backoff never shrinks mid-outage");
            last_sleep = sleep;
        }
        assert_eq!(last_sleep, BACKOFF_MAX, "backoff reaches the cap");
        // An established session failing later = a NEW outage: warn again,
        // backoff restarts from the minimum.
        let (warn, sleep) = p.on_failure(true);
        assert!(warn, "a new outage after a live session warns");
        assert_eq!(sleep, BACKOFF_MIN);
        // And an unestablished retry right after stays latched again.
        let (warn, _) = p.on_failure(false);
        assert!(!warn);
    }

    // Watermark GC honors the grace period: freshly covered items stay (a
    // bounded-stale reader may still need them); items covered longer than
    // MIRROR_GC ago are dropped.
    #[test]
    fn watermark_gc_waits_out_the_grace_period() {
        let mut m = mirror(false);
        feed(&mut m, 3, TailEventKind::Append, Some(&row(1, "a")));
        feed(&mut m, 4, TailEventKind::Watermark, None);
        assert_eq!(m.appends.len(), 1, "fresh coverage: grace period holds");
        // Age the coverage past the grace period and re-run GC.
        let old = Instant::now() - (MIRROR_GC + Duration::from_secs(1));
        m.covered_marks = vec![(old, 4)];
        m.gc();
        assert!(m.appends.is_empty(), "aged coverage: GC drops the items");
    }

    // F9: while a reader is actively scanning the mirror, watermark-covered
    // GC additionally requires the coverage to have been OBSERVED in the
    // reader's own scan metadata — a reader frozen on stale metadata (the
    // stale-read-on-catalog-error default during an outage) must not have
    // rows it is still being served vanish out from under it. With no
    // active reader inside the grace window, the peer watermark alone
    // suffices (nothing can observe, and nothing can read non-monotonically
    // either).
    #[test]
    fn gc_defers_to_reader_observed_watermark_while_scans_are_active() {
        let aged = Instant::now() - (MIRROR_GC + Duration::from_secs(1));
        // Active reader whose scan metadata never advanced past w=0:
        // coverage aged out, but GC must HOLD the items.
        let mut m = mirror(false);
        feed(&mut m, 3, TailEventKind::Append, Some(&row(1, "a")));
        m.covered_marks = vec![(aged, 4)];
        m.watermark = 4;
        m.last_overlay_at = Some(Instant::now());
        m.observed_watermark = 0;
        m.gc();
        assert_eq!(
            m.appends.len(),
            1,
            "aged peer coverage alone must not GC while an active reader's \
             metadata has not observed it"
        );
        // The reader's scans observe covering metadata (w=4): GC unblocks.
        m.covered_marks = vec![(aged, 4)];
        m.observed_watermark = 4;
        m.gc();
        assert!(m.appends.is_empty(), "observed coverage: GC proceeds");
        // No reader within the grace window: peer coverage alone GCs.
        let mut m = mirror(false);
        feed(&mut m, 3, TailEventKind::Append, Some(&row(1, "a")));
        m.covered_marks = vec![(aged, 4)];
        m.watermark = 4;
        m.last_overlay_at = None;
        m.gc();
        assert!(m.appends.is_empty(), "no active reader: GC proceeds");
    }

    // Heartbeats never regress the watermark.
    #[test]
    fn watermark_is_monotonic() {
        let mut m = mirror(false);
        feed(&mut m, 9, TailEventKind::Watermark, None);
        feed(&mut m, 4, TailEventKind::Watermark, None);
        assert_eq!(m.watermark, 9);
    }

    // The PF2 serving bound: a mirror whose peer delivered nothing (not
    // even the 1 Hz heartbeat) for longer than SERVE_AGE_BOUND is treated
    // as ABSENT (commit-cadence fallback, one WARN); serving resumes as
    // soon as events resume.
    #[test]
    fn age_gate_excludes_stale_mirrors_and_resumes() {
        let mirrors = PeerMirrors::new();
        let peer = "127.0.0.1:1";
        assert!(mirrors.install(
            ident(),
            peer,
            "icegres.tail-seq.peer".into(),
            canonical(),
            Vec::new(),
        ));
        let wire = tailapi::wire_schema(&canonical(), &ident(), "icegres.tail-seq.peer", 0, &[]);
        let wb = tailapi::wire_batch(&wire, 3, TailEventKind::Append, Some(&row(1, "a"))).unwrap();
        mirrors.ingest(peer, &ident(), &wb).unwrap();
        let overlay = |m: &PeerMirrors| m.overlay_with(&ident(), |t| t.overlay_at(None)).unwrap();
        assert!(overlay(&mirrors).is_some(), "fresh events: mirror serves");
        // The peer goes silent past the bound: mirror treated as absent.
        let stale = Instant::now() - (SERVE_AGE_BOUND + Duration::from_millis(1));
        mirrors
            .peers
            .lock()
            .unwrap()
            .get_mut(peer)
            .unwrap()
            .last_event = stale;
        assert!(overlay(&mirrors).is_none(), "stale peer: treated as absent");
        assert!(
            mirrors.peers.lock().unwrap()[peer].warned_stale,
            "the stall WARNs once"
        );
        // A second read stays gated without re-warning (flag already set).
        assert!(overlay(&mirrors).is_none());
        // Events resume (any applied batch/heartbeat): serving resumes and
        // the warn latch resets for the next outage.
        let wb = tailapi::wire_batch(&wire, 4, TailEventKind::Append, Some(&row(2, "b"))).unwrap();
        mirrors.ingest(peer, &ident(), &wb).unwrap();
        assert!(overlay(&mirrors).is_some(), "events resumed: serves again");
        assert!(!mirrors.peers.lock().unwrap()[peer].warned_stale);
        // An unknown/never-registered peer never serves.
        assert!(!mirrors.peer_serving("10.0.0.9:9"));
    }

    // The PF3 invariant: with --freshness-ms S, watermark-covered retention
    // is max(MIRROR_GC, 4×S) so a bounded-stale reader can never fall in
    // the gap between a stale committed snapshot and a GC'd mirror item.
    #[test]
    fn effective_mirror_gc_is_max_of_floor_and_4x_freshness() {
        assert_eq!(effective_mirror_gc(0), MIRROR_GC);
        assert_eq!(effective_mirror_gc(1000), MIRROR_GC, "4 s < the 30 s floor");
        assert_eq!(effective_mirror_gc(7500), MIRROR_GC, "exactly the floor");
        assert_eq!(
            effective_mirror_gc(7501),
            Duration::from_millis(30_004),
            "past the floor: 4× the freshness bound"
        );
        assert_eq!(effective_mirror_gc(60_000), Duration::from_secs(240));
        assert_eq!(
            effective_mirror_gc(u64::MAX),
            Duration::from_millis(u64::MAX)
        );
    }

    #[test]
    fn merge_overlays_unions_and_checks_pk() {
        let local = Overlay {
            schema: canonical(),
            batches: vec![row(1, "l")],
            suppress: Some(OverlaySuppress {
                pk_cols: vec!["id".into()],
                keys: Arc::new([vec![1u8]].into_iter().collect()),
            }),
        };
        let peer = Overlay {
            schema: canonical(),
            batches: vec![row(2, "p")],
            suppress: Some(OverlaySuppress {
                pk_cols: vec!["id".into()],
                keys: Arc::new([vec![2u8]].into_iter().collect()),
            }),
        };
        let merged = merge_overlays(Some(local), Some(peer)).unwrap().unwrap();
        assert_eq!(merged.batches.len(), 2);
        assert_eq!(merged.suppress.unwrap().keys.len(), 2);
        // PK disagreement fails loudly.
        let a = Overlay {
            schema: canonical(),
            batches: vec![],
            suppress: Some(OverlaySuppress {
                pk_cols: vec!["id".into()],
                keys: Arc::new(Default::default()),
            }),
        };
        let b = Overlay {
            schema: canonical(),
            batches: vec![],
            suppress: Some(OverlaySuppress {
                pk_cols: vec!["other".into()],
                keys: Arc::new(Default::default()),
            }),
        };
        assert!(merge_overlays(Some(a), Some(b)).is_err());
    }
}
