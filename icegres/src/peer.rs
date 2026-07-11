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
//! at stake but the freshness bonus) — with ONE warn per outage and the
//! `icegres_peer_tail_age_ms` gauge tracking staleness. The
//! single-buffering-writer-per-table deployment model is unchanged.

use std::collections::HashMap;
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
                // Wire data columns are already canonical types, so key
                // encoding compares equal to committed-scan keys.
                let keys = keyed::encode_batch_keys(&run, &self.pk_cols)?;
                for (row, key) in keys.into_iter().enumerate() {
                    self.upsert_key(key, seq, Some(run.slice(row, 1)));
                }
            }
            "delete" => {
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

    /// Route-on-append (the mirror-side `route_appends`): rows whose key has
    /// an OLDER keyed entry supersede it as an upsert at this seq; the rest
    /// stay appends. Ordering-robust: an append older than the key's entry
    /// stays an append and is suppressed at read time by the newer entry.
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
            .map(|k| self.keyed.get(k).is_some_and(|e| e.seq < seq))
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

    /// Drop items covered by a watermark observed at least `gc_grace` ago
    /// (a bounded-stale reader may still need younger coverage;
    /// [`effective_mirror_gc`] sizes the grace to the freshness bound).
    fn gc(&mut self) {
        let now = Instant::now();
        let threshold = self
            .covered_marks
            .iter()
            .filter(|(at, _)| now.duration_since(*at) >= self.gc_grace)
            .map(|(_, w)| *w)
            .max();
        let Some(threshold) = threshold else { return };
        self.appends.retain(|a| a.seq > threshold);
        self.keyed.retain(|_, e| e.seq > threshold);
        self.covered_marks
            .retain(|(at, w)| *w > threshold || now.duration_since(*at) < self.gc_grace);
    }

    /// The overlay this mirror contributes to a scan whose committed
    /// metadata is `metadata` — the exactly-once rule from the module docs.
    fn overlay_for(&self, ident: &TableIdent, metadata: &TableMetadata) -> Result<Option<Overlay>> {
        let w = parse_watermark_property(
            ident,
            metadata
                .properties()
                .get(&self.property_key)
                .map(String::as_str),
        );
        self.overlay_at(w)
    }

    /// [`overlay_for`](Self::overlay_for) at an already-resolved scan
    /// watermark `w` (`None` = the scan's metadata carries no watermark for
    /// this peer — nothing committed, include everything).
    fn overlay_at(&self, w: Option<u64>) -> Result<Option<Overlay>> {
        let newer = |seq: u64| w.is_none_or(|w| seq > w);
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
                batches.push(row.clone());
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

    /// Install (replace) a table mirror from a fresh TailSnapshot header.
    fn install(
        &self,
        ident: TableIdent,
        peer: &str,
        property_key: String,
        schema: ArrowSchemaRef,
        pk_cols: Vec<String>,
    ) {
        let mut tables = self.tables.lock().expect("peer mirror lock poisoned");
        if let Some(existing) = tables.get(&ident) {
            if existing.peer != peer {
                warn!(
                    table = %ident,
                    old_peer = %existing.peer,
                    new_peer = %peer,
                    "two peers serve a tail for the same table — replacing the mirror; \
                     the deployment model is ONE buffering writer per table"
                );
            }
        }
        tables.insert(
            ident,
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
            },
        );
        drop(tables);
        self.touch(peer);
    }

    /// Ingest one wire batch into a table's mirror.
    fn ingest(&self, ident: &TableIdent, batch: &RecordBatch) -> Result<()> {
        let peer = {
            let mut tables = self.tables.lock().expect("peer mirror lock poisoned");
            let mirror = tables
                .get_mut(ident)
                .ok_or_else(|| anyhow!("no mirror installed for {ident}"))?;
            mirror.ingest(batch)?;
            mirror.peer.clone()
        };
        self.touch(&peer);
        Ok(())
    }

    /// Drop a table's mirror (disconnect → fall back to commit cadence).
    fn drop_table(&self, ident: &TableIdent) {
        self.tables
            .lock()
            .expect("peer mirror lock poisoned")
            .remove(ident);
    }

    /// The peer overlay for one scan (cache.rs): `None` when nothing is
    /// mirrored for the table, the mirror's peer is past the serving age
    /// bound (PF2 — a hung peer must not serve unboundedly stale rows), or
    /// everything is covered by the scan's own watermark property.
    pub fn overlay(&self, ident: &TableIdent, metadata: &TableMetadata) -> Result<Option<Overlay>> {
        self.overlay_with(ident, |mirror| mirror.overlay_for(ident, metadata))
    }

    /// [`overlay`](Self::overlay)'s gate + dispatch, with the per-mirror
    /// build injectable so the age gate is unit-testable.
    fn overlay_with(
        &self,
        ident: &TableIdent,
        build: impl FnOnce(&TableMirror) -> Result<Option<Overlay>>,
    ) -> Result<Option<Overlay>> {
        let tables = self.tables.lock().expect("peer mirror lock poisoned");
        let Some(mirror) = tables.get(ident) else {
            return Ok(None);
        };
        if !self.peer_serving(&mirror.peer) {
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
use arrow_flight::Ticket;
use futures::StreamExt;
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

/// Connect-discover-subscribe loop for one peer, forever, with backoff.
/// WARNs once per outage (the connect flag resets it), then logs at debug.
async fn peer_loop(peer: String, mirrors: Arc<PeerMirrors>) {
    let mut backoff = BACKOFF_MIN;
    let mut warned = false;
    loop {
        let mut connected = false;
        let err = run_peer(&peer, &mirrors, &mut connected)
            .await
            .expect_err("run_peer only returns on error");
        if connected {
            // We had a live session: this is a NEW outage.
            warned = false;
            backoff = BACKOFF_MIN;
        }
        if !warned {
            warn!(
                peer = %peer,
                "peer tail unavailable — reads fall back to commit-cadence \
                 freshness until it returns (rows are tail-durable on the \
                 peer; only the freshness bonus is lost): {err:#}"
            );
            warned = true;
        } else {
            tracing::debug!(peer = %peer, "peer tail still unavailable: {err:#}");
        }
        tokio::time::sleep(backoff).await;
        backoff = (backoff * 2).min(BACKOFF_MAX);
    }
}

/// One connected session against a peer: poll its table list and keep one
/// subscriber task per table alive. Returns Err on any connection-level
/// failure (the caller backs off and reconnects); sets `connected` once a
/// session was actually established.
async fn run_peer(peer: &str, mirrors: &Arc<PeerMirrors>, connected: &mut bool) -> Result<()> {
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
    info!(peer = %peer, "connected to peer tail API");
    *connected = true;
    let mut subscribers: HashMap<TableIdent, tokio::task::JoinHandle<()>> = HashMap::new();
    let result: Result<()> = async {
        loop {
            let tables = list_tables(&channel).await?;
            for ident in tables {
                let stale = subscribers
                    .get(&ident)
                    .is_none_or(|handle| handle.is_finished());
                if stale {
                    let channel = channel.clone();
                    let mirrors = mirrors.clone();
                    let peer = peer.to_string();
                    let table = ident.clone();
                    subscribers.insert(
                        ident,
                        tokio::spawn(async move {
                            if let Err(e) = mirror_table(&channel, &peer, &table, &mirrors).await {
                                mirrors.drop_table(&table);
                                warn!(
                                    peer = %peer,
                                    table = %table,
                                    "peer tail mirror dropped (fallback to commit \
                                     cadence); will re-establish: {e:#}"
                                );
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
    // their mirrors (fallback semantics).
    for (ident, handle) in subscribers {
        handle.abort();
        mirrors.drop_table(&ident);
    }
    result
}

/// Fetch the peer's buffered-table list.
async fn list_tables(channel: &Channel) -> Result<Vec<TableIdent>> {
    let mut client = tail_client(channel);
    let ticket = Ticket {
        ticket: crate::tailapi::TailTicket::Tables.encode().into(),
    };
    let stream = client
        .do_get(ticket)
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

/// Snapshot → install → subscribe loop for one table. Returns Err on any
/// stream failure; the caller drops the mirror (fallback) and the discovery
/// loop re-spawns us.
async fn mirror_table(
    channel: &Channel,
    peer: &str,
    table: &TableIdent,
    mirrors: &Arc<PeerMirrors>,
) -> Result<()> {
    let mut client = tail_client(channel);
    // 1. Snapshot.
    let ticket = Ticket {
        ticket: crate::tailapi::TailTicket::Snapshot {
            table: table.clone(),
        }
        .encode()
        .into(),
    };
    let stream = client
        .do_get(ticket)
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
    mirrors.install(table.clone(), peer, property_key, schema.clone(), pk_cols);
    for batch in &backlog {
        mirrors.ingest(table, batch)?;
    }
    info!(
        peer = %peer,
        table = %table,
        high,
        items = backlog.len(),
        "peer tail mirror installed"
    );
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
        .do_get(ticket)
        .await
        .map_err(|e| anyhow!("TailSubscribe({table}) failed: {e}"))?
        .into_inner();
    let mut decoder =
        FlightDataDecoder::new(stream.map(|r| r.map_err(|e| FlightError::Tonic(Box::new(e)))));
    while let Some(msg) = decoder.next().await {
        let msg = msg.map_err(|e| anyhow!("TailSubscribe({table}) stream failed: {e}"))?;
        if let DecodedPayload::RecordBatch(batch) = msg.payload {
            mirrors.ingest(table, &batch)?;
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
        mirrors.install(
            ident(),
            peer,
            "icegres.tail-seq.peer".into(),
            canonical(),
            Vec::new(),
        );
        let wire = tailapi::wire_schema(&canonical(), &ident(), "icegres.tail-seq.peer", 0, &[]);
        let wb = tailapi::wire_batch(&wire, 3, TailEventKind::Append, Some(&row(1, "a"))).unwrap();
        mirrors.ingest(&ident(), &wb).unwrap();
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
        mirrors.ingest(&ident(), &wb).unwrap();
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
