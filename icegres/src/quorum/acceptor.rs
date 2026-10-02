//! The quorum-tail acceptor ("icekeeper"): consensus state machine +
//! storage, served over TCP by the `icekeeperd` binary and by the
//! in-process test harness.
//!
//! Adapted from neondatabase/neon safekeeper (Apache-2.0); substantially
//! modified for icegres's generic tail log. The state machine follows
//! `neon/safekeeper/src/safekeeper.rs` (voting, term-history adoption,
//! divergence truncation, the append guards) and the control-file
//! persistence discipline follows `neon/safekeeper/src/control_file.rs`
//! (durable small-file replacement — here `crate::segment::write_atomic`
//! over a JSON document instead of a versioned binary blob).
//!
//! # Persist-before-respond (the load-bearing rules)
//!
//! * **Greeting adopting a tail id**: the id is persisted BEFORE the
//!   response leaves — it is the permanent wrong-cluster guard.
//! * **Vote grant**: the bumped term is persisted BEFORE the response —
//!   a vote must survive a crash, or two proposers could win one term.
//!   Note ONE `term` field doubles as "voted for term" (neon's design): a
//!   vote for term T just sets `term = T`, and since terms only move up a
//!   vote is granted at most once per term — no candidate identity needed.
//! * **ProposerElected**: term bump, log truncation, and the adopted term
//!   history are all durable before the ack.
//! * **AppendRequest**: the record bytes are fsynced before the response;
//!   the control file is NOT rewritten per append — `commit_lsn` /
//!   `horizon_lsn` advance in memory and persist lazily (their regression
//!   on a crash only causes redundant re-acks, never loss).
//!
//! # Storage
//!
//! `<data-dir>/control.json` (atomic replace) + `<data-dir>/wal/` holding
//! LSN-named segments (`%016x.seg` = the start LSN of the segment's first
//! record) of concatenated record frames. The flush position is NOT
//! persisted: it is recomputed at boot by scanning the segments —
//! truncating at the first torn/corrupt frame, exactly like the local
//! tail's replay scan. A `<data-dir>/.lock` flock makes the dir
//! single-process.

use std::fs::{self, File, OpenOptions};
use std::io::{Read as _, Seek as _, SeekFrom, Write as _};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use anyhow::{anyhow, bail, Context as _, Result};
use serde_json::Value;

use super::proto::{
    decode_records, find_highest_common_point, read_message_admitted, write_message, Message,
    TermHistory, MAX_HEADER_BYTES, MAX_MESSAGE_BYTES, WRONG_CLUSTER_MARK,
};
use crate::segment::{lock_dir_exclusive, scan_frame_bytes, sync_dir, write_atomic, LOG_KIND_LOG};

/// Control-file format version (refuse anything else loudly).
const CONTROL_FORMAT: u64 = 1;

/// Rotate the active segment once it reaches this many bytes, so horizon
/// advances can delete whole covered files.
const SEGMENT_ROTATE_BYTES: u64 = 16 << 20;

/// Everything that must survive a restart (neon `TimelinePersistentState`,
/// stripped to the consensus-critical fields).
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct PersistentState {
    /// The cluster identity, adopted permanently from the first greeting
    /// that carries one; any later greeting with a different id is refused.
    pub tail_id: Option<String>,
    /// Operator-assigned diagnostic id of this acceptor.
    pub node_id: u64,
    /// Last term this acceptor voted for / acknowledged — one field for
    /// both (see the module docs).
    pub term: u64,
    /// Adopted IN FULL from the proposer; may extend beyond the local log
    /// (always consult it through `up_to(flush)`).
    pub term_history: TermHistory,
    /// Quorum-committed position as this acceptor last heard it, clamped
    /// to the local flush. Lazily persisted (regression-safe).
    pub commit_lsn: u64,
    /// Everything below is fully covered (per-table truncated) — segments
    /// under it are deletable. Lazily persisted.
    pub horizon_lsn: u64,
}

impl PersistentState {
    pub fn new(node_id: u64) -> Self {
        PersistentState {
            tail_id: None,
            node_id,
            term: 0,
            term_history: TermHistory::default(),
            commit_lsn: 0,
            horizon_lsn: 0,
        }
    }

    fn to_json(&self) -> Value {
        serde_json::json!({
            "format": CONTROL_FORMAT,
            "tail_id": self.tail_id,
            "node_id": self.node_id,
            "term": self.term,
            "term_history": self.term_history.to_json(),
            "commit_lsn": self.commit_lsn,
            "horizon_lsn": self.horizon_lsn,
        })
    }

    fn from_json(v: &Value) -> Result<Self> {
        let format = v.get("format").and_then(Value::as_u64).unwrap_or(0);
        if format != CONTROL_FORMAT {
            bail!(
                "icekeeper control file declares format {format}, this build reads \
                 {CONTROL_FORMAT}; recover with the version that wrote it"
            );
        }
        Ok(PersistentState {
            tail_id: v.get("tail_id").and_then(Value::as_str).map(str::to_string),
            node_id: v
                .get("node_id")
                .and_then(Value::as_u64)
                .ok_or_else(|| anyhow!("control file has no node_id"))?,
            term: v
                .get("term")
                .and_then(Value::as_u64)
                .ok_or_else(|| anyhow!("control file has no term"))?,
            term_history: TermHistory::from_json(
                v.get("term_history")
                    .ok_or_else(|| anyhow!("control file has no term_history"))?,
            )?,
            commit_lsn: v.get("commit_lsn").and_then(Value::as_u64).unwrap_or(0),
            horizon_lsn: v.get("horizon_lsn").and_then(Value::as_u64).unwrap_or(0),
        })
    }
}

/// Durable home of the [`PersistentState`] (a trait so the state machine
/// unit-tests run with zero I/O, mirroring neon's `control_file::Storage`).
pub(crate) trait ControlStore: Send {
    /// Must not return `Ok` before the state is durable.
    fn persist(&mut self, state: &PersistentState) -> Result<()>;
}

/// Durable home of the record log (trait for the same zero-I/O-test
/// reason). Every method's LSNs are record boundaries.
pub(crate) trait WalStore: Send {
    /// End of the durable log (0 = nothing, or the log's local start).
    fn flush_lsn(&self) -> u64;
    /// True when NOTHING was ever written locally — a fresh acceptor may
    /// then adopt an arbitrary first begin position (joining a cluster
    /// whose covered prefix it never needs).
    fn is_empty(&self) -> bool;
    /// Durably (fsync) append `frames` (validated record bytes) at
    /// `begin_lsn`, which must equal the current write position (or set
    /// the local start when empty).
    fn append(&mut self, begin_lsn: u64, frames: &[u8]) -> Result<()>;
    /// Drop everything at/after `lsn` (divergence truncation). `lsn >=
    /// flush` is a no-op.
    fn truncate_from(&mut self, lsn: u64) -> Result<()>;
    /// Best-effort GC of storage fully below `horizon`.
    fn drop_below(&mut self, horizon: u64);
    /// The lowest LSN still retained locally (the start of the first
    /// surviving segment; the flush position when nothing is retained; 0
    /// for a never-written log). Everything below was GC'd on a
    /// drop_below instruction, i.e. it was covered — so the horizon this
    /// acceptor REPORTS may never be lower (FIX C2/I1 defense in depth:
    /// a stale persisted horizon must not send recovery into a GC'd
    /// range).
    fn retained_start(&self) -> u64;
    /// The raw frame bytes of `[from, to)`; errors when the range is not
    /// fully retained.
    fn read(&self, from: u64, to: u64) -> Result<Vec<u8>>;
}

/// The acceptor state machine: pure message-in/message-out over the two
/// storage traits.
pub(crate) struct Acceptor<C: ControlStore, W: WalStore> {
    pub state: PersistentState,
    pub ctrl: C,
    pub wal: W,
    /// Set when a persist failed AFTER the in-memory state may have
    /// diverged from disk: everything then fails until a restart reloads
    /// the durable truth (mirrors the local tail's poisoning stance).
    wedged: Option<String>,
    /// Held for the process lifetime when the acceptor owns a data dir.
    _lock: Option<File>,
}

impl<C: ControlStore, W: WalStore> Acceptor<C, W> {
    pub fn new(state: PersistentState, ctrl: C, wal: W) -> Self {
        let mut state = state;
        // Never claim committed what we do not hold: the flush position is
        // recomputed at boot and a torn tail may have been truncated.
        state.commit_lsn = state.commit_lsn.min(wal.flush_lsn());
        Acceptor {
            state,
            ctrl,
            wal,
            wedged: None,
            _lock: None,
        }
    }

    /// Handle one request; failures become [`Message::Error`] responses
    /// (the connection stays usable — the proposer decides what to do).
    pub fn process(&mut self, msg: Message) -> Message {
        if let Some(why) = &self.wedged {
            return Message::Error {
                message: format!("acceptor is wedged (restart it): {why}"),
            };
        }
        let res = match msg {
            Message::Greeting { tail_id } => self.handle_greeting(tail_id),
            Message::VoteRequest { term } => self.handle_vote(term),
            Message::Elected {
                term,
                start_lsn,
                term_history,
            } => self.handle_elected(term, start_lsn, term_history),
            Message::Append {
                term,
                begin_lsn,
                end_lsn,
                commit_lsn,
                horizon_lsn,
                records,
            } => self.handle_append(term, begin_lsn, end_lsn, commit_lsn, horizon_lsn, &records),
            Message::Read { from_lsn, to_lsn } => self.handle_read(from_lsn, to_lsn),
            other => Err(anyhow!("unexpected message {other:?} sent to an acceptor")),
        };
        match res {
            Ok(resp) => resp,
            Err(e) => Message::Error {
                message: format!("{e:#}"),
            },
        }
    }

    /// Persist `next`, adopting it in memory ONLY on success — the
    /// persist-before-respond discipline in one place. A failure wedges
    /// the acceptor (memory and disk may now disagree).
    fn persist(&mut self, next: PersistentState) -> Result<()> {
        match self.ctrl.persist(&next) {
            Ok(()) => {
                self.state = next;
                Ok(())
            }
            Err(e) => {
                self.wedged = Some(format!("control file persist failed: {e:#}"));
                Err(e.context("control file persist failed"))
            }
        }
    }

    fn handle_greeting(&mut self, tail_id: Option<String>) -> Result<Message> {
        // A permanent identity must remain encodable as terms/LSNs grow. Check
        // borrowed data before persistence or cloning a legacy retained ID.
        ensure_greeting_response_fits(self.state.tail_id.as_deref().or(tail_id.as_deref()))?;
        if let Some(id) = tail_id {
            match &self.state.tail_id {
                None => {
                    // Adopt PERMANENTLY — durable before the response.
                    let mut next = self.state.clone();
                    next.tail_id = Some(id);
                    self.persist(next)?;
                }
                Some(mine) if *mine != id => bail!(
                    "this acceptor belongs to tail {mine}; refusing a greeting for tail {id} \
                     ({WRONG_CLUSTER_MARK} — check the --tail-quorum addresses)"
                ),
                _ => {}
            }
        }
        Ok(Message::GreetingResp {
            tail_id: self.state.tail_id.clone(),
            term: self.state.term,
            flush_lsn: self.wal.flush_lsn(),
        })
    }

    /// Vote grant is `my.term < msg.term` STRICTLY — no log-completeness
    /// check (safety comes from the proposer adopting the most advanced
    /// voter and the Raft commit rule). A refusal still reports our
    /// positions. (neon safekeeper.rs:1052-1089.)
    fn handle_vote(&mut self, term: u64) -> Result<Message> {
        // The log is always durably flushed here (every append fsyncs), so
        // the reported flush position is durable — the new proposer starts
        // streaming at our real end, without overlap.
        let flush = self.wal.flush_lsn();
        let mut granted = false;
        if self.state.term < term {
            let mut next = self.state.clone();
            next.term = term;
            // Persist the vote BEFORE sending it out.
            self.persist(next)?;
            granted = true;
        }
        Ok(Message::VoteResponse {
            term: self.state.term,
            granted,
            flush_lsn: flush,
            last_log_term: self.state.term_history.last_log_term(flush),
            term_history: self.state.term_history.up_to(flush),
            // FIX (C2/I1b): report the EFFECTIVE horizon — segments were
            // only ever deleted on a drop_below instruction (proven
            // covered), so the first retained LSN is a truthful horizon
            // floor even when the persisted horizon_lsn is stale (the
            // pre-fix crash shapes). Without this, recovery starts below
            // the donor's retained range and open() fails forever.
            horizon_lsn: self.state.horizon_lsn.max(self.wal.retained_start()),
            commit_lsn: self.state.commit_lsn,
        })
    }

    /// Adopt the elected proposer: truncate our divergent suffix, adopt its
    /// term history in full, persist, ack. (neon safekeeper.rs:1106-1260.)
    fn handle_elected(
        &mut self,
        term: u64,
        start_lsn: u64,
        term_history: TermHistory,
    ) -> Result<Message> {
        if self.state.term < term {
            let mut next = self.state.clone();
            next.term = term;
            self.persist(next)?;
        }
        if self.state.term > term {
            // Stale proposer: the higher term in the response is the fence.
            return Ok(Message::ElectedResp {
                term: self.state.term,
                ok: false,
            });
        }
        let flush = self.wal.flush_lsn();
        let sk_th = self.state.term_history.up_to(flush);
        let lcp = find_highest_common_point(&term_history, &sk_th, flush)?;
        let expected = match lcp {
            Some(t) => t.lsn,
            None if sk_th.0.is_empty() && self.wal.is_empty() => {
                // Fresh acceptor joining a cluster with history: adopt the
                // proposer's start as our local log start.
                start_lsn
            }
            None => term_history
                .0
                .first()
                .map(|e| e.lsn)
                .ok_or_else(|| anyhow!("elected message carries an empty term history"))?,
        };
        if expected != start_lsn {
            // Transient race (our flush moved between the vote and this
            // message): error out; the proposer reconnects and recomputes.
            bail!(
                "elected start_lsn {start_lsn} does not match the divergence point \
                 {expected} computed here (flush moved?); retry the handshake"
            );
        }
        if start_lsn < self.state.commit_lsn {
            bail!(
                "refusing to truncate at {start_lsn} BELOW the committed position {} — \
                 a proposer may never rewind committed records",
                self.state.commit_lsn
            );
        }
        self.wal.truncate_from(start_lsn)?;
        let mut next = self.state.clone();
        next.term_history = term_history;
        next.commit_lsn = next.commit_lsn.min(self.wal.flush_lsn());
        self.persist(next)?;
        Ok(Message::ElectedResp {
            term: self.state.term,
            ok: true,
        })
    }

    /// Append records: only legal in the exact term we acknowledged via
    /// ProposerElected; a HIGHER local term is the stale-proposer fence
    /// (term-only response). (neon safekeeper.rs:1293-1396.)
    fn handle_append(
        &mut self,
        term: u64,
        begin_lsn: u64,
        end_lsn: u64,
        commit_lsn: u64,
        horizon_lsn: u64,
        records: &[u8],
    ) -> Result<Message> {
        if self.state.term < term {
            bail!(
                "got an AppendRequest for term {term} before its ProposerElected \
                 (our term is {})",
                self.state.term
            );
        }
        if self.state.term > term {
            // THE fencing mechanism: the old proposer learns the higher
            // term and poisons itself. Never write in a superseded term.
            return Ok(Message::AppendResp {
                term: self.state.term,
                ok: false,
                flush_lsn: 0,
                commit_lsn: 0,
            });
        }
        if records.is_empty() {
            bail!("empty AppendRequest");
        }
        // STRICT validation before anything touches the disk: framing,
        // crcs, and LSN continuity from begin_lsn.
        let recs = decode_records(records, begin_lsn)?;
        drop(recs);
        if begin_lsn + records.len() as u64 != end_lsn {
            bail!(
                "AppendRequest end_lsn {end_lsn} does not match begin {begin_lsn} + \
                 {} payload bytes",
                records.len()
            );
        }
        let pos = if self.wal.is_empty() {
            begin_lsn
        } else {
            self.wal.flush_lsn()
        };
        if begin_lsn != pos {
            bail!(
                "non-consecutive append: write position is {pos}, request begins at \
                 {begin_lsn} (rewrites and gaps are only ever repositioned by \
                 ProposerElected)"
            );
        }
        self.wal.append(begin_lsn, records)?;
        let flush = self.wal.flush_lsn();
        // commit = min(max(candidate, current), flush): monotonic, never
        // beyond what we hold. In-memory only (lazily persisted).
        self.state.commit_lsn = commit_lsn.max(self.state.commit_lsn).min(flush);
        let new_horizon = self
            .state
            .horizon_lsn
            .max(horizon_lsn.min(self.state.commit_lsn));
        if new_horizon > self.state.horizon_lsn {
            // FIX (C2/I1a): the control file must know the horizon BEFORE
            // any segment below it is deleted. Persisting only lazily let a
            // full-cluster restart report a stale horizon while the
            // segments above it were already gone — recovery then read a
            // GC'd range and open() failed forever. Persist-then-delete
            // leaves only the harmless shape (horizon durable, segments
            // still present) on a crash in between.
            let mut next = self.state.clone();
            next.horizon_lsn = new_horizon;
            self.persist(next)?;
            self.wal.drop_below(new_horizon);
        }
        Ok(Message::AppendResp {
            term: self.state.term,
            ok: true,
            flush_lsn: flush,
            commit_lsn: self.state.commit_lsn,
        })
    }

    fn handle_read(&mut self, from_lsn: u64, to_lsn: u64) -> Result<Message> {
        let to = to_lsn.min(self.wal.flush_lsn());
        if to.saturating_sub(from_lsn) > (MAX_MESSAGE_BYTES - MAX_HEADER_BYTES - 4) as u64 {
            bail!("requested WAL range exceeds the quorum response limit");
        }
        let records = if from_lsn >= to {
            Vec::new()
        } else {
            self.wal.read(from_lsn, to)?
        };
        Ok(Message::ReadResp { from_lsn, records })
    }
}

// ---------------------------------------------------------------------------
// File-backed storage
// ---------------------------------------------------------------------------

/// Name of the control file under the data dir.
const CONTROL_FILE: &str = "control.json";
/// Name of the one-process lock file under the data dir.
const LOCK_FILE: &str = ".lock";
/// Subdirectory holding the log segments.
const WAL_DIR: &str = "wal";

/// [`ControlStore`] over `<dir>/control.json` with the tmp + fsync +
/// rename + dir-fsync discipline (`crate::segment::write_atomic`) — the
/// neon control_file.rs pattern.
pub(crate) struct FileControl {
    dir: PathBuf,
    path: PathBuf,
}

impl FileControl {
    fn new(dir: &Path) -> Self {
        FileControl {
            dir: dir.to_path_buf(),
            path: dir.join(CONTROL_FILE),
        }
    }

    fn load(dir: &Path) -> Result<Option<PersistentState>> {
        let path = dir.join(CONTROL_FILE);
        match fs::read_to_string(&path) {
            Ok(raw) => {
                let v: Value = serde_json::from_str(&raw).with_context(|| {
                    format!(
                        "icekeeper control file {} is not valid JSON",
                        path.display()
                    )
                })?;
                Ok(Some(PersistentState::from_json(&v).with_context(|| {
                    format!("icekeeper control file {} is malformed", path.display())
                })?))
            }
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(anyhow!(e).context(format!(
                "cannot read icekeeper control file {}",
                path.display()
            ))),
        }
    }
}

impl ControlStore for FileControl {
    fn persist(&mut self, state: &PersistentState) -> Result<()> {
        let doc = serde_json::to_string_pretty(&state.to_json())
            .context("cannot encode the icekeeper control file")?;
        write_atomic(&self.dir, &self.path, doc.as_bytes(), LOG_KIND_LOG)
    }
}

/// One sealed log segment: `[start, end)` of the record stream.
struct Seg {
    path: PathBuf,
    start: u64,
    end: u64,
}

/// The segment currently receiving appends.
struct ActiveSeg {
    seg: Seg,
    file: File,
}

/// [`WalStore`] over LSN-named segment files (see the module docs).
pub(crate) struct SegmentWal {
    dir: PathBuf,
    sealed: Vec<Seg>,
    active: Option<ActiveSeg>,
    /// End of the durable log; equals the local start when nothing was
    /// ever written.
    flush: u64,
    /// Nothing was ever written locally (no segment holds a record).
    empty: bool,
    rotate_bytes: u64,
    /// A failed append whose rollback also failed: never touch the log
    /// again until a restart rescans it.
    wedged: bool,
}

impl SegmentWal {
    pub fn open(dir: &Path) -> Result<SegmentWal> {
        fs::create_dir_all(dir)
            .with_context(|| format!("cannot create icekeeper wal dir {}", dir.display()))?;
        let mut named: Vec<(u64, PathBuf)> = Vec::new();
        for entry in fs::read_dir(dir)
            .with_context(|| format!("cannot read icekeeper wal dir {}", dir.display()))?
        {
            let path = entry?.path();
            if path.extension().and_then(|e| e.to_str()) != Some("seg") {
                continue;
            }
            let Some(start) = path
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| u64::from_str_radix(s, 16).ok())
            else {
                tracing::warn!(segment = %path.display(), "unparseable segment name; ignoring it");
                continue;
            };
            named.push((start, path));
        }
        named.sort();
        let mut sealed: Vec<Seg> = Vec::new();
        let mut pos: Option<u64> = None;
        let mut stop = false;
        for (start, path) in named {
            if stop {
                // Behind a torn frame or a gap: records past a hole must
                // never replay (they would reorder acked writes) — the
                // quorum re-streams them after the next election.
                tracing::warn!(
                    segment = %path.display(),
                    "deleting icekeeper segment behind a corrupt frame or gap; \
                     the quorum restores its records on the next election"
                );
                let _ = fs::remove_file(&path);
                continue;
            }
            if let Some(p) = pos {
                if start != p {
                    tracing::warn!(
                        segment = %path.display(),
                        expected = p,
                        found = start,
                        "icekeeper segment is not contiguous with the previous one; \
                         deleting it and everything after (the quorum restores the \
                         records on the next election)"
                    );
                    let _ = fs::remove_file(&path);
                    stop = true;
                    continue;
                }
            }
            let (end, hit_bad) = scan_wal_segment(&path, start)?;
            if end == start {
                // No valid frame survived: nothing to keep.
                let _ = fs::remove_file(&path);
            } else {
                sealed.push(Seg { path, start, end });
            }
            pos = Some(end);
            if hit_bad {
                stop = true;
            }
        }
        let flush = pos.unwrap_or(0);
        Ok(SegmentWal {
            dir: dir.to_path_buf(),
            empty: sealed.is_empty() && flush == 0,
            sealed,
            active: None,
            flush,
            rotate_bytes: SEGMENT_ROTATE_BYTES,
            wedged: false,
        })
    }

    fn seal_active(&mut self) {
        if let Some(active) = self.active.take() {
            if active.seg.end > active.seg.start {
                self.sealed.push(active.seg);
            } else {
                let _ = fs::remove_file(&active.seg.path);
            }
        }
    }

    /// Test hook: shrink the rotate threshold so multi-segment shapes are
    /// reachable with tiny records (the in-process integration tests).
    #[cfg(test)]
    pub(crate) fn set_rotate_bytes(&mut self, n: u64) {
        self.rotate_bytes = n;
    }
}

/// Scan one segment: verify every frame (crc + embedded-LSN continuity from
/// `start`), truncating the file at the first invalid one (the expected
/// shape of a power loss mid-append). Returns `(end_lsn, hit_bad_frame)`.
fn scan_wal_segment(path: &Path, start: u64) -> Result<(u64, bool)> {
    let data = fs::read(path)
        .with_context(|| format!("cannot read icekeeper segment {}", path.display()))?;
    let scan = scan_frame_bytes(&data);
    let mut good_end: usize = 0;
    let mut bad = scan.bad;
    let mut pos = start;
    for range in scan.payloads {
        let frame_start = range.start - crate::segment::FRAME_HEADER_BYTES;
        if range.len() < 8 {
            bad = Some("frame shorter than its lsn header".to_string());
            good_end = frame_start;
            break;
        }
        let lsn = u64::from_le_bytes(data[range.start..range.start + 8].try_into().expect("8"));
        if lsn != pos {
            bad = Some(format!("record claims lsn {lsn} at position {pos}"));
            good_end = frame_start;
            break;
        }
        pos = start + range.end as u64;
        good_end = range.end;
    }
    let hit_bad = bad.is_some();
    if let Some(reason) = bad {
        tracing::warn!(
            segment = %path.display(),
            discarded_bytes = data.len() - good_end,
            "icekeeper segment ends in an invalid frame ({reason}); truncating to the \
             last good record (a torn final frame is expected after power loss)"
        );
        let file = OpenOptions::new()
            .write(true)
            .open(path)
            .with_context(|| format!("cannot open {} for truncation", path.display()))?;
        file.set_len(good_end as u64)
            .and_then(|()| file.sync_all())
            .with_context(|| format!("cannot truncate torn segment {}", path.display()))?;
    }
    Ok((start + good_end as u64, hit_bad))
}

impl WalStore for SegmentWal {
    fn flush_lsn(&self) -> u64 {
        self.flush
    }

    fn is_empty(&self) -> bool {
        self.empty
    }

    fn append(&mut self, begin_lsn: u64, frames: &[u8]) -> Result<()> {
        if self.wedged {
            bail!("icekeeper wal is wedged (a failed append could not be rolled back); restart");
        }
        if self.empty {
            self.flush = begin_lsn;
        }
        if begin_lsn != self.flush {
            bail!(
                "wal append at {begin_lsn} does not match the write position {}",
                self.flush
            );
        }
        if self.active.is_none() {
            let path = self.dir.join(format!("{begin_lsn:016x}.seg"));
            let file = OpenOptions::new()
                .create_new(true)
                .append(true)
                .open(&path)
                .with_context(|| format!("cannot create icekeeper segment {}", path.display()))?;
            sync_dir(&self.dir, LOG_KIND_LOG)?;
            self.active = Some(ActiveSeg {
                seg: Seg {
                    path,
                    start: begin_lsn,
                    end: begin_lsn,
                },
                file,
            });
        }
        let active = self.active.as_mut().expect("just ensured");
        let res = active
            .file
            .write_all(frames)
            .and_then(|()| active.file.sync_data());
        if let Err(e) = res {
            // Roll back to the last known-good boundary so acked records
            // never sit behind garbage; a failed rollback wedges the wal.
            let good = active.seg.end - active.seg.start;
            let rolled = active
                .file
                .set_len(good)
                .and_then(|()| active.file.sync_data());
            if let Err(rb) = rolled {
                self.wedged = true;
                tracing::warn!(
                    segment = %active.seg.path.display(),
                    "icekeeper append failed AND its rollback failed ({rb}); wal is \
                     WEDGED until restart (the boot scan re-establishes the boundary)"
                );
            }
            return Err(anyhow!(e).context(format!(
                "icekeeper append to {} failed",
                active.seg.path.display()
            )));
        }
        active.seg.end += frames.len() as u64;
        self.flush = active.seg.end;
        self.empty = false;
        if active.seg.end - active.seg.start >= self.rotate_bytes {
            self.seal_active();
        }
        Ok(())
    }

    fn truncate_from(&mut self, lsn: u64) -> Result<()> {
        if self.wedged {
            bail!("icekeeper wal is wedged; restart");
        }
        if lsn >= self.flush {
            return Ok(());
        }
        self.seal_active();
        // Delete later segments FIRST (descending), then truncate the
        // boundary one — a crash mid-way leaves shapes the boot scan
        // already handles (gaps behind the boundary are deleted).
        self.sealed.sort_by_key(|s| s.start);
        while let Some(seg) = self.sealed.last() {
            if seg.start < lsn {
                break;
            }
            fs::remove_file(&seg.path)
                .with_context(|| format!("cannot delete segment {}", seg.path.display()))?;
            self.sealed.pop();
        }
        if let Some(seg) = self.sealed.last_mut() {
            if seg.end > lsn {
                let file = OpenOptions::new()
                    .write(true)
                    .open(&seg.path)
                    .with_context(|| {
                        format!("cannot open {} for truncation", seg.path.display())
                    })?;
                file.set_len(lsn - seg.start)
                    .and_then(|()| file.sync_all())
                    .with_context(|| format!("cannot truncate segment {}", seg.path.display()))?;
                seg.end = lsn;
                if seg.end == seg.start {
                    let path = seg.path.clone();
                    let _ = fs::remove_file(&path);
                    self.sealed.pop();
                }
            }
        }
        sync_dir(&self.dir, LOG_KIND_LOG)?;
        self.flush = lsn;
        Ok(())
    }

    fn drop_below(&mut self, horizon: u64) {
        self.sealed.sort_by_key(|s| s.start);
        // FIX (C4): the segment holding the current flush position is NEVER
        // deleted, even when fully covered (Neon's rule). With no active
        // segment the flush position lives in the LAST sealed one; deleting
        // it would make a restart rescan derive flush = 0 and the proposer
        // would expel this acceptor (start below the retained base).
        let deletable = if self.active.is_some() {
            self.sealed.len()
        } else {
            self.sealed.len().saturating_sub(1)
        };
        let mut deleted = 0;
        for seg in self.sealed.iter().take(deletable) {
            if seg.end > horizon {
                break;
            }
            // FIX (C3): ascending order, STOP at the first failed deletion.
            // Retain-and-continue left a [kept][gap][newer] shape whose gap
            // made the boot contiguity scan discard the NEWER (possibly
            // acked) segments.
            match fs::remove_file(&seg.path) {
                Ok(()) => deleted += 1,
                Err(e) => {
                    tracing::warn!(
                        segment = %seg.path.display(),
                        "cannot delete covered icekeeper segment (stopping this GC \
                         round; retried on the next horizon advance): {e}"
                    );
                    break;
                }
            }
        }
        self.sealed.drain(..deleted);
    }

    fn retained_start(&self) -> u64 {
        self.sealed
            .iter()
            .map(|s| s.start)
            .chain(self.active.as_ref().map(|a| a.seg.start))
            .min()
            .unwrap_or(self.flush)
    }

    fn read(&self, from: u64, to: u64) -> Result<Vec<u8>> {
        let range = usize::try_from(to.checked_sub(from).context("reversed WAL range")?)
            .context("WAL range exceeds address space")?;
        let mut out: Vec<u8> = Vec::with_capacity(range);
        let mut segs: Vec<&Seg> = self.sealed.iter().collect();
        if let Some(active) = &self.active {
            segs.push(&active.seg);
        }
        segs.sort_by_key(|s| s.start);
        for seg in segs {
            if seg.end <= from || seg.start >= to {
                continue;
            }
            let lo = from.max(seg.start) - seg.start;
            let count = usize::try_from(to.min(seg.end) - from.max(seg.start))
                .context("WAL slice exceeds address space")?;
            let mut file = File::open(&seg.path)
                .with_context(|| format!("cannot read segment {}", seg.path.display()))?;
            file.seek(SeekFrom::Start(lo))?;
            let begin = out.len();
            let end = begin.checked_add(count).context("WAL read size overflow")?;
            anyhow::ensure!(
                end <= range,
                "overlapping WAL segments exceed requested range"
            );
            out.resize(end, 0);
            file.read_exact(&mut out[begin..]).with_context(|| {
                format!(
                    "segment {} is shorter than its bookkeeping",
                    seg.path.display()
                )
            })?;
        }
        if out.len() as u64 != to - from {
            bail!(
                "log range [{from}, {to}) is not fully retained here ({} of {} bytes)",
                out.len(),
                to - from
            );
        }
        Ok(out)
    }
}

/// A file-backed acceptor ready to serve.
pub(crate) type FileAcceptor = Acceptor<FileControl, SegmentWal>;

/// Open (creating if absent) an acceptor data dir: exclusive flock, control
/// file load-or-default, segment scan (recomputing the flush position).
pub(crate) fn open_dir(dir: &Path, node_id: u64) -> Result<FileAcceptor> {
    fs::create_dir_all(dir)
        .with_context(|| format!("cannot create icekeeper data dir {}", dir.display()))?;
    let lock = lock_dir_exclusive(
        dir,
        LOCK_FILE,
        LOG_KIND_LOG,
        &format!(
            "icekeeper data dir {} is LOCKED by another process — most likely another \
             `icekeeperd serve` with the same --data-dir. Give each acceptor its own \
             directory.",
            dir.display()
        ),
    )?;
    let state = match FileControl::load(dir)? {
        Some(state) => {
            if state.node_id != node_id {
                bail!(
                    "icekeeper data dir {} belongs to node {} but --node-id says {node_id}; \
                     refusing to impersonate another acceptor",
                    dir.display(),
                    state.node_id
                );
            }
            state
        }
        None => PersistentState::new(node_id),
    };
    let wal = SegmentWal::open(&dir.join(WAL_DIR))?;
    let mut acceptor = Acceptor::new(state, FileControl::new(dir), wal);
    acceptor._lock = Some(lock);
    Ok(acceptor)
}

/// One serialized state machine; its synchronous work runs on a blocking worker.
pub(crate) type SharedAcceptor = Arc<tokio::sync::Mutex<FileAcceptor>>;

/// Transport allocation budgets, separate from WAL and process RSS. JSON
/// scratch receives conservative headroom and also has a hard header limit.
const HEADER_WORKSPACE_FACTOR: usize = 32;
const RESPONSE_WORKSPACE: usize = HEADER_WORKSPACE_FACTOR * MAX_HEADER_BYTES;

#[derive(Clone, Debug)]
pub(crate) struct ServeConfig {
    pub max_connections: usize,
    pub max_requests: usize,
    pub request_bytes: usize,
    pub response_bytes: usize,
    pub max_read_bytes: usize,
    pub io_timeout: Duration,
}

impl Default for ServeConfig {
    fn default() -> Self {
        Self {
            max_connections: 64,
            max_requests: 16,
            request_bytes: 64 << 20,
            response_bytes: 64 << 20,
            max_read_bytes: 8 << 20,
            io_timeout: Duration::from_secs(30),
        }
    }
}

impl ServeConfig {
    // Only the daemon loads environment settings; shared in-process tests use
    // explicit defaults so parallel tests never mutate the process environment.
    #[cfg_attr(test, allow(dead_code))]
    pub fn from_env() -> Result<Self> {
        fn setting(name: &str, default: usize) -> Result<usize> {
            match std::env::var(name) {
                Ok(value) => value
                    .parse()
                    .with_context(|| format!("{name} must be a positive integer")),
                Err(std::env::VarError::NotPresent) => Ok(default),
                Err(e) => Err(e).with_context(|| format!("cannot read {name}")),
            }
        }
        let defaults = Self::default();
        let cfg = Self {
            max_connections: setting("ICEKEEPER_MAX_CONNECTIONS", defaults.max_connections)?,
            max_requests: setting("ICEKEEPER_MAX_REQUESTS", defaults.max_requests)?,
            request_bytes: setting("ICEKEEPER_REQUEST_BYTES", defaults.request_bytes)?,
            response_bytes: setting("ICEKEEPER_RESPONSE_BYTES", defaults.response_bytes)?,
            max_read_bytes: setting("ICEKEEPER_MAX_READ_BYTES", defaults.max_read_bytes)?,
            io_timeout: Duration::from_millis(u64::try_from(setting(
                "ICEKEEPER_IO_TIMEOUT_MS",
                30_000,
            )?)?),
        };
        cfg.validate()?;
        Ok(cfg)
    }

    fn validate(&self) -> Result<()> {
        for (name, n) in [
            ("max_connections", self.max_connections),
            ("max_requests", self.max_requests),
            ("request_bytes", self.request_bytes),
            ("response_bytes", self.response_bytes),
        ] {
            anyhow::ensure!(
                n > 0 && n <= Semaphore::MAX_PERMITS && u32::try_from(n).is_ok(),
                "{name} must be positive and fit the admission counter"
            );
        }
        anyhow::ensure!(!self.io_timeout.is_zero(), "io_timeout must be positive");
        anyhow::ensure!(
            self.max_read_bytes > 0
                && self.max_read_bytes <= MAX_MESSAGE_BYTES - MAX_HEADER_BYTES - 4,
            "max_read_bytes must fit a quorum response"
        );
        anyhow::ensure!(
            self.response_bytes >= response_charge(self.max_read_bytes)?,
            "response_bytes must hold a maximum read plus encoding/header workspace"
        );
        anyhow::ensure!(
            self.request_bytes >= 1024,
            "request_bytes must be at least 1024"
        );
        Ok(())
    }
}

fn ensure_greeting_response_fits(tail_id: Option<&str>) -> Result<()> {
    struct BoundedCounter(usize);
    impl std::io::Write for BoundedCounter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0 = self.0.checked_sub(bytes.len()).ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "header limit exceeded")
            })?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    // Match GreetingResp's fixed keys with maximum-width numeric values;
    // serializing just the borrowed ID accounts for JSON escaping without
    // allocating a Value/String copy, even for a large legacy identity.
    const FIXED_BYTES: usize = br#"{"type":"greeting_resp","tail_id":"#.len()
        + br#","term":18446744073709551615,"flush_lsn":18446744073709551615}"#.len();
    serde_json::to_writer(BoundedCounter(MAX_HEADER_BYTES - FIXED_BYTES), &tail_id)
        .context("tail identity exceeds the bounded greeting response header")
}

fn request_charge(len: usize, header_len: usize) -> Result<usize> {
    // Record validation also retains decoded records and frame-range metadata.
    len.checked_mul(8)
        .and_then(|n| {
            header_len
                .checked_mul(HEADER_WORKSPACE_FACTOR)
                .and_then(|h| n.checked_add(h))
        })
        .context("quorum request admission size overflow")
}

fn response_charge(payload: usize) -> Result<usize> {
    payload
        .checked_mul(2)
        .and_then(|n| n.checked_add(RESPONSE_WORKSPACE))
        .context("quorum response admission size overflow")
}

fn reserve(pool: &Arc<Semaphore>, bytes: usize, name: &str) -> Result<OwnedSemaphorePermit> {
    let bytes = u32::try_from(bytes)
        .with_context(|| format!("{name} request exceeds admission counter"))?;
    pool.clone().try_acquire_many_owned(bytes).with_context(|| {
        format!("acceptor {name} capacity exhausted; retry after current requests finish")
    })
}

struct Admission {
    config: ServeConfig,
    requests: Arc<Semaphore>,
    input: Arc<Semaphore>,
    output: Arc<Semaphore>,
}

impl Admission {
    fn new(config: ServeConfig) -> Result<Self> {
        config.validate()?;
        Ok(Self {
            requests: Arc::new(Semaphore::new(config.max_requests)),
            input: Arc::new(Semaphore::new(config.request_bytes)),
            output: Arc::new(Semaphore::new(config.response_bytes)),
            config,
        })
    }
}

/// Compatibility wrapper for in-process consensus tests, with finite defaults.
#[cfg(test)]
pub(crate) async fn serve(
    listener: tokio::net::TcpListener,
    acceptor: SharedAcceptor,
) -> Result<()> {
    serve_with_config(listener, acceptor, ServeConfig::default()).await
}

pub(crate) async fn serve_with_config(
    listener: tokio::net::TcpListener,
    acceptor: SharedAcceptor,
    config: ServeConfig,
) -> Result<()> {
    config.validate()?;
    let connections = Arc::new(Semaphore::new(config.max_connections));
    let admission = Arc::new(Admission::new(config)?);
    let mut tasks = tokio::task::JoinSet::new();
    loop {
        // Never accumulate a handler or accepted socket beyond this cap.
        let permit = connections.clone().acquire_owned().await?;
        let (stream, peer) = listener.accept().await.context("icekeeper accept failed")?;
        let _ = stream.set_nodelay(true);
        let acceptor = acceptor.clone();
        let admission = admission.clone();
        while tasks.try_join_next().is_some() {}
        tasks.spawn(async move {
            let _connection = permit;
            if let Err(e) = handle_conn(stream, acceptor, admission).await {
                tracing::debug!(%peer, "icekeeper connection ended: {e:#}");
            }
        });
    }
}

/// Once started, processing survives cancellation of the network task. The
/// owned state guard and permits remain in the blocking closure until fsync
/// finishes. Lock BEFORE spawning, so only one blocking job per acceptor can
/// run or wait in Tokio's blocking queue. An uncertain disk result is never
/// undone merely because its response recipient disappeared.
async fn process_blocking<C: ControlStore + 'static, W: WalStore + 'static>(
    acceptor: Arc<tokio::sync::Mutex<Acceptor<C, W>>>,
    msg: Message,
    input: OwnedSemaphorePermit,
    request: OwnedSemaphorePermit,
    admission: Arc<Admission>,
) -> Result<(Message, OwnedSemaphorePermit)> {
    let mut state = tokio::time::timeout(admission.config.io_timeout, acceptor.lock_owned())
        .await
        .context("acceptor processing queue timed out")?;
    let payload = if let Message::Read { from_lsn, to_lsn } = &msg {
        let span = to_lsn
            .checked_sub(*from_lsn)
            .context("reversed WAL range")?;
        anyhow::ensure!(
            span <= admission.config.max_read_bytes as u64,
            "requested WAL range exceeds ICEKEEPER_MAX_READ_BYTES"
        );
        usize::try_from(span)?
    } else {
        0
    };
    // A decoded legacy control history must also fit the bounded response
    // header. Check before process() clones it into a vote response.
    let history = match &msg {
        Message::VoteRequest { .. } => Some(&state.state.term_history),
        Message::Elected { term_history, .. } => Some(term_history),
        _ => None,
    };
    if let Some(history) = history {
        let header_size = history.0.iter().try_fold(512usize, |n, entry| {
            n.checked_add(4 + entry.term.to_string().len() + entry.lsn.to_string().len())
                .context("term history header overflow")
        })?;
        anyhow::ensure!(
            header_size <= MAX_HEADER_BYTES,
            "term history exceeds the bounded response header"
        );
    }
    let output = reserve(
        &admission.output,
        response_charge(payload)?,
        "response bytes",
    )?;
    tokio::task::spawn_blocking(move || {
        let _input = input;
        let _request = request;
        match std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| state.process(msg))) {
            Ok(response) => Ok((response, output)),
            Err(_) => {
                state.wedged = Some("state-machine worker panicked".to_string());
                Err(anyhow!(
                    "acceptor state-machine worker panicked; restart required"
                ))
            }
        }
    })
    .await
    .context("acceptor blocking worker failed")?
}

async fn handle_conn(
    mut stream: tokio::net::TcpStream,
    acceptor: SharedAcceptor,
    admission: Arc<Admission>,
) -> Result<()> {
    loop {
        let (msg, (request, input)) = tokio::time::timeout(
            admission.config.io_timeout,
            read_message_admitted(&mut stream, |len, header| {
                let request = reserve(&admission.requests, 1, "request count")?;
                let input = reserve(
                    &admission.input,
                    request_charge(len, header)?,
                    "request bytes",
                )?;
                Ok((request, input))
            }),
        )
        .await
        .context("acceptor message read timed out")??;
        anyhow::ensure!(
            matches!(
                msg,
                Message::Greeting { .. }
                    | Message::VoteRequest { .. }
                    | Message::Elected { .. }
                    | Message::Append { .. }
                    | Message::Read { .. }
            ),
            "unexpected response message sent to acceptor"
        );
        let (response, _output) =
            process_blocking(acceptor.clone(), msg, input, request, admission.clone()).await?;
        tokio::time::timeout(
            admission.config.io_timeout,
            write_message(&mut stream, &response),
        )
        .await
        .context("acceptor response write timed out")??;
    }
}

// ---------------------------------------------------------------------------
// Unit tests — the state machine over in-memory stores (zero I/O; the
// shape of neon's safekeeper.rs tests) plus SegmentWal file tests.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use crate::quorum::proto::{Record, TermLsn, RECORD_FRAME};

    struct ScopedTestDir(PathBuf);

    impl ScopedTestDir {
        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for ScopedTestDir {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn admission() -> Arc<Admission> {
        Arc::new(Admission::new(ServeConfig::default()).unwrap())
    }

    async fn run_admitted<C: ControlStore + 'static, W: WalStore + 'static>(
        state: Arc<tokio::sync::Mutex<Acceptor<C, W>>>,
        msg: Message,
        admission: Arc<Admission>,
    ) -> Result<(Message, OwnedSemaphorePermit)> {
        let input = reserve(&admission.input, 1024, "test input")?;
        let request = reserve(&admission.requests, 1, "test request")?;
        process_blocking(state, msg, input, request, admission).await
    }

    #[test]
    fn admission_limits_reject_invalid_and_overflowing_budgets() {
        let cfg = ServeConfig::default();
        assert!(cfg.validate().is_ok());
        for changed in [
            ServeConfig {
                max_connections: 0,
                ..cfg.clone()
            },
            ServeConfig {
                max_requests: usize::MAX,
                ..cfg.clone()
            },
            ServeConfig {
                request_bytes: 0,
                ..cfg.clone()
            },
            ServeConfig {
                response_bytes: 1024,
                ..cfg.clone()
            },
            ServeConfig {
                max_read_bytes: usize::MAX,
                ..cfg.clone()
            },
            ServeConfig {
                io_timeout: Duration::ZERO,
                ..cfg.clone()
            },
        ] {
            assert!(changed.validate().is_err());
        }
        assert!(request_charge(usize::MAX, 1).is_err());
        assert!(response_charge(usize::MAX).is_err());
        let pool = Arc::new(Semaphore::new(1024));
        let held = reserve(&pool, 1024, "test").unwrap();
        assert!(reserve(&pool, 1, "test").is_err());
        drop(held);
        assert_eq!(pool.available_permits(), 1024);
    }

    #[test]
    fn greeting_rejects_identity_that_cannot_fit_future_responses_before_persisting() {
        let id = "x".repeat(MAX_HEADER_BYTES - 64);
        assert!(Message::Greeting {
            tail_id: Some(id.clone())
        }
        .encode()
        .is_ok());
        let mut state = Acceptor::new(PersistentState::new(1), MemControl::new(), MemWal::new());
        let response = state.process(Message::Greeting { tail_id: Some(id) });
        assert!(
            matches!(response, Message::Error { message } if message.contains("greeting response"))
        );
        assert!(state.state.tail_id.is_none());
        assert!(state.ctrl.persisted.is_none());
        assert!(state.wedged.is_none());
        // A legacy identity is checked without cloning it into a response.
        state.state.tail_id = Some("x".repeat(MAX_HEADER_BYTES * 2));
        let response = state.process(Message::Greeting { tail_id: None });
        assert!(
            matches!(response, Message::Error { message } if message.contains("greeting response"))
        );
        // An exactly full response remains valid even at maximum term/LSN.
        let empty = Message::GreetingResp {
            tail_id: Some(String::new()),
            term: u64::MAX,
            flush_lsn: u64::MAX,
        }
        .encode()
        .unwrap();
        let mut boundary_id = "x".repeat(MAX_HEADER_BYTES - (empty.len() - 12));
        assert!(ensure_greeting_response_fits(Some(&boundary_id)).is_ok());
        assert!(Message::GreetingResp {
            tail_id: Some(boundary_id.clone()),
            term: u64::MAX,
            flush_lsn: u64::MAX,
        }
        .encode()
        .is_ok());
        boundary_id.push('x');
        assert!(ensure_greeting_response_fits(Some(&boundary_id)).is_err());
        // JSON escaping and maximum-width numeric fields use the actual encoder.
        let id = "\"".repeat(100);
        assert!(ensure_greeting_response_fits(Some(&id)).is_ok());
        assert!(Message::GreetingResp {
            tail_id: Some(id),
            term: u64::MAX,
            flush_lsn: u64::MAX
        }
        .encode()
        .is_ok());
    }

    #[tokio::test]
    async fn oversized_read_and_exhausted_output_are_rejected_before_wal_access() {
        struct NoReadWal(MemWal);
        impl WalStore for NoReadWal {
            fn flush_lsn(&self) -> u64 {
                u64::MAX
            }
            fn is_empty(&self) -> bool {
                self.0.is_empty()
            }
            fn append(&mut self, _: u64, _: &[u8]) -> Result<()> {
                panic!("not called")
            }
            fn truncate_from(&mut self, _: u64) -> Result<()> {
                panic!("not called")
            }
            fn drop_below(&mut self, _: u64) {
                panic!("not called")
            }
            fn retained_start(&self) -> u64 {
                0
            }
            fn read(&self, _: u64, _: u64) -> Result<Vec<u8>> {
                panic!("read before admission")
            }
        }
        let state = Arc::new(tokio::sync::Mutex::new(Acceptor::new(
            PersistentState::new(1),
            MemControl::new(),
            NoReadWal(MemWal::new()),
        )));
        let admission = admission();
        let error = run_admitted(
            state.clone(),
            Message::Read {
                from_lsn: 0,
                to_lsn: u64::MAX,
            },
            admission.clone(),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("MAX_READ_BYTES"));
        let output = reserve(
            &admission.output,
            admission.config.response_bytes,
            "test output",
        )
        .unwrap();
        let error = run_admitted(
            state.clone(),
            Message::Read {
                from_lsn: 0,
                to_lsn: 1,
            },
            admission.clone(),
        )
        .await
        .unwrap_err();
        assert!(error.to_string().contains("response bytes"));
        assert!(state.lock().await.wedged.is_none());
        assert_eq!(
            admission.input.available_permits(),
            admission.config.request_bytes
        );
        assert_eq!(
            admission.requests.available_permits(),
            admission.config.max_requests
        );
        drop(output);
    }

    #[tokio::test(flavor = "current_thread")]
    async fn blocking_persistence_survives_cancellation_without_blocking_async_progress() {
        struct PausedControl {
            entered: Option<tokio::sync::oneshot::Sender<()>>,
            release: std::sync::mpsc::Receiver<()>,
            durable_term: Arc<std::sync::atomic::AtomicU64>,
        }
        impl ControlStore for PausedControl {
            fn persist(&mut self, state: &PersistentState) -> Result<()> {
                if let Some(entered) = self.entered.take() {
                    let _ = entered.send(());
                    self.release.recv_timeout(Duration::from_secs(3)).unwrap();
                }
                self.durable_term
                    .store(state.term, std::sync::atomic::Ordering::SeqCst);
                Ok(())
            }
        }
        let (entered, seen) = tokio::sync::oneshot::channel();
        let (release, wait) = std::sync::mpsc::channel();
        let durable = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let state = Arc::new(tokio::sync::Mutex::new(Acceptor::new(
            PersistentState::new(1),
            PausedControl {
                entered: Some(entered),
                release: wait,
                durable_term: durable.clone(),
            },
            MemWal::new(),
        )));
        let admission = admission();
        let task = tokio::spawn(run_admitted(
            state.clone(),
            Message::VoteRequest { term: 7 },
            admission.clone(),
        ));
        tokio::time::timeout(Duration::from_secs(1), seen)
            .await
            .unwrap()
            .unwrap();
        // This runtime has only one async worker. The timer can advance while
        // the disk operation is deliberately blocked on another thread.
        tokio::time::sleep(Duration::from_millis(10)).await;
        assert!(!task.is_finished(), "responded before persistence finished");
        task.abort();
        assert!(task.await.unwrap_err().is_cancelled());
        assert!(
            state.try_lock().is_err(),
            "cancellation released the live state guard"
        );
        assert_eq!(
            admission.requests.available_permits(),
            admission.config.max_requests - 1
        );
        release.send(()).unwrap();
        let state_guard = tokio::time::timeout(Duration::from_secs(1), state.lock())
            .await
            .unwrap();
        assert_eq!(state_guard.state.term, 7);
        assert_eq!(durable.load(std::sync::atomic::Ordering::SeqCst), 7);
        drop(state_guard);
        assert_eq!(
            admission.input.available_permits(),
            admission.config.request_bytes
        );
        assert_eq!(
            admission.requests.available_permits(),
            admission.config.max_requests
        );
        tokio::time::timeout(Duration::from_secs(1), async {
            while admission.output.available_permits() != admission.config.response_bytes {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        let (response, _) = run_admitted(state, Message::VoteRequest { term: 7 }, admission)
            .await
            .unwrap();
        assert!(matches!(
            response,
            Message::VoteResponse { granted: false, .. }
        ));
    }

    #[tokio::test]
    async fn blocking_worker_panic_wedges_state_and_releases_admission() {
        struct PanicControl;
        impl ControlStore for PanicControl {
            fn persist(&mut self, _: &PersistentState) -> Result<()> {
                panic!("injected persistence panic")
            }
        }
        let state = Arc::new(tokio::sync::Mutex::new(Acceptor::new(
            PersistentState::new(1),
            PanicControl,
            MemWal::new(),
        )));
        let admission = admission();
        let result = run_admitted(
            state.clone(),
            Message::VoteRequest { term: 1 },
            admission.clone(),
        )
        .await;
        assert!(result.unwrap_err().to_string().contains("panicked"));
        assert!(state.lock().await.wedged.is_some());
        assert_eq!(
            admission.input.available_permits(),
            admission.config.request_bytes
        );
        assert_eq!(
            admission.requests.available_permits(),
            admission.config.max_requests
        );
        assert_eq!(
            admission.output.available_permits(),
            admission.config.response_bytes
        );
        let (response, _) = run_admitted(state, Message::VoteRequest { term: 2 }, admission)
            .await
            .unwrap();
        assert!(matches!(response, Message::Error { message } if message.contains("wedged")));
    }

    #[tokio::test]
    async fn partial_frame_timeout_releases_byte_and_request_permits() {
        use tokio::io::AsyncWriteExt as _;
        let dir = ScopedTestDir(temp_dir("admission"));
        let state = Arc::new(tokio::sync::Mutex::new(open_dir(dir.path(), 1).unwrap()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut client = tokio::net::TcpStream::connect(listener.local_addr().unwrap())
            .await
            .unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let admission = Arc::new(
            Admission::new(ServeConfig {
                io_timeout: Duration::from_millis(50),
                ..ServeConfig::default()
            })
            .unwrap(),
        );
        let task = tokio::spawn(handle_conn(server, state, admission.clone()));
        let wire = Message::Greeting { tail_id: None }.encode().unwrap();
        client.write_all(&wire[..12]).await.unwrap();
        let error = tokio::time::timeout(Duration::from_secs(1), task)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert!(error.to_string().contains("timed out"));
        assert_eq!(
            admission.input.available_permits(),
            admission.config.request_bytes
        );
        assert_eq!(
            admission.requests.available_permits(),
            admission.config.max_requests
        );
    }

    #[tokio::test]
    async fn connection_limit_backpressures_handlers_until_prior_socket_closes() {
        use tokio::io::AsyncWriteExt as _;
        let dir = ScopedTestDir(temp_dir("admission"));
        let state = Arc::new(tokio::sync::Mutex::new(open_dir(dir.path(), 1).unwrap()));
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(serve_with_config(
            listener,
            state,
            ServeConfig {
                max_connections: 1,
                ..ServeConfig::default()
            },
        ));
        let mut first = tokio::net::TcpStream::connect(address).await.unwrap();
        write_message(&mut first, &Message::Greeting { tail_id: None })
            .await
            .unwrap();
        super::super::proto::read_message(&mut first).await.unwrap();
        let mut second = tokio::net::TcpStream::connect(address).await.unwrap();
        second
            .write_all(&Message::Greeting { tail_id: None }.encode().unwrap())
            .await
            .unwrap();
        assert!(
            tokio::time::timeout(Duration::from_millis(30), second.readable())
                .await
                .is_err()
        );
        drop(first);
        let response = tokio::time::timeout(
            Duration::from_secs(1),
            super::super::proto::read_message(&mut second),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(matches!(response, Message::GreetingResp { .. }));
        server.abort();
        let _ = server.await;
    }

    /// In-memory control store; remembers the last persisted state so a
    /// test can "reboot" the acceptor from durable truth.
    struct MemControl {
        persisted: Option<PersistentState>,
        fail: bool,
    }

    impl MemControl {
        fn new() -> Self {
            MemControl {
                persisted: None,
                fail: false,
            }
        }
    }

    impl ControlStore for MemControl {
        fn persist(&mut self, state: &PersistentState) -> Result<()> {
            if self.fail {
                bail!("injected control persist failure");
            }
            self.persisted = Some(state.clone());
            Ok(())
        }
    }

    /// In-memory wal: a byte buffer with a local start.
    struct MemWal {
        log: Option<(u64, Vec<u8>)>,
    }

    impl MemWal {
        fn new() -> Self {
            MemWal { log: None }
        }
    }

    impl WalStore for MemWal {
        fn flush_lsn(&self) -> u64 {
            self.log
                .as_ref()
                .map(|(s, b)| s + b.len() as u64)
                .unwrap_or(0)
        }
        fn is_empty(&self) -> bool {
            self.log.is_none()
        }
        fn append(&mut self, begin_lsn: u64, frames: &[u8]) -> Result<()> {
            match &mut self.log {
                None => self.log = Some((begin_lsn, frames.to_vec())),
                Some((s, b)) => {
                    assert_eq!(*s + b.len() as u64, begin_lsn);
                    b.extend_from_slice(frames);
                }
            }
            Ok(())
        }
        fn truncate_from(&mut self, lsn: u64) -> Result<()> {
            if let Some((s, b)) = &mut self.log {
                if lsn < *s + b.len() as u64 {
                    assert!(lsn >= *s, "truncation below the local start");
                    b.truncate((lsn - *s) as usize);
                }
            }
            Ok(())
        }
        fn drop_below(&mut self, _horizon: u64) {}
        fn retained_start(&self) -> u64 {
            self.log.as_ref().map(|(s, _)| *s).unwrap_or(0)
        }
        fn read(&self, from: u64, to: u64) -> Result<Vec<u8>> {
            let (s, b) = self.log.as_ref().ok_or_else(|| anyhow!("empty wal"))?;
            if from < *s || to > *s + b.len() as u64 {
                bail!("range not retained");
            }
            Ok(b[(from - s) as usize..(to - s) as usize].to_vec())
        }
    }

    fn mem_acceptor() -> Acceptor<MemControl, MemWal> {
        Acceptor::new(PersistentState::new(1), MemControl::new(), MemWal::new())
    }

    fn th(entries: &[(u64, u64)]) -> TermHistory {
        TermHistory(
            entries
                .iter()
                .map(|&(term, lsn)| TermLsn { term, lsn })
                .collect(),
        )
    }

    /// Framed record bytes for a stream of (key, seq, body) starting at
    /// `start`; returns (bytes, end_lsn).
    fn records(start: u64, items: &[(&str, u64, &[u8])]) -> (Vec<u8>, u64) {
        let mut out = Vec::new();
        let mut pos = start;
        for (key, seq, body) in items {
            let rec = Record {
                lsn: pos,
                kind: RECORD_FRAME,
                table_key: key.to_string(),
                seq: *seq,
                body: body.to_vec(),
            };
            let frame = rec.encode().unwrap();
            pos += frame.len() as u64;
            out.extend_from_slice(&frame);
        }
        (out, pos)
    }

    fn vote(a: &mut Acceptor<MemControl, MemWal>, term: u64) -> Message {
        a.process(Message::VoteRequest { term })
    }

    fn elect(
        a: &mut Acceptor<MemControl, MemWal>,
        term: u64,
        start: u64,
        h: TermHistory,
    ) -> Message {
        a.process(Message::Elected {
            term,
            start_lsn: start,
            term_history: h,
        })
    }

    fn append(
        a: &mut Acceptor<MemControl, MemWal>,
        term: u64,
        begin: u64,
        items: &[(&str, u64, &[u8])],
    ) -> Message {
        let (bytes, end) = records(begin, items);
        a.process(Message::Append {
            term,
            begin_lsn: begin,
            end_lsn: end,
            commit_lsn: 0,
            horizon_lsn: 0,
            records: bytes,
        })
    }

    // Vote for term 1 granted; after a simulated reboot (rebuild from the
    // PERSISTED state) the SAME vote is refused — the persistence of the
    // vote is what's under test (neon test_voting).
    #[test]
    fn voting_survives_reboot() {
        let mut a = mem_acceptor();
        let resp = vote(&mut a, 1);
        assert!(matches!(
            resp,
            Message::VoteResponse {
                granted: true,
                term: 1,
                ..
            }
        ));
        // Same term again: refused (term did not move).
        let resp = vote(&mut a, 1);
        assert!(matches!(
            resp,
            Message::VoteResponse {
                granted: false,
                term: 1,
                ..
            }
        ));
        // Reboot from durable truth.
        let persisted = a.ctrl.persisted.clone().unwrap();
        let mut a2 = Acceptor::new(persisted, MemControl::new(), MemWal::new());
        let resp = vote(&mut a2, 1);
        assert!(matches!(
            resp,
            Message::VoteResponse {
                granted: false,
                term: 1,
                ..
            }
        ));
        let resp = vote(&mut a2, 2);
        assert!(matches!(
            resp,
            Message::VoteResponse {
                granted: true,
                term: 2,
                ..
            }
        ));
    }

    // A vote whose persist fails must NOT be granted (persist-before-
    // respond), and the acceptor wedges.
    #[test]
    fn vote_persist_failure_is_not_granted() {
        let mut a = mem_acceptor();
        a.ctrl.fail = true;
        let resp = vote(&mut a, 1);
        assert!(matches!(resp, Message::Error { .. }));
        a.ctrl.fail = false;
        // Wedged: even a healthy retry is refused until restart.
        let resp = vote(&mut a, 1);
        assert!(matches!(resp, Message::Error { .. }));
    }

    // Greeting adopts the tail id permanently; a different id is refused.
    #[test]
    fn greeting_adopts_tail_id_once() {
        let mut a = mem_acceptor();
        let resp = a.process(Message::Greeting { tail_id: None });
        assert!(matches!(resp, Message::GreetingResp { tail_id: None, .. }));
        let resp = a.process(Message::Greeting {
            tail_id: Some("t-1".into()),
        });
        assert!(matches!(&resp, Message::GreetingResp { tail_id: Some(id), .. } if id == "t-1"));
        // Adoption was persisted BEFORE the response.
        assert_eq!(
            a.ctrl.persisted.as_ref().unwrap().tail_id.as_deref(),
            Some("t-1")
        );
        let resp = a.process(Message::Greeting {
            tail_id: Some("t-2".into()),
        });
        assert!(matches!(resp, Message::Error { .. }));
    }

    // ProposerElected + appends: last_log_term flips only when the flush
    // position reaches the new term's start (neon test_last_log_term_switch).
    #[test]
    fn last_log_term_switches_on_flush() {
        let mut a = mem_acceptor();
        assert!(matches!(
            vote(&mut a, 1),
            Message::VoteResponse { granted: true, .. }
        ));
        assert!(matches!(
            elect(&mut a, 1, 0, th(&[(1, 0)])),
            Message::ElectedResp { ok: true, .. }
        ));
        let resp = append(&mut a, 1, 0, &[("demo.t", 1, b"one")]);
        let Message::AppendResp {
            ok: true,
            flush_lsn,
            ..
        } = resp
        else {
            panic!("append refused: {resp:?}");
        };
        // New proposer, term 2 starting beyond our flush.
        let start2 = flush_lsn + 100;
        assert!(matches!(
            vote(&mut a, 2),
            Message::VoteResponse { granted: true, .. }
        ));
        assert!(matches!(
            elect(&mut a, 2, flush_lsn, th(&[(1, 0), (2, start2)])),
            Message::ElectedResp { ok: true, .. }
        ));
        // History adopted in full, beyond the local log: still in term 1.
        assert_eq!(a.state.term_history.last_log_term(a.wal.flush_lsn()), 1);
        // Stream records up to and past the term-2 start.
        let mut pos = flush_lsn;
        while pos < start2 {
            let resp = append(&mut a, 2, pos, &[("demo.t", 2, b"fill")]);
            let Message::AppendResp {
                ok: true,
                flush_lsn,
                ..
            } = resp
            else {
                panic!("fill append refused: {resp:?}");
            };
            pos = flush_lsn;
        }
        assert_eq!(a.state.term_history.last_log_term(a.wal.flush_lsn()), 2);
    }

    // Appends are only legal in the exact elected term: a LOWER request
    // term gets the term-only fence, a HIGHER one an error (no elected).
    #[test]
    fn stale_term_append_is_fenced() {
        let mut a = mem_acceptor();
        vote(&mut a, 1);
        elect(&mut a, 1, 0, th(&[(1, 0)]));
        append(&mut a, 1, 0, &[("demo.t", 1, b"x")]);
        // A newer proposer bumps the term via its vote.
        vote(&mut a, 5);
        let flush = a.wal.flush_lsn();
        let resp = append(&mut a, 1, flush, &[("demo.t", 2, b"y")]);
        assert!(
            matches!(
                resp,
                Message::AppendResp {
                    ok: false,
                    term: 5,
                    flush_lsn: 0,
                    commit_lsn: 0
                }
            ),
            "stale append must get the term-only fence, got {resp:?}"
        );
        // Higher-than-ours term without ProposerElected: an error.
        let resp = append(&mut a, 9, flush, &[("demo.t", 2, b"y")]);
        assert!(matches!(resp, Message::Error { .. }));
    }

    // Gap and rewrite appends are refused (repositioning is only ever done
    // by ProposerElected) — neon test_non_consecutive_write.
    #[test]
    fn non_consecutive_append_is_refused() {
        let mut a = mem_acceptor();
        vote(&mut a, 1);
        elect(&mut a, 1, 0, th(&[(1, 0)]));
        let resp = append(&mut a, 1, 0, &[("demo.t", 1, b"x")]);
        let Message::AppendResp { flush_lsn, .. } = resp else {
            panic!()
        };
        // Gap.
        let resp = append(&mut a, 1, flush_lsn + 8, &[("demo.t", 2, b"y")]);
        assert!(matches!(resp, Message::Error { .. }));
        // Rewrite.
        let resp = append(&mut a, 1, 0, &[("demo.t", 2, b"y")]);
        assert!(matches!(resp, Message::Error { .. }));
    }

    // handle_elected truncates the divergent suffix and adopts the
    // proposer's history; a start below the committed position is refused.
    #[test]
    fn elected_truncates_divergence_and_guards_commit() {
        let mut a = mem_acceptor();
        vote(&mut a, 1);
        elect(&mut a, 1, 0, th(&[(1, 0)]));
        let r1 = append(&mut a, 1, 0, &[("demo.t", 1, b"committed")]);
        let Message::AppendResp {
            flush_lsn: committed_end,
            ..
        } = r1
        else {
            panic!()
        };
        // Mark the first record committed via the piggybacked commit_lsn.
        let (bytes, end) = records(committed_end, &[("demo.t", 2, b"uncommitted")]);
        let resp = a.process(Message::Append {
            term: 1,
            begin_lsn: committed_end,
            end_lsn: end,
            commit_lsn: committed_end,
            horizon_lsn: 0,
            records: bytes,
        });
        assert!(matches!(resp, Message::AppendResp { ok: true, .. }));
        assert_eq!(a.state.commit_lsn, committed_end);
        // New proposer whose history says term 1 ended at committed_end:
        // our uncommitted suffix is truncated away.
        vote(&mut a, 2);
        let resp = elect(&mut a, 2, committed_end, th(&[(1, 0), (2, committed_end)]));
        assert!(
            matches!(resp, Message::ElectedResp { ok: true, .. }),
            "{resp:?}"
        );
        assert_eq!(a.wal.flush_lsn(), committed_end);
        assert_eq!(a.state.term_history, th(&[(1, 0), (2, committed_end)]));
        // A start below the committed position is refused outright.
        vote(&mut a, 3);
        let resp = elect(&mut a, 3, 0, th(&[(3, 0)]));
        assert!(matches!(resp, Message::Error { .. }));
    }

    // A stale proposer's Elected gets ok=false with the higher term.
    #[test]
    fn stale_elected_is_refused_with_term() {
        let mut a = mem_acceptor();
        vote(&mut a, 5);
        let resp = elect(&mut a, 2, 0, th(&[(2, 0)]));
        assert!(matches!(resp, Message::ElectedResp { term: 5, ok: false }));
    }

    // An elected start_lsn that does not match the divergence point we
    // compute is a (transient) error, not an ack.
    #[test]
    fn elected_start_mismatch_errors() {
        let mut a = mem_acceptor();
        vote(&mut a, 1);
        elect(&mut a, 1, 0, th(&[(1, 0)]));
        append(&mut a, 1, 0, &[("demo.t", 1, b"x")]);
        let flush = a.wal.flush_lsn();
        vote(&mut a, 2);
        // Correct divergence point is `flush`; claim something else.
        let resp = elect(&mut a, 2, flush + 4, th(&[(1, 0), (2, flush + 4)]));
        assert!(matches!(resp, Message::Error { .. }));
    }

    // A fresh acceptor (no history, empty log) adopts the proposer's start
    // as its local log start — the node-join path.
    #[test]
    fn fresh_acceptor_adopts_local_start() {
        let mut a = mem_acceptor();
        vote(&mut a, 3);
        let resp = elect(&mut a, 3, 500, th(&[(1, 0), (3, 700)]));
        assert!(
            matches!(resp, Message::ElectedResp { ok: true, .. }),
            "{resp:?}"
        );
        let resp = append(&mut a, 3, 500, &[("demo.t", 1, b"joined")]);
        assert!(
            matches!(resp, Message::AppendResp { ok: true, .. }),
            "{resp:?}"
        );
        assert!(a.wal.flush_lsn() > 500);
    }

    // commit_lsn never exceeds the local flush, and is monotonic.
    #[test]
    fn commit_lsn_is_clamped_and_monotonic() {
        let mut a = mem_acceptor();
        vote(&mut a, 1);
        elect(&mut a, 1, 0, th(&[(1, 0)]));
        let (bytes, end) = records(0, &[("demo.t", 1, b"x")]);
        let resp = a.process(Message::Append {
            term: 1,
            begin_lsn: 0,
            end_lsn: end,
            commit_lsn: end + 1_000_000, // beyond what we hold
            horizon_lsn: 0,
            records: bytes,
        });
        let Message::AppendResp {
            commit_lsn,
            flush_lsn,
            ..
        } = resp
        else {
            panic!()
        };
        assert_eq!(commit_lsn, flush_lsn);
        // A LOWER piggybacked commit never regresses ours.
        let (bytes, end2) = records(end, &[("demo.t", 2, b"y")]);
        let resp = a.process(Message::Append {
            term: 1,
            begin_lsn: end,
            end_lsn: end2,
            commit_lsn: 0,
            horizon_lsn: 0,
            records: bytes,
        });
        let Message::AppendResp { commit_lsn: c2, .. } = resp else {
            panic!()
        };
        assert_eq!(c2, commit_lsn);
    }

    // Control-file JSON roundtrip with all fields non-default (the shape of
    // neon's test_sk_state_bincode_serde_roundtrip).
    #[test]
    fn control_state_json_roundtrip() {
        let state = PersistentState {
            tail_id: Some("abc".into()),
            node_id: 7,
            term: 42,
            term_history: th(&[(1, 0), (40, 999)]),
            commit_lsn: 1234,
            horizon_lsn: 55,
        };
        let v = state.to_json();
        assert_eq!(PersistentState::from_json(&v).unwrap(), state);
    }

    // ---------- SegmentWal file tests ----------

    use std::sync::atomic::{AtomicU64, Ordering};
    static TEST_DIR_SEQ: AtomicU64 = AtomicU64::new(0);

    fn temp_dir(name: &str) -> PathBuf {
        let n = TEST_DIR_SEQ.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!(
            "icegres-quorum-wal-test-{}-{}-{}",
            std::process::id(),
            name,
            n
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn segment_wal_roundtrip_and_reopen() {
        let dir = temp_dir("roundtrip");
        let mut wal = SegmentWal::open(&dir).unwrap();
        assert!(wal.is_empty());
        let (bytes, end) = records(0, &[("demo.t", 1, b"one"), ("demo.t", 2, b"two")]);
        wal.append(0, &bytes).unwrap();
        assert_eq!(wal.flush_lsn(), end);
        assert_eq!(wal.read(0, end).unwrap(), bytes);
        drop(wal);
        // Reopen: flush recomputed by the scan.
        let wal2 = SegmentWal::open(&dir).unwrap();
        assert_eq!(wal2.flush_lsn(), end);
        assert!(!wal2.is_empty());
        assert_eq!(wal2.read(0, end).unwrap(), bytes);
        let recs = decode_records(&wal2.read(0, end).unwrap(), 0).unwrap();
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[1].seq, 2);
    }

    #[test]
    fn segment_wal_truncates_torn_tail_on_open() {
        let dir = temp_dir("torn");
        let mut wal = SegmentWal::open(&dir).unwrap();
        let (b1, e1) = records(0, &[("demo.t", 1, b"keep")]);
        wal.append(0, &b1).unwrap();
        let (b2, _e2) = records(e1, &[("demo.t", 2, b"torn")]);
        wal.append(e1, &b2).unwrap();
        drop(wal);
        // Tear the last frame.
        let seg = dir.join(format!("{:016x}.seg", 0));
        let len = fs::metadata(&seg).unwrap().len();
        let f = OpenOptions::new().write(true).open(&seg).unwrap();
        f.set_len(len - 3).unwrap();
        drop(f);
        let wal2 = SegmentWal::open(&dir).unwrap();
        assert_eq!(wal2.flush_lsn(), e1, "torn record truncated away");
        assert_eq!(wal2.read(0, e1).unwrap(), b1);
    }

    #[test]
    fn segment_wal_truncate_from_and_drop_below() {
        let dir = temp_dir("truncate");
        let mut wal = SegmentWal::open(&dir).unwrap();
        wal.rotate_bytes = 1; // seal after every append
        let (b1, e1) = records(0, &[("demo.t", 1, b"a")]);
        wal.append(0, &b1).unwrap();
        let (b2, e2) = records(e1, &[("demo.t", 2, b"b")]);
        wal.append(e1, &b2).unwrap();
        let (b3, e3) = records(e2, &[("demo.t", 3, b"c")]);
        wal.append(e2, &b3).unwrap();
        assert_eq!(wal.sealed.len(), 3);
        // Divergence truncation drops the suffix.
        wal.truncate_from(e2).unwrap();
        assert_eq!(wal.flush_lsn(), e2);
        assert!(wal.read(e2, e3).is_err());
        // Horizon GC deletes fully covered segments.
        wal.drop_below(e1);
        assert_eq!(wal.sealed.len(), 1);
        assert!(wal.read(0, e1).is_err(), "covered prefix is gone");
        assert_eq!(wal.read(e1, e2).unwrap(), b2);
        drop(wal);
        // Reopen keeps the shape (scan starts at the surviving segment).
        let wal2 = SegmentWal::open(&dir).unwrap();
        assert_eq!(wal2.flush_lsn(), e2);
        assert_eq!(wal2.read(e1, e2).unwrap(), b2);
    }

    #[test]
    fn segment_wal_mid_segment_divergence_truncation() {
        let dir = temp_dir("mid-truncate");
        let mut wal = SegmentWal::open(&dir).unwrap();
        let (b1, e1) = records(0, &[("demo.t", 1, b"keep")]);
        wal.append(0, &b1).unwrap();
        let (b2, e2) = records(e1, &[("demo.t", 2, b"drop")]);
        wal.append(e1, &b2).unwrap();
        wal.truncate_from(e1).unwrap();
        assert_eq!(wal.flush_lsn(), e1);
        // New records append cleanly after the cut, in a fresh segment.
        let (b3, e3) = records(e1, &[("demo.t", 2, b"redo")]);
        wal.append(e1, &b3).unwrap();
        assert_eq!(wal.flush_lsn(), e3);
        drop(wal);
        let wal2 = SegmentWal::open(&dir).unwrap();
        assert_eq!(wal2.flush_lsn(), e3);
        let all = wal2.read(0, e3).unwrap();
        let recs = decode_records(&all, 0).unwrap();
        assert_eq!(recs.len(), 2);
        assert_eq!(recs[1].body, b"redo");
        let _ = e2;
    }

    // FIX (C3): a prefix-GC deletion failure must STOP the walk — the old
    // retain-and-continue left [kept][gap][newer], and the boot contiguity
    // scan then discarded the NEWER (possibly acked) segments.
    #[test]
    fn drop_below_stops_at_the_first_failed_deletion() {
        let dir = temp_dir("gc-stop");
        let mut wal = SegmentWal::open(&dir).unwrap();
        wal.rotate_bytes = 1; // seal after every append
        let (b1, e1) = records(0, &[("demo.t", 1, b"a")]);
        wal.append(0, &b1).unwrap();
        let (b2, e2) = records(e1, &[("demo.t", 2, b"b")]);
        wal.append(e1, &b2).unwrap();
        let (b3, e3) = records(e2, &[("demo.t", 3, b"c")]);
        wal.append(e2, &b3).unwrap();
        assert_eq!(wal.sealed.len(), 3);
        // Make segment 1 undeletable: replace its FILE with a non-empty
        // DIRECTORY of the same name, so remove_file fails. (A read-only
        // segment file would not do it — unlink permission lives on the
        // parent directory, so deleting a chmod 0444 file still succeeds.)
        let seg1 = dir.join(format!("{:016x}.seg", 0));
        fs::remove_file(&seg1).unwrap();
        fs::create_dir(&seg1).unwrap();
        fs::write(seg1.join("occupied"), b"x").unwrap();
        wal.drop_below(e3);
        // The walk stopped at segment 1: segment 2 SURVIVES (no gap), and
        // both stay in the bookkeeping for the next GC round.
        assert!(
            dir.join(format!("{e1:016x}.seg")).exists(),
            "no gap: segment 2 must survive"
        );
        assert_eq!(wal.sealed.len(), 3, "nothing dropped from the bookkeeping");
        assert_eq!(wal.read(e1, e2).unwrap(), b2);
        // Once the blocker clears, the next round deletes 1 and 2 but (C4)
        // keeps the last segment.
        fs::remove_dir_all(&seg1).unwrap();
        let (bytes, _) = records(0, &[("demo.t", 1, b"a")]);
        fs::write(&seg1, &bytes).unwrap();
        wal.drop_below(e3);
        assert_eq!(wal.sealed.len(), 1);
        assert_eq!(wal.sealed[0].start, e2);
        let _ = b1;
        let _ = b3;
    }

    // FIX (C4): even a fully covered log keeps its LAST segment — deleting
    // it made a restart derive flush = 0 and the proposer expelled the
    // acceptor forever (start below the retained base).
    #[test]
    fn drop_below_never_deletes_the_flush_segment() {
        let dir = temp_dir("keep-flush");
        let mut wal = SegmentWal::open(&dir).unwrap();
        wal.rotate_bytes = 1; // seal after every append: no active segment
        let (b1, e1) = records(0, &[("demo.t", 1, b"a")]);
        wal.append(0, &b1).unwrap();
        let (b2, e2) = records(e1, &[("demo.t", 2, b"b")]);
        wal.append(e1, &b2).unwrap();
        assert!(wal.active.is_none());
        // Everything covered: the horizon reached the flush position.
        wal.drop_below(e2);
        assert_eq!(wal.sealed.len(), 1, "the flush segment must survive");
        assert_eq!(wal.sealed[0].start, e1);
        assert_eq!(wal.flush_lsn(), e2);
        assert_eq!(wal.retained_start(), e1);
        drop(wal);
        // Restart keeps the flush position (no expulsion shape).
        let wal2 = SegmentWal::open(&dir).unwrap();
        assert_eq!(wal2.flush_lsn(), e2);
        assert!(!wal2.is_empty());
        let _ = b2;
    }

    // FIX (C2/I1a): the control file learns the horizon BEFORE drop_below
    // deletes anything, so a full-cluster restart never reports a horizon
    // below its own GC line.
    #[test]
    fn horizon_persists_before_segment_gc() {
        let mut a = mem_acceptor();
        vote(&mut a, 1);
        elect(&mut a, 1, 0, th(&[(1, 0)]));
        let (bytes, end) = records(0, &[("demo.t", 1, b"x")]);
        let resp = a.process(Message::Append {
            term: 1,
            begin_lsn: 0,
            end_lsn: end,
            commit_lsn: end,
            horizon_lsn: end,
            records: bytes,
        });
        assert!(matches!(resp, Message::AppendResp { ok: true, .. }));
        let persisted = a.ctrl.persisted.as_ref().expect("persisted at least once");
        assert_eq!(
            persisted.horizon_lsn, end,
            "the horizon advance must hit the control file synchronously"
        );
    }

    // FIX (C2/I1b): a vote reports the EFFECTIVE horizon — never below the
    // first retained LSN of the wal — so recovery is never pointed into a
    // GC'd range by a stale persisted horizon.
    #[test]
    fn vote_reports_effective_horizon_over_stale_control_state() {
        let mut a = mem_acceptor();
        vote(&mut a, 1);
        elect(&mut a, 1, 0, th(&[(1, 0)]));
        append(&mut a, 1, 0, &[("demo.t", 1, b"x")]);
        let flush = a.wal.flush_lsn();
        // Simulate the pre-fix damage: segments below `flush` are gone
        // (MemWal models it as a raised local start) while the persisted
        // horizon still says 0.
        a.wal.log = Some((flush, Vec::new()));
        assert_eq!(a.state.horizon_lsn, 0);
        let resp = vote(&mut a, 2);
        let Message::VoteResponse { horizon_lsn, .. } = resp else {
            panic!("vote response expected");
        };
        assert_eq!(
            horizon_lsn, flush,
            "the reported horizon must cover the GC'd (unretained) prefix"
        );
    }
}
