//! The quorum-tail proposer: election, donor selection, recovery,
//! per-acceptor catch-up streaming, quorum commit tracking, and the
//! per-table horizon bookkeeping the `TailStore` surface maps onto.
//!
//! Adapted from neondatabase/neon safekeeper (Apache-2.0); substantially
//! modified for icegres's generic tail log. The election/donor/commit
//! logic follows `neon/pgxn/neon/walproposer.c` (the C proposer):
//!
//! * term = max(term seen in greetings) + 1; a vote quorum of 2/3 wins;
//! * the donor is the most advanced voter by `(last_log_term, flush_lsn)`
//!   — Raft's up-to-date ordering, applied by the PROPOSER after election
//!   instead of by voters (walproposer.c:1139-1148);
//! * recovery downloads `[max(voter horizons), donor flush)` from the
//!   donor and treats it all as committed ("the voting quorum may differ
//!   from the quorum that committed the last record — treat max(flush) as
//!   committed or lose acked data", neon docs);
//! * each acceptor gets an INDIVIDUAL start position mirroring
//!   `find_highest_common_point` (walproposer.c SendProposerElected), its
//!   divergent suffix truncated by the acceptor, then the proposer streams
//!   the recovered suffix + new records until all converge;
//! * a record commits when the 2nd-highest acked flush covers it, with
//!   flushes below the new term's start zeroed out ("like in Raft, we
//!   aren't allowed to commit entries from previous terms",
//!   walproposer.c:2010-2012);
//! * a response carrying a HIGHER term fences this proposer — but, like
//!   Neon's walproposer, it first attempts ONE internal RE-ELECTION (a
//!   fresh term above everything seen): a crashed competitor that merely
//!   bumped terms with votes must not brick the tail. Only when the
//!   re-election itself shows a live competitor (a vote refused, or the
//!   donor holding records from a term we never owned) — or fails twice
//!   consecutively — does the tail poison itself with "superseded by a
//!   newer server", and every later append fails.
//!
//! # Retention and the horizon
//!
//! The proposer retains the log suffix `[base, head)` in memory — the
//! resend queue for lagging acceptors. The TailStore's per-table
//! truncation maps onto it: `note_covered` records each table's covered
//! sequence, and the horizon (what acceptors may delete) advances over the
//! longest prefix in which every Frame record is covered AND superseded by
//! a RETAINED Watermark record for its table (a frame may never leave the
//! log while no watermark record at/above its sequence remains — that
//! watermark is the table's replay sidecar, and dropping a table's last
//! trace would un-floor its sequences after a restart), and every
//! Watermark record is superseded by a later one for the same table. The
//! latest watermark per table is therefore always retained. A dormant
//! table's last watermark would pin the horizon forever, so a blocked
//! watermark more than [`WATERMARK_REFRESH_SLACK`] bytes behind the head
//! is re-appended fresh (the caller performs the refresh append),
//! unpinning the prefix. An acceptor that falls more than
//! [`MAX_PEER_LAG_BYTES`] behind is dropped from catch-up (it rejoins at
//! the next election — internal re-elections included) so one dead node
//! cannot grow the queue unboundedly.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex as StdMutex};
use std::time::Duration;

use anyhow::{anyhow, bail, Context as _, Result};
use tokio::sync::{watch, Notify};

use super::proto::{
    decode_records, CallTimedOut, Conn, Message, Record, TermHistory, TermLsn, MAX_MESSAGE_BYTES,
    RECORD_FRAME, RECORD_WATERMARK, WRONG_CLUSTER_MARK,
};

/// Static membership: exactly 3 acceptors, quorum 2.
pub(crate) const QUORUM_SIZE: usize = 3;
pub(crate) const QUORUM: usize = 2;

/// Bytes of records per AppendRequest batch.
const MAX_APPEND_BATCH_BYTES: usize = 1 << 20;

/// Headroom reserved inside [`MAX_MESSAGE_BYTES`] for an Append message's
/// JSON header + length/crc framing when capping record and batch sizes
/// (FIX C6): a record admitted into the log must always be encodable into
/// ONE wire message, or every batch containing it wedges into a reconnect
/// loop until the timeout poisons the tail.
const APPEND_MESSAGE_HEADROOM: usize = 4096;

/// A horizon-blocking watermark record this far behind the head gets
/// re-appended fresh so the prefix can be released.
const WATERMARK_REFRESH_SLACK: u64 = 1 << 20;

/// An acceptor lagging this many bytes behind the horizon is dropped from
/// catch-up until the next election (bounds the in-memory resend queue).
const MAX_PEER_LAG_BYTES: u64 = 256 << 20;

/// Consecutive failed internal re-elections before the tail poisons (FIX
/// C5): one transient failure (an acceptor mid-restart) gets a second
/// chance on the next fencing/stall event; a genuine outage still fails
/// fast.
const MAX_REELECTION_FAILURES: u32 = 2;

/// Env var overriding the append (quorum-ack) timeout in milliseconds
/// (FIX I3). Default [`DEFAULT_APPEND_TIMEOUT_MS`], floor
/// [`MIN_APPEND_TIMEOUT_MS`].
pub(crate) const TAIL_QUORUM_TIMEOUT_ENV: &str = "ICEGRES_TAIL_QUORUM_TIMEOUT_MS";
const DEFAULT_APPEND_TIMEOUT_MS: u64 = 10_000;
const MIN_APPEND_TIMEOUT_MS: u64 = 1_000;

/// Parse the [`TAIL_QUORUM_TIMEOUT_ENV`] override: unset/empty = the
/// default; unparseable = WARN + the default (never a silent zero);
/// anything below the floor is clamped up (a sub-second quorum timeout
/// would poison the tail on any fsync hiccup).
pub(crate) fn append_timeout_from_env(raw: Option<&str>) -> Duration {
    let ms = match raw.map(str::trim) {
        None | Some("") => DEFAULT_APPEND_TIMEOUT_MS,
        Some(s) => match s.parse::<u64>() {
            Ok(ms) if ms < MIN_APPEND_TIMEOUT_MS => {
                tracing::warn!(
                    "{TAIL_QUORUM_TIMEOUT_ENV}={ms} is below the {MIN_APPEND_TIMEOUT_MS} ms \
                     floor; clamping up"
                );
                MIN_APPEND_TIMEOUT_MS
            }
            Ok(ms) => ms,
            Err(_) => {
                tracing::warn!(
                    "{TAIL_QUORUM_TIMEOUT_ENV}={s:?} is not a millisecond count; using the \
                     {DEFAULT_APPEND_TIMEOUT_MS} ms default"
                );
                DEFAULT_APPEND_TIMEOUT_MS
            }
        },
    };
    Duration::from_millis(ms)
}

#[derive(Clone)]
pub(crate) struct QuorumConfig {
    /// Exactly [`QUORUM_SIZE`] `host:port` acceptor addresses.
    pub addrs: Vec<String>,
    /// How long an append may wait for a quorum of flush acks before the
    /// tail tries ONE internal re-election, and — should the wait time out
    /// again — POISONS itself (the record may still commit later — restart
    /// re-elects and replays it; never reuse its sequence in-process).
    /// Overridable via [`TAIL_QUORUM_TIMEOUT_ENV`].
    pub append_timeout: Duration,
    pub connect_timeout: Duration,
    /// Bound on every open()/handshake request-response round trip (FIX
    /// I2): a connected-but-silent acceptor is treated as unavailable
    /// instead of hanging the sequential election calls forever.
    pub call_timeout: Duration,
    /// Bytes per donor Read chunk during open()'s recovery download (FIX
    /// H1): one giant Read of the whole `[truncate_lsn, term_start)` range
    /// could exceed [`MAX_MESSAGE_BYTES`] (reachable —
    /// [`MAX_PEER_LAG_BYTES`] equals the wire cap) and would brick every
    /// reopen. Default [`MAX_APPEND_BATCH_BYTES`]; overridden small in
    /// tests to exercise the chunk loop.
    pub recovery_read_chunk: u64,
}

impl QuorumConfig {
    pub fn new(addrs: Vec<String>) -> Self {
        QuorumConfig {
            addrs,
            append_timeout: append_timeout_from_env(
                std::env::var(TAIL_QUORUM_TIMEOUT_ENV).ok().as_deref(),
            ),
            connect_timeout: Duration::from_secs(5),
            call_timeout: Duration::from_secs(5),
            recovery_read_chunk: MAX_APPEND_BATCH_BYTES as u64,
        }
    }
}

/// One retained record's bookkeeping (the frame bytes are the wire/disk
/// encoding, resent verbatim).
struct RecordMeta {
    start: u64,
    end: u64,
    kind: u8,
    table_key: String,
    seq: u64,
    frame: Vec<u8>,
}

/// The retained log suffix + the per-table coverage bookkeeping.
struct LogBuf {
    /// First retained byte (records below are dropped locally).
    base: u64,
    /// End of the log (next record's start).
    head: u64,
    records: VecDeque<RecordMeta>,
    /// Per-table highest covered sequence (TailStore::truncate calls).
    covered: HashMap<String, u64>,
    /// Per-table `(start_lsn, seq)` of the LATEST watermark record in the
    /// log — presence (not just coverage) gates frame GC (FIX C1).
    last_wm: HashMap<String, (u64, u64)>,
}

/// One acceptor's proposer-side state.
struct Peer {
    addr: String,
    /// Its last acked durable flush position IN THE CURRENT TERM (zeroed
    /// on an internal re-election: only new-term acks may feed the commit
    /// rule; the handshake re-seeds it from the acceptor's real position).
    flush: AtomicU64,
    /// Out until the next election — internal re-elections clear it (FIX
    /// C4: expulsion is sticky within a term, never across elections).
    failed: AtomicBool,
    /// Wakes its streaming task when new records arrive.
    notify: Notify,
}

/// The election-scoped consensus state. Immutable per term; replaced
/// wholesale (under the one mutex) by an internal re-election.
#[derive(Clone)]
struct Election {
    term: u64,
    term_start_lsn: u64,
    history: TermHistory,
}

/// Why the supervisor should attempt an internal re-election (FIX C5).
enum ReelectReason {
    /// An acceptor reported this higher term (reconnect fencing or an
    /// AppendResp fence).
    Fenced(u64),
    /// `wait_commit` spent a full `append_timeout` stuck below this
    /// position.
    Stalled(u64),
}

struct Shared {
    cfg: QuorumConfig,
    tail_id: String,
    election: StdMutex<Election>,
    /// Carries the current term; bumped by internal re-elections. Peer
    /// tasks park on it while fenced/failed.
    term_tx: watch::Sender<u64>,
    log: StdMutex<LogBuf>,
    peers: Vec<Peer>,
    commit_tx: watch::Sender<u64>,
    commit: AtomicU64,
    horizon: AtomicU64,
    poison: StdMutex<Option<String>>,
    reelect_tx: tokio::sync::mpsc::UnboundedSender<ReelectReason>,
}

impl Shared {
    fn poisoned(&self) -> Option<String> {
        self.poison.lock().expect("poison lock").clone()
    }

    fn poison(&self, why: String) {
        {
            let mut p = self.poison.lock().expect("poison lock");
            if p.is_none() {
                tracing::error!("quorum tail POISONED: {why}");
                *p = Some(why);
            }
        }
        // Wake every commit waiter so pending appends fail promptly, and
        // every parked peer task so it can exit.
        self.commit_tx
            .send_replace(self.commit.load(Ordering::SeqCst));
        let cur = *self.term_tx.borrow();
        self.term_tx.send_replace(cur);
        for p in &self.peers {
            p.notify.notify_one();
        }
    }

    fn election(&self) -> Election {
        self.election.lock().expect("election lock").clone()
    }

    fn term(&self) -> u64 {
        self.election.lock().expect("election lock").term
    }

    /// Record one acceptor's flush ack, provided it belongs to the CURRENT
    /// term's connection — an in-flight ack from before an internal
    /// re-election must never feed the new term's commit rule (the
    /// acceptor behind it has not adopted the new history yet, so its
    /// position cannot witness new-term commits). Returns whether the ack
    /// was accepted.
    fn store_ack(&self, idx: usize, conn_term: u64, flush: u64) -> bool {
        let el = self.election.lock().expect("election lock");
        if el.term != conn_term {
            return false;
        }
        self.peers[idx].flush.store(flush, Ordering::SeqCst);
        self.recompute_commit_at(el.term_start_lsn);
        true
    }

    fn recompute_commit(&self) {
        let term_start = self.election.lock().expect("election lock").term_start_lsn;
        self.recompute_commit_at(term_start);
    }

    fn recompute_commit_at(&self, term_start_lsn: u64) {
        let flushes: Vec<u64> = self
            .peers
            .iter()
            .map(|p| p.flush.load(Ordering::SeqCst))
            .collect();
        let cand = quorum_commit(&flushes, term_start_lsn);
        if cand >= term_start_lsn && cand > self.commit.load(Ordering::SeqCst) {
            self.commit.store(cand, Ordering::SeqCst);
            self.commit_tx.send_replace(cand);
        }
    }

    fn request_reelection(&self, reason: ReelectReason) {
        let _ = self.reelect_tx.send(reason);
    }

    /// The next batch of records for a peer whose acked flush is `from`.
    fn next_batch(&self, from: u64) -> Batch {
        let log = self.log.lock().expect("log lock");
        if from >= log.head {
            return Batch::UpToDate;
        }
        if from < log.base {
            return Batch::Behind;
        }
        let mut bytes: Vec<u8> = Vec::new();
        let mut begin: Option<u64> = None;
        let mut end = from;
        for rec in &log.records {
            if rec.end <= from {
                continue;
            }
            if begin.is_none() {
                // `from` is always a record boundary (acceptors ack whole
                // records), so the first uncovered record starts there.
                debug_assert_eq!(rec.start, from);
                begin = Some(rec.start);
            } else if bytes.len() + rec.frame.len() + APPEND_MESSAGE_HEADROOM > MAX_MESSAGE_BYTES {
                // Never assemble a batch the wire cap cannot carry (FIX
                // C6) — the record goes alone in the next batch.
                break;
            }
            bytes.extend_from_slice(&rec.frame);
            end = rec.end;
            if bytes.len() >= MAX_APPEND_BATCH_BYTES {
                break;
            }
        }
        match begin {
            Some(b) => Batch::Data {
                begin: b,
                end,
                bytes,
            },
            None => Batch::UpToDate,
        }
    }
}

enum Batch {
    Data {
        begin: u64,
        end: u64,
        bytes: Vec<u8>,
    },
    UpToDate,
    Behind,
}

/// The most advanced voter by `(last_log_term, flush_lsn)` — Raft's
/// up-to-date ordering (walproposer.c:1139-1148). Input: `(index,
/// last_log_term, flush_lsn)` per granted vote.
pub(crate) fn pick_donor(candidates: &[(usize, u64, u64)]) -> Option<usize> {
    candidates
        .iter()
        .max_by_key(|(_, llt, flush)| (*llt, *flush))
        .map(|(i, _, _)| *i)
}

/// The quorum-acked position: sort acked flushes, ZERO OUT any below the
/// new term's start ("we aren't allowed to commit entries from previous
/// terms"), take the (n - quorum + 1)-th highest — for n=3, quorum=2 the
/// 2nd highest. (walproposer.c GetAcknowledgedByQuorumWALPosition.)
pub(crate) fn quorum_commit(flushes: &[u64], term_start_lsn: u64) -> u64 {
    let mut vals: Vec<u64> = flushes
        .iter()
        .map(|&f| if f < term_start_lsn { 0 } else { f })
        .collect();
    vals.sort_unstable_by(|a, b| b.cmp(a));
    vals[QUORUM - 1]
}

/// A record admitted into the log must fit ONE wire message with batching
/// headroom to spare (FIX C6); enforce it at the `Quorum::append` boundary
/// so an oversized statement fails cleanly BEFORE entering LogBuf (where it
/// would wedge every batch encode into a reconnect loop until the timeout
/// poisons the whole tail).
pub(crate) fn check_record_frame_len(frame_len: usize) -> Result<()> {
    if frame_len + APPEND_MESSAGE_HEADROOM > MAX_MESSAGE_BYTES {
        bail!(
            "tail record of {frame_len} framed bytes exceeds the quorum message cap \
             ({} bytes with batching headroom); split the statement into smaller \
             inserts",
            MAX_MESSAGE_BYTES - APPEND_MESSAGE_HEADROOM
        );
    }
    Ok(())
}

/// Where to start streaming to one acceptor: the proposer-side mirror of
/// `find_highest_common_point` (walproposer.c SendProposerElected,
/// :1446-1526). `base` = the first byte the proposer retains — a fresh
/// acceptor (empty history, empty log) joins there.
pub(crate) fn compute_start(
    prop_th: &TermHistory,
    sk_th: &TermHistory,
    sk_flush: u64,
    prop_term: u64,
    base: u64,
) -> Result<u64> {
    let mut idx: Option<usize> = None;
    for i in 0..prop_th.0.len().min(sk_th.0.len()) {
        if prop_th.0[i].term != sk_th.0[i].term {
            break;
        }
        if prop_th.0[i].lsn != sk_th.0[i].lsn {
            bail!(
                "term histories disagree on the start of term {}",
                prop_th.0[i].term
            );
        }
        idx = Some(i);
    }
    Ok(match idx {
        None if sk_th.0.is_empty() && sk_flush == 0 => base,
        None => prop_th
            .0
            .first()
            .map(|e| e.lsn)
            .ok_or_else(|| anyhow!("proposer term history is empty"))?,
        Some(i) if prop_th.0[i].term == prop_term => sk_flush,
        Some(i) => {
            let prop_end = prop_th.0[i + 1].lsn;
            let sk_end = if i + 1 < sk_th.0.len() {
                sk_th.0[i + 1].lsn
            } else {
                sk_flush
            };
            prop_end.min(sk_end)
        }
    })
}

/// The coverable prefix of the retained log: walk from the front while
/// every record is covered and committed. A Frame is coverable only when
/// `covered[table] >= seq` AND a Watermark record with seq at/above it is
/// RETAINED in the log (FIX C1: coverage alone must never let the horizon
/// erase a table's last trace — without a retained watermark record the
/// next restart would replay nothing for the table, never apply its
/// sequence floor, and re-mint sequences under the committed property
/// watermark, silently skipping acked rows). A Watermark is coverable when
/// a LATER watermark for its table exists. Returns `(prefix_end, refresh)`
/// where `refresh = Some((table, seq))` when the walk is blocked by a
/// last-watermark record far behind the head (the caller should re-append
/// it fresh).
fn coverable_prefix(log: &LogBuf, commit: u64) -> (u64, Option<(String, u64)>) {
    let mut prefix_end = log.base;
    for rec in &log.records {
        if rec.end > commit {
            break;
        }
        let coverable = match rec.kind {
            RECORD_FRAME => {
                log.covered.get(&rec.table_key).copied().unwrap_or(0) >= rec.seq
                    && log
                        .last_wm
                        .get(&rec.table_key)
                        .is_some_and(|&(_, wm_seq)| wm_seq >= rec.seq)
            }
            RECORD_WATERMARK => log
                .last_wm
                .get(&rec.table_key)
                .is_some_and(|&(start, _)| start > rec.start),
            _ => false,
        };
        if coverable {
            prefix_end = rec.end;
            continue;
        }
        if rec.kind == RECORD_WATERMARK
            && log.head.saturating_sub(rec.start) > WATERMARK_REFRESH_SLACK
        {
            let seq = rec
                .seq
                .max(log.covered.get(&rec.table_key).copied().unwrap_or(0));
            return (prefix_end, Some((rec.table_key.clone(), seq)));
        }
        break;
    }
    (prefix_end, None)
}

/// The live quorum tail: election done, recovery streamed, peers converging.
pub(crate) struct Quorum {
    shared: Arc<Shared>,
    _tasks: Vec<tokio::task::JoinHandle<()>>,
}

impl Quorum {
    /// Connect, adopt/mint the tail identity, run the election, recover the
    /// unfinished suffix from the donor, converge the acceptors, and wait
    /// until the quorum commit position covers the recovered records.
    /// Returns the recovered committed records `[max(horizons), commit)` —
    /// the replay set.
    pub async fn open(cfg: QuorumConfig) -> Result<(Quorum, Vec<Record>)> {
        if cfg.addrs.len() != QUORUM_SIZE {
            bail!(
                "--tail-quorum needs exactly {QUORUM_SIZE} acceptor addresses, got {}",
                cfg.addrs.len()
            );
        }
        // 1. Connect and query (Greeting with no id only reads state).
        // Every round trip is bounded by call_timeout (FIX I2): a
        // connected-but-silent acceptor counts as unreachable instead of
        // hanging this sequential chain forever.
        let mut conns: Vec<Option<Conn>> = Vec::new();
        let mut greets: Vec<Option<(Option<String>, u64, u64)>> = Vec::new();
        for addr in &cfg.addrs {
            let opened = async {
                let mut c = Conn::connect(addr, cfg.connect_timeout).await?;
                match c
                    .call_timeout(&Message::Greeting { tail_id: None }, cfg.call_timeout)
                    .await?
                {
                    Message::GreetingResp {
                        tail_id,
                        term,
                        flush_lsn,
                    } => Ok::<_, anyhow::Error>((c, tail_id, term, flush_lsn)),
                    other => bail!("unexpected greeting response {other:?}"),
                }
            }
            .await;
            match opened {
                Ok((c, tail_id, term, flush)) => {
                    conns.push(Some(c));
                    greets.push(Some((tail_id, term, flush)));
                }
                Err(e) => {
                    tracing::warn!(acceptor = %addr, "acceptor unreachable at open: {e:#}");
                    conns.push(None);
                    greets.push(None);
                }
            }
        }
        let live = conns.iter().flatten().count();
        if live < QUORUM {
            bail!(
                "only {live} of {QUORUM_SIZE} acceptors are reachable; a quorum of \
                 {QUORUM} is required to open the tail"
            );
        }
        // 2. Tail identity: adopt the one the acceptors know, else mint.
        let known: BTreeSet<String> = greets
            .iter()
            .flatten()
            .filter_map(|(id, _, _)| id.clone())
            .collect();
        if known.len() > 1 {
            bail!(
                "the acceptors disagree on the tail identity ({known:?}) — the \
                 --tail-quorum addresses mix different clusters"
            );
        }
        let tail_id = known
            .into_iter()
            .next()
            .unwrap_or_else(|| uuid::Uuid::new_v4().to_string());
        for (i, conn) in conns.iter_mut().enumerate() {
            let Some(c) = conn else { continue };
            match c
                .call_timeout(
                    &Message::Greeting {
                        tail_id: Some(tail_id.clone()),
                    },
                    cfg.call_timeout,
                )
                .await
            {
                Ok(Message::GreetingResp { .. }) => {}
                Ok(other) => bail!("unexpected identity-adoption response {other:?}"),
                Err(e) if e.downcast_ref::<CallTimedOut>().is_some() => {
                    tracing::warn!(
                        acceptor = %cfg.addrs[i],
                        "acceptor went silent during identity adoption; continuing \
                         without it: {e:#}"
                    );
                    *conn = None;
                }
                Err(e) => {
                    // An adoption REFUSAL is a misconfiguration, not a
                    // transient failure — surface it.
                    return Err(e.context(format!(
                        "acceptor {} refused the tail identity {tail_id}",
                        cfg.addrs[i]
                    )));
                }
            }
        }
        // 3. Election: term = max(seen) + 1, votes from a quorum.
        let term = greets
            .iter()
            .flatten()
            .map(|(_, t, _)| *t)
            .max()
            .unwrap_or(0)
            + 1;
        struct Vote {
            flush: u64,
            last_log_term: u64,
            history: TermHistory,
            horizon: u64,
        }
        let mut votes: Vec<Option<Vote>> = (0..QUORUM_SIZE).map(|_| None).collect();
        for (i, conn) in conns.iter_mut().enumerate() {
            let Some(c) = conn else { continue };
            match c
                .call_timeout(&Message::VoteRequest { term }, cfg.call_timeout)
                .await
            {
                Ok(Message::VoteResponse {
                    term: resp_term,
                    granted,
                    flush_lsn,
                    last_log_term,
                    term_history,
                    horizon_lsn,
                    commit_lsn: _,
                }) => {
                    if resp_term > term {
                        bail!(
                            "superseded during election: acceptor {} is already at term \
                             {resp_term} (another icegres owns this quorum tail)",
                            cfg.addrs[i]
                        );
                    }
                    if !granted {
                        bail!(
                            "acceptor {} refused the vote for term {term}: another \
                             proposer is campaigning concurrently; retry",
                            cfg.addrs[i]
                        );
                    }
                    votes[i] = Some(Vote {
                        flush: flush_lsn,
                        last_log_term,
                        history: term_history,
                        horizon: horizon_lsn,
                    });
                }
                Ok(other) => bail!("unexpected vote response {other:?}"),
                Err(e) => {
                    tracing::warn!(acceptor = %cfg.addrs[i], "vote failed: {e:#}");
                    *conn = None;
                }
            }
        }
        let granted = votes.iter().flatten().count();
        if granted < QUORUM {
            bail!("won only {granted} of {QUORUM_SIZE} votes; a quorum of {QUORUM} is required");
        }
        // 4. Donor + recovery range. Each voter reports its EFFECTIVE
        // horizon (>= the first LSN it still retains — FIX C2/I1), so
        // `truncate_lsn = max(horizons)` is always at/above the donor's
        // retained start: the recovery read below can never dip into a
        // GC'd range that a stale persisted horizon failed to mention.
        let candidates: Vec<(usize, u64, u64)> = votes
            .iter()
            .enumerate()
            .filter_map(|(i, v)| v.as_ref().map(|v| (i, v.last_log_term, v.flush)))
            .collect();
        let donor = pick_donor(&candidates).expect("granted >= quorum");
        let term_start_lsn = votes[donor].as_ref().expect("donor voted").flush;
        let truncate_lsn = votes.iter().flatten().map(|v| v.horizon).max().unwrap_or(0);
        if truncate_lsn > term_start_lsn {
            bail!(
                "acceptor horizon {truncate_lsn} is beyond the donor flush {term_start_lsn} \
                 — inconsistent acceptor state"
            );
        }
        // 5. Download the unfinished suffix from the donor, CHUNKED (FIX
        // H1): a single Read of the whole range can exceed the wire cap
        // (MAX_PEER_LAG_BYTES reaches MAX_MESSAGE_BYTES), and the resulting
        // un-encodable ReadResp would make every reopen fail forever. The
        // acceptor's handle_read already clamps `to_lsn` and serves
        // arbitrary byte ranges (record boundaries are re-established by
        // decode_records over the concatenation), so the loop is purely
        // proposer-side. Everything downloaded must be treated as COMMITTED
        // (the quorum that acked it may differ from the voting quorum).
        // Each chunk's round trip is bounded.
        let read_timeout = cfg.call_timeout.max(cfg.append_timeout);
        let chunk = cfg.recovery_read_chunk.max(1);
        let mut rec_bytes: Vec<u8> =
            Vec::with_capacity(term_start_lsn.saturating_sub(truncate_lsn) as usize);
        let mut read_pos = truncate_lsn;
        while read_pos < term_start_lsn {
            let to_lsn = term_start_lsn.min(read_pos.saturating_add(chunk));
            let c = conns[donor].as_mut().expect("donor is connected");
            match c
                .call_timeout(
                    &Message::Read {
                        from_lsn: read_pos,
                        to_lsn,
                    },
                    read_timeout,
                )
                .await
                .context("the donor did not return the recovery range")?
            {
                Message::ReadResp { from_lsn, records } => {
                    if from_lsn != read_pos {
                        bail!("donor read returned the wrong range start {from_lsn}");
                    }
                    if records.is_empty() {
                        bail!(
                            "donor returned an empty chunk at {read_pos} of \
                             [{truncate_lsn}, {term_start_lsn}) — short read"
                        );
                    }
                    read_pos += records.len() as u64;
                    if read_pos > to_lsn {
                        bail!(
                            "donor overran the requested chunk end {to_lsn} \
                             (returned bytes up to {read_pos})"
                        );
                    }
                    rec_bytes.extend_from_slice(&records);
                }
                other => bail!("unexpected read response {other:?}"),
            }
        }
        if truncate_lsn + rec_bytes.len() as u64 != term_start_lsn {
            bail!(
                "donor returned {} bytes for [{truncate_lsn}, {term_start_lsn}) — short read",
                rec_bytes.len()
            );
        }
        let recovered = decode_records(&rec_bytes, truncate_lsn)
            .context("the donor's recovered records do not decode")?;
        // 6. The new term history: the donor's (already truncated to its
        // flush) + this term starting at the donor's end.
        let mut history = votes[donor].as_ref().expect("donor voted").history.clone();
        if let Some(last) = history.0.last() {
            if last.term >= term {
                bail!(
                    "donor history already contains term {} >= {term}",
                    last.term
                );
            }
        }
        history.0.push(TermLsn {
            term,
            lsn: term_start_lsn,
        });
        // 7. Shared state seeded with the recovered suffix.
        let mut records: VecDeque<RecordMeta> = VecDeque::with_capacity(recovered.len());
        let mut last_wm: HashMap<String, (u64, u64)> = HashMap::new();
        let mut pos = truncate_lsn;
        for rec in &recovered {
            let frame = rec.encode()?;
            let end = pos + frame.len() as u64;
            if rec.kind == RECORD_WATERMARK {
                last_wm.insert(rec.table_key.clone(), (rec.lsn, rec.seq));
            }
            records.push_back(RecordMeta {
                start: pos,
                end,
                kind: rec.kind,
                table_key: rec.table_key.clone(),
                seq: rec.seq,
                frame,
            });
            pos = end;
        }
        let (reelect_tx, reelect_rx) = tokio::sync::mpsc::unbounded_channel();
        let shared = Arc::new(Shared {
            peers: cfg
                .addrs
                .iter()
                .map(|addr| Peer {
                    addr: addr.clone(),
                    flush: AtomicU64::new(0),
                    failed: AtomicBool::new(false),
                    notify: Notify::new(),
                })
                .collect(),
            cfg,
            tail_id,
            election: StdMutex::new(Election {
                term,
                term_start_lsn,
                history,
            }),
            term_tx: watch::Sender::new(term),
            log: StdMutex::new(LogBuf {
                base: truncate_lsn,
                head: term_start_lsn,
                records,
                covered: HashMap::new(),
                last_wm,
            }),
            commit_tx: watch::Sender::new(0),
            commit: AtomicU64::new(0),
            horizon: AtomicU64::new(truncate_lsn),
            poison: StdMutex::new(None),
            reelect_tx,
        });
        // 8. Per-acceptor elected handshake with each live voter, then the
        // streaming tasks (reconnect logic redoes the handshake for the
        // rest).
        let el = shared.election();
        let mut initial: Vec<Option<(Conn, u64)>> = Vec::new();
        for (i, conn) in conns.into_iter().enumerate() {
            let (Some(mut c), Some(vote)) = (conn, votes[i].as_ref()) else {
                initial.push(None);
                continue;
            };
            let start = compute_start(&el.history, &vote.history, vote.flush, term, truncate_lsn)?;
            if start < truncate_lsn {
                tracing::warn!(
                    acceptor = %shared.peers[i].addr,
                    start,
                    base = truncate_lsn,
                    "acceptor needs records below the recovered base; dropping it from \
                     catch-up until the next election"
                );
                shared.peers[i].failed.store(true, Ordering::SeqCst);
                initial.push(None);
                continue;
            }
            match c
                .call_timeout(
                    &Message::Elected {
                        term,
                        start_lsn: start,
                        term_history: el.history.clone(),
                    },
                    shared.cfg.call_timeout,
                )
                .await
            {
                Ok(Message::ElectedResp { ok: true, .. }) => {
                    shared.store_ack(i, term, start);
                    initial.push(Some((c, term)));
                }
                Ok(Message::ElectedResp { term: t, ok: false }) => {
                    bail!("superseded right after election: acceptor is at term {t}");
                }
                Ok(other) => bail!("unexpected elected response {other:?}"),
                Err(e) => {
                    tracing::warn!(
                        acceptor = %shared.peers[i].addr,
                        "elected handshake failed (will reconnect): {e:#}"
                    );
                    initial.push(None);
                }
            }
        }
        shared.recompute_commit();
        let mut tasks: Vec<tokio::task::JoinHandle<()>> = initial
            .into_iter()
            .enumerate()
            .map(|(i, conn)| {
                let shared = shared.clone();
                tokio::spawn(async move { run_peer(shared, i, conn).await })
            })
            .collect();
        // The re-election supervisor (FIX C5): serializes internal
        // re-elections triggered by fencing/stall events.
        {
            let shared = shared.clone();
            tasks.push(tokio::spawn(async move {
                run_supervisor(shared, reelect_rx).await
            }));
        }
        let quorum = Quorum {
            shared: shared.clone(),
            _tasks: tasks,
        };
        // 9. The tail is open once the recovered suffix is quorum-durable
        // IN THIS TERM (commit reaches the term start).
        quorum
            .wait_commit(term_start_lsn)
            .await
            .context("the recovered records could not reach a quorum")?;
        Ok((quorum, recovered))
    }

    pub fn tail_id(&self) -> &str {
        &self.shared.tail_id
    }

    /// Enter one record into the retained log (LSN assigned under the log
    /// lock — callers relying on per-table seq order MUST call this in seq
    /// order) and wake the peer streams. Returns the log position a quorum
    /// must ack; [`wait_commit`](Self::wait_commit) completes the
    /// durability wait (on a stall it attempts ONE internal re-election;
    /// on a second timeout the tail POISONS itself — see [`QuorumConfig`]).
    /// A full append is exactly `submit` + `wait_commit`, split (I2) so the
    /// tail-quorum worker submits records synchronously in job order
    /// (preserving per-table LSN order == seq order) and spawns only the
    /// commit wait — concurrent staged appends pipeline their round trips.
    /// Fails fast (record NOT in the log) when the tail is already
    /// poisoned or the record is oversized.
    pub fn submit(&self, kind: u8, table_key: &str, seq: u64, body: &[u8]) -> Result<u64> {
        if let Some(why) = self.shared.poisoned() {
            bail!("{why}");
        }
        let end = {
            let mut log = self.shared.log.lock().expect("log lock");
            let start = log.head;
            let rec = Record {
                lsn: start,
                kind,
                table_key: table_key.to_string(),
                seq,
                body: body.to_vec(),
            };
            let frame = rec.encode()?;
            // FIX (C6): an oversized record is the STATEMENT's error, never
            // the tail's — it must fail before entering LogBuf.
            check_record_frame_len(frame.len())
                .with_context(|| format!("quorum-tail append for {table_key}/{seq} refused"))?;
            let end = start + frame.len() as u64;
            if kind == RECORD_WATERMARK {
                log.last_wm.insert(table_key.to_string(), (start, seq));
            }
            log.records.push_back(RecordMeta {
                start,
                end,
                kind,
                table_key: table_key.to_string(),
                seq,
                frame,
            });
            log.head = end;
            end
        };
        for p in &self.shared.peers {
            p.notify.notify_one();
        }
        Ok(end)
    }

    /// Wait until the quorum commit position covers `position` — the
    /// second half of an append (see [`submit`](Self::submit)). On a stall
    /// it requests ONE internal re-election; on a second timeout the tail
    /// POISONS itself.
    pub(crate) async fn wait_commit(&self, position: u64) -> Result<()> {
        let mut rx = self.shared.commit_tx.subscribe();
        let mut deadline = tokio::time::Instant::now() + self.shared.cfg.append_timeout;
        let mut stalled_once = false;
        loop {
            if let Some(why) = self.shared.poisoned() {
                bail!("{why}");
            }
            if self.shared.commit.load(Ordering::SeqCst) >= position {
                return Ok(());
            }
            match tokio::time::timeout_at(deadline, rx.changed()).await {
                Ok(Ok(())) => continue,
                Ok(Err(_)) => bail!("quorum tail internal channel closed"),
                Err(_) if !stalled_once => {
                    // FIX (C5): one internal re-election attempt before the
                    // poison — the stall may be a fenced-but-ownerless term
                    // (a crashed competitor) rather than a real outage.
                    stalled_once = true;
                    tracing::warn!(
                        position,
                        "quorum append stalled for {:?}; attempting an internal \
                         re-election before poisoning",
                        self.shared.cfg.append_timeout
                    );
                    self.shared
                        .request_reelection(ReelectReason::Stalled(position));
                    deadline = tokio::time::Instant::now() + self.shared.cfg.append_timeout;
                }
                Err(_) => {
                    let why = format!(
                        "quorum append timed out after {:?} (re-election attempted): \
                         fewer than {QUORUM} of {QUORUM_SIZE} acceptors are acking. The \
                         tail is now POISONED — restart the server once the acceptors \
                         are back (the record may still become durable; the restart's \
                         election replays it exactly once)",
                        self.shared.cfg.append_timeout
                    );
                    self.shared.poison(why.clone());
                    bail!("{why}");
                }
            }
        }
    }

    /// Record that a flush covered `table_key` up to `upto_seq`, advance
    /// the horizon over the newly coverable prefix, and return a watermark
    /// refresh the caller should append (see the module docs).
    pub fn note_covered(&self, table_key: &str, upto_seq: u64) -> Option<(String, u64)> {
        let mut log = self.shared.log.lock().expect("log lock");
        let cur = log.covered.entry(table_key.to_string()).or_insert(0);
        *cur = (*cur).max(upto_seq);
        self.advance_horizon_locked(&mut log)
    }

    fn advance_horizon_locked(&self, log: &mut LogBuf) -> Option<(String, u64)> {
        let commit = self.shared.commit.load(Ordering::SeqCst);
        let (prefix_end, refresh) = coverable_prefix(log, commit);
        let horizon = prefix_end
            .min(commit)
            .max(self.shared.horizon.load(Ordering::SeqCst));
        if horizon > self.shared.horizon.load(Ordering::SeqCst) {
            self.shared.horizon.store(horizon, Ordering::SeqCst);
            // Local retention floor: keep what lagging live peers still
            // need; drop a peer beyond the lag bound instead of growing
            // forever.
            loop {
                let lagging = self
                    .shared
                    .peers
                    .iter()
                    .filter(|p| !p.failed.load(Ordering::SeqCst))
                    .map(|p| p.flush.load(Ordering::SeqCst))
                    .min()
                    .unwrap_or(horizon);
                let drop_to = horizon.min(lagging);
                if horizon - drop_to > MAX_PEER_LAG_BYTES {
                    if let Some(p) = self
                        .shared
                        .peers
                        .iter()
                        .filter(|p| !p.failed.load(Ordering::SeqCst))
                        .min_by_key(|p| p.flush.load(Ordering::SeqCst))
                    {
                        tracing::warn!(
                            acceptor = %p.addr,
                            lag_bytes = horizon - drop_to,
                            "acceptor is too far behind; dropping it from catch-up until \
                             the next election"
                        );
                        p.failed.store(true, Ordering::SeqCst);
                        continue;
                    }
                }
                while let Some(front) = log.records.front() {
                    if front.end <= drop_to {
                        log.base = front.end;
                        log.records.pop_front();
                    } else {
                        break;
                    }
                }
                break;
            }
        }
        refresh
    }

    /// The tables' watermark refresh check without new coverage (used
    /// after watermark appends).
    pub fn nudge_horizon(&self) -> Option<(String, u64)> {
        let mut log = self.shared.log.lock().expect("log lock");
        self.advance_horizon_locked(&mut log)
    }

    /// Last acked flush positions, for tests/diagnostics.
    #[cfg(test)]
    pub fn peer_flushes(&self) -> Vec<u64> {
        self.shared
            .peers
            .iter()
            .map(|p| p.flush.load(Ordering::SeqCst))
            .collect()
    }

    /// The current term, for tests/diagnostics.
    #[cfg(test)]
    pub fn current_term(&self) -> u64 {
        self.shared.term()
    }
}

enum HsError {
    /// A higher term exists — this proposer is fenced (a re-election
    /// candidate, not an immediate poison — FIX C5).
    Fenced(u64),
    /// The acceptor needs records below our retained base.
    Behind,
    /// The acceptor belongs to a DIFFERENT tail (replaced/misconfigured
    /// acceptor) — permanent for this proposer run (FIX I5).
    WrongCluster(anyhow::Error),
    /// Transient (down, mid-restart, refused this round).
    Transient(anyhow::Error),
}

/// Reconnect handshake: greet (identity check), vote at OUR term (a refusal
/// at our exact term is fine — we are already elected; the response's
/// positions are what we need), per-acceptor start computation, elected.
/// Every round trip is bounded by `call_timeout` (FIX I2). Returns the
/// connection, the start position, and the term the connection was
/// handshaked at.
async fn handshake(shared: &Shared, addr: &str) -> Result<(Conn, u64, u64), HsError> {
    let fail = HsError::Transient;
    let el = shared.election();
    let mut c = Conn::connect(addr, shared.cfg.connect_timeout)
        .await
        .map_err(fail)?;
    let greeting = c
        .call_timeout(
            &Message::Greeting {
                tail_id: Some(shared.tail_id.clone()),
            },
            shared.cfg.call_timeout,
        )
        .await;
    match greeting {
        Ok(Message::GreetingResp { term, .. }) => {
            if term > el.term {
                return Err(HsError::Fenced(term));
            }
        }
        Ok(other) => return Err(fail(anyhow!("unexpected greeting response {other:?}"))),
        // FIX (I5): a wrong-tail-id refusal is permanent for this run, not
        // a retry-forever transient.
        Err(e) if format!("{e:#}").contains(WRONG_CLUSTER_MARK) => {
            return Err(HsError::WrongCluster(e))
        }
        Err(e) => return Err(fail(e)),
    }
    let vote = c
        .call_timeout(
            &Message::VoteRequest { term: el.term },
            shared.cfg.call_timeout,
        )
        .await
        .map_err(fail)?;
    let (flush, history) = match vote {
        Message::VoteResponse {
            term,
            flush_lsn,
            term_history,
            ..
        } => {
            if term > el.term {
                return Err(HsError::Fenced(term));
            }
            (flush_lsn, term_history)
        }
        other => return Err(fail(anyhow!("unexpected vote response {other:?}"))),
    };
    let base = shared.log.lock().expect("log lock").base;
    let start = compute_start(&el.history, &history, flush, el.term, base).map_err(fail)?;
    if start < base {
        return Err(HsError::Behind);
    }
    match c
        .call_timeout(
            &Message::Elected {
                term: el.term,
                start_lsn: start,
                term_history: el.history.clone(),
            },
            shared.cfg.call_timeout,
        )
        .await
        .map_err(fail)?
    {
        Message::ElectedResp { ok: true, .. } => Ok((c, start, el.term)),
        Message::ElectedResp { term, ok: false } => Err(HsError::Fenced(term)),
        other => Err(fail(anyhow!("unexpected elected response {other:?}"))),
    }
}

/// Park until an internal re-election moves the term past `seen_term`, or
/// the tail poisons.
async fn park_until_term_change(shared: &Shared, seen_term: u64) {
    let mut rx = shared.term_tx.subscribe();
    loop {
        if shared.poisoned().is_some() {
            return;
        }
        if *rx.borrow_and_update() > seen_term {
            return;
        }
        if rx.changed().await.is_err() {
            return;
        }
    }
}

/// One acceptor's streaming task: (re)connect + handshake, then send
/// AppendRequests from its acked position, updating the quorum commit on
/// every ack. A fencing response requests an internal re-election and
/// parks (FIX C5) instead of poisoning; a `failed` mark parks until the
/// next election (FIX C4). Ends only on poison.
async fn run_peer(shared: Arc<Shared>, idx: usize, mut initial: Option<(Conn, u64)>) {
    let mut backoff = Duration::from_millis(200);
    'outer: loop {
        if shared.poisoned().is_some() {
            return;
        }
        {
            // Expelled for this term: park until the next election clears
            // the flag (or the tail poisons). Read the term BEFORE the
            // flag so a concurrent re-election cannot slip between.
            let seen = shared.term();
            if shared.peers[idx].failed.load(Ordering::SeqCst) {
                park_until_term_change(&shared, seen).await;
                continue;
            }
        }
        let (mut c, conn_term) = match initial.take() {
            Some(pair) => pair,
            None => {
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(2));
                if shared.poisoned().is_some() {
                    return;
                }
                if shared.peers[idx].failed.load(Ordering::SeqCst) {
                    continue;
                }
                match handshake(&shared, &shared.peers[idx].addr.clone()).await {
                    Ok((c, start, term)) => {
                        if !shared.store_ack(idx, term, start) {
                            // A re-election landed mid-handshake: redo it
                            // under the new term.
                            continue;
                        }
                        tracing::info!(
                            acceptor = %shared.peers[idx].addr,
                            start,
                            term,
                            "acceptor reconnected; resuming catch-up"
                        );
                        backoff = Duration::from_millis(200);
                        (c, term)
                    }
                    Err(HsError::Fenced(term)) => {
                        // FIX (C5): request ONE internal re-election
                        // instead of poisoning outright, then park until it
                        // resolves (either a new term or the poison).
                        let my_term = shared.term();
                        if term > my_term {
                            shared.request_reelection(ReelectReason::Fenced(term));
                            park_until_term_change(&shared, my_term).await;
                        }
                        continue;
                    }
                    Err(HsError::Behind) => {
                        tracing::warn!(
                            acceptor = %shared.peers[idx].addr,
                            "acceptor missed the election and lags below the retained \
                             base; dropping it from catch-up until the next election"
                        );
                        shared.peers[idx].failed.store(true, Ordering::SeqCst);
                        continue;
                    }
                    Err(HsError::WrongCluster(e)) => {
                        // FIX (I5): loud, once, and out for this run — a
                        // replaced acceptor answering for a different tail
                        // is an operator problem, not a retry loop.
                        tracing::error!(
                            acceptor = %shared.peers[idx].addr,
                            "acceptor belongs to a DIFFERENT quorum tail (replaced data \
                             dir or wrong --tail-quorum address?); dropping it until the \
                             next election: {e:#}"
                        );
                        shared.peers[idx].failed.store(true, Ordering::SeqCst);
                        continue;
                    }
                    Err(HsError::Transient(e)) => {
                        tracing::debug!(
                            acceptor = %shared.peers[idx].addr,
                            "acceptor reconnect failed: {e:#}"
                        );
                        continue;
                    }
                }
            }
        };
        loop {
            if shared.poisoned().is_some() {
                return;
            }
            if shared.term() != conn_term {
                // An internal re-election happened: this connection's term
                // is stale. Records past the new term's start may only
                // travel on a connection whose Elected handshake made the
                // acceptor adopt the new history — redo it.
                continue 'outer;
            }
            let pos = shared.peers[idx].flush.load(Ordering::SeqCst);
            match shared.next_batch(pos) {
                Batch::UpToDate => {
                    shared.peers[idx].notify.notified().await;
                    // Loop back with the connection intact.
                    initial = Some((c, conn_term));
                    continue 'outer;
                }
                Batch::Behind => {
                    tracing::warn!(
                        acceptor = %shared.peers[idx].addr,
                        "acceptor lags below the retained base; dropping it from \
                         catch-up until the next election"
                    );
                    shared.peers[idx].failed.store(true, Ordering::SeqCst);
                    continue 'outer;
                }
                Batch::Data { begin, end, bytes } => {
                    let msg = Message::Append {
                        term: conn_term,
                        begin_lsn: begin,
                        end_lsn: end,
                        commit_lsn: shared.commit.load(Ordering::SeqCst),
                        horizon_lsn: shared.horizon.load(Ordering::SeqCst),
                        records: bytes,
                    };
                    let append_bound = shared.cfg.append_timeout.max(shared.cfg.call_timeout);
                    match c.call_timeout(&msg, append_bound).await {
                        Ok(Message::AppendResp {
                            ok: true,
                            flush_lsn,
                            ..
                        }) => {
                            if !shared.store_ack(idx, conn_term, flush_lsn) {
                                // Stale ack from before a re-election:
                                // discard it and redo the handshake.
                                continue 'outer;
                            }
                        }
                        Ok(Message::AppendResp {
                            term, ok: false, ..
                        }) => {
                            // FIX (C5): the fence requests a re-election
                            // instead of poisoning outright.
                            let my_term = shared.term();
                            if term > my_term {
                                shared.request_reelection(ReelectReason::Fenced(term));
                                park_until_term_change(&shared, my_term).await;
                            }
                            continue 'outer;
                        }
                        Ok(other) => {
                            tracing::warn!(
                                acceptor = %shared.peers[idx].addr,
                                "unexpected append response {other:?}; reconnecting"
                            );
                            continue 'outer;
                        }
                        Err(e) => {
                            tracing::warn!(
                                acceptor = %shared.peers[idx].addr,
                                "append failed ({e:#}); reconnecting"
                            );
                            continue 'outer;
                        }
                    }
                }
            }
        }
    }
}

/// Why an internal re-election did not complete.
enum ReelectError {
    /// A LIVE competing proposer owns the log (a newer term than our
    /// candidate, a refused vote, or a donor holding records from a term
    /// we never streamed): poison with the fencing message.
    Superseded(u64),
    /// Transient (no reachable/votable quorum right now): retried on the
    /// next fencing event, capped by [`MAX_REELECTION_FAILURES`].
    Failed(anyhow::Error),
}

/// The re-election supervisor (FIX C5): one internal re-election attempt
/// per fencing/stall event, serialized, capped at
/// [`MAX_REELECTION_FAILURES`] consecutive failures.
async fn run_supervisor(
    shared: Arc<Shared>,
    mut rx: tokio::sync::mpsc::UnboundedReceiver<ReelectReason>,
) {
    let mut consecutive_failures: u32 = 0;
    while let Some(reason) = rx.recv().await {
        if shared.poisoned().is_some() {
            return;
        }
        // Skip events a previous re-election (or catch-up) already
        // resolved — one ATTEMPT per event, not per queued duplicate.
        match reason {
            ReelectReason::Fenced(t) if t <= shared.term() => continue,
            ReelectReason::Stalled(pos) if shared.commit.load(Ordering::SeqCst) >= pos => continue,
            _ => {}
        }
        match reelect(&shared).await {
            Ok(new_term) => {
                consecutive_failures = 0;
                tracing::warn!(
                    term = new_term,
                    "quorum tail re-elected itself after a fencing/stall event; \
                     appends continue"
                );
            }
            Err(ReelectError::Superseded(t)) => {
                shared.poison(format!("superseded by a newer server (term {t})"));
                return;
            }
            Err(ReelectError::Failed(e)) => {
                consecutive_failures += 1;
                if consecutive_failures >= MAX_REELECTION_FAILURES {
                    shared.poison(format!(
                        "quorum tail POISONED after {consecutive_failures} consecutive \
                         failed internal re-elections ({e:#}); restart the server once \
                         the acceptors are healthy"
                    ));
                    return;
                }
                tracing::warn!(
                    "internal re-election failed (attempt {consecutive_failures} of \
                     {MAX_REELECTION_FAILURES}): {e:#}"
                );
            }
        }
    }
}

/// One internal re-election (FIX C5): campaign at `max(term seen) + 1`,
/// verify the quorum's log is exactly OUR log (no competitor wrote), adopt
/// the new term with `term_start = donor flush`, and let the peer tasks
/// re-handshake — the unacked LogBuf suffix `[donor flush, head)` then
/// re-streams in the new term. No recovery download is needed: everything
/// the donor holds beyond our base is already retained here (that is what
/// the divergence check proves).
async fn reelect(shared: &Shared) -> Result<u64, ReelectError> {
    let old = shared.election();
    // 1. Greet every acceptor with our identity; collect terms.
    let mut conns: Vec<Option<Conn>> = Vec::new();
    let mut max_term = old.term;
    for addr in &shared.cfg.addrs {
        let opened = async {
            let mut c = Conn::connect(addr, shared.cfg.connect_timeout).await?;
            match c
                .call_timeout(
                    &Message::Greeting {
                        tail_id: Some(shared.tail_id.clone()),
                    },
                    shared.cfg.call_timeout,
                )
                .await?
            {
                Message::GreetingResp { term, .. } => Ok::<_, anyhow::Error>((c, term)),
                other => bail!("unexpected greeting response {other:?}"),
            }
        }
        .await;
        match opened {
            Ok((c, term)) => {
                max_term = max_term.max(term);
                conns.push(Some(c));
            }
            Err(e) => {
                tracing::debug!(acceptor = %addr, "unreachable during re-election: {e:#}");
                conns.push(None);
            }
        }
    }
    if conns.iter().flatten().count() < QUORUM {
        return Err(ReelectError::Failed(anyhow!(
            "fewer than {QUORUM} acceptors reachable for the re-election"
        )));
    }
    // 2. Campaign above everything seen.
    let term = max_term + 1;
    struct Vote {
        flush: u64,
        history: TermHistory,
    }
    let mut votes: Vec<Option<(usize, u64, u64)>> = Vec::new();
    let mut details: Vec<Option<Vote>> = (0..QUORUM_SIZE).map(|_| None).collect();
    for (i, conn) in conns.iter_mut().enumerate() {
        let Some(c) = conn else { continue };
        match c
            .call_timeout(&Message::VoteRequest { term }, shared.cfg.call_timeout)
            .await
        {
            Ok(Message::VoteResponse {
                term: resp_term,
                granted,
                flush_lsn,
                last_log_term,
                term_history,
                ..
            }) => {
                if resp_term > term || !granted {
                    // Someone outbid max(seen) + 1 between the greeting and
                    // the vote: a LIVE campaigning competitor.
                    return Err(ReelectError::Superseded(resp_term));
                }
                votes.push(Some((i, last_log_term, flush_lsn)));
                details[i] = Some(Vote {
                    flush: flush_lsn,
                    history: term_history,
                });
            }
            Ok(other) => {
                tracing::warn!("unexpected re-election vote response {other:?}");
                *conn = None;
            }
            Err(e) => {
                tracing::debug!(acceptor = %shared.cfg.addrs[i], "re-election vote failed: {e:#}");
                *conn = None;
            }
        }
    }
    let granted: Vec<(usize, u64, u64)> = votes.into_iter().flatten().collect();
    if granted.len() < QUORUM {
        return Err(ReelectError::Failed(anyhow!(
            "won only {} of {QUORUM_SIZE} re-election votes",
            granted.len()
        )));
    }
    // 3. Donor + divergence safety: the donor's effective history must be
    // EXACTLY ours truncated to its flush, and its flush must lie inside
    // our retained log. Anything else means a competing proposer WROTE
    // records we do not hold — re-driving our LogBuf would fork the log,
    // so the fence stands and the tail poisons.
    let donor_idx = pick_donor(&granted).expect("granted >= quorum");
    let donor = details[donor_idx].as_ref().expect("donor voted");
    let donor_llt = granted
        .iter()
        .find(|(i, _, _)| *i == donor_idx)
        .map(|(_, llt, _)| *llt)
        .expect("donor is granted");
    let (base, head) = {
        let log = shared.log.lock().expect("log lock");
        (log.base, log.head)
    };
    if donor.history != old.history.up_to(donor.flush) {
        return Err(ReelectError::Superseded(donor_llt.max(max_term)));
    }
    if donor.flush < base || donor.flush > head {
        return Err(ReelectError::Failed(anyhow!(
            "donor flush {} is outside the retained log [{base}, {head}]",
            donor.flush
        )));
    }
    // 4. Adopt the new term. Peer flushes are zeroed: only new-term
    // handshakes may feed the commit rule from here on (a stale in-flight
    // ack must not witness a new-term commit).
    let mut history = old.history.clone();
    history.0.push(TermLsn {
        term,
        lsn: donor.flush,
    });
    {
        let mut el = shared.election.lock().expect("election lock");
        // FIX (C4/M1): expulsion is non-sticky across elections — every
        // peer gets re-evaluated under the new term. ORDER IS LOAD-BEARING:
        // the `failed` flags must clear INSIDE this election-lock critical
        // section, BEFORE the new term becomes observable through
        // `Shared::term()` (which takes the same lock). run_peer reads its
        // term snapshot FIRST and the `failed` flag SECOND; if the new term
        // were published before the clear, a peer could read term = T2 with
        // stale failed = true and `park_until_term_change(> T2)` forever —
        // no further election would ever wake it. With the clear inside the
        // critical section, a peer either sees (T1, any flag) — and any
        // park waits only for > T1, released by the send_replace below — or
        // (T2, failed already cleared).
        for p in &shared.peers {
            p.failed.store(false, Ordering::SeqCst);
            p.flush.store(0, Ordering::SeqCst);
        }
        *el = Election {
            term,
            term_start_lsn: donor.flush,
            history,
        };
    }
    shared.term_tx.send_replace(term);
    for p in &shared.peers {
        p.notify.notify_one();
    }
    Ok(term)
}

// ---------------------------------------------------------------------------
// Unit tests — the pure proposer rules (neon has no Rust unit tests for the
// proposer; these are written fresh from the walproposer.c pseudocode).
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    fn th(entries: &[(u64, u64)]) -> TermHistory {
        TermHistory(
            entries
                .iter()
                .map(|&(term, lsn)| TermLsn { term, lsn })
                .collect(),
        )
    }

    // Donor pick is by (last_log_term, flush) lexicographically: a higher
    // last_log_term beats a longer log from an older term.
    #[test]
    fn donor_pick_orders_by_last_log_term_then_flush() {
        // Acceptor 1 has MORE bytes but from an older term.
        let votes = vec![(0, 3, 100), (1, 2, 500), (2, 3, 90)];
        assert_eq!(pick_donor(&votes), Some(0));
        // Same last_log_term: longer log wins.
        let votes = vec![(0, 3, 100), (1, 3, 500)];
        assert_eq!(pick_donor(&votes), Some(1));
        assert_eq!(pick_donor(&[]), None);
    }

    // Quorum commit = 2nd-highest flush, with flushes below the new term's
    // start ZEROED (the Raft don't-commit-old-terms rule).
    #[test]
    fn quorum_commit_is_second_highest_with_term_start_filter() {
        // All three past term start: plain 2nd-highest.
        assert_eq!(quorum_commit(&[100, 300, 200], 50), 200);
        // One below term start: it counts as 0.
        assert_eq!(quorum_commit(&[40, 300, 200], 50), 200);
        // Two below: no quorum in the new term yet.
        assert_eq!(quorum_commit(&[40, 300, 20], 50), 0);
        // Exactly at term start counts.
        assert_eq!(quorum_commit(&[50, 300, 20], 50), 50);
    }

    // compute_start mirrors the acceptor's divergence point.
    #[test]
    fn compute_start_branches() {
        let prop = th(&[(1, 0), (2, 10), (5, 40)]); // term 5 = the new term
                                                    // Identical prefix ending inside the donor's flush: divergence at
                                                    // min(prop's next start, sk flush).
        let sk = th(&[(1, 0), (2, 10)]);
        assert_eq!(compute_start(&prop, &sk, 35, 5, 0).unwrap(), 35);
        assert_eq!(compute_start(&prop, &sk, 60, 5, 0).unwrap(), 40);
        // sk diverged into its own term 3 at 30: common term 2 ends at 30.
        let sk = th(&[(1, 0), (2, 10), (3, 30)]);
        assert_eq!(compute_start(&prop, &sk, 50, 5, 0).unwrap(), 30);
        // No common term at all: stream from the history start.
        let sk = th(&[(9, 0)]);
        assert_eq!(compute_start(&prop, &sk, 50, 5, 0).unwrap(), 0);
        // Fresh acceptor joins at the retained base.
        let sk = th(&[]);
        assert_eq!(compute_start(&prop, &sk, 0, 5, 123).unwrap(), 123);
        // Acceptor already in the new term (reconnect): resume at its flush.
        let sk = th(&[(1, 0), (2, 10), (5, 40)]);
        assert_eq!(compute_start(&prop, &sk, 77, 5, 0).unwrap(), 77);
    }

    fn meta(start: u64, len: u64, kind: u8, key: &str, seq: u64) -> RecordMeta {
        RecordMeta {
            start,
            end: start + len,
            kind,
            table_key: key.to_string(),
            seq,
            frame: Vec::new(),
        }
    }

    // The coverable prefix walks Frames by coverage + retained-watermark
    // presence, Watermarks by later-watermark retention, stops at the
    // commit position, and suggests refreshing a far-behind blocking
    // watermark.
    #[test]
    fn coverable_prefix_rules() {
        let mut log = LogBuf {
            base: 0,
            head: 40,
            records: VecDeque::new(),
            covered: HashMap::new(),
            last_wm: HashMap::new(),
        };
        log.records.push_back(meta(0, 10, RECORD_FRAME, "t", 1));
        log.records
            .push_back(meta(10, 10, RECORD_WATERMARK, "t", 1));
        log.records.push_back(meta(20, 10, RECORD_FRAME, "t", 2));
        log.records.push_back(meta(30, 10, RECORD_FRAME, "u", 1));
        log.last_wm.insert("t".to_string(), (10, 1));
        // Nothing covered yet: prefix stays at base.
        assert_eq!(coverable_prefix(&log, 40).0, 0);
        // Frame t/1 covered AND the retained watermark (seq 1) covers it;
        // the watermark at 10 is the LATEST for t, so IT blocks (and is
        // close to the head: no refresh suggested).
        log.covered.insert("t".to_string(), 1);
        let (end, refresh) = coverable_prefix(&log, 40);
        assert_eq!(end, 10);
        assert!(refresh.is_none());
        // A newer watermark for t exists: the old one is coverable; then
        // frame t/2 is not covered yet.
        log.last_wm.insert("t".to_string(), (35, 2));
        assert_eq!(coverable_prefix(&log, 40).0, 20);
        // Cover t fully; u is covered but has NO watermark record — the
        // walk must stop at u's frame (FIX C1: presence required).
        log.covered.insert("t".to_string(), 2);
        log.covered.insert("u".to_string(), 1);
        assert_eq!(coverable_prefix(&log, 40).0, 30);
        // With u's watermark retained the walk passes it — still clamped
        // by the commit position.
        log.last_wm.insert("u".to_string(), (45, 1));
        assert_eq!(coverable_prefix(&log, 25).0, 20);
        assert_eq!(coverable_prefix(&log, 40).0, 40);
    }

    // FIX (C1b): coverage alone must NEVER grant the horizon passage over a
    // table's frames — a retained Watermark record with seq >= the frame's
    // is required, or a boot-path truncate that recorded no watermark would
    // let the horizon erase the table's last trace (and with it the
    // sequence floor for the next restart).
    #[test]
    fn coverable_prefix_requires_a_retained_watermark_record() {
        let mut log = LogBuf {
            base: 0,
            head: 20,
            records: VecDeque::new(),
            covered: HashMap::new(),
            last_wm: HashMap::new(),
        };
        log.records.push_back(meta(0, 10, RECORD_FRAME, "t", 1));
        log.records.push_back(meta(10, 10, RECORD_FRAME, "t", 2));
        // Covered far beyond both frames, but NO watermark record retained.
        log.covered.insert("t".to_string(), 9);
        assert_eq!(
            coverable_prefix(&log, 20).0,
            0,
            "covered-but-unwatermarked frames must pin the horizon"
        );
        // A retained watermark BELOW the second frame's seq only releases
        // the first.
        log.last_wm.insert("t".to_string(), (25, 1));
        assert_eq!(coverable_prefix(&log, 20).0, 10);
        // At/above the max seq: both release.
        log.last_wm.insert("t".to_string(), (25, 2));
        assert_eq!(coverable_prefix(&log, 20).0, 20);
    }

    #[test]
    fn coverable_prefix_suggests_watermark_refresh() {
        let head = 10 + WATERMARK_REFRESH_SLACK + 100;
        let mut log = LogBuf {
            base: 0,
            head,
            records: VecDeque::new(),
            covered: HashMap::new(),
            last_wm: HashMap::new(),
        };
        // A lone, far-behind watermark record for a dormant table.
        log.records.push_back(meta(0, 10, RECORD_WATERMARK, "t", 7));
        log.last_wm.insert("t".to_string(), (0, 7));
        let (end, refresh) = coverable_prefix(&log, head);
        assert_eq!(end, 0);
        assert_eq!(refresh, Some(("t".to_string(), 7)));
    }

    // FIX (C6): the record-size cap at the append boundary, tested exactly
    // at it (no multi-hundred-MB allocations needed).
    #[test]
    fn record_frame_len_cap_at_the_boundary() {
        assert!(check_record_frame_len(0).is_ok());
        assert!(check_record_frame_len(MAX_MESSAGE_BYTES - APPEND_MESSAGE_HEADROOM).is_ok());
        let err =
            check_record_frame_len(MAX_MESSAGE_BYTES - APPEND_MESSAGE_HEADROOM + 1).unwrap_err();
        assert!(
            err.to_string().contains("split the statement"),
            "unexpected: {err:#}"
        );
    }

    // FIX (I3): the append-timeout env override — default, floor clamp,
    // garbage tolerance.
    #[test]
    fn append_timeout_env_parsing() {
        assert_eq!(
            append_timeout_from_env(None),
            Duration::from_millis(DEFAULT_APPEND_TIMEOUT_MS)
        );
        assert_eq!(
            append_timeout_from_env(Some("")),
            Duration::from_millis(DEFAULT_APPEND_TIMEOUT_MS)
        );
        assert_eq!(
            append_timeout_from_env(Some("2500")),
            Duration::from_millis(2500)
        );
        assert_eq!(
            append_timeout_from_env(Some(" 1500 ")),
            Duration::from_millis(1500)
        );
        // Below the floor: clamped up, never a hair-trigger poison.
        assert_eq!(
            append_timeout_from_env(Some("10")),
            Duration::from_millis(MIN_APPEND_TIMEOUT_MS)
        );
        // Garbage: the default, never a silent zero.
        assert_eq!(
            append_timeout_from_env(Some("fast")),
            Duration::from_millis(DEFAULT_APPEND_TIMEOUT_MS)
        );
    }
}
