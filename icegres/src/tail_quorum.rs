//! Quorum-replicated durable tail for buffered writes (`--tail-quorum
//! h:p,h:p,h:p`, opt-in) — backend 3 of the durable-tail roadmap
//! (docs/sota-roadmap.md §3): the same [`TailStore`] contract as the local
//! WAL (`tail.rs`) and the Postgres tail (`tail_pg.rs`), with the frames
//! replicated to THREE lightweight `icekeeperd` acceptor processes and
//! acked only once TWO of them have fsynced — Neon SafeKeeper's
//! proposer–acceptor consensus (see `src/quorum/` and NOTICE), CONSENSUS-
//! class durability with no delegated single point.
//!
//! # The durability contract, stated honestly
//!
//! * **Durability = a quorum of independent disks.** Every buffered
//!   INSERT's record is fsynced by 2 of 3 acceptors BEFORE the client ack,
//!   so acked rows survive an unclean kill of this process, losing this
//!   NODE entirely, or losing ANY SINGLE acceptor — strictly stronger than
//!   `--tail-dir` (one disk) and than `--tail-url` (one database's own
//!   replication choices). Place the three acceptors on independent
//!   nodes/disks or the promise degrades accordingly.
//! * **Two live acceptors = writes proceed; one live = writes FAIL.** A
//!   quorum-less append times out, the statement errors, and the tail
//!   POISONS itself (every later append fails; restart the server once the
//!   acceptors are back). Backpressure, never silent loss — and never a
//!   silent downgrade to weaker durability. The poison-on-timeout is
//!   deliberate: a timed-out record may STILL become durable on a lagging
//!   quorum, and continuing to mint sequence numbers past it could
//!   double-apply; the restart's election recovers it exactly once (the
//!   same ambiguous-outcome shape as a Postgres commit whose ack was lost).
//! * **Fencing replaces the flock/advisory locks** of the other backends:
//!   a competing icegres opening the same quorum runs an election with a
//!   higher term, and this server's next append is rejected with that term
//!   — the tail poisons itself with "superseded by a newer server (term
//!   X)". Statements fail cleanly; the flusher keeps flushing
//!   already-buffered rows (safe: the in-commit watermark property + the
//!   catalog's assert-ref-snapshot-id CAS remain the exactly-once guard).
//! * **Same exactly-once protocol as the other backends.** The watermark
//!   lives in the LAKE (`icegres.tail-seq.<tail-id>`; the identity is
//!   minted once and adopted permanently by the acceptors — same quorum =
//!   same logical tail = same cursor). The sidecar's role is played by
//!   Watermark RECORDS in the replicated log: `record_watermark` appends
//!   one (quorum-durable), and replay reports the highest per table.
//! * **The tail append runs under the buffer lock** (like the other
//!   backends): one statement's quorum round trip serializes with other
//!   buffered INSERTs — budget one LAN RTT + one fsync per statement.
//! * **Trusted network only.** No TLS/authentication between proposer and
//!   acceptors yet; keep them on a private segment (docs/limitations.md).
//!
//! # Boot, replay, truncation
//!
//! Open = election: connect (2 of 3 required), adopt/mint the tail
//! identity, win a vote quorum with `term = max(seen) + 1`, pick the most
//! advanced acceptor as donor, download the unfinished committed suffix,
//! reconcile every acceptor's log to it (divergence truncation), and only
//! then serve. `replay()` folds that recovered suffix into per-table
//! frames + watermarks; buffer.rs applies its usual effective-watermark /
//! sequence-floor rules UNCHANGED. Per-table `truncate` marks coverage;
//! the log's horizon advances over fully-covered prefixes and acceptors
//! delete whole segments below it (the latest watermark record per table
//! is always retained — see `quorum::proposer` on horizon lag and
//! watermark refresh).
//!
//! # Sync trait over an async protocol
//!
//! Same worker-thread pattern as `tail_pg.rs`: a dedicated thread owns a
//! current-thread runtime, the three acceptor connections, and the
//! streaming/commit tasks; trait methods send a job over an unbounded
//! channel and block on a std channel for the durable ack.

use std::collections::HashMap;
use std::sync::mpsc as std_mpsc;
use std::sync::Mutex as StdMutex;
use std::thread::JoinHandle;

use anyhow::{anyhow, bail, Context as _, Result};
use arrow::array::RecordBatch;
use iceberg::TableIdent;

use crate::quorum::proposer::{Quorum, QuorumConfig};
use crate::quorum::proto::{Record, RECORD_FRAME, RECORD_WATERMARK};
use crate::tail::{
    decode_op_payload, encode_op_payload, parse_table_dir_name, table_dir_name, ReplayedTable,
    TailOp, TailOpKind, TailStore, TAIL_SEQ_PROPERTY_PREFIX,
};

/// One request to the worker thread; every variant carries its own reply
/// channel (the caller blocks on it for the quorum round trip).
enum Job {
    Append {
        key: String,
        seq: u64,
        payload: Vec<u8>,
        resp: std_mpsc::Sender<Result<()>>,
    },
    Replay {
        resp: std_mpsc::Sender<Result<Vec<Record>>>,
    },
    Truncate {
        key: String,
        upto_seq: u64,
        resp: std_mpsc::Sender<Result<()>>,
    },
    RecordWatermark {
        key: String,
        seq: u64,
        resp: std_mpsc::Sender<Result<()>>,
    },
    /// Test-only: the per-acceptor acked flush positions (convergence
    /// checks in the in-process integration tests).
    #[cfg(test)]
    PeerFlushes {
        resp: std_mpsc::Sender<Result<Vec<u64>>>,
    },
    /// Test-only: an append-shaped job whose spawned responder PANICS
    /// before replying (drops `resp` unsent) — exercises the
    /// closed-channel => poison wiring in `append_roundtrip`.
    #[cfg(test)]
    InjectAppendPanic { resp: std_mpsc::Sender<Result<()>> },
}

/// What the worker reports back once the election + recovery are done.
struct InitState {
    tail_id: String,
    /// Per-table next-sequence seeds from the recovered records.
    seeds: Vec<(String, u64)>,
}

/// [`TailStore`] backed by the acceptor quorum (see the module docs).
pub struct QuorumTail {
    /// `icegres.tail-seq.<tail-id>` — this tail's watermark property key.
    prop_key: String,
    /// `None` only during drop (taken so the worker loop can end).
    job_tx: Option<tokio::sync::mpsc::UnboundedSender<Job>>,
    /// Next sequence per table, seeded at open, bumped only AFTER a
    /// quorum-durable append — a failed append never consumes its number
    /// (and a poisoned tail never appends again, so an ambiguous timed-out
    /// number is never reused either).
    next_seq: StdMutex<HashMap<TableIdent, u64>>,
    /// QuorumTail-level poison (distinct from the proposer's own): set
    /// when an append's responder DIES without reporting an outcome (the
    /// spawned worker task panicked, or the worker runtime tore down
    /// mid-append). The record may or may not have entered the replicated
    /// log; since the failed append did not consume its sequence, letting
    /// a later append reuse `(table, seq)` against a log that may hold the
    /// record would double-apply on replay — so every later append fails
    /// instead (same no-reuse rule as the proposer's timeout poison;
    /// restart the server to recover the ambiguous record exactly-once).
    poisoned: StdMutex<Option<String>>,
    worker: Option<JoinHandle<()>>,
}

impl QuorumTail {
    /// Open the quorum tail: election + recovery + convergence (fails
    /// loudly without a reachable, votable quorum).
    pub fn open(addrs: &[String]) -> Result<Self> {
        Self::open_with_config(QuorumConfig::new(addrs.to_vec()))
    }

    pub(crate) fn open_with_config(cfg: QuorumConfig) -> Result<Self> {
        let (job_tx, job_rx) = tokio::sync::mpsc::unbounded_channel();
        let (init_tx, init_rx) = std_mpsc::channel();
        let worker = std::thread::Builder::new()
            .name("icegres-tail-quorum".into())
            .spawn(move || worker_main(cfg, job_rx, init_tx))
            .context("cannot spawn the tail-quorum worker thread")?;
        let init = match init_rx.recv() {
            Ok(init) => init,
            Err(_) => {
                let _ = worker.join();
                bail!("tail-quorum worker exited before reporting its startup outcome");
            }
        };
        let init = match init {
            Ok(init) => init,
            Err(e) => {
                drop(job_tx);
                let _ = worker.join();
                return Err(e);
            }
        };
        let mut next_seq: HashMap<TableIdent, u64> = HashMap::new();
        for (key, floor) in init.seeds {
            match parse_table_dir_name(&key) {
                Some(ident) => {
                    next_seq.insert(ident, floor);
                }
                None => tracing::warn!(
                    table_key = key,
                    "quorum-tail record does not decode to a table identifier; ignoring \
                     its sequence seed (foreign record?)"
                ),
            }
        }
        Ok(Self {
            prop_key: format!("{TAIL_SEQ_PROPERTY_PREFIX}{}", init.tail_id),
            job_tx: Some(job_tx),
            next_seq: StdMutex::new(next_seq),
            poisoned: StdMutex::new(None),
            worker: Some(worker),
        })
    }

    /// Round-trip one job to the worker: non-blocking send, blocking reply
    /// (the durable-ack wait — the same thread-blocking window the other
    /// backends spend in fsync / the tail database round trip).
    fn call<T>(&self, build: impl FnOnce(std_mpsc::Sender<Result<T>>) -> Job) -> Result<T> {
        let (resp_tx, resp_rx) = std_mpsc::channel();
        self.job_tx
            .as_ref()
            .expect("job_tx lives until drop")
            .send(build(resp_tx))
            .map_err(|_| anyhow!("tail-quorum worker is gone; restart the server"))?;
        resp_rx
            .recv()
            .map_err(|_| anyhow!("tail-quorum worker dropped a request; restart the server"))?
    }

    /// Round-trip an append-shaped job, with the poison rule an append's
    /// AMBIGUITY demands (unlike [`call`](Self::call)): a send failure
    /// means the job never reached the worker (the record certainly did
    /// not enter the log — plain retryable error), but a responder that
    /// DIES without replying (the spawned task panicked inside
    /// `Quorum::append`, or the worker runtime tore down mid-flight) is
    /// ambiguous — the record MAY be in the replicated log — so the tail
    /// poisons itself before erroring: the un-consumed `(table, seq)` must
    /// never be reused against a log that may hold it (double-apply on
    /// replay). Restart recovers the ambiguous record exactly-once via the
    /// election, exactly like the proposer's timeout poison.
    fn append_roundtrip(
        &self,
        table: &TableIdent,
        seq: u64,
        build: impl FnOnce(std_mpsc::Sender<Result<()>>) -> Job,
    ) -> Result<()> {
        let (resp_tx, resp_rx) = std_mpsc::channel();
        self.job_tx
            .as_ref()
            .expect("job_tx lives until drop")
            .send(build(resp_tx))
            .map_err(|_| anyhow!("tail-quorum worker is gone; restart the server"))?;
        match resp_rx.recv() {
            Ok(outcome) => outcome,
            Err(_) => {
                let why = format!(
                    "the tail-quorum append for {table} (seq {seq}) died without \
                     reporting an outcome (worker task panicked or the worker shut \
                     down mid-append); the record may already be in the replicated \
                     log, so its sequence can never be reused"
                );
                let mut poisoned = self.poisoned.lock().expect("tail-quorum poison lock");
                if poisoned.is_none() {
                    *poisoned = Some(why.clone());
                }
                drop(poisoned);
                Err(anyhow!(
                    "quorum tail is POISONED ({why}); restart the server — the \
                     election recovers the ambiguous record exactly once"
                ))
            }
        }
    }

    /// The sticky QuorumTail-level poison check (see the `poisoned` field).
    fn check_poisoned(&self) -> Result<()> {
        let poisoned = self.poisoned.lock().expect("tail-quorum poison lock");
        match poisoned.as_ref() {
            Some(why) => Err(anyhow!(
                "quorum tail is POISONED ({why}); restart the server"
            )),
            None => Ok(()),
        }
    }

    #[cfg(test)]
    fn peer_flushes(&self) -> Result<Vec<u64>> {
        self.call(|resp| Job::PeerFlushes { resp })
    }

    /// Test-only: drive an append-shaped job whose spawned responder
    /// PANICS before replying — the injectable stand-in for a panic inside
    /// `Quorum::append` (see [`append_roundtrip`](Self::append_roundtrip)).
    #[cfg(test)]
    fn inject_append_panic(&self, table: &TableIdent, seq: u64) -> Result<()> {
        self.append_roundtrip(table, seq, |resp| Job::InjectAppendPanic { resp })
    }
}

impl Drop for QuorumTail {
    fn drop(&mut self) {
        // Closing the job channel ends the worker loop; joining tears down
        // the runtime and with it the streaming tasks + connections.
        self.job_tx.take();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl TailStore for QuorumTail {
    fn append(&self, table: &TableIdent, kind: TailOpKind, batches: &[RecordBatch]) -> Result<u64> {
        self.check_poisoned()?;
        let key = table_dir_name(table)?;
        // ICEGRES_QUERY_TIMING tail-ack budget: payload encode vs. the
        // proposer round trip to a 2-of-3 AppendResp quorum. Cached bool
        // when unset.
        let timing = crate::timing::enabled();
        // Encode BEFORE consuming anything: an unencodable statement fails
        // with no seq minted and no round trip made.
        let t = timing.then(std::time::Instant::now);
        let payload = encode_op_payload(kind, batches)?;
        if let Some(t) = t {
            crate::timing::record("tail_encode", t.elapsed());
        }
        let mut map = self.next_seq.lock().expect("tail-quorum seq lock poisoned");
        let entry = map.entry(table.clone()).or_insert(1);
        let seq = *entry;
        let t = timing.then(std::time::Instant::now);
        self.append_roundtrip(table, seq, |resp| Job::Append {
            key,
            seq,
            payload,
            resp,
        })
        .with_context(|| format!("quorum-tail append for {table} (seq {seq}) failed"))?;
        if let Some(t) = t {
            crate::timing::record("tail_quorum_ack", t.elapsed());
        }
        // Only now is the record quorum-durable: consume the sequence.
        *entry += 1;
        Ok(seq)
    }

    fn replay(&self) -> Result<Vec<ReplayedTable>> {
        let records = self.call(|resp| Job::Replay { resp })?;
        // Fold in log order: per-table frames stay in sequence order, the
        // sidecar is the highest watermark record seen for the table.
        type TableFold = (Vec<(u64, TailOp)>, Option<u64>);
        let mut order: Vec<String> = Vec::new();
        let mut tables: HashMap<String, TableFold> = HashMap::new();
        for rec in records {
            let entry = tables.entry(rec.table_key.clone()).or_insert_with(|| {
                order.push(rec.table_key.clone());
                (Vec::new(), None)
            });
            match rec.kind {
                RECORD_FRAME => {
                    let op = decode_op_payload(&rec.body).with_context(|| {
                        format!(
                            "quorum-tail record {}/{} does not decode (its rows hold acked \
                             writes)",
                            rec.table_key, rec.seq
                        )
                    })?;
                    entry.0.push((rec.seq, op));
                }
                RECORD_WATERMARK => {
                    entry.1 = Some(entry.1.unwrap_or(0).max(rec.seq));
                }
                other => bail!("quorum-tail record has unknown kind {other}"),
            }
        }
        let mut out: Vec<ReplayedTable> = Vec::with_capacity(order.len());
        for key in order {
            let (frames, sidecar_watermark) = tables.remove(&key).expect("just inserted");
            let Some(ident) = parse_table_dir_name(&key) else {
                tracing::warn!(
                    table_key = key,
                    "quorum-tail record does not name an <ns>.<table>; skipping it"
                );
                continue;
            };
            out.push(ReplayedTable {
                ident,
                frames,
                sidecar_watermark,
            });
        }
        Ok(out)
    }

    fn truncate(&self, table: &TableIdent, upto_seq: u64) -> Result<()> {
        let key = table_dir_name(table)?;
        self.call(|resp| Job::Truncate {
            key,
            upto_seq,
            resp,
        })
        .with_context(|| format!("quorum-tail truncate for {table} (<= {upto_seq}) failed"))
    }

    fn ensure_seq_floor(&self, table: &TableIdent, floor: u64) -> Result<()> {
        let mut map = self.next_seq.lock().expect("tail-quorum seq lock poisoned");
        let entry = map.entry(table.clone()).or_insert(1);
        *entry = (*entry).max(floor);
        Ok(())
    }

    fn watermark_property(&self) -> &str {
        &self.prop_key
    }

    fn record_watermark(&self, table: &TableIdent, seq: u64) -> Result<()> {
        // The outcome is the caller's to act on (buffer.rs skips the
        // covered-frame truncate when this fails): report it honestly.
        let key = table_dir_name(table).with_context(|| {
            format!("cannot encode the quorum-tail table key for the watermark of {table}")
        })?;
        self.call(|resp| Job::RecordWatermark { key, seq, resp })
            .with_context(|| format!("quorum-tail watermark append for {table} ({seq}) failed"))
    }
}

/// The worker thread: a current-thread runtime driving the election, the
/// per-acceptor streaming tasks, and the job loop until the channel closes
/// (= QuorumTail drop).
fn worker_main(
    cfg: QuorumConfig,
    mut job_rx: tokio::sync::mpsc::UnboundedReceiver<Job>,
    init_tx: std_mpsc::Sender<Result<InitState>>,
) {
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            let _ = init_tx.send(Err(
                anyhow!(e).context("cannot build the tail-quorum runtime")
            ));
            return;
        }
    };
    rt.block_on(async move {
        let (quorum, recovered) = match Quorum::open(cfg).await {
            Ok(opened) => opened,
            Err(e) => {
                let _ = init_tx.send(Err(e));
                return;
            }
        };
        // `Arc` so Append jobs can be SPAWNED off the job loop: an
        // append's quorum round trip must not head-of-line-block the next
        // job (the flusher's truncate/watermark jobs behind a statement's
        // append). HONEST SCOPE: this is NOT append pipelining — appends
        // themselves still serialize upstream (QuorumTail::append holds
        // the seq-map lock across the reply, and the default
        // `append_staged` runs the whole round trip under the buffer
        // lock), so at most ONE statement append is ever in flight.
        // `Quorum::append` would tolerate more (records enter the shared
        // LogBuf under its lock, commit waiters are broadcast-driven), but
        // the per-table sequencing above it does not, yet.
        let quorum = std::sync::Arc::new(quorum);
        let mut seeds: HashMap<String, u64> = HashMap::new();
        // Highest watermark appended per table (never regress the sidecar,
        // mirroring LocalWal's skip).
        let mut wm_max: HashMap<String, u64> = HashMap::new();
        for rec in &recovered {
            let s = seeds.entry(rec.table_key.clone()).or_insert(0);
            *s = (*s).max(rec.seq);
            if rec.kind == RECORD_WATERMARK {
                let w = wm_max.entry(rec.table_key.clone()).or_insert(0);
                *w = (*w).max(rec.seq);
            }
        }
        let init = InitState {
            tail_id: quorum.tail_id().to_string(),
            seeds: seeds.into_iter().map(|(k, s)| (k, s + 1)).collect(),
        };
        if init_tx.send(Ok(init)).is_err() {
            return; // opener gone; nothing to serve
        }
        let mut replay_records: Option<Vec<Record>> = Some(recovered);
        while let Some(job) = job_rx.recv().await {
            run_job(&quorum, &mut replay_records, &mut wm_max, job).await;
        }
        // Channel closed = QuorumTail dropped: block_on returns, dropping
        // the runtime, the streaming tasks, and the connections.
    });
}

async fn run_job(
    quorum: &std::sync::Arc<Quorum>,
    replay_records: &mut Option<Vec<Record>>,
    wm_max: &mut HashMap<String, u64>,
    job: Job,
) {
    match job {
        Job::Append {
            key,
            seq,
            payload,
            resp,
        } => {
            // Spawned so truncate/watermark jobs behind it are not
            // head-of-line blocked — NOT append pipelining (see the
            // worker_main comment: QuorumTail::append holds the seq-map
            // lock across the reply, so at most one append job is in
            // flight; the LSN is assigned inside `Quorum::append` under
            // the log lock). If this task dies WITHOUT replying (a panic
            // inside `Quorum::append`), `resp` drops unsent and the caller
            // observes the closed channel and POISONS the tail — the
            // record may already be in the log and its un-consumed
            // sequence must never be reused (QuorumTail::append_roundtrip).
            let quorum = quorum.clone();
            tokio::spawn(async move {
                let r = quorum.append(RECORD_FRAME, &key, seq, &payload).await;
                let _ = resp.send(r);
            });
        }
        Job::Replay { resp } => {
            let _ = resp.send(Ok(replay_records.take().unwrap_or_default()));
        }
        Job::Truncate {
            key,
            upto_seq,
            resp,
        } => {
            let refresh = quorum.note_covered(&key, upto_seq);
            // Coverage is recorded; the caller need not wait for the
            // (best-effort) watermark refresh below.
            let _ = resp.send(Ok(()));
            if let Some((table, seq)) = refresh {
                match quorum.append(RECORD_WATERMARK, &table, seq, &[]).await {
                    Ok(()) => {
                        let w = wm_max.entry(table).or_insert(0);
                        *w = (*w).max(seq);
                        let _ = quorum.nudge_horizon();
                    }
                    Err(e) => tracing::warn!(
                        table_key = table,
                        "quorum-tail watermark refresh failed (horizon lags; retried on \
                         the next flush): {e:#}"
                    ),
                }
            }
        }
        Job::RecordWatermark { key, seq, resp } => {
            // Never regress a higher watermark (an older flush retrying
            // late) — the LATEST watermark record must carry the max.
            if wm_max.get(&key).copied().unwrap_or(0) >= seq {
                let _ = resp.send(Ok(()));
                return;
            }
            let r = quorum.append(RECORD_WATERMARK, &key, seq, &[]).await;
            if r.is_ok() {
                let w = wm_max.entry(key).or_insert(0);
                *w = (*w).max(seq);
                let _ = quorum.nudge_horizon();
            }
            let _ = resp.send(r);
        }
        #[cfg(test)]
        Job::PeerFlushes { resp } => {
            let _ = resp.send(Ok(quorum.peer_flushes()));
        }
        #[cfg(test)]
        Job::InjectAppendPanic { resp } => {
            tokio::spawn(async move {
                // `resp` is moved in and dropped UNSENT when the panic
                // unwinds — the caller sees the closed channel.
                let _hold = resp;
                panic!("injected append panic (test)");
            });
        }
    }
}

// ---------------------------------------------------------------------------
// In-process integration tests: 3 real acceptors (own threads + runtimes +
// temp dirs + ephemeral ports) driven through the full TailStore surface.
// No external processes, no shell.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use crate::quorum::acceptor;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef as ArrowSchemaRef};
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::{Duration, Instant};

    static TEST_DIR_SEQ: AtomicU64 = AtomicU64::new(0);

    fn temp_dir(name: &str) -> PathBuf {
        let n = TEST_DIR_SEQ.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!(
            "icegres-tail-quorum-test-{}-{}-{}",
            std::process::id(),
            name,
            n
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// One in-process acceptor: its own thread + current-thread runtime.
    struct TestAcceptor {
        addr: String,
        dir: PathBuf,
        node_id: u64,
        /// Segment-rotate override (tiny segments for GC-shape tests);
        /// preserved across `restart`.
        rotate: Option<u64>,
        shutdown: Option<tokio::sync::oneshot::Sender<()>>,
        thread: Option<std::thread::JoinHandle<()>>,
    }

    fn spawn_acceptor(
        dir: &Path,
        bind: Option<&str>,
        node_id: u64,
        rotate: Option<u64>,
    ) -> TestAcceptor {
        let (addr_tx, addr_rx) = std_mpsc::channel();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let dir2 = dir.to_path_buf();
        let want = bind.map(str::to_string);
        let thread = std::thread::Builder::new()
            .name("test-icekeeper".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async move {
                    let mut a = acceptor::open_dir(&dir2, node_id).expect("open acceptor dir");
                    if let Some(n) = rotate {
                        a.wal.set_rotate_bytes(n);
                    }
                    let listener =
                        tokio::net::TcpListener::bind(want.as_deref().unwrap_or("127.0.0.1:0"))
                            .await
                            .expect("bind acceptor listener");
                    addr_tx
                        .send(listener.local_addr().unwrap().to_string())
                        .unwrap();
                    let shared: acceptor::SharedAcceptor = Arc::new(tokio::sync::Mutex::new(a));
                    tokio::select! {
                        res = acceptor::serve(listener, shared) => {
                            res.expect("acceptor serve failed");
                        }
                        _ = shutdown_rx => {}
                    }
                });
            })
            .unwrap();
        let addr = addr_rx.recv().expect("acceptor failed to bind");
        TestAcceptor {
            addr,
            dir: dir.to_path_buf(),
            node_id,
            rotate,
            shutdown: Some(shutdown_tx),
            thread: Some(thread),
        }
    }

    impl TestAcceptor {
        /// Stop the acceptor (the moral kill -9: the runtime is dropped;
        /// only fsynced state survives — which is everything acked).
        fn kill(&mut self) {
            if let Some(tx) = self.shutdown.take() {
                let _ = tx.send(());
            }
            if let Some(th) = self.thread.take() {
                th.join().expect("acceptor thread panicked");
            }
        }

        /// Restart on the SAME address and data dir.
        fn restart(&mut self) {
            assert!(self.thread.is_none(), "kill() first");
            let mut fresh = spawn_acceptor(&self.dir, Some(&self.addr), self.node_id, self.rotate);
            self.shutdown = fresh.shutdown.take();
            self.thread = fresh.thread.take();
            // `fresh` drops with both handles taken: its kill() is a no-op.
        }
    }

    impl Drop for TestAcceptor {
        fn drop(&mut self) {
            self.kill();
        }
    }

    fn spawn_cluster(name: &str) -> (Vec<TestAcceptor>, Vec<String>) {
        spawn_cluster_with_rotate(name, None)
    }

    fn spawn_cluster_with_rotate(
        name: &str,
        rotate: Option<u64>,
    ) -> (Vec<TestAcceptor>, Vec<String>) {
        let acceptors: Vec<TestAcceptor> = (0..3)
            .map(|i| spawn_acceptor(&temp_dir(&format!("{name}-{i}")), None, i as u64, rotate))
            .collect();
        let addrs = acceptors.iter().map(|a| a.addr.clone()).collect();
        (acceptors, addrs)
    }

    fn cfg(addrs: &[String], append_timeout_ms: u64) -> QuorumConfig {
        let mut cfg = QuorumConfig::new(addrs.to_vec());
        cfg.append_timeout = Duration::from_millis(append_timeout_ms);
        cfg.connect_timeout = Duration::from_millis(1000);
        cfg.call_timeout = Duration::from_millis(2000);
        cfg
    }

    fn schema() -> ArrowSchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn ident() -> TableIdent {
        TableIdent::from_strs(["demo", "t"]).unwrap()
    }

    fn batch(vals: &[i64]) -> RecordBatch {
        RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vals.to_vec()))]).unwrap()
    }

    fn ids(b: &RecordBatch) -> Vec<i64> {
        b.column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    // Appends quorum-ack with all three up AND with one acceptor down.
    #[test]
    fn appends_survive_one_acceptor_down() {
        let (mut acceptors, addrs) = spawn_cluster("one-down");
        let tail = QuorumTail::open_with_config(cfg(&addrs, 5000)).unwrap();
        assert_eq!(
            tail.append(&ident(), TailOpKind::Append, &[batch(&[1])])
                .unwrap(),
            1
        );
        acceptors[2].kill();
        // 2 of 3 still ack: writes proceed.
        assert_eq!(
            tail.append(&ident(), TailOpKind::Append, &[batch(&[2])])
                .unwrap(),
            2
        );
        assert_eq!(
            tail.append(&ident(), TailOpKind::Append, &[batch(&[3])])
                .unwrap(),
            3
        );
    }

    // With two acceptors down, appends FAIL (statement error) and the tail
    // poisons itself — no silent downgrade, and no seq reuse after an
    // ambiguous timeout.
    #[test]
    fn appends_fail_with_two_acceptors_down() {
        let (mut acceptors, addrs) = spawn_cluster("two-down");
        let tail = QuorumTail::open_with_config(cfg(&addrs, 700)).unwrap();
        tail.append(&ident(), TailOpKind::Append, &[batch(&[1])])
            .unwrap();
        acceptors[1].kill();
        acceptors[2].kill();
        let err = tail
            .append(&ident(), TailOpKind::Append, &[batch(&[2])])
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("POISONED"),
            "expected the quorum-timeout poison, got: {err:#}"
        );
        // Poison is sticky: later appends fail fast.
        let err = tail
            .append(&ident(), TailOpKind::Append, &[batch(&[3])])
            .unwrap_err();
        assert!(format!("{err:#}").contains("POISONED"), "got: {err:#}");
    }

    // Kill and restart an acceptor mid-stream: proposer-driven catch-up
    // converges it, after which it can carry the quorum alone with one
    // OTHER acceptor down.
    #[test]
    fn killed_acceptor_catches_up_after_restart() {
        let (mut acceptors, addrs) = spawn_cluster("catch-up");
        let tail = QuorumTail::open_with_config(cfg(&addrs, 5000)).unwrap();
        tail.append(&ident(), TailOpKind::Append, &[batch(&[1])])
            .unwrap();
        acceptors[2].kill();
        for v in 2..=5 {
            tail.append(&ident(), TailOpKind::Append, &[batch(&[v])])
                .unwrap();
        }
        acceptors[2].restart();
        // Wait for convergence: all three acked flushes equal.
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            let flushes = tail.peer_flushes().unwrap();
            let max = *flushes.iter().max().unwrap();
            if max > 0 && flushes.iter().all(|&f| f == max) {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "acceptor did not catch up: {flushes:?}"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        // The restarted acceptor now carries the quorum with #1 down.
        acceptors[1].kill();
        assert_eq!(
            tail.append(&ident(), TailOpKind::Append, &[batch(&[6])])
                .unwrap(),
            6
        );
    }

    // Proposer restart: a new election + donor recovery replays exactly
    // the acked records (no loss, no duplicates), sequence numbering
    // resumes above them, and covered frames replay as watermark-only.
    #[test]
    fn proposer_restart_replays_exactly_the_acked_records() {
        let (_acceptors, addrs) = spawn_cluster("replay");
        {
            let tail = QuorumTail::open_with_config(cfg(&addrs, 5000)).unwrap();
            for v in 1..=3i64 {
                tail.append(&ident(), TailOpKind::Append, &[batch(&[v])])
                    .unwrap();
            }
        } // dropped: the next open elects a new term and recovers
        let tail = QuorumTail::open_with_config(cfg(&addrs, 5000)).unwrap();
        let replayed = tail.replay().unwrap();
        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0].ident, ident());
        let seqs: Vec<u64> = replayed[0].frames.iter().map(|(s, _)| *s).collect();
        assert_eq!(seqs, vec![1, 2, 3], "no loss, no duplicates");
        let rows: Vec<i64> = replayed[0]
            .frames
            .iter()
            .flat_map(|(_, op)| op.batches().iter().map(ids).next().unwrap())
            .collect();
        assert_eq!(rows, vec![1, 2, 3]);
        assert_eq!(replayed[0].sidecar_watermark, None);
        // Sequence numbering resumes above the recovered frames.
        assert_eq!(
            tail.append(&ident(), TailOpKind::Append, &[batch(&[4])])
                .unwrap(),
            4
        );
        // A flush drains everything: watermark + truncate. The NEXT
        // restart may still REPORT covered frames (the acceptors' horizon
        // advances lazily, piggybacked on later appends), but the
        // replayed watermark covers them — exactly-once holds through
        // buffer.rs's usual drop_stale_frames, which is the contract.
        tail.record_watermark(&ident(), 4).unwrap();
        tail.truncate(&ident(), 4).unwrap();
        drop(tail);
        let tail = QuorumTail::open_with_config(cfg(&addrs, 5000)).unwrap();
        let replayed = tail.replay().unwrap();
        assert_eq!(replayed.len(), 1);
        assert_eq!(replayed[0].sidecar_watermark, Some(4));
        let (survivors, _dropped) = crate::tail::drop_stale_frames(
            replayed[0].frames.clone(),
            replayed[0].sidecar_watermark,
        );
        assert!(
            survivors.is_empty(),
            "covered frames must not survive the watermark filter: {:?}",
            survivors.iter().map(|(s, _)| *s).collect::<Vec<_>>()
        );
        // The seq floor holds even with no surviving frames.
        assert_eq!(
            tail.append(&ident(), TailOpKind::Append, &[batch(&[5])])
                .unwrap(),
            5
        );
    }

    // A competing proposer (second icegres on the same quorum) fences the
    // first: its next append fails with the superseded error while the new
    // one owns the log.
    #[test]
    fn competing_proposer_fences_the_old_one() {
        let (_acceptors, addrs) = spawn_cluster("fencing");
        let old = QuorumTail::open_with_config(cfg(&addrs, 5000)).unwrap();
        old.append(&ident(), TailOpKind::Append, &[batch(&[1])])
            .unwrap();
        let new = QuorumTail::open_with_config(cfg(&addrs, 5000)).unwrap();
        let err = old
            .append(&ident(), TailOpKind::Append, &[batch(&[2])])
            .unwrap_err();
        assert!(
            format!("{err:#}").contains("superseded by a newer server"),
            "expected the fencing error, got: {err:#}"
        );
        // The new proposer owns the log: its replay holds the acked record
        // and its appends proceed.
        let replayed = new.replay().unwrap();
        assert_eq!(replayed.len(), 1);
        assert_eq!(
            replayed[0]
                .frames
                .iter()
                .map(|(s, _)| *s)
                .collect::<Vec<_>>(),
            vec![1]
        );
        assert_eq!(
            new.append(&ident(), TailOpKind::Append, &[batch(&[2])])
                .unwrap(),
            2
        );
    }

    // Fewer than two reachable acceptors: open fails loudly.
    #[test]
    fn open_fails_without_a_quorum() {
        let (mut acceptors, addrs) = spawn_cluster("no-quorum");
        acceptors[0].kill();
        acceptors[1].kill();
        let err = match QuorumTail::open_with_config(cfg(&addrs, 1000)) {
            Ok(_) => panic!("open must fail without a reachable quorum"),
            Err(e) => e,
        };
        assert!(
            format!("{err:#}").contains("quorum"),
            "expected a quorum error, got: {err:#}"
        );
    }

    fn ident_u() -> TableIdent {
        TableIdent::from_strs(["demo", "u"]).unwrap()
    }

    // FIX (C1): the full boot-path interleaving that used to erase a
    // table's last trace. First flush stamped the LAKE property but crashed
    // BEFORE record_watermark; the restart's boot replay then truncates the
    // covered frames — through the fixed record-watermark-THEN-truncate
    // order (what buffer.rs's tail_truncate_covered does) — and the horizon
    // passes the frames. After a second restart the table MUST still be
    // reported (its watermark record is retained), so the sequence floor
    // applies and a new insert mints a seq ABOVE the property watermark —
    // rows that therefore reach the lake instead of being silently dropped
    // as already-committed by the next replay.
    #[test]
    fn boot_truncate_keeps_the_tables_last_trace() {
        let (_acceptors, addrs) = spawn_cluster("boot-trace");
        {
            // Acked frames 1..=3; "crash" after the flush stamped the
            // property but before any watermark record.
            let tail = QuorumTail::open_with_config(cfg(&addrs, 5000)).unwrap();
            for v in 1..=3i64 {
                tail.append(&ident(), TailOpKind::Append, &[batch(&[v])])
                    .unwrap();
            }
        }
        {
            // Restart 1: boot replay sees the frames covered by the
            // property watermark (3) and runs the boot cleanup in the
            // FIXED order.
            let tail = QuorumTail::open_with_config(cfg(&addrs, 5000)).unwrap();
            let replayed = tail.replay().unwrap();
            assert_eq!(replayed.len(), 1);
            assert_eq!(replayed[0].frames.len(), 3);
            tail.record_watermark(&ident(), 3).unwrap();
            tail.truncate(&ident(), 3).unwrap();
            // Unrelated traffic drives the (piggybacked) horizon to the
            // acceptors, GC'ing t's covered frames for real.
            for v in 1..=2i64 {
                tail.append(&ident_u(), TailOpKind::Append, &[batch(&[v])])
                    .unwrap();
            }
        }
        // Restart 2: t must still be reported via its retained watermark
        // record, and its next sequence must clear the committed watermark.
        let tail = QuorumTail::open_with_config(cfg(&addrs, 5000)).unwrap();
        let replayed = tail.replay().unwrap();
        let t = replayed
            .iter()
            .find(|r| r.ident == ident())
            .expect("the boot truncate must not erase the table's last trace");
        assert_eq!(t.sidecar_watermark, Some(3));
        let (survivors, _) = crate::tail::drop_stale_frames(t.frames.clone(), t.sidecar_watermark);
        assert!(survivors.is_empty(), "no covered frame may replay");
        assert_eq!(
            tail.append(&ident(), TailOpKind::Append, &[batch(&[4])])
                .unwrap(),
            4,
            "sequences must resume ABOVE the committed watermark"
        );
        drop(tail);
        // Restart 3: the new row survives the watermark filter — it reaches
        // the lake.
        let tail = QuorumTail::open_with_config(cfg(&addrs, 5000)).unwrap();
        let replayed = tail.replay().unwrap();
        let t = replayed.iter().find(|r| r.ident == ident()).unwrap();
        let (survivors, _) = crate::tail::drop_stale_frames(t.frames.clone(), t.sidecar_watermark);
        assert_eq!(
            survivors.iter().map(|(s, _)| *s).collect::<Vec<_>>(),
            vec![4],
            "the post-truncate insert must survive replay"
        );
    }

    // FIX (C2/I1): horizon GC + full-cluster restart. The acceptors persist
    // the horizon BEFORE deleting covered segments and report their
    // effective horizon at vote time, so a restarted cluster's recovery
    // never reads a GC'd range — the pre-fix shape bricked open() forever.
    #[test]
    fn horizon_gc_survives_a_full_cluster_restart() {
        // Tiny rotate: every appended batch seals its own segment, so the
        // horizon advance actually deletes files.
        let (mut acceptors, addrs) = spawn_cluster_with_rotate("gc-restart", Some(1));
        let tail = QuorumTail::open_with_config(cfg(&addrs, 5000)).unwrap();
        for v in 1..=5i64 {
            tail.append(&ident(), TailOpKind::Append, &[batch(&[v])])
                .unwrap();
        }
        tail.record_watermark(&ident(), 5).unwrap();
        tail.truncate(&ident(), 5).unwrap();
        // Piggyback the horizon onto follow-up appends.
        for v in 1..=3i64 {
            tail.append(&ident_u(), TailOpKind::Append, &[batch(&[v])])
                .unwrap();
        }
        // Wait until at least one acceptor deleted a leading segment.
        let first_seg = |dir: &Path| -> Option<u64> {
            std::fs::read_dir(dir.join("wal"))
                .ok()?
                .flatten()
                .filter_map(|e| {
                    let p = e.path();
                    if p.extension().and_then(|x| x.to_str()) != Some("seg") {
                        return None;
                    }
                    u64::from_str_radix(p.file_stem()?.to_str()?, 16).ok()
                })
                .min()
        };
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            if acceptors
                .iter()
                .any(|a| first_seg(&a.dir).is_some_and(|s| s > 0))
            {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "no acceptor GC'd its covered prefix"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
        drop(tail);
        // Full-cluster restart.
        for a in acceptors.iter_mut() {
            a.kill();
            a.restart();
        }
        // Pre-fix this open() failed forever ("log range not fully
        // retained"): every vote reported the stale persisted horizon and
        // recovery read a deleted range.
        let tail = QuorumTail::open_with_config(cfg(&addrs, 5000)).unwrap();
        let replayed = tail.replay().unwrap();
        let t = replayed.iter().find(|r| r.ident == ident()).unwrap();
        assert_eq!(t.sidecar_watermark, Some(5));
        let u = replayed.iter().find(|r| r.ident == ident_u()).unwrap();
        assert_eq!(
            u.frames.iter().map(|(s, _)| *s).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        // And the tail still takes writes.
        assert_eq!(
            tail.append(&ident(), TailOpKind::Append, &[batch(&[6])])
                .unwrap(),
            6
        );
    }

    // FIX (C5): a FAILED competing election (terms bumped by bare votes,
    // no records written, campaigner gone) must not poison the tail — one
    // internal re-election recovers ownership and the in-flight append
    // commits. The genuine-live-competitor contrast is
    // competing_proposer_fences_the_old_one above (poison stands).
    #[test]
    fn dormant_term_bump_reelects_instead_of_poisoning() {
        let (_acceptors, addrs) = spawn_cluster("reelect");
        let tail = QuorumTail::open_with_config(cfg(&addrs, 8000)).unwrap();
        assert_eq!(
            tail.append(&ident(), TailOpKind::Append, &[batch(&[1])])
                .unwrap(),
            1
        );
        // The failed competitor: bump TWO acceptors' terms with bare
        // VoteRequests (persisted votes; no Elected, no appends), so the
        // old code's next append hit the fence on a quorum and poisoned.
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            use crate::quorum::proto::{Conn, Message};
            for addr in &addrs[0..2] {
                let mut c = Conn::connect(addr, Duration::from_secs(2)).await.unwrap();
                let resp = c.call(&Message::VoteRequest { term: 42 }).await.unwrap();
                assert!(
                    matches!(resp, Message::VoteResponse { granted: true, .. }),
                    "test setup: the term bump was refused: {resp:?}"
                );
            }
        });
        // The next appends see Fenced, re-elect internally, and COMMIT —
        // no poison, no lost in-flight records (LogBuf survives).
        assert_eq!(
            tail.append(&ident(), TailOpKind::Append, &[batch(&[2])])
                .unwrap(),
            2
        );
        assert_eq!(
            tail.append(&ident(), TailOpKind::Append, &[batch(&[3])])
                .unwrap(),
            3
        );
        drop(tail);
        // A clean reopen replays every acked record exactly once.
        let tail = QuorumTail::open_with_config(cfg(&addrs, 5000)).unwrap();
        let replayed = tail.replay().unwrap();
        assert_eq!(replayed.len(), 1);
        assert_eq!(
            replayed[0]
                .frames
                .iter()
                .map(|(s, _)| *s)
                .collect::<Vec<_>>(),
            vec![1, 2, 3],
            "no loss, no duplicates across the internal re-election"
        );
    }

    // FIX (I2): a connected-but-SILENT acceptor (accepts TCP, never
    // answers) must not hang open() forever — the per-call timeout treats
    // it as unavailable and the other two carry the quorum.
    #[test]
    fn open_succeeds_despite_a_silent_acceptor() {
        let a1 = spawn_acceptor(&temp_dir("silent-1"), None, 1, None);
        let a2 = spawn_acceptor(&temp_dir("silent-2"), None, 2, None);
        let silent = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let silent_addr = silent.local_addr().unwrap().to_string();
        std::thread::spawn(move || {
            // Accept and HOLD every connection, answering nothing.
            let mut held = Vec::new();
            for conn in silent.incoming() {
                match conn {
                    Ok(c) => held.push(c),
                    Err(_) => break,
                }
            }
        });
        let addrs = vec![a1.addr.clone(), a2.addr.clone(), silent_addr];
        let mut c = cfg(&addrs, 5000);
        c.call_timeout = Duration::from_millis(500);
        let started = Instant::now();
        let tail = QuorumTail::open_with_config(c).unwrap();
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "open() must bound the silent acceptor's calls, took {:?}",
            started.elapsed()
        );
        assert_eq!(
            tail.append(&ident(), TailOpKind::Append, &[batch(&[1])])
                .unwrap(),
            1
        );
        drop(tail);
        drop(a1);
        drop(a2);
    }

    // F4: an append whose spawned worker task DIES without reporting an
    // outcome (a panic inside Quorum::append) is AMBIGUOUS — the record
    // may already be in the replicated log — so the tail must POISON
    // itself instead of returning a plain error: the failed append never
    // consumed its sequence, and a later append reusing (table, seq)
    // against a log that may hold the record would double-apply on replay.
    #[test]
    fn append_task_death_without_outcome_poisons_the_tail() {
        let (_acceptors, addrs) = spawn_cluster("panic-poison");
        let tail = QuorumTail::open_with_config(cfg(&addrs, 5000)).unwrap();
        assert_eq!(
            tail.append(&ident(), TailOpKind::Append, &[batch(&[1])])
                .unwrap(),
            1
        );
        // Seq 2 would be next; its injected append dies without an outcome.
        let err = tail.inject_append_panic(&ident(), 2).unwrap_err();
        assert!(
            format!("{err:#}").contains("POISONED"),
            "a dead-responder append must poison, got: {err:#}"
        );
        // The poison is sticky: no later append runs (so the ambiguous
        // sequence is never reused), and the worker is still alive (the
        // panic was confined to the spawned task) — the check fires before
        // any round trip.
        let err = tail
            .append(&ident(), TailOpKind::Append, &[batch(&[2])])
            .unwrap_err();
        assert!(format!("{err:#}").contains("POISONED"), "got: {err:#}");
        drop(tail);
        // A restart (new election) recovers exactly the acked records.
        let tail = QuorumTail::open_with_config(cfg(&addrs, 5000)).unwrap();
        let replayed = tail.replay().unwrap();
        assert_eq!(replayed.len(), 1);
        assert_eq!(
            replayed[0]
                .frames
                .iter()
                .map(|(s, _)| *s)
                .collect::<Vec<_>>(),
            vec![1],
            "exactly the acked record survives the poisoned session"
        );
    }
}
