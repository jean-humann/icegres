//! The icegresd leader lease (P3 §2, `--lease-quorum`): N icegresd
//! instances, ONE leader — elected and fenced by the SAME icekeeperd
//! consensus machinery that already replicates the durable tail. No new
//! system, no new dependency: the lease is a tiny quorum log of its own
//! (three dedicated acceptor instances — same binary, own ports/data
//! dirs), and holding the lease IS holding the proposer election on it.
//!
//! * **Acquire** = [`Quorum::open`]: win a vote quorum with `term =
//!   max(seen) + 1`, recover the previous holder records, and commit them
//!   in the new term — when `open()` returns, this proposer provably owns
//!   the highest term at a quorum. A holder record (JSON `{holder_id,
//!   epoch_ms}`) is then appended and quorum-acked as positive proof.
//! * **Renew** = the same append every TTL/3: a quorum-acked heartbeat
//!   record proves no higher term exists at any quorum (an acceptor at a
//!   higher term answers the `AppendResp` fence instead of acking).
//! * **Expire** (standby side) = read-only polling: `Greeting { tail_id:
//!   None }` returns each acceptor's `(term, flush_lsn)` without touching
//!   its state. The leader's renews advance `flush_lsn`, so "term and
//!   flush frozen at a quorum for >= TTL" means the lease has lapsed.
//! * **Fence/takeover** = the standby's own `Quorum::open`: its election
//!   bumps the term at a quorum; the old leader's next renew hits the
//!   fence, its one internal re-election is rejected by the donor-history
//!   check (the log now carries a term it never owned), the tail poisons
//!   with "superseded by a newer server", and the old leader DEMOTES —
//!   stops routing clients, terminates its computes, re-enters standby
//!   with jittered backoff. Steal-back is impossible for the same reason
//!   the data tail already proves.
//!
//! **The lease log must be a DEDICATED acceptor trio.** One icekeeperd
//! process serves one log (the tail identity is adopted permanently);
//! proposing on the computes' data log would fence the writer. icegresd
//! refuses a `--lease-quorum` that shares an address with
//! `ICEGRES_TAIL_QUORUM` at boot — compared on RESOLVED socket
//! addresses, so plain host aliases (`localhost` vs `127.0.0.1`, a DNS
//! name vs its IPs) are caught too; an alias the resolver cannot see
//! through still fences, so keep the trios obviously disjoint.
//!
//! Split-brain honesty: the old leader learns it lost only at its next
//! renew (<= TTL/3 + the append timeout, which is pinned to ~TTL here).
//! In that window two icegresd can both ANSWER connections — and a
//! deposed-but-unaware leader can still SPAWN a writer (a demote racing
//! a (re)spawn: the leadership re-checks under the spawn lock shrink the
//! window to the spawn itself, but cannot close it without coupling the
//! data-tail election to the lease term). Data is safe regardless: the
//! two writers fence each other on the DATA tail (a fenced compute
//! errors, never acks), so no acked row is ever lost — but the fencing
//! can land on the NEW leader's writer, leaving it wedged-but-alive
//! (accepts TCP, can never ack). That is why icegresd defaults
//! `--health-check-ms` ON when the lease runs over a quorum data tail:
//! the health loop is the recovery route that replaces the wedged
//! writer. Worst case is one spurious failover cycle, never a
//! double-writer.
//!
//! Compiled into `icegresd` only (`#[path]` include, like the quorum tree
//! itself); nothing here touches the arrow/iceberg/datafusion stack.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{bail, Context as _, Result};
use tokio::sync::watch;
use tracing::{error, info, warn};

use crate::quorum::proposer::{Quorum, QuorumConfig, QUORUM, QUORUM_SIZE};
use crate::quorum::proto::{Conn, Message, RECORD_FRAME, RECORD_WATERMARK};

/// The one table key lease records live under.
pub(crate) const LEASE_TABLE_KEY: &str = "icegresd.lease";

/// Acceptors a lease quorum needs (re-exported so the CLI can validate
/// the flag before spawning the loop).
pub(crate) const LEASE_QUORUM_SIZE: usize = QUORUM_SIZE;

/// Append a fresh watermark record every this many renews so the
/// acceptors can GC the lease log's covered prefix (the log is tiny, but
/// unbounded growth is unbounded growth).
const WATERMARK_EVERY: u64 = 32;

/// Parse and validate `--lease-quorum`: exactly [`LEASE_QUORUM_SIZE`]
/// addresses, none of them shared with the computes' data quorum (the
/// `ICEGRES_TAIL_QUORUM` environment icegresd hands its computes). One
/// icekeeperd process serves ONE log — its tail identity is adopted
/// permanently — so running the lease election against the data trio
/// would bump the data log's term and FENCE the tail writer.
///
/// Overlap is checked on RESOLVED socket addresses, not spellings:
/// `localhost:7101` vs `127.0.0.1:7101` (or a DNS alias of the same
/// acceptor) is the same process, and an exact-string guard would wave
/// it through into an unbounded mutual-fencing flap. Best-effort by
/// nature — a spelling the resolver cannot see through (resolution
/// failure, or two names for one box that resolve differently) still
/// falls back to the string comparison, so keep the trios OBVIOUSLY
/// disjoint (docs/limitations.md).
pub(crate) fn parse_lease_addrs(spec: &str, data_quorum: Option<&str>) -> Result<Vec<String>> {
    let addrs: Vec<String> = spec
        .split(',')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    if addrs.len() != LEASE_QUORUM_SIZE {
        bail!(
            "--lease-quorum needs exactly {LEASE_QUORUM_SIZE} acceptor addresses, got {}",
            addrs.len()
        );
    }
    if let Some(data) = data_quorum {
        let data: Vec<&str> = data
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .collect();
        for a in &addrs {
            let a_resolved = resolve_socket_addrs(a);
            for d in &data {
                let aliased = d == a
                    || (!a_resolved.is_empty()
                        && resolve_socket_addrs(d)
                            .iter()
                            .any(|x| a_resolved.contains(x)));
                if aliased {
                    bail!(
                        "--lease-quorum address {a} also appears in ICEGRES_TAIL_QUORUM ({}) \
                         as {d} — the lease needs its OWN dedicated icekeeperd trio (same \
                         binary, different --port/--data-dir): one acceptor serves one log, \
                         and electing the lease on the computes' data log would FENCE the \
                         tail writer",
                        data.join(",")
                    );
                }
            }
        }
    }
    Ok(addrs)
}

/// Resolve one `host:port` spelling to its socket addresses (empty when
/// resolution fails — the caller then compares spellings only). Blocking
/// DNS, boot-time only.
fn resolve_socket_addrs(spec: &str) -> Vec<std::net::SocketAddr> {
    use std::net::ToSocketAddrs as _;
    spec.to_socket_addrs()
        .map(|it| it.collect())
        .unwrap_or_default()
}

#[derive(Clone)]
pub(crate) struct LeaseConfig {
    /// Exactly [`LEASE_QUORUM_SIZE`] dedicated lease-acceptor addresses.
    pub addrs: Vec<String>,
    /// Lease TTL: renew every TTL/3; standbys take over after the lease
    /// log sits frozen at a quorum for >= TTL. The quorum append timeout
    /// is pinned to ~TTL (>= the 1 s floor) so a wedged quorum demotes the
    /// leader within ~2 timeouts instead of the 10 s tail default.
    pub ttl: Duration,
    /// Written into every holder/renew record (diagnostics).
    pub holder_id: String,
}

impl LeaseConfig {
    fn renew_interval(&self) -> Duration {
        (self.ttl / 3).max(Duration::from_millis(100))
    }

    fn quorum_config(&self) -> QuorumConfig {
        let mut cfg = QuorumConfig::new(self.addrs.clone());
        // A lease loop must react in lease time, not tail time: the append
        // (renew) timeout tracks the TTL (proposer floor: 1 s), and the
        // connect/call bounds stay well under one renew interval.
        cfg.append_timeout = self.ttl.max(Duration::from_secs(1));
        cfg.connect_timeout = self.probe_timeout();
        cfg.call_timeout = self.probe_timeout().max(Duration::from_secs(1));
        cfg
    }

    /// Bound on one standby probe (connect or greeting round trip).
    fn probe_timeout(&self) -> Duration {
        (self.ttl / 6).clamp(Duration::from_millis(250), Duration::from_secs(2))
    }
}

// ---------------------------------------------------------------------------
// Expiry tracking (pure, unit-tested)
// ---------------------------------------------------------------------------

/// One acceptor's last observed lease-log position.
#[derive(Clone, Copy)]
struct Observed {
    term: u64,
    flush: u64,
    /// When `(term, flush)` last CHANGED (renews advance flush; elections
    /// advance term — either resets the clock).
    since: Instant,
    /// Whether the LAST probe reached it (an unreachable acceptor can
    /// never witness expiry — its state is unknown, not frozen).
    reachable: bool,
}

/// Decides when the lease has lapsed: a quorum of REACHABLE acceptors
/// whose `(term, flush_lsn)` has not moved for >= TTL. A virgin log (all
/// zeros everywhere) is takeover-ready immediately — first boot must not
/// wait a full TTL for a leader that never existed.
pub(crate) struct ExpiryTracker {
    ttl: Duration,
    seen: Vec<Option<Observed>>,
}

impl ExpiryTracker {
    pub fn new(n: usize, ttl: Duration) -> Self {
        ExpiryTracker {
            ttl,
            seen: vec![None; n],
        }
    }

    /// Record one successful probe of acceptor `idx`.
    pub fn observe(&mut self, idx: usize, term: u64, flush: u64, now: Instant) {
        match &mut self.seen[idx] {
            Some(o) if o.term == term && o.flush == flush => o.reachable = true,
            slot => {
                *slot = Some(Observed {
                    term,
                    flush,
                    since: now,
                    reachable: true,
                })
            }
        }
    }

    /// Record a failed probe of acceptor `idx`. The frozen-since clock is
    /// kept: if the acceptor comes back UNCHANGED, no renew happened
    /// through the outage and the elapsed time still counts.
    pub fn observe_unreachable(&mut self, idx: usize) {
        if let Some(o) = &mut self.seen[idx] {
            o.reachable = false;
        }
    }

    /// Lease lapsed: >= [`QUORUM`] reachable acceptors frozen for >= TTL.
    pub fn expired(&self, now: Instant) -> bool {
        self.seen
            .iter()
            .flatten()
            .filter(|o| o.reachable && now.duration_since(o.since) >= self.ttl)
            .count()
            >= QUORUM
    }

    /// Never-held lease: a quorum of reachable acceptors at `(term 0,
    /// flush 0)` — nothing was ever elected or written, so there is no TTL
    /// to respect.
    pub fn virgin(&self) -> bool {
        self.seen
            .iter()
            .flatten()
            .filter(|o| o.reachable && o.term == 0 && o.flush == 0)
            .count()
            >= QUORUM
    }

    /// Forget everything (used when re-entering standby after leading —
    /// stale frozen-since clocks must not trigger an instant re-takeover).
    pub fn reset(&mut self) {
        self.seen.iter_mut().for_each(|s| *s = None);
    }
}

// ---------------------------------------------------------------------------
// Probing, acquiring, renewing
// ---------------------------------------------------------------------------

/// One read-only poll round: greet every acceptor concurrently (a
/// `Greeting { tail_id: None }` only reads state — acceptor.rs answers
/// term + flush without disturbing anything) and feed the tracker.
async fn poll_acceptors(cfg: &LeaseConfig, tracker: &mut ExpiryTracker) {
    let timeout = cfg.probe_timeout();
    let probes = cfg.addrs.iter().map(|addr| {
        let addr = addr.clone();
        async move {
            let mut conn = Conn::connect(&addr, timeout).await.ok()?;
            match conn
                .call_timeout(&Message::Greeting { tail_id: None }, timeout)
                .await
            {
                Ok(Message::GreetingResp {
                    term, flush_lsn, ..
                }) => Some((term, flush_lsn)),
                _ => None,
            }
        }
    });
    let results = futures::future::join_all(probes).await;
    let now = Instant::now();
    for (idx, res) in results.into_iter().enumerate() {
        match res {
            Some((term, flush)) => tracker.observe(idx, term, flush, now),
            None => tracker.observe_unreachable(idx),
        }
    }
}

fn holder_body(holder_id: &str) -> Vec<u8> {
    serde_json::json!({
        "holder_id": holder_id,
        "epoch_ms": SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64,
    })
    .to_string()
    .into_bytes()
}

/// A held lease: the open proposer election plus the renew cursor.
pub(crate) struct HeldLease {
    quorum: Quorum,
    seq: u64,
    holder_id: String,
}

impl HeldLease {
    /// Acquire the lease: run the election ([`Quorum::open`] — returns
    /// only once the recovered suffix is quorum-durable IN THE NEW TERM,
    /// i.e. fenced), then append + quorum-ack the holder record: positive
    /// proof this instance holds the highest term at a quorum NOW.
    pub async fn acquire(cfg: &LeaseConfig) -> Result<HeldLease> {
        let (quorum, recovered) = Quorum::open(cfg.quorum_config())
            .await
            .context("lease election failed")?;
        let prev = recovered
            .iter()
            .rfind(|r| r.table_key == LEASE_TABLE_KEY && r.kind == RECORD_FRAME);
        if let Some(rec) = prev {
            let holder = serde_json::from_slice::<serde_json::Value>(&rec.body)
                .ok()
                .and_then(|v| {
                    v.get("holder_id")
                        .and_then(|h| h.as_str().map(String::from))
                })
                .unwrap_or_else(|| "(unparsed)".into());
            info!(previous_holder = %holder, seq = rec.seq, "taking over the icegresd lease");
        }
        let seq = recovered
            .iter()
            .filter(|r| r.table_key == LEASE_TABLE_KEY)
            .map(|r| r.seq)
            .max()
            .unwrap_or(0)
            + 1;
        let mut lease = HeldLease {
            quorum,
            seq,
            holder_id: cfg.holder_id.clone(),
        };
        lease
            .append_holder_record()
            .await
            .context("the lease holder record did not reach a quorum")?;
        Ok(lease)
    }

    /// Renew: one quorum-acked holder record. `Err` = DEMOTE (fenced by a
    /// newer leader, or the lease quorum is unreachable/wedged) — the
    /// proposer poisons itself in either case, so this lease object is
    /// spent; drop it and re-acquire from standby.
    pub async fn renew(&mut self) -> Result<()> {
        self.seq += 1;
        self.append_holder_record().await
    }

    async fn append_holder_record(&mut self) -> Result<()> {
        let body = holder_body(&self.holder_id);
        let end = self
            .quorum
            .submit(RECORD_FRAME, LEASE_TABLE_KEY, self.seq, &body)?;
        self.quorum.wait_commit(end).await?;
        // GC bookkeeping: every acked renew covers all earlier records;
        // a periodic watermark record lets the acceptors drop the prefix.
        let refresh = self.quorum.note_covered(LEASE_TABLE_KEY, self.seq);
        if refresh.is_some() || self.seq.is_multiple_of(WATERMARK_EVERY) {
            let wm_seq = refresh.map(|(_, s)| s).unwrap_or(self.seq).max(self.seq);
            let end = self
                .quorum
                .submit(RECORD_WATERMARK, LEASE_TABLE_KEY, wm_seq, &[])?;
            self.quorum.wait_commit(end).await?;
            let _ = self.quorum.nudge_horizon();
        }
        Ok(())
    }

    /// The renew cursor, for tests.
    #[cfg(test)]
    pub fn seq(&self) -> u64 {
        self.seq
    }
}

// ---------------------------------------------------------------------------
// The lease loop
// ---------------------------------------------------------------------------

/// Deterministic-enough jitter without a rand dependency: the sub-second
/// nanos of the wall clock, folded into `[0, spread)`.
fn jitter(spread: Duration) -> Duration {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .subsec_nanos() as u64;
    let spread_ms = spread.as_millis().max(1) as u64;
    Duration::from_millis(nanos % spread_ms)
}

/// The standby/leader state machine, driving `leader_tx` (the daemon's
/// leadership watch): standby polls read-only until the lease lapses,
/// acquires, renews every TTL/3, and DEMOTES (never exits — a flapping
/// quorum must not crash-loop every icegresd replica) on any renew
/// failure, backing off with jitter so N standbys cannot livelock the
/// election with tight `Quorum::open` retries.
pub(crate) async fn lease_loop(
    cfg: LeaseConfig,
    leader_tx: watch::Sender<bool>,
    mut shutdown: watch::Receiver<bool>,
) {
    let renew_every = cfg.renew_interval();
    let mut tracker = ExpiryTracker::new(cfg.addrs.len(), cfg.ttl);
    info!(
        lease_quorum = %cfg.addrs.join(","),
        ttl_ms = cfg.ttl.as_millis() as u64,
        renew_ms = renew_every.as_millis() as u64,
        holder_id = %cfg.holder_id,
        "leader lease enabled: STANDBY until the lease is acquired (clients are refused, \
         no computes spawn)"
    );
    loop {
        // ---- STANDBY: poll read-only until the lease lapses ----
        loop {
            if *shutdown.borrow() {
                return;
            }
            poll_acceptors(&cfg, &mut tracker).await;
            if tracker.virgin() || tracker.expired(Instant::now()) {
                break;
            }
            tokio::select! {
                _ = tokio::time::sleep(renew_every) => {}
                _ = shutdown.changed() => {}
            }
        }
        if *shutdown.borrow() {
            return;
        }
        // ---- ACQUIRE ----
        let mut held = match HeldLease::acquire(&cfg).await {
            Ok(held) => held,
            Err(e) => {
                // Lost the race to another standby (vote refused /
                // superseded) or the quorum dipped mid-election: back off
                // with jitter and watch again.
                warn!("lease takeover failed (another instance may have won): {e:#}");
                tracker.reset();
                tokio::select! {
                    _ = tokio::time::sleep(renew_every + jitter(cfg.ttl)) => {}
                    _ = shutdown.changed() => {}
                }
                continue;
            }
        };
        info!(holder_id = %cfg.holder_id, "lease ACQUIRED: this icegresd is the leader");
        let _ = leader_tx.send_replace(true);
        // ---- LEADER: renew every TTL/3 until fenced/stalled ----
        loop {
            tokio::select! {
                _ = tokio::time::sleep(renew_every) => {}
                _ = shutdown.changed() => {}
            }
            // Shutdown (SIGTERM — a drain/eviction, a rollout, a plain
            // stop): stop renewing IMMEDIATELY and exit. Under this
            // protocol silence IS the release — standbys watch only the
            // acceptors' (term, flush_lsn) freeze, so a farewell
            // "released" append would ADVANCE flush and reset their
            // expiry clocks, DELAYING takeover by a full TTL from the
            // farewell; saying nothing hands over within ~TTL of the
            // last renew (~1-2x TTL end to end with poll granularity
            // and the election — the number the chart docs quote for a
            // leader eviction).
            if *shutdown.borrow() {
                let _ = leader_tx.send_replace(false);
                return;
            }
            if let Err(e) = held.renew().await {
                error!(
                    "lease renew failed — DEMOTING (terminating computes, refusing new \
                     clients, re-entering standby): {e:#}"
                );
                break;
            }
        }
        let _ = leader_tx.send_replace(false);
        drop(held); // tears down the proposer tasks/connections
        tracker.reset();
        // Jittered backoff before standing by again: a demoted leader
        // re-campaigning instantly would fence the new leader right back.
        tokio::select! {
            _ = tokio::time::sleep(cfg.ttl + jitter(cfg.ttl)) => {}
            _ = shutdown.changed() => {}
        }
    }
}

// ---------------------------------------------------------------------------
// Unit tests: the expiry tracker (pure) and the acquire/renew/expire/fence
// ladder over REAL in-process acceptors (the same harness tail_quorum.rs
// uses — no external processes).
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use crate::quorum::acceptor;
    use std::path::{Path, PathBuf};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::mpsc as std_mpsc;
    use std::sync::Arc;

    static TEST_DIR_SEQ: AtomicU64 = AtomicU64::new(0);

    fn temp_dir(name: &str) -> PathBuf {
        let n = TEST_DIR_SEQ.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!(
            "icegresd-lease-test-{}-{}-{}",
            std::process::id(),
            name,
            n
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// One in-process lease acceptor (own thread + current-thread runtime).
    struct TestAcceptor {
        addr: String,
        shutdown: Option<tokio::sync::oneshot::Sender<()>>,
        thread: Option<std::thread::JoinHandle<()>>,
    }

    fn spawn_acceptor(dir: &Path, node_id: u64) -> TestAcceptor {
        let (addr_tx, addr_rx) = std_mpsc::channel();
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let dir2 = dir.to_path_buf();
        let thread = std::thread::Builder::new()
            .name("test-lease-keeper".into())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async move {
                    let a = acceptor::open_dir(&dir2, node_id).expect("open acceptor dir");
                    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
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
            shutdown: Some(shutdown_tx),
            thread: Some(thread),
        }
    }

    impl Drop for TestAcceptor {
        fn drop(&mut self) {
            if let Some(tx) = self.shutdown.take() {
                let _ = tx.send(());
            }
            if let Some(th) = self.thread.take() {
                th.join().expect("acceptor thread panicked");
            }
        }
    }

    fn spawn_cluster(name: &str) -> (Vec<TestAcceptor>, Vec<String>) {
        let acceptors: Vec<TestAcceptor> = (0..3)
            .map(|i| spawn_acceptor(&temp_dir(&format!("{name}-{i}")), i as u64))
            .collect();
        let addrs = acceptors.iter().map(|a| a.addr.clone()).collect();
        (acceptors, addrs)
    }

    fn cfg(addrs: &[String], ttl_ms: u64, holder: &str) -> LeaseConfig {
        LeaseConfig {
            addrs: addrs.to_vec(),
            ttl: Duration::from_millis(ttl_ms),
            holder_id: holder.to_string(),
        }
    }

    // -------------------- ExpiryTracker (pure) --------------------

    #[test]
    fn tracker_expires_only_after_a_frozen_ttl_at_a_quorum() {
        let ttl = Duration::from_secs(1);
        let mut t = ExpiryTracker::new(3, ttl);
        let t0 = Instant::now();
        // Fresh observations: not expired, and (term 3) not virgin.
        for i in 0..3 {
            t.observe(i, 3, 100, t0);
        }
        assert!(!t.virgin());
        assert!(!t.expired(t0 + Duration::from_millis(999)));
        // Frozen past the TTL at all three: expired.
        assert!(t.expired(t0 + Duration::from_millis(1000)));
        // A renew (flush advance) on two acceptors resets their clocks:
        // only one frozen witness remains — not a quorum.
        let t1 = t0 + Duration::from_millis(1500);
        t.observe(0, 3, 200, t1);
        t.observe(1, 3, 200, t1);
        assert!(!t.expired(t1 + Duration::from_millis(999)));
        assert!(t.expired(t1 + Duration::from_millis(1000)));
        // A term bump alone (competing election, no records) also resets.
        let t2 = t1 + Duration::from_millis(1200);
        t.observe(0, 4, 200, t2);
        t.observe(1, 4, 200, t2);
        assert!(!t.expired(t2 + Duration::from_millis(999)));
    }

    #[test]
    fn tracker_unreachable_acceptors_never_witness_expiry() {
        let ttl = Duration::from_secs(1);
        let mut t = ExpiryTracker::new(3, ttl);
        let t0 = Instant::now();
        for i in 0..3 {
            t.observe(i, 3, 100, t0);
        }
        // Two acceptors go dark: one reachable frozen witness is not a
        // quorum, no matter how long it sits.
        t.observe_unreachable(0);
        t.observe_unreachable(1);
        assert!(!t.expired(t0 + Duration::from_secs(60)));
        // One returns UNCHANGED: its frozen clock still counts (no renew
        // could have advanced flush through the outage) — quorum reached.
        t.observe(0, 3, 100, t0 + Duration::from_secs(60));
        assert!(t.expired(t0 + Duration::from_secs(60)));
    }

    #[test]
    fn tracker_virgin_log_is_takeover_ready_immediately() {
        let ttl = Duration::from_secs(3600); // TTL must not matter
        let mut t = ExpiryTracker::new(3, ttl);
        let t0 = Instant::now();
        t.observe(0, 0, 0, t0);
        assert!(!t.virgin(), "one acceptor is not a quorum");
        t.observe(1, 0, 0, t0);
        assert!(t.virgin());
        assert!(!t.expired(t0), "virgin, not expired — different predicates");
        // Any record ever written kills virginity.
        t.observe(1, 1, 40, t0);
        assert!(!t.virgin());
    }

    #[test]
    fn lease_addrs_are_validated_and_disjoint_from_the_data_quorum() {
        let ok = parse_lease_addrs("h1:1, h2:2 ,h3:3", None).unwrap();
        assert_eq!(ok, vec!["h1:1", "h2:2", "h3:3"]);
        assert!(
            parse_lease_addrs("h1:1,h2:2", None).is_err(),
            "two is not a trio"
        );
        let err = parse_lease_addrs("h1:1,h2:2,h3:3", Some("h9:9,h2:2,h8:8")).unwrap_err();
        assert!(
            format!("{err:#}").contains("FENCE the tail writer"),
            "sharing an acceptor with the data quorum must be refused loudly: {err:#}"
        );
        // Disjoint trios pass.
        parse_lease_addrs("h1:1,h2:2,h3:3", Some("h4:4,h5:5,h6:6")).unwrap();
        // Host ALIASES of the same trio must be caught too — the guard
        // compares RESOLVED socket addresses, not spellings (an exact
        // string check would wave `localhost` vs `127.0.0.1` through
        // into a mutual-fencing flap).
        let err = parse_lease_addrs(
            "localhost:7101,localhost:7102,localhost:7103",
            Some("127.0.0.1:7101,127.0.0.1:7102,127.0.0.1:7103"),
        )
        .unwrap_err();
        assert!(
            format!("{err:#}").contains("FENCE the tail writer"),
            "aliased spellings of a shared trio must be refused loudly: {err:#}"
        );
        // Same host, different ports = different acceptors: passes.
        parse_lease_addrs(
            "localhost:7201,localhost:7202,localhost:7203",
            Some("127.0.0.1:7101,127.0.0.1:7102,127.0.0.1:7103"),
        )
        .unwrap();
    }

    // ------------- acquire / renew / expire / fence (real acceptors) -----

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn lease_acquire_renew_expire_fence() {
        let (_acceptors, addrs) = spawn_cluster("ladder");
        let ttl_ms = 1200;
        let cfg_a = cfg(&addrs, ttl_ms, "holder-a");
        let cfg_b = cfg(&addrs, ttl_ms, "holder-b");

        // A standby watching a virgin log is takeover-ready at once.
        let mut tracker = ExpiryTracker::new(3, cfg_a.ttl);
        poll_acceptors(&cfg_a, &mut tracker).await;
        assert!(tracker.virgin(), "fresh acceptors must read as virgin");

        // ACQUIRE: A holds the lease; its holder record is quorum-acked.
        let mut lease_a = HeldLease::acquire(&cfg_a).await.expect("acquire A");
        assert_eq!(lease_a.seq(), 1);

        // RENEW: heartbeats advance flush at the acceptors, so a polling
        // standby never sees the log as expired while A renews on cadence.
        let mut standby = ExpiryTracker::new(3, cfg_b.ttl);
        let renew_every = cfg_a.renew_interval();
        let watch_started = Instant::now();
        while watch_started.elapsed() < Duration::from_millis(2 * ttl_ms) {
            lease_a.renew().await.expect("renew A");
            poll_acceptors(&cfg_b, &mut standby).await;
            assert!(
                !standby.expired(Instant::now()),
                "an actively-renewed lease must never read as expired"
            );
            assert!(!standby.virgin(), "a held lease is not virgin");
            tokio::time::sleep(renew_every).await;
        }

        // EXPIRE: A stops renewing (holds the object, appends nothing);
        // the standby sees the log frozen for >= TTL and only then expired.
        let frozen_from = Instant::now();
        loop {
            poll_acceptors(&cfg_b, &mut standby).await;
            if standby.expired(Instant::now()) {
                break;
            }
            assert!(
                frozen_from.elapsed() < Duration::from_millis(4 * ttl_ms),
                "expiry never observed after the leader stopped renewing"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        assert!(
            frozen_from.elapsed() >= Duration::from_millis(ttl_ms) - renew_every,
            "expiry must not fire before ~TTL of frozen observations \
             (elapsed {:?})",
            frozen_from.elapsed()
        );

        // TAKEOVER + FENCE: B acquires (higher term, recovers A's records,
        // sequences continue above them); A's next renew is fenced — its
        // internal re-election is rejected and the proposer poisons.
        let mut lease_b = HeldLease::acquire(&cfg_b).await.expect("acquire B");
        assert!(
            lease_b.seq() > lease_a.seq(),
            "B's holder record must sequence above A's renews \
             (B {} vs A {})",
            lease_b.seq(),
            lease_a.seq()
        );
        let err = lease_a.renew().await.expect_err("A must be fenced");
        assert!(
            format!("{err:#}").contains("superseded by a newer server"),
            "expected the fencing poison, got: {err:#}"
        );
        // No steal-back: A stays fenced (sticky poison), B keeps renewing.
        let err = lease_a.renew().await.expect_err("A stays fenced");
        assert!(
            format!("{err:#}").contains("POISONED") || format!("{err:#}").contains("superseded"),
            "got: {err:#}"
        );
        lease_b.renew().await.expect("B renews after fencing A");
    }

    // Two concurrent campaigners on the same lapsed lease: exactly one
    // ends up leader; the loser's acquire (or first renew) fails.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_takeover_elects_exactly_one_leader() {
        let (_acceptors, addrs) = spawn_cluster("race");
        let cfg_a = cfg(&addrs, 1000, "racer-a");
        let cfg_b = cfg(&addrs, 1000, "racer-b");
        let (a, b) = tokio::join!(HeldLease::acquire(&cfg_a), HeldLease::acquire(&cfg_b));
        let mut winners = 0;
        for mut held in [a, b].into_iter().flatten() {
            // A racer that survives a quorum-acked renew is a real
            // leader; a fenced one errors here.
            if held.renew().await.is_ok() {
                winners += 1;
            }
        }
        assert_eq!(
            winners, 1,
            "exactly one concurrent campaigner may hold the lease"
        );
    }
}
