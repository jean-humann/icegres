//! Bounded-staleness read freshness (`--freshness-ms N`, opt-in).
//!
//! Default mode (`N = 0`) keeps today's exact-freshness contract
//! byte-identical: every scan performs one catalog `load_table` round trip
//! (~2-3 ms against local Lakekeeper — the single largest line item of the
//! read hot path, measured by the `ICEGRES_QUERY_TIMING` breakdown as ~80%
//! of physical-planning time) purely to detect snapshot changes.
//!
//! With `N > 0` the trade is made explicit and bounded instead:
//!
//! * ONE background task per server (not per table-provider) polls the
//!   catalog for every mounted table each `N` ms and swaps the cached
//!   provider in `cache.rs` when the metadata location moved. Scans serve
//!   the cached snapshot with NO catalog round trip.
//! * **Read-your-own-writes stays exact**: every LOCAL write path that
//!   commits — synchronous copy-on-write DML, PK-enforced INSERT,
//!   transaction COMMIT (both the atomic multi-table endpoint and the
//!   per-table fallback), the write buffer's group-commit flush, and plain
//!   `INSERT` through the DataFusion insert path — synchronously
//!   invalidates the touched table ([`invalidate_key`], called from the
//!   `overwrite.rs` commit chokepoints and from `cache.rs`'s insert
//!   wrapper), so the NEXT read on this server performs a synchronous
//!   catalog load and observes the write immediately. Buffered/keyed writes
//!   are additionally readable pre-commit through the buffer overlay
//!   (buffer.rs), which is taken per-scan and never cached.
//! * **Foreign writers** (other icegres servers, Spark, anything committing
//!   through the catalog) become visible within ~`N` ms plus one refresh
//!   round trip — bounded staleness instead of exact freshness. The bound is
//!   per table and real: each pass refreshes tables CONCURRENTLY (up to
//!   [`MAX_CONCURRENT_REFRESHES`] in flight) and each table's refresh is a
//!   single retry-free load bounded by [`refresh_timeout`] (min(4·N, 2 s);
//!   the next pass is the retry), so one slow or stalled table delays only
//!   itself — never the other tables' freshness.
//! * **Catalog outage honesty**: the refresher keeps serving the last
//!   refreshed snapshot, WARNs (rate-limited) with the staleness age, and
//!   exports the worst-case age as the `icegres_freshness_age_ms` gauge on
//!   `GET /metrics` (metrics.rs). The gauge is sampled at each pass START —
//!   the worst case a read could have observed just before that pass
//!   refreshed — so a healthy gauge reads ≈ the configured interval, and it
//!   keeps GROWING through an outage. Reads never start failing just because
//!   the poll loop cannot reach the catalog (opt out with
//!   `ICEGRES_STALE_READ_ON_CATALOG_ERROR=0` — see cache.rs).
//! * **Refresher supervision**: the refresher task runs under a supervisor
//!   ([`spawn_refresher`]) that logs an ERROR and respawns it (budgeted per
//!   minute) if it ever dies, and whose watchdog keeps bumping the age gauge
//!   from the last pass-start timestamp — a dead refresher shows a growing
//!   age, never a frozen healthy-looking one.
//!
//! # The write/refresh race (why generations, not a boolean)
//!
//! A plain `stale` flag is racy: a refresher poll that loaded PRE-commit
//! metadata but completed AFTER a local write's invalidation would clear
//! the flag and serve the pre-write snapshot — violating read-your-own-
//! writes. [`TableFreshness`] therefore uses two monotonic generations:
//! `write_gen` (bumped by every invalidation) and `fresh_gen` (advanced to
//! the *pre-load* `write_gen` when a load completes). The cache is fresh
//! only while `fresh_gen == write_gen`, so a load that overlapped ANY
//! invalidation can never mark the cache fresh, no matter how the
//! completions interleave. The same generation guards the cached-provider
//! install in `cache.rs` (a slow pre-commit load must not clobber a newer
//! snapshot).

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock, PoisonError, RwLock, Weak};
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use futures::StreamExt;
use iceberg::TableIdent;
use tracing::{debug, error, info, warn};

use crate::cache::CachingTableProvider;

/// Recover a poisoned lock instead of panicking. The freshness registry and
/// per-table state are shared between scans, write paths, and the background
/// refresher: a panic elsewhere while one of these locks was held must
/// degrade to recover-and-continue — the state is generation-guarded, so the
/// worst case after an unwind is one extra synchronous catalog load — and
/// must never kill the refresher (which would freeze staleness silently).
/// Logs an ERROR, rate-limited so a poisoned lock touched every pass does
/// not flood.
pub(crate) fn recover<G>(what: &'static str, result: Result<G, PoisonError<G>>) -> G {
    result.unwrap_or_else(|poisoned| {
        note_poisoned(what);
        poisoned.into_inner()
    })
}

fn note_poisoned(what: &'static str) {
    static START: OnceLock<Instant> = OnceLock::new();
    static LAST_LOG_MS: AtomicU64 = AtomicU64::new(u64::MAX);
    let now_ms = START.get_or_init(Instant::now).elapsed().as_millis() as u64;
    let last = LAST_LOG_MS.load(Ordering::Relaxed);
    if last == u64::MAX || now_ms.saturating_sub(last) >= OUTAGE_WARN_EVERY.as_millis() as u64 {
        LAST_LOG_MS.store(now_ms, Ordering::Relaxed);
        error!(
            lock = what,
            "poisoned lock recovered (a task panicked while holding it); continuing — \
             freshness state is generation-guarded, worst case is one extra catalog load"
        );
    }
}

/// Per-table freshness state machine (see the module docs for the race the
/// generation pair closes).
pub struct TableFreshness {
    /// Bumped by every invalidation (local write, DDL fence). Starts at 1 so
    /// a fresh instance (fresh_gen = 0) is born stale.
    write_gen: AtomicU64,
    /// The `write_gen` observed *before* the most recent successfully
    /// completed catalog load. Fresh iff equal to `write_gen`.
    fresh_gen: AtomicU64,
    /// Instant of the last successful catalog load (staleness gauge input).
    last_ok: Mutex<Option<Instant>>,
    created: Instant,
}

impl Default for TableFreshness {
    fn default() -> Self {
        Self::new()
    }
}

impl TableFreshness {
    pub fn new() -> Self {
        Self {
            write_gen: AtomicU64::new(1),
            fresh_gen: AtomicU64::new(0),
            last_ok: Mutex::new(None),
            created: Instant::now(),
        }
    }

    /// Whether scans may serve the cached provider without a catalog check.
    pub fn is_fresh(&self) -> bool {
        self.fresh_gen.load(Ordering::Acquire) == self.write_gen.load(Ordering::Acquire)
    }

    /// Record the point-in-time a catalog load STARTS. The returned token
    /// must be passed to [`complete_load`](Self::complete_load).
    pub fn begin_load(&self) -> u64 {
        self.write_gen.load(Ordering::Acquire)
    }

    /// Record a successfully completed catalog load that began at `token`.
    /// Marks the cache fresh ONLY if no invalidation raced the load
    /// (`fetch_max`: a stale straggler can never regress a newer load).
    pub fn complete_load(&self, token: u64) {
        self.fresh_gen.fetch_max(token, Ordering::AcqRel);
        *recover("freshness last_ok", self.last_ok.lock()) = Some(Instant::now());
    }

    /// A local write committed (or MAY have committed — ambiguous outcomes
    /// invalidate too): the next scan must load from the catalog.
    pub fn invalidate(&self) {
        self.write_gen.fetch_add(1, Ordering::AcqRel);
    }

    /// Time since the last successful catalog load (since construction when
    /// none has completed yet).
    pub fn age(&self) -> Duration {
        match *recover("freshness last_ok", self.last_ok.lock()) {
            Some(t) => t.elapsed(),
            None => self.created.elapsed(),
        }
    }
}

/// One registered table: its freshness state plus a weak handle to the
/// caching provider the refresher polls/swaps.
struct Registered {
    freshness: Arc<TableFreshness>,
    provider: Weak<CachingTableProvider>,
}

/// Process-global registry of freshness-managed tables, keyed by
/// [`table_key`]. Populated ONLY when `--freshness-ms > 0` built the session
/// context, so in default mode [`invalidate_key`] is a single `OnceLock`
/// load and a no-op — the write paths carry zero extra work.
static REGISTRY: OnceLock<RwLock<HashMap<String, Registered>>> = OnceLock::new();

/// Registry key for `ident`. Must agree with [`commit_key`]: the namespace
/// part is the REST URL form (`to_url_string`), which is what the commit
/// chokepoints in overwrite.rs carry.
pub fn table_key(ident: &TableIdent) -> String {
    commit_key(&ident.namespace().to_url_string(), ident.name())
}

/// Registry key from the raw `(namespace-url, table)` pair a REST commit
/// POST addresses.
pub fn commit_key(namespace_url: &str, table: &str) -> String {
    format!("{namespace_url}\u{1f}{table}")
}

/// Register a freshness-managed table (called by `cache.rs` when the
/// session context is built with freshness enabled). Re-registering a key
/// (table dropped and re-created) replaces the entry.
pub fn register(key: String, freshness: Arc<TableFreshness>, provider: &Arc<CachingTableProvider>) {
    recover(
        "freshness registry",
        REGISTRY.get_or_init(|| RwLock::new(HashMap::new())).write(),
    )
    .insert(
        key,
        Registered {
            freshness,
            provider: Arc::downgrade(provider),
        },
    );
}

/// Drop a table from the registry (DDL fence: `deregister_table`). The
/// entry is invalidated first so any straggling plan-cache entry or cached
/// provider can never be served fresh again.
pub fn deregister(key: &str) {
    let Some(reg) = REGISTRY.get() else { return };
    if let Some(entry) = recover("freshness registry", reg.write()).remove(key) {
        entry.freshness.invalidate();
    }
}

/// Synchronously invalidate a table's cached freshness after a local commit
/// attempt. No-op (one atomic load) when freshness mode is off or the table
/// is unknown.
pub fn invalidate_key(key: &str) {
    let Some(reg) = REGISTRY.get() else { return };
    if let Some(entry) = recover("freshness registry", reg.read()).get(key) {
        entry.freshness.invalidate();
    }
}

/// Look up the live caching provider registered under `key` (plan-cache
/// entry validation). `None` when freshness mode is off, the table was
/// deregistered, or its provider was dropped.
pub fn provider(key: &str) -> Option<Arc<CachingTableProvider>> {
    recover("freshness registry", REGISTRY.get()?.read())
        .get(key)?
        .provider
        .upgrade()
}

/// Snapshot the registry for one refresher pass.
fn snapshot() -> Vec<(String, Weak<CachingTableProvider>, Arc<TableFreshness>)> {
    match REGISTRY.get() {
        Some(reg) => recover("freshness registry", reg.read())
            .iter()
            .map(|(k, r)| (k.clone(), r.provider.clone(), r.freshness.clone()))
            .collect(),
        None => Vec::new(),
    }
}

/// Remove registry entries whose provider was dropped (table deregistered
/// or context torn down).
fn prune(dead: &[String]) {
    if dead.is_empty() {
        return;
    }
    if let Some(reg) = REGISTRY.get() {
        let mut guard = recover("freshness registry", reg.write());
        for key in dead {
            // Only prune if still dead — the table may have been re-created
            // (and re-registered) since the snapshot was taken.
            if guard
                .get(key)
                .is_some_and(|r| r.provider.upgrade().is_none())
            {
                guard.remove(key);
            }
        }
    }
}

/// Minimum interval between catalog-outage WARNs from the refresher (the
/// poll cadence is milliseconds; a WARN per failed poll would flood logs).
const OUTAGE_WARN_EVERY: Duration = Duration::from_secs(10);

/// Maximum per-table refreshes in flight per refresher pass: refreshes run
/// concurrently, so a hung table occupies ONE slot (for at most its
/// per-table timeout) while the remaining slots keep every other table
/// fresh — one slow table delays only itself.
const MAX_CONCURRENT_REFRESHES: usize = 8;

/// Supervisor watchdog cadence: how often the age gauge is bumped from the
/// last pass-start timestamp while the refresher itself is dead or stalled.
const WATCHDOG_TICK: Duration = Duration::from_secs(1);

/// Cap on refresher respawns per rolling minute: beyond it the supervisor
/// stops respawning until the window rolls over, keeps logging errors, and
/// keeps the age gauge growing — loud, never a tight crash loop and never
/// silent staleness.
const MAX_RESPAWNS_PER_MINUTE: u32 = 5;

/// Delay between a refresher death and its respawn.
const RESPAWN_BACKOFF: Duration = Duration::from_millis(200);

/// Refresher-specific per-table load timeout: `min(4 * interval, 2 s)`,
/// retry-free — the NEXT PASS is the retry — and deliberately decoupled
/// from the scan path's `ICEGRES_CATALOG_TIMEOUT_MS`/`ICEGRES_CATALOG_RETRIES`
/// (whose worst case, timeout × retries with backoff, is ~15 s; paying that
/// per table per pass is what used to let one stalled table drag every
/// table's staleness far past the promised bound).
fn refresh_timeout(interval: Duration) -> Duration {
    interval.saturating_mul(4).min(Duration::from_millis(2000))
}

/// Instant of the most recent refresher pass START. Written by the refresher
/// at the top of every pass; read by the supervisor's watchdog, which uses
/// `elapsed()` as a floor on every table's staleness — so a dead or stalled
/// refresher shows a GROWING `icegres_freshness_age_ms`, never a frozen
/// healthy-looking value.
fn last_pass_start() -> &'static Mutex<Option<Instant>> {
    static T: OnceLock<Mutex<Option<Instant>>> = OnceLock::new();
    T.get_or_init(|| Mutex::new(None))
}

/// Watchdog gauge bump: the time since the last pass start is a lower bound
/// on every freshness-managed table's staleness (no pass since then can
/// have refreshed anything), so `fetch_max` keeps the exported age honest —
/// and growing — while the refresher is dead. Healthy passes overwrite it
/// with the exact per-table maximum.
fn bump_gauge_from_pass_start() {
    if let Some(started) = *recover("freshness pass-start", last_pass_start().lock()) {
        crate::metrics::metrics()
            .freshness_age_ms
            .fetch_max(started.elapsed().as_millis() as u64, Ordering::Relaxed);
    }
}

/// Spawn the per-server background refresher under a supervisor.
///
/// The refresher walks the mounted table set every `interval` with bounded
/// parallelism ([`MAX_CONCURRENT_REFRESHES`] loads in flight; each load is
/// retry-free and bounded by [`refresh_timeout`], so a slow table delays
/// only itself), exports the worst-case staleness age — sampled at pass
/// START, see [`refresher_loop`] — as the `icegres_freshness_age_ms` gauge,
/// and WARNs (rate-limited) when the catalog is unreachable while reads keep
/// serving the last refreshed snapshots.
///
/// The supervisor ([`supervise`]) awaits the refresher's `JoinHandle`: if
/// the task ever ends (a panic — its own locks recover from poisoning, but
/// a dependency can still panic), it logs an ERROR and respawns it with a
/// per-minute budget, and its watchdog keeps the age gauge growing from the
/// last pass-start timestamp so a dead refresher is visible on `/metrics`,
/// not frozen at a healthy value.
pub fn spawn_refresher(interval: Duration) -> tokio::task::JoinHandle<()> {
    tokio::spawn(supervise(move || tokio::spawn(refresher_loop(interval))))
}

/// Supervisor for the refresher task (see [`spawn_refresher`]). Generic over
/// the spawn closure so the respawn/backoff policy is unit-testable.
async fn supervise<F>(mut spawn_loop: F)
where
    F: FnMut() -> tokio::task::JoinHandle<()>,
{
    const WINDOW: Duration = Duration::from_secs(60);
    // tokio Instants (not std) so the paused-clock unit test can drive the
    // respawn window deterministically; identical semantics in production.
    let mut window_started = tokio::time::Instant::now();
    let mut respawns_in_window: u32 = 0;
    loop {
        let mut refresher = spawn_loop();
        // Watchdog: while the refresher runs (or hangs), keep the staleness
        // gauge honest from the pass-start timestamp.
        let outcome = loop {
            tokio::select! {
                res = &mut refresher => break res,
                _ = tokio::time::sleep(WATCHDOG_TICK) => bump_gauge_from_pass_start(),
            }
        };
        // The refresher loop is infinite: ending AT ALL is a defect —
        // normally a panic surfacing as a JoinError.
        match outcome {
            Err(e) => error!(
                error = %e,
                "freshness refresher DIED; freshness-managed tables are not being \
                 refreshed (the icegres_freshness_age_ms gauge keeps growing); respawning"
            ),
            Ok(()) => error!("freshness refresher loop returned unexpectedly; respawning"),
        }
        if window_started.elapsed() >= WINDOW {
            window_started = tokio::time::Instant::now();
            respawns_in_window = 0;
        }
        respawns_in_window += 1;
        if respawns_in_window > MAX_RESPAWNS_PER_MINUTE {
            // Respawn budget exhausted: something kills the refresher on
            // every start. Stop the crash loop, keep ERRORING and keep the
            // gauge growing until the minute window rolls over, then try
            // again — loud and self-healing, never silent staleness.
            let mut last_err = tokio::time::Instant::now();
            while window_started.elapsed() < WINDOW {
                tokio::time::sleep(WATCHDOG_TICK).await;
                bump_gauge_from_pass_start();
                if last_err.elapsed() >= OUTAGE_WARN_EVERY {
                    error!(
                        respawns = respawns_in_window,
                        "freshness refresher keeps dying; respawn budget exhausted for \
                         this minute — staleness is growing (icegres_freshness_age_ms)"
                    );
                    last_err = tokio::time::Instant::now();
                }
            }
            window_started = tokio::time::Instant::now();
            respawns_in_window = 0;
        } else {
            tokio::time::sleep(RESPAWN_BACKOFF).await;
        }
    }
}

/// One refresher pass every `interval`: snapshot the registry, refresh every
/// live provider with bounded parallelism and a per-table timeout, prune
/// dead entries, export the worst-case staleness age, and WARN (rate-
/// limited) on catalog failures.
async fn refresher_loop(interval: Duration) {
    info!(
        interval_ms = interval.as_millis() as u64,
        per_table_timeout_ms = refresh_timeout(interval).as_millis() as u64,
        max_in_flight = MAX_CONCURRENT_REFRESHES,
        "freshness refresher started"
    );
    let mut last_warn: Option<Instant> = None;
    loop {
        let pass_started = Instant::now();
        *recover("freshness pass-start", last_pass_start().lock()) = Some(pass_started);
        let timeout = refresh_timeout(interval);
        let mut dead: Vec<String> = Vec::new();
        let mut max_age_ms = 0u64;
        let mut tasks: Vec<RefreshTask> = Vec::new();
        for (key, weak, freshness) in snapshot() {
            let Some(provider) = weak.upgrade() else {
                dead.push(key);
                continue;
            };
            // Gauge input, sampled at pass START: the age a read could have
            // observed just BEFORE this pass refreshes — the honest worst
            // case, not the near-zero sawtooth minimum measured right after
            // a refresh. A healthy gauge therefore reads ≈ `interval`, and
            // failed loads make it grow monotonically pass over pass.
            max_age_ms = max_age_ms.max(freshness.age().as_millis() as u64);
            tasks.push(Box::pin(async move {
                let result = match tokio::time::timeout(timeout, provider.refresh()).await {
                    Ok(Ok(())) => Ok(()),
                    Ok(Err(e)) => Err(e.to_string()),
                    Err(_) => Err(format!(
                        "refresh timed out after {} ms (per-table refresher timeout; \
                         the next pass retries)",
                        timeout.as_millis()
                    )),
                };
                (key, result)
            }));
        }
        prune(&dead);
        crate::metrics::metrics()
            .freshness_age_ms
            .store(max_age_ms, Ordering::Relaxed);
        let (failures, last_error) = refresh_all(tasks, MAX_CONCURRENT_REFRESHES).await;
        if failures > 0 && last_warn.is_none_or(|t| t.elapsed() >= OUTAGE_WARN_EVERY) {
            warn!(
                failed_tables = failures,
                max_staleness_ms = max_age_ms,
                error = last_error.as_deref().unwrap_or("unknown"),
                "catalog unreachable from the freshness refresher; reads keep serving \
                 the last refreshed snapshot (bounded-stale) — see the \
                 icegres_freshness_age_ms gauge"
            );
            last_warn = Some(Instant::now());
        }
        tokio::time::sleep(interval.saturating_sub(pass_started.elapsed())).await;
    }
}

/// One table's refresh work for a pass: resolves to the table key and the
/// refresh outcome (boxed so the pass can mix tables — and so tests can
/// substitute mock work).
type RefreshTask = BoxFuture<'static, (String, Result<(), String>)>;

/// Drive per-table refresh futures with bounded parallelism
/// (`buffer_unordered`): a stalled table occupies ONE of `parallelism`
/// slots — up to its own timeout, applied by the caller inside the future —
/// while every other table keeps refreshing. Returns the failure count and
/// a sample error for the pass-level WARN.
async fn refresh_all(tasks: Vec<RefreshTask>, parallelism: usize) -> (usize, Option<String>) {
    let mut failures = 0usize;
    let mut last_error: Option<String> = None;
    let mut stream = futures::stream::iter(tasks).buffer_unordered(parallelism.max(1));
    while let Some((key, result)) = stream.next().await {
        if let Err(e) = result {
            failures += 1;
            debug!(table = %key, error = %e, "freshness refresh failed");
            last_error = Some(e);
        }
    }
    (failures, last_error)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn born_stale_then_fresh_after_clean_load() {
        let f = TableFreshness::new();
        assert!(
            !f.is_fresh(),
            "a new table must start stale (first scan syncs)"
        );
        let token = f.begin_load();
        f.complete_load(token);
        assert!(f.is_fresh());
    }

    #[test]
    fn invalidation_marks_stale_until_next_clean_load() {
        let f = TableFreshness::new();
        f.complete_load(f.begin_load());
        f.invalidate();
        assert!(
            !f.is_fresh(),
            "a local write must force the next scan to sync"
        );
        f.complete_load(f.begin_load());
        assert!(f.is_fresh());
    }

    #[test]
    fn load_overlapping_an_invalidation_never_marks_fresh() {
        // The refresher race from the module docs: load begins, a local
        // write commits + invalidates, THEN the (pre-commit) load completes.
        let f = TableFreshness::new();
        let token = f.begin_load();
        f.invalidate();
        f.complete_load(token);
        assert!(
            !f.is_fresh(),
            "a load that began before an invalidation must not clear it"
        );
        // A load that began after the invalidation does clear it.
        f.complete_load(f.begin_load());
        assert!(f.is_fresh());
    }

    #[test]
    fn stale_straggler_cannot_regress_a_newer_load() {
        let f = TableFreshness::new();
        let old = f.begin_load(); // slow load A starts
        f.invalidate(); // write commits
        let new = f.begin_load(); // load B starts (post-commit)
        f.complete_load(new); // B completes: fresh
        assert!(f.is_fresh());
        f.complete_load(old); // straggler A completes with pre-commit data
        assert!(
            f.is_fresh(),
            "fetch_max: an old completion must not flip freshness off"
        );
        assert_eq!(f.fresh_gen.load(Ordering::Acquire), new);
    }

    #[test]
    fn invalidate_key_without_registry_is_a_noop() {
        // The N=0 identical path: freshness mode never initialized the
        // registry from THIS test's keyspace — invalidation from the write
        // chokepoints must be a cheap no-op, never a panic or an insert.
        invalidate_key("nonexistent\u{1f}table_n0");
        deregister("nonexistent\u{1f}table_n0");
        assert!(provider("nonexistent\u{1f}table_n0").is_none());
    }

    #[test]
    fn commit_and_ident_keys_agree() {
        let ident = TableIdent::from_strs(["demo", "trips"]).unwrap();
        assert_eq!(table_key(&ident), commit_key("demo", "trips"));
    }

    #[test]
    fn refresher_timeout_is_short_retry_free_and_capped() {
        // min(4·N, 2000 ms): decoupled from the scan path's timeout×retries.
        assert_eq!(
            refresh_timeout(Duration::from_millis(25)),
            Duration::from_millis(100)
        );
        assert_eq!(
            refresh_timeout(Duration::from_millis(500)),
            Duration::from_millis(2000)
        );
        assert_eq!(
            refresh_timeout(Duration::from_secs(60)),
            Duration::from_millis(2000)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_blocked_table_does_not_delay_other_refreshes() {
        // F1 slot isolation: one table's refresh hangs (holding one
        // buffer_unordered slot open until we release it); the other
        // table's refresh must complete regardless — a slow table delays
        // only itself, never the rest of the mounted set.
        let fast_done = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let fast_flag = fast_done.clone();
        let tasks: Vec<RefreshTask> = vec![
            Box::pin(async move {
                let _ = release_rx.await;
                (
                    "blocked".to_string(),
                    Err("refresh timed out after 100 ms".to_string()),
                )
            }),
            Box::pin(async move {
                fast_flag.store(true, Ordering::SeqCst);
                ("fast".to_string(), Ok(()))
            }),
        ];
        let pass = tokio::spawn(refresh_all(tasks, MAX_CONCURRENT_REFRESHES));
        for _ in 0..400 {
            if fast_done.load(Ordering::SeqCst) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        assert!(
            fast_done.load(Ordering::SeqCst),
            "the fast table's refresh must complete while another slot is blocked"
        );
        assert!(
            !pass.is_finished(),
            "the pass is still (correctly) waiting on the blocked slot"
        );
        release_tx.send(()).unwrap();
        let (failures, last_error) = pass.await.unwrap();
        assert_eq!(failures, 1, "only the blocked table failed");
        assert!(last_error.unwrap().contains("timed out"));
    }

    #[tokio::test(start_paused = true)]
    async fn supervisor_respawns_a_dead_refresher_with_a_capped_budget() {
        // F3: the refresher loop never returns in production, so a loop
        // that ends immediately models a task dying on every start. The
        // supervisor must respawn it (self-healing) but within the
        // per-minute budget (no tight crash loop) and must itself never
        // give up.
        let spawns = Arc::new(std::sync::atomic::AtomicU32::new(0));
        let counter = spawns.clone();
        let sup = tokio::spawn(supervise(move || {
            counter.fetch_add(1, Ordering::SeqCst);
            tokio::spawn(async {})
        }));
        // Two full respawn windows of virtual time (the paused clock
        // auto-advances through every sleep).
        tokio::time::sleep(Duration::from_secs(125)).await;
        let n = spawns.load(Ordering::SeqCst);
        assert!(
            n >= 2,
            "the refresher must be respawned after dying (got {n})"
        );
        assert!(
            n <= 3 * (MAX_RESPAWNS_PER_MINUTE + 1),
            "respawns must be capped per minute, not a tight loop (got {n})"
        );
        assert!(!sup.is_finished(), "the supervisor never gives up");
        sup.abort();
    }

    // -----------------------------------------------------------------
    // LIVE test against the local lakehouse stack (Lakekeeper + RustFS),
    // gated on ICEGRES_LIVE_TESTS=1 — the same pattern as tail_pg.rs's
    // ICEGRES_TEST_PG_URL gate. Proves the three freshness-mode contracts
    // end to end on real catalog metadata: (1) the fast path really skips
    // the catalog (foreign commits are invisible until a refresh — bounded
    // staleness), (2) `refresh()` swaps the cached provider on metadata
    // change, (3) a local write invalidates synchronously, so
    // read-your-own-writes stays exact without any refresh.
    // -----------------------------------------------------------------

    fn live_opts() -> crate::CatalogOpts {
        let env = |k: &str, d: &str| std::env::var(k).unwrap_or_else(|_| d.to_string());
        crate::CatalogOpts {
            catalog_uri: env("ICEGRES_CATALOG_URI", "http://127.0.0.1:8181/catalog"),
            warehouse: env("ICEGRES_WAREHOUSE", "lakehouse"),
            s3_endpoint: env("ICEGRES_S3_ENDPOINT", "http://127.0.0.1:9000"),
            s3_access_key: env("ICEGRES_S3_ACCESS_KEY", "rustfsadmin"),
            s3_secret_key: env("ICEGRES_S3_SECRET_KEY", "rustfssecret"),
            s3_region: env("ICEGRES_S3_REGION", "us-east-1"),
        }
    }

    async fn count(ctx: &datafusion::prelude::SessionContext, table: &str) -> i64 {
        let batches = ctx
            .sql(&format!("select count(*) from demo.{table}"))
            .await
            .expect("count plan")
            .collect()
            .await
            .expect("count exec");
        use datafusion::arrow::array::AsArray;
        use datafusion::arrow::datatypes::Int64Type;
        batches[0].column(0).as_primitive::<Int64Type>().value(0)
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn live_refresh_swaps_provider_and_local_writes_stay_exact() {
        if std::env::var("ICEGRES_LIVE_TESTS").as_deref() != Ok("1") {
            eprintln!(
                "skipping: ICEGRES_LIVE_TESTS unset (live freshness test; needs the local \
                 Lakekeeper/RustFS stack)"
            );
            return;
        }
        let opts = live_opts();
        let catalog = crate::context::connect_catalog(&opts)
            .await
            .expect("catalog");
        let ns = iceberg::NamespaceIdent::new("demo".to_string());
        if !catalog.namespace_exists(&ns).await.expect("ns check") {
            catalog
                .create_namespace(&ns, std::collections::HashMap::new())
                .await
                .expect("create ns");
        }
        let table = format!(
            "freshness_ut_{}_{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_millis()
        );
        let schema = iceberg::spec::Schema::builder()
            .with_fields(vec![Arc::new(iceberg::spec::NestedField::required(
                1,
                "id",
                iceberg::spec::Type::Primitive(iceberg::spec::PrimitiveType::Long),
            ))])
            .build()
            .expect("schema");
        let creation = iceberg::TableCreation::builder()
            .name(table.clone())
            .schema(schema)
            .build();
        catalog.create_table(&ns, creation).await.expect("create");
        let ident = TableIdent::new(ns.clone(), table.clone());

        // Contexts AFTER table creation (providers snapshot the table list).
        // `fresh_ctx` runs in freshness mode (interval irrelevant: no
        // refresher task is spawned — refreshes are driven explicitly);
        // `foreign_ctx` is a default-mode writer standing in for another
        // server / external Iceberg writer.
        let fresh_ctx = crate::context::build_session_context_with(
            catalog.clone(),
            Some(1),
            None,
            None,
            60_000,
        )
        .await
        .expect("fresh ctx");
        let foreign_ctx =
            crate::context::build_session_context_with(catalog.clone(), Some(1), None, None, 0)
                .await
                .expect("foreign ctx");

        // First scan loads synchronously (born stale) and marks fresh.
        assert_eq!(count(&fresh_ctx, &table).await, 0);

        // Foreign commit: invisible to the fast path until a refresh —
        // proof the fast path really skips the per-scan catalog check.
        foreign_ctx
            .sql(&format!("insert into demo.{table} values (1)"))
            .await
            .expect("foreign insert plan")
            .collect()
            .await
            .expect("foreign insert exec");
        assert_eq!(
            count(&fresh_ctx, &table).await,
            0,
            "fast path must serve the cached snapshot (bounded staleness)"
        );

        // refresh() swaps the cached provider on metadata change.
        let provider = provider(&table_key(&ident)).expect("registered provider");
        provider.refresh().await.expect("refresh");
        assert_eq!(
            count(&fresh_ctx, &table).await,
            1,
            "refresh must swap the provider to the foreign commit's snapshot"
        );

        // Local write through the freshness context: read-your-own-writes
        // must hold immediately, with NO refresh in between.
        fresh_ctx
            .sql(&format!("insert into demo.{table} values (2)"))
            .await
            .expect("local insert plan")
            .collect()
            .await
            .expect("local insert exec");
        assert_eq!(
            count(&fresh_ctx, &table).await,
            2,
            "a local write must invalidate synchronously (read-your-own-writes)"
        );

        catalog.drop_table(&ident).await.expect("drop");
    }
}
