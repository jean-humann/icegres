//! Lightweight, dependency-free operational metrics with a Prometheus text
//! exporter (production-readiness audit #8: the server ran blind).
//!
//! A single process-global [`Metrics`] of atomic counters/gauges, incremented
//! on the hot paths (queries via [`MetricsHook`], connections in the accept
//! loop, commit conflicts on the write paths) and rendered on `GET /metrics`
//! of the `--health-port` listener. No external metrics crate is pulled in
//! (keeps the pinned dependency graph unchanged); the exposition format is the
//! Prometheus/OpenMetrics text format any scraper understands.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Mutex, OnceLock};

use async_trait::async_trait;
use datafusion::common::ParamValues;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::Statement;
use datafusion_postgres::pgwire::api::results::Response;
use datafusion_postgres::pgwire::api::ClientInfo;
use datafusion_postgres::pgwire::error::PgWireResult;
use datafusion_postgres::QueryHook;

/// Process-global operational metrics. All fields are monotonic counters
/// except `connections_active`, a gauge.
#[derive(Default)]
pub struct Metrics {
    /// Wire statements handled (simple + extended protocol, both listeners).
    pub queries_total: AtomicU64,
    /// Connections accepted since boot (pgwire).
    pub connections_total: AtomicU64,
    /// Connections currently open (gauge).
    pub connections_active: AtomicU64,
    /// COMMIT/DML attempts rejected as serialization failures (SQLSTATE 40001,
    /// first-committer-wins) — a spike means writers are colliding.
    pub commit_conflicts_total: AtomicU64,
    /// Queries currently executing on the pgwire listener (gauge). Rises with
    /// load; a value that stays high while qps is low points at stuck queries.
    pub queries_in_flight: AtomicU64,
    /// Queries whose wall-clock exceeded the slow-query threshold
    /// (`ICEGRES_SLOW_QUERY_MS`). Each also logs a WARN with its duration.
    pub queries_slow_total: AtomicU64,
    /// Summed wall-clock of completed pgwire queries, in milliseconds. Divide
    /// by `queries_total` for a rolling average latency.
    pub query_duration_ms_total: AtomicU64,
    /// Worst-case staleness age across freshness-managed tables, in
    /// milliseconds (gauge, freshness.rs): per-table time since the last
    /// successful catalog load, maximized over the mounted table set and
    /// sampled at the START of each refresher pass — the age a read could
    /// have observed just before that pass refreshed, so a HEALTHY gauge
    /// reads ≈ the `--freshness-ms` interval (not the near-zero sawtooth
    /// minimum right after a refresh). Grows monotonically while the
    /// catalog is unreachable, and the refresher supervisor's watchdog
    /// keeps it growing even if the refresher task itself dies. Stays 0 in
    /// default mode (`--freshness-ms 0`).
    pub freshness_age_ms: AtomicU64,
    /// Physical-plan cache hits (plancache.rs; the cache is active only
    /// with `--freshness-ms > 0`).
    pub plan_cache_hits_total: AtomicU64,
    /// Physical-plan cache misses — including statements that turned out
    /// not to be cacheable (volatile expressions, non-cacheable tables).
    pub plan_cache_misses_total: AtomicU64,
    /// WORST-CASE milliseconds since the last applied peer-tail event,
    /// maximized over every configured peer (gauge, peer.rs; sampled every
    /// second while `--peer-tail` is configured) — so a healthy peer can
    /// never mask a dead one. Grows while any peer is silent; past the
    /// serving age bound that peer's mirrors are excluded from reads
    /// (commit-cadence fallback). Stays 0 without `--peer-tail`. Exported
    /// as `icegres_peer_tail_age_max_ms`; the per-peer breakdown is
    /// [`Metrics::peer_tail_ages_ms`].
    pub peer_tail_age_ms: AtomicU64,
    /// Per-peer milliseconds since that peer's last applied event, keyed by
    /// peer address (gauge with a `peer` label; replaced wholesale by the
    /// 1 Hz sampler so no dropped peer ever lingers with a frozen value). A
    /// peer that never delivered an event reports its age since spawn —
    /// absence of events is reported as growing age, never as a healthy 0.
    /// Empty (no series exported) without `--peer-tail`.
    pub peer_tail_ages_ms: Mutex<HashMap<String, u64>>,
}

/// The process-global metrics registry.
pub fn metrics() -> &'static Metrics {
    static M: OnceLock<Metrics> = OnceLock::new();
    M.get_or_init(Metrics::default)
}

impl Metrics {
    /// Replace the per-peer tail-age map (peer.rs' 1 Hz sampler) and refresh
    /// the global worst-case gauge from it.
    pub fn set_peer_tail_ages(&self, ages: Vec<(String, u64)>) {
        let max = ages.iter().map(|(_, age)| *age).max().unwrap_or(0);
        self.peer_tail_age_ms.store(max, Ordering::Relaxed);
        let mut map = crate::freshness::recover("peer age gauge", self.peer_tail_ages_ms.lock());
        map.clear();
        map.extend(ages);
    }

    /// Render the current values in Prometheus text exposition format.
    pub fn render_prometheus(&self) -> String {
        let q = self.queries_total.load(Ordering::Relaxed);
        let ct = self.connections_total.load(Ordering::Relaxed);
        let ca = self.connections_active.load(Ordering::Relaxed);
        let cc = self.commit_conflicts_total.load(Ordering::Relaxed);
        let qif = self.queries_in_flight.load(Ordering::Relaxed);
        let qs = self.queries_slow_total.load(Ordering::Relaxed);
        let qd = self.query_duration_ms_total.load(Ordering::Relaxed);
        let fa = self.freshness_age_ms.load(Ordering::Relaxed);
        let pch = self.plan_cache_hits_total.load(Ordering::Relaxed);
        let pcm = self.plan_cache_misses_total.load(Ordering::Relaxed);
        let pta = self.peer_tail_age_ms.load(Ordering::Relaxed);
        // Per-peer tail-age series (one line per configured peer, sorted for
        // stable output; empty without --peer-tail).
        let per_peer = {
            let map = crate::freshness::recover("peer age gauge", self.peer_tail_ages_ms.lock());
            let mut peers: Vec<(&String, &u64)> = map.iter().collect();
            peers.sort();
            if peers.is_empty() {
                String::new()
            } else {
                let mut s = String::from(
                    "# HELP icegres_peer_tail_age_ms Milliseconds since this peer's \
                     last applied tail event (grows while the peer is silent; past \
                     the serving bound its mirrors are excluded from reads).\n\
                     # TYPE icegres_peer_tail_age_ms gauge\n",
                );
                for (peer, age) in peers {
                    let label = peer.replace('\\', "\\\\").replace('"', "\\\"");
                    s.push_str(&format!(
                        "icegres_peer_tail_age_ms{{peer=\"{label}\"}} {age}\n"
                    ));
                }
                s
            }
        };
        format!(
            "# HELP icegres_queries_total Wire statements handled.\n\
             # TYPE icegres_queries_total counter\n\
             icegres_queries_total {q}\n\
             # HELP icegres_connections_total Connections accepted since boot.\n\
             # TYPE icegres_connections_total counter\n\
             icegres_connections_total {ct}\n\
             # HELP icegres_connections_active Currently open connections.\n\
             # TYPE icegres_connections_active gauge\n\
             icegres_connections_active {ca}\n\
             # HELP icegres_commit_conflicts_total Write attempts rejected as \
             serialization failures (SQLSTATE 40001).\n\
             # TYPE icegres_commit_conflicts_total counter\n\
             icegres_commit_conflicts_total {cc}\n\
             # HELP icegres_queries_in_flight Queries currently executing (pgwire).\n\
             # TYPE icegres_queries_in_flight gauge\n\
             icegres_queries_in_flight {qif}\n\
             # HELP icegres_queries_slow_total Queries over the slow-query threshold.\n\
             # TYPE icegres_queries_slow_total counter\n\
             icegres_queries_slow_total {qs}\n\
             # HELP icegres_query_duration_ms_total Summed query wall-clock (ms).\n\
             # TYPE icegres_query_duration_ms_total counter\n\
             icegres_query_duration_ms_total {qd}\n\
             # HELP icegres_freshness_age_ms Worst-case staleness age across \
             freshness-managed tables (ms): time since each table's last \
             successful catalog load, sampled at refresher pass START, so a \
             healthy value reads about the --freshness-ms interval; keeps \
             growing during catalog outages or if the refresher dies; 0 when \
             --freshness-ms is 0.\n\
             # TYPE icegres_freshness_age_ms gauge\n\
             icegres_freshness_age_ms {fa}\n\
             # HELP icegres_plan_cache_hits_total Physical-plan cache hits.\n\
             # TYPE icegres_plan_cache_hits_total counter\n\
             icegres_plan_cache_hits_total {pch}\n\
             # HELP icegres_plan_cache_misses_total Physical-plan cache misses.\n\
             # TYPE icegres_plan_cache_misses_total counter\n\
             icegres_plan_cache_misses_total {pcm}\n\
             # HELP icegres_peer_tail_age_max_ms Worst-case milliseconds since \
             the last applied peer-tail event across every configured peer \
             (0 without --peer-tail; grows while any peer is silent and its \
             mirrors fall back to commit cadence).\n\
             # TYPE icegres_peer_tail_age_max_ms gauge\n\
             icegres_peer_tail_age_max_ms {pta}\n\
             {per_peer}"
        )
    }
}

/// A no-op [`QueryHook`] that only observes: it increments `queries_total` for
/// every wire statement and always falls through (`None`) so it never changes
/// query behavior. Registered first in the hook chain.
pub struct MetricsHook;

#[async_trait]
impl QueryHook for MetricsHook {
    async fn handle_simple_query(
        &self,
        _statement: &Statement,
        _session_context: &SessionContext,
        _client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        metrics().queries_total.fetch_add(1, Ordering::Relaxed);
        None
    }

    async fn handle_extended_parse_query(
        &self,
        _sql: &Statement,
        _session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        // Parse phase only; counted at execute (handle_extended_query).
        None
    }

    async fn handle_extended_query(
        &self,
        _statement: &Statement,
        _logical_plan: &LogicalPlan,
        _params: &ParamValues,
        _session_context: &SessionContext,
        _client: &mut (dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<Response>> {
        metrics().queries_total.fetch_add(1, Ordering::Relaxed);
        None
    }
}
