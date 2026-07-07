//! Lightweight, dependency-free operational metrics with a Prometheus text
//! exporter (production-readiness audit #8: the server ran blind).
//!
//! A single process-global [`Metrics`] of atomic counters/gauges, incremented
//! on the hot paths (queries via [`MetricsHook`], connections in the accept
//! loop, commit conflicts on the write paths) and rendered on `GET /metrics`
//! of the `--health-port` listener. No external metrics crate is pulled in
//! (keeps the pinned dependency graph unchanged); the exposition format is the
//! Prometheus/OpenMetrics text format any scraper understands.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

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
}

/// The process-global metrics registry.
pub fn metrics() -> &'static Metrics {
    static M: OnceLock<Metrics> = OnceLock::new();
    M.get_or_init(Metrics::default)
}

impl Metrics {
    /// Render the current values in Prometheus text exposition format.
    pub fn render_prometheus(&self) -> String {
        let q = self.queries_total.load(Ordering::Relaxed);
        let ct = self.connections_total.load(Ordering::Relaxed);
        let ca = self.connections_active.load(Ordering::Relaxed);
        let cc = self.commit_conflicts_total.load(Ordering::Relaxed);
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
             icegres_commit_conflicts_total {cc}\n"
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
