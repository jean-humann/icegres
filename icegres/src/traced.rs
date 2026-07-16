//! Query timing + in-flight visibility for the pgwire listener
//! (production-readiness audit #9).
//!
//! The `QueryHook` chain can observe a statement's PARSE, but a hook that
//! falls through (returns `None`) — which every plain `SELECT` does — never
//! sees the default handler finish, so it cannot time execution. The seam that
//! *does* see the whole thing is the handler itself: `DfSessionService::
//! do_query` runs the hooks AND the default execution. So [`TracedService`]
//! wraps `DfSessionService`, delegates every method, and times `do_query` on
//! both the simple and extended protocols.
//!
//! What it produces, all correlated by the per-connection span established in
//! `ops.rs` (audit #11):
//! * `icegres_queries_in_flight` gauge (a value that stays high while qps is
//!   low is the signature of a stuck query),
//! * `icegres_queries_slow_total` + a WARN per query over
//!   `ICEGRES_SLOW_QUERY_MS` (default 1000; `0` disables) carrying kind +
//!   duration + the connection's span fields (full SQL stays at `debug` — it
//!   can contain user data),
//! * `icegres_query_duration_ms_total` (÷ `queries_total` = average latency),
//! * and an in-flight registry that `drain_connections` dumps on shutdown so a
//!   query still running past the grace period is named, not just counted.

use std::collections::HashMap;
use std::fmt::Debug;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use datafusion_postgres::pgwire::api::portal::Portal;
use datafusion_postgres::pgwire::api::query::{ExtendedQueryHandler, SimpleQueryHandler};
use datafusion_postgres::pgwire::api::results::Response;
use datafusion_postgres::pgwire::api::store::PortalStore;
use datafusion_postgres::pgwire::api::{ClientInfo, ClientPortalStore};
use datafusion_postgres::pgwire::error::{PgWireError, PgWireResult};
use datafusion_postgres::pgwire::messages::PgWireBackendMessage;
use datafusion_postgres::DfSessionService;
use futures::Sink;
use tracing::{debug, warn};

use crate::metrics::metrics;

/// One executing query in the in-flight registry.
struct Entry {
    kind: &'static str,
    started: Instant,
}

/// Process-global registry of currently-executing pgwire queries. Backs the
/// `icegres_queries_in_flight` gauge and the shutdown "still-running" dump.
#[derive(Default)]
pub struct InFlight {
    seq: AtomicU64,
    map: Mutex<HashMap<u64, Entry>>,
}

/// The process-global in-flight registry.
pub fn in_flight() -> &'static InFlight {
    static F: OnceLock<InFlight> = OnceLock::new();
    F.get_or_init(InFlight::default)
}

impl InFlight {
    /// Number of queries currently executing.
    pub fn count(&self) -> usize {
        self.map.lock().expect("in-flight lock poisoned").len()
    }

    /// `(id, kind, elapsed)` for every still-running query — logged on shutdown
    /// so a query outliving the drain grace period can be identified.
    pub fn snapshot(&self) -> Vec<(u64, &'static str, Duration)> {
        self.map
            .lock()
            .expect("in-flight lock poisoned")
            .iter()
            .map(|(id, e)| (*id, e.kind, e.started.elapsed()))
            .collect()
    }
}

/// RAII span of one executing query: registers on construction, and on drop
/// (normal completion OR client-disconnect cancellation) deregisters, records
/// duration, and logs completion / slow WARN. Drop-based so a cancelled query
/// (its future dropped) still decrements the gauge and is logged.
struct QueryGuard {
    id: u64,
    kind: &'static str,
    started: Instant,
    slow_ms: u64,
}

impl QueryGuard {
    fn begin(kind: &'static str, slow_ms: u64) -> Self {
        let reg = in_flight();
        let id = reg.seq.fetch_add(1, Ordering::Relaxed);
        reg.map.lock().expect("in-flight lock poisoned").insert(
            id,
            Entry {
                kind,
                started: Instant::now(),
            },
        );
        metrics().queries_in_flight.fetch_add(1, Ordering::Relaxed);
        Self {
            id,
            kind,
            started: Instant::now(),
            slow_ms,
        }
    }
}

impl Drop for QueryGuard {
    fn drop(&mut self) {
        in_flight()
            .map
            .lock()
            .expect("in-flight lock poisoned")
            .remove(&self.id);
        let ms = self.started.elapsed().as_millis() as u64;
        let m = metrics();
        m.queries_in_flight.fetch_sub(1, Ordering::Relaxed);
        m.query_duration_ms_total.fetch_add(ms, Ordering::Relaxed);
        if self.slow_ms > 0 && ms >= self.slow_ms {
            m.queries_slow_total.fetch_add(1, Ordering::Relaxed);
            // No SQL text at WARN (may contain user data); it is at debug via
            // the default handler's own logging. The connection span supplies
            // conn_id + peer, so this WARN is attributable.
            warn!(kind = self.kind, elapsed_ms = ms, "slow query");
        } else {
            debug!(kind = self.kind, elapsed_ms = ms, "query completed");
        }
    }
}

/// Leading SQL keyword as a cheap, allocation-free statement label. Never
/// echoes user data — only the verb.
fn kind_of(sql: &str) -> &'static str {
    let word = sql
        .trim_start()
        .split(|c: char| c.is_whitespace() || c == '(' || c == ';')
        .next()
        .unwrap_or("");
    // ASCII case-fold the first token to a fixed label.
    match word.to_ascii_uppercase().as_str() {
        "SELECT" => "SELECT",
        "INSERT" => "INSERT",
        "UPDATE" => "UPDATE",
        "DELETE" => "DELETE",
        "BEGIN" | "START" => "BEGIN",
        "COMMIT" | "END" => "COMMIT",
        "ROLLBACK" | "ABORT" => "ROLLBACK",
        "CREATE" => "CREATE",
        "DROP" => "DROP",
        "COPY" => "COPY",
        "SET" => "SET",
        "SHOW" => "SHOW",
        "" => "EMPTY",
        _ => "OTHER",
    }
}

/// Timing wrapper around `DfSessionService`. Same query behavior — it only
/// delegates and measures — plus the raw-SQL `AS OF` pre-rewrite (asof.rs):
/// hooks only ever see PARSED statements, and `AS OF` is not part of the
/// parser's grammar, so the rewrite has to happen here, on the raw text,
/// before delegation. Statements without the exact sugar are untouched
/// (one allocation-free scan). Placed in the handler factory in `ops.rs`.
pub struct TracedService {
    inner: Arc<DfSessionService>,
    /// `AS OF` sugar for the simple protocol; the extended protocol gets it
    /// through [`crate::asof::AsOfParser`] below.
    asof: Arc<crate::asof::AsOfRewriter>,
    /// The extended-protocol parser with the same pre-rewrite, built once.
    asof_parser: Arc<crate::asof::AsOfParser>,
    slow_ms: u64,
}

impl TracedService {
    pub fn new(inner: Arc<DfSessionService>, catalog: Arc<dyn iceberg::Catalog>) -> Self {
        // Slow-query threshold in ms; default 1000, `0` disables the WARN.
        let slow_ms = std::env::var("ICEGRES_SLOW_QUERY_MS")
            .ok()
            .and_then(|v| v.trim().parse::<u64>().ok())
            .unwrap_or(1000);
        let asof = Arc::new(crate::asof::AsOfRewriter::new(catalog));
        let asof_parser = Arc::new(crate::asof::AsOfParser::new(
            inner.query_parser(),
            asof.clone(),
        ));
        Self {
            inner,
            asof,
            asof_parser,
            slow_ms,
        }
    }
}

#[async_trait]
impl SimpleQueryHandler for TracedService {
    async fn do_query<C>(&self, client: &mut C, query: &str) -> PgWireResult<Vec<Response>>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let _guard = QueryGuard::begin(kind_of(query), self.slow_ms);
        // AS OF sugar (asof.rs): rewrite the raw text to table@snapshot when
        // (and only when) the exact gated syntax is present.
        if let Some(rewritten) = self.asof.rewrite(query).await? {
            return SimpleQueryHandler::do_query(self.inner.as_ref(), client, &rewritten).await;
        }
        SimpleQueryHandler::do_query(self.inner.as_ref(), client, query).await
    }
}

#[async_trait]
impl ExtendedQueryHandler for TracedService {
    // Project the associated types through the public trait so the wrapper is
    // interchangeable with the inner service without naming private types.
    type Statement = <DfSessionService as ExtendedQueryHandler>::Statement;
    type QueryParser = crate::asof::AsOfParser;

    fn query_parser(&self) -> Arc<Self::QueryParser> {
        self.asof_parser.clone()
    }

    async fn do_query<C>(
        &self,
        client: &mut C,
        portal: &Portal<Self::Statement>,
        max_rows: usize,
    ) -> PgWireResult<Response>
    where
        C: ClientInfo + ClientPortalStore + Sink<PgWireBackendMessage> + Unpin + Send + Sync,
        C::Error: Debug,
        C::PortalStore: PortalStore<Statement = Self::Statement>,
        PgWireError: From<<C as Sink<PgWireBackendMessage>>::Error>,
    {
        let _guard = QueryGuard::begin(kind_of(&portal.statement.statement.0), self.slow_ms);
        ExtendedQueryHandler::do_query(self.inner.as_ref(), client, portal, max_rows).await
    }

    // do_describe_statement / do_describe_portal / on_parse are inherited from
    // the trait defaults exactly as DfSessionService uses them (they only need
    // `query_parser`, which we delegate) — no metadata round trip is timed.
}
