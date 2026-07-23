//! Per-stage read-path timing behind `ICEGRES_QUERY_TIMING=1`.
//!
//! The pgwire read hot path has five interesting stages, but the default
//! handler (`DfSessionService::do_query`) runs them fused: `ctx.sql()`
//! (parse + logical plan) then `encode_dataframe` (physical plan + execute +
//! row encode inside one stream). To time the stages separately without
//! forking datafusion-postgres, [`TimingHook`] is registered as the LAST
//! query hook: when timing is enabled it takes over plain `SELECT`s that
//! would otherwise fall through to the default handler and runs the exact
//! same pipeline stage by stage, emitting one
//! `target: "icegres::query_timing"` INFO line per stage:
//!
//! * `parse` — the Postgres-compatibility SQL parse (re-run of the parse the
//!   wire handler already did before hooks; same work, measured).
//! * `plan_logical` — `ctx.sql()`: DataFusion's own parse + logical
//!   planning + analysis.
//! * `plan_physical` — logical optimization + physical planning. This is the
//!   stage that calls `TableProvider::scan`, so it CONTAINS the per-scan
//!   catalog freshness check — which cache.rs logs separately as stage
//!   `freshness` so the catalog round trip can be split out.
//! * `execute_collect` — running the physical plan to completion.
//! * `encode` — encoding the collected Arrow batches into pgwire `DataRow`s
//!   (arrow-pg's row encoder is lazy; draining it here performs the work).
//! * `total` — wall time of all of the above.
//!
//! Zero cost when `ICEGRES_QUERY_TIMING` is unset: [`enabled`] is a single
//! cached bool load and the hook immediately falls through (`None`), leaving
//! the default handler byte-identical. Known divergence when ENABLED (a
//! diagnostic mode): rows are buffered rather than streamed, and the
//! per-session `statement_timeout` is not applied to intercepted `SELECT`s.
//!
//! # Write-path stages (same env var, same zero-cost contract)
//!
//! The WRITE hot paths emit stage records through [`record`] too — every
//! call site gates its `Instant::now()` on [`enabled`], so an unset env var
//! costs one cached bool load per statement:
//!
//! * sync commit (overwrite.rs `prepare_commit` / `post_commit` and the
//!   autocommit retry loops): `insert_plan` (txn.rs, shaping INSERT rows
//!   through DataFusion), `file_scan` (reading existing live data files),
//!   `dml_apply` (folding UPDATE/DELETE ops over rows), `parquet_encode`
//!   (data-writer writes), `data_file_put` (writer close = Parquet flush +
//!   object-store PUT(s)), `manifest_put`, `manifest_list_put`,
//!   `prepare_total`, `catalog_post` (the REST commit POST), and one
//!   `commit_attempt`/`commit_retry` per optimistic-concurrency attempt.
//!   Because the stock fast_append INSERT runs fused inside
//!   iceberg-datafusion's execution plan, timing mode routes PLAIN
//!   autocommit INSERTs through the engine path so these stages are
//!   observable (txn.rs `autocommit_insert` docs) — an equivalent append
//!   snapshot, posted via the same REST commit.
//! * tail ack (tail.rs / tail_pg.rs / tail_quorum.rs + buffer.rs):
//!   `tail_encode` (Arrow-IPC frame/payload encode), then per backend
//!   `tail_fsync` (local WAL frame write + group-fsync wait — the whole
//!   durability cost of the statement, shared syncs included) /
//!   `tail_pg_commit` (tail-database INSERT+commit round trip) /
//!   `tail_quorum_ack` (proposer round trip to a 2-of-3 AppendResp
//!   quorum); `buffer_route` (in-memory window bookkeeping),
//!   `buffer_append` (whole statement append: align, staged tail append,
//!   bookkeeping, and the durability wait — which since the group-fsync
//!   change runs AFTER the buffer lock drops), `buffer_ack_total` (whole
//!   buffered-INSERT ack incl. planning).
//! * keyed tail writes (buffer.rs `try_keyed_dml`): `keyed_gate`
//!   (activation resolution: freshness-cached metadata when
//!   `--freshness-ms` is on and fresh — ~0 — otherwise one catalog load),
//!   `keyed_rmw_read` (current-row resolution: keyed-map hit or union-view
//!   read), `keyed_apply` (folding the statement over the row),
//!   `keyed_write` (durable tail append + map insert), `keyed_total`.

use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use datafusion::common::ParamValues;
use datafusion::logical_expr::LogicalPlan;
use datafusion::prelude::SessionContext;
use datafusion::sql::sqlparser::ast::Statement as SqlStatement;
use datafusion_postgres::arrow_pg::datatypes::{arrow_schema_to_pg_fields, encode_recordbatch};
use datafusion_postgres::datafusion_pg_catalog::sql::PostgresCompatibilityParser;
use datafusion_postgres::hooks::HookClient;
use datafusion_postgres::pgwire::api::portal::Format;
use datafusion_postgres::pgwire::api::results::{QueryResponse, Response};
use datafusion_postgres::pgwire::api::ClientInfo;
use datafusion_postgres::pgwire::error::{PgWireError, PgWireResult};
use datafusion_postgres::pgwire::messages::data::DataRow;
use datafusion_postgres::pgwire::types::format::FormatOptions;
use datafusion_postgres::QueryHook;
use tracing::info;

/// Whether `ICEGRES_QUERY_TIMING` is set truthy. Read once per process.
pub fn enabled() -> bool {
    static E: OnceLock<bool> = OnceLock::new();
    *E.get_or_init(|| {
        matches!(
            std::env::var("ICEGRES_QUERY_TIMING")
                .as_deref()
                .map(str::trim),
            Ok("1") | Ok("true") | Ok("on") | Ok("yes")
        )
    })
}

/// Emit one per-stage timing line. Callers gate on [`enabled`] themselves
/// (so the `Instant::now()` is also skipped when timing is off).
pub fn record(stage: &'static str, elapsed: Duration) {
    info!(
        target: "icegres::query_timing",
        stage,
        us = elapsed.as_micros() as u64,
        "stage"
    );
}

/// Last-in-chain query hook that, when timing is enabled, executes plain
/// `SELECT`s stage by stage (see module docs). Every other statement — and
/// everything when timing is off — falls through unchanged.
pub struct TimingHook {
    /// Same compatibility parser the wire handler uses; re-parsing the
    /// statement text with it measures the real stage-(b) cost.
    parser: PostgresCompatibilityParser,
}

impl TimingHook {
    pub fn new() -> Self {
        Self {
            parser: PostgresCompatibilityParser::new(),
        }
    }

    async fn run(
        &self,
        statement: &SqlStatement,
        ctx: &SessionContext,
        client: &mut dyn HookClient,
    ) -> PgWireResult<Response> {
        let total = Instant::now();
        let sql = statement.to_string();

        // (b) SQL parse.
        let t = Instant::now();
        self.parser
            .parse(&sql)
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        record("parse", t.elapsed());

        // (c1) logical planning (includes DataFusion's own parse).
        let t = Instant::now();
        let df = ctx
            .sql(&sql)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        record("plan_logical", t.elapsed());

        // Same field derivation as the default simple-protocol path.
        let format_options = Arc::new(FormatOptions::from_client_metadata(client.metadata()));
        let fields = Arc::new(arrow_schema_to_pg_fields(
            df.schema().as_arrow(),
            &Format::UnifiedText,
            Some(format_options),
        )?);

        // (c2) physical planning — contains the per-scan freshness check
        // (logged separately by cache.rs as stage "freshness").
        let (state, logical) = df.into_parts();
        let t = Instant::now();
        let plan = state
            .create_physical_plan(&logical)
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        record("plan_physical", t.elapsed());

        // (d) execution.
        let t = Instant::now();
        let batches = datafusion::physical_plan::collect(plan, state.task_ctx())
            .await
            .map_err(|e| PgWireError::ApiError(Box::new(e)))?;
        record("execute_collect", t.elapsed());

        // (e) row encoding (the encoder iterator is lazy; draining it here
        // does the work the default path does inside its response stream).
        let t = Instant::now();
        let mut rows: Vec<PgWireResult<DataRow>> = Vec::new();
        for batch in batches {
            rows.extend(encode_recordbatch(fields.clone(), batch));
        }
        record("encode", t.elapsed());

        record("total", total.elapsed());
        Ok(Response::Query(QueryResponse::new(
            fields,
            futures::stream::iter(rows),
        )))
    }
}

#[async_trait]
impl QueryHook for TimingHook {
    async fn handle_simple_query(
        &self,
        statement: &SqlStatement,
        session_context: &SessionContext,
        client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        if !enabled() || !matches!(statement, SqlStatement::Query(_)) {
            return None;
        }
        Some(self.run(statement, session_context, client).await)
    }

    async fn handle_extended_parse_query(
        &self,
        _sql: &SqlStatement,
        _session_context: &SessionContext,
        _client: &(dyn ClientInfo + Send + Sync),
    ) -> Option<PgWireResult<LogicalPlan>> {
        None
    }

    async fn handle_extended_query(
        &self,
        _statement: &SqlStatement,
        _logical_plan: &LogicalPlan,
        _params: &ParamValues,
        _session_context: &SessionContext,
        _client: &mut dyn HookClient,
    ) -> Option<PgWireResult<Response>> {
        None
    }
}
