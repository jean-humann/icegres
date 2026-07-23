//! `icegres flight-serve` — Arrow Flight SQL endpoint over the same Iceberg
//! lakehouse the pgwire listener serves (SPEC A11: ADBC first-class).
//!
//! Second first-class wire protocol next to pgwire, sharing the exact same
//! engine wiring: `context::build_session_context` (snapshot-aware caching
//! schema providers from cache.rs, read-your-writes on snapshot change) and
//! the copy-on-write [`OverwriteEngine`](crate::overwrite) for UPDATE/DELETE.
//! Everything Arrow-native stays Arrow end to end: query results stream as
//! Arrow IPC record batches over gRPC with no row-format round trip.
//!
//! # Surface (verified against `adbc_driver_flightsql`, the Arrow ADBC Go
//! driver — bench/clients/a11_adbc_probe.py)
//!
//! * **Queries**: `CommandStatementQuery` via GetFlightInfo (result schema in
//!   the FlightInfo) → DoGet (Arrow stream).
//! * **Catalog metadata**: `CommandGetCatalogs` / `CommandGetDbSchemas` /
//!   `CommandGetTables` (incl. `include_schema` Arrow schemas, %/_ filter
//!   patterns) / `CommandGetTableTypes` / `CommandGetSqlInfo` — this is what
//!   ADBC's `get_objects` (depth catalogs/schemas/tables/columns) consumes.
//! * **Prepared statements**: `ActionCreatePreparedStatement{Request}` →
//!   handle; `DoPut(CommandPreparedStatementQuery)` binds `$n` parameters
//!   (one row of Arrow values → DataFusion `ParamValues`);
//!   `GetFlightInfo`/`DoGet` execute; `ActionClosePreparedStatement` frees.
//! * **DML**: `DoPut(CommandStatementUpdate)` — INSERT executes through the
//!   session context (same iceberg-datafusion append path as pgwire INSERT,
//!   one commit per statement); UPDATE/DELETE route through
//!   `dml::parse_single_dml` + `OverwriteEngine` with identical scope rules
//!   and row counts. Prepared updates (`CommandPreparedStatementUpdate`)
//!   execute once per bound parameter row.
//! * **Bulk ingest** (`CommandStatementIngest`, ADBC
//!   `cursor.adbc_ingest(table, data, mode="append")`): the whole Arrow
//!   stream lands as ONE Iceberg fast-append commit — batches flow into
//!   iceberg-datafusion's INSERT plan (rolling Parquet writer, default
//!   target file size), so 100k rows become a handful of properly-sized
//!   Parquet files and a single snapshot, not 100k row-commits. Scope:
//!   append into an EXISTING table (`mode="append"`); `mode="create"` /
//!   `"replace"`, `temporary`, and ingest transactions are rejected loudly.
//!
//! # Auth & TLS
//!
//! `--auth-file` (same `user:password` file and env var as `icegres serve`)
//! enables the Flight SQL basic-auth handshake: the client sends
//! `authorization: Basic base64(user:password)`, the server verifies it
//! against the stored SCRAM verifier (pgauth.rs — cleartext is never kept in
//! memory) and answers with a per-boot random `Bearer` token that every
//! subsequent RPC must present. NOTE the trade-off vs pgwire SCRAM: basic
//! auth sends the password itself, so pair it with TLS. In-process TLS is now
//! built in: `--tls-cert`/`--tls-key` terminate TLS with the same rustls stack
//! as pgwire (advertising the `h2` ALPN so gRPC negotiates HTTP/2), so the ADBC
//! `grpc+tls://` client authenticates over an encrypted channel with no front
//! proxy required; a bad cert/key aborts startup rather than downgrading. You
//! may still terminate TLS in front (nginx/envoy grpc_pass) if you prefer.
//! Without `--auth-file` the endpoint is permissive (any/no credentials
//! accepted) and logs the same startup WARN as pgwire.

use std::collections::HashMap;
use std::future::Future;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::{Duration, Instant};

use anyhow::{Context as _, Result};
use arrow::array::{Array, RecordBatch, UInt64Array};
use arrow::datatypes::{Schema, SchemaRef};
use arrow::ipc::writer::IpcWriteOptions;
use arrow_flight::decode::FlightRecordBatchStream;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::metadata::{
    GetCatalogsBuilder, GetDbSchemasBuilder, GetTablesBuilder, SqlInfoData, SqlInfoDataBuilder,
};
use arrow_flight::sql::server::{FlightSqlService, PeekableFlightDataStream};
use arrow_flight::sql::{
    ActionClosePreparedStatementRequest, ActionCreatePreparedStatementRequest,
    ActionCreatePreparedStatementResult, CommandGetCatalogs, CommandGetDbSchemas,
    CommandGetSqlInfo, CommandGetTableTypes, CommandGetTables, CommandPreparedStatementQuery,
    CommandPreparedStatementUpdate, CommandStatementIngest, CommandStatementQuery,
    CommandStatementUpdate, DoPutPreparedStatementResult, ProstMessageExt, SqlInfo,
    TableExistsOption, TableNotExistOption, TicketStatementQuery,
};
use arrow_flight::{
    Action, FlightDescriptor, FlightEndpoint, FlightInfo, HandshakeRequest, HandshakeResponse,
    IpcMessage, SchemaAsIpc, Ticket,
};
use base64::engine::general_purpose::{GeneralPurpose, GeneralPurposeConfig};
use base64::engine::DecodePaddingMode;
use base64::Engine as _;
use datafusion::common::{ParamValues, ScalarValue};
use datafusion::prelude::{DataFrame, SessionContext};
use futures::{stream, Stream, StreamExt, TryStreamExt};
use prost::Message;
use tonic::metadata::MetadataValue;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info, warn};

use crate::authz::{self, Action as AuthzAction, SharedAuthorizer, TableRef};
use crate::buffer::WriteBuffer;
use crate::cache::MetadataVersion;
use crate::context::{self, CATALOG_NAME, DEFAULT_SCHEMA};
use crate::ops::BasicAuthVerifier;
use crate::overwrite::{CommitConflict, ConstraintViolation, OverwriteEngine};
use crate::plancache::{self, PlanCache, PlanKey};
use crate::{dml, CatalogOpts};
use datafusion::physical_plan::ExecutionPlan;
use datafusion::sql::sqlparser::dialect::PostgreSqlDialect;
use datafusion::sql::sqlparser::parser::Parser;

/// Table type reported for every Iceberg table (there are no views).
const TABLE_TYPE: &str = "TABLE";

/// Standard-alphabet base64 that accepts BOTH padded and unpadded input:
/// the Go ADBC Flight SQL driver sends `Basic` credentials WITHOUT `=`
/// padding (RawStdEncoding), other clients pad — reject neither.
const BASE64_ANY_PAD: GeneralPurpose = GeneralPurpose::new(
    &base64::alphabet::STANDARD,
    GeneralPurposeConfig::new().with_decode_padding_mode(DecodePaddingMode::Indifferent),
);

type DoGetStream = Pin<Box<dyn Stream<Item = Result<arrow_flight::FlightData, Status>> + Send>>;
type HandshakeStream = Pin<Box<dyn Stream<Item = Result<HandshakeResponse, Status>> + Send>>;

/// Resource guard wrapping a data DoGet stream: enforces the statement
/// timeout and result-byte cap, holds the concurrency permit for the
/// stream's lifetime, and accounts the per-RPC metrics on completion.
///
/// The deadline is checked against a live timer registered on every poll, so
/// it fires even if the inner stream stalls mid-scan (not only between
/// items) — a genuinely hung query still returns DEADLINE_EXCEEDED. Drop
/// releases the permit, decrements the in-flight gauge, and records the
/// stream's wall-clock, whether it ended cleanly, by error, or by client
/// cancel (tonic drops the stream).
struct GuardedStream {
    inner: DoGetStream,
    deadline: Option<Pin<Box<tokio::time::Sleep>>>,
    byte_budget: Option<u64>,
    bytes_seen: u64,
    started: Instant,
    done: bool,
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl GuardedStream {
    /// Construct a guarded stream and register it in the in-flight gauge. The
    /// paired decrement lives in `Drop`, so every `GuardedStream` — including
    /// those built directly in tests — balances the gauge regardless of how it
    /// ends (clean, error, timeout, or client cancel).
    fn new(
        inner: DoGetStream,
        timeout: Option<Duration>,
        byte_budget: Option<u64>,
        permit: Option<tokio::sync::OwnedSemaphorePermit>,
    ) -> Self {
        crate::metrics::metrics()
            .flight_rpcs_in_flight
            .fetch_add(1, Ordering::Relaxed);
        Self {
            inner,
            deadline: timeout.map(|d| Box::pin(tokio::time::sleep(d))),
            byte_budget,
            bytes_seen: 0,
            started: Instant::now(),
            done: false,
            _permit: permit,
        }
    }
}

impl Stream for GuardedStream {
    type Item = Result<arrow_flight::FlightData, Status>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.done {
            return Poll::Ready(None);
        }
        // Deadline first: a hung inner poll must not outlive the budget.
        if let Some(deadline) = self.deadline.as_mut() {
            if deadline.as_mut().poll(cx).is_ready() {
                self.done = true;
                crate::metrics::metrics()
                    .flight_rpcs_aborted_total
                    .fetch_add(1, Ordering::Relaxed);
                return Poll::Ready(Some(Err(Status::deadline_exceeded(
                    "query exceeded the Flight statement timeout \
                     (--flight-statement-timeout-ms)",
                ))));
            }
        }
        match self.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(fd))) => {
                let n = fd.data_body.len() as u64;
                // Enforce the cap BEFORE counting: a batch that pushes past the
                // budget is dropped (replaced by RESOURCE_EXHAUSTED) and never
                // reaches the client, so its bytes must not land in
                // flight_bytes_out_total ("bytes streamed to clients").
                if let Some(budget) = self.byte_budget {
                    if self.bytes_seen.saturating_add(n) > budget {
                        self.done = true;
                        crate::metrics::metrics()
                            .flight_rpcs_aborted_total
                            .fetch_add(1, Ordering::Relaxed);
                        return Poll::Ready(Some(Err(Status::resource_exhausted(format!(
                            "query result exceeded the Flight result cap of {budget} bytes \
                             (--flight-max-result-bytes); narrow the query or raise the limit"
                        )))));
                    }
                }
                self.bytes_seen += n;
                crate::metrics::metrics()
                    .flight_bytes_out_total
                    .fetch_add(n, Ordering::Relaxed);
                Poll::Ready(Some(Ok(fd)))
            }
            Poll::Ready(Some(Err(e))) => {
                self.done = true;
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(None) => {
                self.done = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl Drop for GuardedStream {
    fn drop(&mut self) {
        let m = crate::metrics::metrics();
        m.flight_rpcs_in_flight.fetch_sub(1, Ordering::Relaxed);
        m.flight_rpc_duration_ms_total
            .fetch_add(self.started.elapsed().as_millis() as u64, Ordering::Relaxed);
    }
}

/// A prepared statement: the SQL text plus the last bound parameter rows
/// (`DoPut(CommandPreparedStatementQuery)` replaces them on every bind).
struct Prepared {
    /// Principal that created the handle. Prepared handles are capabilities,
    /// but UUID secrecy is not an authorization boundary: every subsequent
    /// operation must still match this owner.
    owner: Option<String>,
    last_used: Instant,
    sql: String,
    /// Bound parameter rows; each row is one `$1..$n` value set.
    params: Vec<Vec<ScalarValue>>,
    /// Dataset (result) schema, planned once at create time. `GetFlightInfo`
    /// answers from this instead of re-planning the SQL a second time; a
    /// SELECT's result schema does not depend on the data snapshot, so this
    /// is safe regardless of whether `DoGet` executes the version-validated
    /// create-time plan or re-plans.
    schema: SchemaRef,
    /// The PHYSICAL plan built at create time for the zero-params case,
    /// consumed ONE-SHOT by the paired `DoGet` (ADBC's dbapi prepares every
    /// statement, so create→execute→close is one query — planning once for
    /// it is the pgwire-simple-statement semantic; see the plan-once notes
    /// on [`FlightSqlServiceImpl::physical_plan`]). `None` for DML, for
    /// statements that cannot physical-plan unbound (e.g. `$n`
    /// placeholders), and for statements ineligible to pin at all
    /// ([`StashedPlan`]'s soundness rules); DoGet then re-plans (via the
    /// SQL-keyed cache in freshness mode).
    plan: Option<StashedPlan>,
}

/// Lifetime of a bearer token minted by a handshake. After this the client
/// must re-handshake; expired tokens are pruned lazily on the next RPC so the
/// token map cannot grow without bound across a long-lived server.
const TOKEN_TTL: Duration = Duration::from_secs(3600);
const DEFAULT_AUTH_CACHE_CAP: usize = 4096;
const DEFAULT_PREPARED_CAP: usize = 1024;
const DEFAULT_PREPARED_TTL: Duration = Duration::from_secs(15 * 60);

/// TTL for the per-RPC `Basic` verification cache. Deliberately much shorter
/// than handshake bearer tokens: this cache is the only thing standing
/// between an auth-file edit and a still-connecting browser, so a revoked
/// password stops working within a minute (one KDF per credential per
/// minute is noise; a one-hour revocation lag is not).
const BASIC_CACHE_TTL: Duration = Duration::from_secs(60);

/// A minted bearer token's bound identity and issue time.
struct TokenEntry {
    /// The authenticated principal (empty string when auth is disabled).
    user: String,
    issued: Instant,
    last_used: Instant,
}

fn prune_token_store(store: &mut HashMap<String, TokenEntry>, ttl: Duration, cap: usize) {
    let now = Instant::now();
    store.retain(|_, entry| now.duration_since(entry.issued) < ttl);
    while store.len() >= cap.max(1) {
        let Some(oldest) = store
            .iter()
            .min_by_key(|(_, entry)| entry.last_used)
            .map(|(key, _)| key.clone())
        else {
            break;
        };
        store.remove(&oldest);
    }
}

fn prune_prepared_store(store: &mut HashMap<String, Prepared>, ttl: Duration) {
    store.retain(|_, entry| entry.last_used.elapsed() < ttl);
}

fn make_prepared_room(store: &mut HashMap<String, Prepared>, ttl: Duration, cap: usize) {
    prune_prepared_store(store, ttl);
    while store.len() >= cap.max(1) {
        let Some(oldest) = store
            .iter()
            .min_by_key(|(_, entry)| entry.last_used)
            .map(|(key, _)| key.clone())
        else {
            break;
        };
        store.remove(&oldest);
    }
}

fn check_prepared_owner(entry: &Prepared, principal: &Option<String>) -> Result<(), Status> {
    if entry.owner.as_deref() == principal.as_deref() {
        Ok(())
    } else {
        Err(Status::permission_denied(
            "prepared statement belongs to a different principal",
        ))
    }
}

/// Decode a `Basic` base64 payload into `(user, password)` (shared by the
/// Handshake RPC and the per-RPC header path).
fn decode_basic_credentials(b64: &str) -> Result<(String, String), Status> {
    let decoded = BASE64_ANY_PAD
        .decode(b64)
        .map_err(|_| Status::unauthenticated("Basic credentials are not valid base64"))?;
    let creds = String::from_utf8(decoded)
        .map_err(|_| Status::unauthenticated("Basic credentials are not valid UTF-8"))?;
    let (user, password) = creds
        .split_once(':')
        .ok_or_else(|| Status::unauthenticated("expected user:password credentials"))?;
    Ok((user.to_string(), password.to_string()))
}

/// Verify per-RPC `Basic` credentials, caching successes so the SCRAM
/// PBKDF2 (4096 iterations) runs once per credential per TTL window rather
/// than on every RPC. Failures are re-verified every time — a wrong
/// password never enters the cache, so brute force stays KDF-priced — and
/// additionally pay the same per-peer backoff as a failed pgwire SASL
/// exchange (applied BEFORE the KDF, like pgwire applies it before the
/// exchange; cache hits skip it, so authenticated traffic never sleeps).
async fn verify_basic_cached(
    b64: &str,
    auth: &Arc<dyn BasicAuthVerifier>,
    cache: &Mutex<HashMap<String, TokenEntry>>,
    throttle: &crate::ops::AuthThrottle,
    peer: Option<std::net::IpAddr>,
    cache_cap: usize,
) -> Result<String, Status> {
    {
        // Hot path: one lock + one lookup. TTL is enforced on the fetched
        // entry only; expired strangers are pruned on the (rare) insert path
        // below, so per-RPC cost never grows with the number of principals.
        let mut store = cache.lock().expect("basic token lock");
        if let Some(entry) = store.get_mut(b64) {
            if entry.issued.elapsed() < BASIC_CACHE_TTL {
                entry.last_used = Instant::now();
                return Ok(entry.user.clone());
            }
        }
    }
    if let Some(delay) = peer.and_then(|ip| throttle.penalty(ip)) {
        tokio::time::sleep(delay).await;
    }
    // Account a malformed header (bad base64/UTF-8/no colon) as an auth
    // failure too: otherwise a flood of garbage `Basic` headers from a fresh
    // IP neither escalates the per-peer backoff nor shows in the failure
    // metric, since it returns before the KDF path that records both.
    let (user, password) = match decode_basic_credentials(b64) {
        Ok(creds) => creds,
        Err(e) => {
            if let Some(ip) = peer {
                throttle.record_failure(ip);
            }
            crate::metrics::metrics()
                .flight_auth_failures_total
                .fetch_add(1, Ordering::Relaxed);
            return Err(e);
        }
    };
    // The 4096-iteration PBKDF2 is milliseconds of pure CPU: run it on the
    // blocking pool so a burst of cache misses cannot stall the executor
    // threads that are streaming everyone else's DoGet batches.
    let verified = {
        let auth = auth.clone();
        let user = user.clone();
        tokio::task::spawn_blocking(move || auth.verify_password(&user, &password))
            .await
            .map_err(|e| Status::internal(format!("auth verification task failed: {e}")))?
    };
    if !verified {
        if let Some(ip) = peer {
            throttle.record_failure(ip);
        }
        crate::metrics::metrics()
            .flight_auth_failures_total
            .fetch_add(1, Ordering::Relaxed);
        warn!(user, "flight per-RPC basic auth rejected (bad credentials)");
        return Err(Status::unauthenticated(format!(
            "password authentication failed for user \"{user}\""
        )));
    }
    // The insert path doubles as the success audit trail: exactly one line
    // per credential per cache window, never per RPC (a grep can still tie
    // a session's queries to its principal without log flooding).
    info!(user, "flight basic auth verified");
    let mut store = cache.lock().expect("basic token lock");
    prune_token_store(&mut store, BASIC_CACHE_TTL, cache_cap);
    let now = Instant::now();
    store.insert(
        b64.to_string(),
        TokenEntry {
            user: user.clone(),
            issued: now,
            last_used: now,
        },
    );
    Ok(user)
}

struct FlightSqlServiceImpl {
    ctx: Arc<SessionContext>,
    engine: Arc<OverwriteEngine>,
    /// `Some` = basic-auth handshake required (--auth-file); `None` = permissive.
    auth: Option<Arc<dyn BasicAuthVerifier>>,
    /// ReBAC authorizer (--authz-file, managed add-on). `Some` = every data RPC
    /// is gated by the same policy the pgwire path enforces; `None` = open.
    authorizer: Option<SharedAuthorizer>,
    /// Namespace used to resolve unqualified table names in authorization.
    default_namespace: String,
    /// Bearer tokens issued by successful handshakes (per-boot, random) ->
    /// their bound identity and issue time (TTL-pruned on use).
    tokens: Mutex<HashMap<String, TokenEntry>>,
    /// Per-RPC `Basic` credentials already verified against the SCRAM store
    /// (keyed by the raw base64 credential string) -> identity + verify time.
    /// gRPC-web clients cannot run the bidirectional Handshake RPC, so they
    /// authenticate every call with a Basic header; this cache keeps the
    /// 4096-iteration PBKDF2 check off the per-RPC hot path (same TTL and
    /// pruning as handshake tokens). Failed attempts are never cached.
    basic_tokens: Mutex<HashMap<String, TokenEntry>>,
    /// Result-batch IPC compression (`--result-compression`): `Some(ZSTD)`
    /// by default; `None` serves uncompressed batches for clients whose
    /// arrow build lacks the zstd feature.
    ipc_compression: Option<arrow::ipc::CompressionType>,
    /// Per-peer failed-auth backoff — the same throttle (and constants) the
    /// pgwire listener applies before every SASL exchange, here consulted
    /// before handshake and per-RPC `Basic` verification so all three
    /// credential-guessing surfaces slow a brute-forcer identically.
    throttle: Arc<crate::ops::AuthThrottle>,
    /// Wall-clock ceiling on a single DoGet query stream
    /// (`--flight-statement-timeout-ms`; `None` = unbounded). A dashboard
    /// query that runs past it is aborted with DEADLINE_EXCEEDED rather than
    /// tying up an executor thread indefinitely.
    statement_timeout: Option<Duration>,
    /// Byte ceiling on a single DoGet result (`--flight-max-result-bytes`;
    /// `None` = unbounded), counted over the Arrow IPC body bytes actually
    /// streamed. A `SELECT *` on a huge table is cut with RESOURCE_EXHAUSTED
    /// instead of streaming gigabytes into a browser tab.
    max_result_bytes: Option<u64>,
    /// Concurrency cap on in-flight DoGet query streams
    /// (`--flight-max-concurrent-rpcs`; `None` = uncapped) — the Flight
    /// analogue of the pgwire `--max-connections` accept-loop limit, so a
    /// dashboard fleet cannot open unbounded parallel scans.
    rpc_limiter: Option<Arc<tokio::sync::Semaphore>>,
    prepared: Mutex<HashMap<String, Prepared>>,
    prepared_cap: usize,
    prepared_ttl: Duration,
    auth_cache_cap: usize,
    sql_info: SqlInfoData,
    /// Buffered-write overlay source. `Some` only on the tail-api listener
    /// inside `icegres serve --tail-api-port` (the buffering compute is the
    /// only process holding the overlay state); `flight-serve` always runs
    /// with `None` and answers tail tickets with FAILED_PRECONDITION. Powers
    /// the open tail read API (tailapi.rs, docs/open-tail-protocol.md).
    write_buffer: Option<Arc<WriteBuffer>>,
    /// Read-only posture (the tail-api listener): every WRITE RPC is
    /// rejected, because a Flight write executed inside the serve process
    /// would bypass the pgwire BufferHook ordering fences. Reads (plain SQL
    /// and tail tickets) are served — on the tail-api listener they are
    /// union reads over the same providers the pgwire listener uses.
    read_only: bool,
    /// SQL-keyed reusable physical-plan cache — the same machinery, key
    /// shape, and freshness/overlay eligibility rules as pgwire's
    /// plancache.rs. Only ever populated in freshness mode (default mode's
    /// `plan_cache_version` is `None`, so `analyze` rejects every table).
    plans: PlanCache,
    /// One-shot physical plans stashed by GetFlightInfo for the paired
    /// DoGet (the double-planning fix): the ticket carries `{handle, sql}`
    /// and DoGet consumes the stashed plan instead of re-planning; a miss
    /// (TTL expiry, table-version mismatch, eviction, retry, restart)
    /// degrades to a re-plan, never to an error.
    stash: Mutex<HashMap<String, StashedPlan>>,
}

/// A physical plan stashed for its paired one-shot DoGet. Only plans that
/// satisfy the plan cache's eligibility rules (plancache::analyze — every
/// scan a fresh, overlay-free `CachingTableProvider`; no volatile
/// expressions; `plan_safe_to_cache`) are ever stashed, so default mode,
/// overlay-bearing tables, and time-travel/volatile statements always
/// re-plan at DoGet — an Iceberg physical plan pins the file list of the
/// snapshot it was planned against, and executing it after those tables
/// moved would serve a stale read.
struct StashedPlan {
    plan: Arc<dyn ExecutionPlan>,
    created: Instant,
    /// Every table the plan scans, at the metadata version the plan was
    /// built against (plancache::analyze); re-validated when the plan is
    /// consumed.
    tables: Vec<(String, MetadataVersion)>,
}

impl StashedPlan {
    /// Consume the one-shot plan iff it is still sound to execute: within
    /// [`STASH_TTL`] AND every planned table still at its plan-time version
    /// (`resolve` = [`plancache::current_version`] in production — the same
    /// validation a plan-cache hit performs). `None` = the caller re-plans
    /// (a miss, never an error).
    fn take_if_valid_with(
        self,
        resolve: impl Fn(&str) -> Option<MetadataVersion>,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        (self.created.elapsed() < STASH_TTL
            && plancache::versions_current_with(&self.tables, resolve))
        .then_some(self.plan)
    }

    fn take_if_valid(self) -> Option<Arc<dyn ExecutionPlan>> {
        self.take_if_valid_with(plancache::current_version)
    }
}

/// How long a stashed one-shot plan stays consumable. A GetFlightInfo→DoGet
/// (or prepare→execute) pair on a real client is milliseconds apart; the
/// TTL only bounds abandoned entries' memory — staleness is governed by the
/// per-table version re-validation in [`StashedPlan::take_if_valid`], so a
/// DoGet can never execute a plan whose tables moved, however young the
/// entry.
const STASH_TTL: Duration = Duration::from_secs(60);

/// Bound on abandoned stash entries (GetFlightInfo whose DoGet never came).
const STASH_CAP: usize = 512;

/// Marker + separators for a plan-carrying statement ticket:
/// `IGRESP1 \x1f handle \x1f sql`. A ticket without the marker is treated as
/// raw SQL (the pre-P1 format), so older tickets and hand-built ones keep
/// working.
const PLAN_TICKET_PREFIX: &str = "IGRESP1\x1f";

/// Encode a statement ticket carrying the one-shot plan handle AND the SQL
/// (the miss fallback: eviction/restart degrades to today's re-plan).
fn encode_plan_ticket(handle: &str, sql: &str) -> Vec<u8> {
    format!("{PLAN_TICKET_PREFIX}{handle}\x1f{sql}").into_bytes()
}

/// Decode a statement ticket: `(one-shot plan handle, sql)`. Tickets without
/// the marker are raw SQL.
fn decode_plan_ticket(bytes: &[u8]) -> Result<(Option<String>, String), Status> {
    let text = String::from_utf8(bytes.to_vec())
        .map_err(|e| Status::invalid_argument(format!("ticket is not utf-8: {e}")))?;
    match text.strip_prefix(PLAN_TICKET_PREFIX) {
        Some(rest) => match rest.split_once('\x1f') {
            Some((handle, sql)) => Ok((Some(handle.to_string()), sql.to_string())),
            None => Err(Status::invalid_argument("malformed icegres plan ticket")),
        },
        None => Ok((None, text)),
    }
}

impl FlightSqlServiceImpl {
    /// Enforce the bearer token on every RPC when auth is enabled and resolve
    /// it to the authenticated principal. Returns `None` when auth is disabled
    /// (no identity; authorization is also necessarily disabled in that case).
    async fn authorize<T>(&self, request: &Request<T>) -> Result<Option<String>, Status> {
        let Some(auth) = &self.auth else {
            return Ok(None);
        };
        let header = request
            .metadata()
            .get("authorization")
            .ok_or_else(|| {
                Status::unauthenticated(
                    "no authorization header; handshake first or send per-RPC Basic credentials",
                )
            })?
            .to_str()
            .map_err(|_| Status::unauthenticated("authorization header is not valid ASCII"))?;
        if let Some(token) = header.strip_prefix("Bearer ") {
            let mut store = self.tokens.lock().expect("token lock");
            let now = Instant::now();
            store.retain(|_, e| now.duration_since(e.issued) < TOKEN_TTL);
            return match store.get_mut(token) {
                Some(entry) => {
                    entry.last_used = now;
                    Ok(Some(entry.user.clone()))
                }
                None => Err(Status::unauthenticated("unknown or expired bearer token")),
            };
        }
        // Per-RPC Basic credentials: the only auth flow gRPC-web can carry
        // (Handshake is a bidirectional stream, absent from that protocol).
        // Native clients may use it too — it is how ADBC header-auth modes
        // behave against other Flight SQL servers.
        if let Some(b64) = header.strip_prefix("Basic ") {
            let ip = request.remote_addr().map(|a| a.ip());
            return verify_basic_cached(
                b64,
                auth,
                &self.basic_tokens,
                &self.throttle,
                ip,
                self.auth_cache_cap,
            )
            .await
            .map(Some);
        }
        Err(Status::unauthenticated(
            "expected 'Bearer <token>' or 'Basic <credentials>' authorization",
        ))
    }

    /// Gate a SQL statement against the ReBAC policy (no-op when authz is
    /// disabled). Parses `sql` with the same Postgres dialect the pgwire path
    /// uses and denies on the first failed (action, table) check — the exact
    /// enforcement `AuthzHook` applies on pgwire, so neither wire protocol can
    /// reach a table the principal is not granted.
    fn check_sql(&self, principal: &Option<String>, sql: &str) -> Result<(), Status> {
        // Skip the parse entirely when nothing gates SQL (the common
        // permissive, read-write path).
        if !self.read_only && self.authorizer.is_none() {
            return Ok(());
        }
        let stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql)
            .map_err(|e| Status::invalid_argument(format!("sql parse error: {e}")))?;

        // Read-only enforcement (--read-only): reject anything that is not a
        // pure read BEFORE authz and independently of whether authz is
        // configured. This is the single choke point every SQL entry funnels
        // through (query flow, prepared statements, DML-via-DoGet), so a write
        // that executes through the DoGet path — not just execute_update/DoPut
        // — is caught here too. `is_read_only` is fail-closed and
        // statement-form based (never string matching), so DDL (CREATE/CTAS,
        // ALTER, TRUNCATE) is refused here rather than left to the engine's
        // incidental non-support of it.
        if self.read_only {
            for stmt in &stmts {
                if !authz::is_read_only(stmt) {
                    return Err(Status::permission_denied(
                        "this Flight endpoint is read-only (--read-only): only read \
                         statements (SELECT/SHOW/EXPLAIN) are permitted",
                    ));
                }
            }
        }

        let Some(authorizer) = &self.authorizer else {
            return Ok(());
        };
        let user = principal.as_deref().unwrap_or("");
        for stmt in &stmts {
            let decision = authorizer.authorize_sql(user, stmt, &self.default_namespace);
            if let Some(message) = authz::decision_denial_message(user, &decision) {
                return Err(Status::permission_denied(message));
            }
        }
        Ok(())
    }

    /// Gate a tail-API read (which carries no SQL statement) as a read on
    /// the target table — the same ReBAC decision a `SELECT` would get.
    fn check_read(
        &self,
        principal: &Option<String>,
        ident: &iceberg::TableIdent,
    ) -> Result<(), Status> {
        check_read_with(&self.authorizer, principal, ident)
    }

    /// Gate a bulk-ingest append (which carries no SQL statement) as a write on
    /// the target table.
    fn check_write(
        &self,
        principal: &Option<String>,
        namespace: &str,
        table: &str,
    ) -> Result<(), Status> {
        let Some(authorizer) = &self.authorizer else {
            return Ok(());
        };
        let user = principal.as_deref().unwrap_or("");
        let target = TableRef {
            namespace: namespace.to_string(),
            table: table.to_string(),
        };
        let decision = authorizer.check(user, AuthzAction::WriteData, &target);
        if let Some(message) = authz::decision_denial_message(user, &decision) {
            return Err(Status::permission_denied(message));
        }
        Ok(())
    }

    async fn plan(&self, sql: &str) -> Result<DataFrame, Status> {
        self.ctx
            .sql(sql)
            .await
            .map_err(|e| Status::invalid_argument(format!("planning failed: {e}")))
    }

    /// Plan `sql` to a ready-to-execute PHYSICAL plan via ONE `SessionState`
    /// snapshot, consulting the SQL-keyed reusable plan cache first and
    /// feeding it on an eligible miss (identical rules to pgwire's
    /// plancache.rs: freshness mode only, versions validated before AND
    /// after physical planning, overlay-bearing/volatile/table-less shapes
    /// excluded). `stage` names the `ICEGRES_QUERY_TIMING` record for the
    /// miss path; a hit records `plan_cache_hit`. The DoGet/execution path.
    async fn physical_plan(
        &self,
        sql: &str,
        stage: &'static str,
    ) -> Result<Arc<dyn ExecutionPlan>, Status> {
        let timing = crate::timing::enabled();
        let t = timing.then(Instant::now);
        let state = self.ctx.state();
        let key = PlanKey::from_state(&state, sql.to_string());
        if let Some((plan, _schema, _tables)) = self.plans.lookup(&key) {
            // Rebuild internal nodes so per-instance execution state starts
            // fresh (plancache::reset_plan docs); leaf scans are reused.
            let plan = plancache::reset_plan(plan)
                .map_err(|e| Status::internal(format!("plan reset failed: {e}")))?;
            if let Some(t) = t {
                crate::timing::record("plan_cache_hit", t.elapsed());
            }
            return Ok(plan);
        }
        let logical = state
            .create_logical_plan(sql)
            .await
            .map_err(|e| Status::invalid_argument(format!("planning failed: {e}")))?;
        // Cacheability + versions BEFORE physical planning …
        let tables_before = plancache::analyze(&logical);
        let plan = state
            .create_physical_plan(&logical)
            .await
            .map_err(|e| Status::invalid_argument(format!("planning failed: {e}")))?;
        // … and UNCHANGED after (a write/refresh racing the planning window
        // skips caching) — the same soundness dance as PlanCacheHook::run.
        if self.plans.is_enabled() {
            if let Ok(tables) = tables_before {
                if plancache::versions_current(&tables)
                    && !tables.is_empty()
                    && plancache::plan_safe_to_cache(&plan)
                {
                    self.plans.insert(key, plan.clone(), plan.schema(), tables);
                }
            }
        }
        if let Some(t) = t {
            crate::timing::record(stage, t.elapsed());
        }
        Ok(plan)
    }

    /// The GetFlightInfo/prepared-create planning pass: the statement's
    /// result schema, plus the physical plan (as a ready [`StashedPlan`])
    /// when — and only when — it is sound to pin for the paired DoGet
    /// (plancache eligibility: freshness mode, every scan a fresh
    /// overlay-free `CachingTableProvider`, no volatile expressions,
    /// versions unchanged across physical planning, `plan_safe_to_cache`).
    /// Ineligible statements (default mode, overlay-bearing tables,
    /// time-travel/volatile shapes) skip physical planning entirely — the
    /// schema comes from the logical plan (a SELECT's result schema does
    /// not depend on the data snapshot; the pre-P1 wire shape) and the
    /// paired DoGet re-plans, which is where default mode's per-scan
    /// catalog check lives.
    async fn plan_for_schema(&self, sql: &str) -> Result<(SchemaRef, Option<StashedPlan>), Status> {
        let timing = crate::timing::enabled();
        let t = timing.then(Instant::now);
        let state = self.ctx.state();
        let key = PlanKey::from_state(&state, sql.to_string());
        if let Some((plan, schema, tables)) = self.plans.lookup(&key) {
            // No reset here: execution resets at DoGet, so the cache's Arc
            // stays reusable.
            if let Some(t) = t {
                crate::timing::record("plan_cache_hit", t.elapsed());
            }
            let pinned = StashedPlan {
                plan,
                created: Instant::now(),
                tables,
            };
            return Ok((schema, Some(pinned)));
        }
        // Bound the expensive planning work under the same concurrency cap as
        // DoGet streaming: create_logical_plan/create_physical_plan read
        // Iceberg manifests, so an unbounded fleet of GetFlightInfo /
        // CreatePreparedStatement calls could exhaust catalog IO while never
        // touching the cap. Cache hits above return before this and never wait.
        let _permit = self.acquire_permit().await;
        let logical = state
            .create_logical_plan(sql)
            .await
            .map_err(|e| Status::invalid_argument(format!("planning failed: {e}")))?;
        let Ok(tables) = plancache::analyze(&logical) else {
            let schema: SchemaRef = Arc::new(logical.schema().as_arrow().clone());
            if let Some(t) = t {
                crate::timing::record("flight_info_plan", t.elapsed());
            }
            return Ok((schema, None));
        };
        let plan = state
            .create_physical_plan(&logical)
            .await
            .map_err(|e| Status::invalid_argument(format!("planning failed: {e}")))?;
        // Versions must be UNCHANGED across physical planning (a racing
        // write/refresh skips caching AND pinning) — the same soundness
        // dance as PlanCacheHook::run.
        let pin = plancache::versions_current(&tables) && plancache::plan_safe_to_cache(&plan);
        if pin && self.plans.is_enabled() && !tables.is_empty() {
            // Table-less plans are pinnable (nothing to go stale) but,
            // exactly like pgwire, not worth an LRU slot.
            self.plans
                .insert(key, plan.clone(), plan.schema(), tables.clone());
        }
        if let Some(t) = t {
            crate::timing::record("flight_info_plan", t.elapsed());
        }
        let schema = plan.schema();
        Ok((
            schema,
            pin.then(|| StashedPlan {
                plan,
                created: Instant::now(),
                tables,
            }),
        ))
    }

    /// Stash a pin-eligible one-shot plan for its paired DoGet; returns the
    /// ticket handle. Expired/overflow entries are pruned here so abandoned
    /// GetFlightInfos cannot grow the map without bound.
    fn stash_plan(&self, entry: StashedPlan) -> String {
        let handle = uuid::Uuid::new_v4().to_string();
        let mut stash = self.stash.lock().expect("plan stash lock");
        stash.retain(|_, e| e.created.elapsed() < STASH_TTL);
        while stash.len() >= STASH_CAP {
            let Some(oldest) = stash
                .iter()
                .min_by_key(|(_, e)| e.created)
                .map(|(k, _)| k.clone())
            else {
                break;
            };
            stash.remove(&oldest);
        }
        stash.insert(handle.clone(), entry);
        handle
    }

    /// Consume a stashed one-shot plan (`None` on miss, TTL expiry, or a
    /// table-version mismatch — the caller re-plans against fresh state).
    fn take_stashed(&self, handle: &str) -> Option<Arc<dyn ExecutionPlan>> {
        let entry = self.stash.lock().expect("plan stash lock").remove(handle)?;
        entry.take_if_valid()
    }

    /// Execute a physical plan into a DoGet Arrow stream. Streaming in
    /// normal operation; with `ICEGRES_QUERY_TIMING=1`, collect-then-encode
    /// so the `flight_execute` / `flight_encode` stages can be recorded
    /// (the same disclosed buffered-not-streamed diagnostic divergence as
    /// timing.rs and plancache.rs).
    async fn plan_to_stream(&self, plan: Arc<dyn ExecutionPlan>) -> Result<DoGetStream, Status> {
        let task_ctx = self.ctx.task_ctx();
        let schema = plan.schema();
        if crate::timing::enabled() {
            let t = Instant::now();
            let batches = datafusion::physical_plan::collect(plan, task_ctx)
                .await
                .map_err(|e| Status::internal(format!("execution failed: {e}")))?;
            crate::timing::record("flight_execute", t.elapsed());
            let t = Instant::now();
            let data: Vec<Result<arrow_flight::FlightData, Status>> =
                FlightDataEncoderBuilder::new()
                    .with_options(flight_ipc_options(self.ipc_compression))
                    .with_schema(schema)
                    .build(stream::iter(batches.into_iter().map(Ok)))
                    .map_err(Status::from)
                    .collect::<Vec<_>>()
                    .await;
            crate::timing::record("flight_encode", t.elapsed());
            return Ok(self.guard(Box::pin(stream::iter(data))).await);
        }
        let stream = datafusion::physical_plan::execute_stream(plan, task_ctx)
            .map_err(|e| Status::internal(format!("execution failed: {e}")))?;
        let flight = FlightDataEncoderBuilder::new()
            .with_options(flight_ipc_options(self.ipc_compression))
            .with_schema(schema)
            .build(stream.map_err(|e| FlightError::ExternalError(Box::new(e))))
            .map_err(Status::from);
        Ok(self.guard(Box::pin(flight)).await)
    }

    /// Reject writes on a read-only listener. Two listeners set `read_only`:
    /// the tail-api port inside `icegres serve` (a Flight write there would
    /// bypass the pgwire BufferHook ordering fences), and any `flight-serve
    /// --read-only`. SQL-bearing writes are already caught earlier by
    /// `check_sql`; this guards the no-SQL bulk-ingest path (`DoPut` ingest).
    fn reject_if_read_only(&self) -> Result<(), Status> {
        if self.read_only {
            return Err(Status::permission_denied(
                "this Flight endpoint is read-only; INSERT/UPDATE/DELETE and \
                 bulk ingest are not permitted here",
            ));
        }
        Ok(())
    }

    /// UPDATE/DELETE arriving through the QUERY flow (ADBC `cursor.execute`
    /// runs everything as ExecuteQuery → GetFlightInfo/DoGet): execute
    /// through the copy-on-write engine and answer with a DataFusion-style
    /// one-row `count` batch — DataFusion itself plans these but cannot
    /// execute them (its Iceberg providers are append-only).
    async fn dml_via_doget(&self, sql: &str) -> Result<Option<DoGetStream>, Status> {
        let parsed =
            dml::parse_single_dml(sql).map_err(|e| Status::invalid_argument(format!("{e:#}")))?;
        let Some((stmt, _tag)) = parsed else {
            return Ok(None);
        };
        self.reject_if_read_only()?;
        let outcome = self.engine.execute(&stmt).await.map_err(engine_status)?;
        let batch = RecordBatch::try_new(
            Arc::new(count_schema()),
            vec![Arc::new(UInt64Array::from(vec![outcome.rows]))],
        )
        .map_err(|e| Status::internal(format!("count batch failed: {e}")))?;
        Ok(Some(Self::batch_to_stream(batch)))
    }

    /// Execute a planned DataFrame into a DoGet Arrow stream.
    async fn df_to_stream(&self, df: DataFrame) -> Result<DoGetStream, Status> {
        let stream = df
            .execute_stream()
            .await
            .map_err(|e| Status::internal(format!("execution failed: {e}")))?;
        let schema = stream.schema();
        let flight = FlightDataEncoderBuilder::new()
            .with_options(flight_ipc_options(self.ipc_compression))
            .with_schema(schema)
            .build(stream.map_err(|e| FlightError::ExternalError(Box::new(e))))
            .map_err(Status::from);
        Ok(self.guard(Box::pin(flight)).await)
    }

    /// Acquire a concurrency permit when `--flight-max-concurrent-rpcs` is
    /// set; `None` when uncapped. The permit is held for the lifetime of the
    /// returned guard — a DoGet stream (via [`Self::guard`]) or a planning
    /// region (via [`Self::plan_for_schema`]).
    async fn acquire_permit(&self) -> Option<tokio::sync::OwnedSemaphorePermit> {
        match &self.rpc_limiter {
            Some(sem) => Some(
                sem.clone()
                    .acquire_owned()
                    .await
                    .expect("flight rpc semaphore closed"),
            ),
            None => None,
        }
    }

    /// Wrap a data DoGet stream with the resource guards (statement timeout,
    /// result-byte cap, concurrency permit) and the per-RPC metrics. Applied
    /// to every query stream (`plan_to_stream`/`df_to_stream`); metadata
    /// streams are cheap and bypass it. Acquires the concurrency permit here
    /// (await), so an over-cap fleet waits at the choke point rather than
    /// spawning unbounded scans.
    async fn guard(&self, inner: DoGetStream) -> DoGetStream {
        let permit = self.acquire_permit().await;
        crate::metrics::metrics()
            .flight_rpcs_total
            .fetch_add(1, Ordering::Relaxed);
        Box::pin(GuardedStream::new(
            inner,
            self.statement_timeout,
            self.max_result_bytes,
            permit,
        ))
    }

    /// One-batch DoGet stream (metadata responses).
    fn batch_to_stream(batch: RecordBatch) -> DoGetStream {
        let schema = batch.schema();
        let flight = FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(stream::iter([Ok(batch)]))
            .map_err(Status::from);
        Box::pin(flight)
    }

    /// FlightInfo with `schema`, whose single endpoint's ticket is the
    /// encoded command message itself (do_get re-dispatches on it).
    fn make_info(
        schema: &Schema,
        ticket: impl ProstMessageExt,
        descriptor: FlightDescriptor,
    ) -> Result<FlightInfo, Status> {
        let endpoint = FlightEndpoint::new().with_ticket(Ticket {
            ticket: ticket.as_any().encode_to_vec().into(),
        });
        Ok(FlightInfo::new()
            .try_with_schema(schema)
            .map_err(|e| Status::internal(format!("schema encode failed: {e}")))?
            .with_endpoint(endpoint)
            .with_descriptor(descriptor))
    }

    /// Execute a non-query statement (INSERT / UPDATE / DELETE) and return
    /// the affected-row count. UPDATE/DELETE go through the SAME translation
    /// and copy-on-write engine as the pgwire DmlHook (identical scope rules
    /// and sqlstate-typed errors); everything else executes through the
    /// session context (INSERT = iceberg-datafusion append, one commit).
    async fn execute_update(&self, sql: &str, params: Option<ParamValues>) -> Result<i64, Status> {
        self.reject_if_read_only()?;
        let dml_stmt =
            dml::parse_single_dml(sql).map_err(|e| Status::invalid_argument(format!("{e:#}")))?;
        if let Some((stmt, _tag)) = dml_stmt {
            if params.is_some() {
                return Err(Status::unimplemented(
                    "parameterized UPDATE/DELETE ($n bind values) is not supported; \
                     inline the values",
                ));
            }
            let outcome = self.engine.execute(&stmt).await.map_err(engine_status)?;
            return Ok(outcome.rows as i64);
        }
        let mut df = self.plan(sql).await?;
        if let Some(pv) = params {
            df = df
                .with_param_values(pv)
                .map_err(|e| Status::invalid_argument(format!("parameter binding failed: {e}")))?;
        }
        let batches = df
            .collect()
            .await
            .map_err(|e| Status::internal(format!("execution failed: {e}")))?;
        Ok(count_from_batches(&batches))
    }
}

/// The ReadData ReBAC check behind [`FlightSqlServiceImpl::check_read`],
/// free-standing so the Tables discovery filter (do_get_fallback) is
/// unit-testable: TailSnapshot/TailSubscribe deny with this exact decision,
/// and discovery filters with it, so a denied principal sees the table in
/// neither discovery nor data.
fn check_read_with(
    authorizer: &Option<SharedAuthorizer>,
    principal: &Option<String>,
    ident: &iceberg::TableIdent,
) -> Result<(), Status> {
    let Some(authorizer) = authorizer else {
        return Ok(());
    };
    let user = principal.as_deref().unwrap_or("");
    let target = TableRef {
        namespace: ident.namespace().clone().inner().join("."),
        table: ident.name().to_string(),
    };
    let decision = authorizer.check(user, AuthzAction::ReadData, &target);
    if let Some(message) = authz::decision_denial_message(user, &decision) {
        return Err(Status::permission_denied(message));
    }
    Ok(())
}

/// Map engine errors preserving the DML hook's typed semantics: constraint
/// violations surface as invalid-argument (sqlstate in the message), commit
/// conflicts as aborted (retryable), the rest as internal.
fn engine_status(e: anyhow::Error) -> Status {
    if let Some(v) = e.downcast_ref::<ConstraintViolation>() {
        Status::invalid_argument(format!("{}: {}", v.sqlstate, v.message))
    } else if let Some(c) = e.downcast_ref::<CommitConflict>() {
        Status::aborted(format!("40001: {}", c.message))
    } else {
        Status::internal(format!("{e:#}"))
    }
}

/// Affected-row count from a DML plan's result (DataFusion insert/DML plans
/// return a single batch with a `count` UInt64 column).
fn count_from_batches(batches: &[RecordBatch]) -> i64 {
    for batch in batches {
        if let Some(col) = batch.column_by_name("count") {
            if let Some(arr) = col.as_any().downcast_ref::<UInt64Array>() {
                if !arr.is_empty() {
                    return arr.value(0) as i64;
                }
            }
        }
    }
    0
}

/// IPC-encode an Arrow schema (dataset/parameter schema bytes of
/// `ActionCreatePreparedStatementResult`).
fn encode_schema(schema: &Schema) -> Result<prost::bytes::Bytes, Status> {
    let message: IpcMessage = SchemaAsIpc::new(schema, &IpcWriteOptions::default())
        .try_into()
        .map_err(|e| Status::internal(format!("schema encode failed: {e}")))?;
    Ok(message.0)
}

/// Decode the DoPut Arrow stream into record batches (fully buffered). Used by
/// the prepared-statement param-binding paths, where the payload is small
/// (bound parameter rows), not a bulk data upload.
async fn decode_put_stream(stream: PeekableFlightDataStream) -> Result<Vec<RecordBatch>, Status> {
    // into_peekable(), NOT into_inner(): the do_put dispatcher has already
    // peeked the first message (it carries the descriptor AND the schema),
    // and into_inner() would silently drop it.
    FlightRecordBatchStream::new_from_flight_data(stream.into_peekable().map_err(FlightError::from))
        .try_collect::<Vec<_>>()
        .await
        .map_err(|e| Status::invalid_argument(format!("cannot decode bound Arrow data: {e}")))
}

/// Decode the DoPut Arrow stream LAZILY: a batch stream that yields each
/// RecordBatch as it arrives off the wire, never collecting the whole upload.
/// This is what bulk ingest feeds into the streaming append so server memory
/// stays bounded regardless of ingest volume.
fn decode_put_stream_lazy(
    stream: PeekableFlightDataStream,
) -> impl futures::Stream<Item = anyhow::Result<RecordBatch>> {
    FlightRecordBatchStream::new_from_flight_data(stream.into_peekable().map_err(FlightError::from))
        .map(|r| r.map_err(|e| anyhow::anyhow!("cannot decode ingested Arrow data: {e}")))
}

/// Convert bound parameter batches into per-row `$1..$n` value sets.
fn batches_to_param_rows(batches: &[RecordBatch]) -> Result<Vec<Vec<ScalarValue>>, Status> {
    let mut rows = Vec::new();
    for batch in batches {
        for row in 0..batch.num_rows() {
            let mut values = Vec::with_capacity(batch.num_columns());
            for col in batch.columns() {
                values.push(ScalarValue::try_from_array(col, row).map_err(|e| {
                    Status::invalid_argument(format!("unsupported parameter value: {e}"))
                })?);
            }
            rows.push(values);
        }
    }
    Ok(rows)
}

/// SQL LIKE-style filter pattern (`%`, `_`) used by GetDbSchemas/GetTables.
fn like_match(pattern: &str, value: &str) -> bool {
    // Translate into a regex-free recursive matcher (patterns are tiny).
    fn rec(p: &[u8], v: &[u8]) -> bool {
        match p.first() {
            None => v.is_empty(),
            Some(b'%') => (0..=v.len()).any(|i| rec(&p[1..], &v[i..])),
            Some(b'_') => !v.is_empty() && rec(&p[1..], &v[1..]),
            Some(c) => v.first() == Some(c) && rec(&p[1..], &v[1..]),
        }
    }
    rec(pattern.as_bytes(), value.as_bytes())
}

/// Schema of the one-row `count` batch a DataFusion DML plan produces.
fn count_schema() -> Schema {
    Schema::new(vec![arrow::datatypes::Field::new(
        "count",
        arrow::datatypes::DataType::UInt64,
        false,
    )])
}

/// Flight SQL `CommandGetTableTypes` response schema (the metadata builder
/// for it is not exported by arrow-flight 57.3.1, so it is spelled out).
fn table_types_schema() -> Schema {
    Schema::new(vec![arrow::datatypes::Field::new(
        "table_type",
        arrow::datatypes::DataType::Utf8,
        false,
    )])
}

fn build_sql_info() -> SqlInfoData {
    let mut builder = SqlInfoDataBuilder::new();
    builder.append(SqlInfo::FlightSqlServerName, "icegres");
    builder.append(SqlInfo::FlightSqlServerVersion, env!("CARGO_PKG_VERSION"));
    // Arrow IPC format version (Schema.fbs MetadataVersion V5).
    builder.append(SqlInfo::FlightSqlServerArrowVersion, "1.5");
    builder.append(SqlInfo::FlightSqlServerReadOnly, false);
    builder.append(SqlInfo::FlightSqlServerSql, true);
    builder.append(SqlInfo::FlightSqlServerSubstrait, false);
    builder.append(SqlInfo::FlightSqlServerTransaction, 0i32); // none
    builder.append(SqlInfo::FlightSqlServerCancel, false);
    // The killer feature: ADBC bulk ingest lands as one Iceberg commit.
    builder.append(SqlInfo::FlightSqlServerBulkIngestion, true);
    builder.append(SqlInfo::FlightSqlServerIngestTransactionsSupported, false);
    builder.append(SqlInfo::SqlIdentifierQuoteChar, "\"");
    builder.append(SqlInfo::SqlDdlCatalog, false);
    builder.append(SqlInfo::SqlDdlSchema, false);
    builder.append(SqlInfo::SqlDdlTable, false);
    builder.build().expect("static SqlInfo data must build")
}

#[tonic::async_trait]
impl FlightSqlService for FlightSqlServiceImpl {
    type FlightService = FlightSqlServiceImpl;

    /// Basic-auth handshake (only reachable flow the ADBC driver uses when
    /// username/password are set). Permissive mode accepts anything, like
    /// pgwire without --auth-file; enforcing mode verifies against the
    /// SCRAM verifier store and mints a per-boot bearer token.
    async fn do_handshake(
        &self,
        request: Request<Streaming<HandshakeRequest>>,
    ) -> Result<Response<HandshakeStream>, Status> {
        // Authenticated principal bound to the minted token; empty when auth
        // is disabled (no identity, and authz is disabled too).
        let mut authenticated_user = String::new();
        if let Some(source) = &self.auth {
            let header = request
                .metadata()
                .get("authorization")
                .ok_or_else(|| Status::unauthenticated("authorization header not present"))?
                .to_str()
                .map_err(|_| Status::unauthenticated("authorization header not parsable"))?;
            let b64 = header.strip_prefix("Basic ").ok_or_else(|| {
                Status::unauthenticated(format!("only Basic auth is implemented, got {header:?}"))
            })?;
            // Same verification seam as the per-RPC Basic path: identical
            // per-peer backoff (audit #4), KDF off the executor, and a
            // shared success cache — the two credential surfaces cannot
            // drift in throttle or error behavior.
            let peer = request.remote_addr().map(|a| a.ip());
            let user = verify_basic_cached(
                b64,
                source,
                &self.basic_tokens,
                &self.throttle,
                peer,
                self.auth_cache_cap,
            )
            .await?;
            // (verify_basic_cached already counts the failure metric + throttle.)
            info!(user, "flight handshake authenticated");
            authenticated_user = user;
        }
        let token = uuid::Uuid::new_v4().to_string();
        let mut tokens = self.tokens.lock().expect("token lock");
        prune_token_store(&mut tokens, TOKEN_TTL, self.auth_cache_cap);
        let now = Instant::now();
        tokens.insert(
            token.clone(),
            TokenEntry {
                user: authenticated_user,
                issued: now,
                last_used: now,
            },
        );
        drop(tokens);
        let output: HandshakeStream = Box::pin(stream::iter([Ok(HandshakeResponse {
            protocol_version: 0,
            payload: token.clone().into_bytes().into(),
        })]));
        let mut response = Response::new(output);
        let bearer = format!("Bearer {token}");
        response.metadata_mut().insert(
            "authorization",
            MetadataValue::try_from(bearer.as_str())
                .map_err(|_| Status::internal("token not header-safe"))?,
        );
        Ok(response)
    }

    // ------------------------------------------------------------------
    // Queries
    // ------------------------------------------------------------------

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let principal = self.authorize(&request).await?;
        let sql = query.query.clone();
        self.check_sql(&principal, &sql)?;
        debug!(%sql, "GetFlightInfo(CommandStatementQuery)");
        // UPDATE/DELETE cannot physical-plan (append-only providers): keep
        // the historical shape — logical plan for the schema, raw-SQL ticket,
        // DoGet routes through the engine.
        if dml::parse_single_dml(&sql)
            .map_err(|e| Status::invalid_argument(format!("{e:#}")))?
            .is_some()
        {
            let df = self.plan(&sql).await?;
            let schema = df.schema().as_arrow().clone();
            let ticket = TicketStatementQuery {
                statement_handle: sql.into_bytes().into(),
            };
            return Ok(Response::new(Self::make_info(
                &schema,
                ticket,
                request.into_inner(),
            )?));
        }
        // Plan ONCE — to the physical plan when it is sound to pin
        // (freshness-mode eligible; versions re-validated at DoGet) — and
        // hand DoGet the stashed result through the ticket, killing the
        // historical double planning (bench/COMPARISON.md caveat 4). The
        // SQL rides in the ticket as the miss fallback (version mismatch/
        // TTL/eviction/restart degrades to a re-plan); ineligible
        // statements (default mode, overlays, volatile shapes) get a
        // raw-SQL ticket so DoGet always re-plans them against fresh state.
        let (schema, pinned) = self.plan_for_schema(&sql).await?;
        let statement_handle = match pinned {
            Some(entry) => encode_plan_ticket(&self.stash_plan(entry), &sql).into(),
            None => sql.into_bytes().into(),
        };
        let ticket = TicketStatementQuery { statement_handle };
        Ok(Response::new(Self::make_info(
            &schema,
            ticket,
            request.into_inner(),
        )?))
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self::FlightService as FlightService>::DoGetStream>, Status> {
        let principal = self.authorize(&request).await?;
        let (handle, sql) = decode_plan_ticket(&ticket.statement_handle)?;
        // Authorization stays per-RPC: the ticket's SQL is re-checked even
        // when the plan itself comes from the stash.
        self.check_sql(&principal, &sql)?;
        debug!(%sql, "DoGet(TicketStatementQuery)");
        if let Some(stream) = self.dml_via_doget(&sql).await? {
            return Ok(Response::new(stream));
        }
        // One-shot stash hit: execute the plan GetFlightInfo already built —
        // only after take_stashed re-validated every table's current
        // version against the plan-time set.
        if let Some(plan) = handle.as_deref().and_then(|h| self.take_stashed(h)) {
            let timing = crate::timing::enabled();
            let t = timing.then(Instant::now);
            let plan = plancache::reset_plan(plan)
                .map_err(|e| Status::internal(format!("plan reset failed: {e}")))?;
            if let Some(t) = t {
                crate::timing::record("flight_doget_stash_hit", t.elapsed());
            }
            return Ok(Response::new(self.plan_to_stream(plan).await?));
        }
        // Miss (version mismatch/expired/evicted/retried/foreign ticket):
        // re-plan — via the reusable SQL-keyed cache in freshness mode,
        // from scratch otherwise.
        let plan = self.physical_plan(&sql, "flight_doget_plan").await?;
        Ok(Response::new(self.plan_to_stream(plan).await?))
    }

    // ------------------------------------------------------------------
    // Prepared statements (ADBC parameterized queries)
    // ------------------------------------------------------------------

    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        let principal = self.authorize(&request).await?;
        let sql = query.query.clone();
        self.check_sql(&principal, &sql)?;
        debug!(%sql, "CreatePreparedStatement");
        // Plan for the dataset schema; a plan with untyped `$n` placeholders
        // that DataFusion cannot infer still yields a schema for SELECTs.
        // For plain (non-DML) statements, ALSO try to physical-plan now so
        // the paired zero-params DoGet executes instead of re-planning
        // (ADBC's dbapi prepares EVERY statement, so this is the hot path).
        // DML and placeholder-bearing statements cannot physical-plan here;
        // they keep the logical-only schema pass.
        let is_dml = dml::parse_single_dml(&sql)
            .map_err(|e| Status::invalid_argument(format!("{e:#}")))?
            .is_some();
        let (schema_ref, plan): (SchemaRef, Option<StashedPlan>) = if is_dml {
            let df = self.plan(&sql).await?;
            (Arc::new(df.schema().as_arrow().clone()), None)
        } else {
            // Pins the create-time plan only when it is version-validatable
            // (freshness-mode eligible); otherwise DoGet re-plans against
            // fresh state. A statement plan_for_schema cannot plan at all
            // falls back to the upstream logical pass for the schema.
            match self.plan_for_schema(&sql).await {
                Ok((schema, pinned)) => (schema, pinned),
                Err(_) => {
                    let df = self.plan(&sql).await?;
                    (Arc::new(df.schema().as_arrow().clone()), None)
                }
            }
        };
        let dataset_schema = encode_schema(&schema_ref)?;
        // Parameter types are not inferred (DataFusion resolves them at bind
        // time); advertise an empty parameter schema.
        let parameter_schema = encode_schema(&Schema::empty())?;
        let handle = uuid::Uuid::new_v4().to_string();
        let mut prepared = self.prepared.lock().expect("prepared lock");
        make_prepared_room(&mut prepared, self.prepared_ttl, self.prepared_cap);
        let now = Instant::now();
        prepared.insert(
            handle.clone(),
            Prepared {
                owner: principal,
                last_used: now,
                sql,
                params: Vec::new(),
                schema: schema_ref,
                plan,
            },
        );
        Ok(ActionCreatePreparedStatementResult {
            prepared_statement_handle: handle.into_bytes().into(),
            dataset_schema,
            parameter_schema,
        })
    }

    async fn do_action_close_prepared_statement(
        &self,
        query: ActionClosePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<(), Status> {
        let principal = self.authorize(&request).await?;
        let handle = String::from_utf8(query.prepared_statement_handle.to_vec())
            .map_err(|_| Status::invalid_argument("invalid prepared statement handle"))?;
        let mut prepared = self.prepared.lock().expect("prepared lock");
        prune_prepared_store(&mut prepared, self.prepared_ttl);
        let entry = prepared
            .get(&handle)
            .ok_or_else(|| Status::not_found(format!("unknown prepared statement {handle}")))?;
        check_prepared_owner(entry, &principal)?;
        prepared.remove(&handle);
        Ok(())
    }

    async fn do_put_prepared_statement_query(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<DoPutPreparedStatementResult, Status> {
        let principal = self.authorize(&request).await?;
        let handle = String::from_utf8(query.prepared_statement_handle.to_vec())
            .map_err(|_| Status::invalid_argument("invalid prepared statement handle"))?;
        let batches = decode_put_stream(request.into_inner()).await?;
        let rows = batches_to_param_rows(&batches)?;
        let mut prepared = self.prepared.lock().expect("prepared lock");
        prune_prepared_store(&mut prepared, self.prepared_ttl);
        let entry = prepared
            .get_mut(&handle)
            .ok_or_else(|| Status::not_found(format!("unknown prepared statement {handle}")))?;
        check_prepared_owner(entry, &principal)?;
        entry.last_used = Instant::now();
        entry.params = rows;
        Ok(DoPutPreparedStatementResult {
            prepared_statement_handle: Some(query.prepared_statement_handle),
        })
    }

    async fn get_flight_info_prepared_statement(
        &self,
        cmd: CommandPreparedStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let principal = self.authorize(&request).await?;
        let handle = String::from_utf8(cmd.prepared_statement_handle.to_vec())
            .map_err(|_| Status::invalid_argument("invalid prepared statement handle"))?;
        // Answer from the schema captured at create time — no second plan pass.
        let schema = {
            let mut prepared = self.prepared.lock().expect("prepared lock");
            prune_prepared_store(&mut prepared, self.prepared_ttl);
            let entry = prepared
                .get(&handle)
                .ok_or_else(|| Status::not_found(format!("unknown prepared statement {handle}")))?;
            check_prepared_owner(entry, &principal)?;
            let entry = prepared.get_mut(&handle).expect("checked above");
            entry.last_used = Instant::now();
            entry.schema.clone()
        };
        Ok(Response::new(Self::make_info(
            &schema,
            cmd,
            request.into_inner(),
        )?))
    }

    async fn do_get_prepared_statement(
        &self,
        cmd: CommandPreparedStatementQuery,
        request: Request<Ticket>,
    ) -> Result<Response<<Self::FlightService as FlightService>::DoGetStream>, Status> {
        let principal = self.authorize(&request).await?;
        let handle = String::from_utf8(cmd.prepared_statement_handle.to_vec())
            .map_err(|_| Status::invalid_argument("invalid prepared statement handle"))?;
        let (sql, params, stashed) = {
            let mut prepared = self.prepared.lock().expect("prepared lock");
            prune_prepared_store(&mut prepared, self.prepared_ttl);
            let entry = prepared
                .get_mut(&handle)
                .ok_or_else(|| Status::not_found(format!("unknown prepared statement {handle}")))?;
            check_prepared_owner(entry, &principal)?;
            entry.last_used = Instant::now();
            // The create-time physical plan is consumed ONE-SHOT (a second
            // execute of the same handle re-plans for a fresh snapshot).
            (entry.sql.clone(), entry.params.clone(), entry.plan.take())
        };
        self.check_sql(&principal, &sql)?;
        debug!(%sql, bound_rows = params.len(), "DoGet(CommandPreparedStatementQuery)");
        // ADBC's dbapi prepares EVERY statement, so UPDATE/DELETE arrive
        // here too: same engine routing as the plain-statement flow.
        if params.is_empty() {
            if let Some(stream) = self.dml_via_doget(&sql).await? {
                return Ok(Response::new(stream));
            }
            // Zero-params SELECT: execute the plan built at create time
            // (the double-planning fix) — only if every planned table is
            // still at its plan-time version; a mismatch or TTL expiry
            // re-plans instead.
            if let Some(plan) = stashed.and_then(StashedPlan::take_if_valid) {
                let plan = plancache::reset_plan(plan)
                    .map_err(|e| Status::internal(format!("plan reset failed: {e}")))?;
                return Ok(Response::new(self.plan_to_stream(plan).await?));
            }
            let plan = self.physical_plan(&sql, "flight_doget_plan").await?;
            return Ok(Response::new(self.plan_to_stream(plan).await?));
        } else if dml::parse_single_dml(&sql)
            .map_err(|e| Status::invalid_argument(format!("{e:#}")))?
            .is_some()
        {
            return Err(Status::unimplemented(
                "parameterized UPDATE/DELETE ($n bind values) is not supported; \
                 inline the values",
            ));
        }
        let mut df = self.plan(&sql).await?;
        match params.len() {
            0 => {}
            1 => {
                df = df
                    .with_param_values(ParamValues::from(
                        params.into_iter().next().expect("one row"),
                    ))
                    .map_err(|e| {
                        Status::invalid_argument(format!("parameter binding failed: {e}"))
                    })?;
            }
            n => {
                return Err(Status::unimplemented(format!(
                    "binding {n} parameter rows to a query is not supported (bind one row)"
                )))
            }
        }
        Ok(Response::new(self.df_to_stream(df).await?))
    }

    async fn do_put_prepared_statement_update(
        &self,
        query: CommandPreparedStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let principal = self.authorize(&request).await?;
        let handle = String::from_utf8(query.prepared_statement_handle.to_vec())
            .map_err(|_| Status::invalid_argument("invalid prepared statement handle"))?;
        let sql = {
            let mut prepared = self.prepared.lock().expect("prepared lock");
            prune_prepared_store(&mut prepared, self.prepared_ttl);
            let entry = prepared
                .get(&handle)
                .ok_or_else(|| Status::not_found(format!("unknown prepared statement {handle}")))?;
            check_prepared_owner(entry, &principal)?;
            let entry = prepared.get_mut(&handle).expect("checked above");
            entry.last_used = Instant::now();
            entry.sql.clone()
        };
        self.check_sql(&principal, &sql)?;
        let batches = decode_put_stream(request.into_inner()).await?;
        let rows = batches_to_param_rows(&batches)?;
        debug!(%sql, bound_rows = rows.len(), "DoPut(CommandPreparedStatementUpdate)");
        if rows.is_empty() {
            return self.execute_update(&sql, None).await;
        }
        // One execution (= one Iceberg commit) per bound row: correct but
        // slow for bulk data — that is exactly what CommandStatementIngest
        // (adbc_ingest) exists for, and the docs/bench say so.
        let mut affected = 0i64;
        for row in rows {
            affected += self
                .execute_update(&sql, Some(ParamValues::from(row)))
                .await?;
        }
        Ok(affected)
    }

    // ------------------------------------------------------------------
    // DML + bulk ingest
    // ------------------------------------------------------------------

    async fn do_put_statement_update(
        &self,
        ticket: CommandStatementUpdate,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let principal = self.authorize(&request).await?;
        if ticket.transaction_id.is_some() {
            return Err(Status::unimplemented(
                "Flight SQL transactions are not supported",
            ));
        }
        self.check_sql(&principal, &ticket.query)?;
        debug!(sql = %ticket.query, "DoPut(CommandStatementUpdate)");
        self.execute_update(&ticket.query, None).await
    }

    async fn do_put_statement_ingest(
        &self,
        ticket: CommandStatementIngest,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<i64, Status> {
        let principal = self.authorize(&request).await?;
        self.reject_if_read_only()?;
        if ticket.transaction_id.is_some() {
            return Err(Status::unimplemented(
                "ingest transactions are not supported",
            ));
        }
        if ticket.temporary {
            return Err(Status::unimplemented(
                "temporary-table ingest is not supported",
            ));
        }
        if let Some(catalog) = &ticket.catalog {
            if catalog != CATALOG_NAME {
                return Err(Status::not_found(format!(
                    "unknown catalog {catalog:?} (only {CATALOG_NAME:?} is served)"
                )));
            }
        }
        let namespace = ticket
            .schema
            .clone()
            .unwrap_or_else(|| DEFAULT_SCHEMA.to_string());
        let table = ticket.table.clone();
        self.check_write(&principal, &namespace, &table)?;

        // Scope: append into an EXISTING Iceberg table (ADBC mode="append").
        // mode="create"/"replace" would need DDL through the REST catalog —
        // rejected loudly rather than half-implemented.
        let exists = self
            .ctx
            .catalog(CATALOG_NAME)
            .and_then(|c| c.schema(&namespace))
            .is_some_and(|s| s.table_exist(&table));
        if !exists {
            return Err(Status::not_found(format!(
                "table {namespace}.{table} does not exist; icegres bulk ingest appends into \
                 existing tables only (ADBC mode=\"append\"; create the table first)"
            )));
        }
        if let Some(opts) = &ticket.table_definition_options {
            if opts.if_exists() == TableExistsOption::Replace {
                return Err(Status::unimplemented(
                    "ingest mode \"replace\" is not supported (append only)",
                ));
            }
            if opts.if_exists() == TableExistsOption::Fail
                && opts.if_not_exist() == TableNotExistOption::Create
            {
                return Err(Status::already_exists(format!(
                    "table {namespace}.{table} already exists (ADBC mode=\"create\"); \
                     use mode=\"append\""
                )));
            }
        }

        // Stream the upload straight into a rolling Parquet writer and commit
        // it as ONE fast-append: peak memory is bounded by the writer's target
        // file size, NOT the ingest volume. (The prior path collected every
        // batch into a Vec and re-held it in a MemTable through the INSERT, so
        // a large upload was resident in full.) Same one-commit atomicity.
        let ident = iceberg::TableIdent::from_strs([namespace.as_str(), table.as_str()])
            .map_err(|e| Status::invalid_argument(format!("bad table identifier: {e}")))?;
        let batch_stream = decode_put_stream_lazy(request.into_inner());
        let outcome = self
            .engine
            .append_stream(&ident, batch_stream)
            .await
            .map_err(|e| Status::invalid_argument(format!("ingest failed: {e}")))?;
        info!(
            table = %format!("{namespace}.{table}"),
            rows = outcome.rows,
            snapshot_id = ?outcome.snapshot_id,
            "DoPut(CommandStatementIngest): streamed append committed as one Iceberg commit"
        );
        Ok(outcome.rows as i64)
    }

    // ------------------------------------------------------------------
    // Catalog metadata (ADBC get_objects)
    // ------------------------------------------------------------------

    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.authorize(&request).await?;
        let schema = GetCatalogsBuilder::new().schema();
        Ok(Response::new(Self::make_info(
            &schema,
            query,
            request.into_inner(),
        )?))
    }

    async fn do_get_catalogs(
        &self,
        _query: CommandGetCatalogs,
        request: Request<Ticket>,
    ) -> Result<Response<<Self::FlightService as FlightService>::DoGetStream>, Status> {
        self.authorize(&request).await?;
        let mut builder = GetCatalogsBuilder::new();
        builder.append(CATALOG_NAME);
        let batch = builder
            .build()
            .map_err(|e| Status::internal(format!("catalogs batch failed: {e}")))?;
        Ok(Response::new(Self::batch_to_stream(batch)))
    }

    async fn get_flight_info_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.authorize(&request).await?;
        let schema = GetDbSchemasBuilder::new(None::<String>, None::<String>).schema();
        Ok(Response::new(Self::make_info(
            &schema,
            query,
            request.into_inner(),
        )?))
    }

    async fn do_get_schemas(
        &self,
        query: CommandGetDbSchemas,
        request: Request<Ticket>,
    ) -> Result<Response<<Self::FlightService as FlightService>::DoGetStream>, Status> {
        self.authorize(&request).await?;
        let mut builder = GetDbSchemasBuilder::new(
            query.catalog.clone(),
            query.db_schema_filter_pattern.clone(),
        );
        if query.catalog.as_deref().is_none_or(|c| c == CATALOG_NAME) {
            if let Some(catalog) = self.ctx.catalog(CATALOG_NAME) {
                let mut names = catalog.schema_names();
                names.sort();
                for name in names {
                    builder.append(CATALOG_NAME, name);
                }
            }
        }
        let batch = builder
            .build()
            .map_err(|e| Status::internal(format!("schemas batch failed: {e}")))?;
        Ok(Response::new(Self::batch_to_stream(batch)))
    }

    async fn get_flight_info_tables(
        &self,
        query: CommandGetTables,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.authorize(&request).await?;
        let schema = GetTablesBuilder::new(
            None::<String>,
            None::<String>,
            None::<String>,
            Vec::<String>::new(),
            query.include_schema,
        )
        .schema();
        Ok(Response::new(Self::make_info(
            &schema,
            query,
            request.into_inner(),
        )?))
    }

    async fn do_get_tables(
        &self,
        query: CommandGetTables,
        request: Request<Ticket>,
    ) -> Result<Response<<Self::FlightService as FlightService>::DoGetStream>, Status> {
        self.authorize(&request).await?;
        // The builder applies catalog/table-type filters itself; the schema
        // pattern is applied here (we enumerate schemas), the table pattern
        // by the builder at build() time.
        let mut builder = GetTablesBuilder::new(
            query.catalog.clone(),
            query.db_schema_filter_pattern.clone(),
            query.table_name_filter_pattern.clone(),
            query.table_types.clone(),
            query.include_schema,
        );
        let type_ok =
            query.table_types.is_empty() || query.table_types.iter().any(|t| t == TABLE_TYPE);
        if type_ok && query.catalog.as_deref().is_none_or(|c| c == CATALOG_NAME) {
            if let Some(catalog) = self.ctx.catalog(CATALOG_NAME) {
                let mut schema_names = catalog.schema_names();
                schema_names.sort();
                for schema_name in schema_names {
                    if let Some(pat) = &query.db_schema_filter_pattern {
                        if !like_match(pat, &schema_name) {
                            continue;
                        }
                    }
                    let Some(schema) = catalog.schema(&schema_name) else {
                        continue;
                    };
                    let mut table_names = schema.table_names();
                    table_names.sort();
                    for table_name in table_names {
                        if let Some(pat) = &query.table_name_filter_pattern {
                            if !like_match(pat, &table_name) {
                                continue;
                            }
                        }
                        let table_schema: Schema = if query.include_schema {
                            match schema.table(&table_name).await {
                                Ok(Some(provider)) => provider.schema().as_ref().clone(),
                                _ => Schema::empty(),
                            }
                        } else {
                            Schema::empty()
                        };
                        builder
                            .append(
                                CATALOG_NAME,
                                &schema_name,
                                &table_name,
                                TABLE_TYPE,
                                &table_schema,
                            )
                            .map_err(|e| {
                                Status::internal(format!("tables batch append failed: {e}"))
                            })?;
                    }
                }
            }
        }
        let batch = builder
            .build()
            .map_err(|e| Status::internal(format!("tables batch failed: {e}")))?;
        Ok(Response::new(Self::batch_to_stream(batch)))
    }

    async fn get_flight_info_table_types(
        &self,
        query: CommandGetTableTypes,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.authorize(&request).await?;
        Ok(Response::new(Self::make_info(
            &table_types_schema(),
            query,
            request.into_inner(),
        )?))
    }

    async fn do_get_table_types(
        &self,
        _query: CommandGetTableTypes,
        request: Request<Ticket>,
    ) -> Result<Response<<Self::FlightService as FlightService>::DoGetStream>, Status> {
        self.authorize(&request).await?;
        let batch = RecordBatch::try_new(
            Arc::new(table_types_schema()),
            vec![Arc::new(arrow::array::StringArray::from(vec![TABLE_TYPE]))],
        )
        .map_err(|e| Status::internal(format!("table-types batch failed: {e}")))?;
        Ok(Response::new(Self::batch_to_stream(batch)))
    }

    async fn get_flight_info_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.authorize(&request).await?;
        let schema = self.sql_info.schema();
        Ok(Response::new(Self::make_info(
            &schema,
            query,
            request.into_inner(),
        )?))
    }

    async fn do_get_sql_info(
        &self,
        query: CommandGetSqlInfo,
        request: Request<Ticket>,
    ) -> Result<Response<<Self::FlightService as FlightService>::DoGetStream>, Status> {
        self.authorize(&request).await?;
        let batch = self
            .sql_info
            .record_batch(query.info)
            .map_err(|e| Status::internal(format!("sql-info batch failed: {e}")))?;
        Ok(Response::new(Self::batch_to_stream(batch)))
    }

    /// The open tail read API (tailapi.rs, docs/open-tail-protocol.md):
    /// `Tables` / `TailSnapshot` / `TailSubscribe` arrive as DoGet tickets
    /// with icegres.tail.v1.* type URLs. Served only where a WriteBuffer is
    /// attached (the tail-api listener inside a buffering `icegres serve`);
    /// `flight-serve` answers FAILED_PRECONDITION so consumers get a
    /// precise story instead of a generic unimplemented.
    async fn do_get_fallback(
        &self,
        request: Request<Ticket>,
        message: arrow_flight::sql::Any,
    ) -> Result<Response<<Self::FlightService as FlightService>::DoGetStream>, Status> {
        let principal = self.authorize(&request).await?;
        let ticket = crate::tailapi::TailTicket::from_any(&message)
            .map_err(|e| Status::invalid_argument(format!("{e:#}")))?;
        let Some(ticket) = ticket else {
            return Err(Status::unimplemented(format!(
                "do_get: The defined request is invalid: {}",
                message.type_url
            )));
        };
        let Some(buffer) = &self.write_buffer else {
            return Err(Status::failed_precondition(
                "this endpoint is not buffering: the open tail API is served by the \
                 buffering `icegres serve` process (--write-buffer-ms + a durable tail \
                 + --tail-api-port); reads here already see committed data exactly",
            ));
        };
        match ticket {
            crate::tailapi::TailTicket::Tables => {
                // Discovery is filtered per table by the SAME ReadData check
                // the Snapshot/Subscribe arms enforce, so a denied principal
                // sees the table in neither discovery nor data.
                Ok(Response::new(crate::tailapi::tables_stream(
                    buffer,
                    |ident| check_read_with(&self.authorizer, &principal, ident).is_ok(),
                )?))
            }
            crate::tailapi::TailTicket::Snapshot { table } => {
                self.check_read(&principal, &table)?;
                Ok(Response::new(crate::tailapi::snapshot_stream(
                    buffer, &table,
                )?))
            }
            crate::tailapi::TailTicket::Subscribe { table, from_seq } => {
                self.check_read(&principal, &table)?;
                Ok(Response::new(crate::tailapi::subscribe_stream(
                    buffer, &table, from_seq,
                )?))
            }
        }
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

/// Load the basic-auth verifier for a Flight listener (`--auth-file`, same
/// file and semantics as pgwire; managed add-on). `None` = permissive, with
/// the same startup WARN as pgwire.
fn load_basic_auth(auth_file: &Option<PathBuf>) -> Result<Option<Arc<dyn BasicAuthVerifier>>> {
    match auth_file {
        Some(path) => {
            #[cfg(feature = "managed")]
            {
                let source = Arc::new(crate::pgauth::FileAuthSource::load(path)?);
                info!(
                    auth_file = %path.display(),
                    users = source.user_count(),
                    "Flight SQL basic-auth handshake enabled (bearer tokens per connection)"
                );
                Ok(Some(source as Arc<dyn BasicAuthVerifier>))
            }
            #[cfg(not(feature = "managed"))]
            {
                let _ = path;
                anyhow::bail!(
                    "--auth-file is a managed add-on: this open-source build was compiled \
                     without the `managed` feature. Rebuild with --features managed, or omit \
                     --auth-file to run the Flight SQL endpoint open."
                );
            }
        }
        None => {
            warn!(
                "authentication is DISABLED on the Flight SQL endpoint — any/no credentials \
                 accepted; pass --auth-file (env ICEGRES_AUTH_FILE) to require basic auth"
            );
            Ok(None)
        }
    }
}

/// Bind and spawn the tail-api Flight listener inside a buffering
/// `icegres serve` (`--tail-api-port`): the SAME Flight SQL service as
/// `flight-serve`, but constructed read-only and holding the serve
/// process's WriteBuffer — the only process that can answer
/// TailSnapshot/TailSubscribe (docs/open-tail-protocol.md). SQL reads on
/// this listener are union reads over the serve providers. The bind fails
/// startup loudly; the serving task then runs until process shutdown.
/// Plaintext only in v1 (run it on a trusted network; the pgwire listener
/// keeps its own TLS posture).
#[allow(clippy::too_many_arguments)]
pub async fn spawn_tail_api(
    ctx: Arc<SessionContext>,
    engine: Arc<OverwriteEngine>,
    buffer: Arc<WriteBuffer>,
    host: &str,
    port: u16,
    auth_file: Option<PathBuf>,
    authorizer: Option<SharedAuthorizer>,
) -> Result<()> {
    let auth = load_basic_auth(&auth_file)?;
    let service = FlightSqlServiceImpl {
        ctx,
        engine,
        auth,
        authorizer,
        default_namespace: DEFAULT_SCHEMA.to_string(),
        tokens: Mutex::new(HashMap::new()),
        prepared: Mutex::new(HashMap::new()),
        prepared_cap: DEFAULT_PREPARED_CAP,
        prepared_ttl: DEFAULT_PREPARED_TTL,
        auth_cache_cap: DEFAULT_AUTH_CACHE_CAP,
        sql_info: build_sql_info(),
        write_buffer: Some(buffer),
        read_only: true,
        basic_tokens: Mutex::new(HashMap::new()),
        // Fixed ZSTD, NOT --result-compression: the tail-api's only consumers
        // are icegres peer replicas (--peer-tail) and the pyarrow tail reader,
        // both of which always support zstd. The flag exists for browser
        // clients on the main flight listener; gRPC-web is likewise never
        // enabled here (this listener serves no browsers).
        ipc_compression: Some(arrow::ipc::CompressionType::ZSTD),
        throttle: Arc::new(crate::ops::AuthThrottle::default()),
        // The tail-api serves only icegres peers; the browser-oriented
        // resource guards do not apply.
        statement_timeout: None,
        max_result_bytes: None,
        rpc_limiter: None,
        plans: PlanCache::from_env(),
        stash: Mutex::new(HashMap::new()),
    };
    let addr: std::net::SocketAddr = format!("{host}:{port}")
        .parse()
        .with_context(|| format!("invalid tail-api listen address {host}:{port}"))?;
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("cannot bind the tail-api listener on {addr}"))?;
    info!(
        %addr,
        "tail-api listener ready (open tail read API over Arrow Flight; read-only)"
    );
    let svc = FlightServiceServer::new(service)
        .max_decoding_message_size(64 * 1024 * 1024)
        .max_encoding_message_size(64 * 1024 * 1024);
    let shutdown = async {
        let sig = crate::ops::shutdown_signal().await;
        info!(signal = %sig, "shutdown signal received; draining tail-api RPCs");
    };
    tokio::spawn(async move {
        if let Err(e) = tuned_flight_server()
            .add_service(svc)
            .serve_with_incoming_shutdown(tcp_incoming(listener), shutdown)
            .await
        {
            tracing::error!("tail-api listener failed: {e:#}");
        }
    });
    Ok(())
}

/// Listener configuration for [`run`] beyond the bind address (kept as a
/// struct so the CLI surface can grow without another parameter each time).
pub struct ListenerOpts {
    pub auth_file: Option<PathBuf>,
    pub authorizer: Option<SharedAuthorizer>,
    pub tls: Option<(String, String)>,
    pub freshness_ms: u64,
    /// Also answer gRPC-web (`--grpc-web`): tonic-web translates the
    /// fetch()-compatible framing in-process so browsers can run Flight SQL
    /// against this port directly; native gRPC clients are unaffected.
    pub grpc_web: bool,
    /// CORS origin echoed on gRPC-web responses/preflights (`--cors-origin`).
    pub cors_origin: String,
    /// Result-batch IPC compression (`--result-compression`).
    pub ipc_compression: Option<arrow::ipc::CompressionType>,
    /// Per-query wall-clock ceiling (`--flight-statement-timeout-ms`).
    pub statement_timeout: Option<Duration>,
    /// Per-result byte ceiling (`--flight-max-result-bytes`).
    pub max_result_bytes: Option<u64>,
    /// In-flight DoGet concurrency cap (`--flight-max-concurrent-rpcs`).
    pub max_concurrent_rpcs: Option<usize>,
    /// Bound on retained prepared handles.
    pub max_prepared_statements: usize,
    /// Lifetime of an abandoned prepared handle.
    pub prepared_statement_ttl: Duration,
    /// Bound shared by bearer-token and successful Basic-auth caches.
    pub max_auth_cache_entries: usize,
    /// HTTP liveness/metrics port (`--health-port`); `None` = not served.
    pub health_port: Option<u16>,
    /// Reject every write on this listener (`--read-only`).
    pub read_only: bool,
}

/// Run the Flight SQL endpoint (blocks until SIGINT).
pub async fn run(
    opts: &CatalogOpts,
    host: &str,
    port: u16,
    listener_opts: ListenerOpts,
) -> Result<()> {
    let ListenerOpts {
        auth_file,
        authorizer,
        tls,
        freshness_ms,
        grpc_web,
        cors_origin,
        ipc_compression,
        statement_timeout,
        max_result_bytes,
        max_concurrent_rpcs,
        max_prepared_statements,
        prepared_statement_ttl,
        max_auth_cache_entries,
        health_port,
        read_only,
    } = listener_opts;
    let start = std::time::Instant::now();
    // Validate the CORS origin up front: it is inserted into response headers
    // verbatim, so a non-header-safe value must abort startup, not per-RPC.
    let cors_origin = http::HeaderValue::from_str(&cors_origin)
        .with_context(|| format!("--cors-origin {cors_origin:?} is not a valid header value"))?;
    // Build the TLS acceptor up front so a bad cert/key aborts startup (no
    // silent plaintext fallback), exactly like the pgwire listener.
    let tls_acceptor = match &tls {
        // gRPC is HTTP/2 over TLS: advertise the `h2` ALPN token so clients
        // negotiate HTTP/2 instead of refusing the connection. With
        // --grpc-web, also offer `http/1.1` — gRPC-web is legal over either,
        // and fetch() in some proxies/browsers lands on HTTP/1.1.
        Some((cert, key)) => Some(crate::ops::build_tls_acceptor_with_alpn(
            cert,
            key,
            if grpc_web {
                &[b"h2", b"http/1.1"]
            } else {
                &[b"h2"]
            },
        )?),
        None => None,
    };
    let auth = load_basic_auth(&auth_file)?;
    // Mirror the pgwire listener's posture warnings: gRPC-web auth is a
    // per-RPC Basic header, so WITHOUT TLS the password itself crosses the
    // wire on every call — louder than the handshake-once flow, same fix.
    if auth.is_some() && grpc_web && tls.is_none() {
        warn!(
            "--grpc-web with --auth-file but WITHOUT --tls-cert/--tls-key: browser clients send \
             Basic credentials in CLEARTEXT on every RPC; terminate TLS here or in front"
        );
    }
    if auth.is_some() && grpc_web && cors_origin == http::HeaderValue::from_static("*") {
        warn!(
            "--grpc-web with --auth-file but --cors-origin '*': every web origin may drive this \
             authenticated SQL surface from a visitor's browser; pin --cors-origin to the \
             dashboard origin"
        );
    }

    info!(
        catalog_uri = %opts.catalog_uri,
        warehouse = %opts.warehouse,
        s3_endpoint = %opts.s3_endpoint,
        "connecting to Iceberg REST catalog"
    );
    let catalog = context::connect_catalog(opts).await?;
    // Optional HTTP liveness/metrics listener so a standalone flight-serve is
    // scrapeable (the Flight per-RPC metrics render on the same /metrics as
    // pgwire's). No write buffer here — flight-serve does not buffer.
    if let Some(hp) = health_port {
        crate::ops::spawn_health_listener(host, hp, catalog.clone(), None).await?;
    }
    // Same copy-on-write engine as `icegres serve` for UPDATE/DELETE (main
    // branch, PK enforcement off — the pgwire listener owns that posture).
    let engine = Arc::new(OverwriteEngine::connect(catalog.clone(), opts, false, None).await?);
    // Same session wiring as `icegres serve`: snapshot-aware caching schema
    // providers (cache.rs) — reads refresh on snapshot change, so flight
    // clients see pgwire commits and vice versa. `--freshness-ms > 0` rides
    // the same bounded-staleness machinery as `icegres serve` (freshness.rs)
    // and enables the reusable plan cache below.
    if freshness_ms > 0 {
        warn!(
            freshness_ms,
            "bounded-staleness reads are ENABLED on the Flight SQL endpoint \
             (--freshness-ms): scans serve the cached snapshot with NO per-scan \
             catalog check; commits from OTHER writers become visible within ~{} ms \
             plus one refresh round trip (same contract as `icegres serve`). Also \
             enables the physical-plan cache for repeated statements.",
            freshness_ms
        );
    }
    let ctx = context::build_session_context_with(catalog, None, None, None, freshness_ms).await?;
    if freshness_ms > 0 {
        crate::freshness::spawn_refresher(Duration::from_millis(freshness_ms));
    }

    if authorizer.is_some() {
        info!("ReBAC authorization enabled on the Flight SQL endpoint (managed add-on; per-RPC gating, same policy as pgwire)");
    }
    let service = FlightSqlServiceImpl {
        ctx: Arc::new(ctx),
        engine,
        auth,
        authorizer,
        default_namespace: DEFAULT_SCHEMA.to_string(),
        tokens: Mutex::new(HashMap::new()),
        prepared: Mutex::new(HashMap::new()),
        prepared_cap: max_prepared_statements,
        prepared_ttl: prepared_statement_ttl,
        auth_cache_cap: max_auth_cache_entries,
        sql_info: build_sql_info(),
        write_buffer: None,
        read_only,
        basic_tokens: Mutex::new(HashMap::new()),
        ipc_compression,
        throttle: Arc::new(crate::ops::AuthThrottle::default()),
        statement_timeout,
        max_result_bytes,
        rpc_limiter: max_concurrent_rpcs.map(|n| Arc::new(tokio::sync::Semaphore::new(n))),
        plans: PlanCache::from_env(),
        stash: Mutex::new(HashMap::new()),
    };

    let addr: std::net::SocketAddr = format!("{host}:{port}")
        .parse()
        .with_context(|| format!("invalid listen address {host}:{port}"))?;
    // Bind explicitly before serving so "port accepts" == "catalog wired".
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("cannot bind {addr}"))?;
    info!(
        %addr,
        tls = tls_acceptor.is_some(),
        grpc_web,
        read_only,
        statement_timeout_ms = statement_timeout.map(|d| d.as_millis() as u64),
        max_result_bytes,
        max_concurrent_rpcs,
        max_prepared_statements,
        prepared_statement_ttl_secs = prepared_statement_ttl.as_secs(),
        max_auth_cache_entries,
        startup_ms = start.elapsed().as_millis() as u64,
        "flight-serve ready (Arrow Flight SQL)"
    );

    let svc = FlightServiceServer::new(service)
        // Raise the gRPC message ceilings from tonic's 4 MB default so ADBC
        // bulk-ingest DoPut chunks and large single-batch DoGet responses are
        // not rejected mid-stream.
        .max_decoding_message_size(64 * 1024 * 1024)
        .max_encoding_message_size(64 * 1024 * 1024);
    // Drain on SIGTERM (k8s/systemd) as well as SIGINT — tonic stops accepting
    // and lets in-flight RPCs finish before returning.
    let shutdown = async {
        let sig = crate::ops::shutdown_signal().await;
        info!(signal = %sig, "shutdown signal received; draining Flight RPCs");
    };

    // With --grpc-web the same port answers both wires: tonic-web recognizes
    // the `application/grpc-web*` content types and translates them; native
    // gRPC (h2 + `application/grpc`) passes through untouched. CORS sits
    // OUTSIDE the translator so browser preflights (OPTIONS never reaches a
    // gRPC service) are answered before protocol dispatch.
    match (tls_acceptor, grpc_web) {
        (Some(acceptor), true) => {
            tuned_flight_server()
                .accept_http1(true)
                .layer(CorsLayer::new(cors_origin))
                .layer(tonic_web::GrpcWebLayer::new())
                .add_service(svc)
                .serve_with_incoming_shutdown(tls_incoming(listener, acceptor), shutdown)
                .await
                .context("flight sql server (TLS, grpc-web) failed")?;
        }
        (Some(acceptor), false) => {
            tuned_flight_server()
                .add_service(svc)
                .serve_with_incoming_shutdown(tls_incoming(listener, acceptor), shutdown)
                .await
                .context("flight sql server (TLS) failed")?;
        }
        (None, true) => {
            tuned_flight_server()
                .accept_http1(true)
                .layer(CorsLayer::new(cors_origin))
                .layer(tonic_web::GrpcWebLayer::new())
                .add_service(svc)
                .serve_with_incoming_shutdown(tcp_incoming(listener), shutdown)
                .await
                .context("flight sql server (grpc-web) failed")?;
        }
        (None, false) => {
            tuned_flight_server()
                .add_service(svc)
                .serve_with_incoming_shutdown(tcp_incoming(listener), shutdown)
                .await
                .context("flight sql server failed")?;
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// CORS for gRPC-web (browser dashboards)
// ---------------------------------------------------------------------------
//
// Browsers gate cross-origin fetch() behind CORS; a gRPC-web call from a
// dashboard origin needs the preflight answered and the response marked.
// Hand-rolled as ~80 lines instead of adding tower-http to the supply chain
// for three headers. Scope is deliberately exactly gRPC-web's needs: POST
// with the grpc-web content types plus the OPTIONS preflight.

/// Headers a browser is allowed to send on gRPC-web calls. `authorization`
/// is present because per-RPC Basic credentials are the only auth flow the
/// protocol can carry (no Handshake stream).
const CORS_ALLOW_HEADERS: &str = "content-type, x-grpc-web, x-user-agent, authorization, \
     grpc-timeout, grpc-accept-encoding, connect-protocol-version, connect-timeout-ms";
/// Trailer-carried gRPC status surfaced as headers by the grpc-web protocol —
/// the browser client cannot read them unless they are exposed.
const CORS_EXPOSE_HEADERS: &str =
    "grpc-status, grpc-message, grpc-status-details-bin, grpc-encoding";

#[derive(Clone)]
struct CorsLayer {
    origin: http::HeaderValue,
}

impl CorsLayer {
    fn new(origin: http::HeaderValue) -> Self {
        Self { origin }
    }
}

impl<S> tower::Layer<S> for CorsLayer {
    type Service = CorsService<S>;
    fn layer(&self, inner: S) -> Self::Service {
        CorsService {
            inner,
            origin: self.origin.clone(),
        }
    }
}

#[derive(Clone)]
struct CorsService<S> {
    inner: S,
    origin: http::HeaderValue,
}

impl<S, B> tower::Service<http::Request<B>> for CorsService<S>
where
    S: tower::Service<http::Request<B>, Response = http::Response<tonic::body::Body>>,
    S::Future: Send + 'static,
    S::Error: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = futures::future::BoxFuture<'static, Result<Self::Response, Self::Error>>;

    fn poll_ready(
        &mut self,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, req: http::Request<B>) -> Self::Future {
        if req.method() == http::Method::OPTIONS {
            // Preflight: answer directly — OPTIONS never reaches gRPC.
            let resp = http::Response::builder()
                .status(http::StatusCode::NO_CONTENT)
                .header("access-control-allow-origin", self.origin.clone())
                .header("access-control-allow-methods", "POST, OPTIONS")
                .header("access-control-allow-headers", CORS_ALLOW_HEADERS)
                .header("access-control-max-age", "86400")
                // The preflight answer is origin-specific too: a shared cache
                // must not replay it for a different Origin.
                .header("vary", "Origin")
                .body(tonic::body::Body::empty())
                .expect("static preflight response");
            return Box::pin(futures::future::ok(resp));
        }
        let origin = self.origin.clone();
        let fut = self.inner.call(req);
        Box::pin(async move {
            let mut resp = fut.await?;
            let headers = resp.headers_mut();
            headers.insert("access-control-allow-origin", origin);
            headers.insert(
                "access-control-expose-headers",
                http::HeaderValue::from_static(CORS_EXPOSE_HEADERS),
            );
            // Responses differ per Origin: keep shared caches honest.
            headers.insert("vary", http::HeaderValue::from_static("Origin"));
            Ok(resp)
        })
    }
}

/// IPC write options for Flight result streams: ZSTD-compress the Arrow buffers.
///
/// Compression is applied at the Arrow IPC layer (not gRPC-level) so each buffer
/// stays independently decodable and we avoid double-compression. Requires the
/// arrow `ipc_compression` feature; if it were ever built without it,
/// `try_with_compression` errors and we fall back to uncompressed rather than
/// failing the stream.
fn flight_ipc_options(
    compression: Option<arrow::ipc::CompressionType>,
) -> arrow::ipc::writer::IpcWriteOptions {
    arrow::ipc::writer::IpcWriteOptions::default()
        .try_with_compression(compression)
        .unwrap_or_default()
}

/// A tonic server preconfigured for large, long-lived Flight streams.
///
/// An adaptive HTTP/2 flow-control window lets a big columnar `DoGet` grow past
/// hyper's 64 KB default stream window (which otherwise throttles the stream to
/// one window per round trip over any non-loopback RTT), and keepalives keep the
/// connection alive through load balancers during long result streams.
fn tuned_flight_server() -> Server {
    Server::builder()
        .http2_adaptive_window(Some(true))
        .http2_keepalive_interval(Some(std::time::Duration::from_secs(20)))
        .http2_keepalive_timeout(Some(std::time::Duration::from_secs(10)))
        .tcp_keepalive(Some(std::time::Duration::from_secs(30)))
}

/// Adapt a bound TcpListener into the incoming stream tonic expects.
fn tcp_incoming(
    listener: tokio::net::TcpListener,
) -> impl Stream<Item = std::io::Result<tokio::net::TcpStream>> {
    stream::unfold(listener, |listener| async move {
        // Disable Nagle: the ADBC Flight handshake is a sequence of small
        // request/small response RPCs, so Nagle + delayed-ACK adds a ~40 ms
        // loopback stall to every point query. Mirrors icegresd's listeners.
        let item = listener.accept().await.map(|(s, _)| {
            let _ = s.set_nodelay(true);
            s
        });
        Some((item, listener))
    })
}

// ---------------------------------------------------------------------------
// In-process TLS (production-readiness audit #13)
// ---------------------------------------------------------------------------
//
// tonic 0.14 removed server-side TLS from `tonic::transport` (only the client
// `Endpoint` keeps `tls_config`), so we terminate TLS ourselves with the SAME
// rustls stack the pgwire listener uses (`ops::build_tls_acceptor`, pgwire's
// re-exported `tokio_rustls`) and hand tonic a stream of already-handshaked
// connections via `serve_with_incoming`. `TlsConn` is the minimal newtype that
// makes a `TlsStream` usable as a tonic connection: it delegates the byte
// plumbing and reports the peer address through tonic's `Connected` trait —
// this avoids enabling tonic's own `tls-*` feature (which would pull a second
// tokio-rustls and risk a rustls version split against the pinned matrix).

use datafusion_postgres::pgwire::tokio::tokio_rustls::server::TlsStream;
use datafusion_postgres::pgwire::tokio::TlsAcceptor;
use std::net::SocketAddr;
use std::task::Context;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tonic::transport::server::{Connected, TcpConnectInfo};

/// A TLS-terminated connection presented to tonic. Delegates all IO to the
/// inner `TlsStream` and exposes the peer address as its `ConnectInfo`.
///
/// The ConnectInfo type MUST be `TcpConnectInfo`: `Request::remote_addr()`
/// only reads that extension (or `TlsConnectInfo<TcpConnectInfo>`), so a
/// bespoke type here silently returns `None` for every TLS peer — which
/// would disarm the per-peer failed-auth throttle on exactly the
/// TLS-terminated listeners production runs.
struct TlsConn {
    inner: TlsStream<tokio::net::TcpStream>,
    remote: Option<SocketAddr>,
}

impl Connected for TlsConn {
    type ConnectInfo = TcpConnectInfo;
    fn connect_info(&self) -> Self::ConnectInfo {
        TcpConnectInfo {
            local_addr: None,
            remote_addr: self.remote,
        }
    }
}

impl AsyncRead for TlsConn {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_read(cx, buf)
    }
}

impl AsyncWrite for TlsConn {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.inner).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}

/// Accept TCP, TLS-handshake each connection in its own task (so one slow
/// handshake can't stall the accept loop, mirroring the pgwire per-connection
/// model), and yield the completed `TlsConn`s to tonic. A failed handshake is
/// logged and dropped — never surfaced as a stream error that would stop the
/// server.
fn tls_incoming(
    listener: tokio::net::TcpListener,
    acceptor: TlsAcceptor,
) -> impl Stream<Item = std::io::Result<TlsConn>> {
    use futures::SinkExt;
    let (tx, rx) = futures::channel::mpsc::channel::<std::io::Result<TlsConn>>(1024);
    tokio::spawn(async move {
        loop {
            match listener.accept().await {
                Ok((tcp, peer)) => {
                    let _ = tcp.set_nodelay(true);
                    let acceptor = acceptor.clone();
                    let mut tx = tx.clone();
                    tokio::spawn(async move {
                        match acceptor.accept(tcp).await {
                            Ok(tls) => {
                                let _ = tx
                                    .send(Ok(TlsConn {
                                        inner: tls,
                                        remote: Some(peer),
                                    }))
                                    .await;
                            }
                            Err(e) => warn!(%peer, "Flight TLS handshake failed: {e}"),
                        }
                    });
                }
                Err(e) => {
                    warn!("Flight accept error: {e}");
                    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                }
            }
            // tonic dropped the receiver (shutdown): stop accepting.
            if tx.is_closed() {
                break;
            }
        }
    });
    rx
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn like_pattern_semantics() {
        assert!(like_match("%", "anything"));
        assert!(like_match("tri%", "trips"));
        assert!(like_match("%rips", "trips"));
        assert!(like_match("tr_ps", "trips"));
        assert!(!like_match("tri", "trips"));
        assert!(!like_match("x%", "trips"));
        assert!(like_match("demo", "demo"));
        assert!(like_match("", ""));
        assert!(!like_match("", "x"));
    }

    #[test]
    fn flight_ipc_options_zstd_roundtrips() {
        // The arrow `ipc_compression` feature must be compiled in for Flight
        // result compression to take effect; this proves it is on AND that a
        // ZSTD-compressed IPC stream decodes back to the identical batch.
        use arrow::array::{Int64Array, StringArray};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::ipc::reader::StreamReader;
        use arrow::ipc::writer::StreamWriter;

        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec!["alpha", "beta", "gamma"])),
            ],
        )
        .unwrap();

        // Options must actually carry a compression codec (not silently None).
        let opts = flight_ipc_options(Some(arrow::ipc::CompressionType::ZSTD));
        let mut buf = Vec::new();
        {
            let mut w = StreamWriter::try_new_with_options(&mut buf, &schema, opts).unwrap();
            w.write(&batch).unwrap();
            w.finish().unwrap();
        }
        let mut reader = StreamReader::try_new(std::io::Cursor::new(&buf), None).unwrap();
        let decoded = reader.next().unwrap().unwrap();
        assert_eq!(decoded, batch);

        // --result-compression none: the stream must stay decodable AND be
        // larger than the compressed one (proof compression was really off).
        let plain_opts = flight_ipc_options(None);
        let mut plain = Vec::new();
        {
            let mut w =
                StreamWriter::try_new_with_options(&mut plain, &schema, plain_opts).unwrap();
            // Repetitive payload so ZSTD has something to win on.
            let big = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![7; 4096])),
                    Arc::new(StringArray::from(vec!["repetitive"; 4096])),
                ],
            )
            .unwrap();
            w.write(&big).unwrap();
            w.finish().unwrap();
        }
        let mut zstd = Vec::new();
        {
            let mut w = StreamWriter::try_new_with_options(
                &mut zstd,
                &schema,
                flight_ipc_options(Some(arrow::ipc::CompressionType::ZSTD)),
            )
            .unwrap();
            let big = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![7; 4096])),
                    Arc::new(StringArray::from(vec!["repetitive"; 4096])),
                ],
            )
            .unwrap();
            w.write(&big).unwrap();
            w.finish().unwrap();
        }
        assert!(zstd.len() < plain.len() / 2, "zstd should compress heavily");
        let mut reader = StreamReader::try_new(std::io::Cursor::new(&plain), None).unwrap();
        assert_eq!(reader.next().unwrap().unwrap().num_rows(), 4096);
    }

    #[test]
    fn basic_credentials_decode_padded_and_unpadded() {
        // Padded (standard clients) and unpadded (Go ADBC) both decode.
        let padded = base64::engine::general_purpose::STANDARD.encode("u:pw");
        assert_eq!(
            decode_basic_credentials(&padded).unwrap(),
            ("u".into(), "pw".into())
        );
        let unpadded = padded.trim_end_matches('=').to_string();
        assert_eq!(
            decode_basic_credentials(&unpadded).unwrap(),
            ("u".into(), "pw".into())
        );
        // Password may itself contain ':' — only the FIRST split counts.
        let tricky = base64::engine::general_purpose::STANDARD.encode("u:p:w");
        assert_eq!(
            decode_basic_credentials(&tricky).unwrap(),
            ("u".into(), "p:w".into())
        );
        assert!(decode_basic_credentials("!!notbase64!!").is_err());
        let no_colon = base64::engine::general_purpose::STANDARD.encode("nocolon");
        assert!(decode_basic_credentials(&no_colon).is_err());
    }

    /// Stub verifier counting KDF invocations, so the cache behavior of
    /// per-RPC Basic auth is observable.
    struct CountingVerifier {
        calls: std::sync::atomic::AtomicUsize,
    }
    impl BasicAuthVerifier for CountingVerifier {
        fn verify_password(&self, user: &str, password: &str) -> bool {
            self.calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            user == "bench" && password == "secret"
        }
    }

    /// Build a GuardedStream over `items` with the given guards (no permit).
    fn guarded(
        items: Vec<Result<arrow_flight::FlightData, Status>>,
        timeout: Option<Duration>,
        byte_budget: Option<u64>,
    ) -> GuardedStream {
        GuardedStream::new(Box::pin(stream::iter(items)), timeout, byte_budget, None)
    }

    fn fd(body_len: usize) -> arrow_flight::FlightData {
        arrow_flight::FlightData {
            data_body: vec![0u8; body_len].into(),
            ..Default::default()
        }
    }

    #[tokio::test]
    async fn guarded_stream_enforces_result_byte_cap() {
        // Budget 100 bytes: the first 60-byte batch passes, the second (total
        // 120 > 100) is replaced by RESOURCE_EXHAUSTED and the stream ends.
        let mut s = guarded(vec![Ok(fd(60)), Ok(fd(60)), Ok(fd(60))], None, Some(100));
        assert!(s.next().await.unwrap().is_ok());
        let err = s.next().await.unwrap().unwrap_err();
        assert_eq!(err.code(), tonic::Code::ResourceExhausted);
        assert!(s.next().await.is_none(), "stream fuses after the cap");
    }

    #[tokio::test(start_paused = true)]
    async fn guarded_stream_enforces_statement_timeout() {
        use futures::stream::StreamExt as _;
        // A stream that never yields, guarded by a 50 ms deadline: advancing
        // the paused clock past it makes poll_next return DEADLINE_EXCEEDED
        // even though the inner stream is still pending.
        let inner: DoGetStream = Box::pin(stream::pending());
        let mut s = GuardedStream::new(inner, Some(Duration::from_millis(50)), None, None);
        tokio::time::advance(Duration::from_millis(60)).await;
        let err = s.next().await.unwrap().unwrap_err();
        assert_eq!(err.code(), tonic::Code::DeadlineExceeded);
    }

    #[tokio::test(start_paused = true)]
    async fn per_rpc_basic_auth_caches_successes_and_throttles_failures() {
        let verifier = Arc::new(CountingVerifier {
            calls: std::sync::atomic::AtomicUsize::new(0),
        });
        let auth: Arc<dyn BasicAuthVerifier> = verifier.clone();
        let cache = Mutex::new(HashMap::new());
        let throttle = crate::ops::AuthThrottle::default();
        let peer = Some(std::net::IpAddr::from([10, 0, 0, 7]));
        let good = base64::engine::general_purpose::STANDARD.encode("bench:secret");
        let bad = base64::engine::general_purpose::STANDARD.encode("bench:wrong");

        // First success runs the KDF; the repeat is served from the cache.
        let got = verify_basic_cached(
            &good,
            &auth,
            &cache,
            &throttle,
            peer,
            DEFAULT_AUTH_CACHE_CAP,
        )
        .await;
        assert_eq!(got.unwrap(), "bench");
        let got = verify_basic_cached(
            &good,
            &auth,
            &cache,
            &throttle,
            peer,
            DEFAULT_AUTH_CACHE_CAP,
        )
        .await;
        assert_eq!(got.unwrap(), "bench");
        assert_eq!(verifier.calls.load(std::sync::atomic::Ordering::SeqCst), 1);

        // Failures are never cached: every wrong attempt pays the KDF — and
        // escalates the same per-peer backoff pgwire applies (visible here as
        // paused-clock time consumed by the pre-verification sleep).
        let t0 = tokio::time::Instant::now();
        assert!(
            verify_basic_cached(&bad, &auth, &cache, &throttle, peer, DEFAULT_AUTH_CACHE_CAP,)
                .await
                .is_err()
        );
        assert_eq!(t0.elapsed(), Duration::ZERO, "first failure pays no delay");
        assert!(
            verify_basic_cached(&bad, &auth, &cache, &throttle, peer, DEFAULT_AUTH_CACHE_CAP,)
                .await
                .is_err()
        );
        assert!(
            t0.elapsed() >= Duration::from_millis(250),
            "second attempt from the throttled peer must back off"
        );
        assert_eq!(verifier.calls.load(std::sync::atomic::Ordering::SeqCst), 3);

        // A throttled peer does NOT slow already-authenticated traffic: the
        // cached credential still short-circuits before the penalty.
        let t1 = tokio::time::Instant::now();
        let got = verify_basic_cached(
            &good,
            &auth,
            &cache,
            &throttle,
            peer,
            DEFAULT_AUTH_CACHE_CAP,
        )
        .await;
        assert_eq!(got.unwrap(), "bench");
        assert_eq!(t1.elapsed(), Duration::ZERO, "cache hits skip the backoff");
    }

    fn prepared_for_test(owner: Option<&str>, idle: Duration) -> Prepared {
        let now = Instant::now();
        Prepared {
            owner: owner.map(str::to_string),
            last_used: now - idle,
            sql: "select 1".to_string(),
            params: Vec::new(),
            schema: Arc::new(Schema::empty()),
            plan: None,
        }
    }

    #[test]
    fn retained_flight_state_is_ttl_pruned_and_lru_bounded() {
        let now = Instant::now();
        let mut tokens = HashMap::from([
            (
                "expired".to_string(),
                TokenEntry {
                    user: "old".to_string(),
                    issued: now - Duration::from_secs(120),
                    last_used: now - Duration::from_secs(120),
                },
            ),
            (
                "least-recent".to_string(),
                TokenEntry {
                    user: "a".to_string(),
                    issued: now,
                    last_used: now - Duration::from_secs(2),
                },
            ),
            (
                "recent".to_string(),
                TokenEntry {
                    user: "b".to_string(),
                    issued: now,
                    last_used: now - Duration::from_secs(1),
                },
            ),
        ]);
        prune_token_store(&mut tokens, Duration::from_secs(60), 2);
        assert_eq!(tokens.len(), 1, "one slot is reserved for the new token");
        assert!(tokens.contains_key("recent"));

        let mut prepared = HashMap::from([
            (
                "expired".to_string(),
                prepared_for_test(Some("alice"), Duration::from_secs(20)),
            ),
            (
                "least-recent".to_string(),
                prepared_for_test(Some("alice"), Duration::from_secs(2)),
            ),
            (
                "recent".to_string(),
                prepared_for_test(Some("alice"), Duration::from_secs(1)),
            ),
        ]);
        make_prepared_room(&mut prepared, Duration::from_secs(10), 2);
        assert_eq!(prepared.len(), 1, "one slot is reserved for the new handle");
        assert!(prepared.contains_key("recent"));
    }

    #[test]
    fn prepared_handles_are_bound_to_the_creating_principal() {
        let prepared = prepared_for_test(Some("alice"), Duration::ZERO);
        assert!(check_prepared_owner(&prepared, &Some("alice".to_string())).is_ok());
        let err = check_prepared_owner(&prepared, &Some("bob".to_string())).unwrap_err();
        assert_eq!(err.code(), tonic::Code::PermissionDenied);
        assert!(check_prepared_owner(&prepared, &None).is_err());

        let anonymous = prepared_for_test(None, Duration::ZERO);
        assert!(check_prepared_owner(&anonymous, &None).is_ok());
    }

    /// Inner stub for the CORS layer: answers every request with 200 and a
    /// marker header, so pass-through vs short-circuit is distinguishable.
    #[derive(Clone)]
    struct OkService;
    impl tower::Service<http::Request<()>> for OkService {
        type Response = http::Response<tonic::body::Body>;
        type Error = std::convert::Infallible;
        type Future = futures::future::Ready<Result<Self::Response, Self::Error>>;
        fn poll_ready(
            &mut self,
            _cx: &mut std::task::Context<'_>,
        ) -> std::task::Poll<Result<(), Self::Error>> {
            std::task::Poll::Ready(Ok(()))
        }
        fn call(&mut self, _req: http::Request<()>) -> Self::Future {
            let resp = http::Response::builder()
                .status(200)
                .header("x-inner", "reached")
                .body(tonic::body::Body::empty())
                .unwrap();
            futures::future::ready(Ok(resp))
        }
    }

    #[tokio::test]
    async fn cors_layer_preflight_and_response_marking() {
        use tower::Layer as _;
        let origin = http::HeaderValue::from_static("https://dash.example");
        let mut svc = CorsLayer::new(origin).layer(OkService);

        // OPTIONS preflight is answered by the layer itself (never reaches
        // the gRPC service) with the allow set browsers require.
        let preflight = http::Request::builder()
            .method(http::Method::OPTIONS)
            .uri("/arrow.flight.protocol.FlightService/DoGet")
            .body(())
            .unwrap();
        let resp = tower::Service::call(&mut svc, preflight).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::NO_CONTENT);
        assert!(
            resp.headers().get("x-inner").is_none(),
            "must not pass through"
        );
        assert_eq!(
            resp.headers().get("access-control-allow-origin").unwrap(),
            "https://dash.example"
        );
        let allowed = resp
            .headers()
            .get("access-control-allow-headers")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(
            allowed.contains("authorization"),
            "Basic auth header must be allowed"
        );
        assert_eq!(
            resp.headers().get("vary").unwrap(),
            "Origin",
            "preflight is origin-specific: shared caches must not replay it"
        );

        // A normal call passes through and gets origin + exposed trailers.
        let post = http::Request::builder()
            .method(http::Method::POST)
            .uri("/arrow.flight.protocol.FlightService/DoGet")
            .body(())
            .unwrap();
        let resp = tower::Service::call(&mut svc, post).await.unwrap();
        assert_eq!(resp.status(), http::StatusCode::OK);
        assert_eq!(resp.headers().get("x-inner").unwrap(), "reached");
        assert_eq!(
            resp.headers().get("access-control-allow-origin").unwrap(),
            "https://dash.example"
        );
        assert!(resp
            .headers()
            .get("access-control-expose-headers")
            .unwrap()
            .to_str()
            .unwrap()
            .contains("grpc-status"));
        // A per-origin ACAO response must carry Vary: Origin so shared caches
        // do not serve one origin's allow header to another.
        assert_eq!(resp.headers().get("vary").unwrap(), "Origin");
    }

    #[test]
    fn plan_ticket_roundtrip_and_raw_sql_fallback() {
        // Plan-carrying tickets round-trip handle + SQL (the SQL may itself
        // contain the separator — only the FIRST split counts) …
        let bytes = encode_plan_ticket("abc-123", "select * from t where x = '\x1f? no'");
        let (handle, sql) = decode_plan_ticket(&bytes).unwrap();
        assert_eq!(handle.as_deref(), Some("abc-123"));
        assert_eq!(sql, "select * from t where x = '\x1f? no'");
        // … and a marker-less ticket (pre-P1 / hand-built) is raw SQL.
        let (handle, sql) = decode_plan_ticket(b"select 1").unwrap();
        assert_eq!(handle, None);
        assert_eq!(sql, "select 1");
    }

    // Malformed client-supplied ticket bytes must never panic (SOTA fuzz
    // deliverable). `decode_plan_ticket` is private to this module, so its
    // fuzz target lives here, driven by the shared harness in `crate::fuzz`.
    #[test]
    fn fuzz_decode_plan_ticket_never_panics() {
        let corpus = crate::fuzz::plan_ticket_corpus();
        crate::fuzz::run(
            "decode_plan_ticket",
            0x0DEC_0DE0_0000_0007,
            16_000,
            &corpus,
            |b| {
                let _ = decode_plan_ticket(b);
            },
        );
    }

    #[test]
    fn stashed_plan_version_mismatch_forces_replan() {
        use datafusion::physical_plan::empty::EmptyExec;
        let schema = Arc::new(Schema::empty());
        let v1: MetadataVersion = (Some("v1".into()), Some(1));
        let v2: MetadataVersion = (Some("v2".into()), Some(2));
        let entry = |tables: Vec<(String, MetadataVersion)>, created: Instant| StashedPlan {
            plan: Arc::new(EmptyExec::new(schema.clone())),
            created,
            tables,
        };
        let pinned = vec![("demo\u{1f}t".to_string(), v1.clone())];
        // Fresh entry, every table still at its plan-time version: the
        // one-shot plan is consumable.
        assert!(entry(pinned.clone(), Instant::now())
            .take_if_valid_with(|_| Some(v1.clone()))
            .is_some());
        // The table committed since plan time (current version moved): NOT
        // consumable — DoGet re-plans (the stale-ticket fix). Never an error.
        assert!(entry(pinned.clone(), Instant::now())
            .take_if_valid_with(|_| Some(v2.clone()))
            .is_none());
        // Table no longer resolvable (invalidated/deregistered/default
        // mode): re-plan.
        assert!(entry(pinned.clone(), Instant::now())
            .take_if_valid_with(|_| None)
            .is_none());
        // TTL still bounds abandoned entries, version match notwithstanding.
        let expired = Instant::now() - (STASH_TTL + Duration::from_secs(1));
        assert!(entry(pinned, expired)
            .take_if_valid_with(|_| Some(v1.clone()))
            .is_none());
        // A table-less plan has no version to go stale.
        assert!(entry(Vec::new(), Instant::now())
            .take_if_valid_with(|_| None)
            .is_some());
    }

    #[cfg(feature = "managed")]
    #[test]
    fn tables_discovery_filter_hides_denied_tables() {
        // The exact filter do_get_fallback applies to Tables discovery:
        // a principal denied ReadData on a table must not learn the table
        // exists (same decision the Snapshot/Subscribe arms enforce).
        let policy = std::env::temp_dir().join(format!("icegres-authz-{}", uuid::Uuid::new_v4()));
        std::fs::write(&policy, "grant alice read demo.visible\n").unwrap();
        let authorizer: Option<SharedAuthorizer> = Some(Arc::new(
            crate::authz::FileAuthorizer::load(&policy).unwrap(),
        ));
        std::fs::remove_file(&policy).ok();
        let ident = |name: &str| iceberg::TableIdent::from_strs(["demo", name]).unwrap();
        let mut tables = vec![ident("visible"), ident("secret")];
        let principal = Some("alice".to_string());
        tables.retain(|t| check_read_with(&authorizer, &principal, t).is_ok());
        assert_eq!(tables, vec![ident("visible")]);
        // Snapshot/Subscribe deny the same table with the same decision.
        assert!(check_read_with(&authorizer, &principal, &ident("secret")).is_err());
        // Without an authorizer (open mode) nothing is filtered.
        let mut tables = vec![ident("visible"), ident("secret")];
        tables.retain(|t| check_read_with(&None, &principal, t).is_ok());
        assert_eq!(tables.len(), 2);
    }

    #[test]
    fn read_only_write_classification() {
        // The exact predicate check_sql uses under --read-only: a statement is
        // refused unless it is positively classified read-only. Fail-closed
        // and statement-form based (never string matching), so it catches a
        // write no matter which RPC path executes it.
        let writes = |sql: &str| {
            let stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql).unwrap();
            stmts.iter().any(|stmt| !authz::is_read_only(stmt))
        };
        // Reads pass.
        assert!(!writes("SELECT * FROM demo.trips"));
        assert!(!writes("WITH t AS (SELECT 1) SELECT * FROM t"));
        assert!(!writes(
            "SELECT city, count(*) FROM demo.trips GROUP BY city"
        ));
        assert!(!writes("EXPLAIN SELECT * FROM demo.trips"));
        assert!(!writes("SHOW search_path"));
        // DML is caught — including INSERT, which executes through the DoGet
        // query flow and so is NOT covered by reject_if_read_only.
        assert!(writes("INSERT INTO demo.trips (trip_id) VALUES (1)"));
        assert!(writes("UPDATE demo.trips SET city = 'x' WHERE trip_id = 1"));
        assert!(writes("DELETE FROM demo.trips WHERE trip_id = 1"));
        assert!(writes("DROP TABLE demo.trips"));
        // DDL is caught too — required_checks emits no data-plane check for
        // these, so they would slip past a WriteData/DropTable-only test.
        assert!(writes("CREATE TABLE demo.evil AS SELECT * FROM demo.trips"));
        assert!(writes("CREATE TABLE demo.evil (x INT)"));
        assert!(writes("TRUNCATE TABLE demo.trips"));
        assert!(writes("ALTER TABLE demo.trips ADD COLUMN x INT"));
        // EXPLAIN ANALYZE executes its inner statement: read-only only when
        // that statement is.
        assert!(!writes("EXPLAIN ANALYZE SELECT * FROM demo.trips"));
        assert!(writes(
            "EXPLAIN ANALYZE INSERT INTO demo.trips (trip_id) VALUES (1)"
        ));
        // A write wrapped in a top-level query (parses as Statement::Query with
        // a write body) must NOT slip past the Query arm.
        assert!(writes(
            "WITH t AS (SELECT 1) INSERT INTO demo.trips SELECT * FROM t"
        ));
        // Benign metadata reads stay allowed (not denied by the tightening).
        assert!(!writes("SHOW TABLES"));
        assert!(!writes("DESCRIBE demo.trips"));
    }

    #[test]
    fn count_extraction_defaults_to_zero() {
        assert_eq!(count_from_batches(&[]), 0);
        let schema = Arc::new(Schema::new(vec![arrow::datatypes::Field::new(
            "count",
            arrow::datatypes::DataType::UInt64,
            false,
        )]));
        let batch =
            RecordBatch::try_new(schema, vec![Arc::new(UInt64Array::from(vec![42u64]))]).unwrap();
        assert_eq!(count_from_batches(&[batch]), 42);
    }
}
