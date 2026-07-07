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
//! auth sends the password itself, so run flight-serve behind TLS or on a
//! trusted network. TLS termination is NOT built into the listener (tonic's
//! TLS stack would add a second rustls config surface); terminate TLS in
//! front (nginx/envoy grpc_pass, or any gRPC-aware LB) — the ADBC driver
//! supports `grpc+tls://`. Without `--auth-file` the endpoint is permissive
//! (any/no credentials accepted) and logs the same startup WARN as pgwire.

use std::collections::HashMap;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
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
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::prelude::{DataFrame, SessionContext};
use futures::{stream, Stream, TryStreamExt};
use prost::Message;
use tonic::metadata::MetadataValue;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};
use tracing::{debug, info, warn};

use crate::authz::{self, Action as AuthzAction, SharedAuthorizer, TableRef};
use crate::context::{self, CATALOG_NAME, DEFAULT_SCHEMA};
use crate::ops::BasicAuthVerifier;
use crate::overwrite::{quote_ident, CommitConflict, ConstraintViolation, OverwriteEngine};
use crate::{dml, CatalogOpts};
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

/// A prepared statement: the SQL text plus the last bound parameter rows
/// (`DoPut(CommandPreparedStatementQuery)` replaces them on every bind).
struct Prepared {
    sql: String,
    /// Bound parameter rows; each row is one `$1..$n` value set.
    params: Vec<Vec<ScalarValue>>,
    /// Dataset (result) schema, planned once at create time. `GetFlightInfo`
    /// answers from this instead of re-planning the SQL a second time; a
    /// SELECT's result schema does not depend on the data snapshot, so this is
    /// safe while `DoGet` still re-plans for snapshot-fresh execution.
    schema: SchemaRef,
}

/// Lifetime of a bearer token minted by a handshake. After this the client
/// must re-handshake; expired tokens are pruned lazily on the next RPC so the
/// token map cannot grow without bound across a long-lived server.
const TOKEN_TTL: Duration = Duration::from_secs(3600);

/// A minted bearer token's bound identity and issue time.
struct TokenEntry {
    /// The authenticated principal (empty string when auth is disabled).
    user: String,
    issued: Instant,
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
    prepared: Mutex<HashMap<String, Prepared>>,
    sql_info: SqlInfoData,
}

impl FlightSqlServiceImpl {
    /// Enforce the bearer token on every RPC when auth is enabled and resolve
    /// it to the authenticated principal. Returns `None` when auth is disabled
    /// (no identity; authorization is also necessarily disabled in that case).
    fn authorize<T>(&self, request: &Request<T>) -> Result<Option<String>, Status> {
        if self.auth.is_none() {
            return Ok(None);
        }
        let header = request
            .metadata()
            .get("authorization")
            .ok_or_else(|| Status::unauthenticated("no authorization header; handshake first"))?
            .to_str()
            .map_err(|_| Status::unauthenticated("authorization header is not valid ASCII"))?;
        let token = header
            .strip_prefix("Bearer ")
            .ok_or_else(|| Status::unauthenticated("expected 'Bearer <token>' authorization"))?;
        let mut store = self.tokens.lock().expect("token lock");
        let now = Instant::now();
        store.retain(|_, e| now.duration_since(e.issued) < TOKEN_TTL);
        match store.get(token) {
            Some(entry) => Ok(Some(entry.user.clone())),
            None => Err(Status::unauthenticated("unknown or expired bearer token")),
        }
    }

    /// Gate a SQL statement against the ReBAC policy (no-op when authz is
    /// disabled). Parses `sql` with the same Postgres dialect the pgwire path
    /// uses and denies on the first failed (action, table) check — the exact
    /// enforcement `AuthzHook` applies on pgwire, so neither wire protocol can
    /// reach a table the principal is not granted.
    fn check_sql(&self, principal: &Option<String>, sql: &str) -> Result<(), Status> {
        let Some(authorizer) = &self.authorizer else {
            return Ok(());
        };
        let user = principal.as_deref().unwrap_or("");
        let stmts = Parser::parse_sql(&PostgreSqlDialect {}, sql)
            .map_err(|e| Status::invalid_argument(format!("sql parse error: {e}")))?;
        for stmt in &stmts {
            if let authz::Decision::Deny { action, target } =
                authorizer.authorize_sql(user, stmt, &self.default_namespace)
            {
                return Err(Status::permission_denied(authz::deny_message(
                    user, action, &target,
                )));
            }
        }
        Ok(())
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
        if let authz::Decision::Deny { action, target } =
            authorizer.check(user, AuthzAction::WriteData, &target)
        {
            return Err(Status::permission_denied(authz::deny_message(
                user, action, &target,
            )));
        }
        Ok(())
    }

    async fn plan(&self, sql: &str) -> Result<DataFrame, Status> {
        self.ctx
            .sql(sql)
            .await
            .map_err(|e| Status::invalid_argument(format!("planning failed: {e}")))
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
            .with_schema(schema)
            .build(stream.map_err(|e| FlightError::ExternalError(Box::new(e))))
            .map_err(Status::from);
        Ok(Box::pin(flight))
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

/// Decode the DoPut Arrow stream into record batches.
async fn decode_put_stream(stream: PeekableFlightDataStream) -> Result<Vec<RecordBatch>, Status> {
    // into_peekable(), NOT into_inner(): the do_put dispatcher has already
    // peeked the first message (it carries the descriptor AND the schema),
    // and into_inner() would silently drop it.
    FlightRecordBatchStream::new_from_flight_data(stream.into_peekable().map_err(FlightError::from))
        .try_collect::<Vec<_>>()
        .await
        .map_err(|e| Status::invalid_argument(format!("cannot decode bound Arrow data: {e}")))
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
            let decoded = BASE64_ANY_PAD
                .decode(b64)
                .map_err(|_| Status::unauthenticated("Basic credentials are not valid base64"))?;
            let creds = String::from_utf8(decoded)
                .map_err(|_| Status::unauthenticated("Basic credentials are not valid UTF-8"))?;
            let (user, password) = creds
                .split_once(':')
                .ok_or_else(|| Status::unauthenticated("expected user:password credentials"))?;
            if !source.verify_password(user, password) {
                warn!(user, "flight handshake rejected (bad credentials)");
                return Err(Status::unauthenticated(format!(
                    "password authentication failed for user \"{user}\""
                )));
            }
            info!(user, "flight handshake authenticated");
            authenticated_user = user.to_string();
        }
        let token = uuid::Uuid::new_v4().to_string();
        self.tokens.lock().expect("token lock").insert(
            token.clone(),
            TokenEntry {
                user: authenticated_user,
                issued: Instant::now(),
            },
        );
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
        let principal = self.authorize(&request)?;
        let sql = query.query.clone();
        self.check_sql(&principal, &sql)?;
        debug!(%sql, "GetFlightInfo(CommandStatementQuery)");
        // Plan once to expose the result schema; the ticket carries the SQL
        // text and DoGet re-plans and executes it.
        let df = self.plan(&sql).await?;
        let schema = df.schema().as_arrow().clone();
        let ticket = TicketStatementQuery {
            statement_handle: sql.into_bytes().into(),
        };
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
        let principal = self.authorize(&request)?;
        let sql = String::from_utf8(ticket.statement_handle.to_vec())
            .map_err(|e| Status::invalid_argument(format!("ticket is not utf-8 SQL: {e}")))?;
        self.check_sql(&principal, &sql)?;
        debug!(%sql, "DoGet(TicketStatementQuery)");
        if let Some(stream) = self.dml_via_doget(&sql).await? {
            return Ok(Response::new(stream));
        }
        let df = self.plan(&sql).await?;
        Ok(Response::new(self.df_to_stream(df).await?))
    }

    // ------------------------------------------------------------------
    // Prepared statements (ADBC parameterized queries)
    // ------------------------------------------------------------------

    async fn do_action_create_prepared_statement(
        &self,
        query: ActionCreatePreparedStatementRequest,
        request: Request<Action>,
    ) -> Result<ActionCreatePreparedStatementResult, Status> {
        let principal = self.authorize(&request)?;
        let sql = query.query.clone();
        self.check_sql(&principal, &sql)?;
        debug!(%sql, "CreatePreparedStatement");
        // Plan for the dataset schema; a plan with untyped `$n` placeholders
        // that DataFusion cannot infer still yields a schema for SELECTs.
        let df = self.plan(&sql).await?;
        let schema_ref: SchemaRef = Arc::new(df.schema().as_arrow().clone());
        let dataset_schema = encode_schema(&schema_ref)?;
        // Parameter types are not inferred (DataFusion resolves them at bind
        // time); advertise an empty parameter schema.
        let parameter_schema = encode_schema(&Schema::empty())?;
        let handle = uuid::Uuid::new_v4().to_string();
        self.prepared.lock().expect("prepared lock").insert(
            handle.clone(),
            Prepared {
                sql,
                params: Vec::new(),
                schema: schema_ref,
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
        self.authorize(&request)?;
        let handle = String::from_utf8(query.prepared_statement_handle.to_vec())
            .map_err(|_| Status::invalid_argument("invalid prepared statement handle"))?;
        self.prepared.lock().expect("prepared lock").remove(&handle);
        Ok(())
    }

    async fn do_put_prepared_statement_query(
        &self,
        query: CommandPreparedStatementQuery,
        request: Request<PeekableFlightDataStream>,
    ) -> Result<DoPutPreparedStatementResult, Status> {
        self.authorize(&request)?;
        let handle = String::from_utf8(query.prepared_statement_handle.to_vec())
            .map_err(|_| Status::invalid_argument("invalid prepared statement handle"))?;
        let batches = decode_put_stream(request.into_inner()).await?;
        let rows = batches_to_param_rows(&batches)?;
        let mut prepared = self.prepared.lock().expect("prepared lock");
        let entry = prepared
            .get_mut(&handle)
            .ok_or_else(|| Status::not_found(format!("unknown prepared statement {handle}")))?;
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
        self.authorize(&request)?;
        let handle = String::from_utf8(cmd.prepared_statement_handle.to_vec())
            .map_err(|_| Status::invalid_argument("invalid prepared statement handle"))?;
        // Answer from the schema captured at create time — no second plan pass.
        let schema = {
            let prepared = self.prepared.lock().expect("prepared lock");
            prepared
                .get(&handle)
                .ok_or_else(|| Status::not_found(format!("unknown prepared statement {handle}")))?
                .schema
                .clone()
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
        let principal = self.authorize(&request)?;
        let handle = String::from_utf8(cmd.prepared_statement_handle.to_vec())
            .map_err(|_| Status::invalid_argument("invalid prepared statement handle"))?;
        let (sql, params) = {
            let prepared = self.prepared.lock().expect("prepared lock");
            let entry = prepared
                .get(&handle)
                .ok_or_else(|| Status::not_found(format!("unknown prepared statement {handle}")))?;
            (entry.sql.clone(), entry.params.clone())
        };
        self.check_sql(&principal, &sql)?;
        debug!(%sql, bound_rows = params.len(), "DoGet(CommandPreparedStatementQuery)");
        // ADBC's dbapi prepares EVERY statement, so UPDATE/DELETE arrive
        // here too: same engine routing as the plain-statement flow.
        if params.is_empty() {
            if let Some(stream) = self.dml_via_doget(&sql).await? {
                return Ok(Response::new(stream));
            }
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
        let principal = self.authorize(&request)?;
        let handle = String::from_utf8(query.prepared_statement_handle.to_vec())
            .map_err(|_| Status::invalid_argument("invalid prepared statement handle"))?;
        let sql = {
            let prepared = self.prepared.lock().expect("prepared lock");
            prepared
                .get(&handle)
                .ok_or_else(|| Status::not_found(format!("unknown prepared statement {handle}")))?
                .sql
                .clone()
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
        let principal = self.authorize(&request)?;
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
        let principal = self.authorize(&request)?;
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

        let batches = decode_put_stream(request.into_inner()).await?;
        let rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        if rows == 0 {
            return Ok(0);
        }
        info!(
            table = %format!("{namespace}.{table}"),
            batches = batches.len(),
            rows,
            "DoPut(CommandStatementIngest): appending as one Iceberg commit"
        );
        // The whole stream goes through iceberg-datafusion's INSERT plan in
        // ONE execution: rolling Parquet writer (default target file size)
        // + a single fast-append commit. Same path as `INSERT INTO ... SELECT`.
        let df = self
            .ctx
            .read_batches(batches)
            .map_err(|e| Status::invalid_argument(format!("cannot read Arrow batches: {e}")))?;
        let target = format!(
            "{}.{}.{}",
            quote_ident(CATALOG_NAME),
            quote_ident(&namespace),
            quote_ident(&table)
        );
        let result = df
            .write_table(
                &target,
                DataFrameWriteOptions::new().with_insert_operation(InsertOp::Append),
            )
            .await
            .map_err(|e| Status::invalid_argument(format!("ingest failed: {e}")))?;
        let count = count_from_batches(&result);
        // iceberg-datafusion reports the committed row count; trust it over
        // our pre-count if present, but never report 0 for a non-empty put.
        Ok(if count > 0 { count } else { rows as i64 })
    }

    // ------------------------------------------------------------------
    // Catalog metadata (ADBC get_objects)
    // ------------------------------------------------------------------

    async fn get_flight_info_catalogs(
        &self,
        query: CommandGetCatalogs,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        self.authorize(&request)?;
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
        self.authorize(&request)?;
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
        self.authorize(&request)?;
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
        self.authorize(&request)?;
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
        self.authorize(&request)?;
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
        self.authorize(&request)?;
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
        self.authorize(&request)?;
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
        self.authorize(&request)?;
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
        self.authorize(&request)?;
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
        self.authorize(&request)?;
        let batch = self
            .sql_info
            .record_batch(query.info)
            .map_err(|e| Status::internal(format!("sql-info batch failed: {e}")))?;
        Ok(Response::new(Self::batch_to_stream(batch)))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

/// Run the Flight SQL endpoint (blocks until SIGINT).
pub async fn run(
    opts: &CatalogOpts,
    host: &str,
    port: u16,
    auth_file: Option<PathBuf>,
    authorizer: Option<SharedAuthorizer>,
) -> Result<()> {
    let start = std::time::Instant::now();
    let auth: Option<Arc<dyn BasicAuthVerifier>> = match &auth_file {
        Some(path) => {
            #[cfg(feature = "managed")]
            {
                let source = Arc::new(crate::pgauth::FileAuthSource::load(path)?);
                info!(
                    auth_file = %path.display(),
                    users = source.user_count(),
                    "Flight SQL basic-auth handshake enabled (bearer tokens per connection)"
                );
                Some(source as Arc<dyn BasicAuthVerifier>)
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
            None
        }
    };

    info!(
        catalog_uri = %opts.catalog_uri,
        warehouse = %opts.warehouse,
        s3_endpoint = %opts.s3_endpoint,
        "connecting to Iceberg REST catalog"
    );
    let catalog = context::connect_catalog(opts).await?;
    // Same copy-on-write engine as `icegres serve` for UPDATE/DELETE (main
    // branch, PK enforcement off — the pgwire listener owns that posture).
    let engine = Arc::new(OverwriteEngine::connect(catalog.clone(), opts, false, None).await?);
    // Same session wiring as `icegres serve`: snapshot-aware caching schema
    // providers (cache.rs) — reads refresh on snapshot change, so flight
    // clients see pgwire commits and vice versa.
    let ctx = context::build_session_context(catalog).await?;

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
        sql_info: build_sql_info(),
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
        startup_ms = start.elapsed().as_millis() as u64,
        "flight-serve ready (Arrow Flight SQL)"
    );

    Server::builder()
        .add_service(
            // Raise the gRPC message ceilings from tonic's 4 MB default so
            // ADBC bulk-ingest DoPut chunks and large single-batch DoGet
            // responses are not rejected mid-stream.
            FlightServiceServer::new(service)
                .max_decoding_message_size(64 * 1024 * 1024)
                .max_encoding_message_size(64 * 1024 * 1024),
        )
        .serve_with_incoming_shutdown(tcp_incoming(listener), async {
            // Drain on SIGTERM (k8s/systemd) as well as SIGINT — tonic stops
            // accepting and lets in-flight RPCs finish before returning.
            let sig = crate::ops::shutdown_signal().await;
            info!(signal = %sig, "shutdown signal received; draining Flight RPCs");
        })
        .await
        .context("flight sql server failed")?;
    Ok(())
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
