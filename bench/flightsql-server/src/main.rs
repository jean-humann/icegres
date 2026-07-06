//! Minimal Arrow Flight SQL server over the SAME Iceberg tables icegres
//! serves (Lakekeeper REST catalog + RustFS S3), on the SAME engine stack
//! (DataFusion 52.5.0 + iceberg-rust 0.9.1).
//!
//! Purpose: a transport comparison point for the bench — Flight SQL
//! (gRPC + Arrow IPC streaming) vs pgwire (icegres) with the query engine
//! held constant. No independent OSS Flight SQL Iceberg server is
//! installable in this environment (Dremio needs docker), so this is
//! deliberately the identical engine behind a different wire protocol.
//!
//! Supported: CommandStatementQuery via the standard GetFlightInfo → DoGet
//! flow. No auth, no TLS, no prepared statements, read-only intent.
//! The catalog wiring below is a copy of icegres/src/context.rs
//! (connect_catalog + session build) minus icegres's caching/write-buffer/
//! branch layers — plain IcebergCatalogProvider snapshots at boot.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use anyhow::{Context as _, Result};
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::sql::server::FlightSqlService;
use arrow_flight::sql::{CommandStatementQuery, ProstMessageExt, SqlInfo, TicketStatementQuery};
use arrow_flight::{FlightDescriptor, FlightEndpoint, FlightInfo, Ticket};
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::TryStreamExt;
use iceberg::io::{
    S3_ACCESS_KEY_ID, S3_DISABLE_CONFIG_LOAD, S3_DISABLE_EC2_METADATA, S3_ENDPOINT,
    S3_PATH_STYLE_ACCESS, S3_REGION, S3_SECRET_ACCESS_KEY,
};
use iceberg::{Catalog, CatalogBuilder};
use iceberg_catalog_rest::{
    RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE,
};
use iceberg_datafusion::IcebergCatalogProvider;
use iceberg_storage_opendal::OpenDalStorageFactory;
use prost::Message;
use tonic::transport::Server;
use tonic::{Request, Response, Status};
use tracing::info;

/// Same catalog/schema names as icegres so identical SQL runs on both.
const CATALOG_NAME: &str = "icegres";
const DEFAULT_SCHEMA: &str = "demo";

fn env_or(key: &str, default: &str) -> String {
    std::env::var(key).unwrap_or_else(|_| default.to_string())
}

/// Copy of icegres/src/context.rs::connect_catalog (same env defaults).
async fn connect_catalog() -> Result<Arc<dyn Catalog>> {
    let catalog_uri = env_or("ICEGRES_CATALOG_URI", "http://127.0.0.1:8181/catalog");
    let warehouse = env_or("ICEGRES_WAREHOUSE", "lakehouse");
    let props = HashMap::from([
        (REST_CATALOG_PROP_URI.to_string(), catalog_uri.clone()),
        (REST_CATALOG_PROP_WAREHOUSE.to_string(), warehouse.clone()),
        (
            S3_ENDPOINT.to_string(),
            env_or("ICEGRES_S3_ENDPOINT", "http://127.0.0.1:9000"),
        ),
        (
            S3_ACCESS_KEY_ID.to_string(),
            env_or("ICEGRES_S3_ACCESS_KEY", "rustfsadmin"),
        ),
        (
            S3_SECRET_ACCESS_KEY.to_string(),
            env_or("ICEGRES_S3_SECRET_KEY", "rustfssecret"),
        ),
        (
            S3_REGION.to_string(),
            env_or("ICEGRES_S3_REGION", "us-east-1"),
        ),
        // RustFS has no virtual-hosted-style routing; path style is required.
        (S3_PATH_STYLE_ACCESS.to_string(), "true".to_string()),
        // Avoid AWS config/metadata lookups on a local S3-compatible store.
        (S3_DISABLE_CONFIG_LOAD.to_string(), "true".to_string()),
        (S3_DISABLE_EC2_METADATA.to_string(), "true".to_string()),
    ]);

    let catalog = RestCatalogBuilder::default()
        .with_storage_factory(Arc::new(OpenDalStorageFactory::S3 {
            configured_scheme: "s3".to_string(),
            customized_credential_load: None,
        }))
        .load("lakekeeper", props)
        .await
        .with_context(|| {
            format!("failed to build REST catalog client for {catalog_uri} (warehouse {warehouse})")
        })?;
    Ok(Arc::new(catalog))
}

/// Build the DataFusion session exposing every Iceberg namespace/table,
/// registered under the same `icegres` catalog name.
async fn build_session_context(catalog: Arc<dyn Catalog>) -> Result<SessionContext> {
    let config = SessionConfig::new()
        .with_information_schema(true)
        .with_default_catalog_and_schema(CATALOG_NAME, DEFAULT_SCHEMA);
    let ctx = SessionContext::new_with_config(config);
    let provider = IcebergCatalogProvider::try_new(catalog)
        .await
        .context("failed to enumerate namespaces/tables from the Iceberg catalog")?;
    ctx.register_catalog(CATALOG_NAME, Arc::new(provider));
    Ok(ctx)
}

#[derive(Clone)]
struct FlightSqlServiceImpl {
    ctx: Arc<SessionContext>,
}

type DoGetStream =
    Pin<Box<dyn futures::Stream<Item = Result<arrow_flight::FlightData, Status>> + Send + 'static>>;

#[tonic::async_trait]
impl FlightSqlService for FlightSqlServiceImpl {
    type FlightService = FlightSqlServiceImpl;

    async fn get_flight_info_statement(
        &self,
        query: CommandStatementQuery,
        request: Request<FlightDescriptor>,
    ) -> Result<Response<FlightInfo>, Status> {
        let sql = query.query.clone();
        info!(%sql, "GetFlightInfo(CommandStatementQuery)");
        // Plan once to expose the result schema in FlightInfo; the ticket
        // carries the SQL text and DoGet re-plans and executes it.
        let df = self
            .ctx
            .sql(&sql)
            .await
            .map_err(|e| Status::invalid_argument(format!("planning failed: {e}")))?;
        let schema = df.schema().as_arrow().clone();

        let ticket = TicketStatementQuery {
            statement_handle: sql.into_bytes().into(),
        };
        let endpoint = FlightEndpoint::new().with_ticket(Ticket {
            ticket: ticket.as_any().encode_to_vec().into(),
        });
        let info = FlightInfo::new()
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("schema encode failed: {e}")))?
            .with_endpoint(endpoint)
            .with_descriptor(request.into_inner());
        Ok(Response::new(info))
    }

    async fn do_get_statement(
        &self,
        ticket: TicketStatementQuery,
        _request: Request<Ticket>,
    ) -> Result<Response<<Self::FlightService as FlightService>::DoGetStream>, Status> {
        let sql = String::from_utf8(ticket.statement_handle.to_vec())
            .map_err(|e| Status::invalid_argument(format!("ticket is not utf-8 SQL: {e}")))?;
        info!(%sql, "DoGet(TicketStatementQuery)");
        let df = self
            .ctx
            .sql(&sql)
            .await
            .map_err(|e| Status::invalid_argument(format!("planning failed: {e}")))?;
        let stream = df
            .execute_stream()
            .await
            .map_err(|e| Status::internal(format!("execution failed: {e}")))?;
        let schema = stream.schema();
        let flight_stream = arrow_flight::encode::FlightDataEncoderBuilder::new()
            .with_schema(schema)
            .build(stream.map_err(|e| arrow_flight::error::FlightError::ExternalError(Box::new(e))))
            .map_err(Status::from);
        let boxed: DoGetStream = Box::pin(flight_stream);
        Ok(Response::new(boxed))
    }

    async fn register_sql_info(&self, _id: i32, _result: &SqlInfo) {}
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let start = std::time::Instant::now();
    let host = env_or("FLIGHTSQL_HOST", "0.0.0.0");
    let port = env_or("FLIGHTSQL_PORT", "50051");
    let addr: std::net::SocketAddr = format!("{host}:{port}").parse()?;

    let catalog = connect_catalog().await?;
    let ctx = build_session_context(catalog).await?;
    let svc = FlightSqlServiceImpl { ctx: Arc::new(ctx) };

    // Bind explicitly before serving so "port accepts" == "catalog wired".
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .with_context(|| format!("cannot bind {addr}"))?;
    info!(
        %addr,
        startup_ms = start.elapsed().as_millis() as u64,
        "flightsql-server ready"
    );

    Server::builder()
        .add_service(FlightServiceServer::new(svc))
        .serve_with_incoming_shutdown(
            tokio_stream_incoming(listener),
            async {
                let _ = tokio::signal::ctrl_c().await;
                info!("shutdown signal received");
            },
        )
        .await?;
    Ok(())
}

/// Adapt a bound TcpListener into the incoming stream tonic expects.
fn tokio_stream_incoming(
    listener: tokio::net::TcpListener,
) -> impl futures::Stream<Item = std::io::Result<tokio::net::TcpStream>> {
    futures::stream::unfold(listener, |listener| async move {
        let item = listener.accept().await.map(|(s, _)| s);
        Some((item, listener))
    })
}
