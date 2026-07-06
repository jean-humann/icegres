//! Catalog connection and DataFusion session wiring.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context as _, Result};
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider};
use datafusion::prelude::{SessionConfig, SessionContext};
use iceberg::io::{
    S3_ACCESS_KEY_ID, S3_DISABLE_CONFIG_LOAD, S3_DISABLE_EC2_METADATA, S3_ENDPOINT,
    S3_PATH_STYLE_ACCESS, S3_REGION, S3_SECRET_ACCESS_KEY,
};
use iceberg::{Catalog, CatalogBuilder, NamespaceIdent};
use iceberg_catalog_rest::{
    RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE,
};
use iceberg_datafusion::IcebergCatalogProvider;
use iceberg_storage_opendal::OpenDalStorageFactory;
use tracing::info;

use crate::buffer::WriteBuffer;
use crate::cache::CachingSchemaProvider;
use crate::CatalogOpts;

/// Name under which the lakehouse is registered in the DataFusion session.
pub const CATALOG_NAME: &str = "icegres";
/// Default schema (Iceberg namespace) for unqualified table references.
pub const DEFAULT_SCHEMA: &str = "demo";

/// Connect to the Iceberg REST catalog (Lakekeeper) with S3 file IO
/// pointed at the configured S3-compatible object store (RustFS).
pub async fn connect_catalog(opts: &CatalogOpts) -> Result<Arc<dyn Catalog>> {
    let props = HashMap::from([
        (REST_CATALOG_PROP_URI.to_string(), opts.catalog_uri.clone()),
        (
            REST_CATALOG_PROP_WAREHOUSE.to_string(),
            opts.warehouse.clone(),
        ),
        (S3_ENDPOINT.to_string(), opts.s3_endpoint.clone()),
        (S3_ACCESS_KEY_ID.to_string(), opts.s3_access_key.clone()),
        (S3_SECRET_ACCESS_KEY.to_string(), opts.s3_secret_key.clone()),
        (S3_REGION.to_string(), opts.s3_region.clone()),
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
            format!(
                "failed to build REST catalog client for {} (warehouse {})",
                opts.catalog_uri, opts.warehouse
            )
        })?;
    Ok(Arc::new(catalog))
}

/// DataFusion session configuration tuned for this workload (small-batch
/// Iceberg scans over a local S3 store on a 4-core box), with an env-var
/// escape hatch: `ICEGRES_DF_OPTS` accepts `;`-separated
/// `datafusion.<section>.<key>=<value>` pairs that are applied on top of the
/// tuned defaults (invalid entries fail startup loudly rather than being
/// silently ignored).
fn session_config(target_partitions: Option<usize>) -> Result<SessionConfig> {
    let mut config = SessionConfig::new()
        .with_information_schema(true)
        .with_default_catalog_and_schema(CATALOG_NAME, DEFAULT_SCHEMA);
    if let Some(n) = target_partitions {
        config = config.with_target_partitions(n);
    }
    if let Ok(spec) = std::env::var("ICEGRES_DF_OPTS") {
        for pair in spec.split(';').map(str::trim).filter(|p| !p.is_empty()) {
            let (key, value) = pair.split_once('=').with_context(|| {
                format!("ICEGRES_DF_OPTS entry {pair:?} is not of the form key=value")
            })?;
            config
                .options_mut()
                .set(key.trim(), value.trim())
                .with_context(|| format!("ICEGRES_DF_OPTS: cannot set {pair:?}"))?;
        }
    }
    Ok(config)
}

/// Build a DataFusion session exposing every Iceberg namespace/table under
/// the `icegres` catalog.
///
/// The Iceberg catalog provider snapshots namespaces and tables at
/// construction time, so this must be called AFTER any tables are created
/// through the raw catalog API. It is wrapped in a `MemoryCatalogProvider`
/// because `setup_pg_catalog` needs `register_schema`, which
/// `IcebergCatalogProvider` does not implement.
pub async fn build_session_context(catalog: Arc<dyn Catalog>) -> Result<SessionContext> {
    build_session_context_with(catalog, None, None, None).await
}

/// [`build_session_context`] with an explicit `target_partitions` override,
/// an optional write buffer, and an optional branch pin.
///
/// iceberg-datafusion's INSERT path round-robin-repartitions the input of an
/// unpartitioned-table write across `target_partitions` workers and writes
/// one Parquet file per non-empty worker. Callers that want a guaranteed
/// single data file per commit (e.g. `icegres seed`, which optimizes the
/// demo tables for scan speed) pass `Some(1)`.
///
/// `write_buffer` (serve-only, `--write-buffer-ms`) makes every plain-table
/// scan union the committed snapshot with the buffer's overlay (buffer.rs);
/// `None` keeps scans byte-for-byte on the default path.
///
/// `branch` (serve-only, `--branch`, SPEC D6) pins every plain-table scan to
/// the head of that Iceberg snapshot ref (see cache.rs); `None` = main.
pub async fn build_session_context_with(
    catalog: Arc<dyn Catalog>,
    target_partitions: Option<usize>,
    write_buffer: Option<Arc<WriteBuffer>>,
    branch: Option<String>,
) -> Result<SessionContext> {
    let ctx = SessionContext::new_with_config(session_config(target_partitions)?);

    let iceberg_provider = IcebergCatalogProvider::try_new(catalog.clone())
        .await
        .context("failed to enumerate namespaces/tables from the Iceberg catalog")?;

    let mem = MemoryCatalogProvider::new();
    for schema_name in iceberg_provider.schema_names() {
        if let Some(schema) = iceberg_provider.schema(&schema_name) {
            let tables = schema.table_names();
            info!(
                schema = %schema_name,
                tables = %tables.join(", "),
                "discovered Iceberg namespace"
            );
            // Wrap in the snapshot-aware metadata cache (see cache.rs):
            // scans reuse warm manifest caches, refreshed on snapshot change.
            let caching = CachingSchemaProvider::try_new(
                schema,
                catalog.clone(),
                NamespaceIdent::new(schema_name.clone()),
                write_buffer.clone(),
                branch.clone(),
            )
            .await
            .with_context(|| format!("failed to build caching provider for {schema_name}"))?;
            mem.register_schema(&schema_name, Arc::new(caching))
                .with_context(|| format!("failed to register schema {schema_name}"))?;
        }
    }
    ctx.register_catalog(CATALOG_NAME, Arc::new(mem));
    Ok(ctx)
}
