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
use iceberg::{Catalog, CatalogBuilder};
use iceberg_catalog_rest::{
    RestCatalogBuilder, REST_CATALOG_PROP_URI, REST_CATALOG_PROP_WAREHOUSE,
};
use iceberg_datafusion::IcebergCatalogProvider;
use iceberg_storage_opendal::OpenDalStorageFactory;
use tracing::info;

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

/// Build a DataFusion session exposing every Iceberg namespace/table under
/// the `icegres` catalog.
///
/// The Iceberg catalog provider snapshots namespaces and tables at
/// construction time, so this must be called AFTER any tables are created
/// through the raw catalog API. It is wrapped in a `MemoryCatalogProvider`
/// because `setup_pg_catalog` needs `register_schema`, which
/// `IcebergCatalogProvider` does not implement.
pub async fn build_session_context(catalog: Arc<dyn Catalog>) -> Result<SessionContext> {
    let session_config = SessionConfig::new()
        .with_information_schema(true)
        .with_default_catalog_and_schema(CATALOG_NAME, DEFAULT_SCHEMA);
    let ctx = SessionContext::new_with_config(session_config);

    let iceberg_provider = IcebergCatalogProvider::try_new(catalog)
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
            mem.register_schema(&schema_name, schema)
                .with_context(|| format!("failed to register schema {schema_name}"))?;
        }
    }
    ctx.register_catalog(CATALOG_NAME, Arc::new(mem));
    Ok(ctx)
}
