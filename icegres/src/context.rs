//! Catalog connection and DataFusion session wiring.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::{Context as _, Result};
use datafusion::catalog::{CatalogProvider, MemoryCatalogProvider};
use datafusion::execution::disk_manager::DiskManagerBuilder;
use datafusion::execution::memory_pool::FairSpillPool;
use datafusion::execution::runtime_env::{RuntimeEnv, RuntimeEnvBuilder};
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
use tracing::{info, warn};

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
    build_session_context_with(catalog, None, None, None, 0).await
}

/// [`build_session_context_with`] plus peer tail mirrors (`--peer-tail`,
/// peer.rs): every plain-table scan additionally unions the peers' mirrored
/// tail windows under the property-watermark exactly-once rule. `None` (all
/// other callers) keeps scans byte-identical.
pub async fn build_session_context_with_peers(
    catalog: Arc<dyn Catalog>,
    target_partitions: Option<usize>,
    write_buffer: Option<Arc<WriteBuffer>>,
    branch: Option<String>,
    freshness_ms: u64,
    peer_mirrors: Option<Arc<crate::peer::PeerMirrors>>,
) -> Result<SessionContext> {
    build_session_context_inner(
        catalog,
        target_partitions,
        write_buffer,
        branch,
        freshness_ms,
        peer_mirrors,
    )
    .await
}

/// Total system memory in bytes from `/proc/meminfo`, if readable.
fn system_memory_bytes() -> Option<usize> {
    let meminfo = std::fs::read_to_string("/proc/meminfo").ok()?;
    for line in meminfo.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            let kb: usize = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

/// Build the DataFusion runtime with a BOUNDED memory pool + disk spill so a
/// heavy sort/join/aggregate degrades to `ResourcesExhausted` (and spills to
/// disk) instead of OOM-killing the whole process (production-readiness audit
/// #3). Limit precedence: `ICEGRES_MEMORY_LIMIT_MB` env, else 70% of total
/// system RAM, else a 1 GiB floor if `/proc/meminfo` is unreadable. Set the
/// env to `0` to opt back into the historical unbounded pool.
fn build_runtime_env() -> Result<Arc<RuntimeEnv>> {
    let limit_bytes: Option<usize> = match std::env::var("ICEGRES_MEMORY_LIMIT_MB") {
        Ok(raw) => match raw.trim().parse::<usize>() {
            Ok(0) => None, // explicit opt-out: unbounded pool
            Ok(mb) => Some(mb * 1024 * 1024),
            Err(_) => {
                warn!(value = %raw, "invalid ICEGRES_MEMORY_LIMIT_MB; using the default (70% of RAM)");
                None // fall through to default below
            }
        },
        Err(_) => None,
    };
    // If env was unset/invalid, default to 70% of system RAM (1 GiB floor).
    let limit_bytes = limit_bytes.or_else(|| {
        let sys = system_memory_bytes().unwrap_or(1024 * 1024 * 1024);
        Some((sys as f64 * 0.70) as usize)
    });

    let mut builder =
        RuntimeEnvBuilder::new().with_disk_manager_builder(DiskManagerBuilder::default());
    if let Some(bytes) = limit_bytes {
        info!(
            memory_pool_mb = bytes / 1024 / 1024,
            "bounded DataFusion memory pool (FairSpillPool) with disk spill enabled"
        );
        builder = builder.with_memory_pool(Arc::new(FairSpillPool::new(bytes)));
    }
    Ok(Arc::new(builder.build()?))
}

/// [`build_session_context`] with an explicit `target_partitions` override,
/// an optional write buffer, and an optional branch pin. Runs on a bounded
/// memory pool with disk spill (see [`build_runtime_env`]).
///
/// `write_buffer` (serve-only, `--write-buffer-ms`) makes every plain-table
/// scan union the committed snapshot with the buffer's overlay (buffer.rs);
/// `None` keeps scans byte-for-byte on the default path. `branch` (serve-only,
/// `--branch`, SPEC D6) pins every plain-table scan to the head of that
/// Iceberg snapshot ref (see cache.rs); `None` = main. `freshness_ms`
/// (serve-only, `--freshness-ms`) enables bounded-staleness reads: every
/// plain-table provider is registered with the freshness refresher registry
/// (freshness.rs; the caller spawns the refresher itself); `0` = default
/// exact-freshness mode, byte-identical.
pub async fn build_session_context_with(
    catalog: Arc<dyn Catalog>,
    target_partitions: Option<usize>,
    write_buffer: Option<Arc<WriteBuffer>>,
    branch: Option<String>,
    freshness_ms: u64,
) -> Result<SessionContext> {
    build_session_context_inner(
        catalog,
        target_partitions,
        write_buffer,
        branch,
        freshness_ms,
        None,
    )
    .await
}

async fn build_session_context_inner(
    catalog: Arc<dyn Catalog>,
    target_partitions: Option<usize>,
    write_buffer: Option<Arc<WriteBuffer>>,
    branch: Option<String>,
    freshness_ms: u64,
    peer_mirrors: Option<Arc<crate::peer::PeerMirrors>>,
) -> Result<SessionContext> {
    let ctx = SessionContext::new_with_config_rt(
        session_config(target_partitions)?,
        build_runtime_env()?,
    );

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
                freshness_ms > 0,
                peer_mirrors.clone(),
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
