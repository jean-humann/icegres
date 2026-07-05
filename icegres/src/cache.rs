//! Snapshot-aware table metadata caching.
//!
//! iceberg-datafusion's catalog-backed `IcebergTableProvider` calls
//! `catalog.load_table` on every scan, and each `load_table` builds a fresh
//! `Table` whose internal manifest cache starts cold — so every query
//! re-reads the manifest list plus *every* manifest file from object
//! storage. With a couple hundred commits on `demo.trips` that is hundreds
//! of S3 GETs per query (~1 ms each against local RustFS), which measured as
//! ~220 ms of the ~220 ms point-lookup p50.
//!
//! [`CachingTableProvider`] keeps the last-seen `Table` (and therefore its
//! warm in-memory manifest/manifest-list cache) and performs one cheap REST
//! `load_table` (~2-3 ms against local Lakekeeper) per scan purely to detect
//! snapshot changes: when the metadata location is unchanged the warm
//! provider is reused; when it changed (any commit — from this server or any
//! other writer) the cached provider is rebuilt from the fresh metadata.
//! Freshness is therefore exact: every scan observes the catalog's current
//! snapshot, with no staleness window.
//!
//! Writes (`INSERT`) delegate to the upstream catalog-backed provider, which
//! loads fresh metadata and commits through the catalog; the resulting new
//! snapshot is picked up by the next scan's snapshot check. Metadata tables
//! (`trips$snapshots` etc.) and DDL delegate to the upstream schema provider
//! unchanged.

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::catalog::{SchemaProvider, Session};
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::ExecutionPlan;
use iceberg::table::Table;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use iceberg_datafusion::{to_datafusion_error, IcebergStaticTableProvider};

/// Identity of a table metadata version: metadata file location plus current
/// snapshot id. Any commit (append, schema change, ...) moves the metadata
/// location; the snapshot id is kept as a belt-and-braces fallback for
/// catalogs that do not report a location.
type MetadataVersion = (Option<String>, Option<i64>);

fn metadata_version(table: &Table) -> MetadataVersion {
    (
        table.metadata_location().map(str::to_string),
        table.metadata().current_snapshot_id(),
    )
}

struct CachedSnapshot {
    version: MetadataVersion,
    provider: Arc<IcebergStaticTableProvider>,
}

/// A [`TableProvider`] that serves scans from a cached, snapshot-pinned
/// provider (reusing its warm manifest cache) and refreshes the cache
/// whenever the catalog reports new table metadata.
pub struct CachingTableProvider {
    catalog: Arc<dyn Catalog>,
    ident: TableIdent,
    /// Upstream catalog-backed provider; handles inserts (fresh load +
    /// catalog commit) and supplies the planning schema.
    write_delegate: Arc<dyn TableProvider>,
    schema: ArrowSchemaRef,
    cached: RwLock<Option<CachedSnapshot>>,
}

impl std::fmt::Debug for CachingTableProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachingTableProvider")
            .field("ident", &self.ident)
            .finish_non_exhaustive()
    }
}

impl CachingTableProvider {
    pub fn new(
        catalog: Arc<dyn Catalog>,
        ident: TableIdent,
        write_delegate: Arc<dyn TableProvider>,
    ) -> Self {
        let schema = write_delegate.schema();
        Self {
            catalog,
            ident,
            write_delegate,
            schema,
            cached: RwLock::new(None),
        }
    }

    /// Return a provider for the table's *current* snapshot, reusing the
    /// cached one (and its warm manifest cache) when the metadata is
    /// unchanged. Costs one REST `load_table` round trip per call.
    async fn current_provider(&self) -> iceberg::Result<Arc<IcebergStaticTableProvider>> {
        let fresh = self.catalog.load_table(&self.ident).await?;
        let version = metadata_version(&fresh);
        {
            let guard = self.cached.read().expect("cache lock poisoned");
            if let Some(cached) = guard.as_ref() {
                if cached.version == version {
                    return Ok(cached.provider.clone());
                }
            }
        }
        let provider = Arc::new(IcebergStaticTableProvider::try_new_from_table(fresh).await?);
        let mut guard = self.cached.write().expect("cache lock poisoned");
        *guard = Some(CachedSnapshot {
            version,
            provider: provider.clone(),
        });
        Ok(provider)
    }
}

#[async_trait]
impl TableProvider for CachingTableProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema(&self) -> ArrowSchemaRef {
        self.schema.clone()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        state: &dyn Session,
        projection: Option<&Vec<usize>>,
        filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let provider = self.current_provider().await.map_err(to_datafusion_error)?;
        let plan = provider.scan(state, projection, filters, limit).await?;
        // Re-run plain table scans at higher object-store IO concurrency
        // (see scan.rs); non-scan plans pass through unchanged.
        Ok(crate::scan::tune(plan))
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        // Match the upstream providers: push everything down inexactly; the
        // Iceberg scanner drops what it cannot handle.
        Ok(vec![TableProviderFilterPushDown::Inexact; filters.len()])
    }

    async fn insert_into(
        &self,
        state: &dyn Session,
        input: Arc<dyn ExecutionPlan>,
        insert_op: InsertOp,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        self.write_delegate
            .insert_into(state, input, insert_op)
            .await
    }
}

/// A [`SchemaProvider`] that wraps iceberg-datafusion's schema provider and
/// serves plain-table lookups through [`CachingTableProvider`]s. Metadata
/// tables (`name$type`) and DDL fall through to the inner provider.
pub struct CachingSchemaProvider {
    inner: Arc<dyn SchemaProvider>,
    catalog: Arc<dyn Catalog>,
    namespace: NamespaceIdent,
    cached: RwLock<HashMap<String, Arc<CachingTableProvider>>>,
}

impl std::fmt::Debug for CachingSchemaProvider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CachingSchemaProvider")
            .field("namespace", &self.namespace)
            .finish_non_exhaustive()
    }
}

impl CachingSchemaProvider {
    /// Wrap `inner`, pre-building a caching provider for every plain table
    /// currently in the namespace.
    pub async fn try_new(
        inner: Arc<dyn SchemaProvider>,
        catalog: Arc<dyn Catalog>,
        namespace: NamespaceIdent,
    ) -> DFResult<Self> {
        let mut map = HashMap::new();
        for name in inner.table_names() {
            if name.contains('$') {
                continue;
            }
            if let Some(write_delegate) = inner.table(&name).await? {
                let ident = TableIdent::new(namespace.clone(), name.clone());
                map.insert(
                    name,
                    Arc::new(CachingTableProvider::new(
                        catalog.clone(),
                        ident,
                        write_delegate,
                    )),
                );
            }
        }
        Ok(Self {
            inner,
            catalog,
            namespace,
            cached: RwLock::new(map),
        })
    }
}

#[async_trait]
impl SchemaProvider for CachingSchemaProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn table_names(&self) -> Vec<String> {
        self.inner.table_names()
    }

    fn table_exist(&self, name: &str) -> bool {
        self.inner.table_exist(name)
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        // Metadata tables ($snapshots, $manifests, ...) always come from the
        // inner provider — they are point-in-time views by construction.
        if name.contains('$') {
            return self.inner.table(name).await;
        }
        if let Some(provider) = self
            .cached
            .read()
            .expect("cache lock poisoned")
            .get(name)
            .cloned()
        {
            return Ok(Some(provider));
        }
        // Table created after startup: wrap it lazily so it gets the same
        // caching treatment.
        match self.inner.table(name).await? {
            Some(write_delegate) => {
                let ident = TableIdent::new(self.namespace.clone(), name.to_string());
                let provider = Arc::new(CachingTableProvider::new(
                    self.catalog.clone(),
                    ident,
                    write_delegate,
                ));
                self.cached
                    .write()
                    .expect("cache lock poisoned")
                    .insert(name.to_string(), provider.clone());
                Ok(Some(provider))
            }
            None => Ok(None),
        }
    }

    fn register_table(
        &self,
        name: String,
        table: Arc<dyn TableProvider>,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        self.inner.register_table(name, table)
    }

    fn deregister_table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        self.cached
            .write()
            .expect("cache lock poisoned")
            .remove(name);
        self.inner.deregister_table(name)
    }
}
