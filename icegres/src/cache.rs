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
//!
//! # Time travel (`table@snapshot_id`)
//!
//! [`CachingSchemaProvider`] additionally resolves table references of the
//! form `"<table>@<snapshot_id>"` (quoted, since `@` is not a plain
//! identifier character) to a read-only provider pinned to that Iceberg
//! snapshot via `IcebergStaticTableProvider::try_new_from_table_snapshot` —
//! e.g.
//!
//! ```sql
//! select snapshot_id from demo."trips$snapshots" order by committed_at;
//! select count(*) from demo."trips@4436304835314641572";
//! ```
//!
//! This is the serve-in-place analogue of Lakebase/Neon PITR-style reads
//! (SPEC §1 D4): every historical snapshot retained in table metadata is
//! queryable at full SQL strength. Snapshots are immutable, so pinned
//! providers are cached forever once built (bounded by the number of
//! distinct snapshot ids actually queried). Unknown snapshot ids fail with
//! the underlying "snapshot id ... not found" error; pinned tables are
//! read-only (INSERT into them is rejected by DataFusion's planner since the
//! static provider does not implement `insert_into`).

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
    /// Snapshot-pinned time-travel providers, keyed by the full
    /// `table@snapshot_id` reference. Snapshots are immutable, so entries
    /// never need invalidation.
    pinned: RwLock<HashMap<String, Arc<IcebergStaticTableProvider>>>,
}

/// Parse a `<table>@<snapshot_id>` time-travel reference. Returns the base
/// table name and the snapshot id, or `None` if `name` is not of that form.
fn parse_time_travel(name: &str) -> Option<(&str, i64)> {
    let (base, id) = name.rsplit_once('@')?;
    if base.is_empty() || base.contains('$') {
        return None;
    }
    id.parse::<i64>().ok().map(|id| (base, id))
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
            pinned: RwLock::new(HashMap::new()),
        })
    }

    /// Resolve a `table@snapshot_id` time-travel reference to a read-only
    /// provider pinned to that snapshot, building (and caching) it on first
    /// use. `Ok(None)` when the base table does not exist; an error when the
    /// snapshot id is not in the table's metadata.
    async fn time_travel_table(
        &self,
        name: &str,
        base: &str,
        snapshot_id: i64,
    ) -> DFResult<Option<Arc<dyn TableProvider>>> {
        if let Some(provider) = self
            .pinned
            .read()
            .expect("pinned lock poisoned")
            .get(name)
            .cloned()
        {
            return Ok(Some(provider as Arc<dyn TableProvider>));
        }
        if !self.inner.table_exist(base) {
            return Ok(None);
        }
        let ident = TableIdent::new(self.namespace.clone(), base.to_string());
        let table = self
            .catalog
            .load_table(&ident)
            .await
            .map_err(to_datafusion_error)?;
        let provider = Arc::new(
            IcebergStaticTableProvider::try_new_from_table_snapshot(table, snapshot_id)
                .await
                .map_err(to_datafusion_error)?,
        );
        self.pinned
            .write()
            .expect("pinned lock poisoned")
            .insert(name.to_string(), provider.clone());
        Ok(Some(provider as Arc<dyn TableProvider>))
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
        if let Some((base, _)) = parse_time_travel(name) {
            // Snapshot existence needs IO; report the base table's existence
            // and let `table()` surface an unknown-snapshot error.
            return self.inner.table_exist(base);
        }
        self.inner.table_exist(name)
    }

    async fn table(&self, name: &str) -> DFResult<Option<Arc<dyn TableProvider>>> {
        // Metadata tables ($snapshots, $manifests, ...) always come from the
        // inner provider — they are point-in-time views by construction.
        if name.contains('$') {
            return self.inner.table(name).await;
        }
        // Time-travel reference: "<table>@<snapshot_id>" pins a snapshot.
        if let Some((base, snapshot_id)) = parse_time_travel(name) {
            return self.time_travel_table(name, base, snapshot_id).await;
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
