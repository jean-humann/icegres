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
//! queryable at full SQL strength. Snapshots are immutable so cached pinned
//! providers never need invalidation, but the cache must not grow without
//! bound: a client issuing many *distinct* `AS OF` queries (adversarially or
//! via a dashboard sweeping history) would otherwise pin one provider — table
//! metadata, schema, warm manifest cache — per snapshot id, forever. The
//! pinned map is therefore capped at [`MAX_PINNED_PER_TABLE`] providers per
//! base table with least-recently-used eviction; an evicted snapshot remains
//! fully queryable, it just pays the one-time rebuild cost again
//! (`icegres/tests/pinned_bound.sh` proves RSS stays bounded across 56
//! distinct historical snapshots). Unknown snapshot ids fail with the
//! underlying "snapshot id ... not found" error; pinned tables are read-only
//! (INSERT into them is rejected by DataFusion's planner since the static
//! provider does not implement `insert_into`).

use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, RwLock};

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::catalog::{SchemaProvider, Session};
use datafusion::datasource::{MemTable, TableProvider, TableType};
use datafusion::error::Result as DFResult;
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use iceberg::table::Table;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use iceberg_datafusion::{to_datafusion_error, IcebergStaticTableProvider};

use crate::buffer::WriteBuffer;

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
    /// Metadata of the snapshot the provider serves — the write buffer's
    /// union read needs it to decide which flushed generations this
    /// committed view already contains (buffer.rs).
    metadata: iceberg::spec::TableMetadataRef,
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
    /// Buffered write mode (`--write-buffer-ms`, buffer.rs): scans union
    /// the committed snapshot with this buffer's overlay so acked-but-
    /// unflushed rows are readable on this server. `None` = default mode,
    /// scans unchanged.
    write_buffer: Option<Arc<WriteBuffer>>,
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
        write_buffer: Option<Arc<WriteBuffer>>,
    ) -> Self {
        let schema = write_delegate.schema();
        Self {
            catalog,
            ident,
            write_delegate,
            schema,
            cached: RwLock::new(None),
            write_buffer,
        }
    }

    /// Return a provider for the table's *current* snapshot (plus the
    /// metadata it serves), reusing the cached one (and its warm manifest
    /// cache) when the metadata is unchanged. Costs one REST `load_table`
    /// round trip per call.
    async fn current_provider(
        &self,
    ) -> iceberg::Result<(
        Arc<IcebergStaticTableProvider>,
        iceberg::spec::TableMetadataRef,
    )> {
        let fresh = self.catalog.load_table(&self.ident).await?;
        let version = metadata_version(&fresh);
        {
            let guard = self.cached.read().expect("cache lock poisoned");
            if let Some(cached) = guard.as_ref() {
                if cached.version == version {
                    return Ok((cached.provider.clone(), cached.metadata.clone()));
                }
            }
        }
        let metadata = fresh.metadata_ref();
        let provider = Arc::new(IcebergStaticTableProvider::try_new_from_table(fresh).await?);
        let mut guard = self.cached.write().expect("cache lock poisoned");
        *guard = Some(CachedSnapshot {
            version,
            provider: provider.clone(),
            metadata: metadata.clone(),
        });
        Ok((provider, metadata))
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
        let (provider, metadata) = self.current_provider().await.map_err(to_datafusion_error)?;
        // Buffered write mode: take the overlay AFTER loading the committed
        // metadata — this ordering is what makes the union exactly-once
        // under concurrent flushes (see the protocol in buffer.rs).
        let overlay = self
            .write_buffer
            .as_ref()
            .and_then(|b| b.overlay(&self.ident, &metadata));
        if let Some(overlay) = overlay {
            // Same union shape as the transaction hook's read view (txn.rs
            // UnionProvider): both children scanned with the same projection,
            // filters reported Inexact so DataFusion re-applies them above
            // the union, no per-child limit (union only concatenates).
            let committed =
                crate::scan::tune(provider.scan(state, projection, filters, None).await?);
            let mem = MemTable::try_new(overlay.schema, vec![overlay.batches])?;
            let buffered = mem.scan(state, projection, filters, None).await?;
            return Ok(UnionExec::try_new(vec![committed, buffered])?);
        }
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

/// Maximum number of snapshot-pinned time-travel providers cached per base
/// table. Each pinned provider holds a full `Table` (metadata, schema, warm
/// manifest cache), so an unbounded map is a memory leak under many distinct
/// `table@snapshot` queries; beyond this cap the least-recently-used pinned
/// snapshot for that table is evicted (and transparently rebuilt if queried
/// again).
const MAX_PINNED_PER_TABLE: usize = 16;

/// A cached snapshot-pinned provider plus the logical timestamp of its last
/// use (ticks come from [`PinnedCache::clock`]; relaxed atomics are fine —
/// LRU order only needs to be approximate under concurrency).
struct PinnedEntry {
    provider: Arc<IcebergStaticTableProvider>,
    last_used: AtomicU64,
}

/// Bounded LRU cache of snapshot-pinned time-travel providers, keyed by the
/// full `table@snapshot_id` reference and capped at [`MAX_PINNED_PER_TABLE`]
/// entries per base table.
struct PinnedCache {
    map: RwLock<HashMap<String, PinnedEntry>>,
    clock: AtomicU64,
}

impl PinnedCache {
    fn new() -> Self {
        Self {
            map: RwLock::new(HashMap::new()),
            clock: AtomicU64::new(0),
        }
    }

    fn tick(&self) -> u64 {
        // fetch_add returns the previous value; +1 keeps ticks strictly
        // positive so a fresh entry always outranks the initial 0.
        self.clock.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Look up a pinned provider, refreshing its LRU timestamp on hit.
    fn get(&self, name: &str) -> Option<Arc<IcebergStaticTableProvider>> {
        let guard = self.map.read().expect("pinned lock poisoned");
        let entry = guard.get(name)?;
        entry.last_used.store(self.tick(), Ordering::Relaxed);
        Some(entry.provider.clone())
    }

    /// Insert a pinned provider for `name` (= `<base>@<snapshot_id>`), then
    /// evict least-recently-used entries of `base` down to the per-table cap.
    fn insert(&self, name: &str, base: &str, provider: Arc<IcebergStaticTableProvider>) {
        let mut guard = self.map.write().expect("pinned lock poisoned");
        guard.insert(
            name.to_string(),
            PinnedEntry {
                provider,
                last_used: AtomicU64::new(self.tick()),
            },
        );
        let evicted = evict_lru_over_cap(&mut guard, base, MAX_PINNED_PER_TABLE, |e| {
            e.last_used.load(Ordering::Relaxed)
        });
        for key in &evicted {
            tracing::debug!(table = base, key = %key, "evicted LRU pinned snapshot provider");
        }
        let remaining = guard
            .keys()
            .filter(|k| parse_time_travel(k).is_some_and(|(b, _)| b == base))
            .count();
        tracing::debug!(
            table = base,
            pinned = remaining,
            cap = MAX_PINNED_PER_TABLE,
            "pinned snapshot cache size"
        );
    }
}

/// Evict the least-recently-used entries whose `<table>@<snapshot_id>` key
/// refers to `base` until at most `cap` remain; entries for other tables are
/// untouched. Returns the evicted keys (empty when under the cap). Generic
/// over the entry type so the LRU policy is unit-testable without building
/// real Iceberg providers.
fn evict_lru_over_cap<T>(
    map: &mut HashMap<String, T>,
    base: &str,
    cap: usize,
    last_used: impl Fn(&T) -> u64,
) -> Vec<String> {
    let mut entries: Vec<(String, u64)> = map
        .iter()
        .filter(|(key, _)| parse_time_travel(key).is_some_and(|(b, _)| b == base))
        .map(|(key, value)| (key.clone(), last_used(value)))
        .collect();
    if entries.len() <= cap {
        return Vec::new();
    }
    entries.sort_by_key(|(_, tick)| *tick);
    entries.truncate(entries.len() - cap);
    let evicted: Vec<String> = entries.into_iter().map(|(key, _)| key).collect();
    for key in &evicted {
        map.remove(key);
    }
    evicted
}

/// A [`SchemaProvider`] that wraps iceberg-datafusion's schema provider and
/// serves plain-table lookups through [`CachingTableProvider`]s. Metadata
/// tables (`name$type`) and DDL fall through to the inner provider.
pub struct CachingSchemaProvider {
    inner: Arc<dyn SchemaProvider>,
    catalog: Arc<dyn Catalog>,
    namespace: NamespaceIdent,
    cached: RwLock<HashMap<String, Arc<CachingTableProvider>>>,
    /// Snapshot-pinned time-travel providers (bounded LRU; snapshots are
    /// immutable, so entries never need invalidation — only eviction).
    pinned: PinnedCache,
    /// Buffered write mode (buffer.rs); threaded into every
    /// [`CachingTableProvider`]. Time-travel and metadata tables stay
    /// point-in-time by design and never see the buffer.
    write_buffer: Option<Arc<WriteBuffer>>,
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
        write_buffer: Option<Arc<WriteBuffer>>,
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
                        write_buffer.clone(),
                    )),
                );
            }
        }
        Ok(Self {
            inner,
            catalog,
            namespace,
            cached: RwLock::new(map),
            pinned: PinnedCache::new(),
            write_buffer,
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
        if let Some(provider) = self.pinned.get(name) {
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
        self.pinned.insert(name, base, provider.clone());
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
                    self.write_buffer.clone(),
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_time_travel_accepts_valid_refs() {
        assert_eq!(parse_time_travel("trips@42"), Some(("trips", 42)));
        assert_eq!(
            parse_time_travel("trips@4436304835314641572"),
            Some(("trips", 4436304835314641572))
        );
    }

    #[test]
    fn parse_time_travel_rejects_invalid_refs() {
        assert_eq!(parse_time_travel("trips"), None); // no @
        assert_eq!(parse_time_travel("@42"), None); // empty base
        assert_eq!(parse_time_travel("trips@abc"), None); // non-numeric id
        assert_eq!(parse_time_travel("trips$snapshots@42"), None); // metadata table
        assert_eq!(parse_time_travel("trips@"), None); // empty id
    }

    /// Build a map of `<table>@<i>` keys whose LRU tick equals `i`.
    fn pinned_fixture(base: &str, n: u64) -> HashMap<String, u64> {
        (1..=n).map(|i| (format!("{base}@{i}"), i)).collect()
    }

    #[test]
    fn evict_lru_over_cap_is_noop_under_cap() {
        let mut map = pinned_fixture("trips", 16);
        let evicted = evict_lru_over_cap(&mut map, "trips", 16, |t| *t);
        assert!(evicted.is_empty());
        assert_eq!(map.len(), 16);
    }

    #[test]
    fn evict_lru_over_cap_drops_least_recently_used() {
        let mut map = pinned_fixture("trips", 20);
        let mut evicted = evict_lru_over_cap(&mut map, "trips", 16, |t| *t);
        evicted.sort();
        assert_eq!(evicted, vec!["trips@1", "trips@2", "trips@3", "trips@4"]);
        assert_eq!(map.len(), 16);
        assert!(map.contains_key("trips@5") && map.contains_key("trips@20"));
    }

    #[test]
    fn evict_lru_over_cap_respects_recent_use() {
        let mut map = pinned_fixture("trips", 17);
        // "trips@1" was just used: bump its tick past everything else.
        *map.get_mut("trips@1").unwrap() = 100;
        let evicted = evict_lru_over_cap(&mut map, "trips", 16, |t| *t);
        assert_eq!(evicted, vec!["trips@2"]);
        assert!(map.contains_key("trips@1"));
    }

    #[test]
    fn evict_lru_over_cap_scopes_to_base_table() {
        let mut map = pinned_fixture("trips", 20);
        map.extend(pinned_fixture("cities", 20));
        let evicted = evict_lru_over_cap(&mut map, "trips", 16, |t| *t);
        assert_eq!(evicted.len(), 4);
        assert!(evicted.iter().all(|k| k.starts_with("trips@")));
        // All 20 cities entries untouched.
        assert_eq!(map.keys().filter(|k| k.starts_with("cities@")).count(), 20);
    }

    #[test]
    fn pinned_map_holds_per_table_bound_under_churn() {
        // Simulate an adversarial sweep over 100 distinct snapshots, applying
        // the same insert-then-evict step `PinnedCache::insert` performs.
        // (The end-to-end bound with real Iceberg providers and process RSS
        // is proven by icegres/tests/pinned_bound.sh.)
        let mut map: HashMap<String, u64> = HashMap::new();
        for tick in 1..=100u64 {
            map.insert(format!("trips@{tick}"), tick);
            let _ = evict_lru_over_cap(&mut map, "trips", MAX_PINNED_PER_TABLE, |t| *t);
            assert!(map.len() <= MAX_PINNED_PER_TABLE);
        }
        assert_eq!(map.len(), MAX_PINNED_PER_TABLE);
        // The survivors are exactly the 16 most recently queried snapshots.
        for tick in 85..=100u64 {
            assert!(map.contains_key(&format!("trips@{tick}")));
        }
    }
}
