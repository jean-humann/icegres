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
use std::sync::{Arc, OnceLock, RwLock};
use std::time::Duration;

use async_trait::async_trait;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::catalog::{SchemaProvider, Session};
use datafusion::datasource::{MemTable, TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::dml::InsertOp;
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::ExecutionPlan;
use iceberg::table::Table;
use iceberg::{Catalog, NamespaceIdent, TableIdent};
use iceberg_datafusion::{to_datafusion_error, IcebergStaticTableProvider};
use tracing::warn;

use crate::buffer::WriteBuffer;
use crate::freshness::TableFreshness;

/// Per-attempt timeout for a catalog `load_table` from
/// `ICEGRES_CATALOG_TIMEOUT_MS` (default 5000; `0` = no timeout).
fn catalog_timeout() -> Option<Duration> {
    static T: OnceLock<Option<Duration>> = OnceLock::new();
    *T.get_or_init(|| match std::env::var("ICEGRES_CATALOG_TIMEOUT_MS") {
        Ok(raw) => match raw.trim().parse::<u64>() {
            Ok(0) => None,
            Ok(ms) => Some(Duration::from_millis(ms)),
            Err(_) => Some(Duration::from_millis(5000)),
        },
        Err(_) => Some(Duration::from_millis(5000)),
    })
}

/// Number of retries after the first failed `load_table` from
/// `ICEGRES_CATALOG_RETRIES` (default 2).
fn catalog_retries() -> u32 {
    static R: OnceLock<u32> = OnceLock::new();
    *R.get_or_init(|| {
        std::env::var("ICEGRES_CATALOG_RETRIES")
            .ok()
            .and_then(|r| r.trim().parse().ok())
            .unwrap_or(2)
    })
}

/// Tri-state parse of `ICEGRES_STALE_READ_ON_CATALOG_ERROR`: unset → `None`
/// (mode default applies), truthy → `Some(true)`, any other set value
/// (`0`, `false`, ...) → `Some(false)` — an EXPLICIT fail-loud override.
fn parse_stale_read_override(raw: Option<&str>) -> Option<bool> {
    raw.map(|r| matches!(r.trim(), "1" | "true" | "on" | "yes"))
}

/// Whether a scan whose catalog `load_table` fails (after timeout+retries)
/// serves the last cached snapshot instead of erroring. See
/// [`stale_read_policy`] for the decision table.
fn stale_read_on_catalog_error(freshness_enabled: bool) -> bool {
    static S: OnceLock<Option<bool>> = OnceLock::new();
    let override_ = *S.get_or_init(|| {
        parse_stale_read_override(
            std::env::var("ICEGRES_STALE_READ_ON_CATALOG_ERROR")
                .ok()
                .as_deref(),
        )
    });
    stale_read_policy(override_, freshness_enabled)
}

/// Stale-serve-on-catalog-error policy (availability vs fail-loud), pure so
/// the matrix is unit-testable:
///
/// * Default mode (`--freshness-ms 0`), env unset: FAIL LOUD — serving
///   stale would silently change the exact-freshness contract.
/// * Freshness mode (`--freshness-ms > 0`), env unset: SERVE STALE — the
///   contract is already bounded staleness, the refresher rides out the
///   outage, and the staleness is visible on the `icegres_freshness_age_ms`
///   gauge (availability by default).
/// * `ICEGRES_STALE_READ_ON_CATALOG_ERROR` set overrides BOTH ways: truthy
///   opts default mode into stale serving; falsy (`=0`) forces fail-loud
///   even in freshness mode — the explicit opt-out for deployments where a
///   local write followed by an outage must ERROR rather than silently
///   regress read-your-own-writes to the last snapshot
///   (docs/limitations.md documents both modes).
fn stale_read_policy(override_: Option<bool>, freshness_enabled: bool) -> bool {
    override_.unwrap_or(freshness_enabled)
}

/// `catalog.load_table` with a bounded per-attempt timeout and bounded
/// retries, so a catalog blip surfaces as a bounded error (or a stale-cache
/// fallback) instead of hanging every read indefinitely (production-readiness
/// audit #6).
async fn load_table_with_retry(
    catalog: &Arc<dyn Catalog>,
    ident: &TableIdent,
) -> iceberg::Result<Table> {
    let timeout = catalog_timeout();
    let retries = catalog_retries();
    let mut last: Option<iceberg::Error> = None;
    for attempt in 0..=retries {
        let res = match timeout {
            Some(d) => match tokio::time::timeout(d, catalog.load_table(ident)).await {
                Ok(r) => r,
                Err(_) => Err(iceberg::Error::new(
                    iceberg::ErrorKind::Unexpected,
                    format!("catalog load_table timed out after {} ms", d.as_millis()),
                )),
            },
            None => catalog.load_table(ident).await,
        };
        match res {
            Ok(t) => return Ok(t),
            Err(e) => {
                if attempt < retries {
                    let backoff = Duration::from_millis(50u64 << attempt);
                    warn!(%ident, attempt, error = %e, "catalog load_table failed; retrying");
                    tokio::time::sleep(backoff).await;
                }
                last = Some(e);
            }
        }
    }
    Err(last.expect("at least one attempt was made"))
}

/// Identity of a table metadata version: metadata file location plus current
/// snapshot id. Any commit (append, schema change, ...) moves the metadata
/// location; the snapshot id is kept as a belt-and-braces fallback for
/// catalogs that do not report a location.
pub(crate) type MetadataVersion = (Option<String>, Option<i64>);

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
    /// Freshness write-generation observed BEFORE the catalog load that
    /// produced this snapshot (freshness.rs). Guards concurrent installs:
    /// a slow load that began before a local write's invalidation must not
    /// clobber a snapshot installed by a later (post-commit) load. Always 0
    /// in default mode, where every scan reloads anyway.
    loaded_gen: u64,
}

/// Which caller is loading table metadata (`load_current`): the scan path
/// gets the bounded timeout+retries from `ICEGRES_CATALOG_TIMEOUT_MS`/
/// `ICEGRES_CATALOG_RETRIES` and the optional stale-serve fallback; the
/// background refresher gets ONE retry-free attempt (its next pass is the
/// retry, and freshness.rs bounds the call with its own short per-table
/// timeout) whose error propagates to the refresher's failure accounting.
#[derive(Clone, Copy, PartialEq, Eq)]
enum LoadPath {
    Scan,
    Refresher,
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
    /// Branch pin (`--branch`, SPEC D6): scans serve the head of this
    /// Iceberg snapshot ref instead of `main`'s current snapshot. A table
    /// without the ref fails loudly at scan time (never silently falls back
    /// to main). `None` = default mode, scans unchanged.
    branch: Option<String>,
    /// Bounded-staleness mode (`--freshness-ms`, freshness.rs): when fresh,
    /// scans serve the cached provider with NO catalog round trip; the
    /// background refresher and local-write invalidation keep it honest.
    /// `None` = default mode, every scan pays the exact-freshness catalog
    /// check — byte-identical to the historical path.
    freshness: Option<Arc<TableFreshness>>,
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
        branch: Option<String>,
        freshness: Option<Arc<TableFreshness>>,
    ) -> Self {
        let schema = write_delegate.schema();
        Self {
            catalog,
            ident,
            write_delegate,
            schema,
            cached: RwLock::new(None),
            write_buffer,
            branch,
            freshness,
        }
    }

    /// The freshness-registry key for this table (freshness.rs, plan cache).
    pub(crate) fn table_key(&self) -> String {
        crate::freshness::table_key(&self.ident)
    }

    /// The cached table metadata (+ its metadata location, when known) —
    /// `Some` ONLY when serving it without a catalog round trip is sound
    /// under the freshness contract: freshness mode is on and the cache is
    /// currently fresh (a local write/DDL invalidates synchronously; a
    /// foreign writer's change lands within the configured bound). `None`
    /// in default mode, so callers keep their exact per-statement catalog
    /// load byte-identical. Used by the keyed-DML activation gate
    /// (buffer.rs): the gate's per-statement `load_table` rides the same
    /// bounded-staleness cache reads already ride.
    pub(crate) fn fresh_metadata(
        &self,
    ) -> Option<(Option<String>, iceberg::spec::TableMetadataRef)> {
        let f = self.freshness.as_ref()?;
        if !f.is_fresh() {
            return None;
        }
        let guard = crate::freshness::recover("cache lock", self.cached.read());
        let cached = guard.as_ref()?;
        Some((cached.version.0.clone(), cached.metadata.clone()))
    }

    /// The metadata version a cached physical plan over this table may be
    /// reused at (plancache.rs). `Some(version)` ONLY when a plan-cache hit
    /// is sound without any catalog round trip: freshness mode is on, the
    /// cache is currently fresh, and the table carries no per-scan write-
    /// buffer overlay (a cached plan must never bake in a stale overlay —
    /// the scope's overlay trap). `None` in default mode, so the plan cache
    /// can never bypass the exact per-scan freshness check.
    pub(crate) fn plan_cache_version(&self) -> Option<MetadataVersion> {
        if !plan_cache_eligible(self.freshness.is_some(), self.write_buffer.is_some()) {
            return None;
        }
        let f = self.freshness.as_ref()?;
        if !f.is_fresh() {
            return None;
        }
        let guard = crate::freshness::recover("cache lock", self.cached.read());
        guard.as_ref().map(|c| c.version.clone())
    }

    /// Refresh the cached provider from the catalog (background refresher
    /// path, freshness.rs): ONE retry-free `load_table` round trip that
    /// swaps the cached provider on metadata change and marks the cache
    /// fresh — unless a local write invalidated it mid-load (generation
    /// guard). Unlike the scan path this neither retries (the refresher's
    /// next pass IS the retry) nor falls back to the cached snapshot on
    /// error (the cache is simply left as-is and the error propagates so
    /// the refresher can count and WARN about it); the refresher bounds the
    /// call with its own short per-table timeout. Background loads are also
    /// excluded from the `ICEGRES_QUERY_TIMING` stage records: stage
    /// `freshness` means per-SCAN catalog work only.
    pub(crate) async fn refresh(&self) -> iceberg::Result<()> {
        self.load_current(LoadPath::Refresher).await.map(|_| ())
    }

    /// Return a provider for the table's *current* snapshot — the head of
    /// the configured branch (`main` by default) — plus the metadata it
    /// serves. In freshness mode ([`TableFreshness`]) a fresh cache is
    /// served directly with NO catalog round trip; otherwise (default mode,
    /// or a stale/invalidated cache) this costs one REST `load_table` round
    /// trip, reusing the cached provider (and its warm manifest cache) when
    /// the metadata is unchanged.
    async fn current_provider(
        &self,
    ) -> iceberg::Result<(
        Arc<IcebergStaticTableProvider>,
        iceberg::spec::TableMetadataRef,
    )> {
        if let Some(f) = &self.freshness {
            if f.is_fresh() {
                let guard = crate::freshness::recover("cache lock", self.cached.read());
                if let Some(cached) = guard.as_ref() {
                    return Ok((cached.provider.clone(), cached.metadata.clone()));
                }
            }
        }
        self.load_current(LoadPath::Scan).await
    }

    /// The synchronous load path: one catalog `load_table`, snapshot-change
    /// detection, and cache install. The `path` decides the load strategy —
    /// scans get the bounded timeout+retries (and the stale-serve fallback
    /// per [`stale_read_policy`]) plus the `ICEGRES_QUERY_TIMING` stage
    /// record; the background refresher gets one retry-free attempt whose
    /// error propagates (see [`Self::refresh`]).
    async fn load_current(
        &self,
        path: LoadPath,
    ) -> iceberg::Result<(
        Arc<IcebergStaticTableProvider>,
        iceberg::spec::TableMetadataRef,
    )> {
        // Freshness generation observed BEFORE the load: completing the
        // load only marks the cache fresh if no local write invalidated it
        // in between (freshness.rs module docs).
        let load_token = self.freshness.as_ref().map(|f| f.begin_load());
        // Stage (a) of the ICEGRES_QUERY_TIMING breakdown (timing.rs): the
        // per-scan catalog round trip that gives exact freshness. The
        // `Instant::now()` itself is skipped when timing is off.
        let load_started =
            (path == LoadPath::Scan && crate::timing::enabled()).then(std::time::Instant::now);
        let loaded = match path {
            LoadPath::Scan => load_table_with_retry(&self.catalog, &self.ident).await,
            // Refresher: a single retry-free attempt. The refresher applies
            // its own short per-table timeout around this call and its next
            // pass is the retry — the scan path's timeout×retries config
            // must not stack up here, where one stalled table used to drag
            // every table's refresh behind it.
            LoadPath::Refresher => self.catalog.load_table(&self.ident).await,
        };
        let fresh = match loaded {
            Ok(t) => t,
            Err(e) => {
                // Scan path, catalog unreachable after timeout+retries:
                // optionally fall back to the last cached snapshot
                // (bounded-stale read) so reads stay available during a
                // catalog outage; otherwise the error propagates loudly.
                // Freshness mode serves stale by DEFAULT (its contract is
                // already bounded staleness, the refresher rides the outage
                // out, and the staleness is visible on the
                // icegres_freshness_age_ms gauge), but
                // ICEGRES_STALE_READ_ON_CATALOG_ERROR=0 explicitly opts
                // into fail-loud even there — see stale_read_policy.
                // The refresher path never falls back: its caller counts
                // the failure and the cache is simply left as-is.
                if path == LoadPath::Scan && stale_read_on_catalog_error(self.freshness.is_some()) {
                    let guard = crate::freshness::recover("cache lock", self.cached.read());
                    if let Some(cached) = guard.as_ref() {
                        warn!(
                            ident = %self.ident,
                            error = %e,
                            "catalog unreachable; serving last cached snapshot (bounded-stale read)"
                        );
                        return Ok((cached.provider.clone(), cached.metadata.clone()));
                    }
                }
                return Err(e);
            }
        };
        if let Some(started) = load_started {
            crate::timing::record("freshness", started.elapsed());
        }
        // S5 (buffer.rs): every scan pays this `load_table` anyway — record
        // the keyed-activation decision so the write path's fallback never
        // needs its own per-statement catalog call, and so a property
        // change by ANY writer (a commit moves the metadata location) is
        // picked up by the next scan. Cheap: short-circuits on an unchanged
        // metadata location.
        if let Some(buffer) = &self.write_buffer {
            buffer.note_activation(&self.ident, fresh.metadata_location(), fresh.metadata());
        }
        // Branch mode: the cache version and the served snapshot are the
        // branch HEAD (a commit to any other branch moves the metadata
        // location without changing what this endpoint serves; a commit to
        // THIS branch changes the head). A missing ref is a loud error,
        // never a silent fallback to main.
        let branch_pin: Option<i64> = match &self.branch {
            None => None,
            Some(branch) => crate::overwrite::branch_head(fresh.metadata(), branch)
                .map_err(|e| {
                    iceberg::Error::new(
                        iceberg::ErrorKind::Unexpected,
                        format!("cannot read {}: {e:#}", self.ident),
                    )
                })?
                .map(|s| s.snapshot_id()),
        };
        let version: MetadataVersion = match branch_pin {
            Some(head) => (None, Some(head)),
            None => metadata_version(&fresh),
        };
        {
            let guard = crate::freshness::recover("cache lock", self.cached.read());
            if let Some(cached) = guard.as_ref() {
                if cached.version == version {
                    // The load proved the cached snapshot is still current.
                    if let (Some(f), Some(token)) = (&self.freshness, load_token) {
                        f.complete_load(token);
                    }
                    return Ok((cached.provider.clone(), cached.metadata.clone()));
                }
            }
        }
        let metadata = fresh.metadata_ref();
        let provider = match branch_pin {
            Some(head) => Arc::new(
                IcebergStaticTableProvider::try_new_from_table_snapshot(fresh, head).await?,
            ),
            None => Arc::new(IcebergStaticTableProvider::try_new_from_table(fresh).await?),
        };
        let loaded_gen = load_token.unwrap_or(0);
        {
            let mut guard = crate::freshness::recover("cache lock", self.cached.write());
            // Generation-guarded install (freshness mode): a slow load that
            // began before a local write's invalidation must not overwrite a
            // snapshot installed by a later (post-commit) load — otherwise a
            // "fresh" cache could serve pre-write data. In default mode both
            // generations are 0 and this is the historical unconditional
            // install.
            let stale_straggler = guard
                .as_ref()
                .is_some_and(|existing| existing.loaded_gen > loaded_gen);
            if !stale_straggler {
                *guard = Some(CachedSnapshot {
                    version,
                    provider: provider.clone(),
                    metadata: metadata.clone(),
                    loaded_gen,
                });
            }
        }
        if let (Some(f), Some(token)) = (&self.freshness, load_token) {
            f.complete_load(token);
        }
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
        let overlay = match &self.write_buffer {
            Some(b) => b
                .overlay(&self.ident, &metadata)
                .map_err(|e| DataFusionError::External(e.into()))?,
            None => None,
        };
        if let Some(overlay) = overlay {
            // Same union shape as the transaction hook's read view (txn.rs
            // UnionProvider): both children scanned with the same projection,
            // filters reported Inexact so DataFusion re-applies them above
            // the union, no per-child limit (union only concatenates).
            //
            // Keyed tail tables (Phase 2) add suppression: committed rows
            // whose PK was updated/deleted in the buffer window are hidden
            // by a KeySuppressExec above the committed child (the overlay's
            // own batches were already filtered layer-aware in buffer.rs).
            // When the scan's projection lacks a PK column, the committed
            // child is scanned WIDENED (PK columns appended) so the filter
            // can evaluate, and KeySuppressExec projects back down — the
            // MemTable child keeps the original projection, so the union's
            // child schemas still match.
            let committed = match &overlay.suppress {
                None => crate::scan::tune(provider.scan(state, projection, filters, None).await?),
                Some(sup) => {
                    let (scan_proj, out_proj) =
                        widen_projection(projection, &self.schema, &sup.pk_cols)
                            .map_err(|e| DataFusionError::External(e.into()))?;
                    let inner = crate::scan::tune(
                        provider
                            .scan(state, scan_proj.as_ref(), filters, None)
                            .await?,
                    );
                    Arc::new(crate::keyed::KeySuppressExec::try_new(
                        inner,
                        &sup.pk_cols,
                        sup.keys.clone(),
                        out_proj,
                    )?) as Arc<dyn ExecutionPlan>
                }
            };
            if overlay.batches.is_empty() {
                // Pure suppression (e.g. only keyed deletes buffered):
                // nothing to union in.
                return Ok(committed);
            }
            let mem = MemTable::try_new(overlay.schema, vec![overlay.batches])?;
            let buffered = mem.scan(state, projection, filters, None).await?;
            return UnionExec::try_new(vec![committed, buffered]);
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
        // Branch mode routes every INSERT through the overwrite engine (the
        // TxnHook intercepts them before planning reaches this provider);
        // the upstream delegate would fast_append to MAIN, silently leaking
        // a branch write onto the default branch — refuse instead.
        if let Some(branch) = &self.branch {
            return Err(datafusion::error::DataFusionError::Plan(format!(
                "INSERT into {} on branch {branch:?} must go through the icegres write \
                 engine (this path would write to 'main'); this is a bug — please report it",
                self.ident
            )));
        }
        let plan = self
            .write_delegate
            .insert_into(state, input, insert_op)
            .await?;
        // Freshness mode: the upstream provider commits when this plan is
        // EXECUTED, so wrap it to invalidate the cached snapshot as its
        // stream completes — a plan-time invalidation could be cleared by a
        // refresher poll that loaded pre-commit metadata (freshness.rs).
        match &self.freshness {
            Some(f) => Ok(Arc::new(MarkStaleExec::new(plan, f.clone()))),
            None => Ok(plan),
        }
    }
}

/// Whether a physical plan over a table may be cached (plancache.rs):
/// freshness mode must be on (a plan-cache hit skips the per-scan catalog
/// check, which is only sound under the bounded-staleness contract) and the
/// table must carry no write-buffer overlay (overlays are per-scan state;
/// a cached plan would bake a stale one in).
pub(crate) fn plan_cache_eligible(freshness_enabled: bool, has_overlay_source: bool) -> bool {
    freshness_enabled && !has_overlay_source
}

/// Wraps an INSERT execution plan so the table's freshness cache is
/// invalidated when the plan's stream finishes — i.e. after the upstream
/// provider's Iceberg commit has happened (or failed ambiguously; an extra
/// invalidation only costs one synchronous catalog check on the next scan).
struct MarkStaleExec {
    inner: Arc<dyn ExecutionPlan>,
    freshness: Arc<TableFreshness>,
    properties: datafusion::physical_plan::PlanProperties,
}

impl MarkStaleExec {
    fn new(inner: Arc<dyn ExecutionPlan>, freshness: Arc<TableFreshness>) -> Self {
        let properties = inner.properties().clone();
        Self {
            inner,
            freshness,
            properties,
        }
    }
}

impl std::fmt::Debug for MarkStaleExec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MarkStaleExec").finish_non_exhaustive()
    }
}

impl datafusion::physical_plan::DisplayAs for MarkStaleExec {
    fn fmt_as(
        &self,
        _t: datafusion::physical_plan::DisplayFormatType,
        f: &mut std::fmt::Formatter<'_>,
    ) -> std::fmt::Result {
        write!(f, "MarkStaleExec")
    }
}

impl ExecutionPlan for MarkStaleExec {
    fn name(&self) -> &str {
        "MarkStaleExec"
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn properties(&self) -> &datafusion::physical_plan::PlanProperties {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.inner]
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let [child]: [Arc<dyn ExecutionPlan>; 1] = children
            .try_into()
            .map_err(|_| DataFusionError::Internal("MarkStaleExec expects one child".into()))?;
        Ok(Arc::new(MarkStaleExec::new(child, self.freshness.clone())))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<datafusion::execution::TaskContext>,
    ) -> DFResult<datafusion::execution::SendableRecordBatchStream> {
        let inner = self.inner.execute(partition, context)?;
        Ok(Box::pin(MarkStaleStream {
            inner,
            freshness: self.freshness.clone(),
            polled: false,
            invalidated: false,
        }))
    }
}

struct MarkStaleStream {
    inner: datafusion::execution::SendableRecordBatchStream,
    freshness: Arc<TableFreshness>,
    /// Any poll was started: the wrapped insert may have issued its catalog
    /// commit request (the drop guard's trigger condition).
    polled: bool,
    /// A poll returned `Ready`: the on-poll invalidation already ran, so the
    /// drop guard has nothing left to do.
    invalidated: bool,
}

impl futures::Stream for MarkStaleStream {
    type Item = DFResult<datafusion::arrow::record_batch::RecordBatch>;

    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.polled = true;
        let poll = std::pin::Pin::new(&mut self.inner).poll_next(cx);
        // The insert sink commits before it emits its count batch (and the
        // stream then ends): invalidate on every yielded item AND at stream
        // end so the commit can never be observed by a fresh-path scan
        // before the invalidation lands. Idempotent and cheap (one atomic).
        if matches!(poll, std::task::Poll::Ready(_)) {
            self.invalidated = true;
            self.freshness.invalidate();
        }
        poll
    }
}

impl Drop for MarkStaleStream {
    /// Drop guard for a stream abandoned mid-poll (client disconnect while
    /// the INSERT executes): once the inner stream has been polled, the
    /// upstream insert may have issued its catalog commit request, and that
    /// commit can still LAND after the drop — with no `Ready` ever observed,
    /// the on-poll invalidation would never run and a "fresh" cache could
    /// keep serving pre-write data. Ambiguous outcomes must invalidate (the
    /// same rule as the on-poll path): a false positive costs one
    /// synchronous catalog check on the next scan; a miss silently breaks
    /// the freshness contract. A never-polled stream cannot have started the
    /// commit, so dropping it clean keeps the cache fresh.
    fn drop(&mut self) {
        if self.polled && !self.invalidated {
            self.freshness.invalidate();
        }
    }
}

impl datafusion::execution::RecordBatchStream for MarkStaleStream {
    fn schema(&self) -> ArrowSchemaRef {
        self.inner.schema()
    }
}

/// Widen a scan projection so the committed child exposes every PK column
/// the keyed suppression filter needs. Returns `(scan_projection,
/// output_indices)`: the projection to scan the committed child with, and —
/// when widening appended columns — the indices (into the WIDENED schema)
/// KeySuppressExec must project back down to so the union's child schemas
/// match. `projection = None` (all columns) needs no widening.
#[allow(clippy::type_complexity)]
fn widen_projection(
    projection: Option<&Vec<usize>>,
    full_schema: &ArrowSchemaRef,
    pk_cols: &[String],
) -> anyhow::Result<(Option<Vec<usize>>, Option<Vec<usize>>)> {
    let Some(proj) = projection else {
        return Ok((None, None));
    };
    let pk_idx: Vec<usize> = pk_cols
        .iter()
        .map(|c| {
            full_schema
                .fields()
                .iter()
                .position(|f| f.name().eq_ignore_ascii_case(c))
                .ok_or_else(|| anyhow::anyhow!("PK column {c:?} missing from table schema"))
        })
        .collect::<anyhow::Result<_>>()?;
    let missing: Vec<usize> = pk_idx
        .iter()
        .copied()
        .filter(|i| !proj.contains(i))
        .collect();
    if missing.is_empty() {
        return Ok((Some(proj.clone()), None));
    }
    let mut widened = proj.clone();
    widened.extend(missing);
    // The original columns are the widened schema's leading prefix.
    let out: Vec<usize> = (0..proj.len()).collect();
    Ok((Some(widened), Some(out)))
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
    /// Branch pin (`--branch`); threaded into every
    /// [`CachingTableProvider`]. Explicit `table@snapshot` time travel and
    /// metadata tables are unaffected (they address snapshots directly).
    branch: Option<String>,
    /// Bounded-staleness mode (`--freshness-ms > 0`): every plain-table
    /// provider gets a [`TableFreshness`] and is registered with the
    /// process-global refresher/invalidation registry (freshness.rs).
    freshness_enabled: bool,
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
        branch: Option<String>,
        freshness_enabled: bool,
    ) -> DFResult<Self> {
        let mut map = HashMap::new();
        for name in inner.table_names() {
            if name.contains('$') {
                continue;
            }
            if let Some(write_delegate) = inner.table(&name).await? {
                let ident = TableIdent::new(namespace.clone(), name.clone());
                let provider = Self::build_provider(
                    catalog.clone(),
                    ident,
                    write_delegate,
                    write_buffer.clone(),
                    branch.clone(),
                    freshness_enabled,
                );
                map.insert(name, provider);
            }
        }
        Ok(Self {
            inner,
            catalog,
            namespace,
            cached: RwLock::new(map),
            pinned: PinnedCache::new(),
            write_buffer,
            branch,
            freshness_enabled,
        })
    }

    /// Build one caching table provider and, in freshness mode, register it
    /// with the global refresher/invalidation registry (freshness.rs).
    fn build_provider(
        catalog: Arc<dyn Catalog>,
        ident: TableIdent,
        write_delegate: Arc<dyn TableProvider>,
        write_buffer: Option<Arc<WriteBuffer>>,
        branch: Option<String>,
        freshness_enabled: bool,
    ) -> Arc<CachingTableProvider> {
        let freshness = freshness_enabled.then(|| Arc::new(TableFreshness::new()));
        let key = freshness
            .is_some()
            .then(|| crate::freshness::table_key(&ident));
        let provider = Arc::new(CachingTableProvider::new(
            catalog,
            ident,
            write_delegate,
            write_buffer,
            branch,
            freshness.clone(),
        ));
        if let (Some(key), Some(freshness)) = (key, freshness) {
            crate::freshness::register(key, freshness, &provider);
        }
        provider
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
                let provider = Self::build_provider(
                    self.catalog.clone(),
                    ident,
                    write_delegate,
                    self.write_buffer.clone(),
                    self.branch.clone(),
                    self.freshness_enabled,
                );
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
        // DDL fence (freshness mode): a dropped table's provider leaves the
        // refresher registry invalidated, so neither the freshness fast
        // path nor a cached plan can ever serve it again.
        if self.freshness_enabled {
            let ident = TableIdent::new(self.namespace.clone(), name.to_string());
            crate::freshness::deregister(&crate::freshness::table_key(&ident));
        }
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
    fn plan_cache_excludes_overlay_bearing_tables_and_default_mode() {
        // The overlay trap (scope, load-bearing): a buffered/keyed table's
        // overlay is per-scan state, so a plan over it must NEVER be cached
        // — even in freshness mode.
        assert!(!plan_cache_eligible(true, true));
        // Default mode (--freshness-ms 0): a plan-cache hit would skip the
        // exact per-scan freshness check, so nothing is eligible.
        assert!(!plan_cache_eligible(false, false));
        assert!(!plan_cache_eligible(false, true));
        // Freshness mode without an overlay is the one cacheable shape.
        assert!(plan_cache_eligible(true, false));
    }

    #[test]
    fn stale_read_policy_matrix() {
        // Env unset: mode default — freshness mode serves stale
        // (availability; the contract is already bounded staleness), exact
        // mode fails loud (stale would change the exactness contract).
        assert!(stale_read_policy(None, true));
        assert!(!stale_read_policy(None, false));
        // Explicit truthy: stale-serve everywhere (incl. default mode).
        assert!(stale_read_policy(Some(true), false));
        assert!(stale_read_policy(Some(true), true));
        // Explicit falsy (=0): fail-loud even in freshness mode — the
        // opt-out for deployments where RYW after a local write + outage
        // must error, never silently regress (docs/limitations.md).
        assert!(!stale_read_policy(Some(false), true));
        assert!(!stale_read_policy(Some(false), false));
    }

    #[test]
    fn stale_read_env_parses_as_tristate() {
        assert_eq!(parse_stale_read_override(None), None);
        assert_eq!(parse_stale_read_override(Some("1")), Some(true));
        assert_eq!(parse_stale_read_override(Some(" true ")), Some(true));
        assert_eq!(parse_stale_read_override(Some("0")), Some(false));
        assert_eq!(parse_stale_read_override(Some("off")), Some(false));
        assert_eq!(parse_stale_read_override(Some("")), Some(false));
    }

    /// A MarkStaleStream over a forever-pending inner stream, standing in
    /// for an INSERT whose commit request is in flight.
    fn pending_markstale(freshness: Arc<TableFreshness>) -> MarkStaleStream {
        use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
        let schema: ArrowSchemaRef = Arc::new(datafusion::arrow::datatypes::Schema::empty());
        let inner: datafusion::execution::SendableRecordBatchStream = Box::pin(
            RecordBatchStreamAdapter::new(schema, futures::stream::pending()),
        );
        MarkStaleStream {
            inner,
            freshness,
            polled: false,
            invalidated: false,
        }
    }

    #[test]
    fn markstale_stream_dropped_mid_poll_invalidates() {
        use futures::Stream;
        let f = Arc::new(TableFreshness::new());
        f.complete_load(f.begin_load());
        assert!(f.is_fresh());
        let mut stream = pending_markstale(f.clone());
        // First poll returns Pending — the commit request may have been
        // issued — then the client disconnects and the stream is dropped
        // without ever observing completion.
        let waker = futures::task::noop_waker();
        let mut cx = std::task::Context::from_waker(&waker);
        assert!(std::pin::Pin::new(&mut stream)
            .poll_next(&mut cx)
            .is_pending());
        drop(stream);
        assert!(
            !f.is_fresh(),
            "a stream dropped after a started poll must invalidate — the commit may still land"
        );
    }

    #[test]
    fn markstale_stream_dropped_unpolled_stays_fresh() {
        // Never polled = the insert never started executing = no commit
        // could have been issued: the drop guard must NOT invalidate.
        let f = Arc::new(TableFreshness::new());
        f.complete_load(f.begin_load());
        let stream = pending_markstale(f.clone());
        drop(stream);
        assert!(
            f.is_fresh(),
            "an unpolled dropped stream must not invalidate"
        );
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
