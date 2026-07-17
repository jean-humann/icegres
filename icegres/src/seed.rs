//! Idempotent demo data seeding.
//!
//! Creates the `demo` namespace with two tables and populates them:
//!   - `demo.cities`  (city, country, population) — 20 rows
//!   - `demo.trips`   (trip_id, city, distance_km, fare, ts) — 280 rows
//!
//! Namespace and tables are created through the raw Iceberg catalog API
//! (skipped when they already exist). Rows are written by running
//! `INSERT INTO ... VALUES` through a local DataFusion session with the
//! Iceberg table providers, which appends real Parquet files to the object
//! store and commits them through the REST catalog.
//!
//! File layout: each table is seeded with a SINGLE `INSERT` statement through
//! a session pinned to `target_partitions = 1`, so every table's seed data
//! lands as exactly ONE Parquet file in ONE Iceberg commit (iceberg-datafusion
//! writes one file per non-empty output partition of the write plan). Small
//! files dominate read latency on this stack (~1 ms per object-store GET), so
//! the seed layout is itself a performance feature; see bench/SPEC.md §4.4.
//!
//! No `icegres compact` subcommand exists — deliberately. Rewriting existing
//! small files into one needs a replace-files snapshot (add rewritten file +
//! remove source files atomically); the pinned iceberg-rust 0.9.1
//! `Transaction` API only exposes `fast_append`, and while the copy-on-write
//! snapshot machinery in `overwrite.rs` (UPDATE/DELETE) could carry a
//! `replace`-operation compaction, that is future work. Recovering a compact
//! layout is done by dropping the demo tables (Lakekeeper REST,
//! `purgeRequested=true`) and re-running `icegres seed`.
//!
//! Re-seed semantics: rows are inserted only when the seeded dataset is
//! absent (observed row count is zero), so re-running `icegres seed` never
//! duplicates data and repairs a table left empty by an interrupted earlier
//! run. Existing rows are left untouched; for `demo.trips` the check counts
//! only `trip_id` 1..280 so rows appended by the e2e harness don't mask a
//! missing seed.

use std::collections::HashMap;

use anyhow::{bail, Context as _, Result};
use arrow::array::AsArray;
use arrow::datatypes::Int64Type;
use datafusion::prelude::SessionContext;
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type};
use iceberg::{Catalog, NamespaceIdent, TableCreation, TableIdent};
use tracing::{info, warn};

use crate::context::{build_session_context_with, connect_catalog};
use crate::CatalogOpts;

const NAMESPACE: &str = "demo";

/// (city, country, population) reference data; trips reference these cities.
const CITIES: &[(&str, &str, i64)] = &[
    ("Paris", "France", 2_102_650),
    ("Lyon", "France", 522_969),
    ("Berlin", "Germany", 3_677_472),
    ("Munich", "Germany", 1_487_708),
    ("Madrid", "Spain", 3_223_334),
    ("Barcelona", "Spain", 1_620_343),
    ("Rome", "Italy", 2_749_031),
    ("Milan", "Italy", 1_371_498),
    ("London", "United Kingdom", 8_799_800),
    ("Manchester", "United Kingdom", 551_938),
    ("Amsterdam", "Netherlands", 921_402),
    ("Brussels", "Belgium", 1_222_637),
    ("Vienna", "Austria", 1_982_442),
    ("Zurich", "Switzerland", 421_878),
    ("Lisbon", "Portugal", 545_923),
    ("Dublin", "Ireland", 592_713),
    ("Copenhagen", "Denmark", 644_431),
    ("Stockholm", "Sweden", 984_748),
    ("Oslo", "Norway", 709_037),
    ("Warsaw", "Poland", 1_863_056),
];

const TRIP_COUNT: usize = 280;

pub async fn run(opts: &CatalogOpts) -> Result<()> {
    info!(
        catalog_uri = %opts.catalog_uri,
        warehouse = %opts.warehouse,
        "seeding demo data"
    );
    let catalog = connect_catalog(opts).await?;

    let ns = NamespaceIdent::new(NAMESPACE.to_string());
    if catalog
        .namespace_exists(&ns)
        .await
        .context("failed to check namespace existence")?
    {
        info!("namespace {NAMESPACE} already exists");
    } else {
        catalog
            .create_namespace(&ns, HashMap::new())
            .await
            .context("failed to create namespace")?;
        info!("created namespace {NAMESPACE}");
    }

    ensure_table(catalog.as_ref(), &ns, "cities", cities_schema()?).await?;
    ensure_table(catalog.as_ref(), &ns, "trips", trips_schema()?).await?;
    // Deliberately left EMPTY: scratch target for ADBC bulk-ingest tests and
    // the adbc_bulk_ingest bench extra (trips-shaped; callers clear it with
    // an unpredicated DELETE between runs). Created here because servers
    // snapshot the table list at boot.
    ensure_table(catalog.as_ref(), &ns, "adbc_ingest", trips_schema()?).await?;

    // Build the session AFTER table creation: the Iceberg catalog provider
    // snapshots the table list at construction time. target_partitions is
    // pinned to 1 so each table's single INSERT writes exactly one Parquet
    // file (the write plan emits one file per non-empty output partition).
    let ctx = build_session_context_with(catalog, Some(1), None, None, 0).await?;

    // Insert based on the observed row count (not on whether this run created
    // the table), so a table left empty by an interrupted earlier seed run is
    // repopulated instead of being skipped forever.
    let cities_count = count_rows(&ctx, "cities", "").await?;
    if cities_count == 0 {
        insert_all(&ctx, "cities", "city, country, population", &cities_rows()).await?;
    } else {
        info!("demo.cities already populated; leaving data as-is");
    }
    // Count only the deterministic seed range: the e2e harness appends extra
    // rows with trip_id >= 900000 that must not mask a missing seed.
    let seeded_trips_filter = format!(" WHERE trip_id BETWEEN 1 AND {TRIP_COUNT}");
    let seeded_trips = count_rows(&ctx, "trips", &seeded_trips_filter).await?;
    if seeded_trips == 0 {
        insert_all(
            &ctx,
            "trips",
            "trip_id, city, distance_km, fare, ts",
            &trips_rows(),
        )
        .await?;
    } else if seeded_trips < TRIP_COUNT as i64 {
        warn!(
            "demo.trips holds a partial seed ({seeded_trips} of {TRIP_COUNT} seeded rows); \
             leaving data as-is — drop the table and re-run seed to repair"
        );
    } else {
        info!("demo.trips already populated; leaving data as-is");
    }

    for table in ["cities", "trips"] {
        let count = count_rows(&ctx, table, "").await?;
        info!("demo.{table}: {count} rows");
    }
    info!("seed complete");
    Ok(())
}

/// Create the table via the raw catalog API unless it already exists.
async fn ensure_table(
    catalog: &dyn Catalog,
    ns: &NamespaceIdent,
    name: &str,
    schema: Schema,
) -> Result<()> {
    let ident = TableIdent::new(ns.clone(), name.to_string());
    if catalog
        .table_exists(&ident)
        .await
        .with_context(|| format!("failed to check existence of table {NAMESPACE}.{name}"))?
    {
        info!("table {NAMESPACE}.{name} already exists");
        return Ok(());
    }
    let creation = TableCreation::builder()
        .name(name.to_string())
        .schema(schema)
        .build();
    catalog
        .create_table(ns, creation)
        .await
        .with_context(|| format!("failed to create table {NAMESPACE}.{name}"))?;
    info!("created table {NAMESPACE}.{name}");
    Ok(())
}

fn cities_schema() -> Result<Schema> {
    Schema::builder()
        .with_fields(vec![
            NestedField::optional(1, "city", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(2, "country", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(3, "population", Type::Primitive(PrimitiveType::Long)).into(),
        ])
        .build()
        .context("failed to build cities schema")
}

fn trips_schema() -> Result<Schema> {
    Schema::builder()
        .with_fields(vec![
            NestedField::optional(1, "trip_id", Type::Primitive(PrimitiveType::Long)).into(),
            NestedField::optional(2, "city", Type::Primitive(PrimitiveType::String)).into(),
            NestedField::optional(3, "distance_km", Type::Primitive(PrimitiveType::Double)).into(),
            NestedField::optional(4, "fare", Type::Primitive(PrimitiveType::Double)).into(),
            NestedField::optional(5, "ts", Type::Primitive(PrimitiveType::Timestamp)).into(),
        ])
        .build()
        .context("failed to build trips schema")
}

fn cities_rows() -> Vec<String> {
    CITIES
        .iter()
        .map(|(city, country, population)| format!("('{city}', '{country}', {population})"))
        .collect()
}

/// Deterministic pseudo-random trips over the demo cities (LCG, fixed seed).
fn trips_rows() -> Vec<String> {
    let mut rng = Lcg::new(42);
    (1..=TRIP_COUNT as i64)
        .map(|trip_id| {
            let city = CITIES[(rng.next() as usize) % CITIES.len()].0;
            let distance_km = 0.5 + f64::from(rng.next() % 3_000) / 100.0; // 0.5..30.5 km
            let fare = 2.5 + distance_km * 1.35 + f64::from(rng.next() % 200) / 100.0;
            // Timestamps spread across June 2026.
            let offset_minutes = rng.next() % (30 * 24 * 60);
            let day = 1 + offset_minutes / (24 * 60);
            let hour = (offset_minutes / 60) % 24;
            let minute = offset_minutes % 60;
            format!(
                "({trip_id}, '{city}', {distance_km:.2}, {fare:.2}, \
                 TIMESTAMP '2026-06-{day:02} {hour:02}:{minute:02}:00')"
            )
        })
        .collect()
}

/// Append all rows through the DataFusion INSERT path as ONE statement, i.e.
/// one Iceberg commit — and, with the seed session's `target_partitions = 1`,
/// one Parquet data file (see the module docs on file layout).
async fn insert_all(
    ctx: &SessionContext,
    table: &str,
    columns: &str,
    rows: &[String],
) -> Result<()> {
    let sql = format!(
        "INSERT INTO {NAMESPACE}.{table} ({columns}) VALUES {}",
        rows.join(", ")
    );
    ctx.sql(&sql)
        .await
        .with_context(|| format!("failed to plan INSERT into {NAMESPACE}.{table}"))?
        .collect()
        .await
        .with_context(|| format!("failed to execute INSERT into {NAMESPACE}.{table}"))?;
    info!(
        "inserted {} rows into {NAMESPACE}.{table} (single commit)",
        rows.len()
    );
    Ok(())
}

/// Count rows in a table; `filter` is either empty or a ` WHERE ...` clause.
async fn count_rows(ctx: &SessionContext, table: &str, filter: &str) -> Result<i64> {
    let batches = ctx
        .sql(&format!("SELECT count(*) FROM {NAMESPACE}.{table}{filter}"))
        .await
        .with_context(|| format!("failed to plan count for {NAMESPACE}.{table}"))?
        .collect()
        .await
        .with_context(|| format!("failed to count rows in {NAMESPACE}.{table}"))?;
    let Some(batch) = batches.first() else {
        bail!("count query for {NAMESPACE}.{table} returned no batches");
    };
    Ok(batch.column(0).as_primitive::<Int64Type>().value(0))
}

/// Minimal deterministic linear congruential generator (no rand dependency).
struct Lcg(u64);

impl Lcg {
    fn new(seed: u64) -> Self {
        Self(seed)
    }

    fn next(&mut self) -> u32 {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.0 >> 33) as u32
    }
}
