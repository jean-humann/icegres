//! `icegres maintain` — table lifecycle maintenance over Iceberg metadata.
//!
//! Long-lived Iceberg tables accumulate a snapshot per commit forever: every
//! INSERT/UPDATE/DELETE/branch op adds one, and nothing ever removes them
//! automatically. Unbounded, that makes `$snapshots` (and the metadata JSON
//! the catalog loads on every table open) grow without limit. Expiring old
//! snapshots is the standard remedy — a metadata-only commit that drops
//! snapshots older than a retention window while keeping the newest N and
//! every snapshot still reachable from a branch/tag ref.
//!
//! ```text
//! icegres maintain expire-snapshots demo.trips            # keep newest 10
//! icegres maintain expire-snapshots demo.trips --keep 50  # keep newest 50
//! ```
//!
//! Expiry is metadata-only: the expired snapshots' data and manifest files
//! are left in object storage. `remove-orphans` is the second half — the
//! orphan-file GC that reclaims those bytes:
//!
//! ```text
//! icegres maintain remove-orphans demo.trips                        # dry run
//! icegres maintain remove-orphans demo.trips --execute              # delete (72h grace)
//! icegres maintain remove-orphans demo.trips --older-than-hours 168 --execute
//! ```
//!
//! It lists every object under the table's storage prefix, subtracts the
//! LIVE set (every data file, manifest and manifest list reachable from
//! EVERY retained snapshot — all branches/tags included, DELETED manifest
//! entries too — plus the current metadata JSON, every metadata JSON in the
//! metadata log, and any statistics files), and treats the difference as
//! orphans, filtered to objects older than `--older-than-hours` (default
//! 72 h — the grace window is THE guard for in-flight commits: files written
//! by a commit, ours or a foreign writer's, that has not landed in the
//! catalog yet are protected only by their young age). Dry-run by default;
//! `--execute` deletes.
//!
//! Safety rules (each fails closed):
//! * a table whose metadata (or any manifest) cannot be fully loaded aborts
//!   the whole run — an incomplete live set must never be subtracted from;
//! * a recorded metadata path that cannot be mapped into the listed bucket
//!   (foreign bucket/scheme — bucket aliases, endpoint rewrites) aborts the
//!   whole run: liveness cannot be verified against a listing that cannot
//!   see the file, and silently shrinking the live set could delete live
//!   objects;
//! * recorded paths and listed keys are canonicalized identically
//!   (duplicate `/` collapsed, exactly like opendal's path normalization)
//!   before any live-set membership test, so a table location with a
//!   trailing slash (recording `pfx//data/x.parquet` while the store lists
//!   `pfx/data/x.parquet`) can never make a live file look orphaned;
//! * `--execute` with a grace window under 1 hour is refused unless
//!   `--unsafe-grace` explicitly asserts the table is quiescent (dry runs
//!   stay unrestricted — they delete nothing);
//! * clock skew between this host and the object store's last-modified
//!   stamps is covered by a fixed 15-minute allowance folded into the
//!   cutoff (cutoff = now − grace − allowance); `--execute` additionally
//!   measures the REAL skew with a tiny write/stat/delete probe object
//!   under `metadata/` and aborts when |skew| exceeds the allowance (a
//!   probe failure also aborts; dry runs never write the probe;
//!   `--unsafe-grace` drops the allowance from the cutoff — quiescent
//!   tables only — but the probe still runs on every `--execute`);
//! * objects with an unknown/unparseable age are NEVER deleted (WARN+skip);
//! * objects under the prefix that are not clearly a data/metadata artifact
//!   of this table (`data/*.parquet`, `metadata/*.avro|*.metadata.json|
//!   *.stats|*.puffin`) are skipped with a WARN;
//! * after listing, the table is reloaded: a UUID change aborts, and a head
//!   (metadata-location) change re-derives the live set against the fresh
//!   metadata so a commit racing the run can never lose its files.
//!
//! Both commands are safe to run against a live serving endpoint: expiry is
//! anchored with `assert-table-uuid` + `assert-ref-snapshot-id main=<head>`,
//! and the GC's grace window + reload check keep concurrent commits whole.
//!
//! The third maintenance command, `icegres maintain compact` (bin-pack
//! small-file compaction, dry-run by default), lives in compact.rs — it
//! rides the same anchored-commit discipline, and its replaced files are
//! NOT orphans to this module's GC until expiry drops the snapshots that
//! still reference them (the live set includes DELETED manifest entries).

use std::collections::HashSet;
use std::time::{Duration, SystemTime};

use anyhow::{anyhow, bail, Context as _, Result};
use futures::TryStreamExt as _;
use iceberg::table::Table;
use opendal::services::S3;
use opendal::Operator;
use tracing::warn;

use crate::branch::parse_table;
use crate::context;
use crate::overwrite::OverwriteEngine;
use crate::CatalogOpts;

/// `icegres maintain expire-snapshots <table> [--keep N]`
pub async fn expire_snapshots(opts: &CatalogOpts, table: &str, keep_last: usize) -> Result<()> {
    let ident = parse_table(table)?;
    let catalog = context::connect_catalog(opts).await?;
    let engine = OverwriteEngine::connect(catalog, opts, false, None).await?;
    let removed = engine
        .expire_snapshots(&ident, keep_last)
        .await
        .with_context(|| format!("failed to expire snapshots on {ident}"))?;
    if removed == 0 {
        println!(
            "expired 0 snapshots on {ident} (nothing older than the newest {keep_last} + \
             referenced refs)"
        );
    } else {
        println!(
            "expired {removed} snapshot(s) on {ident} (kept newest {keep_last} + all \
             ref-referenced; reclaim their files with: icegres maintain remove-orphans {ident})"
        );
    }
    Ok(())
}

/// One orphan candidate: object key (bucket-relative) + size in bytes.
struct Orphan {
    key: String,
    bytes: u64,
}

/// Fixed clock-skew allowance between this host's clock and the object
/// store's last-modified stamps. Folded into the age cutoff (fail closed:
/// a store clock running behind can never make an in-flight commit's fresh
/// file look old enough to delete), and used as the abort threshold for the
/// `--execute` skew probe.
const CLOCK_SKEW_ALLOWANCE: Duration = Duration::from_secs(15 * 60);

/// `icegres maintain remove-orphans <table> [--older-than-hours N]
/// [--execute] [--unsafe-grace]`
///
/// Dry-run by default: prints the orphan count/bytes and up to 20 sample
/// paths. With `execute = true`, deletes the orphans and reports what was
/// deleted. See the module docs for the exact live-set / safety semantics.
pub async fn remove_orphans(
    opts: &CatalogOpts,
    table: &str,
    older_than_hours: u64,
    execute: bool,
    unsafe_grace: bool,
) -> Result<()> {
    // SAFETY: refuse an --execute whose grace window cannot protect
    // in-flight commits, unless --unsafe-grace asserts quiescence.
    check_grace_window(older_than_hours, execute, unsafe_grace)?;

    let ident = parse_table(table)?;
    let catalog = context::connect_catalog(opts).await?;

    // SAFETY: a table whose metadata cannot be fully loaded must never be
    // GC'd — an incomplete live set would classify live files as orphans.
    let tbl = catalog
        .load_table(&ident)
        .await
        .map_err(|e| anyhow!("refusing to GC {ident}: table metadata cannot be loaded: {e}"))?;
    let uuid = tbl.metadata().uuid();
    let location = tbl.metadata().location().trim_end_matches('/').to_string();

    // Only S3-scheme locations are supported: the listing backend is built
    // from the CatalogOpts S3 settings. Anything else fails loudly rather
    // than silently reporting "no orphans".
    let (bucket, key_prefix) = split_s3_location(&location)?;
    let op = build_s3_operator(opts, &bucket)?;
    let data_prefix = format!("{key_prefix}/data/");
    let meta_prefix = format!("{key_prefix}/metadata/");

    // SAFETY (clock skew): the age cutoff compares OUR clock against
    // last-modified stamps written by the STORE's clock. A fixed allowance
    // is folded into the cutoff so a skewed store clock cannot erode the
    // grace window; --execute additionally measures the real skew with a
    // probe object and aborts beyond the allowance. --unsafe-grace drops
    // the allowance from the cutoff (the operator asserts the table is
    // quiescent, so there are no in-flight commits for it to protect); the
    // probe still runs on every --execute.
    let skew_allowance = if unsafe_grace {
        Duration::ZERO
    } else {
        CLOCK_SKEW_ALLOWANCE
    };
    let cutoff = SystemTime::now()
        .checked_sub(Duration::from_secs(older_than_hours.saturating_mul(3600)))
        .and_then(|t| t.checked_sub(skew_allowance))
        .with_context(|| format!("--older-than-hours {older_than_hours} is out of range"))?;
    if execute {
        // Probe failure = abort; dry runs never write the probe.
        abort_on_clock_skew(&op, &bucket, &meta_prefix).await?;
    }

    // LIVE set first, then the listing: an object created between the two
    // steps is caught either by the grace window or the reload check below.
    let mut live = live_object_keys(&tbl, &bucket).await?;

    // Enumerate every object under the table prefix.
    let list_path = format!("{key_prefix}/");
    let mut lister = op
        .lister_with(&list_path)
        .recursive(true)
        .await
        .with_context(|| {
            format!(
                "cannot list s3://{bucket}/{list_path} via {} — object-store listing is \
                 required for orphan GC (nothing was deleted)",
                opts.s3_endpoint
            )
        })?;

    let mut orphans: Vec<Orphan> = Vec::new();
    let (mut total, mut live_seen, mut too_young, mut unknown_age, mut foreign) =
        (0u64, 0u64, 0u64, 0u64, 0u64);
    while let Some(entry) = lister
        .try_next()
        .await
        .with_context(|| format!("object listing of s3://{bucket}/{list_path} failed mid-stream"))?
    {
        if entry.metadata().is_dir() {
            continue;
        }
        // SAFETY: canonicalize the listed key exactly like every recorded
        // metadata path (canonical_key on BOTH sides) so no separator
        // asymmetry can ever make a live file miss the live-set lookup.
        let key = canonical_key(entry.path());
        total += 1;
        if live.contains(&key) {
            live_seen += 1;
            continue;
        }
        // SAFETY: only objects that are clearly this table's own data or
        // metadata artifacts are ever candidates; anything else under the
        // prefix is skipped with a WARN (a foreign tool's file, a marker
        // object, an artifact class this build does not know).
        if !is_table_artifact(&key, &data_prefix, &meta_prefix) {
            foreign += 1;
            warn!(
                object = %format!("s3://{bucket}/{key}"),
                "not a recognized data/metadata artifact of this table; skipping (never deleted)"
            );
            continue;
        }
        // SAFETY: unknown/unparseable object age is never deleted. The
        // lister usually carries last-modified; fall back to a HEAD.
        let last_modified = match entry.metadata().last_modified() {
            Some(t) => Some(t),
            None => op.stat(&key).await.ok().and_then(|m| m.last_modified()),
        };
        let Some(last_modified) = last_modified else {
            unknown_age += 1;
            warn!(
                object = %format!("s3://{bucket}/{key}"),
                "object age unknown; skipping (never deleted)"
            );
            continue;
        };
        if SystemTime::from(last_modified) > cutoff {
            too_young += 1; // inside the grace window: possibly an in-flight commit
            continue;
        }
        orphans.push(Orphan {
            key,
            bytes: entry.metadata().content_length(),
        });
    }

    // Reload check: a commit that landed while we listed may have written
    // files our (older) live set does not contain. Re-derive the live set
    // until the head is stable, dropping candidates that became live.
    let mut current_metadata_location = tbl
        .metadata_location()
        .context("table has no metadata location; refusing to GC")?
        .to_string();
    for attempt in 0..3 {
        let fresh = catalog.load_table(&ident).await.map_err(|e| {
            anyhow!("refusing to GC {ident}: table metadata cannot be reloaded: {e}")
        })?;
        if fresh.metadata().uuid() != uuid {
            bail!(
                "table {ident} UUID changed mid-run (dropped and recreated?); aborting, \
                 nothing was deleted"
            );
        }
        let fresh_location = fresh
            .metadata_location()
            .context("table has no metadata location; refusing to GC")?
            .to_string();
        if fresh_location == current_metadata_location {
            break;
        }
        if attempt == 2 {
            bail!(
                "table {ident} keeps being committed to while computing the orphan set; \
                 aborting, nothing was deleted — re-run when write traffic allows"
            );
        }
        let fresh_live = live_object_keys(&fresh, &bucket).await?;
        orphans.retain(|o| !fresh_live.contains(&o.key));
        live = fresh_live;
        current_metadata_location = fresh_location;
    }
    drop(live);

    let orphan_bytes: u64 = orphans.iter().map(|o| o.bytes).sum();
    let n = orphans.len();
    println!(
        "scanned {total} object(s) under {location}: {live_seen} live, {too_young} within the \
         {older_than_hours}h grace window, {unknown_age} of unknown age (kept), {foreign} \
         unrecognized (kept)"
    );
    if n == 0 {
        println!("found 0 orphan file(s) under {location} (nothing to reclaim)");
        return Ok(());
    }
    if !execute {
        println!(
            "found {n} orphan file(s) totaling {orphan_bytes} bytes under {location} \
             (DRY RUN — nothing deleted; re-run with --execute to delete)"
        );
        for o in orphans.iter().take(20) {
            println!("  s3://{bucket}/{}", o.key);
        }
        if n > 20 {
            println!("  ... and {} more", n - 20);
        }
        return Ok(());
    }

    let mut deleted = 0usize;
    let mut deleted_bytes = 0u64;
    for o in &orphans {
        op.delete(&o.key).await.with_context(|| {
            format!(
                "failed to delete s3://{bucket}/{} — aborting after {deleted} of {n} orphan(s) \
                 ({deleted_bytes} of {orphan_bytes} bytes) deleted; re-run to finish",
                o.key
            )
        })?;
        deleted += 1;
        deleted_bytes += o.bytes;
    }
    println!("deleted {deleted} orphan file(s) totaling {deleted_bytes} bytes under {location}");
    Ok(())
}

/// FAIL-CLOSED guard on the grace window: `--execute` with a sub-1-hour
/// window offers in-flight commits no protection at all, so it is refused
/// unless `--unsafe-grace` explicitly asserts the table is quiescent.
/// Dry runs stay unrestricted — they delete nothing.
fn check_grace_window(older_than_hours: u64, execute: bool, unsafe_grace: bool) -> Result<()> {
    if execute && older_than_hours < 1 && !unsafe_grace {
        bail!(
            "refusing --execute with --older-than-hours {older_than_hours}: a grace window \
             under 1 hour cannot protect files written by in-flight commits. Pass \
             --unsafe-grace ONLY if the table is quiescent (e.g. tests) — concurrent \
             writers WILL lose in-flight files. Nothing was deleted"
        );
    }
    Ok(())
}

/// Canonicalize an object key the way opendal's `normalize_path` does:
/// split on `/`, drop empty segments (leading/trailing/duplicate
/// separators), rejoin. Applied to BOTH sides of every live-set membership
/// test — every recorded metadata path, every listed candidate key, and
/// the data/metadata prefixes — so a table location with a trailing slash
/// (which records `pfx//data/x.parquet` while the store lists
/// `pfx/data/x.parquet`) can never make a live file look orphaned.
fn canonical_key(key: &str) -> String {
    key.split('/')
        .filter(|seg| !seg.is_empty())
        .collect::<Vec<_>>()
        .join("/")
}

/// SAFETY probe for `--execute`: measure the REAL clock skew between this
/// host and the object store by writing a tiny probe object under the
/// table's `metadata/` prefix, stat'ing its last-modified against our own
/// clock, and deleting it. Aborts (nothing deleted) when |skew| exceeds
/// [`CLOCK_SKEW_ALLOWANCE`] — a store clock that far off would erode the
/// grace window's in-flight-commit protection. Any probe failure (write,
/// stat, missing last-modified, delete) also aborts: fail closed.
async fn abort_on_clock_skew(op: &Operator, bucket: &str, meta_prefix: &str) -> Result<()> {
    let nanos = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0);
    let probe_key = format!(
        "{meta_prefix}.icegres-gc-skew-probe-{}-{nanos}",
        std::process::id()
    );
    let before = SystemTime::now();
    op.write(
        &probe_key,
        b"icegres remove-orphans clock-skew probe".to_vec(),
    )
    .await
    .with_context(|| {
        format!(
            "clock-skew probe write to s3://{bucket}/{probe_key} failed — aborting, \
                 nothing was deleted"
        )
    })?;
    // Stat BEFORE deleting, but always attempt the delete so a stat failure
    // does not leak the probe object.
    let stat = op.stat(&probe_key).await;
    let after = SystemTime::now();
    let deleted = op.delete(&probe_key).await;
    let meta = stat.with_context(|| {
        format!(
            "clock-skew probe stat of s3://{bucket}/{probe_key} failed — aborting, \
             nothing was deleted"
        )
    })?;
    deleted.with_context(|| {
        format!(
            "clock-skew probe delete of s3://{bucket}/{probe_key} failed — aborting, \
             nothing was deleted (remove the probe object manually)"
        )
    })?;
    let last_modified = meta.last_modified().with_context(|| {
        format!(
            "clock-skew probe s3://{bucket}/{probe_key} came back without a last-modified \
             stamp — the store's object ages cannot be trusted; aborting, nothing was deleted"
        )
    })?;
    let last_modified = SystemTime::from(last_modified);
    // Skew = how far the store's stamp falls outside our [before, after]
    // write window (stores commonly truncate to whole seconds; the window
    // comparison absorbs that).
    let skew = if last_modified > after {
        last_modified.duration_since(after).unwrap_or_default()
    } else if last_modified < before {
        before.duration_since(last_modified).unwrap_or_default()
    } else {
        Duration::ZERO
    };
    if skew > CLOCK_SKEW_ALLOWANCE {
        bail!(
            "object-store clock is ~{}s off from this host (allowance {}s): the grace \
             window cannot be trusted to protect in-flight commits; aborting, nothing \
             was deleted — fix the clocks and re-run",
            skew.as_secs(),
            CLOCK_SKEW_ALLOWANCE.as_secs()
        );
    }
    Ok(())
}

/// Split an `s3://bucket/key/prefix` table location into (bucket, key
/// prefix). The key prefix is canonicalized (see [`canonical_key`]) so the
/// `data/`/`metadata/` prefixes and the listing path match the store's
/// normalized keys even for locations written with duplicate or trailing
/// slashes. Non-S3 locations are refused: this GC's listing backend is S3.
fn split_s3_location(location: &str) -> Result<(String, String)> {
    let rest = location
        .strip_prefix("s3://")
        .or_else(|| location.strip_prefix("s3a://"))
        .with_context(|| {
            format!(
                "table location {location:?} is not s3:// — orphan GC supports S3-compatible \
                 object stores only (the listing backend is built from the --s3-* options)"
            )
        })?;
    let (bucket, key) = rest
        .split_once('/')
        .with_context(|| format!("table location {location:?} has no key below the bucket"))?;
    let key = canonical_key(key);
    if bucket.is_empty() || key.is_empty() {
        bail!("table location {location:?} has an empty bucket or key");
    }
    Ok((bucket.to_string(), key))
}

/// Build an opendal S3 operator over `bucket` from the same CatalogOpts S3
/// settings context.rs feeds to FileIO: path-style addressing (RustFS has no
/// virtual-hosted-style routing; opendal defaults to path style), no AWS
/// config-file/EC2-metadata lookups.
fn build_s3_operator(opts: &CatalogOpts, bucket: &str) -> Result<Operator> {
    let builder = S3::default()
        .endpoint(&opts.s3_endpoint)
        .region(&opts.s3_region)
        .access_key_id(&opts.s3_access_key)
        .secret_access_key(&opts.s3_secret_key)
        .bucket(bucket)
        .disable_config_load()
        .disable_ec2_metadata();
    Ok(Operator::new(builder)
        .with_context(|| {
            format!(
                "cannot build the S3 listing backend for bucket {bucket:?} at {}",
                opts.s3_endpoint
            )
        })?
        .finish())
}

/// Whether `key` is clearly a data/metadata artifact of the table whose
/// data/metadata prefixes are given. Everything else is skipped with a WARN
/// by the caller (never deleted).
fn is_table_artifact(key: &str, data_prefix: &str, meta_prefix: &str) -> bool {
    if let Some(rest) = key.strip_prefix(data_prefix) {
        return !rest.is_empty() && rest.ends_with(".parquet");
    }
    if let Some(rest) = key.strip_prefix(meta_prefix) {
        return !rest.is_empty()
            && (rest.ends_with(".avro")
                || rest.ends_with(".metadata.json")
                || rest.ends_with(".stats")
                || rest.ends_with(".puffin"));
    }
    false
}

/// Normalize an absolute `s3://bucket/key` (or `s3a://`) file path from
/// table metadata into a canonicalized bucket-relative object key, or
/// `None` when it points at a different bucket/scheme. The caller must
/// treat `None` as fatal (see [`LiveSet`]): a recorded live file our
/// listing cannot see means liveness cannot be verified.
fn object_key(path: &str, bucket: &str) -> Option<String> {
    let rest = path
        .strip_prefix("s3://")
        .or_else(|| path.strip_prefix("s3a://"))?;
    let (b, key) = rest.split_once('/')?;
    if b != bucket {
        return None;
    }
    let key = canonical_key(key);
    if key.is_empty() {
        return None;
    }
    Some(key)
}

/// Live-set accumulator. Recorded metadata paths are canonicalized into
/// bucket-relative keys; any recorded path that cannot be mapped into the
/// listed bucket (different bucket, different scheme, empty key) FAILS the
/// whole run at [`LiveSet::finish`]: silently dropping it would shrink the
/// live set, and under bucket aliasing / endpoint rewrites the very same
/// physical object may appear in our listing and be deleted while live.
/// Fail closed.
struct LiveSet {
    bucket: String,
    keys: HashSet<String>,
    unmappable_samples: Vec<String>,
    unmappable_total: usize,
}

impl LiveSet {
    fn new(bucket: &str) -> Self {
        LiveSet {
            bucket: bucket.to_string(),
            keys: HashSet::new(),
            unmappable_samples: Vec::new(),
            unmappable_total: 0,
        }
    }

    fn add(&mut self, path: &str) {
        match object_key(path, &self.bucket) {
            Some(key) => {
                self.keys.insert(key);
            }
            None => {
                self.unmappable_total += 1;
                if self.unmappable_samples.len() < 5 {
                    self.unmappable_samples.push(path.to_string());
                }
            }
        }
    }

    /// The completed live set — or an error (abort, nothing deleted) when
    /// any recorded path could not be mapped into the listed bucket.
    fn finish(self) -> Result<HashSet<String>> {
        if self.unmappable_total > 0 {
            bail!(
                "table records {} file(s) outside the listed bucket {:?} (e.g. {}); \
                 remove-orphans cannot verify liveness against a listing that cannot see \
                 them — aborting, nothing was deleted",
                self.unmappable_total,
                self.bucket,
                self.unmappable_samples.join(", ")
            );
        }
        Ok(self.keys)
    }
}

/// Enumerate every object key the table still references — the LIVE set:
/// for EVERY retained snapshot (which covers every branch/tag ref, since a
/// ref can only point at a retained snapshot) the manifest list, every
/// manifest, and every data file referenced by any manifest entry INCLUDING
/// non-alive (DELETED) entries — a DELETED entry still names a physical file
/// the previous snapshot reads; plus the current metadata JSON, every
/// metadata JSON in the metadata log, and any statistics/partition-statistics
/// files. Any unreadable manifest fails the WHOLE run (never skips): an
/// incomplete live set must never be subtracted from. Any recorded path
/// outside `bucket` also fails the whole run (see [`LiveSet`]): a live set
/// that silently dropped paths must never be subtracted from either.
async fn live_object_keys(table: &Table, bucket: &str) -> Result<HashSet<String>> {
    let metadata = table.metadata();
    let file_io = table.file_io();
    let mut live = LiveSet::new(bucket);

    // Metadata JSONs: the current one is mandatory (refuse without it — we
    // could not even anchor the run), previous ones come from the metadata
    // log (bounded by write.metadata.previous-versions-max; metadata JSONs
    // older than the log are deletable candidates like any other artifact).
    let current = table
        .metadata_location()
        .context("table has no metadata location; refusing to GC")?;
    live.add(current);
    for log in metadata.metadata_log() {
        live.add(&log.metadata_file);
    }

    // Statistics files (not written by icegres, but a foreign writer may
    // have attached them; anything referenced is live).
    for stats in metadata.statistics_iter() {
        live.add(&stats.statistics_path);
    }
    for stats in metadata.partition_statistics_iter() {
        live.add(&stats.statistics_path);
    }

    for snapshot in metadata.snapshots() {
        live.add(snapshot.manifest_list());
        let manifest_list =
            crate::overwrite::load_manifest_list(file_io, snapshot, &table.metadata_ref())
                .await
                .map_err(|e| {
                    anyhow!(
                        "failed to load manifest list {} (snapshot {}): {e} — aborting the \
                         whole run: an incomplete live set must never drive deletions",
                        snapshot.manifest_list(),
                        snapshot.snapshot_id()
                    )
                })?;
        for manifest_file in manifest_list.entries() {
            live.add(&manifest_file.manifest_path);
            let manifest = manifest_file.load_manifest(file_io).await.map_err(|e| {
                anyhow!(
                    "failed to load manifest {}: {e} — aborting the whole run: an incomplete \
                     live set must never drive deletions",
                    manifest_file.manifest_path
                )
            })?;
            let (entries, _meta) = manifest.into_parts();
            for entry in &entries {
                // ALL entries, including DELETED: a DELETED entry still
                // references a physical file other snapshots may read.
                live.add(entry.data_file().file_path());
            }
        }
    }
    live.finish()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn split_s3_location_accepts_s3_and_s3a() {
        let (b, k) = split_s3_location("s3://lakehouse/abc/def").unwrap();
        assert_eq!((b.as_str(), k.as_str()), ("lakehouse", "abc/def"));
        let (b, k) = split_s3_location("s3a://bkt/x/").unwrap();
        assert_eq!((b.as_str(), k.as_str()), ("bkt", "x"));
    }

    #[test]
    fn split_s3_location_rejects_non_s3() {
        assert!(split_s3_location("file:///tmp/t").is_err());
        assert!(split_s3_location("gs://bkt/x").is_err());
        assert!(split_s3_location("s3://bucketonly").is_err());
        assert!(split_s3_location("s3:///nokey").is_err());
    }

    #[test]
    fn object_key_normalizes_and_filters_buckets() {
        assert_eq!(
            object_key("s3://lakehouse/a/b/c.parquet", "lakehouse").as_deref(),
            Some("a/b/c.parquet")
        );
        assert_eq!(
            object_key("s3a://lakehouse/a.avro", "lakehouse").as_deref(),
            Some("a.avro")
        );
        // Different bucket or scheme: cannot be mapped into the listing —
        // the LiveSet accumulator turns these into a whole-run abort.
        assert_eq!(object_key("s3://other/a.parquet", "lakehouse"), None);
        assert_eq!(object_key("file:///a.parquet", "lakehouse"), None);
        assert_eq!(object_key("s3://lakehouse/", "lakehouse"), None);
    }

    #[test]
    fn canonical_key_collapses_duplicate_separators() {
        assert_eq!(canonical_key("a/b/c.parquet"), "a/b/c.parquet");
        assert_eq!(canonical_key("a//b/c.parquet"), "a/b/c.parquet");
        assert_eq!(canonical_key("/a///b/"), "a/b");
        assert_eq!(canonical_key(""), "");
        assert_eq!(canonical_key("///"), "");
    }

    #[test]
    fn trailing_slash_location_roundtrips_to_listed_key() {
        // A table location with a trailing slash ("s3://bkt/pfx/") records
        // "s3://bkt/pfx//data/x.parquet" in metadata while the store lists
        // "pfx/data/x.parquet": both sides must canonicalize to the same
        // key or the live file would be classified as an orphan.
        let recorded = object_key("s3://bkt/pfx//data/x.parquet", "bkt").unwrap();
        let listed = canonical_key("pfx/data/x.parquet");
        assert_eq!(recorded, listed);
    }

    #[test]
    fn double_slash_inside_recorded_path_is_canonicalized() {
        assert_eq!(
            object_key("s3://bkt/a//b///c.avro", "bkt").as_deref(),
            Some("a/b/c.avro")
        );
    }

    #[test]
    fn split_s3_location_canonicalizes_key_prefix() {
        let (b, k) = split_s3_location("s3://bkt/a//b/").unwrap();
        assert_eq!((b.as_str(), k.as_str()), ("bkt", "a/b"));
    }

    #[test]
    fn live_set_accepts_in_bucket_paths_canonicalized() {
        let mut live = LiveSet::new("lakehouse");
        live.add("s3://lakehouse/t//data/a.parquet");
        live.add("s3a://lakehouse/t/metadata/m.avro");
        let keys = live.finish().unwrap();
        assert!(keys.contains("t/data/a.parquet"));
        assert!(keys.contains("t/metadata/m.avro"));
        assert_eq!(keys.len(), 2);
    }

    #[test]
    fn live_set_fails_closed_on_foreign_bucket_paths() {
        let mut live = LiveSet::new("lakehouse");
        live.add("s3://lakehouse/t/data/a.parquet");
        live.add("s3://other-bucket/t/data/b.parquet");
        live.add("file:///t/data/c.parquet");
        let err = live.finish().unwrap_err().to_string();
        assert!(err.contains("2 file(s)"), "{err}");
        assert!(err.contains("s3://other-bucket/t/data/b.parquet"), "{err}");
        assert!(err.contains("file:///t/data/c.parquet"), "{err}");
        assert!(err.contains("nothing was deleted"), "{err}");
    }

    #[test]
    fn live_set_abort_samples_are_capped_at_five() {
        let mut live = LiveSet::new("lakehouse");
        for i in 0..7 {
            live.add(&format!("s3://elsewhere/f{i}.parquet"));
        }
        let err = live.finish().unwrap_err().to_string();
        assert!(err.contains("7 file(s)"), "{err}");
        assert!(err.contains("f4.parquet"), "{err}");
        assert!(!err.contains("f5.parquet"), "{err}");
    }

    #[test]
    fn grace_window_refuses_unsafe_execute_only() {
        // --execute with a sub-1h window is refused without --unsafe-grace.
        let err = check_grace_window(0, true, false).unwrap_err().to_string();
        assert!(err.contains("--unsafe-grace"), "{err}");
        assert!(err.contains("Nothing was deleted"), "{err}");
        // Explicit --unsafe-grace overrides (quiescent tables, e.g. tests).
        assert!(check_grace_window(0, true, true).is_ok());
        // Dry runs stay unrestricted: they delete nothing.
        assert!(check_grace_window(0, false, false).is_ok());
        // A grace window of >= 1 hour needs no flag.
        assert!(check_grace_window(1, true, false).is_ok());
        assert!(check_grace_window(72, true, false).is_ok());
    }

    #[test]
    fn artifact_classification_is_conservative() {
        let d = "t/data/";
        let m = "t/metadata/";
        assert!(is_table_artifact("t/data/00000-x.parquet", d, m));
        assert!(is_table_artifact("t/data/sub/part.parquet", d, m));
        assert!(is_table_artifact("t/metadata/snap-1-x.avro", d, m));
        assert!(is_table_artifact("t/metadata/00003-x.metadata.json", d, m));
        assert!(is_table_artifact("t/metadata/s.stats", d, m));
        assert!(is_table_artifact("t/metadata/dv.puffin", d, m));
        // Unknown classes are NOT artifacts: skipped with WARN, never deleted.
        assert!(!is_table_artifact("t/data/notes.txt", d, m));
        assert!(!is_table_artifact("t/metadata/version-hint.text", d, m));
        assert!(!is_table_artifact("t/other/x.parquet", d, m));
        assert!(!is_table_artifact("t/x.parquet", d, m));
        assert!(!is_table_artifact("t/data/", d, m));
        assert!(!is_table_artifact("elsewhere/data/x.parquet", d, m));
    }
}
