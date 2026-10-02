//! Catalog connection and DataFusion session wiring.

use std::collections::HashMap;
use std::path::{Component, Path, PathBuf};
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
    let mut props = HashMap::from([
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

    // Iceberg REST catalog authentication (breadth). Inserted ONLY when the
    // operator set the corresponding flag/env, so the default open-Lakekeeper
    // path leaves `props` byte-identical to before (invariant I3). These are
    // the exact literal string keys iceberg-catalog-rest 0.9.1 reads
    // (catalog.rs): `token`, `credential`, `oauth2-server-uri`, `scope`. The
    // crate is not re-exporting them as constants, so they are inserted as
    // literals — the RestCatalog `load()` builder copies every prop except
    // uri/warehouse into the client config, where the OAuth2 client (already
    // vendored in iceberg-rust 0.9.1) consumes them.
    apply_catalog_auth(&mut props, opts);

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

/// Literal Iceberg REST prop keys the pinned `iceberg-catalog-rest 0.9.1`
/// client reads for OAuth2 (it exports no constants for them — see
/// catalog.rs `get_token_endpoint`/`token`/`credential`/`extra_oauth_params`).
const PROP_TOKEN: &str = "token";
const PROP_CREDENTIAL: &str = "credential";
const PROP_OAUTH2_URI: &str = "oauth2-server-uri";
const PROP_SCOPE: &str = "scope";

/// Insert the REST-catalog auth props into `props` for every auth flag the
/// operator actually set. Absent flags insert nothing, so the default path is
/// byte-identical (invariant I3). Factored out so it is unit-testable without
/// a live catalog. Also used by the write client (overwrite.rs) to keep the
/// static `token` in sync.
pub(crate) fn apply_catalog_auth(props: &mut HashMap<String, String>, opts: &CatalogOpts) {
    if let Some(token) = &opts.catalog_token {
        props.insert(PROP_TOKEN.to_string(), token.clone());
    }
    if let Some(credential) = &opts.catalog_credential {
        props.insert(PROP_CREDENTIAL.to_string(), credential.clone());
    }
    if let Some(uri) = &opts.catalog_oauth2_uri {
        props.insert(PROP_OAUTH2_URI.to_string(), uri.clone());
    }
    if let Some(scope) = &opts.catalog_scope {
        props.insert(PROP_SCOPE.to_string(), scope.clone());
    }
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

const MIB: usize = 1024 * 1024;

fn parse_system_memory(meminfo: &str) -> Option<usize> {
    for line in meminfo.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            let kb: usize = rest.split_whitespace().next()?.parse().ok()?;
            return kb.checked_mul(1024).filter(|n| *n > 0);
        }
    }
    None
}

/// Decode the escapes used for mountinfo paths without treating arbitrary
/// backslashes as filesystem traversal. Paths must remain absolute and clean.
fn mount_path(raw: &str) -> Option<PathBuf> {
    let mut out = Vec::with_capacity(raw.len());
    let mut bytes = raw.as_bytes().iter().copied();
    while let Some(byte) = bytes.next() {
        if byte == b'\\' {
            let escaped = [bytes.next()?, bytes.next()?, bytes.next()?];
            out.push(match &escaped {
                b"040" => b' ',
                b"011" => b'\t',
                b"012" => b'\n',
                b"134" => b'\\',
                _ => return None,
            });
        } else {
            out.push(byte);
        }
    }
    let path = PathBuf::from(String::from_utf8(out).ok()?);
    (path.is_absolute() && !path.components().any(|c| matches!(c, Component::ParentDir)))
        .then_some(path)
}

/// Resolve this process's memory cgroup through its mount, including delegated
/// roots and cgroup namespaces. Read every visible ancestor: a parent's hard
/// limit still constrains a child whose own limit is `max`.
fn cgroup_memory_limit(
    membership: &str,
    mountinfo: &str,
    read: impl Fn(&Path) -> Option<String>,
) -> Option<usize> {
    let groups: Vec<(&str, &str)> = membership
        .lines()
        .filter_map(|line| {
            let mut parts = line.splitn(3, ':');
            parts.next()?;
            Some((parts.next()?, parts.next()?))
        })
        .collect();
    let mut limit: Option<usize> = None;
    for line in mountinfo.lines() {
        let Some((before, after)) = line.split_once(" - ") else {
            continue;
        };
        let fields: Vec<&str> = before.split_whitespace().collect();
        let fs: Vec<&str> = after.split_whitespace().collect();
        if fields.len() < 6 || fs.len() < 3 {
            continue;
        }
        let (controller, filename) = match fs[0] {
            "cgroup2" => ("", "memory.max"),
            "cgroup" if fs[2].split(',').any(|c| c == "memory") => {
                ("memory", "memory.limit_in_bytes")
            }
            _ => continue,
        };
        let Some((_, raw_group)) = groups.iter().find(|(controllers, _)| {
            if controller.is_empty() {
                controllers.is_empty()
            } else {
                controllers.split(',').any(|c| c == controller)
            }
        }) else {
            continue;
        };
        let (Some(root), Some(mount), Some(group)) = (
            mount_path(fields[3]),
            mount_path(fields[4]),
            mount_path(raw_group),
        ) else {
            continue;
        };
        let relative = group
            .strip_prefix(&root)
            // A cgroup namespace reports membership relative to its root,
            // while an inherited mount may still name the host-side root.
            .or_else(|_| group.strip_prefix("/"))
            .ok()?;
        let leaf = mount.join(relative);
        for dir in leaf.ancestors().take_while(|p| p.starts_with(&mount)) {
            let Some(raw) = read(&dir.join(filename)) else {
                continue;
            };
            let Ok(bytes) = raw.trim().parse::<usize>() else {
                continue; // v2 `max`, malformed values or integer overflow
            };
            // v1 represents no limit by a page-aligned LONG_MAX. Ignore
            // that sentinel even when host RAM cannot be determined.
            if controller == "memory" && bytes as u128 >= (1u128 << 60) {
                continue;
            }
            limit = Some(limit.map_or(bytes, |previous| previous.min(bytes)));
        }
    }
    limit
}

fn effective_memory_bytes() -> Option<usize> {
    let host = std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|s| parse_system_memory(&s));
    let cgroup = std::fs::read_to_string("/proc/self/cgroup")
        .ok()
        .zip(std::fs::read_to_string("/proc/self/mountinfo").ok())
        .and_then(|(membership, mounts)| {
            cgroup_memory_limit(&membership, &mounts, |path| {
                std::fs::read_to_string(path).ok()
            })
        });
    match (host, cgroup) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    }
}

fn memory_pool_limit(raw: Option<&str>, available: Option<usize>) -> Result<Option<usize>> {
    if let Some(raw) = raw {
        let mb = raw
            .trim()
            .parse::<usize>()
            .context("ICEGRES_MEMORY_LIMIT_MB must be a nonnegative integer")?;
        return if mb == 0 {
            Ok(None)
        } else {
            Ok(Some(
                mb.checked_mul(MIB)
                    .context("ICEGRES_MEMORY_LIMIT_MB is too large")?,
            ))
        };
    }
    // Compute 70% without floating-point rounding or multiplication overflow.
    Ok(Some(match available {
        Some(bytes) => (bytes / 10) * 7 + (bytes % 10) * 7 / 10,
        None => 1024 * MIB,
    }))
}

/// Configure the DataFusion operator pool, not a process RSS limit. The
/// default is 70% of host RAM or a smaller visible cgroup hard limit, with
/// a 1 GiB fallback if neither can be read. An explicit zero opts out;
/// invalid/overflowing settings fail startup. Arrow batches, buffered writes,
/// peer mirrors and other allocations require their own budgets.
pub(crate) fn build_runtime_env() -> Result<Arc<RuntimeEnv>> {
    let raw = match std::env::var("ICEGRES_MEMORY_LIMIT_MB") {
        Ok(raw) => Some(raw),
        Err(std::env::VarError::NotPresent) => None,
        Err(e) => return Err(e).context("ICEGRES_MEMORY_LIMIT_MB is not valid Unicode"),
    };
    let limit_bytes = memory_pool_limit(raw.as_deref(), effective_memory_bytes())?;

    let mut builder =
        RuntimeEnvBuilder::new().with_disk_manager_builder(DiskManagerBuilder::default());
    if let Some(bytes) = limit_bytes {
        info!(
            memory_pool_mb = bytes / 1024 / 1024,
            "bounded DataFusion memory pool (FairSpillPool) with disk spill enabled"
        );
        builder = builder.with_memory_pool(Arc::new(FairSpillPool::new(bytes)));
    } else {
        warn!("ICEGRES_MEMORY_LIMIT_MB=0 disables the DataFusion operator memory limit");
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
        anyhow::ensure!(
            !matches!(schema_name.to_ascii_lowercase().as_str(), "pg_catalog" | "information_schema"),
            "Iceberg namespace {schema_name:?} uses a reserved system schema name; refusing to expose physical tables through the metadata authorization exemption"
        );
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn physical_catalog_cannot_register_reserved_system_schemas() {
        use iceberg::memory::MemoryCatalogBuilder;
        use iceberg::CatalogBuilder;

        for name in ["pg_catalog", "information_schema", "PG_CATALOG"] {
            let catalog = MemoryCatalogBuilder::default()
                .load(
                    "test",
                    HashMap::from([("warehouse".to_string(), "memory://warehouse".to_string())]),
                )
                .await
                .unwrap();
            catalog
                .create_namespace(&NamespaceIdent::new(name.to_string()), HashMap::new())
                .await
                .unwrap();
            let error = build_session_context(Arc::new(catalog))
                .await
                .err()
                .expect("physical system schema must be rejected");
            assert!(
                error.to_string().contains("reserved system schema"),
                "{error:#}"
            );
        }
    }

    #[test]
    fn memory_pool_zero_is_an_explicit_opt_out_and_invalid_values_fail() {
        assert_eq!(
            memory_pool_limit(Some("0"), Some(4096 * MIB)).unwrap(),
            None
        );
        assert_eq!(
            memory_pool_limit(Some(" 32 "), None).unwrap(),
            Some(32 * MIB)
        );
        for invalid in ["", "-1", "1.5", "unbounded"] {
            assert!(memory_pool_limit(Some(invalid), Some(4096 * MIB)).is_err());
        }
        let overflow = (usize::MAX / MIB + 1).to_string();
        assert!(memory_pool_limit(Some(&overflow), None).is_err());
        assert_eq!(memory_pool_limit(None, Some(1000)).unwrap(), Some(700));
        assert_eq!(memory_pool_limit(None, None).unwrap(), Some(1024 * MIB));
        assert_eq!(memory_pool_limit(None, Some(0)).unwrap(), Some(0));
        assert_eq!(
            memory_pool_limit(None, Some(usize::MAX)).unwrap(),
            Some(((usize::MAX as u128) * 7 / 10) as usize)
        );
    }

    #[test]
    fn host_memory_parser_rejects_overflow() {
        assert_eq!(parse_system_memory("MemTotal: 4096 kB\n"), Some(4 * MIB));
        assert_eq!(parse_system_memory("MemTotal: 0 kB\n"), None);
        assert_eq!(
            parse_system_memory(&format!("MemTotal: {} kB\n", usize::MAX)),
            None
        );
    }

    fn limits_from_fixture(
        membership: &str,
        mounts: &str,
        files: &[(&str, &str)],
    ) -> Option<usize> {
        cgroup_memory_limit(membership, mounts, |path| {
            files
                .iter()
                .find(|(name, _)| Path::new(name) == path)
                .map(|(_, value)| value.to_string())
        })
    }

    #[test]
    fn cgroup_v2_uses_nested_membership_and_lowest_ancestor_limit() {
        let mounts = "31 20 0:30 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n";
        assert_eq!(
            limits_from_fixture(
                "0::/workload/child\n",
                mounts,
                &[
                    ("/sys/fs/cgroup/workload/child/memory.max", "max\n"),
                    ("/sys/fs/cgroup/workload/memory.max", "4096\n"),
                    ("/sys/fs/cgroup/memory.max", "8192\n"),
                ],
            ),
            Some(4096)
        );
        assert_eq!(
            limits_from_fixture(
                "0::/workload/child\n",
                mounts,
                &[
                    ("/sys/fs/cgroup/workload/child/memory.max", "2048"),
                    ("/sys/fs/cgroup/workload/memory.max", "4096"),
                ],
            ),
            Some(2048)
        );
    }

    #[test]
    fn cgroup_v1_matches_controller_and_delegated_mount_root() {
        assert_eq!(
            limits_from_fixture(
                "2:cpu,cpuacct:/ignored\n7:memory:/docker/container/child\n",
                "31 20 0:30 / /cpu rw - cgroup cgroup rw,cpu,cpuacct\n\
                 32 20 0:31 /docker/container /memory rw - cgroup cgroup rw,memory\n",
                &[
                    ("/cpu/ignored/memory.limit_in_bytes", "1"),
                    ("/memory/child/memory.limit_in_bytes", "9223372036854771712"),
                    ("/memory/memory.limit_in_bytes", "8192"),
                    ("/memory.limit_in_bytes", "1"),
                ],
            ),
            Some(8192)
        );
    }

    #[test]
    fn cgroup_namespace_and_escaped_mounts_are_resolved() {
        let mounts = "31 20 0:30 /docker/container /mounted\\040cgroup rw - cgroup2 cgroup rw\n";
        assert_eq!(
            limits_from_fixture(
                "0::/child\n",
                mounts,
                &[("/mounted cgroup/child/memory.max", "1024")],
            ),
            Some(1024)
        );
        assert_eq!(
            limits_from_fixture("0::/\n", mounts, &[("/mounted cgroup/memory.max", "2048")],),
            Some(2048)
        );
    }

    #[test]
    fn cgroup_unlimited_malformed_and_parent_traversal_do_not_invent_limits() {
        let mounts = "31 20 0:30 / /sys/fs/cgroup rw - cgroup2 cgroup rw\n";
        assert_eq!(
            limits_from_fixture(
                "0::/child\n",
                mounts,
                &[
                    ("/sys/fs/cgroup/child/memory.max", "not-a-number"),
                    ("/sys/fs/cgroup/memory.max", "max"),
                ],
            ),
            None
        );
        assert_eq!(
            cgroup_memory_limit("0::/../../other\n", mounts, |_| {
                panic!("must not read a traversal path")
            }),
            None
        );
        assert!(mount_path("/bad\\000path").is_none());
        assert!(mount_path("relative").is_none());
    }

    /// A CatalogOpts with the historical (open-Lakekeeper) defaults and no
    /// auth flags set — the byte-identical default path.
    fn opts_no_auth() -> CatalogOpts {
        CatalogOpts {
            catalog_uri: "http://127.0.0.1:8181/catalog".to_string(),
            warehouse: "lakehouse".to_string(),
            s3_endpoint: "http://127.0.0.1:9000".to_string(),
            s3_access_key: "rustfsadmin".to_string(),
            s3_secret_key: "rustfssecret".to_string(),
            s3_region: "us-east-1".to_string(),
            catalog_token: None,
            catalog_credential: None,
            catalog_oauth2_uri: None,
            catalog_scope: None,
        }
    }

    #[test]
    fn auth_props_absent_when_no_flags_set() {
        let mut props = HashMap::new();
        apply_catalog_auth(&mut props, &opts_no_auth());
        assert!(props.is_empty(), "no auth flag set must insert no props");
        for key in [PROP_TOKEN, PROP_CREDENTIAL, PROP_OAUTH2_URI, PROP_SCOPE] {
            assert!(!props.contains_key(key), "{key} must be absent");
        }
    }

    #[test]
    fn token_prop_present_only_when_set() {
        let mut opts = opts_no_auth();
        opts.catalog_token = Some("pre-minted-bearer-xyz".to_string());
        let mut props = HashMap::new();
        apply_catalog_auth(&mut props, &opts);
        assert_eq!(
            props.get(PROP_TOKEN).map(String::as_str),
            Some("pre-minted-bearer-xyz")
        );
        // Only the token prop — nothing else leaks in.
        assert!(!props.contains_key(PROP_CREDENTIAL));
        assert!(!props.contains_key(PROP_OAUTH2_URI));
        assert!(!props.contains_key(PROP_SCOPE));
    }

    #[test]
    fn credential_flow_props_present_only_when_set() {
        let mut opts = opts_no_auth();
        opts.catalog_credential = Some("icegres:supersecret".to_string());
        opts.catalog_oauth2_uri = Some("http://127.0.0.1:8182/v1/oauth/tokens".to_string());
        opts.catalog_scope = Some("catalog".to_string());
        let mut props = HashMap::new();
        apply_catalog_auth(&mut props, &opts);
        assert_eq!(
            props.get(PROP_CREDENTIAL).map(String::as_str),
            Some("icegres:supersecret")
        );
        assert_eq!(
            props.get(PROP_OAUTH2_URI).map(String::as_str),
            Some("http://127.0.0.1:8182/v1/oauth/tokens")
        );
        assert_eq!(props.get(PROP_SCOPE).map(String::as_str), Some("catalog"));
        assert!(!props.contains_key(PROP_TOKEN));
    }

    #[test]
    fn default_props_are_byte_identical_with_and_without_the_helper() {
        // The full props map built by connect_catalog, minus the auth call,
        // must equal the same map WITH apply_catalog_auth when no flag is set.
        let opts = opts_no_auth();
        let base = || {
            HashMap::from([
                (REST_CATALOG_PROP_URI.to_string(), opts.catalog_uri.clone()),
                (
                    REST_CATALOG_PROP_WAREHOUSE.to_string(),
                    opts.warehouse.clone(),
                ),
                (S3_ENDPOINT.to_string(), opts.s3_endpoint.clone()),
                (S3_ACCESS_KEY_ID.to_string(), opts.s3_access_key.clone()),
                (S3_SECRET_ACCESS_KEY.to_string(), opts.s3_secret_key.clone()),
                (S3_REGION.to_string(), opts.s3_region.clone()),
                (S3_PATH_STYLE_ACCESS.to_string(), "true".to_string()),
                (S3_DISABLE_CONFIG_LOAD.to_string(), "true".to_string()),
                (S3_DISABLE_EC2_METADATA.to_string(), "true".to_string()),
            ])
        };
        let untouched = base();
        let mut with_auth = base();
        apply_catalog_auth(&mut with_auth, &opts);
        assert_eq!(untouched, with_auth);
    }

    #[test]
    fn debug_redacts_secret_fields_but_keeps_others() {
        let mut opts = opts_no_auth();
        opts.catalog_token = Some("SECRET_BEARER_TOKEN".to_string());
        opts.catalog_credential = Some("icegres:SECRET_CLIENT_PW".to_string());
        opts.catalog_oauth2_uri = Some("http://127.0.0.1:8182/v1/oauth/tokens".to_string());
        opts.catalog_scope = Some("catalog".to_string());
        let dbg = format!("{opts:?}");
        // Secrets must NOT appear in any log/debug rendering.
        assert!(
            !dbg.contains("SECRET_BEARER_TOKEN"),
            "bearer token leaked: {dbg}"
        );
        assert!(
            !dbg.contains("SECRET_CLIENT_PW"),
            "client secret leaked: {dbg}"
        );
        assert!(
            dbg.contains("<redacted>"),
            "redaction marker missing: {dbg}"
        );
        // Non-secret fields stay visible for debuggability.
        assert!(dbg.contains("http://127.0.0.1:8182/v1/oauth/tokens"));
        assert!(dbg.contains("catalog"));
        assert!(dbg.contains("lakehouse"));
    }

    #[test]
    fn debug_renders_none_for_unset_secrets() {
        let dbg = format!("{:?}", opts_no_auth());
        // Absence is still debuggable: unset secrets render as None, not <redacted>.
        assert!(dbg.contains("catalog_token: None"), "{dbg}");
        assert!(dbg.contains("catalog_credential: None"), "{dbg}");
    }
}
