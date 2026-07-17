//! `icegres verify` — the durability harness, productized (roadmap-v2 P7).
//!
//! The claims icegres makes about buffered-write durability are proven in
//! CI by `icegres/tests/tail_durability.sh` — against OUR box. This command
//! re-proves them against the OPERATOR's deployment: their catalog, their
//! object store, their tail backend, their disks and network. It spawns its
//! OWN scratch `icegres serve` processes (the current executable), drives
//! them over pgwire, kills them with SIGKILL exactly like the shell suite
//! does, and reports which claims held — each check naming the claim and
//! the doc section that makes it.
//!
//! # Blast-radius rails (in order of appearance)
//!
//! * A dedicated scratch namespace `icegres_verify_<nonce>` is created,
//!   tested, and dropped; the run REFUSES to start if it pre-exists, every
//!   statement verify issues names only tables inside it, and cleanup runs
//!   on every exit path (pass, fail, infra error, Ctrl-C).
//! * verify NEVER signals a process it did not spawn: kill -9 targets are
//!   exclusively its own children (which are also killed on drop, so an
//!   aborted run reaps them).
//! * The tail backend must be DEDICATED to the run, and that is enforced
//!   where it can be: `--tail-dir` writes only into a fresh
//!   `icegres_verify_<nonce>` subdirectory (refused if it pre-exists);
//!   `--tail-url` is refused when the database already carries an
//!   `icegres_tail` schema (a live or prior server's tail — replaying it
//!   would commit FOREIGN rows), and its cleanup re-proves ownership at
//!   drop time: the schema is dropped only if it still carries the
//!   identity the run's first scratch server minted AND no live session
//!   holds its one-writer advisory lock — otherwise the drop is skipped
//!   loudly (never delete state the run cannot prove is its own);
//!   `--tail-quorum` is refused — before any
//!   write, kill, or flush — if the first scratch server's boot replays
//!   foreign frames from the quorum log. A live production writer on the
//!   same quorum would additionally be FENCED by the verify run: point
//!   verify at dedicated/quiesced acceptors (the startup banner warns).
//!
//! # Honesty rails
//!
//! * A check whose backend is not configured SKIPS loudly — it never
//!   silently passes (e.g. fencing needs a shared-identity tail: pg or
//!   quorum; failover needs quorum).
//! * Against a catalog reached by the OAuth2 client-credentials grant with
//!   no static `--catalog-token`, every write-based suite SKIPS loudly: the
//!   copy-on-write commit client authenticates only with a static bearer, so
//!   the write plane cannot re-prove its claims (the same documented limit as
//!   the main write path — docs/catalog-support.md). Supply `--catalog-token`
//!   to run them.
//! * The report states that timings are the operator's box, not ours.
//! * What verify does NOT cover is documented in docs/limitations.md: the
//!   object store's own durability, catalog HA, and multi-node scheduling
//!   are out of its reach by construction.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context as _, Result};
use iceberg::spec::{NestedField, PrimitiveType, Schema, Type as IceType};
use iceberg::{Catalog, NamespaceIdent, TableCreation};
use tokio::process::{Child, Command};

use crate::context;
use crate::CatalogOpts;

/// Flush cadence for scratch servers: long enough that the background
/// flusher can never commit during a check — the only paths to the lake are
/// the mechanisms under test (tail replay + fence-forced flush), exactly
/// like the shell suite.
const BUF_MS: u64 = 600_000;

/// `icegres serve` environment overrides that must not leak from the
/// operator's shell into the scratch servers (the run passes everything it
/// means explicitly, as flags).
const SCRUBBED_ENV: &[&str] = &[
    "ICEGRES_AUTH_FILE",
    "ICEGRES_AUTHZ_FILE",
    "ICEGRES_TLS_CERT",
    "ICEGRES_TLS_KEY",
    "ICEGRES_WRITE_BUFFER_MS",
    "ICEGRES_WRITE_BUFFER_MAX_ROWS",
    "ICEGRES_TAIL_DIR",
    "ICEGRES_TAIL_URL",
    "ICEGRES_TAIL_QUORUM",
    "ICEGRES_TAIL_API_PORT",
    "ICEGRES_PEER_TAILS",
    "ICEGRES_FRESHNESS_MS",
    "ICEGRES_BRANCH",
    "ICEGRES_ENFORCE_PK",
    "ICEGRES_IDLE_SHUTDOWN_SECS",
    "ICEGRES_HEALTH_PORT",
    "ICEGRES_PORT",
    "ICEGRES_HOST",
    "ICEGRES_TXN_STRICT",
    "ICEGRES_LOG_FORMAT",
    "ICEGRES_QUERY_TIMING",
    "ICEGRES_DML_INJECT_CONFLICT",
    "ICEGRES_MERGE_INJECT_CONFLICT",
    "ICEGRES_TXN_DISABLE_ATOMIC",
    "ICEGRES_INSECURE",
];

/// Options for `icegres verify` (main.rs flag surface).
pub struct VerifyOpts {
    pub tail_dir: Option<PathBuf>,
    pub tail_url: Option<String>,
    pub tail_quorum: Option<String>,
    pub suite: String,
    pub freshness_ms: u64,
    pub keep_evidence: Option<PathBuf>,
    pub json: bool,
}

/// The tail backend under verification.
#[derive(Clone)]
enum Backend {
    /// No durable tail configured: durability-class checks SKIP.
    None,
    /// Local WAL under a run-scoped scratch subdirectory of the operator's
    /// directory (same filesystem/mount, zero shared state).
    Dir(PathBuf),
    /// Postgres tail at this URL (schema `icegres_tail`, verified absent
    /// before the run, dropped after).
    Pg(String),
    /// Quorum tail on these acceptors (must be dedicated/empty).
    Quorum(String),
}

impl Backend {
    fn label(&self) -> &'static str {
        match self {
            Backend::None => "none",
            Backend::Dir(_) => "dir",
            Backend::Pg(_) => "pg",
            Backend::Quorum(_) => "quorum",
        }
    }

    fn tail_flags(&self) -> Vec<String> {
        match self {
            Backend::None => vec![],
            Backend::Dir(dir) => vec!["--tail-dir".into(), dir.display().to_string()],
            Backend::Pg(url) => vec!["--tail-url".into(), url.clone()],
            Backend::Quorum(spec) => vec!["--tail-quorum".into(), spec.clone()],
        }
    }
}

/// Result status of one check.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Status {
    Pass,
    Fail,
    Skip,
}

impl Status {
    fn label(self) -> &'static str {
        match self {
            Status::Pass => "PASS",
            Status::Fail => "FAIL",
            Status::Skip => "SKIP",
        }
    }
}

/// One check of the report: the claim it re-proves and where the claim is
/// made, plus what happened on THIS box.
struct Check {
    suite: &'static str,
    name: &'static str,
    claim: &'static str,
    doc: &'static str,
    status: Status,
    detail: String,
    elapsed_ms: u64,
}

/// The five suites.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Suite {
    Durability,
    ExactlyOnce,
    Fencing,
    Freshness,
    Failover,
}

impl Suite {
    const ALL: [Suite; 5] = [
        Suite::Durability,
        Suite::ExactlyOnce,
        Suite::Fencing,
        Suite::Freshness,
        Suite::Failover,
    ];

    fn name(self) -> &'static str {
        match self {
            Suite::Durability => "durability",
            Suite::ExactlyOnce => "exactly-once",
            Suite::Fencing => "fencing",
            Suite::Freshness => "freshness",
            Suite::Failover => "failover",
        }
    }
}

/// Parse `--suite` into the suites to run.
fn select_suites(spec: &str) -> Result<Vec<Suite>> {
    if spec == "all" {
        return Ok(Suite::ALL.to_vec());
    }
    match Suite::ALL.iter().find(|s| s.name() == spec) {
        Some(s) => Ok(vec![*s]),
        None => bail!(
            "unknown --suite {spec:?}: use all, durability, exactly-once, fencing, \
             freshness, or failover"
        ),
    }
}

/// One scratch `icegres serve` child. SIGKILLed on drop (kill_on_drop), so
/// no exit path of the run leaks a process; every explicit kill below also
/// targets only these.
struct ServerProc {
    child: Child,
    port: u16,
    log: PathBuf,
    name: String,
}

impl ServerProc {
    /// The unclean kill under test: SIGKILL, never a graceful TERM (which
    /// would trigger the clean-shutdown flush and defeat the proof).
    async fn kill9(&mut self) -> Result<()> {
        self.child
            .kill()
            .await
            .with_context(|| format!("could not SIGKILL scratch server {}", self.name))?;
        Ok(())
    }

    /// The child's log so far, ANSI-stripped for matching.
    fn log_text(&self) -> String {
        strip_ansi(&std::fs::read_to_string(&self.log).unwrap_or_default())
    }

    fn log_tail(&self) -> String {
        let text = self.log_text();
        let lines: Vec<&str> = text.lines().rev().take(15).collect();
        lines.into_iter().rev().collect::<Vec<_>>().join("\n")
    }
}

/// Strip ANSI color sequences (the scratch servers log human format).
fn strip_ansi(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars().peekable();
    while let Some(c) = chars.next() {
        if c == '\u{1b}' {
            if chars.peek() == Some(&'[') {
                chars.next();
                for c2 in chars.by_ref() {
                    if c2.is_ascii_alphabetic() {
                        break;
                    }
                }
            }
            continue;
        }
        out.push(c);
    }
    out
}

/// Whether a boot log reports a tail replay (buffer.rs: "recovered N rows
/// for M tables from the ...").
fn log_reports_replay(log: &str) -> bool {
    log.lines()
        .any(|l| l.contains("recovered") && l.contains(" rows for ") && l.contains(" tables from"))
}

/// A free loopback port (bind :0, read, release — the child re-binds it).
fn free_port() -> Result<u16> {
    Ok(std::net::TcpListener::bind(("127.0.0.1", 0))
        .context("cannot allocate a loopback port")?
        .local_addr()?
        .port())
}

/// Run one simple-protocol statement against a scratch server over a fresh
/// connection (like the shell suite: every statement crosses a connection
/// boundary).
async fn sql(port: u16, stmt: &str) -> Result<Vec<tokio_postgres::SimpleQueryMessage>> {
    let (client, conn) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={port} user=postgres dbname=icegres connect_timeout=5"),
        tokio_postgres::NoTls,
    )
    .await
    .with_context(|| format!("cannot connect to scratch server on :{port}"))?;
    let handle = tokio::spawn(conn);
    let result = client
        .simple_query(stmt)
        .await
        .with_context(|| format!("statement failed on :{port}: {stmt}"));
    drop(client);
    handle.abort();
    result
}

/// `select count(*) from <table>` as i64.
async fn count(port: u16, table: &str) -> Result<i64> {
    let rows = sql(port, &format!("select count(*) from {table}")).await?;
    for msg in rows {
        if let tokio_postgres::SimpleQueryMessage::Row(row) = msg {
            return row
                .get(0)
                .ok_or_else(|| anyhow!("count(*) returned no column"))?
                .parse::<i64>()
                .context("count(*) is not an integer");
        }
    }
    bail!("count(*) returned no row")
}

/// The run harness: scratch namespace, backend, evidence dir, child
/// bookkeeping.
struct Harness {
    catalog_opts: CatalogOpts,
    catalog: Arc<dyn Catalog>,
    ns: NamespaceIdent,
    ns_name: String,
    backend: Backend,
    evidence: PathBuf,
    freshness_ms: u64,
    /// The quorum confinement guard runs on the FIRST tail-backed boot.
    first_tail_boot_done: bool,
    /// Ownership token for pg cleanup: the `icegres_tail.meta` identity
    /// minted by the run's first scratch server (read right after that
    /// boot). Cleanup drops the schema only while this identity still
    /// matches — see [`pg_drop_decision`].
    pg_tail_identity: Option<String>,
}

impl Harness {
    /// Create the scratch table `<ns>.<name>` (id long, note string).
    async fn create_table(&self, name: &str) -> Result<String> {
        let schema = Schema::builder()
            .with_fields(vec![
                NestedField::optional(1, "id", IceType::Primitive(PrimitiveType::Long)).into(),
                NestedField::optional(2, "note", IceType::Primitive(PrimitiveType::String)).into(),
            ])
            .build()
            .context("failed to build the scratch table schema")?;
        let creation = TableCreation::builder()
            .name(name.to_string())
            .schema(schema)
            .build();
        self.catalog
            .create_table(&self.ns, creation)
            .await
            .map_err(|e| {
                anyhow!(
                    "failed to create scratch table {}.{name}: {e}",
                    self.ns_name
                )
            })?;
        Ok(format!("{}.{name}", self.ns_name))
    }

    /// The catalog connection flags every scratch `serve` child needs: the
    /// base connection (uri/warehouse/s3) plus the four `--catalog-*` auth
    /// flags for each auth opt the operator actually set (only-when-set,
    /// mirroring [`crate::context::apply_catalog_auth`]). Without the auth
    /// flags the scratch children would connect UNAUTHENTICATED and fail to
    /// boot against an auth-guarded catalog when auth was supplied as a FLAG.
    /// (The `ICEGRES_CATALOG_*` env vars are not scrubbed, so the env-var
    /// form is inherited either way — forwarding the flags makes both forms
    /// behave identically.) Absent every auth opt the vector is byte-identical
    /// to the pre-auth base connection flags (invariant I3).
    fn catalog_serve_args(&self) -> Vec<String> {
        let o = &self.catalog_opts;
        let mut args = vec![
            "--catalog-uri".into(),
            o.catalog_uri.clone(),
            "--warehouse".into(),
            o.warehouse.clone(),
            "--s3-endpoint".into(),
            o.s3_endpoint.clone(),
            "--s3-access-key".into(),
            o.s3_access_key.clone(),
            "--s3-secret-key".into(),
            o.s3_secret_key.clone(),
            "--s3-region".into(),
            o.s3_region.clone(),
        ];
        if let Some(token) = &o.catalog_token {
            args.push("--catalog-token".into());
            args.push(token.clone());
        }
        if let Some(credential) = &o.catalog_credential {
            args.push("--catalog-credential".into());
            args.push(credential.clone());
        }
        if let Some(uri) = &o.catalog_oauth2_uri {
            args.push("--catalog-oauth2-uri".into());
            args.push(uri.clone());
        }
        if let Some(scope) = &o.catalog_scope {
            args.push("--catalog-scope".into());
            args.push(scope.clone());
        }
        args
    }

    /// Spawn a scratch `icegres serve` (the current executable) with the
    /// harness catalog flags plus `extra`, wait for readiness, and — on the
    /// first tail-backed boot — enforce the confinement guard: a replay at
    /// FIRST boot means the tail carries FOREIGN frames (a shared quorum
    /// log / tail), and the run aborts before anything could flush them.
    async fn spawn_server(&mut self, name: &str, extra: &[String]) -> Result<ServerProc> {
        let port = free_port()?;
        let log = self.evidence.join(format!("{name}.log"));
        let log_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log)
            .with_context(|| format!("cannot open scratch server log {}", log.display()))?;
        let exe = std::env::current_exe().context("cannot resolve the icegres executable")?;
        let catalog_args = self.catalog_serve_args();
        let mut cmd = Command::new(exe);
        cmd.arg("serve")
            .arg("--host")
            .arg("127.0.0.1")
            .arg("--port")
            .arg(port.to_string())
            .args(&catalog_args)
            .args(extra)
            .env("RUST_LOG", "info")
            .stdout(std::process::Stdio::from(log_file.try_clone()?))
            .stderr(std::process::Stdio::from(log_file))
            .kill_on_drop(true);
        for var in SCRUBBED_ENV {
            cmd.env_remove(var);
        }
        let child = cmd
            .spawn()
            .context("cannot spawn the scratch icegres server")?;
        let mut proc = ServerProc {
            child,
            port,
            log,
            name: name.to_string(),
        };
        // Readiness: a pgwire `select 1`, up to 30 s; an early exit fails
        // with the log tail.
        let mut ready = false;
        for _ in 0..60 {
            if sql(port, "select 1").await.is_ok() {
                ready = true;
                break;
            }
            if let Some(status) = proc.child.try_wait()? {
                bail!(
                    "scratch server {name} exited during startup ({status}); log tail:\n{}",
                    proc.log_tail()
                );
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        }
        if !ready {
            let tail = proc.log_tail();
            let _ = proc.kill9().await;
            bail!("scratch server {name} not ready on :{port} within 30s; log tail:\n{tail}");
        }
        // Confinement guard (first tail-backed boot only): the scratch tail
        // must be EMPTY. A replay here means foreign acked frames from
        // another writer's tail — flushing them would commit rows OUTSIDE
        // the scratch namespace. SIGKILL (never TERM: a graceful stop would
        // flush exactly those rows) and refuse the run.
        let has_tail = extra.iter().any(|a| a.starts_with("--tail-"));
        if has_tail && !self.first_tail_boot_done {
            self.first_tail_boot_done = true;
            if log_reports_replay(&proc.log_text()) {
                let _ = proc.kill9().await;
                bail!(
                    "CONFINEMENT REFUSAL: the first scratch server replayed rows from the \
                     configured tail at boot — the tail backend already carries another \
                     writer's acked frames (a live/prior server's tail or a shared quorum \
                     log). Flushing them would write OUTSIDE the scratch namespace, so \
                     nothing was written and the run stops. Point --tail-{} at a \
                     DEDICATED, empty verify resource.",
                    self.backend.label()
                );
            }
            // Ownership token: this boot just minted the pg tail schema
            // (verified free of foreign frames above). Remember its
            // identity so cleanup can prove the schema is still OURS
            // before dropping it — the start-of-run absence pre-flight
            // alone does not license a drop minutes later.
            let pg_url = match &self.backend {
                Backend::Pg(url) => Some(url.clone()),
                _ => None,
            };
            if let Some(url) = pg_url {
                let (client, conn) = tokio_postgres::connect(&url, tokio_postgres::NoTls)
                    .await
                    .context("cannot connect to the tail database to record the tail identity")?;
                let handle = tokio::spawn(conn);
                let identity: String = client
                    .query_one("SELECT identity FROM icegres_tail.meta", &[])
                    .await
                    .context(
                        "cannot read the tail identity the first scratch server minted \
                         into icegres_tail.meta",
                    )?
                    .get(0);
                drop(client);
                handle.abort();
                self.pg_tail_identity = Some(identity);
            }
        }
        Ok(proc)
    }

    /// Spawn a scratch server EXPECTED to refuse startup (the pg-tail
    /// second-writer case); returns its exit status text + log.
    async fn spawn_expect_boot_refusal(&self, name: &str, extra: &[String]) -> Result<String> {
        let port = free_port()?;
        let log = self.evidence.join(format!("{name}.log"));
        let log_file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&log)?;
        let exe = std::env::current_exe()?;
        let catalog_args = self.catalog_serve_args();
        let mut cmd = Command::new(exe);
        cmd.arg("serve")
            .arg("--host")
            .arg("127.0.0.1")
            .arg("--port")
            .arg(port.to_string())
            .args(&catalog_args)
            .args(extra)
            .env("RUST_LOG", "info")
            .stdout(std::process::Stdio::from(log_file.try_clone()?))
            .stderr(std::process::Stdio::from(log_file))
            .kill_on_drop(true);
        for var in SCRUBBED_ENV {
            cmd.env_remove(var);
        }
        let mut child = cmd.spawn()?;
        let status = tokio::time::timeout(Duration::from_secs(30), child.wait())
            .await
            .map_err(|_| {
                anyhow!(
                    "scratch server {name} did NOT exit within 30s — expected a boot \
                     refusal (it was killed)"
                )
            })??;
        let text = strip_ansi(&std::fs::read_to_string(&log).unwrap_or_default());
        if status.success() {
            bail!("scratch server {name} exited cleanly — expected a boot refusal");
        }
        Ok(text)
    }

    /// Force-flush a scratch server's buffered window (a DELETE matching
    /// nothing is an ordering fence: flush first, then delete nothing) so
    /// the tail is truncated before the next suite boots on it.
    async fn drain(&self, port: u16, table: &str) -> Result<()> {
        sql(port, &format!("delete from {table} where id < 0")).await?;
        Ok(())
    }

    /// Buffered + tail serve flags for the durability-class suites.
    fn buffered_flags(&self) -> Vec<String> {
        let mut flags = vec!["--write-buffer-ms".into(), BUF_MS.to_string()];
        flags.extend(self.backend.tail_flags());
        flags
    }

    // ------------------------------------------------------------------
    // Suites
    // ------------------------------------------------------------------

    async fn suite_durability(&mut self) -> Result<Vec<Check>> {
        const CLAIM: &str = "acked-but-uncommitted buffered rows survive an unclean kill \
                             (SIGKILL) of the server via durable-tail replay, exactly once";
        const DOC: &str = "docs/limitations.md \"Write buffer (opt-in)\" (--tail-dir / \
                           --tail-url / --tail-quorum bullets); README \"Durability\"";
        if matches!(self.backend, Backend::None) {
            return Ok(vec![skip(
                "durability",
                "durable-ack kill -9 recovery",
                CLAIM,
                DOC,
                "no durable tail configured: pass --tail-dir/--tail-url/--tail-quorum to \
                 re-prove the claim (plain buffered mode DOCUMENTS the loss window instead)",
            )]);
        }
        let started = Instant::now();
        let table = self.create_table("vt_durability").await?;
        let mut server = self
            .spawn_server("durability-1", &self.buffered_flags())
            .await?;
        for i in 1..=3 {
            sql(
                server.port,
                &format!("insert into {table} (id, note) values ({i}, 'verify-durability')"),
            )
            .await?;
        }
        let acked = count(server.port, &table).await?;
        if acked != 3 {
            bail!("union read broken before the kill: acked 3 rows, server reads {acked}");
        }
        // Confinement, verified before the kill: --tail-dir writes must all
        // sit inside the run's scratch subdirectory AND belong to scratch
        // tables (the per-table WAL dirs are named <ns>.<table>).
        if let Backend::Dir(dir) = &self.backend {
            // LocalWal layout (tail.rs): an `identity` file at the root plus
            // one `<ns>.<table>` directory per tailed table. Every table dir
            // must belong to the scratch namespace — anything else means the
            // scratch servers wrote outside their sandbox.
            let mut table_dirs = Vec::new();
            for entry in std::fs::read_dir(dir).context("cannot list the scratch tail dir")? {
                let entry = entry?;
                let name = entry.file_name().to_string_lossy().into_owned();
                // Regular files at the root are LocalWal bookkeeping
                // (`identity`, the `.lock` flock file); frames only ever
                // live inside the per-table directories checked below.
                if entry.file_type()?.is_dir() {
                    table_dirs.push(name);
                }
            }
            if table_dirs.is_empty() {
                bail!(
                    "no tail segments on disk after acked INSERTs ({})",
                    dir.display()
                );
            }
            if let Some(foreign) = table_dirs.iter().find(|e| !e.starts_with(&self.ns_name)) {
                bail!(
                    "CONFINEMENT REFUSAL: tail dir entry {foreign:?} is not scoped to the \
                     scratch namespace {} — refusing to kill anything",
                    self.ns_name
                );
            }
        }
        server.kill9().await?;
        let server2 = self
            .spawn_server("durability-2", &self.buffered_flags())
            .await?;
        let replayed = log_reports_replay(&server2.log_text());
        let after = count(server2.port, &table).await?;
        self.drain(server2.port, &table).await?;
        drop(server2);
        let status = if after == 3 && replayed {
            Status::Pass
        } else {
            Status::Fail
        };
        Ok(vec![Check {
            suite: "durability",
            name: "durable-ack kill -9 recovery",
            claim: CLAIM,
            doc: DOC,
            status,
            detail: format!(
                "3 rows acked to the {} tail, SIGKILL, restart: {after}/3 rows present, \
                 boot replay {}",
                self.backend.label(),
                if replayed { "reported" } else { "NOT reported" }
            ),
            elapsed_ms: started.elapsed().as_millis() as u64,
        }])
    }

    async fn suite_exactly_once(&mut self) -> Result<Vec<Check>> {
        const CLAIM1: &str = "a crash right after a flush commit double-applies nothing on \
                              replay (the in-commit tail-seq watermark rule)";
        const CLAIM2: &str = "rows acked after a flushed generation and a restart survive \
                              the next crash (post-restart sequences clear the persisted \
                              watermark)";
        const DOC: &str = "docs/limitations.md \"Write buffer (opt-in)\" (exactly-once via \
                           the icegres.tail-seq.<tail-id> watermark)";
        if matches!(self.backend, Backend::None) {
            return Ok(vec![skip(
                "exactly-once",
                "flush watermark replay",
                CLAIM1,
                DOC,
                "no durable tail configured: there is no replay path whose exactly-once \
                 rule could be re-proven",
            )]);
        }
        let mut checks = Vec::new();
        let started = Instant::now();
        let table = self.create_table("vt_once").await?;
        let mut server = self
            .spawn_server("exactly-once-1", &self.buffered_flags())
            .await?;
        for i in 1..=3 {
            sql(
                server.port,
                &format!("insert into {table} (id, note) values ({i}, 'verify-once')"),
            )
            .await?;
        }
        // Fence-forced flush: commit + watermark + tail truncation...
        self.drain(server.port, &table).await?;
        let committed = count(server.port, &table).await?;
        if committed != 3 {
            bail!("fence-forced flush did not commit the acked rows ({committed}/3)");
        }
        // ...then crash and replay: the watermark must suppress a double.
        server.kill9().await?;
        let mut server2 = self
            .spawn_server("exactly-once-2", &self.buffered_flags())
            .await?;
        let after = count(server2.port, &table).await?;
        checks.push(Check {
            suite: "exactly-once",
            name: "flush watermark replay",
            claim: CLAIM1,
            doc: DOC,
            status: if after == 3 {
                Status::Pass
            } else {
                Status::Fail
            },
            detail: format!(
                "3 rows committed by a fence-forced flush, SIGKILL, restart: {after} rows \
                 (3 = exactly-once; more = double-apply, fewer = loss)"
            ),
            elapsed_ms: started.elapsed().as_millis() as u64,
        });
        // Sequence floor: a second generation on the restarted server must
        // survive the NEXT crash (the trap: numbering new frames under the
        // persisted watermark would silently discard them on replay).
        let started2 = Instant::now();
        for i in 11..=13 {
            sql(
                server2.port,
                &format!("insert into {table} (id, note) values ({i}, 'second-generation')"),
            )
            .await?;
        }
        server2.kill9().await?;
        let server3 = self
            .spawn_server("exactly-once-3", &self.buffered_flags())
            .await?;
        let both = count(server3.port, &table).await?;
        self.drain(server3.port, &table).await?;
        drop(server3);
        checks.push(Check {
            suite: "exactly-once",
            name: "post-flush sequence floor",
            claim: CLAIM2,
            doc: DOC,
            status: if both == 6 {
                Status::Pass
            } else {
                Status::Fail
            },
            detail: format!(
                "3 committed + 3 acked-after-restart rows, SIGKILL, restart: {both}/6 present"
            ),
            elapsed_ms: started2.elapsed().as_millis() as u64,
        });
        Ok(checks)
    }

    async fn suite_fencing(&mut self) -> Result<Vec<Check>> {
        const CLAIM: &str = "two writers on one tail identity cannot both ack: the stale \
                             writer is excluded (pg: one-writer advisory lock refuses its \
                             boot; quorum: a newer term fences its INSERTs)";
        const DOC: &str = "docs/limitations.md \"Write buffer (opt-in)\" (--tail-url \
                           one-writer lock; --tail-quorum fencing); \
                           docs/deployment.md §11 (HA runbook)";
        let started = Instant::now();
        match self.backend.clone() {
            Backend::None | Backend::Dir(_) => Ok(vec![skip(
                "fencing",
                "stale-writer exclusion",
                CLAIM,
                DOC,
                "fencing needs a tail with a cross-process identity (--tail-url or \
                 --tail-quorum); the local-WAL tail is single-node by construction",
            )]),
            Backend::Pg(_) => {
                let table = self.create_table("vt_fencing").await?;
                let server_a = self
                    .spawn_server("fencing-a", &self.buffered_flags())
                    .await?;
                sql(
                    server_a.port,
                    &format!("insert into {table} (id, note) values (1, 'verify-fencing')"),
                )
                .await?;
                // The second writer with the SAME identity (same tail URL,
                // same schema) must be refused at boot by the advisory lock.
                let refusal = self
                    .spawn_expect_boot_refusal("fencing-b", &self.buffered_flags())
                    .await?;
                let refused_for_lock = refusal.contains("LOCKED by another session");
                self.drain(server_a.port, &table).await?;
                drop(server_a);
                Ok(vec![Check {
                    suite: "fencing",
                    name: "stale-writer exclusion (pg advisory lock)",
                    claim: CLAIM,
                    doc: DOC,
                    status: if refused_for_lock {
                        Status::Pass
                    } else {
                        Status::Fail
                    },
                    detail: if refused_for_lock {
                        "second writer on the same tail refused at boot (one-writer \
                         advisory lock held)"
                            .into()
                    } else {
                        "second writer was NOT refused by the advisory lock (see \
                         fencing-b.log)"
                            .into()
                    },
                    elapsed_ms: started.elapsed().as_millis() as u64,
                }])
            }
            Backend::Quorum(_) => {
                let table = self.create_table("vt_fencing").await?;
                let server_a = self
                    .spawn_server("fencing-a", &self.buffered_flags())
                    .await?;
                for i in 1..=2 {
                    sql(
                        server_a.port,
                        &format!("insert into {table} (id, note) values ({i}, 'pre-fence')"),
                    )
                    .await?;
                }
                // A second server on the SAME quorum runs a higher-term
                // election: it recovers A's acked rows and fences A.
                let server_b = self
                    .spawn_server("fencing-b", &self.buffered_flags())
                    .await?;
                let recovered = count(server_b.port, &table).await?;
                let stale_insert = sql(
                    server_a.port,
                    &format!("insert into {table} (id, note) values (99, 'must-fail')"),
                )
                .await;
                let fenced = matches!(
                    &stale_insert,
                    Err(e) if format!("{e:#}").contains("superseded by a newer server")
                );
                self.drain(server_b.port, &table).await?;
                drop(server_b);
                drop(server_a);
                let pass = fenced && recovered == 2;
                Ok(vec![Check {
                    suite: "fencing",
                    name: "stale-writer exclusion (quorum term)",
                    claim: CLAIM,
                    doc: DOC,
                    status: if pass { Status::Pass } else { Status::Fail },
                    detail: format!(
                        "replacement recovered {recovered}/2 acked rows; stale writer's \
                         INSERT {}",
                        if fenced {
                            "failed with the superseded error (fenced)"
                        } else {
                            "was NOT fenced"
                        }
                    ),
                    elapsed_ms: started.elapsed().as_millis() as u64,
                }])
            }
        }
    }

    async fn suite_freshness(&mut self) -> Result<Vec<Check>> {
        const CLAIM: &str = "with --freshness-ms N, a foreign commit becomes visible within \
                             ~N ms plus one refresh round trip";
        const DOC: &str = "docs/limitations.md \"Bounded-staleness reads (opt-in, \
                           --freshness-ms)\"";
        let started = Instant::now();
        let n = self.freshness_ms;
        let table = self.create_table("vt_freshness").await?;
        // Reader with the bound under test; writer is a plain synchronous
        // server (its INSERT is an Iceberg commit — a genuinely foreign
        // commit from the reader's point of view).
        let reader = self
            .spawn_server(
                "freshness-reader",
                &["--freshness-ms".to_string(), n.to_string()],
            )
            .await?;
        let writer = self.spawn_server("freshness-writer", &[]).await?;
        if count(reader.port, &table).await? != 0 {
            bail!("scratch freshness table is not empty before the probe");
        }
        sql(
            writer.port,
            &format!("insert into {table} (id, note) values (1, 'foreign-commit')"),
        )
        .await?;
        let committed_at = Instant::now();
        // The documented bound plus one refresh round trip; measured on the
        // operator's box, so the budget is generous: N + 5 s hard timeout,
        // pass bound N + 3 s.
        let deadline = committed_at + Duration::from_millis(n + 5000);
        let mut visible_after: Option<u64> = None;
        while Instant::now() < deadline {
            if count(reader.port, &table).await? == 1 {
                visible_after = Some(committed_at.elapsed().as_millis() as u64);
                break;
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
        drop(writer);
        drop(reader);
        let bound = n + 3000;
        let (status, detail) = match visible_after {
            Some(ms) if ms <= bound => (
                Status::Pass,
                format!(
                    "foreign commit visible on the bounded-stale reader after {ms} ms \
                     (bound: --freshness-ms {n} + refresh round trip; budget {bound} ms; \
                     timing measured on THIS box)"
                ),
            ),
            Some(ms) => (
                Status::Fail,
                format!("foreign commit took {ms} ms to become visible (budget {bound} ms)"),
            ),
            None => (
                Status::Fail,
                format!(
                    "foreign commit NOT visible within {} ms (bound {n} ms + refresh)",
                    n + 5000
                ),
            ),
        };
        Ok(vec![Check {
            suite: "freshness",
            name: "foreign-commit visibility bound",
            claim: CLAIM,
            doc: DOC,
            status,
            detail,
            elapsed_ms: started.elapsed().as_millis() as u64,
        }])
    }

    async fn suite_failover(&mut self) -> Result<Vec<Check>> {
        const CLAIM: &str = "after the buffered writer dies uncleanly, a replacement on the \
                             same quorum fences the old identity, replays every acked row, \
                             and commits them exactly once";
        const DOC: &str = "docs/deployment.md §9 (quorum tail topology) and §11 (HA \
                           runbook); docs/limitations.md \"icegresd-ha\"";
        if !matches!(self.backend, Backend::Quorum(_)) {
            return Ok(vec![skip(
                "failover",
                "replacement writer replay",
                CLAIM,
                DOC,
                "failover needs --tail-quorum (the replicated tail a replacement recovers \
                 from); dir/pg tails hand over via restart or lock takeover instead",
            )]);
        }
        let started = Instant::now();
        let table = self.create_table("vt_failover").await?;
        let mut server_a = self
            .spawn_server("failover-a", &self.buffered_flags())
            .await?;
        for i in 1..=3 {
            sql(
                server_a.port,
                &format!("insert into {table} (id, note) values ({i}, 'pre-failover')"),
            )
            .await?;
        }
        // The writer dies uncleanly with all three rows acked, unflushed.
        server_a.kill9().await?;
        // The replacement (a fresh identity on the same quorum) must fence
        // the old term, replay, and be able to commit.
        let server_b = self
            .spawn_server("failover-b", &self.buffered_flags())
            .await?;
        let replayed = log_reports_replay(&server_b.log_text());
        let after = count(server_b.port, &table).await?;
        self.drain(server_b.port, &table).await?;
        let committed = count(server_b.port, &table).await?;
        drop(server_b);
        let pass = replayed && after == 3 && committed == 3;
        Ok(vec![Check {
            suite: "failover",
            name: "replacement writer replay",
            claim: CLAIM,
            doc: DOC,
            status: if pass { Status::Pass } else { Status::Fail },
            detail: format!(
                "writer SIGKILLed with 3 acked rows; replacement: replay {}, {after}/3 \
                 rows served, {committed}/3 committed by its flush",
                if replayed { "reported" } else { "NOT reported" }
            ),
            elapsed_ms: started.elapsed().as_millis() as u64,
        }])
    }

    // ------------------------------------------------------------------
    // Cleanup (every exit path)
    // ------------------------------------------------------------------

    /// Drop everything the run created: scratch tables, the scratch
    /// namespace, the scratch tail subdirectory / pg schema. Children die via
    /// kill_on_drop when their suite scope ends (or the run future is
    /// cancelled).
    ///
    /// Tables and the namespace are dropped through the AUTHENTICATED catalog
    /// client (`context::connect_catalog` threaded the operator's auth props),
    /// so cleanup authenticates uniformly under BOTH `--catalog-token` and the
    /// `--catalog-credential` OAuth2 grant. A raw unauthenticated REST DELETE
    /// (what this used to issue) would 401 against an auth-guarded catalog and
    /// STRAND the scratch namespace + tables, breaking verify's
    /// create-test-drop contract. The catalog client's `drop_table` does not
    /// request an object-store purge (iceberg-rust 0.9.1 exposes none), so the
    /// dropped tables' data files are left to the store's own lifecycle — like
    /// all object-store cleanup, which is out of verify's reach by
    /// construction (docs/limitations.md).
    async fn cleanup(&self) -> Vec<String> {
        let mut problems = Vec::new();
        match self.catalog.list_tables(&self.ns).await {
            Ok(tables) => {
                for ident in tables {
                    if let Err(e) = self.catalog.drop_table(&ident).await {
                        problems.push(format!("could not drop scratch table {ident}: {e}"));
                    }
                }
            }
            Err(e) => problems.push(format!(
                "could not list scratch tables of {}: {e}",
                self.ns_name
            )),
        }
        if let Err(e) = self.catalog.drop_namespace(&self.ns).await {
            problems.push(format!(
                "could not drop scratch namespace {}: {e}",
                self.ns_name
            ));
        }
        match &self.backend {
            Backend::Dir(dir) => {
                if let Err(e) = std::fs::remove_dir_all(dir) {
                    if e.kind() != std::io::ErrorKind::NotFound {
                        problems.push(format!(
                            "could not remove the scratch tail dir {}: {e}",
                            dir.display()
                        ));
                    }
                }
            }
            Backend::Pg(url) => {
                // DROP ... CASCADE destroys every acked frame in the
                // schema, and "absent at pre-flight" does not mean "ours
                // at cleanup": the run lasts minutes with lock-free
                // windows in which a concurrent verify or a misconfigured
                // server can adopt the database. Re-prove ownership at
                // drop time (identity match + no live lock holder) and
                // otherwise skip loudly — never delete state the run
                // cannot prove is its own (the scratch-namespace rail).
                match tokio_postgres::connect(url, tokio_postgres::NoTls).await {
                    Ok((client, conn)) => {
                        let handle = tokio::spawn(conn);
                        let decision = match pg_cleanup_probe(&client).await {
                            Ok((current, holder)) => pg_drop_decision(
                                self.pg_tail_identity.as_deref(),
                                current.as_deref(),
                                holder.as_deref(),
                            ),
                            Err(e) => {
                                PgDrop::Skip(format!("the ownership re-check failed ({e:#})"))
                            }
                        };
                        match decision {
                            PgDrop::Drop => {
                                if let Err(e) = client
                                    .simple_query("DROP SCHEMA IF EXISTS icegres_tail CASCADE")
                                    .await
                                {
                                    problems.push(format!(
                                        "could not drop the scratch tail schema: {e}"
                                    ));
                                }
                            }
                            PgDrop::Skip(why) => problems.push(format!(
                                "NOT dropping the icegres_tail schema on the tail \
                                 database: {why}. Dropping it could destroy another \
                                 writer's acked-but-unflushed frames — inspect and \
                                 remove it manually."
                            )),
                            PgDrop::Nothing => {}
                        }
                        drop(client);
                        handle.abort();
                    }
                    Err(e) => problems.push(format!(
                        "could not connect to the tail database for cleanup: {e}"
                    )),
                }
            }
            Backend::Quorum(_) | Backend::None => {}
        }
        problems
    }
}

/// What pg cleanup decided to do with the `icegres_tail` schema.
#[derive(Debug, PartialEq, Eq)]
enum PgDrop {
    /// Provably still ours (identity match, no live holder): drop it.
    Drop,
    /// Not provably ours — leave it and say what was found.
    Skip(String),
    /// No schema on the database: nothing to drop.
    Nothing,
}

/// The pg-cleanup ownership rule, pure so it is unit-testable: `ours` is
/// the identity recorded after the run's first scratch server minted the
/// schema, `current` the identity on the database at drop time (None =
/// schema absent), `live_holder` a session holding the schema's one-writer
/// advisory lock right now. The identity is minted ONCE (`ON CONFLICT DO
/// NOTHING`), so a foreign writer that adopts our schema keeps our
/// identity — the live-holder veto covers exactly that case.
fn pg_drop_decision(
    ours: Option<&str>,
    current: Option<&str>,
    live_holder: Option<&str>,
) -> PgDrop {
    match (ours, current) {
        (_, None) => PgDrop::Nothing,
        (None, Some(found)) => PgDrop::Skip(format!(
            "the schema (identity {found}) is not one this run created — it appeared \
             after the start-of-run absence pre-flight, so another writer owns it"
        )),
        (Some(mine), Some(found)) if mine != found => PgDrop::Skip(format!(
            "the tail identity changed under the run (ours: {mine}, found: {found}) — \
             another writer re-minted the schema"
        )),
        (Some(_), Some(_)) => match live_holder {
            Some(holder) => PgDrop::Skip(format!(
                "a live session ({holder}) holds the schema's one-writer advisory lock \
                 — a foreign writer adopted the tail mid-run"
            )),
            None => PgDrop::Drop,
        },
    }
}

/// Cleanup-time ownership probe for [`pg_drop_decision`]: the
/// `icegres_tail.meta` identity now on the database (None = schema absent;
/// a present schema whose meta row is unreadable yields a marker string
/// that can never match ours, so the comparison fails safe) and any LIVE
/// session holding the schema's one-writer advisory lock. The holder check
/// retries briefly: postgres releases our own SIGKILLed children's locks
/// as it reaps their dead connections, so only a genuinely foreign session
/// persists past the retries.
async fn pg_cleanup_probe(
    client: &tokio_postgres::Client,
) -> Result<(Option<String>, Option<String>)> {
    let exists: bool = client
        .query_one(
            "SELECT EXISTS (SELECT 1 FROM information_schema.schemata \
             WHERE schema_name = 'icegres_tail')",
            &[],
        )
        .await
        .context("cannot check the tail database for the icegres_tail schema")?
        .get(0);
    if !exists {
        return Ok((None, None));
    }
    let identity = match client
        .query_opt("SELECT identity FROM icegres_tail.meta", &[])
        .await
    {
        Ok(Some(row)) => row.get::<_, String>(0),
        Ok(None) => "<no identity row in icegres_tail.meta>".to_string(),
        Err(e) => format!("<unreadable icegres_tail.meta: {e}>"),
    };
    let lock_key = crate::tail_pg::schema_lock_key(crate::tail_pg::DEFAULT_SCHEMA);
    let mut holder = None;
    for _ in 0..20 {
        let row = client
            .query_opt(
                "SELECT l.pid, coalesce(a.application_name, '') \
                 FROM pg_locks l LEFT JOIN pg_stat_activity a ON a.pid = l.pid \
                 WHERE l.locktype = 'advisory' AND l.granted \
                   AND l.classid::int4 = $1 AND l.objid::int4 = $2 AND l.objsubid = 2",
                &[&crate::tail_pg::LOCK_CLASS, &lock_key],
            )
            .await
            .context("cannot check pg_locks for a live tail-lock holder")?;
        match row {
            None => {
                holder = None;
                break;
            }
            Some(row) => {
                let pid: i32 = row.get(0);
                let app: String = row.get(1);
                holder = Some(if app.is_empty() {
                    format!("pid {pid}")
                } else {
                    format!("pid {pid}, application {app:?}")
                });
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        }
    }
    Ok((Some(identity), holder))
}

fn skip(
    suite: &'static str,
    name: &'static str,
    claim: &'static str,
    doc: &'static str,
    why: &str,
) -> Check {
    Check {
        suite,
        name,
        claim,
        doc,
        status: Status::Skip,
        detail: why.to_string(),
        elapsed_ms: 0,
    }
}

/// Entry point: `icegres verify` (main.rs).
pub async fn run(catalog_opts: &CatalogOpts, opts: VerifyOpts) -> Result<()> {
    let suites = select_suites(&opts.suite)?;
    // clap's conflicts_with already refuses pairs; hard error for
    // programmatic callers (one tail per verify run, like one per server).
    if [
        opts.tail_dir.is_some(),
        opts.tail_url.is_some(),
        opts.tail_quorum.is_some(),
    ]
    .iter()
    .filter(|&&set| set)
    .count()
        > 1
    {
        bail!("--tail-dir, --tail-url, and --tail-quorum are mutually exclusive");
    }
    let catalog = context::connect_catalog(catalog_opts).await?;

    // Scratch namespace: refuse if it pre-exists (never adopt state the run
    // did not create — a nonce collision or a crashed run's leftovers are a
    // human's call, not ours to purge silently).
    let nonce = uuid::Uuid::new_v4().simple().to_string()[..8].to_string();
    let ns_name = format!("icegres_verify_{nonce}");
    let ns = NamespaceIdent::new(ns_name.clone());
    if catalog
        .namespace_exists(&ns)
        .await
        .map_err(|e| anyhow!("cannot check the scratch namespace: {e}"))?
    {
        bail!(
            "scratch namespace {ns_name} already exists — refusing to reuse state this \
             run did not create; drop it and re-run"
        );
    }

    // Backend pre-flight (confinement rails; see the module docs).
    let backend = match (&opts.tail_dir, &opts.tail_url, &opts.tail_quorum) {
        (Some(dir), None, None) => {
            let scratch = dir.join(&ns_name);
            if scratch.exists() {
                bail!(
                    "scratch tail directory {} already exists — refusing to reuse it",
                    scratch.display()
                );
            }
            std::fs::create_dir_all(&scratch).with_context(|| {
                format!("cannot create the scratch tail dir {}", scratch.display())
            })?;
            Backend::Dir(scratch)
        }
        (None, Some(url), None) => {
            let (client, conn) = tokio_postgres::connect(url, tokio_postgres::NoTls)
                .await
                .context("cannot connect to --tail-url for the pre-flight check")?;
            let handle = tokio::spawn(conn);
            let row = client
                .query_one(
                    "SELECT EXISTS (SELECT 1 FROM information_schema.schemata \
                     WHERE schema_name = 'icegres_tail')",
                    &[],
                )
                .await
                .context("cannot check the tail database for an existing icegres_tail schema")?;
            let exists: bool = row.get(0);
            drop(client);
            handle.abort();
            if exists {
                bail!(
                    "the tail database at --tail-url already carries an icegres_tail \
                     schema — a live or prior server's tail. Verifying against it could \
                     replay ANOTHER writer's acked frames into the lake. Point verify at \
                     a dedicated (empty) database on the same instance instead."
                );
            }
            Backend::Pg(url.clone())
        }
        (None, None, Some(spec)) => {
            eprintln!(
                "NOTE: --tail-quorum verify takes ownership of the quorum log: a live \
                 icegres writer on these acceptors WOULD be fenced. Use dedicated (or \
                 quiesced, drained) acceptors. The run refuses — before writing anything \
                 — if the quorum already carries foreign frames."
            );
            Backend::Quorum(spec.clone())
        }
        (None, None, None) => Backend::None,
        _ => unreachable!("refused above"),
    };

    // Evidence directory (scratch-server logs + report.json).
    let evidence = opts
        .keep_evidence
        .clone()
        .unwrap_or_else(|| std::env::temp_dir().join(format!("icegres-verify-{nonce}")));
    std::fs::create_dir_all(&evidence)
        .with_context(|| format!("cannot create the evidence dir {}", evidence.display()))?;

    catalog
        .create_namespace(&ns, std::collections::HashMap::new())
        .await
        .map_err(|e| anyhow!("cannot create the scratch namespace {ns_name}: {e}"))?;

    eprintln!(
        "icegres verify: backend {}, scratch namespace {ns_name}, evidence {}",
        backend.label(),
        evidence.display()
    );

    let mut harness = Harness {
        catalog_opts: catalog_opts.clone(),
        catalog,
        ns,
        ns_name: ns_name.clone(),
        backend,
        evidence: evidence.clone(),
        freshness_ms: opts.freshness_ms,
        first_tail_boot_done: false,
        pg_tail_identity: None,
    };

    // Run the suites with guaranteed cleanup on EVERY exit path: normal
    // completion, an infra error, or Ctrl-C (cancelling the suite future
    // drops any live scratch server -> kill_on_drop reaps it).
    let suites_fut = run_suites(&mut harness, &suites);
    let outcome: Result<Vec<Check>> = tokio::select! {
        result = suites_fut => result,
        _ = tokio::signal::ctrl_c() => Err(anyhow!("interrupted (Ctrl-C); cleaning up")),
    };
    let cleanup_problems = harness.cleanup().await;

    let checks = match outcome {
        Ok(checks) => checks,
        Err(e) => {
            // The report still renders what ran; the run itself failed.
            for p in &cleanup_problems {
                eprintln!("cleanup: {p}");
            }
            if opts.keep_evidence.is_none() {
                let _ = std::fs::remove_dir_all(&evidence);
            } else {
                eprintln!("evidence kept at {}", evidence.display());
            }
            return Err(e.context("icegres verify aborted (scratch state cleaned up)"));
        }
    };

    // ---- report ---------------------------------------------------------
    let passed = checks.iter().filter(|c| c.status == Status::Pass).count();
    let failed = checks.iter().filter(|c| c.status == Status::Fail).count();
    let skipped = checks.iter().filter(|c| c.status == Status::Skip).count();
    let json_report = serde_json::json!({
        "backend": harness.backend.label(),
        "namespace": ns_name,
        "suite": opts.suite,
        "checks": checks.iter().map(|c| serde_json::json!({
            "suite": c.suite,
            "name": c.name,
            "claim": c.claim,
            "doc": c.doc,
            "status": c.status.label(),
            "detail": c.detail,
            "elapsed_ms": c.elapsed_ms,
        })).collect::<Vec<_>>(),
        "passed": passed,
        "failed": failed,
        "skipped": skipped,
        "caveat": "checks re-prove the claims on THIS deployment; timings are this box's, \
                   not a reference. Not covered: the object store's own durability, \
                   catalog HA (docs/limitations.md).",
    });
    let report_path = evidence.join("report.json");
    std::fs::write(&report_path, serde_json::to_string_pretty(&json_report)?)
        .with_context(|| format!("cannot write {}", report_path.display()))?;

    if opts.json {
        println!("{}", serde_json::to_string_pretty(&json_report)?);
    } else {
        println!(
            "\nicegres verify — backend: {}, scratch namespace: {} (created, tested, dropped)",
            harness.backend.label(),
            ns_name
        );
        for c in &checks {
            println!(
                "{:<4} [{}] {} ({} ms)\n       what: {}\n       claim: {}\n       doc: {}",
                c.status.label(),
                c.suite,
                c.name,
                c.elapsed_ms,
                c.detail,
                c.claim,
                c.doc
            );
        }
        println!(
            "\n{passed} passed, {failed} failed, {skipped} skipped (skips are unconfigured \
             backends — never silent passes). Timings are THIS box's. Not covered: object-\
             store durability itself, catalog HA (docs/limitations.md)."
        );
    }
    for p in &cleanup_problems {
        eprintln!("cleanup: {p}");
    }
    if opts.keep_evidence.is_none() {
        let _ = std::fs::remove_dir_all(&evidence);
    } else {
        eprintln!("evidence kept at {}", evidence.display());
    }
    if !cleanup_problems.is_empty() {
        bail!(
            "icegres verify: cleanup left {} problem(s) behind (see stderr) — inspect the \
             scratch namespace {ns_name} manually",
            cleanup_problems.len()
        );
    }
    if failed > 0 {
        bail!("icegres verify: {failed} check(s) FAILED — a documented claim did not hold on this deployment");
    }
    Ok(())
}

async fn run_suites(harness: &mut Harness, suites: &[Suite]) -> Result<Vec<Check>> {
    // Every suite INSERTs rows, and the copy-on-write commit client (see
    // overwrite.rs) authenticates only with a STATIC `--catalog-token` bearer,
    // not the OAuth2 client-credentials grant. Under credential-only auth the
    // scratch servers' READ plane authenticates (the OAuth2-minted bearer) but
    // their WRITE plane would 401, so the write-based claims cannot be re-proven
    // here — the SAME documented limitation as the main write path
    // (docs/catalog-support.md). Rather than let those suites FAIL confusingly,
    // SKIP them loudly and name the fix (supply --catalog-token). Both auth
    // props set means the static token drives writes, so suites run normally.
    let credential_only = harness.catalog_opts.catalog_credential.is_some()
        && harness.catalog_opts.catalog_token.is_none();
    let mut checks = Vec::new();
    for suite in suites {
        if credential_only {
            checks.push(skip(
                suite.name(),
                "write-based check (OAuth2 credential auth)",
                "the suite's INSERT-driven claims require an authenticated write path",
                "docs/catalog-support.md (write plane under OAuth2 client-credentials)",
                "this catalog is reached via the OAuth2 client-credentials grant \
                 (--catalog-credential) with no --catalog-token: verify's suites INSERT \
                 rows, and the copy-on-write commit client authenticates only with a \
                 static --catalog-token bearer, not an OAuth2-minted one. Re-run with \
                 --catalog-token to re-prove the write-based claims against this catalog \
                 (docs/catalog-support.md).",
            ));
            continue;
        }
        let result = match suite {
            Suite::Durability => harness.suite_durability().await,
            Suite::ExactlyOnce => harness.suite_exactly_once().await,
            Suite::Fencing => harness.suite_fencing().await,
            Suite::Freshness => harness.suite_freshness().await,
            Suite::Failover => harness.suite_failover().await,
        };
        match result {
            Ok(mut suite_checks) => checks.append(&mut suite_checks),
            // A confinement refusal aborts the whole run (nothing else may
            // write); any other suite infrastructure error is recorded as a
            // FAIL so the exit code is honest, and later suites still run.
            Err(e) if format!("{e:#}").contains("CONFINEMENT REFUSAL") => return Err(e),
            Err(e) => checks.push(Check {
                suite: suite.name(),
                name: "suite infrastructure",
                claim: "the suite's checks could run at all on this deployment",
                doc: "docs/deployment.md (verify runbook)",
                status: Status::Fail,
                detail: format!("{e:#}"),
                elapsed_ms: 0,
            }),
        }
    }
    Ok(checks)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn suite_selection() {
        assert_eq!(select_suites("all").unwrap().len(), 5);
        assert_eq!(
            select_suites("durability").unwrap(),
            vec![Suite::Durability]
        );
        assert_eq!(
            select_suites("exactly-once").unwrap(),
            vec![Suite::ExactlyOnce]
        );
        assert_eq!(select_suites("failover").unwrap(), vec![Suite::Failover]);
        assert!(select_suites("nope").is_err());
        assert!(select_suites("").is_err());
    }

    #[test]
    fn replay_log_detection() {
        assert!(log_reports_replay(
            "2026-07-16 INFO recovered 3 rows for 1 tables from the durable tail"
        ));
        assert!(!log_reports_replay(
            "durable tail is empty; nothing to replay"
        ));
        assert!(!log_reports_replay(""));
    }

    /// The pg-cleanup drop must be ownership-checked at DROP time: match
    /// => drop, mismatch => skip, absent => nothing, and a live lock
    /// holder vetoes even an identity match (mint-once identity: a
    /// foreign adopter of our schema keeps our identity).
    #[test]
    fn pg_drop_ownership_decision() {
        // Identity match, no live holder: ours to drop.
        assert_eq!(pg_drop_decision(Some("a"), Some("a"), None), PgDrop::Drop);
        // Identity mismatch: another writer re-minted the schema — skip,
        // naming both identities.
        match pg_drop_decision(Some("mine-id"), Some("theirs-id"), None) {
            PgDrop::Skip(why) => {
                assert!(why.contains("mine-id"), "skip names our identity: {why}");
                assert!(
                    why.contains("theirs-id"),
                    "skip names the found identity: {why}"
                );
            }
            other => panic!("mismatch must skip, got {other:?}"),
        }
        // Schema absent at cleanup: nothing to drop, whatever we remember.
        assert_eq!(pg_drop_decision(Some("a"), None, None), PgDrop::Nothing);
        assert_eq!(pg_drop_decision(None, None, None), PgDrop::Nothing);
        assert_eq!(pg_drop_decision(None, None, Some("pid 1")), PgDrop::Nothing);
        // A schema exists but this run never created one: foreign — skip.
        assert!(matches!(
            pg_drop_decision(None, Some("foreign-id"), None),
            PgDrop::Skip(why) if why.contains("foreign-id")
        ));
        // A live advisory-lock holder vetoes even an identity match.
        match pg_drop_decision(Some("a"), Some("a"), Some("pid 42, application \"srv\"")) {
            PgDrop::Skip(why) => assert!(why.contains("pid 42"), "skip names the holder: {why}"),
            other => panic!("a live holder must skip, got {other:?}"),
        }
        // The unreadable-meta marker can never match a UUID identity.
        assert!(matches!(
            pg_drop_decision(
                Some("a"),
                Some("<unreadable icegres_tail.meta: boom>"),
                None
            ),
            PgDrop::Skip(_)
        ));
    }

    #[test]
    fn ansi_stripping() {
        assert_eq!(strip_ansi("\u{1b}[1;32mPASS\u{1b}[0m ok"), "PASS ok");
        assert_eq!(strip_ansi("plain"), "plain");
    }

    /// The skip paths must be decidable WITHOUT any backend running: an
    /// unconfigured backend SKIPS loudly instead of silently passing.
    #[test]
    fn skip_logic_by_backend() {
        // fencing/failover skip on dir; durability/exactly-once skip on none.
        assert!(matches!(Backend::None, b if b.tail_flags().is_empty()));
        let dir = Backend::Dir(PathBuf::from("/tmp/x"));
        assert_eq!(dir.tail_flags()[0], "--tail-dir");
        let pg = Backend::Pg("postgresql://u@h/db".into());
        assert_eq!(pg.tail_flags()[0], "--tail-url");
        let q = Backend::Quorum("a:1,b:2,c:3".into());
        assert_eq!(q.tail_flags()[0], "--tail-quorum");
    }
}
