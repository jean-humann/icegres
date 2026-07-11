//! Durable Postgres-backed tail for buffered writes (`--tail-url <postgres
//! url>`, opt-in) — backend 2 of the durable-tail roadmap
//! (docs/sota-roadmap.md §3): the same [`TailStore`] contract as the local
//! WAL (`tail.rs`), with the frames living in a Postgres database instead of
//! this node's disk. The natural zero-new-infra target is a dedicated
//! database on the instance already backing Lakekeeper — every icegres
//! deployment already runs one and already treats it as availability-
//! critical.
//!
//! # The durability contract, stated honestly
//!
//! * **Durability = the tail database's own fsync/replication.** Every
//!   buffered INSERT's rows are committed to the `frames` table BEFORE the
//!   client ack (one SQL `INSERT`, implicit transaction,
//!   `synchronous_commit` pinned to `on` at SESSION scope right after
//!   connect and verified with `SHOW` — a per-database/per-role
//!   `ALTER ... SET synchronous_commit = off` on the tail database can
//!   never silently void the ack), so the tail survives losing this
//!   COMPUTE NODE entirely — the upgrade over `--tail-dir`, whose tail
//!   dies with the node's disk. Only server-side `fsync = off` (a
//!   cluster-wide postgresql.conf choice no session can override) remains
//!   genuinely delegated to the tail database's operator.
//! * **A tail-database outage BLOCKS buffered writes.** Unreachable at
//!   boot = a startup error; unreachable mid-flight = the append fails =
//!   the INSERT's statement error (never a silent downgrade to non-durable
//!   buffering). Backpressure, not loss. The worker never reconnects: a
//!   broken tail connection also dropped the one-writer advisory lock, and
//!   silently re-acquiring it would race a replacement process that took
//!   the lock in between — restart the server instead (already-buffered
//!   rows keep flushing; already-acked rows are in the tail).
//! * **Same exactly-once protocol as the local WAL.** The watermark lives
//!   in the LAKE (the `icegres.tail-seq.<tail-id>` snapshot property; see
//!   `tail.rs` module docs), namespaced by an identity minted once into
//!   this schema's `meta` table — same URL + schema = same logical tail =
//!   same cursor across restarts. The `watermarks` table is the sidecar
//!   (second gate against a foreign writer dropping the property), written
//!   best-effort after each covered flush and never regressing.
//! * **One process per logical tail — best-effort BOOT-TIME mutual
//!   exclusion, NOT the correctness guard.** The `flock` equivalent is a
//!   session-scoped advisory lock (`pg_try_advisory_lock`) on a key
//!   derived from the schema name, taken at open on the dedicated
//!   connection and held for that CONNECTION's lifetime (the connection IS
//!   the lock), so a second `icegres serve` on the same tail fails loudly
//!   at boot. But the lock releases with its session: if the tail
//!   connection dies while this process lives, a replacement server can
//!   take the lock while the old process is still flushing its buffered
//!   in-memory rows. That overlap cannot double-apply — exactly-once is
//!   guaranteed by the in-commit watermark property + the catalog's
//!   assert-ref-snapshot-id CAS + the fresh metadata reload before every
//!   flush attempt (`buffer.rs::flush_table` and its
//!   `generation_already_committed` guard), never by the lock itself.
//!   Fleet-SHARED tails — multiple computes overlaying one tail
//!   (LISTEN/NOTIFY, flush leases) — are the roadmap's explicit next
//!   increment, NOT this backend.
//! * **The tail append runs under the buffer lock** (same as LocalWal's
//!   fsync): one statement's durable ack serializes with every other
//!   buffered INSERT and with same-server union reads for that round
//!   trip's window (~1-3 ms to a same-box database, per the roadmap
//!   budget; a WAN tail database taxes every buffered ack accordingly).
//! * **TLS URLs are not supported yet** (`NoTls` — the client is compiled
//!   without a TLS stack): keep the tail database on localhost or a
//!   trusted network segment.
//!
//! # Schema (auto-created, idempotent, self-contained)
//!
//! Everything lives in one schema (default `icegres_tail`) so a shared
//! database stays tidy and a test can use a throwaway schema:
//!
//! * `meta(singleton, identity, format)` — one row holding the tail
//!   identity UUID and the payload format version (a pre-v2 schema, which
//!   lacks the column entirely, is refused loudly at open)
//!   (the `<dir>/identity` equivalent), minted client-side on first open
//!   with `ON CONFLICT DO NOTHING` so a racing first boot keeps exactly one.
//! * `frames(table_key, seq, payload, PRIMARY KEY (table_key, seq))` —
//!   one row per STATEMENT: `table_key` is the same percent-encoded
//!   `<ns>.<table>` string LocalWal uses for directory names
//!   (`tail::table_dir_name`), `payload` the same statement-atomic
//!   versioned op payload (`tail::encode_op_payload`: format byte + op
//!   discriminator + Arrow IPC stream) — the file framing's crc/torn-write
//!   machinery is dropped because Postgres' own WAL and page checksums
//!   already guarantee an INSERT is all-or-nothing.
//! * `watermarks(table_key, seq)` — the sidecar; also what floors sequence
//!   numbering after a full truncate + restart (`max(seq)+1` from frames,
//!   `seq+1` from here, whichever is higher, seeded at open).
//!
//! # Sync trait over an async client
//!
//! [`TailStore`] is synchronous and `append` runs under the buffer's std
//! mutex on a tokio worker thread, so the tokio-postgres client cannot be
//! driven there (`block_on` inside a runtime panics). A dedicated worker
//! THREAD owns a single-threaded runtime, the connection, and the prepared
//! statements; trait methods send a job over an unbounded channel (a
//! non-blocking send, safe in async context) and block on a std channel
//! for the reply — blocking the calling thread for exactly the durable
//! round trip, precisely like LocalWal's fsync. Dropping [`PgTail`] closes
//! the channel, ends the worker, and tears down the connection (releasing
//! the advisory lock server-side).

use std::collections::HashMap;
use std::sync::mpsc as std_mpsc;
use std::sync::Mutex as StdMutex;
use std::thread::JoinHandle;

use anyhow::{anyhow, bail, Context as _, Result};
use arrow::array::RecordBatch;
use iceberg::TableIdent;
use tokio_postgres::{Client, NoTls, Statement};

use crate::tail::{
    decode_op_payload, encode_op_payload, parse_table_dir_name, table_dir_name, ReplayedTable,
    TailOp, TailOpKind, TailStore, TAIL_PAYLOAD_FORMAT, TAIL_SEQ_PROPERTY_PREFIX,
};

/// Schema every production tail lives in (tests pass their own throwaway
/// schema through [`PgTail::open_with_schema`]).
const DEFAULT_SCHEMA: &str = "icegres_tail";

/// First key of the two-int advisory lock (the second is derived from the
/// schema name) — a constant tag so icegres tail locks can never collide
/// with another application's advisory locks in a shared database. Kept
/// within i32/oid range so the holder-diagnosis query can compare it
/// against `pg_locks.classid` directly.
const LOCK_CLASS: i32 = 0x1CE9_7A11;

/// One request to the worker thread; every variant carries its own reply
/// channel (a std channel — the caller blocks on it for the round trip).
enum Job {
    Append {
        key: String,
        seq: i64,
        payload: Vec<u8>,
        resp: std_mpsc::Sender<Result<()>>,
    },
    Replay {
        resp: std_mpsc::Sender<Result<RawReplay>>,
    },
    Truncate {
        key: String,
        upto_seq: i64,
        resp: std_mpsc::Sender<Result<()>>,
    },
    RecordWatermark {
        key: String,
        seq: i64,
        resp: std_mpsc::Sender<Result<()>>,
    },
    /// Test-only: read the session's `synchronous_commit` over the worker
    /// connection. No SQL can inspect ANOTHER session's GUCs, so asserting
    /// the FIX-2 pin held must ride the tail connection itself.
    #[cfg(test)]
    ShowSyncCommit {
        resp: std_mpsc::Sender<Result<String>>,
    },
}

/// One table's decoded frames: `(seq, op-of-one-statement)` in sequence
/// order — the shape [`ReplayedTable::frames`] carries.
type TableFrames = Vec<(u64, TailOp)>;

/// Raw replay rows as they leave the database; decoding (table keys, IPC
/// payloads) happens on the caller's thread, not the connection's.
struct RawReplay {
    /// `(table_key, seq, payload)` ordered by key then seq.
    frames: Vec<(String, i64, Vec<u8>)>,
    /// `(table_key, watermark)` — includes tables with zero frames.
    watermarks: Vec<(String, i64)>,
}

/// What the worker reports back once the connection is open, locked, and
/// the schema ensured.
struct InitState {
    identity: String,
    /// Per-table next-sequence seeds: `max(frames.seq) + 1` and
    /// `watermarks.seq + 1`, whichever is higher.
    seeds: Vec<(String, u64)>,
}

/// [`TailStore`] backed by a Postgres database (see the module docs for the
/// schema, the durability class, and the honest scope).
pub struct PgTail {
    /// `icegres.tail-seq.<tail-id>` — this tail's watermark property key,
    /// derived from the identity persisted in the `meta` table.
    prop_key: String,
    /// `None` only during drop (taken so the worker loop can end).
    job_tx: Option<tokio::sync::mpsc::UnboundedSender<Job>>,
    /// Next sequence per table, seeded at open (frames max / watermarks),
    /// bumped only AFTER a durable append — a failed INSERT never consumes
    /// its number, exactly like LocalWal's rollback contract.
    next_seq: StdMutex<HashMap<TableIdent, u64>>,
    /// Joined on drop so the connection (= the advisory lock) is torn down
    /// before `drop` returns.
    worker: Option<JoinHandle<()>>,
}

impl PgTail {
    /// Open the tail at `url` (creating schema/tables if absent), take the
    /// one-writer advisory lock, load or mint the persistent identity, and
    /// seed per-table sequence numbering. Fails loudly when the database
    /// is unreachable or another process holds the lock.
    pub fn open(url: &str) -> Result<Self> {
        Self::open_with_schema(url, DEFAULT_SCHEMA)
    }

    /// [`open`](Self::open) into an explicit schema (tests give every case
    /// its own throwaway schema; production uses [`DEFAULT_SCHEMA`]).
    pub fn open_with_schema(url: &str, schema: &str) -> Result<Self> {
        validate_schema_name(schema)?;
        let (job_tx, job_rx) = tokio::sync::mpsc::unbounded_channel();
        let (init_tx, init_rx) = std_mpsc::channel();
        let url = url.to_string();
        let schema = schema.to_string();
        let worker = std::thread::Builder::new()
            .name("icegres-tail-pg".into())
            .spawn(move || worker_main(url, schema, job_rx, init_tx))
            .context("cannot spawn the tail-pg worker thread")?;
        let init = match init_rx.recv() {
            Ok(init) => init,
            Err(_) => {
                let _ = worker.join();
                bail!("tail-pg worker exited before reporting its startup outcome");
            }
        };
        let init = match init {
            Ok(init) => init,
            Err(e) => {
                drop(job_tx);
                let _ = worker.join();
                return Err(e);
            }
        };
        let mut next_seq: HashMap<TableIdent, u64> = HashMap::new();
        for (key, floor) in init.seeds {
            match parse_table_dir_name(&key) {
                Some(ident) => {
                    next_seq.insert(ident, floor);
                }
                // Loud, but not fatal: a foreign row cannot hold rows WE
                // acked (appends only ever write encodable keys), so only
                // its seed is meaningless. Replay WARNs about it too.
                None => tracing::warn!(
                    table_key = key,
                    "tail-pg row does not decode to a table identifier; ignoring its \
                     sequence seed (foreign row in the frames/watermarks table?)"
                ),
            }
        }
        Ok(Self {
            prop_key: format!("{TAIL_SEQ_PROPERTY_PREFIX}{}", init.identity),
            job_tx: Some(job_tx),
            next_seq: StdMutex::new(next_seq),
            worker: Some(worker),
        })
    }

    /// Round-trip one job to the worker: non-blocking send, blocking reply
    /// (the durable-ack wait — the same thread-blocking window LocalWal
    /// spends in fsync).
    fn call<T>(&self, build: impl FnOnce(std_mpsc::Sender<Result<T>>) -> Job) -> Result<T> {
        let (resp_tx, resp_rx) = std_mpsc::channel();
        self.job_tx
            .as_ref()
            .expect("job_tx lives until drop")
            .send(build(resp_tx))
            .map_err(|_| anyhow!("tail-pg worker is gone; restart the server"))?;
        resp_rx
            .recv()
            .map_err(|_| anyhow!("tail-pg worker dropped a request; restart the server"))?
    }
}

impl Drop for PgTail {
    fn drop(&mut self) {
        // Closing the job channel ends the worker loop; joining guarantees
        // the runtime (and with it the connection holding the advisory
        // lock) is torn down before drop returns.
        self.job_tx.take();
        if let Some(worker) = self.worker.take() {
            let _ = worker.join();
        }
    }
}

impl TailStore for PgTail {
    fn append(&self, table: &TableIdent, kind: TailOpKind, batches: &[RecordBatch]) -> Result<u64> {
        let key = table_dir_name(table)?;
        // ICEGRES_QUERY_TIMING tail-ack budget: payload encode vs. the
        // durable INSERT+commit round trip. Cached bool when unset.
        let timing = crate::timing::enabled();
        // Encode BEFORE consuming anything: an unencodable statement fails
        // with no seq minted and no round trip made.
        let t = timing.then(std::time::Instant::now);
        let payload = encode_op_payload(kind, batches)?;
        if let Some(t) = t {
            crate::timing::record("tail_encode", t.elapsed());
        }
        let mut map = self.next_seq.lock().expect("tail-pg seq lock poisoned");
        let entry = map.entry(table.clone()).or_insert(1);
        let seq = *entry;
        let seq_i64 = i64::try_from(seq)
            .map_err(|_| anyhow!("tail sequence {seq} for {table} overflows BIGINT"))?;
        let t = timing.then(std::time::Instant::now);
        self.call(|resp| Job::Append {
            key,
            seq: seq_i64,
            payload,
            resp,
        })
        .with_context(|| format!("tail-pg append for {table} (seq {seq}) failed"))?;
        if let Some(t) = t {
            crate::timing::record("tail_pg_commit", t.elapsed());
        }
        // Only now is the frame durable: consume the sequence number (a
        // failed INSERT left no row, so reusing its number is safe).
        *entry += 1;
        Ok(seq)
    }

    fn replay(&self) -> Result<Vec<ReplayedTable>> {
        let raw = self.call(|resp| Job::Replay { resp })?;
        // Rows arrive ordered by (table_key, seq): group by adjacency.
        let mut by_key: Vec<(String, TableFrames)> = Vec::new();
        for (key, seq, payload) in raw.frames {
            let seq = u64::try_from(seq)
                .map_err(|_| anyhow!("tail-pg frame for {key:?} holds a negative seq {seq}"))?;
            let op = decode_op_payload(&payload).with_context(|| {
                format!("tail-pg frame {key:?}/{seq} does not decode (its rows hold acked writes)")
            })?;
            match by_key.last_mut() {
                Some((k, frames)) if *k == key => frames.push((seq, op)),
                _ => by_key.push((key, vec![(seq, op)])),
            }
        }
        let mut watermarks: HashMap<String, u64> = HashMap::new();
        for (key, seq) in raw.watermarks {
            if let Ok(seq) = u64::try_from(seq) {
                watermarks.insert(key, seq);
            }
        }
        let mut out: Vec<ReplayedTable> = Vec::new();
        for (key, frames) in by_key {
            let Some(ident) = parse_table_dir_name(&key) else {
                tracing::warn!(
                    table_key = key,
                    "tail-pg frames row does not name an <ns>.<table>; skipping it"
                );
                continue;
            };
            let sidecar_watermark = watermarks.remove(&key);
            out.push(ReplayedTable {
                ident,
                frames,
                sidecar_watermark,
            });
        }
        // Watermark rows WITHOUT frames are the frameless-table case: the
        // caller must still apply the sequence floor to them (tail.rs on
        // why), exactly like LocalWal's empty table directories.
        for (key, seq) in watermarks {
            let Some(ident) = parse_table_dir_name(&key) else {
                tracing::warn!(
                    table_key = key,
                    "tail-pg watermarks row does not name an <ns>.<table>; skipping it"
                );
                continue;
            };
            out.push(ReplayedTable {
                ident,
                frames: Vec::new(),
                sidecar_watermark: Some(seq),
            });
        }
        Ok(out)
    }

    fn truncate(&self, table: &TableIdent, upto_seq: u64) -> Result<()> {
        let key = table_dir_name(table)?;
        // A watermark past i64::MAX cannot exist in the table (appends cap
        // at BIGINT); saturate rather than error on a nonsense argument.
        let upto_seq = i64::try_from(upto_seq).unwrap_or(i64::MAX);
        self.call(|resp| Job::Truncate {
            key,
            upto_seq,
            resp,
        })
        .with_context(|| format!("tail-pg truncate for {table} (<= {upto_seq}) failed"))
    }

    fn ensure_seq_floor(&self, table: &TableIdent, floor: u64) -> Result<()> {
        let mut map = self.next_seq.lock().expect("tail-pg seq lock poisoned");
        let entry = map.entry(table.clone()).or_insert(1);
        *entry = (*entry).max(floor);
        Ok(())
    }

    fn watermark_property(&self) -> &str {
        &self.prop_key
    }

    fn record_watermark(&self, table: &TableIdent, seq: u64) -> Result<()> {
        // The outcome is the caller's to act on (buffer.rs skips the
        // covered-frame truncate when this fails, so one flush can never
        // leave a table with neither frames nor a watermark row): report
        // it instead of swallowing it.
        let key = table_dir_name(table).with_context(|| {
            format!("cannot encode the tail-pg table key for the watermark sidecar of {table}")
        })?;
        let seq_i64 = i64::try_from(seq)
            .map_err(|_| anyhow!("tail-pg watermark {seq} for {table} overflows BIGINT"))?;
        self.call(|resp| Job::RecordWatermark {
            key,
            seq: seq_i64,
            resp,
        })
        .with_context(|| format!("tail-pg watermark UPSERT for {table} ({seq}) failed"))
    }
}

/// Only `[A-Za-z0-9_]`, not digit-leading, <= 63 bytes (NAMEDATALEN - 1):
/// the schema name is interpolated into DDL as a quoted identifier, and
/// restricting the alphabet keeps that interpolation trivially safe.
fn validate_schema_name(schema: &str) -> Result<()> {
    let ok = !schema.is_empty()
        && schema.len() <= 63
        && !schema.starts_with(|c: char| c.is_ascii_digit())
        && schema
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_');
    if !ok {
        bail!(
            "invalid tail schema name {schema:?}: use ASCII letters, digits, and '_' \
             (not digit-leading, at most 63 bytes)"
        );
    }
    Ok(())
}

/// Prepared statements + query strings the worker runs (all schema-
/// qualified once at init).
struct Sql {
    append: Statement,
    truncate: Statement,
    record_watermark: Statement,
    replay_frames: String,
    replay_watermarks: String,
}

/// The worker thread: a single-threaded runtime driving the dedicated
/// connection and serving jobs until the channel closes (= PgTail drop).
fn worker_main(
    url: String,
    schema: String,
    mut job_rx: tokio::sync::mpsc::UnboundedReceiver<Job>,
    init_tx: std_mpsc::Sender<Result<InitState>>,
) {
    let rt = match tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
    {
        Ok(rt) => rt,
        Err(e) => {
            let _ = init_tx.send(Err(anyhow!(e).context("cannot build the tail-pg runtime")));
            return;
        }
    };
    rt.block_on(async move {
        let (client, sql, init) = match open_connection(&url, &schema).await {
            Ok(opened) => opened,
            Err(e) => {
                let _ = init_tx.send(Err(e));
                return;
            }
        };
        if init_tx.send(Ok(init)).is_err() {
            return; // opener gone; nothing to serve
        }
        while let Some(job) = job_rx.recv().await {
            run_job(&client, &sql, job).await;
        }
        // Channel closed = PgTail dropped: block_on returns, the runtime
        // and connection drop, and the advisory lock releases server-side.
    });
}

/// Connect, take the one-writer lock, ensure the schema, load/mint the
/// identity, seed sequence numbering, prepare the hot statements.
async fn open_connection(url: &str, schema: &str) -> Result<(Client, Sql, InitState)> {
    let mut config: tokio_postgres::Config = url
        .parse()
        .context("--tail-url is not a valid Postgres connection URL")?;
    if config.get_application_name().is_none() {
        config.application_name("icegres-tail");
    }
    // A dead HOST (power loss, network partition) never sends a FIN, so
    // only TCP keepalive notices the death — and tokio-postgres' default
    // keepalives_idle is 2 HOURS. The dead node's session (and with it the
    // one-writer advisory lock) would linger that long, blocking a
    // replacement server's boot. Probe after 30 s of idle instead, unless
    // the URL carries its own keepalive tuning (then the operator's
    // settings win untouched).
    if !url.contains("keepalives") {
        config.keepalives(true);
        config.keepalives_idle(std::time::Duration::from_secs(30));
    }
    // NoTls: the client is compiled without a TLS stack (module docs).
    let (client, connection) = config.connect(NoTls).await.context(
        "cannot connect to the tail database (--tail-url); buffered writes need the \
         tail REACHABLE — a durable tail nothing can append to would be acked loss",
    )?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            // The connection IS the advisory lock: once it dies every
            // append fails until the operator restarts (module docs on why
            // reconnecting would be a correctness hole, not a convenience).
            tracing::error!(
                "the tail-pg connection DIED. Consequences: every buffered INSERT on \
                 this server now FAILS (backpressure, not loss — acked rows are safe \
                 in the tail database); rows already buffered in memory keep flushing \
                 to Iceberg; and the one-writer advisory lock is RELEASED, so a \
                 replacement server can take over this tail. Restart this server — it \
                 never reconnects on its own (silently re-taking the lock would race \
                 such a replacement): {e}"
            );
        }
    });

    // Durable-before-ack must not be voidable by a per-database/per-role
    // `ALTER ... SET synchronous_commit = off` on the tail database: pin it
    // at SESSION scope (session beats role/db defaults) and verify it
    // stuck. With this pinned, only server-side `fsync = off` (a
    // cluster-wide postgresql.conf choice) remains genuinely delegated to
    // the tail database's operator.
    client
        .batch_execute("SET synchronous_commit = on")
        .await
        .context("cannot pin synchronous_commit = on on the tail connection")?;
    let sync_commit: String = client
        .query_one("SHOW synchronous_commit", &[])
        .await
        .context("cannot read back synchronous_commit from the tail connection")?
        .get(0);
    if sync_commit != "on" {
        bail!(
            "the tail connection reports synchronous_commit = {sync_commit:?} even \
             after `SET synchronous_commit = on`: every buffered ack could be lost by \
             a tail-database crash, silently voiding durable-before-ack. Is a proxy \
             between icegres and the tail database rewriting session settings?"
        );
    }

    // One-writer guard FIRST, so two racing first boots cannot interleave
    // schema setup or identity minting either. Session-scoped: held until
    // this connection ends.
    let lock_key = schema_lock_key(schema);
    let locked: bool = client
        .query_one(
            "SELECT pg_try_advisory_lock($1, $2)",
            &[&LOCK_CLASS, &lock_key],
        )
        .await
        .context("cannot take the tail advisory lock")?
        .get(0);
    if !locked {
        let (holder, takeover) = match describe_lock_holder(&client, lock_key).await {
            Some((pid, desc)) => (
                format!(" ({desc})"),
                format!(
                    " If the holder is a DEAD node whose connection has not timed out \
                     yet, an operator can force takeover with `SELECT \
                     pg_terminate_backend({pid})` on the tail database — only after \
                     confirming that process is really gone (terminating a LIVE \
                     holder puts two writers on one tail)."
                ),
            ),
            None => (String::new(), String::new()),
        };
        bail!(
            "the tail database is LOCKED by another session{holder} — most likely \
             another `icegres serve` with the same --tail-url (schema {schema:?}). Two \
             writers on one tail would double-apply recovered rows and truncate each \
             other's frames; give each server its own tail database or schema.{takeover}"
        );
    }

    // The lock was granted — but grant alone is not enough: behind a
    // TRANSACTION-mode pooler (pgbouncer transaction pooling, RDS Proxy)
    // each statement can land on a DIFFERENT backend, so the session lock
    // "taken" above would silently be absent from every later statement's
    // session and the one-writer guard would be void without any error.
    // Verify the lock is visible from THIS session's own backend.
    let lock_visible: bool = client
        .query_one(
            "SELECT EXISTS (
                 SELECT 1 FROM pg_locks
                 WHERE locktype = 'advisory' AND granted
                   AND pid = pg_backend_pid()
                   AND classid::int4 = $1 AND objid::int4 = $2 AND objsubid = 2
             )",
            &[&LOCK_CLASS, &lock_key],
        )
        .await
        .context("cannot verify the tail advisory lock is visible from this session")?
        .get(0);
    if !lock_visible {
        bail!(
            "the tail advisory lock was granted but is NOT visible from this \
             session's backend — --tail-url almost certainly points at a \
             TRANSACTION-mode connection pooler, which scatters this session's \
             statements across backends and silently voids the one-writer guard. \
             --tail-url must be a direct connection or session-pooled."
        );
    }

    let q = format!("\"{schema}\""); // safe: validate_schema_name ran first
    let fmt = TAIL_PAYLOAD_FORMAT;
    client
        .batch_execute(&format!(
            "CREATE SCHEMA IF NOT EXISTS {q};
             CREATE TABLE IF NOT EXISTS {q}.meta (
                 singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
                 identity  text    NOT NULL,
                 format    int     NOT NULL DEFAULT {fmt}
             );
             CREATE TABLE IF NOT EXISTS {q}.frames (
                 table_key text   NOT NULL,
                 seq       bigint NOT NULL,
                 payload   bytea  NOT NULL,
                 PRIMARY KEY (table_key, seq)
             );
             CREATE TABLE IF NOT EXISTS {q}.watermarks (
                 table_key text   PRIMARY KEY,
                 seq       bigint NOT NULL
             );"
        ))
        .await
        .context("cannot create the tail schema/tables in the tail database")?;

    // Mint-once identity: the INSERT is a no-op when a row exists, so the
    // first boot's UUID wins forever (the LocalWal <dir>/identity shape).
    let minted = uuid::Uuid::new_v4().to_string();
    client
        .execute(
            &format!(
                "INSERT INTO {q}.meta (singleton, identity) VALUES (true, $1) \
                 ON CONFLICT (singleton) DO NOTHING"
            ),
            &[&minted],
        )
        .await
        .context("cannot mint the tail identity")?;
    // Read identity AND format in one statement: a pre-v2 schema has no
    // `format` column at all, so the SELECT itself fails there — refuse
    // loudly instead of mis-decoding unversioned frames.
    let meta_row = client
        .query_one(&format!("SELECT identity, format FROM {q}.meta"), &[])
        .await
        .with_context(|| {
            format!(
                "cannot read the tail identity/format from {q}.meta — if the error names a \
                 missing \"format\" column, this tail schema was written by a \
                 pre-v{TAIL_PAYLOAD_FORMAT} icegres (unversioned frame layout) and its \
                 frames may hold acked rows this build cannot decode. Recover them with \
                 the version that wrote them, or DROP SCHEMA {q} CASCADE to acknowledge \
                 losing them"
            )
        })?;
    let identity: String = meta_row.get(0);
    let format: i32 = meta_row.get(1);
    if format != i32::from(TAIL_PAYLOAD_FORMAT) {
        bail!(
            "tail schema {q} declares on-disk format {format}, but this icegres \
             reads/writes format {TAIL_PAYLOAD_FORMAT}. Recover its frames with the \
             version that wrote them, or DROP SCHEMA {q} CASCADE to acknowledge \
             losing them."
        );
    }
    let identity = identity.trim().to_string();
    // A corrupt identity is a loud error (same as LocalWal): silently
    // minting a NEW one would orphan every watermark the old one stamped.
    uuid::Uuid::parse_str(&identity).with_context(|| {
        format!(
            "the tail identity row does not hold a UUID ({identity:?}); if the meta \
             table is corrupt beyond recovery, delete the row to mint a new identity \
             (acknowledging that watermarks stamped under the old identity are orphaned)"
        )
    })?;

    // Sequence-numbering seeds: above every surviving frame AND above every
    // sidecar watermark (the frameless-after-truncate restart case).
    let mut seeds: HashMap<String, u64> = HashMap::new();
    for row in client
        .query(
            &format!("SELECT table_key, max(seq) FROM {q}.frames GROUP BY table_key"),
            &[],
        )
        .await
        .context("cannot scan tail frames for sequence seeds")?
    {
        let key: String = row.get(0);
        let max: i64 = row.get(1);
        let floor = u64::try_from(max).unwrap_or(0).saturating_add(1);
        seeds.insert(key, floor);
    }
    for row in client
        .query(&format!("SELECT table_key, seq FROM {q}.watermarks"), &[])
        .await
        .context("cannot scan tail watermarks for sequence seeds")?
    {
        let key: String = row.get(0);
        let seq: i64 = row.get(1);
        let floor = u64::try_from(seq).unwrap_or(0).saturating_add(1);
        let entry = seeds.entry(key).or_insert(1);
        *entry = (*entry).max(floor);
    }

    let sql = Sql {
        append: client
            .prepare(&format!(
                "INSERT INTO {q}.frames (table_key, seq, payload) VALUES ($1, $2, $3)"
            ))
            .await
            .context("cannot prepare the tail append statement")?,
        truncate: client
            .prepare(&format!(
                "DELETE FROM {q}.frames WHERE table_key = $1 AND seq <= $2"
            ))
            .await
            .context("cannot prepare the tail truncate statement")?,
        record_watermark: client
            .prepare(&format!(
                "INSERT INTO {q}.watermarks AS w (table_key, seq) VALUES ($1, $2) \
                 ON CONFLICT (table_key) DO UPDATE SET seq = GREATEST(w.seq, EXCLUDED.seq)"
            ))
            .await
            .context("cannot prepare the tail watermark statement")?,
        replay_frames: format!(
            "SELECT table_key, seq, payload FROM {q}.frames ORDER BY table_key, seq"
        ),
        replay_watermarks: format!("SELECT table_key, seq FROM {q}.watermarks"),
    };
    Ok((
        client,
        sql,
        InitState {
            identity,
            seeds: seeds.into_iter().collect(),
        },
    ))
}

/// Second advisory-lock key: the schema-name hash, masked non-negative so
/// the holder-diagnosis query can cast it to `oid` for `pg_locks.objid`.
///
/// crc32 means two DIFFERENT schema names in one database CAN collide
/// (~2^-31 per pair, after the mask): the failure mode is a loud, safe
/// boot refusal whose diagnosis may name the OTHER tail's holder — never
/// two writers on one tail. That negligible probability does not warrant
/// code (a wider key would break the pg_locks-diagnosable two-int shape);
/// the refusal message already tells the operator to give each server its
/// own schema or database.
fn schema_lock_key(schema: &str) -> i32 {
    (crc32fast::hash(schema.as_bytes()) & 0x7fff_ffff) as i32
}

/// Best-effort "who holds the lock" for the refusal message: the holder's
/// backend pid (for the pg_terminate_backend takeover hint) and a display
/// string. `None` when the catalog view is unreadable or the holder is
/// gone (never a second error).
async fn describe_lock_holder(client: &Client, lock_key: i32) -> Option<(i32, String)> {
    let row = client
        .query_opt(
            "SELECT a.pid, coalesce(nullif(a.application_name, ''), '')
             FROM pg_locks l JOIN pg_stat_activity a ON a.pid = l.pid
             WHERE l.locktype = 'advisory' AND l.granted
               AND l.classid::int4 = $1 AND l.objid::int4 = $2 AND l.objsubid = 2",
            &[&LOCK_CLASS, &lock_key],
        )
        .await
        .ok()
        .flatten()?;
    let pid: i32 = row.get(0);
    let app: String = row.get(1);
    let desc = if app.is_empty() {
        format!("pid {pid}")
    } else {
        format!("pid {pid} ({app})")
    };
    Some((pid, desc))
}

/// Serve one job; every arm replies exactly once (a dropped reply channel
/// only means the caller gave up — ignore it).
async fn run_job(client: &Client, sql: &Sql, job: Job) {
    match job {
        Job::Append {
            key,
            seq,
            payload,
            resp,
        } => {
            let r = client
                .execute(&sql.append, &[&key, &seq, &payload])
                .await
                .map(|_| ())
                .map_err(|e| anyhow!("tail database INSERT failed (is it reachable?): {e}"));
            let _ = resp.send(r);
        }
        Job::Replay { resp } => {
            let _ = resp.send(replay_rows(client, sql).await);
        }
        Job::Truncate {
            key,
            upto_seq,
            resp,
        } => {
            let r = client
                .execute(&sql.truncate, &[&key, &upto_seq])
                .await
                .map(|_| ())
                .map_err(|e| anyhow!("tail database DELETE failed: {e}"));
            let _ = resp.send(r);
        }
        Job::RecordWatermark { key, seq, resp } => {
            let r = client
                .execute(&sql.record_watermark, &[&key, &seq])
                .await
                .map(|_| ())
                .map_err(|e| anyhow!("tail database watermark UPSERT failed: {e}"));
            let _ = resp.send(r);
        }
        #[cfg(test)]
        Job::ShowSyncCommit { resp } => {
            let r = client
                .query_one("SHOW synchronous_commit", &[])
                .await
                .map(|row| row.get::<_, String>(0))
                .map_err(|e| anyhow!("SHOW synchronous_commit failed: {e}"));
            let _ = resp.send(r);
        }
    }
}

async fn replay_rows(client: &Client, sql: &Sql) -> Result<RawReplay> {
    let mut frames = Vec::new();
    for row in client
        .query(sql.replay_frames.as_str(), &[])
        .await
        .map_err(|e| anyhow!("cannot read tail frames for replay: {e}"))?
    {
        frames.push((
            row.get::<_, String>(0),
            row.get::<_, i64>(1),
            row.get::<_, Vec<u8>>(2),
        ));
    }
    let mut watermarks = Vec::new();
    for row in client
        .query(sql.replay_watermarks.as_str(), &[])
        .await
        .map_err(|e| anyhow!("cannot read tail watermarks for replay: {e}"))?
    {
        watermarks.push((row.get::<_, String>(0), row.get::<_, i64>(1)));
    }
    Ok(RawReplay { frames, watermarks })
}

// ---------------------------------------------------------------------------
// Unit tests — LIVE against a real Postgres, gated on ICEGRES_TEST_PG_URL
// (the local stack's extra db: postgresql://lakekeeper:lakekeeper@127.0.0.1:
// 5433/icegres_test). Unset = each test prints a skip note and passes. Every
// test owns a throwaway schema (unique per process/test) and drops it on the
// way out, so runs are self-contained and re-runnable.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef as ArrowSchemaRef};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    fn test_url() -> Option<String> {
        match std::env::var("ICEGRES_TEST_PG_URL") {
            Ok(url) if !url.trim().is_empty() => Some(url),
            _ => {
                eprintln!(
                    "skipping: ICEGRES_TEST_PG_URL unset (live tail-database test; point it \
                     at e.g. postgresql://lakekeeper:lakekeeper@127.0.0.1:5433/icegres_test)"
                );
                None
            }
        }
    }

    static TEST_SCHEMA_SEQ: AtomicU64 = AtomicU64::new(0);

    /// Drops its schema on drop (declare BEFORE the PgTail so the tail —
    /// and its connection — goes first).
    struct SchemaGuard {
        url: String,
        schema: String,
    }

    impl SchemaGuard {
        fn new(url: &str, name: &str) -> Self {
            let schema = format!(
                "icegres_tail_t_{}_{}_{}",
                std::process::id(),
                name,
                TEST_SCHEMA_SEQ.fetch_add(1, Ordering::SeqCst)
            );
            let guard = Self {
                url: url.to_string(),
                schema,
            };
            guard.drop_schema(); // pre-clean a leftover from a crashed run
            guard
        }

        fn drop_schema(&self) {
            admin_exec(
                &self.url,
                &format!("DROP SCHEMA IF EXISTS \"{}\" CASCADE", self.schema),
            );
        }
    }

    impl Drop for SchemaGuard {
        fn drop(&mut self) {
            self.drop_schema();
        }
    }

    /// Run admin SQL over a throwaway connection (its own tiny runtime —
    /// these tests are plain #[test], no ambient runtime exists).
    fn admin_exec(url: &str, sql: &str) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("admin runtime");
        rt.block_on(async {
            let (client, connection) = tokio_postgres::connect(url, NoTls)
                .await
                .expect("admin connect");
            let driver = tokio::spawn(connection);
            client.batch_execute(sql).await.expect("admin sql");
            drop(client);
            let _ = driver.await;
        });
    }

    fn schema() -> ArrowSchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn ident(name: &str) -> TableIdent {
        TableIdent::from_strs(["demo", name]).unwrap()
    }

    fn batch(vals: &[i64]) -> RecordBatch {
        RecordBatch::try_new(schema(), vec![Arc::new(Int64Array::from(vals.to_vec()))]).unwrap()
    }

    fn ids(b: &RecordBatch) -> Vec<i64> {
        b.column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .values()
            .to_vec()
    }

    // Frames round-trip through a process "restart" (a new PgTail on the
    // same schema), per-table sequences stay independent and monotonic,
    // replay comes back in seq order, and numbering resumes ABOVE the
    // recovered frames.
    #[test]
    fn append_replay_roundtrip_and_seq_resume() {
        let Some(url) = test_url() else { return };
        let guard = SchemaGuard::new(&url, "roundtrip");
        let tail = PgTail::open_with_schema(&url, &guard.schema).unwrap();
        // FIX (phase1b-2): the session GUC is pinned on at open, so a
        // per-database/per-role `synchronous_commit = off` default can
        // never silently void durable-before-ack. (Reaching this point
        // also means open's pg_locks visibility check passed on this
        // direct connection — the FIX-5 anti-transaction-pooler guard.)
        assert_eq!(
            tail.call(|resp| Job::ShowSyncCommit { resp }).unwrap(),
            "on",
            "the tail session must run with synchronous_commit = on"
        );
        assert_eq!(
            tail.append(&ident("t1"), TailOpKind::Append, &[batch(&[1, 2])])
                .unwrap(),
            1
        );
        assert_eq!(
            tail.append(&ident("t1"), TailOpKind::Append, &[batch(&[3])])
                .unwrap(),
            2
        );
        // A second table numbers from 1: sequences are per-table.
        assert_eq!(
            tail.append(&ident("t2"), TailOpKind::Append, &[batch(&[9])])
                .unwrap(),
            1
        );
        drop(tail);
        let tail2 = PgTail::open_with_schema(&url, &guard.schema).unwrap();
        let mut replayed = tail2.replay().unwrap();
        replayed.sort_by_key(|t| t.ident.to_string());
        assert_eq!(replayed.len(), 2);
        assert_eq!(replayed[0].ident, ident("t1"));
        let seqs: Vec<u64> = replayed[0].frames.iter().map(|(s, _)| *s).collect();
        assert_eq!(seqs, vec![1, 2]);
        assert_eq!(ids(&replayed[0].frames[0].1.batches()[0]), vec![1, 2]);
        assert_eq!(ids(&replayed[0].frames[1].1.batches()[0]), vec![3]);
        assert_eq!(replayed[1].ident, ident("t2"));
        assert_eq!(ids(&replayed[1].frames[0].1.batches()[0]), vec![9]);
        // Numbering resumes above the recovered frames after the restart.
        assert_eq!(
            tail2
                .append(&ident("t1"), TailOpKind::Append, &[batch(&[4])])
                .unwrap(),
            3
        );
        assert_eq!(
            tail2
                .append(&ident("t2"), TailOpKind::Append, &[batch(&[10])])
                .unwrap(),
            2
        );
    }

    // A multi-batch statement is ONE frame (one seq, one INSERT, one
    // commit) and replay returns its batches in order — statement-atomic
    // by construction, same as a LocalWal frame.
    #[test]
    fn statement_frame_holds_all_batches() {
        let Some(url) = test_url() else { return };
        let guard = SchemaGuard::new(&url, "stmt");
        let tail = PgTail::open_with_schema(&url, &guard.schema).unwrap();
        let seq = tail
            .append(
                &ident("t"),
                TailOpKind::Append,
                &[batch(&[1]), batch(&[2, 3]), batch(&[4])],
            )
            .unwrap();
        assert_eq!(seq, 1);
        drop(tail);
        let tail2 = PgTail::open_with_schema(&url, &guard.schema).unwrap();
        let replayed = tail2.replay().unwrap();
        assert_eq!(replayed.len(), 1);
        assert_eq!(
            replayed[0].frames.len(),
            1,
            "3 batches = 1 statement = 1 frame"
        );
        let per_batch: Vec<Vec<i64>> = replayed[0].frames[0].1.batches().iter().map(ids).collect();
        assert_eq!(per_batch, vec![vec![1], vec![2, 3], vec![4]]);
    }

    // truncate forgets frames <= upto_seq and nothing else.
    #[test]
    fn truncate_forgets_covered_frames() {
        let Some(url) = test_url() else { return };
        let guard = SchemaGuard::new(&url, "truncate");
        let tail = PgTail::open_with_schema(&url, &guard.schema).unwrap();
        for v in 1..=3i64 {
            tail.append(&ident("t"), TailOpKind::Append, &[batch(&[v])])
                .unwrap();
        }
        tail.truncate(&ident("t"), 2).unwrap();
        let replayed = tail.replay().unwrap();
        assert_eq!(replayed.len(), 1);
        let seqs: Vec<u64> = replayed[0].frames.iter().map(|(s, _)| *s).collect();
        assert_eq!(seqs, vec![3], "only the uncovered frame survives");
        // Idempotent: truncating below the survivor deletes nothing.
        tail.truncate(&ident("t"), 2).unwrap();
        assert_eq!(tail.replay().unwrap()[0].frames.len(), 1);
    }

    // The watermark row floors sequence numbering across a reopen even
    // with ZERO surviving frames (the full-truncate-then-restart shape),
    // never regresses to a lower value, and replay reports the frameless
    // table so the caller can apply its own floor too.
    #[test]
    fn watermark_row_floors_sequences_across_reopen() {
        let Some(url) = test_url() else { return };
        let guard = SchemaGuard::new(&url, "wmfloor");
        let tail = PgTail::open_with_schema(&url, &guard.schema).unwrap();
        tail.record_watermark(&ident("t"), 5).unwrap();
        tail.record_watermark(&ident("t"), 3).unwrap(); // lower: must not regress
        drop(tail);
        let tail2 = PgTail::open_with_schema(&url, &guard.schema).unwrap();
        let replayed = tail2.replay().unwrap();
        assert_eq!(replayed.len(), 1, "frameless table still reported");
        assert_eq!(replayed[0].ident, ident("t"));
        assert!(replayed[0].frames.is_empty());
        assert_eq!(replayed[0].sidecar_watermark, Some(5));
        // The seed alone (no ensure_seq_floor call) already clears the
        // watermark: the next append cannot land under it.
        assert_eq!(
            tail2
                .append(&ident("t"), TailOpKind::Append, &[batch(&[10])])
                .unwrap(),
            6
        );
    }

    // ensure_seq_floor bumps the next sequence and never lowers it.
    #[test]
    fn ensure_seq_floor_bumps_never_lowers() {
        let Some(url) = test_url() else { return };
        let guard = SchemaGuard::new(&url, "floor");
        let tail = PgTail::open_with_schema(&url, &guard.schema).unwrap();
        tail.ensure_seq_floor(&ident("t"), 10).unwrap();
        assert_eq!(
            tail.append(&ident("t"), TailOpKind::Append, &[batch(&[1])])
                .unwrap(),
            10
        );
        tail.ensure_seq_floor(&ident("t"), 5).unwrap(); // lower: no-op
        assert_eq!(
            tail.append(&ident("t"), TailOpKind::Append, &[batch(&[2])])
                .unwrap(),
            11
        );
    }

    // The identity is minted once and persists across reopens, so the
    // watermark property key never changes for a given tail; a DIFFERENT
    // schema on the same database is a different tail with its own key.
    #[test]
    fn identity_persists_across_reopen() {
        let Some(url) = test_url() else { return };
        let guard = SchemaGuard::new(&url, "identity");
        let tail = PgTail::open_with_schema(&url, &guard.schema).unwrap();
        let key = tail.watermark_property().to_string();
        assert!(key.starts_with(TAIL_SEQ_PROPERTY_PREFIX));
        let id = key.strip_prefix(TAIL_SEQ_PROPERTY_PREFIX).unwrap();
        uuid::Uuid::parse_str(id).expect("identity is a uuid");
        drop(tail);
        let tail2 = PgTail::open_with_schema(&url, &guard.schema).unwrap();
        assert_eq!(tail2.watermark_property(), key);
        let other_guard = SchemaGuard::new(&url, "identity_other");
        let other = PgTail::open_with_schema(&url, &other_guard.schema).unwrap();
        assert_ne!(
            other.watermark_property(),
            key,
            "distinct schemas are distinct logical tails"
        );
    }

    // The one-writer guard: a second open on the SAME schema is refused
    // while the first holds the advisory lock, and succeeds once the
    // holder is gone (the lock releases with its connection).
    #[test]
    fn second_open_on_same_schema_is_refused() {
        let Some(url) = test_url() else { return };
        let guard = SchemaGuard::new(&url, "lock");
        let tail = PgTail::open_with_schema(&url, &guard.schema).unwrap();
        let err = match PgTail::open_with_schema(&url, &guard.schema) {
            Ok(_) => panic!("second open on a locked tail schema must fail"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("LOCKED by another session"),
            "unexpected error: {err:#}"
        );
        drop(tail);
        // Drop joined the worker (connection closed client-side); the
        // server may lag a beat releasing the lock — retry briefly.
        let mut reopened = None;
        for _ in 0..50 {
            match PgTail::open_with_schema(&url, &guard.schema) {
                Ok(t) => {
                    reopened = Some(t);
                    break;
                }
                Err(_) => std::thread::sleep(std::time::Duration::from_millis(100)),
            }
        }
        assert!(
            reopened.is_some(),
            "lock must release once the holder is gone"
        );
    }

    // PHASE 2: keyed op kinds round-trip through the BYTEA payload and a
    // reopen — Upsert/Delete come back as themselves in seq order.
    #[test]
    fn keyed_op_kinds_roundtrip() {
        let Some(url) = test_url() else { return };
        let guard = SchemaGuard::new(&url, "opkinds");
        let tail = PgTail::open_with_schema(&url, &guard.schema).unwrap();
        tail.append(&ident("t"), TailOpKind::Append, &[batch(&[1])])
            .unwrap();
        tail.append(&ident("t"), TailOpKind::Upsert, &[batch(&[2])])
            .unwrap();
        tail.append(&ident("t"), TailOpKind::Delete, &[batch(&[3])])
            .unwrap();
        drop(tail);
        let tail2 = PgTail::open_with_schema(&url, &guard.schema).unwrap();
        let replayed = tail2.replay().unwrap();
        assert_eq!(replayed.len(), 1);
        let kinds: Vec<(u64, TailOpKind)> = replayed[0]
            .frames
            .iter()
            .map(|(seq, op)| (*seq, op.kind()))
            .collect();
        assert_eq!(
            kinds,
            vec![
                (1, TailOpKind::Append),
                (2, TailOpKind::Upsert),
                (3, TailOpKind::Delete)
            ]
        );
        assert_eq!(ids(&replayed[0].frames[1].1.batches()[0]), vec![2]);
    }

    // PHASE 2: a pre-v2 tail schema (meta without the format column — the
    // exact shape an older icegres leaves) is refused loudly at open.
    #[test]
    fn old_schema_without_format_column_is_refused() {
        let Some(url) = test_url() else { return };
        let guard = SchemaGuard::new(&url, "oldschema");
        // Hand-build the pre-v2 schema shape.
        admin_exec(
            &url,
            &format!(
                "CREATE SCHEMA \"{s}\";
                 CREATE TABLE \"{s}\".meta (
                     singleton boolean PRIMARY KEY DEFAULT true CHECK (singleton),
                     identity  text    NOT NULL
                 );
                 INSERT INTO \"{s}\".meta (singleton, identity)
                 VALUES (true, '00000000-0000-0000-0000-000000000000');",
                s = guard.schema
            ),
        );
        let err = match PgTail::open_with_schema(&url, &guard.schema) {
            Ok(_) => panic!("a pre-v2 tail schema must be refused"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("identity/format"),
            "unexpected error: {err:#}"
        );
    }
}
