//! Durable local tail for buffered writes (`--tail-dir <dir>`, opt-in) —
//! Phase 1 of the durable-tail roadmap (docs/sota-roadmap.md §1/§3).
//!
//! With `--write-buffer-ms N > 0`, buffered INSERTs today ack from process
//! memory and an UNCLEAN kill loses up to `N` ms of acked writes (the
//! documented trade in `buffer.rs`). The tail closes that window: every
//! buffered INSERT's rows are appended to an fsync'd per-table WAL segment
//! BEFORE the client ack, and on the next boot with the same `--tail-dir`
//! any acked-but-uncommitted rows are replayed into the write buffer and
//! committed by the normal flusher. A SIGKILL/power loss of the process
//! loses NOTHING.
//!
//! # The durability contract, stated honestly
//!
//! * **Durability = THIS node's disk.** The tail is a local file; losing the
//!   node or the disk still loses acked-but-uncommitted rows. This backend
//!   is a strict upgrade over in-memory buffering (kill-safe, not
//!   node-loss-safe) — the shared/quorum backends in the roadmap are the
//!   path to node-loss durability.
//! * **Exactly-once across crashes is anchored in the lake.** Every flush
//!   commit records the highest tail sequence it drained as a table
//!   property namespaced by this tail's identity —
//!   `icegres.tail-seq.<tail-id>` (see [`TAIL_SEQ_PROPERTY_PREFIX`]), where
//!   `<tail-id>` is a UUID generated once and persisted at
//!   `<dir>/identity` — in the SAME atomic REST commit as the snapshot.
//!   Boot replay drops frames with `seq <= watermark` read from current
//!   table metadata, so a crash between commit and tail truncation cannot
//!   double-apply, and a crash before the commit cannot lose an acked row.
//!   Namespacing means several buffered writers on ONE table (each with its
//!   own tail dir) keep independent exactly-once cursors instead of
//!   clobbering a shared cell. A local sidecar (`<dir>/<table>/watermark`,
//!   written best-effort after each successful flush) is the second gate:
//!   replay uses `max(property, sidecar)`, so a foreign writer dropping the
//!   property (e.g. a REPLACE TABLE that resets properties) does not defeat
//!   the double-apply guard. Residual window, honestly: a crash between the
//!   commit and the sidecar write COMBINED with a foreign property drop can
//!   still double-apply that one generation.
//! * **One writer per tail dir.** `LocalWal::open` takes an exclusive
//!   `flock` on `<dir>/.lock` held for the process lifetime; a second
//!   process (or a second open in this process) fails loudly instead of
//!   double-applying recovered rows and cross-truncating segments.
//! * **Fail loudly.** A tail append error is the INSERT's statement error
//!   (no silent downgrade to non-durable); an unreadable tail at boot
//!   aborts startup rather than silently dropping acked rows.
//! * **Tail fsync runs under the buffer lock.** One statement's durable
//!   append serializes with every other buffered INSERT and with the
//!   same-server union-read overlay, so a slow tail disk stalls other
//!   tables' buffered inserts AND union reads for that fsync's window.
//!   Per-table locking is the known follow-up.
//! * **flock is advisory — and unreliable on NFS.** The one-writer guard
//!   only binds processes on a filesystem with sound flock semantics; put
//!   the tail dir on a LOCAL filesystem, never NFS.
//!
//! # On-disk format
//!
//! `<dir>/<table-dir>/` holds numbered segment files (`%016x.seg`), where
//! `<table-dir>` is the namespace levels + table name, each component
//! percent-encoded (`%`, `.`, and `/` escaped; NUL rejected outright) and
//! joined with `.` — so
//! `ns=["a"], table="b.c"` and `ns=["a","b"], table="c"` never collide. The
//! active segment receives appends; at flush start the flusher rotates
//! ([`TailStore::rotate`]) so a successful commit can delete whole covered
//! segments ([`TailStore::truncate`]) instead of head-truncating a live
//! file. Each frame is `[u32 len][u32 crc32(payload)][payload]` where the
//! payload is the little-endian `u64` sequence number followed by the Arrow
//! IPC stream encoding of ALL batches of ONE statement (schema per frame —
//! simple and self-describing; fine for v1's per-statement volumes). One
//! statement = one frame = one fsync = one sequence number, so a statement
//! is durable all-or-nothing: a mid-statement failure can never leave a
//! replayable prefix of a statement the client was told failed.
//!
//! # Failed appends never poison the segment
//!
//! The whole frame is built in memory first; on ANY write/fsync error the
//! segment is rolled back (`set_len`) to the last known-good frame boundary
//! so later acked frames never sit behind garbage, and the un-consumed
//! sequence number is safely reused. If the rollback itself fails (disk
//! truly failing), the segment is POISONED: sealed at the last known-good
//! offset and never written again — the next append opens a fresh segment.
//! Residual window, honestly: a poisoned segment whose truncation failed
//! keeps its trailing garbage on disk, and a later crash-replay stops at
//! that garbage (discarding any later segments) — the price of a disk that
//! rejects both the write and its undo. When a segment is poisoned, a
//! best-effort `<segment>.poisoned` marker records the last good byte
//! length (ASCII u64) so a later replay CLAMPS the scan there: bytes the
//! failing disk wrote past the boundary — possibly a whole crc-valid
//! "ghost" frame whose un-consumed sequence was later reused — never
//! replay. Residual window, honestly: if the marker write fails too, a
//! later crash-replay stops at the trailing garbage instead.
//!
//! # Torn-write tolerance
//!
//! Replay stops a segment at the first frame whose length/crc/payload is
//! invalid, truncates the file to the last good frame, and WARNs with
//! counts. A torn FINAL frame is the expected shape of a power loss — the
//! frames before it replay normally, never an error. When a bad frame has
//! LATER segments behind it, a later segment is kept only if its first
//! frame's sequence is contiguous with the last good frame — the shape a
//! poisoned append leaves (its failed sequence was reused by the fresh
//! segment, so no acked row is missing). A later segment past a REAL
//! sequence gap is deleted with a loud WARN, because replaying rows from
//! beyond a hole would reorder acked writes.

use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write as _;
use std::os::unix::io::AsRawFd as _;
use std::path::{Path, PathBuf};
use std::sync::Mutex as StdMutex;

use anyhow::{anyhow, bail, Context as _, Result};
use arrow::array::RecordBatch;
use arrow::ipc::reader::StreamReader;
use arrow::ipc::writer::StreamWriter;
use iceberg::TableIdent;

/// Prefix of the table property carrying the highest tail sequence a flush
/// commit drained (the exactly-once watermark; see the module docs). The
/// full key is `icegres.tail-seq.<tail-id>` — namespaced by the tail's
/// persistent identity ([`TailStore::watermark_property`]) so independent
/// buffered writers on one table never clobber each other's cursor.
pub const TAIL_SEQ_PROPERTY_PREFIX: &str = "icegres.tail-seq.";

/// Name of the per-tail identity file under the tail dir.
const IDENTITY_FILE: &str = "identity";

/// Name of the exclusive-lock file under the tail dir.
const LOCK_FILE: &str = ".lock";

/// Name of the per-table committed-watermark sidecar file (second gate
/// against table-property loss; see the module docs).
const WATERMARK_FILE: &str = "watermark";

/// One table's surviving tail state as seen by boot replay. Tables whose
/// directory exists but holds no frames are still reported — the caller
/// must apply the committed-watermark sequence floor
/// ([`TailStore::ensure_seq_floor`]) even to frameless tables, otherwise a
/// fully-truncated tail restarts numbering at 1 UNDER the persisted
/// watermark and the next crash-replay silently drops acked rows.
pub struct ReplayedTable {
    pub ident: TableIdent,
    /// `(seq, batches-of-one-statement)` in sequence order; may be empty.
    pub frames: Vec<(u64, Vec<RecordBatch>)>,
    /// The local watermark sidecar, if present and parseable.
    pub sidecar_watermark: Option<u64>,
}

/// A durable staging log for acked-but-uncommitted buffered rows. The
/// buffer appends BEFORE acking, the flusher truncates AFTER the Iceberg
/// commit lands — the tail is never a second source of truth, only the gap
/// between the ack and the commit.
pub trait TailStore: Send + Sync {
    /// Durably (fsync) append ONE STATEMENT's batches for `table` as a
    /// single frame; returns the monotonic per-table sequence number. Must
    /// not return before the bytes are on disk — the caller acks the INSERT
    /// on the strength of this. All-or-nothing: on error no partial frame
    /// survives (see the module docs). `batches` must be non-empty.
    fn append(&self, table: &TableIdent, batches: &[RecordBatch]) -> Result<u64>;

    /// Segment-management hint called at flush start: seal the active
    /// segment so a later [`truncate`](TailStore::truncate) can delete it
    /// whole. Backends without segments may keep the no-op default.
    fn rotate(&self, _table: &TableIdent) -> Result<()> {
        Ok(())
    }

    /// Every table directory's surviving frames (in per-table sequence
    /// order) plus its sidecar watermark — INCLUDING tables with zero
    /// surviving frames, so the caller can apply the sequence floor. Called
    /// once at boot, before any appends.
    fn replay(&self) -> Result<Vec<ReplayedTable>>;

    /// Forget everything with `seq <= upto_seq` for `table` (called after
    /// the Iceberg commit recording that watermark succeeded).
    fn truncate(&self, table: &TableIdent, upto_seq: u64) -> Result<()>;

    /// Guarantee the next sequence handed out for `table` is at least
    /// `floor`. Boot replay calls this with `committed watermark + 1` for
    /// EVERY table directory (even frameless ones): after a full truncate +
    /// restart, surviving frames alone would restart numbering at 1 below
    /// the persisted watermark, and the NEXT crash-replay would drop those
    /// acked rows as "already covered".
    fn ensure_seq_floor(&self, table: &TableIdent, floor: u64) -> Result<()>;

    /// The table-property key THIS tail stamps and reads for its committed
    /// watermark (`icegres.tail-seq.<tail-id>`). Stable across restarts of
    /// the same tail dir.
    fn watermark_property(&self) -> &str;

    /// Record locally that a flush commit covered `seq` (the watermark
    /// sidecar — second gate against table-property loss). Implementations
    /// must report the REAL outcome instead of swallowing it: the caller
    /// (`buffer.rs::tail_truncate_covered`) skips the covered-frame
    /// truncate when this fails, so one flush can never leave a table with
    /// NEITHER frames NOR a watermark row (that table would vanish from
    /// replay entirely and the property sequence floor would never apply).
    /// The failure itself still never fails the flush — the property
    /// stamped in the commit keeps replay exact; skipping the truncate is
    /// a bounded leak, not a loss.
    fn record_watermark(&self, _table: &TableIdent, _seq: u64) -> Result<()> {
        Ok(())
    }
}

/// Drop replayed frames already covered by the committed watermark
/// (`seq <= watermark`) — the crash-after-commit-before-truncate guard.
/// Returns `(survivors, dropped_count)`.
pub fn drop_stale_frames<T>(
    frames: Vec<(u64, T)>,
    watermark: Option<u64>,
) -> (Vec<(u64, T)>, usize) {
    let Some(w) = watermark else {
        return (frames, 0);
    };
    let before = frames.len();
    let survivors: Vec<(u64, T)> = frames.into_iter().filter(|(seq, _)| *seq > w).collect();
    let dropped = before - survivors.len();
    (survivors, dropped)
}

/// Parse a watermark table-property value, WARNing loudly on garbage — a
/// garbled value must never silently become "no watermark" (that would
/// re-apply covered frames); the caller falls back to the sidecar.
pub fn parse_watermark_property(table: &TableIdent, raw: Option<&str>) -> Option<u64> {
    let raw = raw?;
    match raw.trim().parse::<u64>() {
        Ok(v) => Some(v),
        Err(_) => {
            tracing::warn!(
                table = %table,
                value = raw,
                "tail watermark table property is UNPARSEABLE (foreign writer or \
                 corruption?); falling back to the local watermark sidecar"
            );
            None
        }
    }
}

/// The replay watermark for one table: the max of the table-property value
/// (this tail's own key only) and the local sidecar. The max defends
/// against a foreign writer dropping/garbling the property (sidecar wins)
/// AND against a stale sidecar (property wins).
pub fn effective_watermark(
    table: &TableIdent,
    property: Option<&str>,
    sidecar: Option<u64>,
) -> Option<u64> {
    match (parse_watermark_property(table, property), sidecar) {
        (Some(p), Some(s)) => Some(p.max(s)),
        (p, s) => p.or(s),
    }
}

/// The segment currently receiving appends for one table.
struct ActiveSegment {
    path: PathBuf,
    file: File,
    /// Highest sequence written to this segment (0 = no frames yet).
    max_seq: u64,
    /// Byte length of the last known-good frame boundary — the rollback
    /// target when an append fails partway (see the module docs).
    good_len: u64,
}

/// A sealed (rotated, recovered, or poisoned) segment awaiting truncation
/// coverage.
struct SealedSegment {
    path: PathBuf,
    max_seq: u64,
}

/// Per-table WAL state.
struct TableWal {
    dir: PathBuf,
    next_seq: u64,
    next_segment: u64,
    active: Option<ActiveSegment>,
    sealed: Vec<SealedSegment>,
}

/// Seal the active segment (if any): frames it holds await truncation
/// coverage; a frameless segment file is simply removed. Shared by
/// [`TailStore::rotate`] and the poisoned-append path — after this, the
/// next append opens a fresh segment (a poisoned file handle is never
/// reused).
fn seal_active(entry: &mut TableWal) {
    if let Some(active) = entry.active.take() {
        if active.max_seq > 0 {
            entry.sealed.push(SealedSegment {
                path: active.path,
                max_seq: active.max_seq,
            });
        } else {
            // A segment without one good frame: just remove it (best-effort
            // — on the poisoned path the disk may refuse even this). Its
            // poison marker (if any) must go WITH the file — segment ids can
            // be reused after a restart, and a stale marker would clamp a
            // future healthy segment of the same id — but only once the file
            // itself is gone (a surviving garbage file must keep its clamp).
            if fs::remove_file(&active.path).is_ok() {
                let _ = fs::remove_file(poison_marker_path(&active.path));
            }
        }
    }
}

/// Roll a segment file back to its last known-good frame boundary and make
/// the truncation durable — the undo of a failed append's partial bytes.
fn roll_back_segment(file: &File, good_len: u64) -> std::io::Result<()> {
    file.set_len(good_len)?;
    file.sync_data()
}

/// [`TailStore`] backed by fsync'd WAL segments under a local directory
/// (see the module docs for layout, frame format, and the honest scope of
/// the durability it buys).
pub struct LocalWal {
    root: PathBuf,
    /// `icegres.tail-seq.<tail-id>` — this tail's watermark property key,
    /// derived from the identity persisted at `<dir>/identity`.
    prop_key: String,
    /// Exclusive `flock` on `<dir>/.lock`, held (the fd lives in the
    /// struct) for the process lifetime — one writer per tail dir.
    _lock: File,
    tables: StdMutex<HashMap<TableIdent, TableWal>>,
}

impl LocalWal {
    /// Open (creating if absent) the tail directory: take the exclusive
    /// dir lock, load or mint the persistent tail identity. Existing
    /// segments are read lazily: [`TailStore::replay`] at boot, or a
    /// directory scan on a table's first append if replay was skipped.
    pub fn open(root: &Path) -> Result<Self> {
        fs::create_dir_all(root)
            .with_context(|| format!("cannot create tail dir {}", root.display()))?;
        let lock = lock_dir_exclusive(root)?;
        let identity = load_or_create_identity(root)?;
        Ok(Self {
            root: root.to_path_buf(),
            prop_key: format!("{TAIL_SEQ_PROPERTY_PREFIX}{identity}"),
            _lock: lock,
            tables: StdMutex::new(HashMap::new()),
        })
    }
}

impl TailStore for LocalWal {
    fn append(&self, table: &TableIdent, batches: &[RecordBatch]) -> Result<u64> {
        let mut tables = self.tables.lock().expect("tail lock poisoned");
        let entry = table_entry(&self.root, &mut tables, table)?;
        if entry.active.is_none() {
            let id = entry.next_segment;
            let path = entry.dir.join(format!("{id:016x}.seg"));
            let file = OpenOptions::new()
                .create_new(true)
                .append(true)
                .open(&path)
                .with_context(|| format!("cannot create tail segment {}", path.display()))?;
            // Make the new directory entry itself durable before any frame
            // relies on it.
            sync_dir(&entry.dir)?;
            entry.next_segment += 1;
            entry.active = Some(ActiveSegment {
                path,
                file,
                max_seq: 0,
                good_len: 0,
            });
        }
        let seq = entry.next_seq;
        // The whole frame is built in memory FIRST so a failure can only
        // ever leave partial bytes of one contiguous write — which the
        // rollback below erases.
        let frame = encode_frame(seq, batches)?;
        let (write_res, good_len, path) = {
            let active = entry.active.as_mut().expect("just ensured active");
            let res = active
                .file
                .write_all(&frame)
                .and_then(|()| active.file.sync_data());
            (res, active.good_len, active.path.clone())
        };
        if let Err(e) = write_res {
            // Roll back to the last known-good frame boundary so later
            // acked frames never sit behind garbage; retry through a fresh
            // handle before giving up (the original handle may be wedged).
            let rolled_back = {
                let active = entry.active.as_mut().expect("just ensured active");
                roll_back_segment(&active.file, good_len).or_else(|_| {
                    OpenOptions::new()
                        .write(true)
                        .open(&path)
                        .and_then(|f| roll_back_segment(&f, good_len))
                })
            };
            if let Err(rb) = rolled_back {
                // POISONED: the disk refused both the write and its undo.
                // Seal at the last known-good offset and never touch this
                // file again — the next append opens a fresh segment.
                tracing::warn!(
                    segment = %path.display(),
                    good_len,
                    "tail segment is POISONED (append failed AND rollback failed: {rb}); \
                     sealing it at the last good frame — new appends go to a fresh \
                     segment. A .poisoned marker records the boundary so replay \
                     clamps there"
                );
                // Best-effort marker: record the last good byte length so a
                // later replay clamps the scan there. The failing disk may
                // have persisted ANY prefix of the frame — even the whole
                // thing, a crc-valid "ghost" whose sequence is about to be
                // reused — and without the clamp it would replay.
                let marker = poison_marker_path(&path);
                if let Err(me) = fs::write(&marker, good_len.to_string()) {
                    tracing::warn!(
                        marker = %marker.display(),
                        "cannot write tail poison marker (best-effort; a crash \
                         before a covering flush may then stop replay at the \
                         trailing garbage): {me}"
                    );
                }
                seal_active(entry);
            }
            // Either way the sequence number was NOT consumed (it is only
            // consumed after a durable append), so reusing it is safe: no
            // durable frame carries it.
            return Err(anyhow!(e).context(format!("tail append to {} failed", path.display())));
        }
        // Only now is the frame durable: consume the sequence number.
        {
            let active = entry.active.as_mut().expect("just ensured active");
            active.good_len += frame.len() as u64;
            active.max_seq = seq;
        }
        entry.next_seq += 1;
        Ok(seq)
    }

    fn rotate(&self, table: &TableIdent) -> Result<()> {
        let mut tables = self.tables.lock().expect("tail lock poisoned");
        if let Some(entry) = tables.get_mut(table) {
            seal_active(entry);
        }
        Ok(())
    }

    fn replay(&self) -> Result<Vec<ReplayedTable>> {
        let mut tables = self.tables.lock().expect("tail lock poisoned");
        let mut out: Vec<ReplayedTable> = Vec::new();
        let mut dirs: Vec<PathBuf> = Vec::new();
        for entry in fs::read_dir(&self.root)
            .with_context(|| format!("cannot read tail dir {}", self.root.display()))?
        {
            let path = entry?.path();
            if path.is_dir() {
                dirs.push(path);
            }
        }
        dirs.sort();
        for dir in dirs {
            let name = dir
                .file_name()
                .and_then(|n| n.to_str())
                .unwrap_or_default()
                .to_string();
            let Some(ident) = parse_table_dir_name(&name) else {
                tracing::warn!(
                    dir = %dir.display(),
                    "tail dir entry does not name an <ns>.<table>; skipping it"
                );
                continue;
            };
            let scan = scan_table(&dir)?;
            out.push(ReplayedTable {
                ident: ident.clone(),
                frames: scan.frames.clone(),
                sidecar_watermark: read_sidecar_watermark(&dir),
            });
            tables.insert(
                ident,
                TableWal {
                    dir,
                    next_seq: scan.next_seq,
                    next_segment: scan.next_segment,
                    active: None,
                    sealed: scan.sealed,
                },
            );
        }
        Ok(out)
    }

    fn truncate(&self, table: &TableIdent, upto_seq: u64) -> Result<()> {
        let mut tables = self.tables.lock().expect("tail lock poisoned");
        let Some(entry) = tables.get_mut(table) else {
            return Ok(());
        };
        let mut first_err: Option<anyhow::Error> = None;
        // A failed delete keeps its segment in the bookkeeping (and on
        // disk); it stays until a later truncate covers it — replay remains
        // safe regardless via the committed watermark.
        entry.sealed.retain(|seg| {
            if seg.max_seq > upto_seq {
                return true;
            }
            match fs::remove_file(&seg.path) {
                Ok(()) => {
                    // Any poison marker must go WITH its segment: ids can be
                    // reused after a restart, and a stale marker would clamp
                    // a future healthy segment of the same id to garbage.
                    let _ = fs::remove_file(poison_marker_path(&seg.path));
                    false
                }
                Err(e) => {
                    first_err.get_or_insert(anyhow!(
                        "cannot delete covered tail segment {}: {e}",
                        seg.path.display()
                    ));
                    true
                }
            }
        });
        // The active segment can only be fully covered when the caller
        // truncated without rotating first; handle it anyway.
        if entry
            .active
            .as_ref()
            .is_some_and(|a| a.max_seq > 0 && a.max_seq <= upto_seq)
        {
            let active = entry.active.take().expect("just checked");
            match fs::remove_file(&active.path) {
                Ok(()) => {
                    let _ = fs::remove_file(poison_marker_path(&active.path));
                }
                Err(e) => {
                    first_err.get_or_insert(anyhow!(
                        "cannot delete covered tail segment {}: {e}",
                        active.path.display()
                    ));
                }
            }
        }
        match first_err {
            None => Ok(()),
            Some(e) => Err(e),
        }
    }

    fn ensure_seq_floor(&self, table: &TableIdent, floor: u64) -> Result<()> {
        let mut tables = self.tables.lock().expect("tail lock poisoned");
        let entry = table_entry(&self.root, &mut tables, table)?;
        entry.next_seq = entry.next_seq.max(floor);
        Ok(())
    }

    fn watermark_property(&self) -> &str {
        &self.prop_key
    }

    fn record_watermark(&self, table: &TableIdent, seq: u64) -> Result<()> {
        // Unencodable names cannot have appended (append fails on them), so
        // this arm is theoretical — but the outcome is the caller's to act
        // on (it skips the covered-frame truncate on failure), so report it
        // instead of swallowing it.
        let dir = self.root.join(table_dir_name(table).with_context(|| {
            format!("cannot encode tail table dir name for the watermark sidecar of {table}")
        })?);
        // Never regress a higher sidecar (an older flush retrying late).
        if read_sidecar_watermark(&dir).is_some_and(|cur| cur >= seq) {
            return Ok(());
        }
        fs::create_dir_all(&dir)
            .map_err(anyhow::Error::from)
            .and_then(|()| {
                write_atomic(&dir, &dir.join(WATERMARK_FILE), seq.to_string().as_bytes())
            })
            .with_context(|| format!("cannot write tail watermark sidecar for {table} ({seq})"))
    }
}

/// Take the exclusive advisory lock on `<root>/.lock`. flock is per open
/// file description, so even a second `LocalWal::open` in THIS process
/// contends. The fd must stay open for the lock to hold — the caller keeps
/// it in the struct for the process lifetime.
fn lock_dir_exclusive(root: &Path) -> Result<File> {
    let path = root.join(LOCK_FILE);
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .with_context(|| format!("cannot open tail lock file {}", path.display()))?;
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc != 0 {
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::EWOULDBLOCK) {
            bail!(
                "tail dir {} is LOCKED by another process — most likely another \
                 `icegres serve` with the same --tail-dir. Two writers on one tail \
                 dir would double-apply recovered rows and truncate each other's \
                 segments; give each server its own directory.",
                root.display()
            );
        }
        return Err(anyhow!(err).context(format!("cannot flock tail dir {}", root.display())));
    }
    Ok(file)
}

/// Load the persisted tail identity from `<root>/identity`, minting and
/// durably persisting (tmp + rename + fsync) a fresh UUIDv4 on first open.
/// A corrupt identity file is a loud error: silently minting a NEW identity
/// would orphan every watermark the old one stamped.
fn load_or_create_identity(root: &Path) -> Result<String> {
    let path = root.join(IDENTITY_FILE);
    match fs::read_to_string(&path) {
        Ok(s) => {
            let id = s.trim();
            uuid::Uuid::parse_str(id).with_context(|| {
                format!(
                    "tail identity file {} does not hold a UUID ({id:?}); if the file \
                     is corrupt beyond recovery, delete it to mint a new identity \
                     (acknowledging that watermarks stamped under the old identity \
                     are orphaned)",
                    path.display()
                )
            })?;
            Ok(id.to_string())
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
            let id = uuid::Uuid::new_v4().to_string();
            write_atomic(root, &path, id.as_bytes())
                .with_context(|| format!("cannot persist tail identity {}", path.display()))?;
            Ok(id)
        }
        Err(e) => Err(anyhow!(e).context(format!("cannot read tail identity {}", path.display()))),
    }
}

/// Durably write `bytes` to `path` (inside `dir`): tmp file + fsync +
/// rename + dir fsync, so a crash leaves either the old content or the new,
/// never a torn file.
fn write_atomic(dir: &Path, path: &Path, bytes: &[u8]) -> Result<()> {
    let name = path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| anyhow!("write_atomic target {} has no file name", path.display()))?;
    let tmp = dir.join(format!(".{name}.tmp"));
    (|| -> std::io::Result<()> {
        let mut f = File::create(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
        fs::rename(&tmp, path)
    })()
    .with_context(|| format!("cannot write {} atomically", path.display()))?;
    sync_dir(dir)
}

/// Read the per-table watermark sidecar. Garbage is a WARN + `None`, never
/// a silent success — the property (read by the caller) is the other gate.
fn read_sidecar_watermark(dir: &Path) -> Option<u64> {
    let path = dir.join(WATERMARK_FILE);
    let s = fs::read_to_string(&path).ok()?;
    match s.trim().parse::<u64>() {
        Ok(v) => Some(v),
        Err(_) => {
            tracing::warn!(
                sidecar = %path.display(),
                content = s.trim(),
                "tail watermark sidecar is unparseable; ignoring it (the table \
                 property is the primary watermark)"
            );
            None
        }
    }
}

/// Get-or-create the per-table WAL state (tables lock held by the caller).
/// A directory with pre-existing segments but no in-memory entry (appends
/// without a prior replay) is scanned so sequence numbering and the sealed
/// list stay correct.
fn table_entry<'a>(
    root: &Path,
    tables: &'a mut HashMap<TableIdent, TableWal>,
    ident: &TableIdent,
) -> Result<&'a mut TableWal> {
    if !tables.contains_key(ident) {
        let dir = root.join(table_dir_name(ident)?);
        fs::create_dir_all(&dir)
            .with_context(|| format!("cannot create tail table dir {}", dir.display()))?;
        sync_dir(root)?;
        let scan = scan_table(&dir)?;
        tables.insert(
            ident.clone(),
            TableWal {
                dir,
                next_seq: scan.next_seq,
                next_segment: scan.next_segment,
                active: None,
                sealed: scan.sealed,
            },
        );
    }
    Ok(tables.get_mut(ident).expect("just ensured present"))
}

/// Percent-encode one name component so `.` can safely join components AND
/// the result stays one single directory entry: `%` -> `%25`, `.` -> `%2e`,
/// `/` -> `%2f` (a raw `/` would silently split the name into nested
/// directories). A NUL byte is rejected outright — no filesystem accepts
/// it, and failing here names the component instead of surfacing a cryptic
/// OS error mid-append. Everything else passes through (the filesystem is
/// the judge of the rest — fail loudly there, not here).
fn encode_component(part: &str) -> Result<String> {
    let mut out = String::with_capacity(part.len());
    for c in part.chars() {
        match c {
            '\0' => bail!("table name component {part:?} contains a NUL byte"),
            '%' => out.push_str("%25"),
            '.' => out.push_str("%2e"),
            '/' => out.push_str("%2f"),
            c => out.push(c),
        }
    }
    Ok(out)
}

/// Undo [`encode_component`]. `None` on a malformed escape.
fn decode_component(part: &str) -> Option<String> {
    let mut out = String::with_capacity(part.len());
    let mut chars = part.chars();
    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }
        let hex: String = chars.by_ref().take(2).collect();
        if hex.len() != 2 {
            return None;
        }
        let byte = u8::from_str_radix(&hex, 16).ok()?;
        out.push(byte as char);
    }
    Some(out)
}

/// `<ns>.<table>` with each component percent-encoded (see
/// [`encode_component`]) so dotted name components never collide — e.g.
/// `ns=["a"], table="b.c"` becomes `a.b%2ec`, distinct from
/// `ns=["a","b"], table="c"` = `a.b.c`. Round-trips through
/// [`parse_table_dir_name`]. Errors on a component no directory name can
/// carry (NUL). `pub(crate)`: this is the canonical table-key encoding
/// shared by every tail backend — `tail_pg.rs` stores exactly this string
/// in its `table_key` column, so one table addresses the same logical tail
/// state whichever backend holds it.
pub(crate) fn table_dir_name(ident: &TableIdent) -> Result<String> {
    let mut parts: Vec<String> = ident
        .namespace()
        .clone()
        .inner()
        .iter()
        .map(|p| encode_component(p))
        .collect::<Result<_>>()?;
    parts.push(encode_component(ident.name())?);
    Ok(parts.join("."))
}

/// Undo [`table_dir_name`]. `None` on anything that does not decode to a
/// namespaced table identifier (`pub(crate)` for the same backend-sharing
/// reason as its inverse).
pub(crate) fn parse_table_dir_name(name: &str) -> Option<TableIdent> {
    let parts: Vec<String> = name
        .split('.')
        .map(decode_component)
        .collect::<Option<_>>()?;
    if parts.len() < 2 || parts.iter().any(|p| p.is_empty()) {
        return None;
    }
    TableIdent::from_strs(parts).ok()
}

/// fsync a directory so freshly created/removed entries survive power loss.
fn sync_dir(dir: &Path) -> Result<()> {
    File::open(dir)
        .and_then(|d| d.sync_all())
        .with_context(|| format!("cannot fsync tail dir {}", dir.display()))
}

/// A frame's length header is a `u32`: a payload longer than `u32::MAX`
/// would silently WRAP the header, producing an acked frame that can never
/// replay (the scan reads a garbage length). Error instead, so the
/// oversized statement fails loudly before any ack.
fn check_frame_len(len: usize) -> Result<()> {
    if len > u32::MAX as usize {
        bail!(
            "tail frame payload is {len} bytes, over the u32 frame-length limit \
             ({}); split the statement into smaller inserts",
            u32::MAX
        );
    }
    Ok(())
}

/// Arrow IPC stream encoding of ALL batches of ONE statement — the
/// statement-atomic payload every tail backend stores (schema per payload:
/// simple and self-describing, fine for v1's per-statement volumes).
/// LocalWal wraps it in `[len][crc]` file framing plus the LE seq prefix;
/// `tail_pg.rs` stores it verbatim as a `BYTEA` column (Postgres' own page
/// checksums/WAL replace the torn-write machinery). `pub(crate)` so the
/// backends share ONE payload format and frames stay interchangeable.
pub(crate) fn encode_ipc(batches: &[RecordBatch]) -> Result<Vec<u8>> {
    let first = batches
        .first()
        .ok_or_else(|| anyhow!("tail frame needs at least one batch"))?;
    let size_hint: usize = batches.iter().map(|b| b.get_array_memory_size()).sum();
    let mut out = Vec::with_capacity(64 + size_hint);
    {
        let mut writer = StreamWriter::try_new(&mut out, first.schema_ref())
            .map_err(|e| anyhow!("tail frame IPC writer failed: {e}"))?;
        for batch in batches {
            writer
                .write(batch)
                .map_err(|e| anyhow!("tail frame IPC encode failed: {e}"))?;
        }
        writer
            .finish()
            .map_err(|e| anyhow!("tail frame IPC finish failed: {e}"))?;
    }
    Ok(out)
}

/// Undo [`encode_ipc`]: every batch of one statement, in order. An empty
/// stream is an error — a tail frame is never rowless.
pub(crate) fn decode_ipc(bytes: &[u8]) -> Result<Vec<RecordBatch>> {
    let reader = StreamReader::try_new(std::io::Cursor::new(bytes), None)
        .map_err(|e| anyhow!("tail frame IPC header invalid: {e}"))?;
    let batches: Vec<RecordBatch> = reader
        .collect::<std::result::Result<_, _>>()
        .map_err(|e| anyhow!("tail frame IPC decode failed: {e}"))?;
    if batches.is_empty() {
        bail!("tail frame IPC stream holds no batch");
    }
    Ok(batches)
}

/// `[u32 len][u32 crc32(payload)][payload]`, payload = LE u64 seq + Arrow
/// IPC stream bytes of ALL batches of one statement ([`encode_ipc`]).
fn encode_frame(seq: u64, batches: &[RecordBatch]) -> Result<Vec<u8>> {
    let ipc = encode_ipc(batches)?;
    let mut payload = Vec::with_capacity(8 + ipc.len());
    payload.extend_from_slice(&seq.to_le_bytes());
    payload.extend_from_slice(&ipc);
    check_frame_len(payload.len())?;
    let mut frame = Vec::with_capacity(8 + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&crc32fast::hash(&payload).to_le_bytes());
    frame.extend_from_slice(&payload);
    Ok(frame)
}

fn decode_payload(payload: &[u8]) -> Result<(u64, Vec<RecordBatch>)> {
    if payload.len() < 8 {
        bail!("payload shorter than its sequence header");
    }
    let seq = u64::from_le_bytes(payload[..8].try_into().expect("8 bytes"));
    let batches = decode_ipc(&payload[8..])?;
    Ok((seq, batches))
}

struct SegmentScan {
    frames: Vec<(u64, Vec<RecordBatch>)>,
    /// Highest sequence recovered (None = no valid frames).
    max_seq: Option<u64>,
    /// The segment ended in an invalid frame (now truncated away).
    hit_bad_frame: bool,
}

/// Path of a segment's poison marker (`<segment>.poisoned`) — written
/// best-effort when an append fails AND its rollback fails; holds the last
/// good byte length (ASCII u64) so replay clamps the scan there.
fn poison_marker_path(segment: &Path) -> PathBuf {
    let mut name = segment.as_os_str().to_os_string();
    name.push(".poisoned");
    PathBuf::from(name)
}

/// Read a poison marker's recorded good byte length. Garbage is a WARN +
/// `None` — the marker is best-effort defense, never a replay failure.
fn read_poison_marker(path: &Path) -> Option<u64> {
    let s = fs::read_to_string(path).ok()?;
    match s.trim().parse::<u64>() {
        Ok(v) => Some(v),
        Err(_) => {
            tracing::warn!(
                marker = %path.display(),
                content = s.trim(),
                "tail poison marker is unparseable; ignoring it (replay falls \
                 back to stopping at the first invalid frame)"
            );
            None
        }
    }
}

/// Read every valid frame of one segment. On the first invalid frame
/// (torn length/payload, crc mismatch, undecodable IPC) the file is
/// truncated to the last good frame with a WARN — the expected shape of a
/// power loss mid-append. A poison marker (see [`poison_marker_path`])
/// clamps the scan to its recorded good length FIRST, so a crc-valid
/// "ghost" frame the failing disk wrote past that boundary — whose
/// sequence was reused by a later append — never replays.
fn scan_segment(path: &Path) -> Result<SegmentScan> {
    let mut data =
        fs::read(path).with_context(|| format!("cannot read tail segment {}", path.display()))?;
    let marker = poison_marker_path(path);
    let mut clamp: Option<String> = None;
    if let Some(good) = read_poison_marker(&marker) {
        if (data.len() as u64) > good {
            clamp = Some(format!(
                "poison marker clamps the scan to {good} bytes ({} bytes of \
                 post-poisoning garbage past it)",
                data.len() as u64 - good
            ));
            data.truncate(good as usize);
        }
    }
    let mut frames: Vec<(u64, Vec<RecordBatch>)> = Vec::new();
    let mut off: usize = 0;
    let mut good_end: usize = 0;
    let mut bad: Option<String> = None;
    while off < data.len() {
        if data.len() - off < 8 {
            bad = Some(format!("torn header ({} trailing bytes)", data.len() - off));
            break;
        }
        let len = u32::from_le_bytes(data[off..off + 4].try_into().expect("4 bytes")) as usize;
        let crc = u32::from_le_bytes(data[off + 4..off + 8].try_into().expect("4 bytes"));
        if data.len() - off - 8 < len {
            bad = Some(format!(
                "torn payload (frame wants {len} bytes, {} present)",
                data.len() - off - 8
            ));
            break;
        }
        let payload = &data[off + 8..off + 8 + len];
        if crc32fast::hash(payload) != crc {
            bad = Some("crc mismatch".to_string());
            break;
        }
        match decode_payload(payload) {
            Ok(frame) => frames.push(frame),
            Err(e) => {
                bad = Some(format!("{e:#}"));
                break;
            }
        }
        off += 8 + len;
        good_end = off;
    }
    // A clean scan of CLAMPED data still ends in garbage on disk: route it
    // through the same WARN + truncate below so the ghost bytes go away.
    if bad.is_none() {
        bad = clamp;
    }
    let hit_bad_frame = bad.is_some();
    if let Some(reason) = bad {
        tracing::warn!(
            segment = %path.display(),
            recovered_frames = frames.len(),
            discarded_bytes = data.len() - good_end,
            "tail segment ends in an invalid frame ({reason}); truncating to the last \
             good frame (a torn FINAL frame is expected after power loss)"
        );
        let file = OpenOptions::new()
            .write(true)
            .open(path)
            .with_context(|| format!("cannot open {} for truncation", path.display()))?;
        file.set_len(good_end as u64)
            .and_then(|()| file.sync_all())
            .with_context(|| format!("cannot truncate torn tail segment {}", path.display()))?;
        // The clamp is physical now — the marker has done its job (leaving
        // it would clamp a future segment if this id is ever reused).
        let _ = fs::remove_file(&marker);
    }
    Ok(SegmentScan {
        max_seq: frames.last().map(|(seq, _)| *seq),
        hit_bad_frame,
        frames,
    })
}

struct TableScan {
    frames: Vec<(u64, Vec<RecordBatch>)>,
    sealed: Vec<SealedSegment>,
    next_seq: u64,
    next_segment: u64,
}

/// Scan one table's segments in name (= creation) order. Frames come back
/// in sequence order. A bad frame truncates its segment at the last good
/// frame; a LATER segment then survives only when its first frame's
/// sequence is contiguous with the last good frame — the shape a poisoned
/// append leaves (its failed sequence was reused by the fresh segment, so
/// no acked row is missing and replay may continue). A later segment past
/// a REAL sequence gap is deleted with a loud WARN: replaying rows from
/// beyond a hole would reorder acked writes.
fn scan_table(dir: &Path) -> Result<TableScan> {
    let mut seg_paths: Vec<PathBuf> = Vec::new();
    for entry in
        fs::read_dir(dir).with_context(|| format!("cannot read tail dir {}", dir.display()))?
    {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) == Some("seg") {
            seg_paths.push(path);
        }
    }
    seg_paths.sort();
    let mut frames: Vec<(u64, Vec<RecordBatch>)> = Vec::new();
    let mut sealed: Vec<SealedSegment> = Vec::new();
    let mut next_segment: u64 = 1;
    // Segment whose scan ended in a bad frame; later segments must prove
    // sequence continuity to be kept while this is set.
    let mut bad_frame_at: Option<PathBuf> = None;
    for path in &seg_paths {
        if let Some(id) = path
            .file_stem()
            .and_then(|s| s.to_str())
            .and_then(|s| u64::from_str_radix(s, 16).ok())
        {
            next_segment = next_segment.max(id + 1);
        }
        let scan = scan_segment(path)?;
        let Some(max_seq) = scan.max_seq else {
            // Truncated to zero valid frames: nothing left to keep (and
            // nothing at stake behind a bad frame either). The marker goes
            // only WITH its file — a surviving file must keep its clamp.
            if fs::remove_file(path).is_ok() {
                let _ = fs::remove_file(poison_marker_path(path));
            }
            if scan.hit_bad_frame {
                bad_frame_at = Some(path.clone());
            }
            continue;
        };
        if let Some(bad) = &bad_frame_at {
            let expected = frames.last().map(|(seq, _)| seq + 1);
            let first = scan.frames.first().map(|(seq, _)| *seq);
            if expected.is_none() || first != expected {
                tracing::warn!(
                    segment = %path.display(),
                    after = %bad.display(),
                    first_seq = first.unwrap_or_default(),
                    expected_seq = expected.unwrap_or_default(),
                    "deleting tail segment BEHIND a corrupt frame: its first \
                     sequence is not contiguous with the last recovered frame, \
                     and replaying rows from beyond a hole would reorder acked \
                     writes; the rows in this segment are LOST"
                );
                if fs::remove_file(path).is_ok() {
                    let _ = fs::remove_file(poison_marker_path(path));
                }
                continue;
            }
            // Contiguous: the bad frame's sequence was reused by this
            // segment (the poisoned-append shape) — no hole, keep going.
            bad_frame_at = None;
        }
        sealed.push(SealedSegment {
            path: path.clone(),
            max_seq,
        });
        frames.extend(scan.frames);
        if scan.hit_bad_frame {
            bad_frame_at = Some(path.clone());
        }
    }
    let next_seq = frames.last().map(|(seq, _)| seq + 1).unwrap_or(1);
    Ok(TableScan {
        frames,
        sealed,
        next_seq,
        next_segment,
    })
}

// ---------------------------------------------------------------------------
// Unit tests — frame format, replay, torn/corrupt tolerance, rotation,
// truncation, watermark filtering, sequence floor, rollback/poisoning,
// dir lock, identity/sidecar persistence, dir-name encoding. All offline
// against a temp dir.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::Int64Array;
    use arrow::datatypes::{DataType, Field, Schema, SchemaRef as ArrowSchemaRef};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;

    static TEST_DIR_SEQ: AtomicU64 = AtomicU64::new(0);

    /// Fresh per-test directory (unique per process run; cleaned by the OS
    /// temp policy — tests must not depend on pre-existing state).
    fn temp_root(name: &str) -> PathBuf {
        let n = TEST_DIR_SEQ.fetch_add(1, Ordering::SeqCst);
        let dir = std::env::temp_dir().join(format!(
            "icegres-tail-test-{}-{}-{}",
            std::process::id(),
            name,
            n
        ));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn schema() -> ArrowSchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }

    fn ident() -> TableIdent {
        TableIdent::from_strs(["demo", "t"]).unwrap()
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

    /// Replay flattened to `(ident, seq, batches)` triples, in order.
    fn replay_frames(wal: &LocalWal) -> Vec<(TableIdent, u64, Vec<RecordBatch>)> {
        let mut out = Vec::new();
        for table in wal.replay().unwrap() {
            for (seq, batches) in table.frames {
                out.push((table.ident.clone(), seq, batches));
            }
        }
        out
    }

    /// Segment files currently in the demo.t table dir, sorted.
    fn seg_files(root: &Path) -> Vec<PathBuf> {
        let dir = root.join("demo.t");
        let mut out: Vec<PathBuf> = fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().and_then(|e| e.to_str()) == Some("seg"))
            .collect();
        out.sort();
        out
    }

    // Frames round-trip bytes-to-batch through a process "restart" (a new
    // LocalWal over the same dir), preserving table, seq, and row values.
    #[test]
    fn frame_roundtrip_through_replay() {
        let root = temp_root("roundtrip");
        let wal = LocalWal::open(&root).unwrap();
        assert_eq!(wal.append(&ident(), &[batch(&[1, 2])]).unwrap(), 1);
        assert_eq!(wal.append(&ident(), &[batch(&[3])]).unwrap(), 2);
        drop(wal);
        let wal2 = LocalWal::open(&root).unwrap();
        let frames = replay_frames(&wal2);
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[0].0, ident());
        assert_eq!(frames[0].1, 1);
        assert_eq!(ids(&frames[0].2[0]), vec![1, 2]);
        assert_eq!(frames[1].1, 2);
        assert_eq!(ids(&frames[1].2[0]), vec![3]);
        // Sequence numbering resumes above the recovered frames.
        assert_eq!(wal2.append(&ident(), &[batch(&[4])]).unwrap(), 3);
    }

    // A multi-batch statement is ONE frame (one seq, one fsync) and replay
    // returns its batches in order — statement-atomic by construction.
    #[test]
    fn statement_frame_holds_all_batches() {
        let root = temp_root("stmt-frame");
        let wal = LocalWal::open(&root).unwrap();
        let seq = wal
            .append(&ident(), &[batch(&[1]), batch(&[2, 3]), batch(&[4])])
            .unwrap();
        assert_eq!(seq, 1);
        drop(wal);
        let wal2 = LocalWal::open(&root).unwrap();
        let frames = replay_frames(&wal2);
        assert_eq!(frames.len(), 1, "3 batches = 1 statement = 1 frame");
        assert_eq!(frames[0].1, 1);
        let per_batch: Vec<Vec<i64>> = frames[0].2.iter().map(ids).collect();
        assert_eq!(per_batch, vec![vec![1], vec![2, 3], vec![4]]);
    }

    // Multi-segment replay: frames come back in seq order across a
    // rotation boundary.
    #[test]
    fn multi_segment_replay_preserves_seq_order() {
        let root = temp_root("order");
        let wal = LocalWal::open(&root).unwrap();
        wal.append(&ident(), &[batch(&[1])]).unwrap();
        wal.append(&ident(), &[batch(&[2])]).unwrap();
        wal.rotate(&ident()).unwrap();
        wal.append(&ident(), &[batch(&[3])]).unwrap();
        assert_eq!(seg_files(&root).len(), 2);
        drop(wal);
        let wal2 = LocalWal::open(&root).unwrap();
        let seqs: Vec<u64> = replay_frames(&wal2).iter().map(|f| f.1).collect();
        assert_eq!(seqs, vec![1, 2, 3]);
    }

    // A torn FINAL frame (power loss mid-append) recovers every earlier
    // frame and truncates the file so the next replay is clean.
    #[test]
    fn torn_final_frame_recovers_earlier_frames() {
        let root = temp_root("torn");
        let wal = LocalWal::open(&root).unwrap();
        wal.append(&ident(), &[batch(&[1])]).unwrap();
        wal.append(&ident(), &[batch(&[2])]).unwrap();
        wal.append(&ident(), &[batch(&[3])]).unwrap();
        drop(wal);
        // Tear the final frame: chop a few bytes off the segment.
        let seg = seg_files(&root).pop().unwrap();
        let len = fs::metadata(&seg).unwrap().len();
        let f = OpenOptions::new().write(true).open(&seg).unwrap();
        f.set_len(len - 5).unwrap();
        drop(f);
        let wal2 = LocalWal::open(&root).unwrap();
        let frames = replay_frames(&wal2);
        assert_eq!(frames.iter().map(|f| f.1).collect::<Vec<_>>(), vec![1, 2]);
        drop(wal2);
        // The tear was truncated away: a second replay is clean and equal.
        let wal3 = LocalWal::open(&root).unwrap();
        assert_eq!(replay_frames(&wal3).len(), 2);
    }

    // Corrupting a MIDDLE frame's payload stops replay at that point:
    // only the frames before it survive.
    #[test]
    fn corrupt_middle_frame_stops_replay_there() {
        let root = temp_root("corrupt");
        let wal = LocalWal::open(&root).unwrap();
        wal.append(&ident(), &[batch(&[1])]).unwrap();
        wal.append(&ident(), &[batch(&[2])]).unwrap();
        wal.append(&ident(), &[batch(&[3])]).unwrap();
        drop(wal);
        let seg = seg_files(&root).pop().unwrap();
        let mut data = fs::read(&seg).unwrap();
        // Frame 2 starts after frame 1 (8-byte header + payload); flip a
        // byte inside frame 2's payload so its crc no longer matches.
        let len1 = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
        let f2_payload = 8 + len1 + 8;
        data[f2_payload + 2] ^= 0xff;
        fs::write(&seg, &data).unwrap();
        let wal2 = LocalWal::open(&root).unwrap();
        let frames = replay_frames(&wal2);
        assert_eq!(frames.iter().map(|f| f.1).collect::<Vec<_>>(), vec![1]);
    }

    // Rotation + truncation: only segments fully covered by the watermark
    // are deleted; the active segment (newer appends) survives.
    #[test]
    fn truncate_deletes_only_covered_segments() {
        let root = temp_root("truncate");
        let wal = LocalWal::open(&root).unwrap();
        wal.append(&ident(), &[batch(&[1])]).unwrap();
        wal.append(&ident(), &[batch(&[2])]).unwrap();
        wal.rotate(&ident()).unwrap();
        wal.append(&ident(), &[batch(&[3])]).unwrap();
        assert_eq!(seg_files(&root).len(), 2);
        // Commit covered seqs 1..=2: the sealed segment goes, seq 3 stays.
        wal.truncate(&ident(), 2).unwrap();
        assert_eq!(seg_files(&root).len(), 1);
        drop(wal);
        let wal2 = LocalWal::open(&root).unwrap();
        let frames = replay_frames(&wal2);
        assert_eq!(frames.iter().map(|f| f.1).collect::<Vec<_>>(), vec![3]);
        // A truncation below the surviving frame's seq deletes nothing.
        wal2.truncate(&ident(), 2).unwrap();
        assert_eq!(seg_files(&root).len(), 1);
    }

    // Watermark filtering: frames at or below the committed watermark are
    // dropped (crash-after-commit-before-truncate double-apply guard);
    // no watermark = everything survives.
    #[test]
    fn watermark_filtering_drops_covered_seqs() {
        let frames = vec![
            (1, vec![batch(&[1])]),
            (2, vec![batch(&[2])]),
            (3, vec![batch(&[3])]),
        ];
        let (all, dropped) = drop_stale_frames(frames.clone(), None);
        assert_eq!((all.len(), dropped), (3, 0));
        let (survivors, dropped) = drop_stale_frames(frames.clone(), Some(2));
        assert_eq!(dropped, 2);
        assert_eq!(survivors.iter().map(|f| f.0).collect::<Vec<_>>(), vec![3]);
        let (none_left, dropped) = drop_stale_frames(frames, Some(3));
        assert_eq!((none_left.len(), dropped), (0, 3));
    }

    // FIX 1(a): a table dir that exists but holds NO segments (the shape a
    // full truncate leaves behind) still honors the sequence floor — the
    // next append starts AT the floor, not back at 1 under the watermark.
    #[test]
    fn seq_floor_applies_to_frameless_table_dir() {
        let root = temp_root("floor-empty");
        let wal = LocalWal::open(&root).unwrap();
        fs::create_dir_all(root.join("demo.t")).unwrap();
        // Replay reports the frameless dir (so the caller CAN floor it)...
        let tables = wal.replay().unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].ident, ident());
        assert!(tables[0].frames.is_empty());
        // ... and the floor takes effect on the next append.
        wal.ensure_seq_floor(&ident(), 11).unwrap();
        assert_eq!(wal.append(&ident(), &[batch(&[1])]).unwrap(), 11);
    }

    // FIX 1(b), the end-to-end shape of the seq-restart bug: append 1..3,
    // truncate all, reopen (a restart), floor from the persisted watermark;
    // new appends land ABOVE the watermark and replay filtered by it
    // returns exactly the new frames.
    #[test]
    fn seq_floor_survives_full_truncate_and_restart() {
        let root = temp_root("floor-restart");
        let wal = LocalWal::open(&root).unwrap();
        for v in 1..=3i64 {
            wal.append(&ident(), &[batch(&[v])]).unwrap();
        }
        wal.rotate(&ident()).unwrap();
        wal.truncate(&ident(), 3).unwrap(); // the flush committed 1..=3
        drop(wal);
        // "Restart": without the floor, next_seq would rewind to 1 (< the
        // watermark 3 persisted in table properties) and the new rows
        // would be dropped by the NEXT crash-replay.
        let wal2 = LocalWal::open(&root).unwrap();
        let tables = wal2.replay().unwrap();
        assert!(tables.iter().all(|t| t.frames.is_empty()));
        wal2.ensure_seq_floor(&ident(), 4).unwrap(); // watermark 3 + 1
        let seq = wal2.append(&ident(), &[batch(&[10])]).unwrap();
        assert!(seq >= 4, "post-restart seq {seq} must clear the watermark");
        drop(wal2);
        let wal3 = LocalWal::open(&root).unwrap();
        let frames: Vec<(u64, Vec<RecordBatch>)> = wal3
            .replay()
            .unwrap()
            .into_iter()
            .flat_map(|t| t.frames)
            .collect();
        let (survivors, dropped) = drop_stale_frames(frames, Some(3));
        assert_eq!(
            dropped, 0,
            "no post-restart frame may fall under the watermark"
        );
        assert_eq!(survivors.len(), 1);
        assert_eq!(survivors[0].0, seq);
        assert_eq!(ids(&survivors[0].1[0]), vec![10]);
    }

    // FIX 2: the rollback helper erases a failed append's partial bytes —
    // garbage behind the last good frame is truncated away and replay sees
    // only the good frames (nothing acked ever sat behind garbage).
    #[test]
    fn rollback_erases_partial_append_bytes() {
        let root = temp_root("rollback");
        let wal = LocalWal::open(&root).unwrap();
        wal.append(&ident(), &[batch(&[1])]).unwrap();
        wal.append(&ident(), &[batch(&[2])]).unwrap();
        drop(wal);
        let seg = seg_files(&root).pop().unwrap();
        let good_len = fs::metadata(&seg).unwrap().len();
        // A failed append's residue: partial garbage bytes at the end.
        let mut f = OpenOptions::new().append(true).open(&seg).unwrap();
        f.write_all(&[0xde, 0xad, 0xbe, 0xef, 0x42]).unwrap();
        f.sync_data().unwrap();
        roll_back_segment(&f, good_len).unwrap();
        drop(f);
        assert_eq!(fs::metadata(&seg).unwrap().len(), good_len);
        let wal2 = LocalWal::open(&root).unwrap();
        let frames = replay_frames(&wal2);
        assert_eq!(frames.iter().map(|f| f.1).collect::<Vec<_>>(), vec![1, 2]);
        // No truncation WARN path was needed: the bytes were already gone.
        assert_eq!(wal2.append(&ident(), &[batch(&[3])]).unwrap(), 3);
    }

    // FIX 2, poisoned path: sealing the active segment (the exact move the
    // poisoned-append arm makes, shared with rotate() via seal_active)
    // forces the next append into a FRESH segment, and replay returns the
    // frames of both segments in order.
    #[test]
    fn poisoned_seal_forces_fresh_segment() {
        let root = temp_root("poison");
        let wal = LocalWal::open(&root).unwrap();
        wal.append(&ident(), &[batch(&[1])]).unwrap();
        {
            // Same code path the poisoned-append arm takes: seal_active.
            let mut tables = wal.tables.lock().unwrap();
            let entry = tables.get_mut(&ident()).unwrap();
            seal_active(entry);
            assert!(entry.active.is_none(), "poisoned handle never reused");
        }
        wal.append(&ident(), &[batch(&[2])]).unwrap();
        assert_eq!(seg_files(&root).len(), 2, "second frame in a fresh segment");
        drop(wal);
        let wal2 = LocalWal::open(&root).unwrap();
        let frames = replay_frames(&wal2);
        assert_eq!(frames.iter().map(|f| f.1).collect::<Vec<_>>(), vec![1, 2]);
        assert_eq!(ids(&frames[0].2[0]), vec![1]);
        assert_eq!(ids(&frames[1].2[0]), vec![2]);
    }

    // FIX 4: the tail dir is single-writer — a second LocalWal on the same
    // dir (flock contends per open file description, so even in-process)
    // fails loudly; the lock releases when the first is dropped.
    #[test]
    fn second_open_on_same_dir_is_refused() {
        let root = temp_root("lock");
        let wal = LocalWal::open(&root).unwrap();
        let err = match LocalWal::open(&root) {
            Ok(_) => panic!("second open on a locked tail dir must fail"),
            Err(e) => e,
        };
        assert!(
            err.to_string().contains("LOCKED by another process"),
            "unexpected error: {err:#}"
        );
        drop(wal);
        LocalWal::open(&root).expect("lock released on drop");
    }

    // FIX 5: the tail identity is minted once and persists across reopens,
    // so the watermark property key never changes for a given tail dir.
    #[test]
    fn identity_persists_across_reopen() {
        let root = temp_root("identity");
        let wal = LocalWal::open(&root).unwrap();
        let key = wal.watermark_property().to_string();
        assert!(key.starts_with(TAIL_SEQ_PROPERTY_PREFIX));
        let id = key.strip_prefix(TAIL_SEQ_PROPERTY_PREFIX).unwrap();
        uuid::Uuid::parse_str(id).expect("identity is a uuid");
        drop(wal);
        let wal2 = LocalWal::open(&root).unwrap();
        assert_eq!(wal2.watermark_property(), key);
    }

    // FIX 5: the watermark sidecar round-trips through record + replay and
    // never regresses to a lower value.
    #[test]
    fn sidecar_watermark_roundtrip() {
        let root = temp_root("sidecar");
        let wal = LocalWal::open(&root).unwrap();
        wal.append(&ident(), &[batch(&[1])]).unwrap();
        wal.record_watermark(&ident(), 7).unwrap();
        wal.record_watermark(&ident(), 5).unwrap(); // lower: must not regress
        drop(wal);
        let wal2 = LocalWal::open(&root).unwrap();
        let tables = wal2.replay().unwrap();
        assert_eq!(tables.len(), 1);
        assert_eq!(tables[0].sidecar_watermark, Some(7));
    }

    // FIX 5: the replay watermark is max(property, sidecar); an
    // unparseable property WARNs and the sidecar carries the guard alone.
    #[test]
    fn effective_watermark_is_max_and_survives_garbled_property() {
        let t = ident();
        assert_eq!(effective_watermark(&t, None, None), None);
        assert_eq!(effective_watermark(&t, Some("9"), None), Some(9));
        assert_eq!(effective_watermark(&t, None, Some(4)), Some(4));
        // Property ahead of sidecar (crash before the sidecar write).
        assert_eq!(effective_watermark(&t, Some("9"), Some(4)), Some(9));
        // Sidecar ahead of property (foreign writer regressed/replaced it).
        assert_eq!(effective_watermark(&t, Some("2"), Some(6)), Some(6));
        // Garbled property: WARN (in parse_watermark_property) + sidecar.
        assert_eq!(effective_watermark(&t, Some("banana"), Some(6)), Some(6));
        assert_eq!(effective_watermark(&t, Some("banana"), None), None);
    }

    // FIX 7: dotted name components do not collide — ns=["a","b"]/table "c"
    // and ns=["a"]/table "b.c" map to distinct dirs, both round-tripping.
    // FIX (r3-3): `/` escapes like `%` and `.` (a raw slash would nest
    // directories) and NUL is rejected outright.
    #[test]
    fn table_dir_name_roundtrips_dotted_components() {
        let plain = TableIdent::from_strs(["a", "b", "c"]).unwrap();
        let dotted = TableIdent::from_strs(["a", "b.c"]).unwrap();
        let plain_dir = table_dir_name(&plain).unwrap();
        let dotted_dir = table_dir_name(&dotted).unwrap();
        assert_eq!(plain_dir, "a.b.c");
        assert_eq!(dotted_dir, "a.b%2ec");
        assert_ne!(plain_dir, dotted_dir);
        assert_eq!(parse_table_dir_name(&plain_dir), Some(plain));
        assert_eq!(parse_table_dir_name(&dotted_dir), Some(dotted));
        // Percent itself escapes and round-trips too.
        let pct = TableIdent::from_strs(["a", "b%2ec"]).unwrap();
        assert_eq!(
            parse_table_dir_name(&table_dir_name(&pct).unwrap()),
            Some(pct)
        );
        // A slash escapes (one flat dir entry, no nesting) and round-trips.
        let slashed = TableIdent::from_strs(["a", "x/y"]).unwrap();
        let slashed_dir = table_dir_name(&slashed).unwrap();
        assert_eq!(slashed_dir, "a.x%2fy");
        assert_eq!(parse_table_dir_name(&slashed_dir), Some(slashed));
        // NUL is an error, not a filesystem surprise.
        let nul = TableIdent::from_strs(["a", "b\0c"]).unwrap();
        let err = table_dir_name(&nul).unwrap_err();
        assert!(err.to_string().contains("NUL"), "unexpected: {err:#}");
    }

    // FIX (r3-2): a payload that would wrap the u32 length header is an
    // error (the statement fails) instead of an acked-but-unreplayable
    // frame. Boundary-tested through the factored check — no 4 GiB builds.
    #[test]
    fn frame_len_check_rejects_u32_overflow() {
        assert!(check_frame_len(0).is_ok());
        assert!(check_frame_len(u32::MAX as usize).is_ok());
        let err = check_frame_len(u32::MAX as usize + 1).unwrap_err();
        assert!(
            err.to_string().contains("frame-length limit"),
            "unexpected: {err:#}"
        );
    }

    // FIX (r3-4a): a poison marker clamps replay to the recorded good
    // length, dropping a crc-valid "ghost" frame the failing disk wrote
    // past it — that frame's sequence was reused after the failed append,
    // so replaying it would double-apply under a duplicate seq.
    #[test]
    fn poison_marker_clamps_ghost_frame() {
        let root = temp_root("poison-marker");
        let wal = LocalWal::open(&root).unwrap();
        wal.append(&ident(), &[batch(&[1])]).unwrap();
        wal.append(&ident(), &[batch(&[2])]).unwrap();
        drop(wal);
        let seg = seg_files(&root).pop().unwrap();
        let good_len = fs::metadata(&seg).unwrap().len();
        // The ghost: a fully crc-valid frame for seq 3, persisted by the
        // failing disk even though the append was reported failed (so the
        // statement was NOT acked and seq 3 was handed out again).
        let ghost = encode_frame(3, &[batch(&[99])]).unwrap();
        let mut f = OpenOptions::new().append(true).open(&seg).unwrap();
        f.write_all(&ghost).unwrap();
        drop(f);
        fs::write(poison_marker_path(&seg), good_len.to_string()).unwrap();
        let wal2 = LocalWal::open(&root).unwrap();
        let frames = replay_frames(&wal2);
        assert_eq!(
            frames.iter().map(|f| f.1).collect::<Vec<_>>(),
            vec![1, 2],
            "the ghost frame past the marker must not replay"
        );
        // The clamp became physical (file truncated, marker consumed) and
        // seq 3 is free again for the statement that really owns it.
        assert_eq!(fs::metadata(&seg).unwrap().len(), good_len);
        assert!(!poison_marker_path(&seg).exists());
        assert_eq!(wal2.append(&ident(), &[batch(&[3])]).unwrap(), 3);
    }

    // FIX (r3-4b): a later segment whose first frame is CONTIGUOUS with
    // the last good frame survives a bad frame before it — the bad frame's
    // sequence was reused by the fresh segment (the poisoned-append
    // shape), so there is no real hole and nothing may be deleted.
    #[test]
    fn contiguous_segments_survive_bad_frame() {
        let root = temp_root("contig");
        let wal = LocalWal::open(&root).unwrap();
        wal.append(&ident(), &[batch(&[1])]).unwrap();
        wal.append(&ident(), &[batch(&[2])]).unwrap();
        wal.rotate(&ident()).unwrap();
        wal.append(&ident(), &[batch(&[3])]).unwrap();
        wal.rotate(&ident()).unwrap();
        wal.append(&ident(), &[batch(&[4])]).unwrap();
        wal.append(&ident(), &[batch(&[5])]).unwrap();
        drop(wal);
        let segs = seg_files(&root);
        assert_eq!(segs.len(), 3);
        // Trailing garbage after frame 3 in seg2: a failed append's residue.
        let mut f = OpenOptions::new().append(true).open(&segs[1]).unwrap();
        f.write_all(&[0xde, 0xad, 0xbe, 0xef, 0x99]).unwrap();
        drop(f);
        let wal2 = LocalWal::open(&root).unwrap();
        let frames = replay_frames(&wal2);
        assert_eq!(
            frames.iter().map(|f| f.1).collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5],
            "contiguous seg3 must survive the garbage tail in seg2"
        );
        assert_eq!(
            seg_files(&root).len(),
            3,
            "nothing deleted; only seg2's garbage tail was truncated"
        );
    }

    // FIX (r3-4b) contrast: a later segment past a REAL sequence gap is
    // deleted with a WARN — replaying rows from beyond a hole would
    // reorder acked writes.
    #[test]
    fn gapped_segment_behind_bad_frame_is_deleted() {
        let root = temp_root("gap");
        let wal = LocalWal::open(&root).unwrap();
        wal.append(&ident(), &[batch(&[1])]).unwrap();
        wal.append(&ident(), &[batch(&[2])]).unwrap();
        wal.rotate(&ident()).unwrap();
        wal.append(&ident(), &[batch(&[3])]).unwrap();
        wal.rotate(&ident()).unwrap();
        // seg3 starts at seq 7: seqs 4-6 are a real hole behind seg2's
        // (about to be planted) bad frame.
        wal.ensure_seq_floor(&ident(), 7).unwrap();
        wal.append(&ident(), &[batch(&[7])]).unwrap();
        drop(wal);
        let segs = seg_files(&root);
        assert_eq!(segs.len(), 3);
        let mut f = OpenOptions::new().append(true).open(&segs[1]).unwrap();
        f.write_all(&[0xde, 0xad, 0xbe, 0xef, 0x99]).unwrap();
        drop(f);
        let wal2 = LocalWal::open(&root).unwrap();
        let frames = replay_frames(&wal2);
        assert_eq!(
            frames.iter().map(|f| f.1).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert_eq!(seg_files(&root).len(), 2, "the gapped segment is deleted");
    }
}
