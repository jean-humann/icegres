//! Shared low-level durable-log machinery, factored (move-only where
//! possible) out of `tail.rs` so the quorum acceptor
//! (`src/quorum/acceptor.rs`, compiled into both the `icegres` and
//! `icekeeperd` binaries) reuses the exact frame/atomic-write code the local
//! tail already proved through its adversarial reviews:
//!
//! * the `[u32 len][u32 crc32(payload)][payload]` frame wrap
//!   ([`frame_bytes`]) and its torn-write-tolerant walker
//!   ([`scan_frame_bytes`]) — payload-agnostic here; each caller layers its
//!   own payload format (seq + op for the local tail, LSN-headed records for
//!   the quorum log) on top;
//! * durable small-file replacement ([`write_atomic`]: tmp file, fsync,
//!   rename, dir fsync) — also exactly the control-file persistence
//!   discipline of neon's safekeeper (`control_file.rs`), which the quorum
//!   acceptor's JSON control file follows;
//! * directory fsync ([`sync_dir`]) and the exclusive one-writer directory
//!   flock ([`lock_dir_exclusive`]).

use std::fs::{self, File, OpenOptions};
use std::io::Write as _;
use std::os::unix::io::AsRawFd as _;
use std::path::Path;

use anyhow::{anyhow, bail, Context as _, Result};

/// Bytes of one frame's `[u32 len][u32 crc]` header.
pub(crate) const FRAME_HEADER_BYTES: usize = 8;

/// The word naming the log in operator-visible messages. The local tail
/// (`--tail-dir`) keeps its ORIGINAL pre-factoring texts verbatim ("tail
/// frame payload", "cannot fsync tail dir", ...); the quorum acceptor says
/// "log". Every message-producing helper here takes one of these so the
/// factoring never drifts an operator-visible string again.
pub(crate) const LOG_KIND_TAIL: &str = "tail";
/// See [`LOG_KIND_TAIL`].
pub(crate) const LOG_KIND_LOG: &str = "log";

/// A frame's length header is a `u32`: a payload longer than `u32::MAX`
/// would silently WRAP the header, producing an acked frame that can never
/// replay (the scan reads a garbage length). Error instead, so the
/// oversized statement fails loudly before any ack.
pub(crate) fn check_frame_len(len: usize, log_kind: &str) -> Result<()> {
    if len > u32::MAX as usize {
        bail!(
            "{log_kind} frame payload is {len} bytes, over the u32 frame-length limit \
             ({}); split the statement into smaller inserts",
            u32::MAX
        );
    }
    Ok(())
}

/// Wrap `payload` in the shared frame format:
/// `[u32 len][u32 crc32(payload)][payload]` (little-endian headers).
pub(crate) fn frame_bytes(payload: &[u8], log_kind: &str) -> Result<Vec<u8>> {
    check_frame_len(payload.len(), log_kind)?;
    let mut frame = Vec::with_capacity(FRAME_HEADER_BYTES + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(&crc32fast::hash(payload).to_le_bytes());
    frame.extend_from_slice(payload);
    Ok(frame)
}

/// Result of walking a buffer of concatenated frames ([`scan_frame_bytes`]).
pub(crate) struct RawFrameScan {
    /// Byte range of each valid frame's PAYLOAD inside the scanned buffer,
    /// in order. The frame itself starts [`FRAME_HEADER_BYTES`] earlier.
    pub payloads: Vec<std::ops::Range<usize>>,
    /// End offset of the last valid frame (0 = none) — the truncation
    /// target when the buffer ends in garbage.
    pub good_end: usize,
    /// Why the walk stopped early (`None` = the whole buffer was valid
    /// frames). A torn/corrupt tail is the caller's WARN + truncate, never
    /// an error here.
    pub bad: Option<String>,
}

/// Walk `data` as concatenated `[len][crc][payload]` frames, stopping at the
/// first invalid one (torn header, torn payload, crc mismatch) — the shared
/// torn-write-tolerant scan both log formats build their replay on.
pub(crate) fn scan_frame_bytes(data: &[u8]) -> RawFrameScan {
    let mut payloads: Vec<std::ops::Range<usize>> = Vec::new();
    let mut off: usize = 0;
    let mut good_end: usize = 0;
    let mut bad: Option<String> = None;
    while off < data.len() {
        if data.len() - off < FRAME_HEADER_BYTES {
            bad = Some(format!("torn header ({} trailing bytes)", data.len() - off));
            break;
        }
        let len = u32::from_le_bytes(data[off..off + 4].try_into().expect("4 bytes")) as usize;
        let crc = u32::from_le_bytes(data[off + 4..off + 8].try_into().expect("4 bytes"));
        if data.len() - off - FRAME_HEADER_BYTES < len {
            bad = Some(format!(
                "torn payload (frame wants {len} bytes, {} present)",
                data.len() - off - FRAME_HEADER_BYTES
            ));
            break;
        }
        let start = off + FRAME_HEADER_BYTES;
        if crc32fast::hash(&data[start..start + len]) != crc {
            bad = Some("crc mismatch".to_string());
            break;
        }
        payloads.push(start..start + len);
        off = start + len;
        good_end = off;
    }
    RawFrameScan {
        payloads,
        good_end,
        bad,
    }
}

/// Durably write `bytes` to `path` (inside `dir`): tmp file + fsync +
/// rename + dir fsync, so a crash leaves either the old content or the new,
/// never a torn file.
pub(crate) fn write_atomic(dir: &Path, path: &Path, bytes: &[u8], log_kind: &str) -> Result<()> {
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
    sync_dir(dir, log_kind)
}

/// fsync a directory so freshly created/removed entries survive power loss.
pub(crate) fn sync_dir(dir: &Path, log_kind: &str) -> Result<()> {
    File::open(dir)
        .and_then(|d| d.sync_all())
        .with_context(|| format!("cannot fsync {log_kind} dir {}", dir.display()))
}

/// Take an exclusive advisory lock on `<root>/<lock_name>`. flock is per
/// open file description, so even a second open in THIS process contends.
/// The fd must stay open for the lock to hold — the caller keeps it for the
/// process lifetime. `contended_msg` is the error shown when another
/// process already holds the lock.
pub(crate) fn lock_dir_exclusive(
    root: &Path,
    lock_name: &str,
    log_kind: &str,
    contended_msg: &str,
) -> Result<File> {
    let path = root.join(lock_name);
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(&path)
        .with_context(|| format!("cannot open {log_kind} lock file {}", path.display()))?;
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc != 0 {
        let err = std::io::Error::last_os_error();
        if err.raw_os_error() == Some(libc::EWOULDBLOCK) {
            bail!("{contended_msg}");
        }
        return Err(anyhow!(err).context(format!("cannot flock {log_kind} dir {}", root.display())));
    }
    Ok(file)
}
