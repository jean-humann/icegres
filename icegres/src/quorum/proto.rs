//! Quorum-tail wire protocol + consensus core types.
//!
//! Adapted from neondatabase/neon safekeeper (Apache-2.0); substantially
//! modified for icegres's generic tail log. The consensus types
//! ([`TermLsn`], [`TermHistory`], [`TermHistory::up_to`],
//! [`TermHistory::last_log_term`], [`find_highest_common_point`]) follow
//! `neon/safekeeper/src/safekeeper.rs` closely — the comments note where.
//!
//! # Wire format
//!
//! One message = `[u32 len][u32 crc32(rest)][rest]` where
//! `rest = [u32 header_len][JSON header][raw payload]` (little-endian
//! integers). The JSON header carries a `"type"` tag plus the message's
//! scalar fields; the payload is raw record bytes (append/read traffic
//! only). The crc covers header AND payload, so a torn/garbled message is
//! detected before any field is trusted.
//!
//! # The record log
//!
//! The replicated log is a byte stream of records; an LSN is a byte offset
//! into it, and every record advances the LSN by its framed byte length —
//! identically on every acceptor, because the bytes ARE the log. One record
//! = `crate::segment::frame_bytes` of
//! `[u64 lsn][u8 kind][u32 key_len][table_key][u64 seq][body]`:
//!
//! * kind [`RECORD_FRAME`]: one acked tail statement; `body` is the shared
//!   statement-atomic op payload (`tail::encode_op_payload` — format byte +
//!   op discriminator + Arrow IPC stream), so quorum frames stay
//!   interchangeable with the other backends'.
//! * kind [`RECORD_WATERMARK`]: a committed-watermark note (`seq`, empty
//!   body) — the quorum replacement for LocalWal's sidecar file.
//!
//! The embedded `lsn` makes every record self-describing on replay and lets
//! the acceptor verify stream continuity on every append.

use std::time::Duration;

use anyhow::{anyhow, bail, Context as _, Result};
use serde_json::Value;
use tokio::io::{AsyncReadExt as _, AsyncWriteExt as _};
use tokio::net::TcpStream;

use crate::segment::{frame_bytes, scan_frame_bytes, FRAME_HEADER_BYTES, LOG_KIND_LOG};

/// Hard cap on one wire message (header + payload). Appends batch multiple
/// records but never beyond this; a peer announcing a bigger message is
/// treated as corrupt/foreign traffic.
pub(crate) const MAX_MESSAGE_BYTES: usize = 256 << 20;

/// Marker the acceptor embeds in its wrong-tail-id greeting refusal and the
/// proposer matches to classify the failure as PERMANENT for its run (FIX
/// I5): errors cross the wire as strings, so the marker is the contract.
pub(crate) const WRONG_CLUSTER_MARK: &str = "wrong cluster";

/// Record kind: one acked tail statement (body = the shared op payload).
pub(crate) const RECORD_FRAME: u8 = 0;
/// Record kind: a committed-watermark note for one table (empty body).
pub(crate) const RECORD_WATERMARK: u8 = 1;

// ---------------------------------------------------------------------------
// Consensus core types (adapted from neon safekeeper.rs)
// ---------------------------------------------------------------------------

/// `(term, lsn)` — the LSN at which `term` began. The unit of
/// [`TermHistory`]. (neon safekeeper.rs `TermLsn`.)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct TermLsn {
    pub term: u64,
    pub lsn: u64,
}

/// The sequence of term switches this log has adopted, ordered by term and
/// lsn (both strictly increase). NOTE: an acceptor's stored history is
/// adopted IN FULL from the proposer and often extends BEYOND its local
/// log, so the effective history/last_log_term must always be computed via
/// [`up_to`](Self::up_to) with the local flush position — never from the
/// raw last entry. (neon safekeeper.rs `TermHistory`.)
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct TermHistory(pub Vec<TermLsn>);

impl TermHistory {
    /// Entries whose start lsn is `<= lsn` — the switches that have
    /// actually happened within a log flushed up to `lsn` (strictly-after
    /// switches dropped; neon safekeeper.rs:100-109).
    pub fn up_to(&self, lsn: u64) -> TermHistory {
        TermHistory(self.0.iter().copied().filter(|e| e.lsn <= lsn).collect())
    }

    /// The term the log's LAST record belongs to (neon's "epoch",
    /// safekeeper.rs:202-210): the last history entry at or below
    /// `flush_lsn`, 0 for an empty log. The acceptor never explicitly
    /// switches — adopting the proposer's history plants
    /// `(new_term, term_start_lsn)` and this flips automatically the moment
    /// the flush position reaches it.
    pub fn last_log_term(&self, flush_lsn: u64) -> u64 {
        self.up_to(flush_lsn).0.last().map(|e| e.term).unwrap_or(0)
    }

    pub fn to_json(&self) -> Value {
        Value::Array(
            self.0
                .iter()
                .map(|e| serde_json::json!([e.term, e.lsn]))
                .collect(),
        )
    }

    pub fn from_json(v: &Value) -> Result<TermHistory> {
        let arr = v
            .as_array()
            .ok_or_else(|| anyhow!("term_history is not an array"))?;
        let mut out = Vec::with_capacity(arr.len());
        for e in arr {
            let pair = e
                .as_array()
                .filter(|p| p.len() == 2)
                .ok_or_else(|| anyhow!("term_history entry is not a [term, lsn] pair"))?;
            let term = pair[0]
                .as_u64()
                .ok_or_else(|| anyhow!("term_history term is not a u64"))?;
            let lsn = pair[1]
                .as_u64()
                .ok_or_else(|| anyhow!("term_history lsn is not a u64"))?;
            out.push(TermLsn { term, lsn });
        }
        Ok(TermHistory(out))
    }
}

/// Where the proposer's history and an acceptor's history diverge: the
/// highest point common to both. The proposer's history conceptually
/// extends to +infinity (it is the authority for its terms); the
/// acceptor's ends at its flush position `sk_wal_end` — that asymmetry is
/// the point. `None` = no common term at all (stream from the beginning).
/// (Adapted from neon safekeeper.rs `TermHistory::find_highest_common_point`,
/// :115-167.)
pub(crate) fn find_highest_common_point(
    prop_th: &TermHistory,
    sk_th: &TermHistory,
    sk_wal_end: u64,
) -> Result<Option<TermLsn>> {
    if let Some(last) = sk_th.0.last() {
        if last.lsn > sk_wal_end {
            bail!(
                "acceptor term history end {} is beyond its flush position {sk_wal_end}",
                last.lsn
            );
        }
    }
    let mut last_common_idx: Option<usize> = None;
    for i in 0..prop_th.0.len().min(sk_th.0.len()) {
        if prop_th.0[i].term != sk_th.0[i].term {
            break;
        }
        // Term histories are propagated by the proposer that owns the term,
        // so a common term always has a common start lsn.
        if prop_th.0[i].lsn != sk_th.0[i].lsn {
            bail!(
                "term histories disagree on the start of term {}: {} vs {}",
                prop_th.0[i].term,
                prop_th.0[i].lsn,
                sk_th.0[i].lsn
            );
        }
        last_common_idx = Some(i);
    }
    let Some(i) = last_common_idx else {
        return Ok(None);
    };
    // End of the common term: the start of the next entry, or the end of
    // the history — +infinity for the proposer, flush position for the
    // acceptor.
    if i == prop_th.0.len() - 1 {
        return Ok(Some(TermLsn {
            term: prop_th.0[i].term,
            lsn: sk_wal_end,
        }));
    }
    let prop_end = prop_th.0[i + 1].lsn;
    let sk_end = if i + 1 < sk_th.0.len() {
        sk_th.0[i + 1].lsn
    } else {
        sk_wal_end
    };
    Ok(Some(TermLsn {
        term: prop_th.0[i].term,
        lsn: prop_end.min(sk_end),
    }))
}

// ---------------------------------------------------------------------------
// Records
// ---------------------------------------------------------------------------

/// One decoded log record (see the module docs for the byte layout).
#[derive(Debug, Clone)]
pub(crate) struct Record {
    /// Start LSN — must equal the record's byte position in the log.
    pub lsn: u64,
    /// [`RECORD_FRAME`] or [`RECORD_WATERMARK`].
    pub kind: u8,
    /// The canonical percent-encoded `<ns>.<table>` key
    /// (`tail::table_dir_name`).
    pub table_key: String,
    /// The per-table tail sequence number this record carries/covers.
    pub seq: u64,
    /// Frame records: the shared op payload. Watermark records: empty.
    pub body: Vec<u8>,
}

impl Record {
    /// The framed on-wire/on-disk bytes; `self.lsn + bytes.len()` is the
    /// record's end LSN.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let key = self.table_key.as_bytes();
        let mut p = Vec::with_capacity(8 + 1 + 4 + key.len() + 8 + self.body.len());
        p.extend_from_slice(&self.lsn.to_le_bytes());
        p.push(self.kind);
        p.extend_from_slice(
            &(u32::try_from(key.len()).context("table key over u32 bytes")?).to_le_bytes(),
        );
        p.extend_from_slice(key);
        p.extend_from_slice(&self.seq.to_le_bytes());
        p.extend_from_slice(&self.body);
        frame_bytes(&p, LOG_KIND_LOG)
    }
}

/// Decode one record's frame PAYLOAD (the bytes inside the `[len][crc]`
/// wrap).
pub(crate) fn decode_record_payload(p: &[u8]) -> Result<Record> {
    if p.len() < 8 + 1 + 4 {
        bail!(
            "record payload of {} bytes is shorter than its header",
            p.len()
        );
    }
    let lsn = u64::from_le_bytes(p[0..8].try_into().expect("8 bytes"));
    let kind = p[8];
    if kind != RECORD_FRAME && kind != RECORD_WATERMARK {
        bail!("unknown record kind {kind}");
    }
    let key_len = u32::from_le_bytes(p[9..13].try_into().expect("4 bytes")) as usize;
    if p.len() < 13 + key_len + 8 {
        bail!("record payload is torn inside its table key/seq");
    }
    let table_key = std::str::from_utf8(&p[13..13 + key_len])
        .context("record table key is not UTF-8")?
        .to_string();
    let seq = u64::from_le_bytes(
        p[13 + key_len..13 + key_len + 8]
            .try_into()
            .expect("8 bytes"),
    );
    let body = p[13 + key_len + 8..].to_vec();
    Ok(Record {
        lsn,
        kind,
        table_key,
        seq,
        body,
    })
}

/// STRICT decode of a concatenated record stream starting at LSN
/// `expect_start`: every frame must be valid, every record's embedded LSN
/// must equal its running byte position, and the stream must end exactly at
/// a record boundary. Used for in-flight message payloads — unlike the
/// on-disk boot scan, a bad frame here is a protocol error, never a
/// tolerated torn write.
pub(crate) fn decode_records(bytes: &[u8], expect_start: u64) -> Result<Vec<Record>> {
    let scan = scan_frame_bytes(bytes);
    if let Some(bad) = scan.bad {
        bail!("record stream is invalid at byte {}: {bad}", scan.good_end);
    }
    let mut out = Vec::with_capacity(scan.payloads.len());
    let mut pos = expect_start;
    for range in scan.payloads {
        let frame_len = (range.len() + FRAME_HEADER_BYTES) as u64;
        let rec = decode_record_payload(&bytes[range])?;
        if rec.lsn != pos {
            bail!(
                "record stream discontinuity: record claims lsn {} at stream position {pos}",
                rec.lsn
            );
        }
        pos += frame_len;
        out.push(rec);
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Messages
// ---------------------------------------------------------------------------

/// One protocol message (either direction). The shape follows neon's
/// proposer–acceptor protocol (`ProposerGreeting`/`VoteRequest`/
/// `ProposerElected`/`AppendRequest` and their responses), stripped of
/// everything Postgres and with JSON headers instead of the binary codec.
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum Message {
    /// Proposer -> acceptor, first message on every connection. With
    /// `tail_id = None` it only queries; with `Some` the acceptor adopts
    /// the id permanently if fresh and refuses a mismatch (wrong-cluster
    /// guard).
    Greeting {
        tail_id: Option<String>,
    },
    GreetingResp {
        tail_id: Option<String>,
        term: u64,
        flush_lsn: u64,
    },
    VoteRequest {
        term: u64,
    },
    /// A refusal (`granted = false`) still carries the acceptor's
    /// positions — the proposer uses them for the per-acceptor handshake.
    VoteResponse {
        term: u64,
        granted: bool,
        flush_lsn: u64,
        /// `term_history.up_to(flush_lsn).last().term` (0 if empty) —
        /// derivable from the fields below; carried for clarity.
        last_log_term: u64,
        /// Already truncated to `flush_lsn` by the acceptor.
        term_history: TermHistory,
        horizon_lsn: u64,
        commit_lsn: u64,
    },
    Elected {
        term: u64,
        /// Where the proposer will start streaming to THIS acceptor — must
        /// equal the divergence point the acceptor computes itself.
        start_lsn: u64,
        term_history: TermHistory,
    },
    /// `ok = false` carries the acceptor's higher term (stale proposer).
    ElectedResp {
        term: u64,
        ok: bool,
    },
    /// `records` = concatenated framed records `[begin_lsn, end_lsn)`.
    Append {
        term: u64,
        begin_lsn: u64,
        end_lsn: u64,
        commit_lsn: u64,
        horizon_lsn: u64,
        records: Vec<u8>,
    },
    /// `ok = false` (with `term` > the proposer's) is the FENCE: a newer
    /// proposer owns the log now (flush/commit are 0 in that case).
    AppendResp {
        term: u64,
        ok: bool,
        flush_lsn: u64,
        commit_lsn: u64,
    },
    /// Recovery/replay read of `[from_lsn, to_lsn)` (both record
    /// boundaries).
    Read {
        from_lsn: u64,
        to_lsn: u64,
    },
    ReadResp {
        from_lsn: u64,
        records: Vec<u8>,
    },
    /// A refusal/failure the peer reports instead of a normal response.
    Error {
        message: String,
    },
}

impl Message {
    fn header_and_payload(&self) -> (Value, &[u8]) {
        match self {
            Message::Greeting { tail_id } => (
                serde_json::json!({"type": "greeting", "tail_id": tail_id}),
                &[],
            ),
            Message::GreetingResp {
                tail_id,
                term,
                flush_lsn,
            } => (
                serde_json::json!({"type": "greeting_resp", "tail_id": tail_id,
                                   "term": term, "flush_lsn": flush_lsn}),
                &[],
            ),
            Message::VoteRequest { term } => {
                (serde_json::json!({"type": "vote_req", "term": term}), &[])
            }
            Message::VoteResponse {
                term,
                granted,
                flush_lsn,
                last_log_term,
                term_history,
                horizon_lsn,
                commit_lsn,
            } => (
                serde_json::json!({"type": "vote_resp", "term": term, "granted": granted,
                                   "flush_lsn": flush_lsn, "last_log_term": last_log_term,
                                   "term_history": term_history.to_json(),
                                   "horizon_lsn": horizon_lsn, "commit_lsn": commit_lsn}),
                &[],
            ),
            Message::Elected {
                term,
                start_lsn,
                term_history,
            } => (
                serde_json::json!({"type": "elected", "term": term, "start_lsn": start_lsn,
                                   "term_history": term_history.to_json()}),
                &[],
            ),
            Message::ElectedResp { term, ok } => (
                serde_json::json!({"type": "elected_resp", "term": term, "ok": ok}),
                &[],
            ),
            Message::Append {
                term,
                begin_lsn,
                end_lsn,
                commit_lsn,
                horizon_lsn,
                records,
            } => (
                serde_json::json!({"type": "append", "term": term, "begin_lsn": begin_lsn,
                                   "end_lsn": end_lsn, "commit_lsn": commit_lsn,
                                   "horizon_lsn": horizon_lsn}),
                records.as_slice(),
            ),
            Message::AppendResp {
                term,
                ok,
                flush_lsn,
                commit_lsn,
            } => (
                serde_json::json!({"type": "append_resp", "term": term, "ok": ok,
                                   "flush_lsn": flush_lsn, "commit_lsn": commit_lsn}),
                &[],
            ),
            Message::Read { from_lsn, to_lsn } => (
                serde_json::json!({"type": "read", "from_lsn": from_lsn, "to_lsn": to_lsn}),
                &[],
            ),
            Message::ReadResp { from_lsn, records } => (
                serde_json::json!({"type": "read_resp", "from_lsn": from_lsn}),
                records.as_slice(),
            ),
            Message::Error { message } => (
                serde_json::json!({"type": "error", "message": message}),
                &[],
            ),
        }
    }

    /// The full on-wire bytes: `[u32 len][u32 crc][u32 header_len][header]
    /// [payload]`.
    pub fn encode(&self) -> Result<Vec<u8>> {
        let (header, payload) = self.header_and_payload();
        let header = serde_json::to_vec(&header).context("cannot encode message header")?;
        let rest_len = 4 + header.len() + payload.len();
        if rest_len > MAX_MESSAGE_BYTES {
            bail!(
                "quorum message of {rest_len} bytes exceeds the {MAX_MESSAGE_BYTES}-byte cap; \
                 split the statement into smaller inserts"
            );
        }
        let mut rest = Vec::with_capacity(rest_len);
        rest.extend_from_slice(&(header.len() as u32).to_le_bytes());
        rest.extend_from_slice(&header);
        rest.extend_from_slice(payload);
        let mut out = Vec::with_capacity(8 + rest.len());
        out.extend_from_slice(&(rest.len() as u32).to_le_bytes());
        out.extend_from_slice(&crc32fast::hash(&rest).to_le_bytes());
        out.extend_from_slice(&rest);
        Ok(out)
    }

    /// Decode the `rest` part (crc already verified by the reader).
    fn decode_rest(rest: &[u8]) -> Result<Message> {
        if rest.len() < 4 {
            bail!("quorum message shorter than its header-length field");
        }
        let header_len = u32::from_le_bytes(rest[0..4].try_into().expect("4 bytes")) as usize;
        if rest.len() - 4 < header_len {
            bail!("quorum message header length {header_len} overruns the message");
        }
        let header: Value = serde_json::from_slice(&rest[4..4 + header_len])
            .context("quorum message header is not valid JSON")?;
        let payload = &rest[4 + header_len..];
        let ty = header
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("quorum message header has no type"))?;
        let msg = match ty {
            "greeting" => Message::Greeting {
                tail_id: jstr_opt(&header, "tail_id"),
            },
            "greeting_resp" => Message::GreetingResp {
                tail_id: jstr_opt(&header, "tail_id"),
                term: ju64(&header, "term")?,
                flush_lsn: ju64(&header, "flush_lsn")?,
            },
            "vote_req" => Message::VoteRequest {
                term: ju64(&header, "term")?,
            },
            "vote_resp" => Message::VoteResponse {
                term: ju64(&header, "term")?,
                granted: jbool(&header, "granted")?,
                flush_lsn: ju64(&header, "flush_lsn")?,
                last_log_term: ju64(&header, "last_log_term")?,
                term_history: jhistory(&header)?,
                horizon_lsn: ju64(&header, "horizon_lsn")?,
                commit_lsn: ju64(&header, "commit_lsn")?,
            },
            "elected" => Message::Elected {
                term: ju64(&header, "term")?,
                start_lsn: ju64(&header, "start_lsn")?,
                term_history: jhistory(&header)?,
            },
            "elected_resp" => Message::ElectedResp {
                term: ju64(&header, "term")?,
                ok: jbool(&header, "ok")?,
            },
            "append" => Message::Append {
                term: ju64(&header, "term")?,
                begin_lsn: ju64(&header, "begin_lsn")?,
                end_lsn: ju64(&header, "end_lsn")?,
                commit_lsn: ju64(&header, "commit_lsn")?,
                horizon_lsn: ju64(&header, "horizon_lsn")?,
                records: payload.to_vec(),
            },
            "append_resp" => Message::AppendResp {
                term: ju64(&header, "term")?,
                ok: jbool(&header, "ok")?,
                flush_lsn: ju64(&header, "flush_lsn")?,
                commit_lsn: ju64(&header, "commit_lsn")?,
            },
            "read" => Message::Read {
                from_lsn: ju64(&header, "from_lsn")?,
                to_lsn: ju64(&header, "to_lsn")?,
            },
            "read_resp" => Message::ReadResp {
                from_lsn: ju64(&header, "from_lsn")?,
                records: payload.to_vec(),
            },
            "error" => Message::Error {
                message: header
                    .get("message")
                    .and_then(Value::as_str)
                    .unwrap_or("(no message)")
                    .to_string(),
            },
            other => bail!("unknown quorum message type {other:?}"),
        };
        Ok(msg)
    }
}

fn ju64(v: &Value, key: &str) -> Result<u64> {
    v.get(key)
        .and_then(Value::as_u64)
        .ok_or_else(|| anyhow!("quorum message header field {key:?} missing or not a u64"))
}

fn jbool(v: &Value, key: &str) -> Result<bool> {
    v.get(key)
        .and_then(Value::as_bool)
        .ok_or_else(|| anyhow!("quorum message header field {key:?} missing or not a bool"))
}

fn jstr_opt(v: &Value, key: &str) -> Option<String> {
    v.get(key).and_then(Value::as_str).map(str::to_string)
}

fn jhistory(v: &Value) -> Result<TermHistory> {
    TermHistory::from_json(
        v.get("term_history")
            .ok_or_else(|| anyhow!("quorum message header has no term_history"))?,
    )
}

/// Write one message to an async stream.
pub(crate) async fn write_message<W>(w: &mut W, msg: &Message) -> Result<()>
where
    W: tokio::io::AsyncWrite + Unpin,
{
    let bytes = msg.encode()?;
    w.write_all(&bytes).await.context("quorum write failed")?;
    w.flush().await.context("quorum flush failed")?;
    Ok(())
}

/// Read one message from an async stream (crc-verified).
pub(crate) async fn read_message<R>(r: &mut R) -> Result<Message>
where
    R: tokio::io::AsyncRead + Unpin,
{
    let mut hdr = [0u8; 8];
    r.read_exact(&mut hdr).await.context("quorum read failed")?;
    let len = u32::from_le_bytes(hdr[0..4].try_into().expect("4 bytes")) as usize;
    let crc = u32::from_le_bytes(hdr[4..8].try_into().expect("4 bytes"));
    if !(4..=MAX_MESSAGE_BYTES).contains(&len) {
        bail!("implausible quorum message length {len}");
    }
    let mut rest = vec![0u8; len];
    r.read_exact(&mut rest)
        .await
        .context("quorum read failed mid-message")?;
    if crc32fast::hash(&rest) != crc {
        bail!("quorum message crc mismatch (torn or foreign traffic)");
    }
    Message::decode_rest(&rest)
}

/// Typed marker for a [`Conn::call_timeout`] expiry, so callers can tell a
/// SILENT acceptor (treat as unavailable, continue with the others) from a
/// refusal or transport error (FIX I2). Reachable through anyhow's
/// `downcast_ref` from any context chain built on it.
#[derive(Debug)]
pub(crate) struct CallTimedOut(pub Duration);

impl std::fmt::Display for CallTimedOut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "acceptor did not respond within {:?} (treating it as unavailable)",
            self.0
        )
    }
}

impl std::error::Error for CallTimedOut {}

/// One proposer-side connection to an acceptor: strict request/response.
pub(crate) struct Conn {
    stream: TcpStream,
}

impl Conn {
    pub async fn connect(addr: &str, timeout: Duration) -> Result<Conn> {
        let stream = tokio::time::timeout(timeout, TcpStream::connect(addr))
            .await
            .map_err(|_| anyhow!("connect to acceptor {addr} timed out"))?
            .with_context(|| format!("cannot connect to acceptor {addr}"))?;
        let _ = stream.set_nodelay(true);
        Ok(Conn { stream })
    }

    /// Send a request and read its response; a peer [`Message::Error`]
    /// becomes an `Err` here.
    pub async fn call(&mut self, msg: &Message) -> Result<Message> {
        write_message(&mut self.stream, msg).await?;
        match read_message(&mut self.stream).await? {
            Message::Error { message } => bail!("acceptor refused: {message}"),
            resp => Ok(resp),
        }
    }

    /// [`Self::call`] bounded by `dur` (FIX I2): a connected-but-SILENT
    /// acceptor must never hang the sequential open()/handshake() call
    /// chain forever. On expiry the error is a [`CallTimedOut`] and the
    /// connection must be dropped (a late response would desynchronize the
    /// request/response framing).
    pub async fn call_timeout(&mut self, msg: &Message, dur: Duration) -> Result<Message> {
        match tokio::time::timeout(dur, self.call(msg)).await {
            Ok(res) => res,
            Err(_) => Err(anyhow::Error::new(CallTimedOut(dur))),
        }
    }
}

// ---------------------------------------------------------------------------
// Unit tests — message roundtrips, record framing, term-history algebra
// (the find_highest_common_point cases mirror neon's unit tests).
// ---------------------------------------------------------------------------
#[cfg(test)]
mod tests {
    use super::*;

    fn th(entries: &[(u64, u64)]) -> TermHistory {
        TermHistory(
            entries
                .iter()
                .map(|&(term, lsn)| TermLsn { term, lsn })
                .collect(),
        )
    }

    fn roundtrip(msg: Message) {
        let bytes = msg.encode().unwrap();
        let len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        assert_eq!(len + 8, bytes.len());
        let decoded = Message::decode_rest(&bytes[8..]).unwrap();
        assert_eq!(decoded, msg);
    }

    #[test]
    fn message_roundtrips() {
        roundtrip(Message::Greeting { tail_id: None });
        roundtrip(Message::Greeting {
            tail_id: Some("id-1".into()),
        });
        roundtrip(Message::GreetingResp {
            tail_id: Some("id-1".into()),
            term: 3,
            flush_lsn: 99,
        });
        roundtrip(Message::VoteRequest { term: 7 });
        roundtrip(Message::VoteResponse {
            term: 7,
            granted: true,
            flush_lsn: 120,
            last_log_term: 6,
            term_history: th(&[(1, 0), (6, 40)]),
            horizon_lsn: 10,
            commit_lsn: 100,
        });
        roundtrip(Message::Elected {
            term: 8,
            start_lsn: 120,
            term_history: th(&[(1, 0), (6, 40), (8, 120)]),
        });
        roundtrip(Message::ElectedResp { term: 8, ok: true });
        roundtrip(Message::Append {
            term: 8,
            begin_lsn: 120,
            end_lsn: 150,
            commit_lsn: 100,
            horizon_lsn: 10,
            records: vec![1, 2, 3, 4],
        });
        roundtrip(Message::AppendResp {
            term: 8,
            ok: true,
            flush_lsn: 150,
            commit_lsn: 120,
        });
        roundtrip(Message::Read {
            from_lsn: 10,
            to_lsn: 120,
        });
        roundtrip(Message::ReadResp {
            from_lsn: 10,
            records: vec![9, 9],
        });
        roundtrip(Message::Error {
            message: "nope".into(),
        });
    }

    #[test]
    fn message_crc_detects_corruption() {
        let mut bytes = Message::VoteRequest { term: 7 }.encode().unwrap();
        let last = bytes.len() - 1;
        bytes[last] ^= 0xff;
        let len = u32::from_le_bytes(bytes[0..4].try_into().unwrap()) as usize;
        let crc = u32::from_le_bytes(bytes[4..8].try_into().unwrap());
        assert_eq!(len + 8, bytes.len());
        assert_ne!(
            crc32fast::hash(&bytes[8..]),
            crc,
            "corruption must break the crc"
        );
    }

    fn rec(lsn: u64, kind: u8, key: &str, seq: u64, body: &[u8]) -> Record {
        Record {
            lsn,
            kind,
            table_key: key.to_string(),
            seq,
            body: body.to_vec(),
        }
    }

    #[test]
    fn record_stream_roundtrip() {
        let r1 = rec(100, RECORD_FRAME, "demo.t", 1, b"payload-1");
        let f1 = r1.encode().unwrap();
        let r2 = rec(100 + f1.len() as u64, RECORD_WATERMARK, "demo.u", 5, b"");
        let f2 = r2.encode().unwrap();
        let mut stream = f1.clone();
        stream.extend_from_slice(&f2);
        let out = decode_records(&stream, 100).unwrap();
        assert_eq!(out.len(), 2);
        assert_eq!(out[0].lsn, 100);
        assert_eq!(out[0].kind, RECORD_FRAME);
        assert_eq!(out[0].table_key, "demo.t");
        assert_eq!(out[0].seq, 1);
        assert_eq!(out[0].body, b"payload-1");
        assert_eq!(out[1].lsn, 100 + f1.len() as u64);
        assert_eq!(out[1].kind, RECORD_WATERMARK);
        assert_eq!(out[1].seq, 5);
        assert!(out[1].body.is_empty());
    }

    #[test]
    fn record_stream_is_strict() {
        let r1 = rec(0, RECORD_FRAME, "demo.t", 1, b"x");
        let mut stream = r1.encode().unwrap();
        // Wrong expected start = discontinuity.
        assert!(decode_records(&stream, 5).is_err());
        // A torn tail is a protocol error on the wire (unlike the disk scan).
        stream.pop();
        assert!(decode_records(&stream, 0).is_err());
        // Corrupt a payload byte: crc catches it.
        let mut corrupt = r1.encode().unwrap();
        let last = corrupt.len() - 1;
        corrupt[last] ^= 0xff;
        assert!(decode_records(&corrupt, 0).is_err());
    }

    // The four find_highest_common_point shapes, mirroring neon's
    // test_find_highest_common_point_{none,middle,sk_end,walprop}.
    #[test]
    fn fhcp_no_common_term() {
        let prop = th(&[(4, 10)]);
        let sk = th(&[(2, 20)]);
        assert_eq!(find_highest_common_point(&prop, &sk, 40).unwrap(), None);
    }

    #[test]
    fn fhcp_diverges_mid_history() {
        // Common prefix [1,2]; divergence at term 2's end: proposer's term-2
        // ends at 25 (next entry 25? no: min of prop next start and sk next
        // start): prop switches to 4 at 40, sk switches to 3 at 30 => the
        // common term 2 ends at min(40, 30) = 30.
        let prop = th(&[(1, 0), (2, 10), (4, 40)]);
        let sk = th(&[(1, 0), (2, 10), (3, 30)]);
        assert_eq!(
            find_highest_common_point(&prop, &sk, 50).unwrap(),
            Some(TermLsn { term: 2, lsn: 30 })
        );
    }

    #[test]
    fn fhcp_common_prefix_ends_inside_sk_last_term() {
        // sk's history ends inside term 2 (its flush = 32); proposer moved
        // on to term 4 at 40 => common point (2, min(40, 32)) = (2, 32).
        let prop = th(&[(1, 0), (2, 10), (4, 40)]);
        let sk = th(&[(1, 0), (2, 10)]);
        assert_eq!(
            find_highest_common_point(&prop, &sk, 32).unwrap(),
            Some(TermLsn { term: 2, lsn: 32 })
        );
    }

    #[test]
    fn fhcp_identical_histories() {
        // Proposer's history ends at +infinity, sk's at its flush.
        let prop = th(&[(1, 0), (2, 10)]);
        let sk = th(&[(1, 0), (2, 10)]);
        assert_eq!(
            find_highest_common_point(&prop, &sk, 32).unwrap(),
            Some(TermLsn { term: 2, lsn: 32 })
        );
    }

    #[test]
    fn fhcp_rejects_history_beyond_flush() {
        let prop = th(&[(1, 0)]);
        let sk = th(&[(1, 50)]);
        assert!(find_highest_common_point(&prop, &sk, 40).is_err());
    }

    #[test]
    fn last_log_term_switches_at_term_start() {
        // Adopted history goes beyond the local log: last_log_term must be
        // computed through up_to(flush), flipping only when flush reaches
        // the new term's start (the implicit epoch switch).
        let h = th(&[(1, 0), (2, 30)]);
        assert_eq!(h.last_log_term(0), 1);
        assert_eq!(h.last_log_term(29), 1);
        assert_eq!(h.last_log_term(30), 2);
        assert_eq!(h.last_log_term(31), 2);
        assert_eq!(TermHistory::default().last_log_term(10), 0);
    }
}
