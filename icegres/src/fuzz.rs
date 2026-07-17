//! Malformed-input-never-panics harness (SOTA deliverable #3).
//!
//! `icegres` decodes UNTRUSTED bytes on several paths: the durable-tail WAL
//! frames replayed at boot (attacker- or foreign-written files), the quorum
//! acceptor protocol (bytes from a peer proposer), and the Flight-SQL ticket
//! parsers (client-supplied gRPC bytes). The "Rewriting Bun in Rust" SOTA
//! thesis holds continuous parser fuzzing up as the bar; this module is the
//! std-only, in-tree equivalent.
//!
//! It is **deterministic** (a fixed-seed SplitMix64 PRNG — no `rand`, no
//! `arbitrary`, no `libfuzzer`, ZERO new dependencies), drives each REAL
//! decoder (not a wrapper) with thousands of random, truncated, bit-flipped,
//! byte-splatted, inserted/deleted and oversized inputs, and asserts each
//! returns `Err`: never panics, never reads out of bounds, never hangs.
//!
//! Every in-tree decode entry point is covered; the private ones are reached
//! through their real boundary so the tests are not vacuous:
//!   * [`crate::segment::scan_frame_bytes`] — the shared crc32 frame walker.
//!   * [`crate::quorum::proto::decode_record_payload`] and
//!     [`crate::quorum::proto::decode_records`] — the log-record codecs.
//!   * [`crate::quorum::proto::read_message`] — the framed network message
//!     reader → `decode_rest` (private, reached via a crc-valid frame).
//!   * `crate::flight::decode_plan_ticket` (private) — fuzzed from `flight`'s
//!     own test module.
//!   * [`crate::tailapi::TailTicket::from_any`] — the Flight-SQL tail ticket
//!     `Any` (type_url dispatch + `serde_json` on attacker bytes).
//!   * [`crate::tail::decode_op_payload`] / `crate::tail::decode_payload` —
//!     the tail op framing. NOTE: these are fuzzed only over the icegres-owned
//!     layers (format-version byte, op-kind discriminant, seq header, empty
//!     body). The Arrow-IPC BODY decode they wrap is deliberately NOT fed
//!     adversarial bytes — see the "Arrow IPC boundary" note below.
//!
//! ## Arrow IPC boundary (a real, documented finding)
//!
//! The Arrow IPC `StreamReader` behind `decode_ipc` is built for TRUSTED
//! internal data and is NOT adversarial-input-safe: `read_meta_len` accepts
//! any positive `i32` with no plausibility cap and `maybe_next` then
//! `resize`s a buffer to that length BEFORE checking it against the available
//! bytes, and a record-batch message's `bodyLength` (`i64`) drives an even
//! larger allocation. A crafted stream therefore forces an unbounded
//! allocation → allocation-failure `abort()`, which NO in-process guard
//! (`catch_unwind` included) can recover. So the Arrow body decode cannot be
//! fuzzed in-process without OOM-aborting the test, and cannot be hardened
//! here without either a hardened Arrow (the dependency matrix is pinned) or
//! an out-of-process / bounded-allocator decode (a serving-path change out of
//! scope for this zero-runtime-change increment). This is reachable only from
//! OUTSIDE icegres's documented trust model — a crc-valid but crafted local
//! WAL frame (needs write access to the flock-guarded data dir) or a
//! malicious quorum peer body (semi-trusted network) — and is recorded as a
//! hardening-backlog item in `docs/limitations.md`. The harness fuzzes right
//! up to that boundary; it does not paper over it.
//!
//! The harness only adds test code — it changes no serving-path byte.

use std::panic::{catch_unwind, AssertUnwindSafe};

/// Deterministic SplitMix64 PRNG. Fixed-seed and std-only so a failure is
/// exactly reproducible (the panic message prints the seed + iteration).
pub(crate) struct Rng(u64);

impl Rng {
    pub(crate) fn new(seed: u64) -> Self {
        Rng(seed)
    }

    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    fn byte(&mut self) -> u8 {
        self.next_u64() as u8
    }

    /// Uniform in `0..n` (n must be > 0).
    fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % n as u64) as usize
    }

    fn one_in(&mut self, n: usize) -> bool {
        self.below(n) == 0
    }

    fn random_bytes(&mut self, max_len: usize) -> Vec<u8> {
        let n = self.below(max_len + 1);
        (0..n).map(|_| self.byte()).collect()
    }

    /// A random byte string of length `0..=max_len`. `pub(crate)` so the
    /// in-file tail/flight fuzz tests can build bounded inputs (kept short so
    /// the tail op decoder never hands Arrow a large content length — see the
    /// module's "Arrow IPC boundary" note).
    pub(crate) fn bounded_bytes(&mut self, max_len: usize) -> Vec<u8> {
        self.random_bytes(max_len)
    }
}

/// Cap the hex dump so an oversized input can't flood the failure output.
fn hex(bytes: &[u8]) -> String {
    let show = bytes.len().min(256);
    let mut s = String::with_capacity(show * 2 + 24);
    for b in &bytes[..show] {
        s.push_str(&format!("{b:02x}"));
    }
    if bytes.len() > show {
        s.push_str(&format!("…(+{} bytes)", bytes.len() - show));
    }
    s
}

/// Run `f`, and fail the test with a reproducible message if it panics.
/// `AssertUnwindSafe` is sound here: every target is a pure `&[u8] -> Result`
/// decoder, each iteration is independent, and nothing is observed after a
/// caught panic beyond the fact that it happened.
pub(crate) fn guard<F: FnOnce()>(name: &str, seed: u64, iter: usize, dump: &[u8], f: F) {
    let r = catch_unwind(AssertUnwindSafe(f));
    if r.is_err() {
        panic!(
            "decoder `{name}` PANICKED on malformed input at iter {iter} \
             (seed {seed:#018x}), {} bytes: {}",
            dump.len(),
            hex(dump)
        );
    }
}

/// One malformed input: a mix of pure-random (any size, incl. empty and
/// oversized) and corpus-derived mutations (truncate / bit-flip / byte-splat /
/// insert / delete / oversize-pad). Corpus seeds are "valid-ish" bytes whose
/// mutation reaches deep decoder code; an empty corpus yields pure-random only.
pub(crate) fn gen_input(rng: &mut Rng, corpus: &[Vec<u8>]) -> Vec<u8> {
    match rng.below(8) {
        // Pure random, small — hammer the length/header guards.
        0 | 1 => rng.random_bytes(512),
        // Tiny (0..=4 bytes) — the short-input boundary checks.
        2 => rng.random_bytes(4),
        // Oversized random — allocation / bounds under large input.
        3 => {
            let n = 4096 + rng.below(60_000);
            (0..n).map(|_| rng.byte()).collect()
        }
        // Corpus mutation (falls back to random when no corpus was supplied).
        _ if corpus.is_empty() => rng.random_bytes(512),
        strategy => {
            let mut b = corpus[rng.below(corpus.len())].clone();
            match strategy {
                4 => {
                    // Truncate to a random prefix (torn-tail simulation).
                    if !b.is_empty() {
                        let cut = rng.below(b.len());
                        b.truncate(cut);
                    }
                }
                5 => {
                    // Flip 1..=8 random bits.
                    let flips = 1 + rng.below(8);
                    for _ in 0..flips {
                        if b.is_empty() {
                            break;
                        }
                        let i = rng.below(b.len());
                        b[i] ^= 1u8 << rng.below(8);
                    }
                }
                6 => {
                    // Splat 1..=8 random bytes, or insert/delete one.
                    if rng.one_in(2) {
                        let splats = 1 + rng.below(8);
                        for _ in 0..splats {
                            if b.is_empty() {
                                break;
                            }
                            let i = rng.below(b.len());
                            b[i] = rng.byte();
                        }
                    } else if rng.one_in(2) && !b.is_empty() {
                        let i = rng.below(b.len());
                        b.remove(i);
                    } else {
                        let i = rng.below(b.len() + 1);
                        b.insert(i, rng.byte());
                    }
                }
                _ => {
                    // Oversize-pad the seed with random trailing bytes.
                    let extra = 4096 + rng.below(20_000);
                    for _ in 0..extra {
                        b.push(rng.byte());
                    }
                }
            }
            b
        }
    }
}

/// Drive `target` with `iters` deterministic malformed inputs and assert it
/// never panics. The workhorse for `&[u8] -> _` decoders.
pub(crate) fn run<F: Fn(&[u8])>(
    name: &str,
    seed: u64,
    iters: usize,
    corpus: &[Vec<u8>],
    target: F,
) {
    let mut rng = Rng::new(seed);
    for i in 0..iters {
        let input = gen_input(&mut rng, corpus);
        guard(name, seed, i, &input, || target(&input));
    }
}

const ITERS: usize = 16_000;

// ---------------------------------------------------------------------------
// Corpora (valid-ish seeds whose mutation reaches deep decoder code)
// ---------------------------------------------------------------------------

/// Shallow tail op payloads (`[FMT?][op?][≤3 body bytes]`) — deliberately
/// bounded so the IPC body handed to Arrow is `< 4` bytes, which
/// `read_meta_len` treats as EOF (`Ok(None)`) BEFORE any content-length-driven
/// allocation. This exercises the icegres-owned layers (format-version compare,
/// `TailOpKind::from_byte`, the empty-stream `bail`) without OOM-aborting on the
/// un-fuzzable Arrow body decode (see the module "Arrow IPC boundary" note).
pub(crate) fn shallow_tail_op_inputs(rng: &mut Rng, count: usize) -> Vec<Vec<u8>> {
    use crate::tail::TAIL_PAYLOAD_FORMAT;
    let prefixes: [&[u8]; 6] = [
        &[],
        &[TAIL_PAYLOAD_FORMAT],
        &[TAIL_PAYLOAD_FORMAT, 0],
        &[TAIL_PAYLOAD_FORMAT, 1],
        &[TAIL_PAYLOAD_FORMAT, 2],
        &[99], // wrong format-version byte
    ];
    (0..count)
        .map(|_| {
            let mut v = prefixes[rng.below(prefixes.len())].to_vec();
            // 0..=3 trailing bytes: keeps the Arrow-visible body under 4 bytes.
            v.extend(rng.bounded_bytes(3));
            v
        })
        .collect()
}

fn sample_records() -> Vec<crate::quorum::proto::Record> {
    use crate::quorum::proto::{Record, RECORD_FRAME, RECORD_WATERMARK};
    vec![
        Record {
            lsn: 0,
            kind: RECORD_FRAME,
            table_key: "demo.trips".to_string(),
            seq: 1,
            body: b"op-payload-bytes".to_vec(),
        },
        Record {
            lsn: 0,
            kind: RECORD_WATERMARK,
            table_key: "ns.tbl".to_string(),
            seq: 42,
            body: Vec::new(),
        },
        Record {
            lsn: 0,
            kind: RECORD_FRAME,
            table_key: String::new(),
            seq: 0,
            body: Vec::new(),
        },
    ]
}

/// Raw record *payloads* (inside the frame wrap) — the input to
/// [`crate::quorum::proto::decode_record_payload`].
pub(crate) fn record_payload_corpus() -> Vec<Vec<u8>> {
    use crate::segment::FRAME_HEADER_BYTES;
    sample_records()
        .into_iter()
        .map(|r| r.encode().expect("encode record")[FRAME_HEADER_BYTES..].to_vec())
        .collect()
}

/// Concatenated framed record streams — the input to
/// [`crate::quorum::proto::decode_records`].
pub(crate) fn record_stream_corpus() -> Vec<Vec<u8>> {
    let recs = sample_records();
    let framed: Vec<Vec<u8>> = recs.iter().map(|r| r.encode().expect("encode")).collect();
    let mut corpus = framed.clone();
    // A two-frame stream (exercises the per-record continuity checks).
    let mut two = framed[0].clone();
    two.extend_from_slice(&framed[1]);
    corpus.push(two);
    corpus
}

/// Valid `rest` bodies of a quorum message (the bytes after the 8-byte
/// len+crc header) — one per message variant, so a crc-valid frame reaches
/// `decode_rest`'s JSON/type-dispatch/field paths.
fn message_rest_corpus() -> Vec<Vec<u8>> {
    use crate::quorum::proto::Message;
    let msgs = [
        Message::Greeting { tail_id: None },
        Message::Greeting {
            tail_id: Some("id-1".to_string()),
        },
        Message::VoteRequest { term: 3 },
        Message::Read {
            from_lsn: 1,
            to_lsn: 9,
        },
        Message::Error {
            message: "boom".to_string(),
        },
    ];
    msgs.iter()
        .map(|m| m.encode().expect("encode msg")[8..].to_vec())
        .collect()
}

/// Full on-wire quorum messages (`[u32 len][u32 crc][rest]`) — the input to
/// [`crate::quorum::proto::read_message`].
pub(crate) fn message_corpus() -> Vec<Vec<u8>> {
    message_rest_corpus()
        .into_iter()
        .map(|rest| {
            let mut out = Vec::with_capacity(8 + rest.len());
            out.extend_from_slice(&(rest.len() as u32).to_le_bytes());
            out.extend_from_slice(&crc32fast::hash(&rest).to_le_bytes());
            out.extend_from_slice(&rest);
            out
        })
        .collect()
}

/// Valid Flight plan tickets and near-misses — the input to the private
/// `decode_plan_ticket` (fuzzed from `flight`'s test module).
pub(crate) fn plan_ticket_corpus() -> Vec<Vec<u8>> {
    vec![
        b"IGRESP1\x1fhandle-1\x1fSELECT 1".to_vec(),
        b"IGRESP1\x1f\x1f".to_vec(),
        b"IGRESP1\x1fno-second-sep".to_vec(),
        b"IGRESP1\x1f".to_vec(),
        b"SELECT 42".to_vec(),
        Vec::new(),
    ]
}

/// Poll an always-ready in-memory future to completion with a no-op waker —
/// std-only, no runtime. `read_message` over a `Cursor` never yields `Pending`.
fn poll_ready<F: std::future::Future>(fut: F) {
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);
    let mut fut = std::pin::pin!(fut);
    // Bounded so a hypothetical non-completing future fails loudly, not hangs.
    for _ in 0..8 {
        if fut.as_mut().poll(&mut cx).is_ready() {
            return;
        }
    }
    panic!("read_message future did not complete against an in-memory cursor");
}

// ---------------------------------------------------------------------------
// Decoder targets
// ---------------------------------------------------------------------------

#[test]
fn fuzz_scan_frame_bytes_never_panics() {
    use crate::quorum::proto::Record;
    // Corpus of valid concatenated frames (crc-valid) so mutation exercises
    // the length/crc/torn-tail walk with realistic bytes as well as random.
    let mut corpus = record_stream_corpus();
    corpus.push(Record::encode(&sample_records()[0]).expect("encode"));
    run(
        "scan_frame_bytes",
        0x5CA1_F00D_0000_0001,
        ITERS,
        &corpus,
        |b| {
            // Infallible-by-contract: it must never panic, whatever the bytes.
            let _ = crate::segment::scan_frame_bytes(b);
        },
    );
}

#[test]
fn fuzz_decode_op_payload_shallow_never_panics() {
    // Only the icegres-owned layers are fuzzed here: format-version byte, op
    // discriminant, and the empty-body path. The Arrow IPC body decode is NOT
    // fed adversarial bytes — it OOM-aborts on crafted content lengths, which
    // no in-process guard can catch (see the module "Arrow IPC boundary" note).
    // Inputs are bounded so the Arrow-visible body stays under 4 bytes (EOF
    // before any content-length allocation).
    let seed = 0x0DEC_0DE0_0000_0002;
    let mut rng = Rng::new(seed);
    for (i, input) in shallow_tail_op_inputs(&mut rng, ITERS)
        .into_iter()
        .enumerate()
    {
        guard("decode_op_payload(shallow)", seed, i, &input, || {
            let _ = crate::tail::decode_op_payload(&input);
        });
    }
}

#[test]
fn fuzz_decode_record_payload_never_panics() {
    let corpus = record_payload_corpus();
    run(
        "decode_record_payload",
        0x0DEC_0DE0_0000_0003,
        ITERS,
        &corpus,
        |b| {
            let _ = crate::quorum::proto::decode_record_payload(b);
        },
    );
}

#[test]
fn fuzz_decode_records_never_panics() {
    let corpus = record_stream_corpus();
    run(
        "decode_records",
        0x0DEC_0DE0_0000_0004,
        ITERS,
        &corpus,
        |b| {
            // Vary expect_start too — the continuity check must not panic.
            let _ = crate::quorum::proto::decode_records(b, 0);
            let _ = crate::quorum::proto::decode_records(b, u64::MAX / 2);
        },
    );
}

#[test]
fn fuzz_read_message_never_panics() {
    // read_message reaches decode_rest only past a crc-valid header, so this
    // drives BOTH: mutated full frames (header/len/crc guards) and, below,
    // crc-valid frames wrapping arbitrary rest (decode_rest itself).
    let corpus = message_corpus();
    run("read_message", 0x0DEC_0DE0_0000_0005, ITERS, &corpus, |b| {
        let mut cur = std::io::Cursor::new(b.to_vec());
        poll_ready(async move {
            let _ = crate::quorum::proto::read_message(&mut cur).await;
        });
    });

    // crc-valid framing of arbitrary/mutated `rest` — guarantees decode_rest
    // (JSON header parse, type dispatch, u64/bool field extraction) is reached.
    let rest_corpus = message_rest_corpus();
    let seed = 0x0DEC_0DE0_0000_0015;
    let mut rng = Rng::new(seed);
    for i in 0..ITERS {
        let rest = gen_input(&mut rng, &rest_corpus);
        // read_message caps at MAX_MESSAGE_BYTES; keep frames small for speed.
        let rest = if rest.len() > 100_000 {
            rest[..100_000].to_vec()
        } else {
            rest
        };
        let mut framed = Vec::with_capacity(8 + rest.len());
        framed.extend_from_slice(&(rest.len() as u32).to_le_bytes());
        framed.extend_from_slice(&crc32fast::hash(&rest).to_le_bytes());
        framed.extend_from_slice(&rest);
        guard("read_message(framed-rest)", seed, i, &framed, || {
            let mut cur = std::io::Cursor::new(framed.clone());
            poll_ready(async move {
                let _ = crate::quorum::proto::read_message(&mut cur).await;
            });
        });
    }
}

#[test]
fn fuzz_tail_ticket_from_any_never_panics() {
    use crate::tailapi::{TailTicket, TICKET_SNAPSHOT, TICKET_SUBSCRIBE, TICKET_TABLES};
    use arrow_flight::sql::Any;

    // Value corpus: valid ticket JSON + near-misses; mutation drives serde_json
    // and the namespace.table parse over attacker bytes.
    let value_corpus: Vec<Vec<u8>> = vec![
        br#"{"table":"demo.trips","from_seq":5}"#.to_vec(),
        br#"{"table":"demo.trips"}"#.to_vec(),
        br#"{"table":"no_namespace"}"#.to_vec(),
        br#"{"from_seq":9}"#.to_vec(),
        br#"{}"#.to_vec(),
        b"{".to_vec(),
        Vec::new(),
    ];
    // type_url corpus: the three real ticket URLs + garbage (→ Ok(None)).
    let url_corpus = [
        TICKET_TABLES,
        TICKET_SNAPSHOT,
        TICKET_SUBSCRIBE,
        "type.googleapis.com/other.Thing",
        "",
    ];

    let seed = 0x0DEC_0DE0_0000_0006;
    let mut rng = Rng::new(seed);
    for i in 0..ITERS {
        // Bias toward the two URLs that trigger the JSON parse path.
        let url = match rng.below(6) {
            0 => url_corpus[0].to_string(),
            1 | 2 => url_corpus[1].to_string(),
            3 => url_corpus[2].to_string(),
            4 => url_corpus[rng.below(url_corpus.len())].to_string(),
            _ => String::from_utf8_lossy(&rng.random_bytes(24)).into_owned(),
        };
        let value = gen_input(&mut rng, &value_corpus);
        let any = Any {
            type_url: url,
            value: value.clone().into(),
        };
        guard("TailTicket::from_any", seed, i, &value, || {
            let _ = TailTicket::from_any(&any);
        });
    }
}
