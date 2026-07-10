//! Quorum-replicated durable tail (`--tail-quorum`, roadmap §3 backend 3):
//! tail frames are durable when a quorum (2 of 3) of lightweight acceptor
//! processes (`icekeeperd`) have fsynced them. The consensus protocol —
//! terms, single-shot voting, term-history reconciliation, divergence
//! truncation, the Raft commit rule — is Neon SafeKeeper's proposer–acceptor
//! algorithm, adapted with attribution (see NOTICE and the per-file
//! headers). Postgres-WAL framing is replaced by icegres's own crc-framed
//! record log (`crate::segment`).
//!
//! Layout:
//! * [`proto`] — wire protocol messages + record framing + the consensus
//!   core types (`TermHistory`, divergence-point computation);
//! * [`acceptor`] — the acceptor state machine and its storage (control
//!   file + LSN-named log segments), plus the tokio serve loop `icekeeperd`
//!   runs;
//! * [`proposer`] — the proposer: election, donor selection, recovery,
//!   per-acceptor catch-up streaming, quorum commit tracking, and the
//!   per-table horizon bookkeeping the `TailStore` surface maps onto.
//!
//! This module tree is also compiled into the standalone `icekeeperd`
//! binary via `#[path]` includes (`src/bin/icekeeperd.rs`), so nothing here
//! may depend on the arrow/iceberg/datafusion stack — the `TailStore` glue
//! lives outside, in `src/tail_quorum.rs`.

// The acceptor is exercised by the `icekeeperd` crate root and by the
// in-process integration tests; in the `icegres` binary's non-test build
// it is dead code by design (the proposer talks to acceptor PROCESSES).
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) mod acceptor;
pub(crate) mod proposer;
pub(crate) mod proto;
