# Scope — write-path compression + Flight transport tuning

A performance audit (Arrow / Flight SQL / ADBC / Iceberg, mid-2026) found the
serving hot path already optimal — streaming `DoGet`, plan reuse, projection +
predicate pushdown, IO-tuned scans — but flagged two locally-fixable leaks. This
increment closes both. Everything deeper (page-index pruning, selective-DML file
pruning, deletion vectors, parallel scan planning) is bounded by iceberg-rust
0.9.1 and the coupled arrow/DataFusion pin and is out of scope here.

## 1. Compress written Parquet (was: UNCOMPRESSED)

Both data-file writers (DML inserts and `maintain compact`) built
`WriterProperties::default()`, which the parquet crate leaves
`Compression::UNCOMPRESSED`. The scan path is bytes/IO-bound (~1 ms object GET
per file), so uncompressed files inflate the dominant read cost and storage.

`data_file_write_properties(table)` now honors the Iceberg table properties
`write.parquet.compression-codec` (`zstd` / `snappy` / `gzip` / `lz4` /
`uncompressed`) and `write.parquet.compression-level`, defaulting to **zstd** —
the modern Iceberg default — when unset. A malformed level falls back to the
codec default rather than failing the write. Statistics and dictionary encoding
keep their defaults (row-group pruning unaffected).

## 2. Compress Flight IPC + tune HTTP/2 transport

- **IPC body compression.** Flight result streams now ZSTD-compress the Arrow
  buffers via `IpcWriteOptions` on the encoder. Applied at the Arrow IPC layer
  (not gRPC-level) so buffers stay independently decodable and there is no
  double-compression. Enabled by the arrow `ipc_compression` feature, which
  reuses the `zstd`/`lz4_flex` crates already in the graph — no new dependency.
  Metadata one-batch responses stay uncompressed (not worth the CPU).
- **HTTP/2 flow control + keepalive.** Every Flight listener now uses an
  adaptive HTTP/2 window so a large columnar `DoGet` grows past hyper's 64 KB
  default stream window (which otherwise throttles the stream to one window per
  round trip over any non-loopback RTT), plus HTTP/2 and TCP keepalives to
  survive long streams through load balancers.

## Invariants

- **Zero protocol change.** Arrow IPC compression and HTTP/2 flow control are
  negotiated/transparent to conformant Flight/ADBC clients; results are
  byte-identical after decode. gRPC message ceilings and `TCP_NODELAY` unchanged.
- **No dependency/version bump.** Only the `ipc_compression` feature is added to
  the already-pinned `arrow`; the matrix (`rust-toolchain.toml`, crate versions)
  is untouched.
- Correctness proven by the existing e2e Flight round-trip + tail-durability
  suites; effect proven by a drift-controlled bench A/B recorded in the
  scorecard.
