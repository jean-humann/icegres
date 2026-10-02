# Pinned Iceberg notification correction

`iceberg-0.9.1/` is the complete Apache Iceberg Rust 0.9.1 crate archive, with
its LICENSE, NOTICE, tests and resources retained. The source archive is
[iceberg 0.9.1 on crates.io](https://crates.io/api/v1/crates/iceberg/0.9.1/download).
Its SHA-256 matches the previously locked crates.io checksum:

```text
4d9c3fc1f55c84ff64645c0d2ee35159f5574d33f729fc0860783bc247c8b8c5
```

The application uses this crate through Cargo's local `[patch.crates-io]`
override. The Iceberg version and the pinned DataFusion/Arrow/toolchain matrix
remain unchanged. The local correction is recorded in
[iceberg-notify.patch](iceberg-notify.patch). Only these upstream source files
change:

- `src/delete_file_index.rs`
- `src/arrow/delete_filter.rs`
- `src/arrow/caching_delete_file_loader.rs`

The original code inspects loading state under a lock, releases that lock, then
creates a `Notify::notified()` future. Completion in the gap sends
`notify_waiters()` before a future exists, leaving that scan waiting forever.
The correction creates an owned notification future while holding the state
lock. This captures the notification generation before a publisher can replace
the loading state. The positional-delete loader carries the prepared future
across its action boundary instead of carrying only a notifier.

Deterministic `lost_wakeup_` tests publish completion after the loading-state
check but before the returned future is polled, covering the delete-file index,
equality deletes and positional deletes. The tests use a no-op callback in the
production path to control that boundary; they do not add sleeps or retries to
production.

This is a source-level race correction. Attribution of an observed query stall
and comparative performance claims still require the controlled workload to
complete on the corrected binary. No catalog, row format or delete semantics
are intentionally changed.

Run the dependency regressions explicitly from `icegres/` (Cargo's application
suite does not run dependency unit tests):

```sh
cargo test --release --locked --manifest-path vendor/iceberg-0.9.1/Cargo.toml --lib delete_file_index
cargo test --release --locked --manifest-path vendor/iceberg-0.9.1/Cargo.toml --lib delete_filter
```

These standalone dependency tests use the archive's unchanged upstream test
lockfile. Application builds continue to use `icegres/Cargo.lock`. The new
interleaving tests passed with the patch and each timed out in an isolated
source copy restored to the original notification ordering. A negative-control
copy must use a separate target directory or explicitly force a rebuild before
returning to fixed-source tests: identical package names/versions can reuse the
same test artifact when multiple workspaces share one target directory.
