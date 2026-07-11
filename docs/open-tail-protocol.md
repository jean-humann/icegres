# The icegres open tail protocol (version 1)

The open specification of roadmap-v2 P1: how ANY Arrow Flight client — a
peer icegres compute, Spark, DuckDB, a ten-line pyarrow script — reads the
un-flushed tail window of a buffering icegres server and merges it with
committed Iceberg state into an exactly-once *merged-fresh* view. This is
the read primitive LTAP-style systems reserve for their own engines,
served here as a documented wire protocol.

Reference implementations: server `icegres/src/tailapi.rs` (+ the DoGet
fallback in `flight.rs`), Rust consumer `icegres/src/peer.rs`
(`--peer-tail`), Python consumer `bench/clients/p1_tail_reader.py`.

## Who serves it

The **buffering compute** serves it — never the tail backends. Only the
`icegres serve` process holds the overlay state (pending appends, keyed
ops, retained flushed generations, each with its durable-tail sequence),
which makes the protocol identical across `--tail-dir`, `--tail-url`, and
`--tail-quorum`.

* `icegres serve --write-buffer-ms N --tail-dir D --tail-api-port P`
  serves the API on gRPC port `P` (opt-in; off = no listener, byte-
  identical server). Requires buffered mode AND a durable tail — the
  protocol is keyed by tail sequences.
* The listener is a full Arrow Flight SQL endpoint in **read-only** mode
  (writes are rejected so they cannot bypass the pgwire ordering fences);
  plain SQL `SELECT`s on it are union reads.
* `icegres flight-serve` recognizes the same tickets but answers
  `FAILED_PRECONDITION` — it never buffers, so its SQL reads are already
  exactly as fresh as committed state.
* Auth: the standard Flight basic-auth handshake when `--auth-file` is
  set (`authorization: Basic ...` → per-boot `Bearer` token); `--authz-file`
  gates Snapshot/Subscribe as `ReadData` on the table. v1 is plaintext —
  run it on a trusted network.

## Fallback contract (honesty)

Best-effort, read-side only. If the buffering compute dies, consumers fall
back to **commit-cadence freshness**: the rows themselves are tail-durable
and replay on the next boot/takeover of the **same tail identity** (the
same tail dir/database/quorum log — re-minting the identity abandons the
old un-flushed frames) — durability is never at stake, only the freshness
bonus. Consumers must treat any stream error as "drop the mirror,
re-snapshot when the server returns", and must not serve a mirror whose
stream has gone silent (see the serving age bound under Consumers). The
deployment model stays single-buffering-writer-per-table; the protocol
does not add any cross-compute write coordination.

## Requests: DoGet tickets

A ticket is a protobuf `Any` (the same envelope Flight SQL commands use):

```text
Any { type_url: <one of the URLs below>, value: <UTF-8 JSON> }
```

| type_url                    | value JSON                                | answers |
|-----------------------------|-------------------------------------------|---------|
| `icegres.tail.v1.Tables`    | `{}`                                       | tables with a tail window on this server |
| `icegres.tail.v1.Snapshot`  | `{"table": "demo.trips"}`                  | one consistent snapshot of the window |
| `icegres.tail.v1.Subscribe` | `{"table": "demo.trips", "from_seq": 42}`  | infinite incremental stream |

Hand-rolling the `Any` envelope needs no protobuf library — it is two
length-delimited fields (see `p1_tail_reader.py`):

```python
def any_ticket(type_url: bytes, value: bytes) -> bytes:
    def ld(tag, b):  # length-delimited field
        out = bytes([tag]); n = len(b)
        while True:
            byte = n & 0x7F; n >>= 7
            out += bytes([byte | (0x80 if n else 0)])
            if not n: return out + b
    return ld(0x0A, type_url) + ld(0x12, value)
```

Errors: `NOT_FOUND` (table has no window yet — nothing was ever buffered
for it this boot), `FAILED_PRECONDITION` (endpoint not buffering / no
durable tail), `DATA_LOSS` (subscriber lagged; re-snapshot),
`PERMISSION_DENIED` (authz).

## Responses: one Arrow stream

### `Tables`

Schema `(namespace: utf8, table: utf8)`; schema metadata carries
`icegres.tail.version` and `icegres.tail.watermark-property`.

### `Snapshot` and `Subscribe` (shared wire schema)

The table's **canonical Arrow schema** with every field made nullable,
plus two trailing columns:

* `__icegres_seq` (uint64, non-null) — the op's durable-tail sequence;
* `__icegres_op` (utf8, non-null) — `append` | `upsert` | `delete` |
  `watermark`.

Schema-level metadata (the header):

| key | meaning |
|-----|---------|
| `icegres.tail.version` | protocol version, `"1"` |
| `icegres.tail.table` | `namespace.table` |
| `icegres.tail.watermark-property` | the FULL `icegres.tail-seq.<tail-id>` property key this server's flushes stamp — what the consumer looks up in ITS scan metadata |
| `icegres.tail.high` | highest tail sequence in the window at snapshot/subscribe time (the `from_seq` resume cursor) |
| `icegres.tail.pk-cols` | comma-joined declared PK columns (empty = no keyed ops possible → no suppression) |

Row semantics by `__icegres_op`:

* `append` — buffered INSERT rows (all data columns populated).
* `upsert` — a key's full replacement row (all data columns populated).
* `delete` — only the PK columns are populated (the deleted key); other
  columns are null.
* `watermark` — one all-null heartbeat row; `__icegres_seq` is the highest
  tail sequence provably stamped into a committed
  `icegres.tail-seq.<tail-id>` property (safe to garbage-collect mirror
  items at or below it, after a grace period for bounded-stale readers).
  Subscribe streams also emit a 1 Hz liveness heartbeat repeating the last
  watermark.

A `Snapshot` stream ends when the window has been sent (possibly zero
batches). A `Subscribe` stream never ends voluntarily: it backfills every
window op with `from_seq < seq <= high`, then streams ops with
`seq > high` as they become durable (an op is NEVER sent before its
durability wait succeeded), interleaved with watermark events. Sequences
are per-table, strictly increasing, and never reused; events may arrive
slightly out of order across concurrent statements — consumers must key on
`seq`, not arrival order.

## The exactly-once merge rule (the whole point)

Every icegres flush stamps `max(previous, highest-drained-seq)` into the
`icegres.tail-seq.<tail-id>` table property **in the same atomic Iceberg
commit** as the rows. Therefore, for ANY table metadata `M` a consumer
scans (fresh or stale):

> Let `w` = integer value of the served watermark-property key in `M`'s
> properties (absent ⇒ treat as −∞). Then `w >= seq` ⟺ `M`'s data already
> contains the op with sequence `seq`.

A consumer producing a merged-fresh view therefore:

1. Reads committed data at metadata `M` (any Iceberg reader).
2. Reads the tail window (Snapshot, optionally + Subscribe).
3. **Includes** served `append`/`upsert` rows iff `seq > w`.
4. **Suppresses** committed rows whose PK equals a served keyed op
   (`upsert`/`delete`) with `seq > w` — compare PK values, canonical
   types.
5. Among included tail rows, the **newest seq per key wins** (an `append`
   newer than a key's `upsert` replaces it; anything older than the key's
   newest op is dropped). Rows without a PK declaration are append-only —
   include all with `seq > w`.

No snapshot ids, no manifest reasoning: correct whether `M` predates or
contains any given flush, across crashes, and across ambiguous-flush
recovery (the property never regresses).

## Versioning

The header's `icegres.tail.version` is bumped on any incompatible change;
consumers MUST refuse a version they do not speak (the Rust mirror does).
Additive changes (new ops, new metadata keys) keep the version; unknown
ops SHOULD be ignored with a warning.

## Consumers

* **Peer icegres computes** (`--peer-tail host:port[,...]`, env
  `ICEGRES_PEER_TAILS`): maintain per-table mirrors via
  Tables→Snapshot→Subscribe with reconnect/backoff; scans union the mirror
  through the same `KeySuppressExec`/union machinery as the local overlay.
  Mirror staleness is the per-peer `icegres_peer_tail_age_ms{peer=…}`
  gauge (worst case: `icegres_peer_tail_age_max_ms`); a disconnect drops
  the mirror (fallback, one WARN per outage). Two consumer-side bounds a
  reimplementation should copy:
  * **Serving age bound.** The subscriber channel runs HTTP/2 + TCP
    keepalives, and a mirror whose peer delivered no event (the 1 Hz
    heartbeat counts) for over 5 s is treated as ABSENT — a hung-but-
    connected peer must not serve unboundedly stale rows. Serving resumes
    with the next applied event. v1's header carries no flush-cadence
    hint, so the bound is a constant ≥ 3× the heartbeat interval.
  * **GC retention vs. bounded-stale reads.** A consumer may GC mirror
    items once a watermark ≥ their seq has been observed for a grace
    period. If the consumer's OWN committed reads can be stale by S
    (icegres: `--freshness-ms S`), the grace period must comfortably
    exceed S — icegres enforces max(30 s, 4×S), computed at startup —
    otherwise a row can be absent from BOTH the stale committed snapshot
    and the already-GC'd mirror, silently vanishing from the union.
* **External engines**: `bench/clients/p1_tail_reader.py` demonstrates the
  full merged-fresh read in ~100 lines of pyarrow — committed state via
  ADBC/pgwire, tail via one DoGet, merge per the rule above — and asserts
  it equals the buffering server's own union read.
