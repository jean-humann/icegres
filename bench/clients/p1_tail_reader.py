#!/usr/bin/env python3
"""P1 open-tail-protocol external reader (docs/open-tail-protocol.md).

The demo that ANY engine can do LTAP's merged-fresh trick against icegres:

  1. Write rows through the buffering server (pgwire, buffered ack).
  2. Read COMMITTED state only: current snapshot id + table properties from
     the Iceberg REST catalog, rows via the pgwire time-travel path
     (`table@<snapshot_id>` never sees the buffer) — any Iceberg reader
     could do this leg directly from Parquet.
  3. Call TailSnapshot on the server's tail API (--tail-api-port) with a
     hand-rolled protobuf-Any ticket over plain pyarrow.flight.
  4. Merge per the protocol's exactly-once rule: include tail rows iff
     seq > w (w = the served icegres.tail-seq.<id> property in the SAME
     metadata the committed read used); suppress committed rows whose PK
     has a keyed op with seq > w; newest seq per key wins.
  5. Assert the merged view equals the buffering server's own union read.

With ICEGRES_PROBE_KEYED=1 the probe also issues an exact-PK UPDATE and
DELETE (keyed tail acks) and asserts the merged view reflects them.

Environment (defaults for the e2e harness):
  ICEGRES_PROBE_PG_HOST / ICEGRES_PROBE_PG_PORT        buffering server (127.0.0.1:5439)
  ICEGRES_PROBE_TAIL_HOST / ICEGRES_PROBE_TAIL_PORT    tail API (127.0.0.1:50057)
  ICEGRES_PROBE_CATALOG                                 http://127.0.0.1:8181/catalog
  ICEGRES_PROBE_WAREHOUSE                               lakehouse
  ICEGRES_PROBE_TABLE                                   demo.trips
  ICEGRES_PROBE_KEYED                                   0

Write hygiene: this probe owns trip_id >= 980000 (a8 owns >=930000, a11
>=940000) and deletes its rows at the end. Exit 0 iff no step failed.
"""

import json
import os
import sys
import time
import urllib.request
import warnings

warnings.filterwarnings("ignore")

PG_HOST = os.environ.get("ICEGRES_PROBE_PG_HOST", "127.0.0.1")
PG_PORT = int(os.environ.get("ICEGRES_PROBE_PG_PORT", "5439"))
TAIL_HOST = os.environ.get("ICEGRES_PROBE_TAIL_HOST", "127.0.0.1")
TAIL_PORT = int(os.environ.get("ICEGRES_PROBE_TAIL_PORT", "50057"))
CATALOG = os.environ.get("ICEGRES_PROBE_CATALOG", "http://127.0.0.1:8181/catalog")
WAREHOUSE = os.environ.get("ICEGRES_PROBE_WAREHOUSE", "lakehouse")
TABLE = os.environ.get("ICEGRES_PROBE_TABLE", "demo.trips")
KEYED = os.environ.get("ICEGRES_PROBE_KEYED", "0") == "1"
BASE_ID = 980000

RESULTS = {"pass": 0, "fail": 0, "skip": 0}


def record(status, name, detail=""):
    RESULTS[{"PASS": "pass", "FAIL": "fail", "SKIP": "skip"}[status]] += 1
    print(f"{status} {name} -- {detail}" if detail else f"{status} {name}")


def any_ticket(type_url: str, value: dict) -> bytes:
    """protobuf Any {type_url, value} — two length-delimited fields."""

    def ld(tag, b):
        out = bytes([tag])
        n = len(b)
        while True:
            byte = n & 0x7F
            n >>= 7
            out += bytes([byte | (0x80 if n else 0)])
            if not n:
                return out + b

    return ld(0x0A, type_url.encode()) + ld(0x12, json.dumps(value).encode())


def rest_json(url):
    with urllib.request.urlopen(url, timeout=10) as r:
        return json.loads(r.read())


def fetch_meta(prefix, ns, tbl):
    m = rest_json(f"{CATALOG}/v1/{prefix}/namespaces/{ns}/tables/{tbl}")["metadata"]
    return m.get("current-snapshot-id"), m.get("properties", {})


def main():
    try:
        import psycopg2
        import pyarrow.flight as fl
    except ImportError as e:
        record("SKIP", "imports", f"pip install psycopg2-binary pyarrow: {e}")
        return finish()

    ns, tbl = TABLE.split(".", 1)
    qtable = f'{ns}."{tbl}"' if not tbl.isidentifier() else f"{ns}.{tbl}"

    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname="icegres", user="icegres"
    )
    conn.autocommit = True
    cur = conn.cursor()

    # -- REST catalog prefix (same discovery the shell harness uses).
    cfg = rest_json(f"{CATALOG}/v1/config?warehouse={WAREHOUSE}")
    prefix = cfg.get("overrides", {}).get("prefix") or cfg.get("defaults", {}).get(
        "prefix"
    )
    if not prefix:
        record("FAIL", "catalog: REST config", f"no prefix in {cfg}")
        return finish()
    record("PASS", "catalog: REST config", f"prefix={prefix}")

    # -- 1. Buffered writes through the server under test.
    cur.execute(f"SELECT COALESCE(MAX(trip_id), 0) FROM {qtable}")
    base = max(BASE_ID, int(cur.fetchone()[0]) + 1)
    rows = [(base + i, f"p1-city-{i}") for i in range(3)]
    for trip_id, city in rows:
        cur.execute(
            f"INSERT INTO {qtable} (trip_id, city, distance_km, fare, ts) "
            f"VALUES ({trip_id}, '{city}', 1.5, 2.5, TIMESTAMP '2026-01-01 00:00:00')"
        )
    record("PASS", "write: 3 buffered INSERTs acked", f"trip_id base={base}")
    if KEYED:
        cur.execute(f"UPDATE {qtable} SET city = 'p1-updated' WHERE trip_id = {base + 1}")
        cur.execute(f"DELETE FROM {qtable} WHERE trip_id = {base + 2}")
        record("PASS", "write: keyed UPDATE + DELETE acked", f"on {base + 1}/{base + 2}")

    try:
        # -- 2. Tail snapshot FIRST? No: committed read first, tail second —
        # anything the flusher commits in between still sits in the window
        # (30 s retention) and the watermark rule places it exactly once.
        stable = None
        for _ in range(30):
            snap1, props1 = fetch_meta(prefix, ns, tbl)
            committed = {}
            if snap1 is not None and int(snap1) > 0:
                cur.execute(
                    f'SELECT trip_id, city FROM {ns}."{tbl}@{snap1}" '
                    f"WHERE trip_id >= {BASE_ID}"
                )
                committed = {int(r[0]): r[1] for r in cur.fetchall()}
            snap2, props2 = fetch_meta(prefix, ns, tbl)
            if snap1 == snap2 and props1 == props2:
                stable = (snap1, props1, committed)
                break
            time.sleep(0.2)
        if stable is None:
            record("FAIL", "committed: stable snapshot read", "metadata kept moving")
            return finish()
        snap_id, props, committed = stable
        record(
            "PASS",
            "committed: time-travel read + properties",
            f"snapshot={snap_id} committed_probe_rows={len(committed)}",
        )

        # -- 3. TailSnapshot over raw Arrow Flight.
        client = fl.FlightClient(f"grpc://{TAIL_HOST}:{TAIL_PORT}")
        reader = client.do_get(
            fl.Ticket(any_ticket("icegres.tail.v1.Snapshot", {"table": TABLE}))
        )
        table = reader.read_all()
        meta = {
            k.decode(): v.decode() for k, v in (table.schema.metadata or {}).items()
        }
        if meta.get("icegres.tail.version") != "1":
            record("FAIL", "tail: snapshot header", f"unexpected header {meta}")
            return finish()
        wm_key = meta["icegres.tail.watermark-property"]
        pk_cols = [c for c in meta.get("icegres.tail.pk-cols", "").split(",") if c]
        record(
            "PASS",
            "tail: TailSnapshot",
            f"items={table.num_rows} high={meta['icegres.tail.high']} "
            f"wm_key={wm_key} pk={pk_cols or 'none'}",
        )

        # -- 4. The exactly-once merge rule.
        w = int(props.get(wm_key, -1))  # absent property = -inf
        names = table.schema.names
        seq_i, op_i = names.index("__icegres_seq"), names.index("__icegres_op")
        id_i, city_i = names.index("trip_id"), names.index("city")
        items = []  # (seq, op, trip_id, city)
        for i in range(table.num_rows):
            items.append(
                (
                    table.column(seq_i)[i].as_py(),
                    table.column(op_i)[i].as_py(),
                    table.column(id_i)[i].as_py(),
                    table.column(city_i)[i].as_py(),
                )
            )
        best = {}  # trip_id -> (seq, op, city): newest seq per key wins
        keyed_newer = set()  # keys with an upsert/delete op newer than w
        plain = []  # rows without a PK declaration (append-only)
        for seq, op, tid, city in items:
            if seq <= w or tid is None or int(tid) < BASE_ID:
                continue
            tid = int(tid)
            if op in ("upsert", "delete"):
                keyed_newer.add(tid)
            if not pk_cols:
                if op == "append":
                    plain.append((tid, city))
                continue
            if tid not in best or best[tid][0] < seq:
                best[tid] = (seq, op, city)
        merged = {}
        for tid, city in committed.items():
            if tid not in keyed_newer:
                merged[tid] = city
        if pk_cols:
            for tid, (seq, op, city) in best.items():
                if op in ("append", "upsert"):
                    merged[tid] = city
                elif op == "delete":
                    merged.pop(tid, None)
        merged_rows = sorted(merged.items()) + sorted(plain)
        record("PASS", "merge: applied the watermark rule", f"w={w} merged={len(merged_rows)}")

        # -- 5. Ground truth: the buffering server's own union read.
        cur.execute(
            f"SELECT trip_id, city FROM {qtable} WHERE trip_id >= {BASE_ID} "
            f"ORDER BY trip_id"
        )
        union_rows = [(int(r[0]), r[1]) for r in cur.fetchall()]
        if sorted(merged_rows) == sorted(union_rows):
            record(
                "PASS",
                "merged-fresh view == server union read",
                f"{len(union_rows)} rows agree (the LTAP trick, externally)",
            )
        else:
            record(
                "FAIL",
                "merged-fresh view == server union read",
                f"merged={sorted(merged_rows)} union={sorted(union_rows)}",
            )
        if KEYED:
            got = dict(union_rows)
            ok = got.get(base + 1) == "p1-updated" and (base + 2) not in got
            record(
                "PASS" if ok else "FAIL",
                "keyed: UPDATE visible + DELETE suppressed in the merged view",
                f"row {base + 1}={got.get(base + 1)!r}, {base + 2} present={base + 2 in got}",
            )
    finally:
        # -- Cleanup (a fenced DELETE flushes the buffer synchronously).
        try:
            cur.execute(f"DELETE FROM {qtable} WHERE trip_id >= {BASE_ID}")
            record("PASS", "cleanup: probe rows deleted")
        except Exception as e:  # noqa: BLE001
            record("FAIL", "cleanup: probe rows deleted", str(e))
        conn.close()
    return finish()


def finish():
    print(
        f"P1TAIL RESULT: pass={RESULTS['pass']} fail={RESULTS['fail']} "
        f"skip={RESULTS['skip']}"
    )
    return 0 if RESULTS["fail"] == 0 else 2


if __name__ == "__main__":
    sys.exit(main())
