"""Fetch-and-write core: ADBC Flight SQL in, .parquet / .hyper out.

Layering rules kept deliberately simple:

- ``adbc_driver_flightsql`` + ``pyarrow`` are hard dependencies (the wire).
- ``pantab`` (Hyper) and ``tableauserverclient`` (publish) are imported
  lazily so the Parquet lane works on a minimal install; a missing optional
  dependency surfaces as a clear error naming the extra to install.
"""

from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Iterator, Optional

import pyarrow as pa


@dataclass
class ExtractReport:
    """What one extract run did, for logs and cron output."""

    out_path: str
    fmt: str
    rows: int
    out_bytes: int
    elapsed_s: float

    def line(self) -> str:
        mib = self.out_bytes / (1024 * 1024)
        return (
            f"wrote {self.out_path} [{self.fmt}] rows={self.rows} "
            f"size={mib:.1f}MiB elapsed={self.elapsed_s:.2f}s"
        )


def build_query(
    table: Optional[str] = None,
    query: Optional[str] = None,
    snapshot: Optional[int] = None,
) -> str:
    """Return the SQL to run: an explicit --query, or SELECT * over --table.

    ``snapshot`` pins the table read to an Iceberg snapshot id via the
    ``"table@<id>"`` form — the Flight lane has no ``AS OF`` sugar
    (docs/limitations.md), so this is the one spelling that works there.
    """
    if (table is None) == (query is None):
        raise ValueError("exactly one of table= or query= is required")
    if query is not None:
        if snapshot is not None:
            raise ValueError("snapshot pinning applies to table=, not query=")
        return query
    parts = table.split(".")
    if snapshot is not None:
        parts[-1] = f"{parts[-1]}@{snapshot}"
    quoted = ".".join('"' + p.replace('"', '""') + '"' for p in parts)
    return f"SELECT * FROM {quoted}"


def _connect(
    dsn: str,
    username: Optional[str],
    password: Optional[str],
    tls_skip_verify: bool,
):
    import adbc_driver_flightsql.dbapi as flight
    from adbc_driver_flightsql import DatabaseOptions

    db_kwargs = {}
    if username is not None:
        db_kwargs["username"] = username
    if password is not None:
        db_kwargs["password"] = password
    if tls_skip_verify:
        db_kwargs[DatabaseOptions.TLS_SKIP_VERIFY.value] = "true"
    return flight.connect(dsn, db_kwargs=db_kwargs or None)


def _counting_reader(reader: pa.RecordBatchReader, counter: dict) -> pa.RecordBatchReader:
    """Wrap a RecordBatchReader so consumers see the same stream while we
    count rows — lets both writer lanes report rows without a second pass."""

    def gen() -> Iterator[pa.RecordBatch]:
        for batch in reader:
            counter["rows"] += batch.num_rows
            yield batch

    return pa.RecordBatchReader.from_batches(reader.schema, gen())


def _require_pantab():
    """Import pantab, with the install hint. Called BEFORE connecting so a
    minimal install pointed at .hyper fails fast, not after the server has
    already executed the query."""
    try:
        import pantab
    except ImportError as exc:  # pragma: no cover - environment-dependent
        raise RuntimeError(
            ".hyper output needs pantab (Tableau Hyper engine): "
            "pip install 'icegres-bi-extract[hyper]'"
        ) from exc
    return pantab


def _write_parquet(reader: pa.RecordBatchReader, out_path: str, compression: str) -> None:
    import pyarrow.parquet as pq

    # Atomic like the Hyper lane (pantab defaults to temp+rename): a
    # mid-stream failure must neither destroy last night's extract nor
    # leave a truncated file behind for the BI tool to trip on.
    tmp_path = out_path + ".tmp"
    try:
        with pq.ParquetWriter(tmp_path, reader.schema, compression=compression) as writer:
            for batch in reader:
                writer.write_batch(batch)
        os.replace(tmp_path, out_path)
    except BaseException:
        try:
            os.unlink(tmp_path)
        except OSError:
            pass
        raise


def _write_hyper(reader: pa.RecordBatchReader, out_path: str, table_name: str) -> None:
    pantab = _require_pantab()
    # pantab >= 4 accepts any Arrow C-stream capsule producer, a
    # RecordBatchReader included, and writes through the Hyper API
    # (atomically: temp file + rename by default).
    pantab.frame_to_hyper(reader, out_path, table=table_name)


def run_extract(
    dsn: str,
    out_path: str,
    table: Optional[str] = None,
    query: Optional[str] = None,
    snapshot: Optional[int] = None,
    username: Optional[str] = None,
    password: Optional[str] = None,
    tls_skip_verify: bool = False,
    hyper_table: str = "Extract",
    parquet_compression: str = "zstd",
) -> ExtractReport:
    """Run one extract: connect, stream the result into the output file.

    The output format follows the file suffix: ``.parquet`` or ``.hyper``.
    """
    if out_path.endswith(".parquet"):
        fmt = "parquet"
    elif out_path.endswith(".hyper"):
        fmt = "hyper"
    else:
        raise ValueError(f"unsupported output suffix (want .parquet or .hyper): {out_path}")
    if fmt == "hyper":
        _require_pantab()  # fail fast, before the server runs the query

    sql = build_query(table=table, query=query, snapshot=snapshot)
    started = time.monotonic()
    counter = {"rows": 0}
    with _connect(dsn, username, password, tls_skip_verify) as conn:
        with conn.cursor() as cur:
            cur.execute(sql)
            reader = _counting_reader(cur.fetch_record_batch(), counter)
            if fmt == "parquet":
                _write_parquet(reader, out_path, parquet_compression)
            else:
                _write_hyper(reader, out_path, hyper_table)
    return ExtractReport(
        out_path=out_path,
        fmt=fmt,
        rows=counter["rows"],
        out_bytes=os.path.getsize(out_path),
        elapsed_s=time.monotonic() - started,
    )


def publish_hyper(
    hyper_path: str,
    server_url: str,
    site: str,
    project: str,
    token_name: str,
    token_value: str,
    datasource_name: Optional[str] = None,
) -> str:
    """Publish a .hyper file to Tableau Server / Tableau Cloud (overwrite
    mode) using a personal access token. Returns the datasource id."""
    try:
        import tableauserverclient as TSC
    except ImportError as exc:  # pragma: no cover - environment-dependent
        raise RuntimeError(
            "publishing needs tableauserverclient: "
            "pip install 'icegres-bi-extract[publish]'"
        ) from exc

    auth = TSC.PersonalAccessTokenAuth(token_name, token_value, site_id=site)
    server = TSC.Server(server_url, use_server_version=True)
    with server.auth.sign_in(auth):
        matches = [p for p in TSC.Pager(server.projects) if p.name == project]
        if not matches:
            raise RuntimeError(f"Tableau project not found: {project!r}")
        item = TSC.DatasourceItem(matches[0].id, name=datasource_name)
        published = server.datasources.publish(
            item, hyper_path, mode=TSC.Server.PublishMode.Overwrite
        )
        return published.id
