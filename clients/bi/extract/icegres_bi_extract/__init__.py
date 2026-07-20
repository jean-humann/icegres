"""Columnar BI extracts from icegres over ADBC / Arrow Flight SQL.

The refresh half of the BI story (docs/bi-integration.md section 6): pull a
query result out of icegres on the Arrow-native lane — measured 10-16x
faster than the row drivers packaged BI connectors use for full extracts —
and hand it to the BI tool in the format its own engine loads natively:

- ``.hyper``  -> Tableau (the extract file its Hyper engine serves directly)
- ``.parquet`` -> Power BI (Parquet connector), DuckDB, or anything columnar

The wire pull streams: batches flow from the server into the writer as they
arrive, so client memory is bounded by a batch, not the extract size (the
.hyper lane is bounded by pantab's writer behavior instead — see README).
"""

from .core import ExtractReport, publish_hyper, run_extract

__all__ = ["ExtractReport", "publish_hyper", "run_extract"]
