import os
import tempfile
import threading
import unittest

import pyarrow as pa
import pyarrow.parquet as pq

from icegres_bi_extract.core import _write_parquet, build_query


def reader(values):
    batch = pa.record_batch([pa.array(values, type=pa.int64())], names=["value"])
    return pa.RecordBatchReader.from_batches(batch.schema, [batch])


class CoreExtractTest(unittest.TestCase):
    def test_build_query_quotes_identifiers(self):
        self.assertEqual(
            build_query(table='demo.weird"name', snapshot=42),
            'SELECT * FROM "demo"."weird""name@42"',
        )

    def test_failed_write_preserves_previous_extract_and_cleans_temp(self):
        with tempfile.TemporaryDirectory() as directory:
            output = os.path.join(directory, "extract.parquet")
            _write_parquet(reader([7]), output, "zstd")
            with open(output, "rb") as previous_file:
                previous = previous_file.read()

            schema = pa.schema([("value", pa.int64())])

            def broken_batches():
                yield pa.record_batch([pa.array([8])], schema=schema)
                raise RuntimeError("injected stream failure")

            broken = pa.RecordBatchReader.from_batches(schema, broken_batches())
            with self.assertRaisesRegex(RuntimeError, "injected"):
                _write_parquet(broken, output, "zstd")

            with open(output, "rb") as current_file:
                self.assertEqual(current_file.read(), previous)
            self.assertEqual(os.listdir(directory), ["extract.parquet"])

    def test_concurrent_writers_never_share_or_mix_temporary_files(self):
        with tempfile.TemporaryDirectory() as directory:
            output = os.path.join(directory, "extract.parquet")
            barrier = threading.Barrier(2)
            errors = []

            def run(value):
                try:
                    barrier.wait(timeout=5)
                    _write_parquet(reader([value] * 2000), output, "zstd")
                except BaseException as exc:  # captured for the parent assertion
                    errors.append(exc)

            threads = [threading.Thread(target=run, args=(value,)) for value in (1, 2)]
            for thread in threads:
                thread.start()
            for thread in threads:
                thread.join(timeout=10)

            self.assertFalse(errors)
            self.assertFalse(any(thread.is_alive() for thread in threads))
            values = pq.read_table(output).column("value").to_pylist()
            self.assertEqual(len(values), 2000)
            self.assertIn(set(values), ({1}, {2}))
            self.assertEqual(os.listdir(directory), ["extract.parquet"])


if __name__ == "__main__":
    unittest.main()
