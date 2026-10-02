"""Gate regressions: only documented PostgreSQL errors count as expected."""

import contextlib
import importlib.util
import io
from pathlib import Path
from types import SimpleNamespace
import unittest


spec = importlib.util.spec_from_file_location(
    "a8_orm_probe", Path(__file__).with_name("a8_orm_probe.py")
)
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


class ExpectedFailureTests(unittest.TestCase):
    def test_native_driver_error_requires_code_and_message(self):
        known = "SELECT inside an explicit transaction is supported on the simple query protocol only"
        self.assertTrue(probe.postgres_error_matches(
            Exception({"C": "0A000", "M": known}), "0A000", known
        ))
        for error in [
            Exception({"C": "XX000", "M": known}),
            Exception({"C": "0A000", "M": "unrelated unsupported operation"}),
            TimeoutError(known),
            ValueError("incorrect result"),
        ]:
            self.assertFalse(probe.postgres_error_matches(error, "0A000", known))

    def test_psycopg_diagnostics_are_checked(self):
        error = Exception("opaque client text")
        error.pgcode = "0A000"
        error.diag = SimpleNamespace(message_primary="documented unsupported operation")
        self.assertTrue(probe.postgres_error_matches(error, "0A000", "documented unsupported"))
        error.pgcode = "08006"
        self.assertFalse(probe.postgres_error_matches(error, "0A000", "documented unsupported"))

    def test_unrelated_exceptions_fail_even_when_step_has_xfail_description(self):
        def timeout():
            raise TimeoutError("network failure")

        probe.RESULTS.clear()
        with contextlib.redirect_stdout(io.StringIO()):
            probe.step("unexpected failure", timeout, xfail="known limit",
                       xfail_when=lambda error: probe.postgres_error_matches(error, "0A000", "known limit"))
            probe.step("missing matcher", timeout, xfail="known limit")
        self.assertEqual([row[0] for row in probe.RESULTS], ["FAIL", "FAIL"])


if __name__ == "__main__":
    unittest.main()
