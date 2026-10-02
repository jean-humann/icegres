import copy
import importlib.util
import math
import json
from pathlib import Path
import subprocess
import tempfile
import unittest


spec = importlib.util.spec_from_file_location("metrics", Path(__file__).parents[1] / "bench/check_metrics.py")
metrics = importlib.util.module_from_spec(spec)
spec.loader.exec_module(metrics)


def fixture():
    return {"metrics": {
        **{name: {"n": 100, "p50": 10, "p95": 20} for name in metrics.LATENCIES},
        "qps_8conn": {"value": 100},
        **{name: {"value": 100} for name in metrics.RESOURCES},
    }}


class MetricGateTests(unittest.TestCase):
    def test_equal_complete_measurements_pass(self):
        self.assertEqual(metrics.compare(fixture(), fixture()), [])

    def test_recorded_insert_tail_regression_fails_even_with_fast_median(self):
        baseline, candidate = fixture(), fixture()
        baseline["metrics"]["insert_single_ms"].update(p50=60, p95=86.58)
        candidate["metrics"]["insert_single_ms"].update(p50=58.89, p95=1738.47)
        failures = metrics.compare(baseline, candidate)
        self.assertEqual(len(failures), 1)
        self.assertIn("insert_single_ms.p95", failures[0])

    def test_missing_nonfinite_negative_and_sparse_values_fail(self):
        for key, value in (("p95", None), ("p50", math.nan), ("p95", math.inf),
                           ("p50", -1), ("n", 19), ("n", True), ("p95", "20")):
            with self.subTest(key=key, value=value), self.assertRaises(ValueError):
                candidate = fixture()
                candidate["metrics"]["connect_ms"][key] = value
                metrics.compare(fixture(), candidate)

    def test_inverted_percentiles_fail(self):
        candidate = fixture()
        candidate["metrics"]["connect_ms"]["p95"] = 1
        with self.assertRaises(ValueError):
            metrics.compare(fixture(), candidate)

    def test_shell_gate_fails_for_tail_regression_and_invalid_parity(self):
        root = Path(__file__).parents[1]
        with tempfile.TemporaryDirectory() as directory:
            paths = [Path(directory) / name for name in ("base.json", "candidate.json", "parity.json")]
            baseline, candidate = fixture(), fixture()
            candidate["metrics"]["insert_single_ms"]["p95"] = 1000
            paths[0].write_text(json.dumps(baseline))
            paths[1].write_text(json.dumps(candidate))
            command = ["bash", str(root / "bench/gate.sh"), str(paths[0]), str(paths[1])]
            result = subprocess.run([*command, "--skip-e2e"], capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
            self.assertIn("insert_single_ms.p95", result.stdout)
            paths[1].write_text(json.dumps(baseline))
            paths[2].write_text("not valid JSON")
            result = subprocess.run([*command, str(paths[2]), str(paths[2]), "--skip-e2e"],
                                    capture_output=True, text=True, timeout=30)
            self.assertEqual(result.returncode, 1, result.stdout + result.stderr)
            self.assertIn("parity result is invalid", result.stdout)

    def test_mixed_requires_correctness_comparability_and_p99(self):
        baseline = {
            "schema_version": 1,
            "workload": {"rows": 1000, "files": 10, "transactions": 100, "memory_mb": 1024,
                         "readers": 2, "durability": "sync", "freshness": "replica", "fixture_version": 1},
            "environment": {"platform": "test", "cpus": 4, "catalog_uri": "local",
                            "warehouse": "test", "s3_endpoint": "local", "resource_scope": "compute"},
            "errors": 0,
            "complete": True, "correctness": True,
            "metrics": {
                **{name: {"n": 100, "p50": 10, "p95": 20, "p99": 30}
                   for name in ("transaction_ms", "analytics_ms", "point_read_ms", "freshness_ms")},
                "rss_peak_mb": {"value": 100}, "operations_per_second": {"value": 100},
                "transactions_per_second": {"value": 10},
            },
        }
        self.assertEqual(metrics.compare(baseline, baseline, mixed=True), [])
        candidate = copy.deepcopy(baseline)
        candidate["metrics"]["analytics_ms"]["p99"] = 100
        self.assertIn("p99", metrics.compare(baseline, candidate, mixed=True)[0])
        candidate = copy.deepcopy(baseline)
        candidate["errors"] = 1
        self.assertTrue(metrics.compare(baseline, candidate, mixed=True))
        candidate["workload"]["rows"] = 2000
        with self.assertRaises(ValueError):
            metrics.compare(baseline, candidate, mixed=True)


if __name__ == "__main__":
    unittest.main()
