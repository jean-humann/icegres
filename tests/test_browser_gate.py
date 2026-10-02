"""Exercise browser gate exit behavior without a build, network, or browser."""

import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


class BrowserGateTests(unittest.TestCase):
    def run_gate(self, *, strict=False, chromium=True, cargo_status=42):
        with tempfile.TemporaryDirectory(prefix="icegres-browser-gate-") as tmp:
            root = Path(tmp)
            for name in ("tests", "icegres", "bin"):
                (root / name).mkdir()
            shutil.copyfile(
                Path(__file__).with_name("browser-flight.sh"),
                root / "tests/browser-flight.sh",
            )
            for name, status in (
                ("node", 0), ("curl", 0), ("chromium", 0), ("cargo", cargo_status)
            ):
                stub = root / "bin" / name
                stub.write_text(f"#!/bin/sh\nexit {status}\n")
                stub.chmod(0o755)
            env = os.environ.copy()
            env.update(
                PATH=str(root / "bin") + ":/usr/bin:/bin",
                CHROMIUM_PATH=str(root / "bin" / ("chromium" if chromium else "absent")),
                ICEGRES_REQUIRE_LIVE_TESTS="1" if strict else "0",
            )
            return subprocess.run(
                ["/bin/bash", str(root / "tests/browser-flight.sh")],
                env=env, capture_output=True, text=True, timeout=10,
            )

    def test_optional_missing_prerequisite_skips(self):
        result = self.run_gate(chromium=False)
        self.assertEqual(result.returncode, 0)
        self.assertIn("SKIP", result.stdout)

    def test_required_missing_prerequisite_fails(self):
        result = self.run_gate(strict=True, chromium=False)
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("required prerequisite missing", result.stderr)

    def test_build_failure_always_fails(self):
        for strict in (False, True):
            with self.subTest(strict=strict):
                result = self.run_gate(strict=strict)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("icegres build failed", result.stderr)
                self.assertNotIn("SKIP", result.stdout)


if __name__ == "__main__":
    unittest.main()
