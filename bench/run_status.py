"""Atomic workload checkpoints and supervision for owned benchmark processes."""

import contextlib
import copy
import json
import os
from pathlib import Path
import signal
import subprocess
import threading
import time


def write_json(path, value):
    path = Path(path)
    temporary = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    temporary.write_text(json.dumps(value, indent=2) + "\n")
    temporary.replace(path)


def progress_path(output, namespace):
    return Path(output).with_name(f"{Path(output).name}.{namespace}.progress.json")


class Progress:
    def __init__(self, output, namespace, logs, metrics):
        self.path = progress_path(output, namespace)
        self.lock = threading.Lock()
        self.samples = {name: [] for name in metrics}
        self.state = {"namespace": namespace, "logs": str(logs), "phase": "starting", "workers": {}}
        self.checkpoint()

    def phase(self, name):
        with self.lock:
            self.state["phase"] = name
        self.checkpoint()

    def operation(self, worker, operation, **details):
        with self.lock:
            self.state["workers"][worker] = {
                "operation": operation, "started_at_unix_seconds": time.time(), **details,
            }

    def sample(self, metric, value):
        with self.lock:
            self.samples[metric].append(value)

    def checkpoint(self):
        with self.lock:
            now = time.time()
            state = copy.deepcopy(self.state)
            state["checkpoint_at_unix_seconds"] = now
            state["samples_ms"] = {name: list(values) for name, values in self.samples.items()}
            state["completed_samples"] = {name: len(values) for name, values in self.samples.items()}
            for worker in state["workers"].values():
                worker["operation_age_seconds"] = max(0, now - worker["started_at_unix_seconds"])
        write_json(self.path, state)


def invalidate(output, initial, reason):
    """Retain completed measurements and the last checkpoint, but fail the gate."""
    try:
        failed = json.loads(Path(output).read_text())
        if not isinstance(failed, dict):
            raise ValueError("artifact is not an object")
    except (OSError, ValueError):
        failed = dict(initial)
    failed["complete"] = False
    failed["correctness"] = False
    errors = failed.get("errors", 0)
    failed["errors"] = max(1, errors) if isinstance(errors, (int, float)) else 1
    details = failed.get("error_details")
    failed["error_details"] = (details if isinstance(details, list) else []) + [reason]
    failed["namespace"] = initial["namespace"]
    try:
        checkpoint = json.loads(progress_path(output, initial["namespace"]).read_text())
        if checkpoint.get("namespace") == initial["namespace"]:
            failed["progress"] = checkpoint
            failed.setdefault("samples_ms", checkpoint["samples_ms"])
    except (OSError, ValueError, KeyError, AttributeError):
        pass
    write_json(output, failed)


def supervise(command, timeout, output, initial):
    """Bound the whole process group, including setup, queries and cleanup."""
    write_json(output, initial)
    worker = subprocess.Popen(command, start_new_session=True)
    try:
        code = worker.wait(timeout=timeout)
    except (KeyboardInterrupt, subprocess.TimeoutExpired) as error:
        # The group may disappear between wait() and killpg(). Still invalidate
        # its artifact, including if the worker completed in that narrow race.
        with contextlib.suppress(ProcessLookupError):
            os.killpg(worker.pid, signal.SIGKILL)
        worker.wait(timeout=5)
        reason = ("run interrupted" if isinstance(error, KeyboardInterrupt)
                  else "whole-run deadline exceeded")
        invalidate(output, initial, reason + "; owned processes killed; catalog cleanup may remain")
        return 130 if isinstance(error, KeyboardInterrupt) else 1
    if code != 0:
        # A failed worker can leave descendants behind if it died before finally.
        with contextlib.suppress(ProcessLookupError):
            os.killpg(worker.pid, signal.SIGKILL)
        invalidate(output, initial, f"worker exited with status {code}")
    return code
