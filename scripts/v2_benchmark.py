#!/usr/bin/env python3
"""Correctness-gated V2 benchmark. No timing is printed before every gate passes."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile
import threading
import time

PHASES = ["discovery", "links", "events", "clicks", "sort", "sweep", "write"]
TABLES = ["user_daily", "campaign_daily"]


class GateFailure(RuntimeError):
    pass


class ScratchMonitor:
    def __init__(self, root: Path, interval_seconds: float = 0.1):
        self.root = root
        self.interval_seconds = interval_seconds
        self.high_water_bytes = 0
        self.cpu_ns = 0
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="scratch-high-water", daemon=True)

    def start(self):
        self._thread.start()

    def stop(self):
        self._stop.set()
        self._thread.join()

    def _run(self):
        cpu_start = time.thread_time_ns()
        while not self._stop.is_set():
            self.high_water_bytes = max(self.high_water_bytes, tree_size(self.root))
            self._stop.wait(self.interval_seconds)
        self.high_water_bytes = max(self.high_water_bytes, tree_size(self.root))
        self.cpu_ns = time.thread_time_ns() - cpu_start


def tree_size(root: Path) -> int:
    total = 0
    try:
        paths = list(root.rglob("*"))
    except OSError:
        return total
    for path in paths:
        try:
            if path.is_file() and not path.is_symlink():
                total += path.stat().st_size
        except OSError:
            pass
    return total


def run_candidate(command: list[str], data: Path, output: Path, *, measured: bool = False):
    output.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="v2-scratch-") as scratch_name:
        scratch = Path(scratch_name)
        telemetry_path = scratch / "telemetry.json"
        environment = dict(
            os.environ,
            DATA_DIR=str(data),
            OUTPUT_DIR=str(output),
            SCRATCH_DIR=str(scratch),
            V2_TELEMETRY_PATH=str(telemetry_path),
        )
        monitor = ScratchMonitor(scratch)
        if measured:
            monitor.start()
        started = time.perf_counter_ns()
        process = subprocess.run(
            command + [str(data), str(output)],
            env=environment,
            capture_output=True,
            text=True,
        )
        elapsed_ns = time.perf_counter_ns() - started
        if measured:
            monitor.stop()
        if process.returncode:
            raise GateFailure(
                f"candidate exited {process.returncode}; diagnostics withheld until correctness passes"
            )
        telemetry = None
        if telemetry_path.is_file():
            try:
                telemetry = json.loads(telemetry_path.read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError) as error:
                raise GateFailure(f"invalid candidate telemetry: {error}") from error
        return elapsed_ns, monitor, telemetry


def run_checked(arguments: list[str], label: str):
    process = subprocess.run(arguments, capture_output=True, text=True)
    if process.returncode:
        raise GateFailure(f"{label} failed; diagnostics withheld until correctness passes")


def compare_outputs(left: Path, right: Path, label: str):
    run_checked(
        [sys.executable, str(Path(__file__).with_name("compare.py")), str(left), str(right)],
        label,
    )


def output_digest(output: Path) -> str:
    digest = hashlib.sha256()
    for table in TABLES:
        files = sorted(
            path for path in (output / table).glob("*.parquet")
            if path.is_file() and not path.is_symlink() and not path.name.startswith((".", "_"))
        )
        if [path.name for path in files] != ["part-0.parquet"]:
            raise GateFailure(f"{table}: expected only part-0.parquet")
        for path in files:
            digest.update(table.encode())
            digest.update(path.name.encode())
            with path.open("rb") as handle:
                while block := handle.read(1024 * 1024):
                    digest.update(block)
    return digest.hexdigest()


def validate_telemetry(value):
    if not isinstance(value, dict) or value.get("format_version") != 1:
        raise GateFailure("candidate telemetry format_version must be 1")
    phases = value.get("phases")
    if not isinstance(phases, list) or [item.get("phase") for item in phases] != PHASES:
        raise GateFailure(f"candidate telemetry must contain phases in order: {PHASES}")
    required_counters = {
        "discovered_files", "input_rows", "accepted_rows", "dropped_rows",
        "output_rows", "bytes_read", "bytes_written",
    }
    for item in phases:
        if not isinstance(item.get("elapsed_ns"), int) or item["elapsed_ns"] < 0:
            raise GateFailure(f"invalid elapsed_ns for {item.get('phase')}")
        counters = item.get("counters")
        if not isinstance(counters, dict) or set(counters) != required_counters:
            raise GateFailure(f"invalid counters for {item.get('phase')}")
        if any(not isinstance(count, int) or count < 0 for count in counters.values()):
            raise GateFailure(f"negative or non-integer counter for {item.get('phase')}")
    resources = value.get("resources")
    if not isinstance(resources, dict) or set(resources) != {
        "cgroup_memory_peak_bytes", "scratch_high_water_bytes"
    }:
        raise GateFailure("candidate telemetry resources are missing")
    if any(not isinstance(value, int) or value < 0 for value in resources.values()):
        raise GateFailure("timed runs require captured cgroup and scratch high-water bytes")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", required=True, help="candidate command, shell-quoted")
    parser.add_argument("--fixtures", type=Path, required=True)
    parser.add_argument("--dev-data", type=Path, required=True)
    parser.add_argument("--dev-answer", type=Path, required=True)
    parser.add_argument("--expected-dev-events", type=int, default=1_000_000)
    parser.add_argument("--max-overhead-percent", type=float, default=0.5)
    args = parser.parse_args()

    if args.expected_dev_events != 1_000_000:
        raise GateFailure("the timed V2 gate requires the exact 1M development dataset")
    for path, label in [
        (args.fixtures, "fixtures"),
        (args.dev_data, "1M development input"),
        (args.dev_answer, "1M development answer"),
    ]:
        if not path.is_dir():
            raise GateFailure(f"{label} is unavailable at {path}; refusing to report timing")

    provenance_path = args.dev_data / ".v2-generation.json"
    try:
        provenance = json.loads(provenance_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise GateFailure(
            f"1M input provenance is unavailable at {provenance_path}: {error}; "
            "regenerate it with scripts/v2_generate.py"
        ) from error
    if provenance.get("format_version") != 1 or provenance.get("events") != args.expected_dev_events:
        raise GateFailure(
            f"development input provenance does not certify exactly {args.expected_dev_events} events"
        )

    command = shlex.split(args.candidate)
    if not command:
        raise GateFailure("candidate command is empty")

    fixture_runner = Path(__file__).with_name("run_fixtures.py")
    run_checked(
        [
            sys.executable, str(fixture_runner), str(args.fixtures),
            "--require-count", "36", "--require-single-part", "--", *command,
        ],
        "36-fixture and schema gate",
    )

    with tempfile.TemporaryDirectory(prefix="v2-correctness-") as temporary:
        temporary = Path(temporary)
        first = temporary / "first"
        second = temporary / "second"
        run_candidate(command, args.dev_data, first)
        compare_outputs(first, args.dev_answer, "exact 1M and schema gate")
        output_digest(first)
        run_candidate(command, args.dev_data, second)
        compare_outputs(second, args.dev_answer, "repeat exact 1M and schema gate")
        compare_outputs(first, second, "deterministic-repeat row and schema gate")
        output_digest(second)

    with tempfile.TemporaryDirectory(prefix="v2-benchmark-") as temporary:
        output = Path(temporary) / "output"
        elapsed_ns, monitor, telemetry = run_candidate(command, args.dev_data, output, measured=True)
        compare_outputs(output, args.dev_answer, "timed-run correctness gate")
        output_digest(output)
        if telemetry is None:
            raise GateFailure("candidate emitted no telemetry; refusing to report timing")
        validate_telemetry(telemetry)
        overhead_percent = (monitor.cpu_ns / elapsed_ns * 100) if elapsed_ns else 100.0
        if overhead_percent >= args.max_overhead_percent:
            raise GateFailure(
                f"harness overhead {overhead_percent:.4f}% is not below "
                f"{args.max_overhead_percent:.4f}%; refusing to report timing"
            )

        result = {
            "format_version": 1,
            "correctness": {
                "fixtures": "36/36",
                "exact_dev_events": args.expected_dev_events,
                "schema": "pass",
                "deterministic_repeat": "pass",
            },
            "benchmark": {
                "wall_time_ns": elapsed_ns,
                "harness_cpu_ns": monitor.cpu_ns,
                "harness_overhead_percent": overhead_percent,
            },
            "capture": {
                "scratch_high_water_bytes": monitor.high_water_bytes,
                "candidate": telemetry,
            },
        }
        print("CORRECTNESS PASS: 36/36 fixtures, exact 1M, schema, deterministic repeat")
        print(json.dumps(result, sort_keys=True))
        print(f"TIMING {elapsed_ns / 1_000_000_000:.6f}s")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except GateFailure as error:
        print(f"V2 BENCHMARK REFUSED: {error}", file=sys.stderr)
        sys.exit(1)
