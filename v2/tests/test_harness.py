#!/usr/bin/env python3
"""Behavioral tests for the correctness-gated benchmark interface."""

import importlib.util
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "v2_benchmark.py"
SPEC = importlib.util.spec_from_file_location("v2_benchmark", SCRIPT)
BENCHMARK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BENCHMARK)


class BenchmarkContracts(unittest.TestCase):
    def test_missing_correctness_inputs_never_print_timing(self):
        with tempfile.TemporaryDirectory() as temporary:
            missing = Path(temporary) / "missing"
            process = subprocess.run(
                [
                    sys.executable,
                    str(SCRIPT),
                    "--candidate",
                    "false",
                    "--fixtures",
                    str(missing),
                    "--dev-data",
                    str(missing),
                    "--dev-answer",
                    str(missing),
                ],
                capture_output=True,
                text=True,
            )
        self.assertNotEqual(process.returncode, 0)
        self.assertIn("V2 BENCHMARK REFUSED", process.stderr)
        self.assertNotIn("TIMING", process.stdout)
        self.assertNotIn("TIMING", process.stderr)

    def test_failed_candidate_timing_is_not_forwarded(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            command = [
                sys.executable,
                "-c",
                "import sys; print('TIMING 0.001s', file=sys.stderr); sys.exit(1)",
            ]
            with self.assertRaises(BENCHMARK.GateFailure) as failure:
                BENCHMARK.run_candidate(command, root, root / "output")
        self.assertNotIn("TIMING", str(failure.exception))
        self.assertNotIn("0.001", str(failure.exception))

    def test_telemetry_requires_all_phases_and_counters(self):
        counters = {
            "discovered_files": 0,
            "input_rows": 0,
            "accepted_rows": 0,
            "dropped_rows": 0,
            "output_rows": 0,
            "bytes_read": 0,
            "bytes_written": 0,
        }
        telemetry = {
            "format_version": 1,
            "outcome": "success",
            "phases": [
                {"phase": phase, "elapsed_ns": 0, "counters": counters}
                for phase in BENCHMARK.PHASES
            ],
            "resources": {
                "cgroup_memory_peak_bytes": 0,
                "scratch_high_water_bytes": 0,
            },
        }
        BENCHMARK.validate_telemetry(json.loads(json.dumps(telemetry)))
        telemetry["phases"].pop()
        with self.assertRaises(BENCHMARK.GateFailure):
            BENCHMARK.validate_telemetry(telemetry)

    def test_output_digest_requires_one_canonical_part_per_table(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            for table in BENCHMARK.TABLES:
                folder = output / table
                folder.mkdir()
                (folder / "part-0.parquet").write_bytes(table.encode())
            first = BENCHMARK.output_digest(output)
            second = BENCHMARK.output_digest(output)
            self.assertEqual(first, second)
            (output / "user_daily" / "part-1.parquet").write_bytes(b"extra")
            with self.assertRaises(BENCHMARK.GateFailure):
                BENCHMARK.output_digest(output)


if __name__ == "__main__":
    unittest.main()
