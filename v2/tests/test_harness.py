#!/usr/bin/env python3
"""Behavioral tests for the correctness-gated benchmark interface."""

import importlib.util
import json
import os
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

    def test_docker_wrapper_accepts_telemetry_path_inside_caller_scratch(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            data = root / "data"
            output = root / "output"
            scratch = root / "scratch"
            fake_bin = root / "bin"
            data.mkdir()
            scratch.mkdir()
            fake_bin.mkdir()
            docker = fake_bin / "docker"
            docker.write_text(
                """#!/bin/sh
set -eu
scratch=
while [ "$#" -gt 0 ]; do
  if [ "$1" = "-v" ]; then
    shift
    case "$1" in
      *:/scratch:z) scratch=${1%:/scratch:z} ;;
    esac
  fi
  shift
done
test -n "$scratch"
printf spill > "$scratch/spill.bin"
printf '{}\\n' > "$scratch/v2-telemetry.json"
""",
                encoding="utf-8",
            )
            docker.chmod(0o755)
            environment = dict(
                os.environ,
                PATH=f"{fake_bin}{os.pathsep}{os.environ['PATH']}",
                SCRATCH_DIR=str(scratch),
                V2_TELEMETRY_PATH=str(scratch / "v2-telemetry.json"),
            )
            process = subprocess.run(
                [str(ROOT / "v2" / "docker_run.sh"), str(data), str(output)],
                env=environment,
                capture_output=True,
                text=True,
            )
            self.assertEqual(process.returncode, 0, process.stderr)
            self.assertEqual((scratch / "spill.bin").read_bytes(), b"spill")
            self.assertEqual((scratch / "v2-telemetry.json").read_text(), "{}\n")

    def test_telemetry_requires_successful_reconciled_counters(self):
        counters = {
            phase: {
                "discovered_files": 0,
                "input_rows": 0,
                "accepted_rows": 0,
                "dropped_rows": 0,
                "output_rows": 0,
                "bytes_read": 0,
                "bytes_written": 0,
            }
            for phase in BENCHMARK.PHASES
        }
        counters["discovery"]["discovered_files"] = 3
        counters["links"].update(input_rows=10, accepted_rows=10)
        counters["events"].update(
            input_rows=BENCHMARK.DEV_EVENTS,
            accepted_rows=BENCHMARK.DEV_EVENTS - 1,
            dropped_rows=1,
        )
        counters["clicks"].update(input_rows=250, accepted_rows=250)
        counters["write"].update(output_rows=2, bytes_written=100)
        telemetry = {
            "format_version": 1,
            "outcome": "success",
            "phases": [
                {"phase": phase, "elapsed_ns": 0, "counters": counters[phase]}
                for phase in BENCHMARK.PHASES
            ],
            "resources": {
                "cgroup_memory_peak_bytes": 1,
                "scratch_high_water_bytes": 0,
            },
        }
        BENCHMARK.validate_telemetry(
            json.loads(json.dumps(telemetry)), expected_clicks=250
        )

        failed = json.loads(json.dumps(telemetry))
        failed["outcome"] = "failed"
        with self.assertRaises(BENCHMARK.GateFailure):
            BENCHMARK.validate_telemetry(failed, expected_clicks=250)

        empty = json.loads(json.dumps(telemetry))
        for phase in empty["phases"]:
            phase["counters"] = dict.fromkeys(phase["counters"], 0)
        with self.assertRaises(BENCHMARK.GateFailure):
            BENCHMARK.validate_telemetry(empty, expected_clicks=250)

    def test_output_layout_requires_one_canonical_part_per_table(self):
        with tempfile.TemporaryDirectory() as temporary:
            output = Path(temporary)
            for table in BENCHMARK.TABLES:
                folder = output / table
                folder.mkdir()
                (folder / "part-0.parquet").write_bytes(table.encode())
            BENCHMARK.validate_output_layout(output)
            (output / "user_daily" / "unexpected.txt").write_bytes(b"extra")
            with self.assertRaises(BENCHMARK.GateFailure):
                BENCHMARK.validate_output_layout(output)


if __name__ == "__main__":
    unittest.main()
