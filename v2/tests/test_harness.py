#!/usr/bin/env python3
"""Behavioral tests for the correctness-gated benchmark interface."""

import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / "scripts" / "v2_benchmark.py"
SPEC = importlib.util.spec_from_file_location("v2_benchmark", SCRIPT)
BENCHMARK = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BENCHMARK)


class BenchmarkContracts(unittest.TestCase):
    def test_tree_size_counts_nested_regular_file_apparent_bytes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            nested = root / "one" / "two"
            nested.mkdir(parents=True)
            (root / "plain").write_bytes(b"abc")
            sparse = nested / "sparse"
            with sparse.open("wb") as stream:
                stream.seek(1024 * 1024)
                stream.write(b"z")

            self.assertEqual(
                BENCHMARK.tree_size(root),
                3 + 1024 * 1024 + 1,
            )

    def test_tree_size_ignores_symlinks_without_following_directory_links(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            nested = root / "nested"
            nested.mkdir()
            target = nested / "target"
            target.write_bytes(b"12345")
            try:
                (root / "file-link").symlink_to(target)
                (root / "directory-link").symlink_to(nested, target_is_directory=True)
                (root / "broken-link").symlink_to(root / "missing")
            except (NotImplementedError, OSError) as error:
                self.skipTest(f"symlinks unavailable: {error}")

            self.assertEqual(BENCHMARK.tree_size(root), 5)
            self.assertEqual(BENCHMARK.tree_size(root / "directory-link"), 5)

    def test_tree_size_handles_empty_non_directory_and_missing_roots(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            self.assertEqual(BENCHMARK.tree_size(root), 0)
            regular_file = root / "file"
            regular_file.write_bytes(b"not a tree")
            self.assertEqual(BENCHMARK.tree_size(regular_file), 0)
            self.assertEqual(BENCHMARK.tree_size(root / "missing"), 0)

    def test_tree_size_ignores_an_entry_that_disappears_during_scan(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            stable = root / "stable"
            vanished = root / "vanished"
            stable.write_bytes(b"1234")
            vanished.write_bytes(b"12345678")
            real_scandir = BENCHMARK.os.scandir

            class RemovingScan:
                def __init__(self, path):
                    self._entries = real_scandir(path)

                def __enter__(self):
                    return self

                def __exit__(self, *_):
                    self._entries.close()

                def __iter__(self):
                    return self

                def __next__(self):
                    entry = next(self._entries)
                    if entry.name == vanished.name and vanished.exists():
                        vanished.unlink()
                    return entry

            with mock.patch.object(BENCHMARK.os, "scandir", side_effect=RemovingScan):
                self.assertEqual(BENCHMARK.tree_size(root), 4)

    def test_tree_size_treats_scan_errors_as_an_empty_sample(self):
        with mock.patch.object(
            BENCHMARK.os, "scandir", side_effect=PermissionError("unreadable")
        ):
            self.assertEqual(BENCHMARK.tree_size(Path("unreadable")), 0)

    def test_scratch_monitor_retains_peak_after_files_disappear(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            monitor = BENCHMARK.ScratchMonitor(root, interval_seconds=0.005)
            monitor.start()
            peak = root / "peak"
            peak.write_bytes(b"x" * 4096)
            deadline = time.monotonic() + 1
            try:
                while monitor.high_water_bytes < 4096 and time.monotonic() < deadline:
                    time.sleep(0.005)
                peak.unlink()
            finally:
                monitor.stop()

            self.assertEqual(monitor.high_water_bytes, 4096)
            self.assertGreater(monitor.cpu_ns, 0)

    def test_measured_candidate_preserves_output_and_telemetry(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            output = root / "output"
            candidate = root / "candidate.py"
            telemetry_text = '{"format_version": 99, "marker": "same"}'
            candidate.write_text(
                "from pathlib import Path\n"
                "import os\n"
                "Path(os.environ['OUTPUT_DIR'], 'marker').write_bytes(b'same')\n"
                "Path(os.environ['SCRATCH_DIR'], 'spill').write_bytes(b'1234567')\n"
                f"Path(os.environ['V2_TELEMETRY_PATH']).write_text({telemetry_text!r})\n",
                encoding="utf-8",
            )

            elapsed_ns, monitor, telemetry = BENCHMARK.run_candidate(
                [sys.executable, str(candidate)], root, output, measured=True
            )

            self.assertGreater(elapsed_ns, 0)
            self.assertEqual((output / "marker").read_bytes(), b"same")
            self.assertEqual(telemetry, {"format_version": 99, "marker": "same"})
            self.assertEqual(
                monitor.high_water_bytes,
                7 + len(telemetry_text.encode("utf-8")),
            )

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
