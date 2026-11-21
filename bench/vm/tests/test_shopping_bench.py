#!/usr/bin/env python3
import csv
import importlib.util
import json
import multiprocessing
from pathlib import Path
import tempfile
import unittest

MODULE_PATH = Path(__file__).parents[1] / "shopping_bench.py"
SPEC = importlib.util.spec_from_file_location("shopping_bench", MODULE_PATH)
bench = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
SPEC.loader.exec_module(bench)
SHA = "8a35b964aed48ecba5e44d061606b2a4bb9a2239"


def hold_lock(path: str, ready: multiprocessing.Event, release: multiprocessing.Event) -> None:
    with bench.BenchLock(Path(path)):
        ready.set()
        release.wait(5)


class ValidationTests(unittest.TestCase):
    def test_requires_full_lowercase_sha(self):
        self.assertEqual(bench.validate_sha(SHA), SHA)
        for invalid in (SHA[:12], SHA.upper(), "main", "g" * 40):
            with self.assertRaises(ValueError):
                bench.validate_sha(invalid)

    def test_requires_progressive_scale_and_click_ratio(self):
        bench.validate_scale(100_000, 25_000)
        with self.assertRaises(ValueError):
            bench.validate_scale(99_999, 25_000)
        with self.assertRaises(ValueError):
            bench.validate_scale(100_000, 24_999)

    def test_docker_timestamp_preserves_nanoseconds(self):
        start = bench.docker_timestamp_ns("2026-11-21T02:54:00.560882343Z")
        finish = bench.docker_timestamp_ns("2026-11-21T02:54:02.690844267Z")
        self.assertEqual(finish - start, 2_129_961_924)

    def test_provenance_rejects_dirty_wrong_head_and_origin(self):
        bench.validate_checkout_provenance(SHA, SHA, "")
        for head, dirty, origin in (
            ("0" * 40, "", bench.REPO_URL),
            (SHA, " M pipeline/pipeline.py\n", bench.REPO_URL),
            (SHA, "", "https://example.invalid/repo.git"),
        ):
            with self.assertRaises(RuntimeError):
                bench.validate_checkout_provenance(SHA, head, dirty, origin)


class LockTests(unittest.TestCase):
    def test_second_process_is_refused(self):
        with tempfile.TemporaryDirectory() as temporary:
            lock = str(Path(temporary) / "run.lock")
            ready = multiprocessing.Event()
            release = multiprocessing.Event()
            process = multiprocessing.Process(target=hold_lock, args=(lock, ready, release))
            process.start()
            self.assertTrue(ready.wait(3))
            try:
                with self.assertRaises(bench.BusyError):
                    with bench.BenchLock(Path(lock)):
                        pass
            finally:
                release.set()
                process.join(3)
            self.assertEqual(process.exitcode, 0)


class ResultTests(unittest.TestCase):
    def result(self):
        return {
            "schema_version": 1, "run_id": "test-run", "measurement_label": "V1 smoke measurement",
            "outcome": "pass", "eligible_timing": True,
            "provenance": {"git_sha": SHA, "image_id": "sha256:" + "1" * 64,
                           "generator_binary_sha256": "2" * 64,
                           "dataset_manifest_sha256": "3" * 64,
                           "seed": 1, "events": 100_000, "clicks": 25_000},
            "timing": {"class": "warm", "wall_ns": 123},
            "container": {"exit_code": 0, "oom_killed": False},
            "resources": {"memory_peak_bytes": 10, "cpu_usage_usec": 20,
                          "cpu_utilization_percent_of_8": 30.0, "io_read_bytes": 40,
                          "io_write_bytes": 50, "output_peak_allocated_bytes": 60,
                          "scratch_peak_allocated_bytes": 70, "combined_peak_allocated_bytes": 80},
            "correctness": {"fixtures_passed": 36, "fixtures_total": 36, "self_check": True},
            "parity": {"combined_disk_cap": "monitor-only-no-hard-cap"},
        }

    def test_result_finalization_writes_authoritative_json_and_csv(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            result = self.result()
            bench.finalize_result(root / "run", root / "results.csv", result)
            self.assertEqual(json.loads((root / "run/result.json").read_text()), result)
            with (root / "results.csv").open(newline="") as handle:
                rows = list(csv.DictReader(handle))
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["git_sha"], SHA)
            self.assertEqual(rows[0]["wall_ns"], "123")
            self.assertFalse(list((root / "run").glob("*.tmp.*")))


class ProgressiveGateTests(unittest.TestCase):
    def test_requires_prior_passing_tier(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "results").mkdir()
            with self.assertRaises(RuntimeError):
                bench.ensure_progressive_gate(root, SHA, 1, 1_000_000)
            result = ResultTests().result()
            bench.finalize_result(root / "run", root / "results/results.csv", result)
            bench.ensure_progressive_gate(root, SHA, 1, 1_000_000)


class CommandTests(unittest.TestCase):
    def test_dry_run_constructs_exact_constraints(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            args = bench.parse_args(["--root", str(root), "plan", "--sha", SHA,
                                     "--events", "100000", "--clicks", "25000"])
            command = bench.command_for_container("sha256:" + "0" * 64, "name",
                                                  root / "data", root / "out", root / "scratch")
            joined = " ".join(command)
            for required in ("--platform linux/amd64", "--network none", "--read-only",
                             "/tmp:rw,size=64m,mode=1777", "--cap-drop ALL",
                             "--security-opt no-new-privileges", "--pids-limit 4096",
                             "--cpus 8", "--memory 24g", "--memory-swap 24g",
                             "--user 1000:1000", "DATA_DIR=/data", "NCPU=8", "MEM_GB=24"):
                self.assertIn(required, joined)
            self.assertEqual(args.sha, SHA)


if __name__ == "__main__":
    unittest.main()
