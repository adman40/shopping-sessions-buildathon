#!/usr/bin/env python3
import csv
import importlib.util
import json
import multiprocessing
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest import mock

MODULE_PATH = Path(__file__).parents[1] / "shopping_bench.py"
SPEC = importlib.util.spec_from_file_location("shopping_bench", MODULE_PATH)
bench = importlib.util.module_from_spec(SPEC)
assert SPEC.loader
SPEC.loader.exec_module(bench)
SHA = "e1c9cc89ffaa51a4047372b1c3d39ceea4068fc0"


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

    def test_manifest_rejects_unlisted_dataset_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            data = root / "data"
            data.mkdir()
            (data / "listed.csv").write_text("listed\n")
            manifest = root / "DATA_SHA256SUMS"
            bench.write_manifest(data, manifest)
            (data / "extra.csv").write_text("extra\n")
            with self.assertRaisesRegex(RuntimeError, "extra.csv"):
                bench.verify_manifest(data, manifest)

    def test_dataset_provenance_requires_every_immutable_identifier(self):
        generation = {
            "schema_version": 1, "generator_sha": bench.BASELINE_SHA,
            "generator_binary_sha256": "1" * 64,
            "dataset_manifest_sha256": "2" * 64,
            "seed": 1, "events": 100_000, "clicks": 25_000, "threads": 8,
        }
        arguments = dict(
            generator_sha=bench.BASELINE_SHA, binary_sha="1" * 64, manifest_sha="2" * 64,
            seed=1, events=100_000, clicks=25_000,
        )
        bench.validate_dataset_provenance(generation, **arguments)
        for field in ("generator_sha", "generator_binary_sha256",
                      "dataset_manifest_sha256", "events"):
            changed = dict(generation)
            changed[field] = "wrong"
            with self.assertRaises(RuntimeError):
                bench.validate_dataset_provenance(changed, **arguments)

    def test_idle_preflight_rejects_similarly_named_unmanaged_container(self):
        running = mock.Mock(stdout="deadbeef shopping-bench-cache\n")
        with mock.patch.object(bench, "run", return_value=running):
            with self.assertRaisesRegex(RuntimeError, "shopping-bench-cache"):
                bench.idle_preflight()

    def test_host_validation_requires_exact_vm_identity_and_capacity(self):
        facts = {
            **bench.EXPECTED_HOST,
            "mem_total_bytes": 31 * 1024**3,
            "cgroup_version": 2,
        }
        bench.validate_host_facts(facts)
        for field, invalid in (
            ("machine_type", "n2-standard-4"),
            ("architecture", "aarch64"),
            ("cpu_model", "different CPU"),
            ("vcpus", 4),
            ("mem_total_bytes", 16 * 1024**3),
        ):
            changed = dict(facts)
            changed[field] = invalid
            with self.assertRaisesRegex(RuntimeError, field):
                bench.validate_host_facts(changed)


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
    def result(self, *, seed=1, events=100_000):
        return {
            "schema_version": bench.SCHEMA_VERSION, "run_id": "test-run",
            "result_status": "eligible-v2-candidate",
            "outcome": "pass", "eligible_timing": True,
            "provenance": {"git_sha": SHA, "image_id": "sha256:" + "1" * 64,
                           "baseline_sha": bench.BASELINE_SHA, "baseline_tree": "a" * 40,
                           "fixtures_tree": "b" * 40,
                           "oracle_image_id": "sha256:" + "5" * 64,
                           "generator_binary_sha256": "2" * 64,
                           "dataset_manifest_sha256": "3" * 64,
                           "oracle_output_manifest_sha256": "6" * 64,
                           "seed": seed, "events": events, "clicks": events // 4},
            "timing": {"class": "warm", "wall_ns": 123},
            "container": {"exit_code": 0, "oom_killed": False},
            "resources": {"memory_peak_bytes": 10, "cpu_usage_usec": 20,
                          "cpu_utilization_percent_of_8": 30.0, "io_read_bytes": 40,
                          "io_write_bytes": 50, "output_peak_allocated_bytes": 60,
                          "scratch_peak_allocated_bytes": 70, "combined_peak_allocated_bytes": 80},
            "correctness": {"fixtures_passed": 36, "fixtures_total": 36,
                            "exact_oracle_match": True},
            "telemetry": {"candidate": {}, "artifact": {"sha256": "4" * 64}},
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
            self.assertEqual(rows[0]["baseline_sha"], bench.BASELINE_SHA)
            self.assertEqual(rows[0]["result_status"], "eligible-v2-candidate")
            self.assertEqual(rows[0]["candidate_telemetry_sha256"], "4" * 64)
            self.assertEqual(rows[0]["exact_oracle_match"], "true")
            self.assertEqual(rows[0]["wall_ns"], "123")
            self.assertFalse(list((root / "run").glob("*.tmp.*")))

    def test_result_finalization_rejects_non_v2_index_schema(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            index = root / "results.csv"
            index.write_text("schema_version,run_id,measurement_label\n")
            with self.assertRaisesRegex(RuntimeError, "unsupported results CSV fields"):
                bench.finalize_result(root / "run", index, self.result())
            self.assertFalse((root / "run/result.json").exists())


class TelemetryTests(unittest.TestCase):
    @staticmethod
    def telemetry():
        phases = []
        for phase in bench.PHASES:
            counters = {
                "discovered_files": 0, "input_rows": 0, "accepted_rows": 0,
                "dropped_rows": 0, "output_rows": 0, "bytes_read": 0,
                "bytes_written": 0,
            }
            if phase == "discovery":
                counters["discovered_files"] = 4
            elif phase == "links":
                counters.update(input_rows=1, accepted_rows=1)
            elif phase == "events":
                counters.update(input_rows=100_000, accepted_rows=100_000)
            elif phase == "clicks":
                counters.update(input_rows=25_000, accepted_rows=25_000)
            elif phase == "write":
                counters.update(output_rows=2, bytes_written=512)
            phases.append({"phase": phase, "elapsed_ns": 1, "counters": counters})
        return {
            "format_version": 1, "outcome": "success", "phases": phases,
            "resources": {"cgroup_memory_peak_bytes": 1024,
                          "scratch_high_water_bytes": 2048},
        }

    def test_v2_telemetry_is_validated_ingested_and_checksummed(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            scratch, run_dir = root / "scratch", root / "run"
            scratch.mkdir()
            payload = (json.dumps(self.telemetry(), sort_keys=True) + "\n").encode()
            (scratch / "v2-telemetry.json").write_bytes(payload)
            value, artifact = bench.ingest_candidate_telemetry(
                scratch, run_dir, 100_000, 25_000
            )
            self.assertEqual(value["outcome"], "success")
            self.assertTrue(artifact["required"])
            self.assertTrue(artifact["present"])
            self.assertTrue(artifact["valid"])
            self.assertEqual((run_dir / artifact["path"]).read_bytes(), payload)
            self.assertEqual(artifact["sha256"], bench.sha256_file(run_dir / artifact["path"]))

    def test_invalid_or_missing_v2_telemetry_is_ineligible_but_preserved(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            scratch, run_dir = root / "scratch", root / "run"
            scratch.mkdir()
            telemetry = self.telemetry()
            telemetry["outcome"] = "failure"
            (scratch / "v2-telemetry.json").write_text(json.dumps(telemetry))
            _, artifact = bench.ingest_candidate_telemetry(
                scratch, run_dir, 100_000, 25_000
            )
            self.assertFalse(artifact["valid"])
            self.assertIn("outcome must be success", artifact["validation_error"])
            self.assertTrue((run_dir / artifact["path"]).is_file())

            _, missing = bench.ingest_candidate_telemetry(
                root / "empty", root / "second-run", 100_000, 25_000
            )
            self.assertFalse(missing["valid"])
            self.assertFalse(missing["present"])

class OracleTests(unittest.TestCase):
    def test_trusted_oracle_uses_nonqualification_timeout(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            run_dir = root / "run"
            output = run_dir / "oracle-output"
            output.mkdir(parents=True)
            (output / "part.parquet").write_bytes(b"oracle")
            process = mock.Mock()

            def wait(timeout):
                if timeout == bench.TIMEOUT_SECONDS:
                    raise AssertionError("oracle used candidate qualification timeout")
                return 0

            process.wait.side_effect = wait
            state = {"State": {"ExitCode": 0, "OOMKilled": False}}
            with (
                mock.patch.object(bench, "create_container", return_value="container"),
                mock.patch.object(bench.subprocess, "Popen", return_value=process),
                mock.patch.object(bench, "run", return_value=mock.Mock(stdout=json.dumps([state]))),
                mock.patch.object(bench, "check_output", return_value={"ok": True}),
            ):
                authorized, metadata = bench.trusted_oracle_run(
                    "oracle-image", root / "data", run_dir, "run-id"
                )
            self.assertEqual(authorized, output)
            process.wait.assert_called_once_with(timeout=bench.ORACLE_TIMEOUT_SECONDS)
            self.assertEqual(metadata["timeout_seconds"], bench.ORACLE_TIMEOUT_SECONDS)
            self.assertGreater(bench.ORACLE_TIMEOUT_SECONDS, bench.TIMEOUT_SECONDS)


class QualificationTests(unittest.TestCase):
    @staticmethod
    def passing_inputs():
        return {
            "timed_out": False,
            "wall_ns": bench.TIMEOUT_NS - 1,
            "exit_code": 0,
            "oom_killed": False,
            "oom_event": False,
            "memory_peak_bytes": bench.MEMORY_QUALIFY_BYTES - 1,
            "combined_peak_allocated_bytes": bench.DISK_STOP_BYTES - 1,
            "disk_threshold_exceeded": False,
            "fixtures_ok": True,
            "exact_oracle_match": True,
            "telemetry_valid": True,
        }

    def test_eligibility_requires_exact_oracle_match(self):
        values = self.passing_inputs()
        self.assertEqual(bench.qualification_outcome(**values), (True, "pass"))
        values["exact_oracle_match"] = False
        self.assertEqual(
            bench.qualification_outcome(**values),
            (False, "correctness_fail"),
        )

    def test_eligibility_enforces_fixed_time_and_resource_thresholds(self):
        for field, value, outcome in (
            ("wall_ns", bench.TIMEOUT_NS, "timeout"),
            ("memory_peak_bytes", None, "resource_fail"),
            ("memory_peak_bytes", bench.MEMORY_QUALIFY_BYTES, "resource_fail"),
            ("combined_peak_allocated_bytes", bench.DISK_STOP_BYTES, "resource_fail"),
        ):
            values = self.passing_inputs()
            values[field] = value
            self.assertEqual(
                bench.qualification_outcome(**values),
                (False, outcome),
            )


class ProgressiveGateTests(unittest.TestCase):
    def test_ordinary_tiers_require_same_seed_predecessor(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "results").mkdir()
            with self.assertRaises(RuntimeError):
                bench.ensure_progressive_gate(root, SHA, 1, 1_000_000)
            result = ResultTests().result(seed=1, events=100_000)
            bench.finalize_result(root / "run", root / "results/results.csv", result)
            bench.ensure_progressive_gate(root, SHA, 1, 1_000_000)
            with self.assertRaises(RuntimeError):
                bench.ensure_progressive_gate(root, SHA, 2, 1_000_000)

    def test_release_seeds_require_full_seed_one_progression(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            index = root / "results/results.csv"
            for events in bench.TIERS[:-1]:
                result = ResultTests().result(seed=1, events=events)
                bench.finalize_result(root / f"run-{events}", index, result)
            bench.ensure_progressive_gate(root, SHA, 2, 60_000_000)
            bench.ensure_progressive_gate(root, SHA, 3, 60_000_000)
            with self.assertRaises(RuntimeError):
                bench.ensure_progressive_gate(root, SHA, 4, 60_000_000)

    def test_release_seed_rejects_incomplete_seed_one_progression(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            result = ResultTests().result(seed=1, events=30_000_000)
            bench.finalize_result(root / "run", root / "results/results.csv", result)
            with self.assertRaisesRegex(RuntimeError, "predecessor tiers"):
                bench.ensure_progressive_gate(root, SHA, 2, 60_000_000)


class CommandTests(unittest.TestCase):
    def test_checker_rejects_unsigned_aggregate_columns(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            table = root / "user_daily"
            modules = root / "modules"
            table.mkdir()
            modules.mkdir()
            (table / "part-0.parquet").write_bytes(b"unsigned fixture")
            (modules / "polars.py").write_text(
                """class Type:
 def __init__(self, name, signed=False): self.name, self.signed = name, signed
 def is_signed_integer(self): return self.signed
 def base_type(self): return self
 def __repr__(self): return self.name
String = Type('String')
Date = Type('Date')
Int128 = Type('Int128', True)
UInt64 = Type('UInt64')
Categorical = Type('Categorical')
Enum = Type('Enum')
class Frame:
 columns = ['user_id', 'day', 'sessions', 'events', 'revenue_cents', 'top_category']
 schema = {'user_id': String, 'day': Date, 'sessions': UInt64, 'events': Int128, 'revenue_cents': Int128, 'top_category': String}
def read_parquet(path): return Frame()
def concat(frames): return frames[0]
""",
                encoding="utf-8",
            )
            environment = dict(
                os.environ,
                PYTHONPATH=f"{modules}{os.pathsep}{os.environ.get('PYTHONPATH', '')}",
            )
            completed = subprocess.run(
                [sys.executable, "-c", bench.CHECKER, str(root)],
                env=environment, capture_output=True, text=True,
            )
            result = json.loads(completed.stdout)
            self.assertNotEqual(completed.returncode, 0)
            self.assertFalse(result["ok"])
            self.assertIn("wrong type user_daily.sessions", result["error"])

    def test_fixture_gate_requires_exact_published_count_before_running(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "fixtures").mkdir()
            for index in range(35):
                (root / "fixtures" / f"case-{index}").mkdir()
            result = bench.fixture_gates(root, "candidate", "checker", root / "work")
            self.assertFalse(result["ok"])
            self.assertEqual(result["total"], 35)
            self.assertIn("expected exactly 36 fixtures", result["failures"][0]["error"])

    def test_candidate_build_uses_v2_dockerfile_and_immutable_labels(self):
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary)
            (source / "v2").mkdir()
            (source / "v2/Dockerfile").write_text("FROM scratch\n")
            calls = []

            def fake_run(command, **_kwargs):
                calls.append(command)
                if command[1:3] == ["image", "inspect"]:
                    value = [{
                        "Id": "sha256:" + "5" * 64,
                        "Config": {"Labels": {
                            "org.opencontainers.image.revision": SHA,
                            "org.shopping-bench.role": "candidate",
                        }},
                    }]
                    return mock.Mock(stdout=json.dumps(value))
                return mock.Mock(stdout="")

            with mock.patch.object(bench, "run", side_effect=fake_run):
                image_id, metadata = bench.build_image(
                    source, SHA, "v2/Dockerfile", "candidate"
                )
            build = calls[0]
            self.assertEqual(build[build.index("--file") + 1], str(source / "v2/Dockerfile"))
            self.assertEqual(build[-1], str(source))
            self.assertIn(f"org.opencontainers.image.revision={SHA}", build)
            self.assertEqual(image_id, "sha256:" + "5" * 64)
            self.assertEqual(metadata["dockerfile"], "v2/Dockerfile")

    def test_container_verification_rejects_additional_mounts(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            data, output, scratch = root / "data", root / "output", root / "scratch"
            info = {
                "HostConfig": {
                    "NetworkMode": "none", "ReadonlyRootfs": True,
                    "Memory": bench.MEMORY_BYTES, "MemorySwap": bench.MEMORY_BYTES,
                    "PidsLimit": 4096, "CapDrop": ["ALL"],
                    "SecurityOpt": ["no-new-privileges"], "Tmpfs": {"/tmp": "rw"},
                    "NanoCpus": 8_000_000_000,
                },
                "Config": {
                    "User": "1000:1000",
                    "Env": [
                        "DATA_DIR=/data", "OUTPUT_DIR=/output", "SCRATCH_DIR=/scratch",
                        "HOME=/scratch", "NCPU=8", "MEM_GB=24",
                        "V2_TELEMETRY_PATH=/scratch/v2-telemetry.json",
                    ],
                },
                "Mounts": [
                    {"Destination": "/data", "Source": str(data.resolve()), "RW": False},
                    {"Destination": "/output", "Source": str(output.resolve()), "RW": True},
                    {"Destination": "/scratch", "Source": str(scratch.resolve()), "RW": True},
                ],
            }
            with mock.patch.object(
                bench, "run",
                side_effect=lambda *_args, **_kwargs: mock.Mock(stdout=json.dumps([info])),
            ):
                bench.verify_container(
                    "container", data, output, scratch, telemetry_required=True
                )
                info["Mounts"].append({
                    "Destination": "/cache", "Source": "/var/lib/docker/cache", "RW": True,
                })
                with self.assertRaisesRegex(RuntimeError, "mounts"):
                    bench.verify_container(
                        "container", data, output, scratch, telemetry_required=True
                    )

    def test_dry_run_constructs_exact_v2_constraints(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            args = bench.parse_args(["--root", str(root), "plan", "--sha", SHA,
                                     "--events", "100000", "--clicks", "25000"])
            command = bench.command_for_container(
                "sha256:" + "0" * 64, "name", root / "data", root / "out",
                root / "scratch", telemetry_required=True
            )
            joined = " ".join(command)
            for required in ("--platform linux/amd64", "--network none", "--read-only",
                             "/tmp:rw,size=64m,mode=1777", "--cap-drop ALL",
                             "--security-opt no-new-privileges", "--pids-limit 4096",
                             "--cpus 8", "--memory 24g", "--memory-swap 24g",
                             "--user 1000:1000", "DATA_DIR=/data", "NCPU=8", "MEM_GB=24",
                             "V2_TELEMETRY_PATH=/scratch/v2-telemetry.json"):
                self.assertIn(required, joined)
            self.assertEqual(args.sha, SHA)

    def test_campaign_rejects_removed_modes_and_gate_overrides(self):
        common = ["campaign", "--sha", SHA, "--events", "100000", "--clicks", "25000"]
        for removed in (
            ["--mode", "v1-smoke"],
            ["--idle-seconds", "0"],
            ["--timeout-seconds", "300"],
            ["--label", "manual"],
        ):
            with self.assertRaises(SystemExit):
                bench.parse_args(common + removed)

    def test_campaign_sources_validation_workload_from_pinned_baseline(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            candidate_source = root / "candidate"
            baseline_source = root / "baseline"
            data = root / "dataset/data"
            args = mock.Mock(
                sha=SHA, events=100_000, clicks=25_000,
                seed=1, root=str(root),
            )
            candidate_image = "sha256:" + "1" * 64
            oracle_image = "sha256:" + "2" * 64
            image_meta = {"inspect": {}, "dockerfile": "v2/Dockerfile"}
            oracle_meta = {"inspect": {}, "dockerfile": "Dockerfile"}
            result = {"run_id": "run", "outcome": "pass", "eligible_timing": True}
            with (
                mock.patch.object(bench, "recover"),
                mock.patch.object(bench, "ensure_progressive_gate"),
                mock.patch.object(bench, "host_facts", return_value={}),
                mock.patch.object(
                    bench, "source_checkout",
                    side_effect=[(candidate_source, "candidate-tree"),
                                 (baseline_source, "baseline-tree")],
                ) as checkout,
                mock.patch.object(
                    bench, "checkout_tree", side_effect=["fixtures-tree", "generator-tree"]
                ),
                mock.patch.object(
                    bench, "build_generator", return_value=(root / "generate", "binary-sha")
                ) as generator,
                mock.patch.object(
                    bench, "prepare_dataset", return_value=(data, {"generator_sha": bench.BASELINE_SHA})
                ) as dataset,
                mock.patch.object(
                    bench, "build_image",
                    side_effect=[(candidate_image, image_meta), (oracle_image, oracle_meta)],
                ),
                mock.patch.object(
                    bench, "fixture_gates",
                    return_value={"passed": 36, "total": 36, "failures": [], "ok": True},
                ) as fixtures,
                mock.patch.object(bench, "timed_run", return_value=(result, root / "runs/run")),
            ):
                self.assertEqual(bench.campaign(args), 0)
            self.assertEqual(
                checkout.call_args_list,
                [mock.call(root, SHA), mock.call(root, bench.BASELINE_SHA)],
            )
            generator.assert_called_once_with(root, baseline_source, bench.BASELINE_SHA)
            self.assertEqual(dataset.call_args.args[2], bench.BASELINE_SHA)
            self.assertEqual(fixtures.call_args.args[0], baseline_source)
            self.assertEqual(fixtures.call_args.args[2], oracle_image)


if __name__ == "__main__":
    unittest.main()
