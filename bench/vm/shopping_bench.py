#!/usr/bin/env python3
"""Manual, serialized benchmark runner for the dedicated GCP VM.

The scored boundary is CLOCK_MONOTONIC immediately before ``docker start -a``
through observed container exit. Image builds, fixture checks, dataset generation,
and result checks are deliberately outside that boundary.
"""

from __future__ import annotations

import argparse
import calendar
import csv
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import threading
import time
from typing import Any
import urllib.error
import urllib.request

SCHEMA_VERSION = 2
REPO_URL = "https://github.com/adman40/shopping-sessions-buildathon.git"
BASELINE_SHA = "8a35b964aed48ecba5e44d061606b2a4bb9a2239"
SHA_RE = re.compile(r"^[0-9a-f]{40}$")
TIERS = (100_000, 1_000_000, 5_000_000, 15_000_000, 30_000_000, 60_000_000)
MEMORY_BYTES = 24 * 1024**3
MEMORY_QUALIFY_BYTES = 22 * 1024**3
TMPFS_BYTES = 64 * 1024**2
DISK_STOP_BYTES = 45 * 1024**3
IDLE_SECONDS = 5.0
TIMEOUT_SECONDS = 180.0
TIMEOUT_NS = int(TIMEOUT_SECONDS * 1_000_000_000)
EXPECTED_FIXTURES = 36
EXPECTED_CPU_MODEL = "Intel(R) Xeon(R) CPU @ 2.80GHz"
EXPECTED_HOST = {
    "instance": "benchmark-x86",
    "project": "buildathon-benchmarks-515401",
    "zone": "us-central1-a",
    "machine_type": "n2-standard-8",
    "architecture": "x86_64",
    "cpu_model": EXPECTED_CPU_MODEL,
    "vcpus": 8,
}
PHASES = ("discovery", "links", "events", "clicks", "sort", "sweep", "write")
CSV_FIELDS = (
    "schema_version", "run_id", "result_status", "outcome", "eligible_timing",
    "git_sha", "image_id", "baseline_sha", "baseline_tree", "fixtures_tree",
    "oracle_image_id", "generator_binary_sha256", "dataset_manifest_sha256",
    "oracle_output_manifest_sha256", "candidate_telemetry_sha256",
    "seed", "events", "clicks", "cache_class", "wall_ns", "exit_code", "oom_killed",
    "memory_peak_bytes", "cpu_usage_usec", "cpu_utilization_percent_of_8",
    "io_read_bytes", "io_write_bytes", "output_peak_allocated_bytes",
    "scratch_peak_allocated_bytes", "combined_peak_allocated_bytes",
    "fixtures_passed", "fixtures_total", "self_check", "disk_cap_parity",
)


def run(cmd: list[str], *, check: bool = True, capture: bool = True,
        cwd: Path | None = None, env: dict[str, str] | None = None,
        timeout: float | None = None) -> subprocess.CompletedProcess[str]:
    return subprocess.run(cmd, check=check, text=True, capture_output=capture,
                          cwd=cwd, env=env, timeout=timeout)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def artifact_metadata(path: Path, relative_to: Path) -> dict[str, Any]:
    return {
        "path": path.relative_to(relative_to).as_posix(),
        "sha256": sha256_file(path),
        "size_bytes": path.stat().st_size,
    }


def validate_sha(value: str) -> str:
    if not SHA_RE.fullmatch(value):
        raise ValueError("--sha must be a lower-case, full 40-character commit SHA")
    return value


def validate_scale(events: int, clicks: int) -> None:
    if events not in TIERS:
        raise ValueError(f"--events must be one of {', '.join(map(str, TIERS))}")
    if clicks != events // 4:
        raise ValueError("--clicks must equal --events / 4")


def command_for_container(image: str, name: str, data: Path, output: Path,
                          scratch: Path, *, telemetry_required: bool) -> list[str]:
    """Return the single authoritative scorer-equivalent create command."""
    command = [
        "docker", "create", "--name", name,
        "--label", "org.shopping-bench.managed=true",
        "--label", f"org.shopping-bench.run={name}",
        "--platform", "linux/amd64", "--network", "none", "--read-only",
        "--tmpfs", "/tmp:rw,size=64m,mode=1777", "--cap-drop", "ALL",
        "--security-opt", "no-new-privileges", "--pids-limit", "4096",
        "--cpus", "8", "--memory", "24g", "--memory-swap", "24g",
        "--user", "1000:1000",
        "--mount", f"type=bind,src={data.resolve()},dst=/data,readonly",
        "--mount", f"type=bind,src={output.resolve()},dst=/output",
        "--mount", f"type=bind,src={scratch.resolve()},dst=/scratch",
        "--env", "DATA_DIR=/data", "--env", "OUTPUT_DIR=/output",
        "--env", "SCRATCH_DIR=/scratch", "--env", "HOME=/scratch",
        "--env", "NCPU=8", "--env", "MEM_GB=24",
    ]
    if telemetry_required:
        command += ["--env", "V2_TELEMETRY_PATH=/scratch/v2-telemetry.json"]
    command.append(image)
    return command


class BusyError(RuntimeError):
    pass


class BenchLock:
    def __init__(self, path: Path):
        self.path = path
        self.handle: Any = None

    def __enter__(self) -> "BenchLock":
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("a+")
        try:
            fcntl.flock(self.handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            self.handle.close()
            raise BusyError(f"benchmark lock is held: {self.path}") from error
        self.handle.seek(0)
        self.handle.truncate()
        self.handle.write(f"pid={os.getpid()}\n")
        self.handle.flush()
        return self

    def __exit__(self, *_: object) -> None:
        fcntl.flock(self.handle, fcntl.LOCK_UN)
        self.handle.close()


def atomic_bytes(path: Path, value: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.tmp.{os.getpid()}")
    with temporary.open("wb") as handle:
        handle.write(value)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def atomic_json(path: Path, value: dict[str, Any]) -> None:
    encoded = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")
    atomic_bytes(path, encoded)


def flatten_result(result: dict[str, Any]) -> dict[str, Any]:
    provenance = result["provenance"]
    resources = result["resources"]
    correctness = result["correctness"]
    timing = result["timing"]
    container = result["container"]
    return {
        "schema_version": result["schema_version"], "run_id": result["run_id"],
        "result_status": result["result_status"], "outcome": result["outcome"],
        "eligible_timing": str(result["eligible_timing"]).lower(),
        "git_sha": provenance["git_sha"], "image_id": provenance["image_id"],
        "baseline_sha": provenance["baseline_sha"], "baseline_tree": provenance["baseline_tree"],
        "fixtures_tree": provenance["fixtures_tree"],
        "oracle_image_id": provenance["oracle_image_id"],
        "generator_binary_sha256": provenance["generator_binary_sha256"],
        "dataset_manifest_sha256": provenance["dataset_manifest_sha256"],
        "oracle_output_manifest_sha256": provenance["oracle_output_manifest_sha256"],
        "candidate_telemetry_sha256": result["telemetry"]["artifact"].get("sha256", ""),
        "seed": provenance["seed"], "events": provenance["events"], "clicks": provenance["clicks"],
        "cache_class": timing["class"], "wall_ns": timing["wall_ns"],
        "exit_code": container["exit_code"], "oom_killed": str(container["oom_killed"]).lower(),
        "memory_peak_bytes": resources.get("memory_peak_bytes"),
        "cpu_usage_usec": resources.get("cpu_usage_usec"),
        "cpu_utilization_percent_of_8": resources.get("cpu_utilization_percent_of_8"),
        "io_read_bytes": resources.get("io_read_bytes"), "io_write_bytes": resources.get("io_write_bytes"),
        "output_peak_allocated_bytes": resources["output_peak_allocated_bytes"],
        "scratch_peak_allocated_bytes": resources["scratch_peak_allocated_bytes"],
        "combined_peak_allocated_bytes": resources["combined_peak_allocated_bytes"],
        "fixtures_passed": correctness["fixtures_passed"], "fixtures_total": correctness["fixtures_total"],
        "self_check": str(correctness["self_check"]).lower(),
        "disk_cap_parity": result["parity"]["combined_disk_cap"],
    }


def validate_results_csv(results_csv: Path) -> None:
    if not results_csv.exists():
        return
    with results_csv.open(newline="", encoding="utf-8") as handle:
        fields = tuple(csv.DictReader(handle).fieldnames or ())
    if fields != CSV_FIELDS:
        raise RuntimeError(f"unsupported results CSV fields: {fields}")


def finalize_result(run_dir: Path, results_csv: Path, result: dict[str, Any]) -> None:
    """Atomically finalize authoritative JSON, then append its CSV projection."""
    validate_results_csv(results_csv)
    atomic_json(run_dir / "result.json", result)
    row = flatten_result(result)
    results_csv.parent.mkdir(parents=True, exist_ok=True)
    new_file = not results_csv.exists()
    with results_csv.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        if new_file:
            writer.writeheader()
        writer.writerow(row)
        handle.flush()
        os.fsync(handle.fileno())
    local_csv = run_dir / "result.csv"
    temporary = local_csv.with_name(f".{local_csv.name}.tmp.{os.getpid()}")
    with temporary.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerow(row)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, local_csv)


def ensure_layout(root: Path) -> None:
    for item in ("checkouts", "datasets", "generators", "images", "runs", "results", "locks", "tmp"):
        (root / item).mkdir(parents=True, exist_ok=True)


def validate_checkout_provenance(requested_sha: str, head_sha: str, dirty: str,
                                 origin: str = REPO_URL) -> None:
    validate_sha(requested_sha)
    if origin != REPO_URL:
        raise RuntimeError(f"checkout origin mismatch: {origin}")
    if head_sha != requested_sha or dirty:
        raise RuntimeError("source checkout is not the requested clean commit")


def source_checkout(root: Path, sha: str) -> tuple[Path, str]:
    source = root / "checkouts" / sha
    if not source.exists():
        run(["git", "clone", "--no-checkout", "--filter=blob:none", REPO_URL, str(source)])
    origin = run(["git", "-C", str(source), "remote", "get-url", "origin"]).stdout.strip()
    if origin != REPO_URL:
        raise RuntimeError(f"checkout origin mismatch: {origin}")
    env = dict(os.environ, GIT_TERMINAL_PROMPT="0")
    run(["git", "-c", "credential.helper=", "-C", str(source), "fetch", "--no-tags", "--force",
         "origin", sha], env=env)
    run(["git", "-C", str(source), "checkout", "--detach", "--force", sha])
    head = run(["git", "-C", str(source), "rev-parse", "HEAD"]).stdout.strip()
    dirty = run(["git", "-C", str(source), "status", "--porcelain"]).stdout
    validate_checkout_provenance(sha, head, dirty, origin)
    tree = run(["git", "-C", str(source), "rev-parse", "HEAD^{tree}"]).stdout.strip()
    return source, tree


def checkout_tree(source: Path, path: str) -> str:
    return run(["git", "-C", str(source), "rev-parse", f"HEAD:{path}"]).stdout.strip()


def verify_live_checkout(source: Path, sha: str) -> None:
    head = run(["git", "-C", str(source), "rev-parse", "HEAD"]).stdout.strip()
    dirty = run(["git", "-C", str(source), "status", "--porcelain"]).stdout
    validate_checkout_provenance(sha, head, dirty)


def build_generator(root: Path, source: Path, sha: str) -> tuple[Path, str]:
    target = root / "generators" / sha
    binary = target / "release" / "generate"
    cargo = shutil.which("cargo") or str(Path.home() / ".cargo" / "bin" / "cargo")
    if not Path(cargo).is_file():
        raise RuntimeError("cargo was not found on PATH or under ~/.cargo/bin")
    env = dict(os.environ, CARGO_TARGET_DIR=str(target))
    run([cargo, "build", "--release", "--locked", "--manifest-path",
         str(source / "generator" / "Cargo.toml")], env=env, timeout=1800)
    return binary, sha256_file(binary)


def write_manifest(data: Path, manifest: Path) -> str:
    lines = []
    entries = sorted(data.rglob("*"))
    symlinks = [path.relative_to(data).as_posix() for path in entries if path.is_symlink()]
    if symlinks:
        raise RuntimeError(f"dataset contains symbolic links: {symlinks}")
    for path in (item for item in entries if item.is_file()):
        relative = path.relative_to(data).as_posix()
        lines.append(f"{sha256_file(path)}  {path.stat().st_size}  {relative}\n")
    if not lines:
        raise RuntimeError("generated dataset is empty")
    manifest.write_text("".join(lines), encoding="utf-8")
    return sha256_file(manifest)


def verify_manifest(data: Path, manifest: Path) -> str:
    lines = manifest.read_text(encoding="utf-8").splitlines()
    if not lines:
        raise RuntimeError("dataset manifest is empty")
    listed: set[str] = set()
    for line in lines:
        digest, size, relative = line.split("  ", 2)
        relative_path = Path(relative)
        if relative_path.is_absolute() or ".." in relative_path.parts:
            raise RuntimeError(f"dataset manifest has an unsafe path: {relative}")
        normalized = relative_path.as_posix()
        if normalized in listed:
            raise RuntimeError(f"dataset manifest repeats a path: {relative}")
        listed.add(normalized)
        path = data / relative_path
        if not path.is_file() or path.stat().st_size != int(size) or sha256_file(path) != digest:
            raise RuntimeError(f"dataset checksum mismatch: {relative}")
    entries = list(data.rglob("*"))
    symlinks = [path.relative_to(data).as_posix() for path in entries if path.is_symlink()]
    if symlinks:
        raise RuntimeError(f"dataset contains symbolic links: {symlinks}")
    actual = {
        path.relative_to(data).as_posix()
        for path in entries
        if path.is_file()
    }
    if listed != actual:
        missing = sorted(listed - actual)
        extra = sorted(actual - listed)
        raise RuntimeError(f"dataset manifest path mismatch: missing={missing}, extra={extra}")
    return sha256_file(manifest)


def validate_dataset_provenance(generation: dict[str, Any], *, generator_sha: str,
                                binary_sha: str, manifest_sha: str, seed: int,
                                events: int, clicks: int) -> None:
    expected = {
        "generator_sha": generator_sha,
        "generator_binary_sha256": binary_sha,
        "dataset_manifest_sha256": manifest_sha,
        "seed": seed,
        "events": events,
        "clicks": clicks,
        "threads": 8,
    }
    mismatches = [key for key, value in expected.items() if generation.get(key) != value]
    if generation.get("schema_version") != 1:
        mismatches.append("schema_version")
    if mismatches:
        raise RuntimeError("existing dataset provenance mismatch: " + ", ".join(mismatches))


def prepare_dataset(root: Path, binary: Path, generator_sha: str, binary_sha: str,
                    seed: int, events: int, clicks: int) -> tuple[Path, dict[str, Any]]:
    dataset_id = f"g{generator_sha}-s{seed}-e{events}-c{clicks}"
    dataset = root / "datasets" / dataset_id
    data = dataset / "data"
    manifest = dataset / "DATA_SHA256SUMS"
    if dataset.exists():
        digest = verify_manifest(data, manifest)
        generation = json.loads((dataset / "generation.json").read_text(encoding="utf-8"))
        validate_dataset_provenance(
            generation, generator_sha=generator_sha, binary_sha=binary_sha,
            manifest_sha=digest, seed=seed, events=events, clicks=clicks,
        )
        return data, generation
    temporary = root / "tmp" / f"{dataset_id}.partial.{os.getpid()}"
    shutil.rmtree(temporary, ignore_errors=True)
    (temporary / "data").mkdir(parents=True)
    started = time.time_ns()
    argv = [str(binary), "--threads", "8", "--seed", str(seed), "--events", str(events),
            "--clicks", str(clicks), "--output", str(temporary / "data")]
    run(argv, timeout=1800)
    digest = write_manifest(temporary / "data", temporary / "DATA_SHA256SUMS")
    generation = {
        "schema_version": 1, "generator_sha": generator_sha,
        "generator_binary_sha256": binary_sha, "seed": seed, "events": events,
        "clicks": clicks, "threads": 8, "argv": argv[:-1] + ["<dataset>/data"],
        "started_unix_ns": started, "finished_unix_ns": time.time_ns(),
        "dataset_manifest_sha256": digest,
    }
    validate_dataset_provenance(
        generation, generator_sha=generator_sha, binary_sha=binary_sha,
        manifest_sha=digest, seed=seed, events=events, clicks=clicks,
    )
    atomic_json(temporary / "generation.json", generation)
    os.replace(temporary, dataset)
    return data, generation


def build_image(source: Path, sha: str, dockerfile_relative: str,
                role: str) -> tuple[str, dict[str, Any]]:
    dockerfile = source / dockerfile_relative
    tag = f"shopping-bench-{role}:{sha}"
    run(["docker", "build", "--pull", "--platform", "linux/amd64",
         "--label", "org.shopping-bench.managed=true",
         "--label", f"org.opencontainers.image.revision={sha}",
         "--label", f"org.shopping-bench.commit={sha}",
         "--label", f"org.shopping-bench.role={role}", "--tag", tag,
         "--file", str(dockerfile), str(source)], timeout=3600)
    image = json.loads(run(["docker", "image", "inspect", tag]).stdout)[0]
    image_id = image["Id"]
    labels = image.get("Config", {}).get("Labels", {})
    if not image_id.startswith("sha256:"):
        raise RuntimeError("Docker returned a non-content-addressed image ID")
    if labels.get("org.opencontainers.image.revision") != sha:
        raise RuntimeError("image revision label does not match source SHA")
    if labels.get("org.shopping-bench.role") != role:
        raise RuntimeError("image role label does not match the requested build")
    return image_id, {
        "dockerfile": dockerfile_relative,
        "dockerfile_sha256": sha256_file(dockerfile),
        "inspect": image,
        "role": role,
    }


def create_container(image: str, name: str, data: Path, output: Path, scratch: Path,
                     *, telemetry_required: bool) -> str:
    output.mkdir(parents=True, exist_ok=False)
    scratch.mkdir(parents=True, exist_ok=False)
    output.chmod(0o777)
    scratch.chmod(0o777)
    container_id = run(command_for_container(
        image, name, data, output, scratch, telemetry_required=telemetry_required
    )).stdout.strip()
    verify_container(container_id, data, output, scratch,
                     telemetry_required=telemetry_required)
    return container_id


def verify_container(container_id: str, data: Path, output: Path, scratch: Path,
                     *, telemetry_required: bool) -> None:
    info = json.loads(run(["docker", "inspect", container_id]).stdout)[0]
    host, config = info["HostConfig"], info["Config"]
    failures = []
    expected_env = {"DATA_DIR=/data", "OUTPUT_DIR=/output", "SCRATCH_DIR=/scratch",
                    "HOME=/scratch", "NCPU=8", "MEM_GB=24"}
    if telemetry_required:
        expected_env.add("V2_TELEMETRY_PATH=/scratch/v2-telemetry.json")
    checks = {
        "network none": host.get("NetworkMode") == "none",
        "read-only root": host.get("ReadonlyRootfs") is True,
        "memory": host.get("Memory") == MEMORY_BYTES,
        "equal memory/swap": host.get("MemorySwap") == MEMORY_BYTES,
        "pids": host.get("PidsLimit") == 4096,
        "user": config.get("User") == "1000:1000",
        "cap drop": "ALL" in (host.get("CapDrop") or []),
        "no-new-privileges": any("no-new-privileges" in x for x in (host.get("SecurityOpt") or [])),
        "tmpfs": "/tmp" in (host.get("Tmpfs") or {}),
        "cpu quota": host.get("NanoCpus") == 8_000_000_000,
        "environment": expected_env.issubset(set(config.get("Env") or [])),
    }
    mounts = {m["Destination"]: m for m in info.get("Mounts", [])}
    checks["mounts"] = (
        mounts.get("/data", {}).get("Source") == str(data.resolve()) and not mounts["/data"]["RW"] and
        mounts.get("/output", {}).get("Source") == str(output.resolve()) and mounts["/output"]["RW"] and
        mounts.get("/scratch", {}).get("Source") == str(scratch.resolve()) and mounts["/scratch"]["RW"]
    )
    for label, passed in checks.items():
        if not passed:
            failures.append(label)
    if failures:
        raise RuntimeError("container parity verification failed: " + ", ".join(failures))


def remove_container(container_id: str) -> None:
    run(["docker", "rm", "-f", container_id], check=False)


CHECKER = r'''
import hashlib, json, pathlib, sys
import polars as pl
root = pathlib.Path("/checked")
expected_root = pathlib.Path("/expected") if pathlib.Path("/expected").exists() else None
schemas = {
 "user_daily": {"user_id": pl.String, "day": pl.Date, "sessions": pl.Int128, "events": pl.Int128, "revenue_cents": pl.Int128, "top_category": pl.String},
 "campaign_daily": {"campaign_id": pl.String, "channel": pl.String, "day": pl.Date, "sessions": pl.Int128, "purchasing_sessions": pl.Int128, "revenue_cents": pl.Int128, "assisted_sessions": pl.Int128},
}
keys = {"user_daily": ["user_id", "day"], "campaign_daily": ["campaign_id", "day"]}
summary = {"ok": True, "tables": {}}
def parquet(folder, table):
 if folder.is_symlink() or not folder.is_dir(): raise ValueError(f"missing real directory {table}")
 visible = [p for p in folder.iterdir() if not p.name.startswith((".", "_"))]
 if not visible or any(p.is_symlink() or not p.is_file() or p.suffix != ".parquet" for p in visible): raise ValueError(f"invalid visible files in {table}")
 return pl.concat([pl.read_parquet(p) for p in sorted(visible)]), visible
def rows(df, schema):
 return sorted(df.select(schema.keys()).cast(schema).rows(), key=lambda r: tuple("" if v is None else str(v) for v in r))
try:
 for table, schema in schemas.items():
  frame, visible = parquet(root / table, table)
  if set(frame.columns) != set(schema): raise ValueError(f"wrong columns in {table}: {frame.columns}")
  for column, expected in schema.items():
   actual = frame.schema[column]
   if expected == pl.Int128 and actual.is_integer(): continue
   if expected == pl.String and actual.base_type() in (pl.String, pl.Categorical, pl.Enum): continue
   if actual != expected: raise ValueError(f"wrong type {table}.{column}: {actual}")
  frame = frame.select(schema.keys()).cast(schema)
  if frame.select(pl.struct(keys[table]).is_duplicated().any()).item(): raise ValueError(f"duplicate keys in {table}")
  for column in [c for c in frame.columns if c in ("sessions", "events", "revenue_cents", "purchasing_sessions", "assisted_sessions")]:
   if frame.select((pl.col(column) < 0).any()).item(): raise ValueError(f"negative {table}.{column}")
  if expected_root:
   expected_folder = expected_root / table
   if expected_folder.is_dir():
    exp, _ = parquet(expected_folder, f"expected {table}")
   else:
    exp = pl.read_csv(expected_root / f"{table}.csv", schema_overrides={k: (pl.String if v == pl.Date else v) for k,v in schema.items()})
    if exp.height: exp = exp.with_columns(pl.col("day").str.to_date())
    else: exp = pl.DataFrame(schema=schema)
   if rows(frame, schema) != rows(exp, schema): raise ValueError(f"row mismatch in {table}")
  digest = hashlib.sha256()
  for p in sorted(visible): digest.update(p.read_bytes())
  summary["tables"][table] = {"rows": frame.height, "files": len(visible), "parquet_sha256": digest.hexdigest()}
except Exception as error:
 summary["ok"] = False; summary["error"] = str(error)
print(json.dumps(summary, sort_keys=True))
sys.exit(0 if summary["ok"] else 1)
'''


def check_output(image: str, output: Path, expected: Path | None = None) -> dict[str, Any]:
    cmd = ["docker", "run", "--rm", "--platform", "linux/amd64", "--network", "none",
           "--read-only", "--tmpfs", "/tmp:rw,size=64m,mode=1777", "--cap-drop", "ALL",
           "--security-opt", "no-new-privileges", "--user", "1000:1000",
           "--mount", f"type=bind,src={output.resolve()},dst=/checked,readonly"]
    if expected:
        cmd += ["--mount", f"type=bind,src={expected.resolve()},dst=/expected,readonly"]
    cmd += ["--entrypoint", "python3", image, "-c", CHECKER]
    completed = run(cmd, check=False)
    try:
        value = json.loads(completed.stdout.strip().splitlines()[-1])
    except (ValueError, IndexError):
        value = {"ok": False, "error": completed.stderr[-1000:] or "checker returned no JSON"}
    value["exit_code"] = completed.returncode
    return value


def fixture_gates(source: Path, image: str, checker_image: str,
                  work: Path) -> dict[str, Any]:
    fixtures = sorted(path for path in (source / "fixtures").iterdir() if path.is_dir())
    if len(fixtures) != EXPECTED_FIXTURES:
        return {
            "passed": 0, "total": len(fixtures), "ok": False,
            "failures": [{"error": f"expected exactly {EXPECTED_FIXTURES} fixtures"}],
        }
    passed, failures = 0, []
    for fixture in fixtures:
        case = re.sub(r"[^A-Za-z0-9_.-]", "-", fixture.name)
        run_path = work / f"fixture-{case}"
        shutil.rmtree(run_path, ignore_errors=True)
        data = fixture / ("data" if (fixture / "data").is_dir() else "input")
        container_id = ""
        try:
            container_id = create_container(
                image, f"shopping-bench-fixture-{os.getpid()}-{case.lower()}",
                data, run_path / "output", run_path / "scratch", telemetry_required=True
            )
            completed = run(
                ["docker", "start", "-a", container_id],
                check=False, timeout=TIMEOUT_SECONDS,
            )
            state = json.loads(run(["docker", "inspect", container_id]).stdout)[0]["State"]
            check = check_output(checker_image, run_path / "output", fixture / "expected")
            if completed.returncode == 0 and state["ExitCode"] == 0 and not state["OOMKilled"] and check["ok"]:
                passed += 1
            else:
                failures.append({"case": fixture.name, "state": state, "check": check,
                                 "stderr": completed.stderr[-1000:]})
        except Exception as error:
            failures.append({"case": fixture.name, "error": str(error)})
        finally:
            if container_id:
                remove_container(container_id)
            shutil.rmtree(run_path, ignore_errors=True)
        if failures:
            break
    return {"passed": passed, "total": len(fixtures), "failures": failures,
            "ok": passed == len(fixtures)}


def tree_usage(path: Path) -> tuple[int, int]:
    apparent = allocated = 0
    try:
        for root, _, files in os.walk(path):
            for name in files:
                try:
                    stat = os.stat(os.path.join(root, name), follow_symlinks=False)
                    apparent += stat.st_size
                    allocated += stat.st_blocks * 512
                except FileNotFoundError:
                    pass
    except FileNotFoundError:
        pass
    return apparent, allocated


def parse_key_values(path: Path) -> dict[str, int]:
    values: dict[str, int] = {}
    try:
        for line in path.read_text().splitlines():
            parts = line.split()
            if len(parts) >= 2 and parts[1].isdigit():
                values[parts[0]] = int(parts[1])
    except (FileNotFoundError, PermissionError):
        pass
    return values


def parse_io(path: Path) -> dict[str, int]:
    totals = {"rbytes": 0, "wbytes": 0, "rios": 0, "wios": 0}
    try:
        for line in path.read_text().splitlines():
            for item in line.split()[1:]:
                key, value = item.split("=", 1)
                if key in totals:
                    totals[key] += int(value)
    except (FileNotFoundError, PermissionError, ValueError):
        pass
    return totals


def cgroup_path_for_pid(pid: int) -> Path | None:
    try:
        for line in Path(f"/proc/{pid}/cgroup").read_text().splitlines():
            fields = line.split(":", 2)
            if fields[:2] == ["0", ""]:
                return Path("/sys/fs/cgroup") / fields[2].lstrip("/")
    except (FileNotFoundError, PermissionError):
        return None
    return None


class Monitor(threading.Thread):
    def __init__(self, container_id: str, output: Path, scratch: Path):
        super().__init__(daemon=True)
        self.container_id, self.output, self.scratch = container_id, output, scratch
        self.stop_event = threading.Event()
        self.samples = 0
        self.output_app = self.output_alloc = self.scratch_app = self.scratch_alloc = 0
        self.combined = 0
        self.cgroup: Path | None = None
        self.memory_peak: int | None = None
        self.cpu_first: dict[str, int] | None = None
        self.cpu_last: dict[str, int] | None = None
        self.io_first: dict[str, int] | None = None
        self.io_last: dict[str, int] | None = None
        self.memory_events: dict[str, int] = {}
        self.disk_threshold_exceeded = False

    def sample(self) -> None:
        out_app, out_alloc = tree_usage(self.output)
        scratch_app, scratch_alloc = tree_usage(self.scratch)
        self.output_app, self.output_alloc = max(self.output_app, out_app), max(self.output_alloc, out_alloc)
        self.scratch_app, self.scratch_alloc = max(self.scratch_app, scratch_app), max(self.scratch_alloc, scratch_alloc)
        self.combined = max(self.combined, out_alloc + scratch_alloc)
        if self.combined > DISK_STOP_BYTES:
            self.disk_threshold_exceeded = True
            run(["docker", "kill", self.container_id], check=False)
        if self.cgroup is None:
            inspected = run(["docker", "inspect", "--format", "{{.State.Pid}}", self.container_id], check=False)
            if inspected.returncode == 0 and inspected.stdout.strip().isdigit() and int(inspected.stdout) > 0:
                self.cgroup = cgroup_path_for_pid(int(inspected.stdout))
        if self.cgroup:
            try:
                peak = int((self.cgroup / "memory.peak").read_text().strip())
                self.memory_peak = max(self.memory_peak or 0, peak)
            except (FileNotFoundError, PermissionError, ValueError):
                pass
            cpu = parse_key_values(self.cgroup / "cpu.stat")
            io = parse_io(self.cgroup / "io.stat")
            if cpu:
                self.cpu_first = self.cpu_first or cpu
                self.cpu_last = cpu
            if io:
                self.io_first = self.io_first or io
                self.io_last = io
            self.memory_events = parse_key_values(self.cgroup / "memory.events") or self.memory_events
        self.samples += 1

    def run(self) -> None:
        while not self.stop_event.wait(0.05):
            self.sample()
        self.sample()

    def finish(self, wall_ns: int) -> dict[str, Any]:
        self.stop_event.set()
        self.join(timeout=2)
        cpu_first, cpu_last = self.cpu_first or {}, self.cpu_last or {}
        io_first, io_last = self.io_first or {}, self.io_last or {}
        delta = lambda last, first, key: max(0, last.get(key, 0) - first.get(key, 0))
        usage = delta(cpu_last, cpu_first, "usage_usec") if self.cpu_first else None
        utilization = (100 * usage / (wall_ns / 1000 * 8)) if usage is not None and wall_ns else None
        return {
            "counter_support": {
                "cgroup_v2": self.cgroup is not None,
                "memory_peak": self.memory_peak is not None,
                "cpu_stat": self.cpu_first is not None,
                "io_stat": self.io_first is not None,
                "pids_peak": bool(self.cgroup and (self.cgroup / "pids.peak").exists()),
            },
            "memory_peak_bytes": self.memory_peak,
            "memory_events": self.memory_events,
            "cpu_usage_usec": usage,
            "cpu_user_usec": delta(cpu_last, cpu_first, "user_usec") if self.cpu_first else None,
            "cpu_system_usec": delta(cpu_last, cpu_first, "system_usec") if self.cpu_first else None,
            "cpu_throttled_usec": delta(cpu_last, cpu_first, "throttled_usec") if self.cpu_first else None,
            "cpu_utilization_percent_of_8": utilization,
            "io_read_bytes": delta(io_last, io_first, "rbytes") if self.io_first else None,
            "io_write_bytes": delta(io_last, io_first, "wbytes") if self.io_first else None,
            "output_peak_apparent_bytes": self.output_app,
            "output_peak_allocated_bytes": self.output_alloc,
            "scratch_peak_apparent_bytes": self.scratch_app,
            "scratch_peak_allocated_bytes": self.scratch_alloc,
            "combined_peak_allocated_bytes": self.combined,
            "samples": self.samples,
        }


def idle_preflight() -> dict[str, Any]:
    containers = run(["docker", "ps", "--format", "{{.ID}} {{.Names}}"], check=False).stdout.splitlines()
    if containers:
        raise RuntimeError("Docker containers are running: " + ", ".join(containers))
    def cpu() -> list[int]:
        return [int(value) for value in Path("/proc/stat").read_text().splitlines()[0].split()[1:]]
    before = cpu()
    time.sleep(IDLE_SECONDS)
    after = cpu()
    deltas = [b - a for a, b in zip(before, after)]
    total = sum(deltas)
    idle = sum(deltas[index] for index in (3, 4) if index < len(deltas))
    steal = deltas[7] if len(deltas) > 7 else 0
    idle_ratio = idle / total if total else 1.0
    steal_ratio = steal / total if total else 0.0
    load = os.getloadavg()
    if idle_ratio < 0.90 or steal_ratio > 0.01 or load[0] > 2.0:
        raise RuntimeError(f"VM is not idle enough: idle={idle_ratio:.3f} steal={steal_ratio:.3f} load1={load[0]:.2f}")
    return {"sample_seconds": IDLE_SECONDS, "idle_ratio": idle_ratio, "steal_ratio": steal_ratio,
            "loadavg": load, "running_containers": containers}


def metadata_value(path: str) -> str:
    request = urllib.request.Request(
        f"http://metadata.google.internal/computeMetadata/v1/{path}",
        headers={"Metadata-Flavor": "Google"},
    )
    try:
        with urllib.request.urlopen(request, timeout=2) as response:
            if response.headers.get("Metadata-Flavor") != "Google":
                raise RuntimeError("GCP metadata response lacks the Google flavor header")
            return response.read().decode("utf-8").strip()
    except (OSError, UnicodeDecodeError, urllib.error.URLError) as error:
        raise RuntimeError(f"cannot verify GCP metadata path {path}: {error}") from error


def validate_host_facts(facts: dict[str, Any]) -> None:
    mismatches = [
        key for key, expected in EXPECTED_HOST.items()
        if facts.get(key) != expected
    ]
    memory = facts.get("mem_total_bytes")
    if type(memory) is not int or not 29 * 1024**3 <= memory <= 33 * 1024**3:
        mismatches.append("mem_total_bytes")
    if facts.get("cgroup_version") != 2:
        mismatches.append("cgroup_version")
    if mismatches:
        raise RuntimeError("benchmark host identity mismatch: " + ", ".join(mismatches))


def host_facts() -> dict[str, Any]:
    cpu_model = "unknown"
    flags = ""
    for line in Path("/proc/cpuinfo").read_text().splitlines():
        if line.startswith("model name") and cpu_model == "unknown": cpu_model = line.split(":", 1)[1].strip()
        if line.startswith("flags") and not flags: flags = line.split(":", 1)[1].strip()
    mem_total = next((int(line.split()[1]) * 1024 for line in Path("/proc/meminfo").read_text().splitlines()
                      if line.startswith("MemTotal:")), 0)
    facts = {
        "instance": metadata_value("instance/name"),
        "project": metadata_value("project/project-id"),
        "zone": metadata_value("instance/zone").rsplit("/", 1)[-1],
        "machine_type": metadata_value("instance/machine-type").rsplit("/", 1)[-1],
        "kernel": run(["uname", "-r"]).stdout.strip(),
        "architecture": run(["uname", "-m"]).stdout.strip(), "cpu_model": cpu_model,
        "cpu_flags_sha256": hashlib.sha256(flags.encode()).hexdigest(), "vcpus": os.cpu_count(),
        "mem_total_bytes": mem_total,
        "docker_server": run(["docker", "version", "--format", "{{.Server.Version}}"]).stdout.strip(),
        "cgroup_version": 2 if Path("/sys/fs/cgroup/cgroup.controllers").exists() else 1,
        "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
    }
    validate_host_facts(facts)
    return facts


def docker_timestamp_ns(value: str) -> int:
    match = re.fullmatch(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d+))?Z", value)
    if not match:
        raise ValueError(f"invalid Docker timestamp: {value}")
    seconds = calendar.timegm(time.strptime(match.group(1), "%Y-%m-%dT%H:%M:%S"))
    fraction = (match.group(2) or "")[:9].ljust(9, "0")
    return seconds * 1_000_000_000 + int(fraction or 0)


def validate_candidate_telemetry(value: Any, expected_events: int,
                                 expected_clicks: int) -> None:
    if not isinstance(value, dict) or value.get("format_version") != 1:
        raise RuntimeError("candidate telemetry format_version must be 1")
    if value.get("outcome") != "success":
        raise RuntimeError("candidate telemetry outcome must be success")
    phases = value.get("phases")
    if (not isinstance(phases, list) or any(not isinstance(item, dict) for item in phases)
            or tuple(item.get("phase") for item in phases) != PHASES):
        raise RuntimeError(f"candidate telemetry must contain phases in order: {PHASES}")
    required_counters = {
        "discovered_files", "input_rows", "accepted_rows", "dropped_rows",
        "output_rows", "bytes_read", "bytes_written",
    }
    counters_by_phase: dict[str, dict[str, int]] = {}
    for item in phases:
        phase = item.get("phase")
        if type(item.get("elapsed_ns")) is not int or item["elapsed_ns"] < 0:
            raise RuntimeError(f"invalid elapsed_ns for {phase}")
        counters = item.get("counters")
        if not isinstance(counters, dict) or set(counters) != required_counters:
            raise RuntimeError(f"invalid counters for {phase}")
        if any(type(count) is not int or count < 0 for count in counters.values()):
            raise RuntimeError(f"negative or non-integer counter for {phase}")
        counters_by_phase[phase] = counters
    if counters_by_phase["discovery"]["discovered_files"] == 0:
        raise RuntimeError("discovery telemetry reports no input files")
    for phase, minimum in (("links", 1), ("events", expected_events),
                           ("clicks", expected_clicks)):
        counters = counters_by_phase[phase]
        if counters["input_rows"] < minimum:
            raise RuntimeError(f"{phase} telemetry reports too few input rows")
        if counters["accepted_rows"] + counters["dropped_rows"] != counters["input_rows"]:
            raise RuntimeError(f"{phase} accepted and dropped rows do not reconcile with input")
    write = counters_by_phase["write"]
    if write["output_rows"] == 0 or write["bytes_written"] == 0:
        raise RuntimeError("write telemetry reports no output")
    resources = value.get("resources")
    expected_resources = {"cgroup_memory_peak_bytes", "scratch_high_water_bytes"}
    if not isinstance(resources, dict) or set(resources) != expected_resources:
        raise RuntimeError("candidate telemetry resources are missing")
    if any(type(resource) is not int or resource < 0 for resource in resources.values()):
        raise RuntimeError("candidate telemetry resources must be non-negative integers")


def ingest_candidate_telemetry(scratch: Path, run_dir: Path,
                               expected_events: int,
                               expected_clicks: int) -> tuple[Any, dict[str, Any]]:
    source = scratch / "v2-telemetry.json"
    metadata: dict[str, Any] = {
        "required": True, "present": False, "valid": False,
        "path": None, "sha256": "", "size_bytes": 0,
    }
    if not source.is_file() or source.is_symlink():
        metadata["validation_error"] = "candidate emitted no regular telemetry file"
        return None, metadata
    raw = source.read_bytes()
    destination = run_dir / "candidate-telemetry.json"
    atomic_bytes(destination, raw)
    metadata.update(artifact_metadata(destination, run_dir))
    metadata["present"] = True
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        metadata["validation_error"] = f"candidate telemetry is not valid JSON: {error}"
        return None, metadata
    try:
        validate_candidate_telemetry(value, expected_events, expected_clicks)
    except RuntimeError as error:
        metadata["validation_error"] = str(error)
        return value, metadata
    metadata["valid"] = True
    return value, metadata


def ensure_progressive_gate(root: Path, sha: str, seed: int, events: int) -> None:
    if events == TIERS[0]:
        return
    previous = TIERS[TIERS.index(events) - 1]
    index = root / "results" / "results.csv"
    if not index.exists():
        raise RuntimeError(f"progressive gate requires an eligible {previous}-event result first")
    validate_results_csv(index)
    with index.open(newline="", encoding="utf-8") as handle:
        rows = [
            row for row in csv.DictReader(handle)
            if row["git_sha"] == sha
            and row["baseline_sha"] == BASELINE_SHA
            and int(row["seed"]) == seed
            and int(row["events"]) == previous
            and row["result_status"] == "eligible-v2-candidate"
            and row["outcome"] == "pass"
            and row["eligible_timing"] == "true"
            and row["self_check"] == "true"
        ]
    if not rows:
        raise RuntimeError(f"progressive gate requires an eligible {previous}-event result first")
    prior = rows[-1]
    memory = int(prior["memory_peak_bytes"] or 0)
    disk = int(prior["combined_peak_allocated_bytes"] or 0)
    if memory <= 0 or memory >= MEMORY_QUALIFY_BYTES:
        raise RuntimeError("progressive gate stopped: prior memory did not pass 22 GiB threshold")
    if disk >= DISK_STOP_BYTES:
        raise RuntimeError("progressive gate stopped: prior output/scratch reached 45 GiB")
    projected_ns = int(prior["wall_ns"]) * events / previous
    if projected_ns >= TIMEOUT_NS:
        raise RuntimeError("progressive gate stopped: linear next-tier projection reaches 180 seconds")


def trusted_oracle_run(image_id: str, data: Path, run_dir: Path,
                       run_id: str) -> tuple[Path, dict[str, Any]]:
    output = run_dir / "oracle-output"
    scratch = run_dir / "oracle-scratch"
    stdout_path = run_dir / "oracle-stdout.log"
    stderr_path = run_dir / "oracle-stderr.log"
    manifest_path = run_dir / "ORACLE_SHA256SUMS"
    name = f"shopping-bench-oracle-{run_id.lower()}"
    container_id = create_container(
        image_id, name, data, output, scratch, telemetry_required=False
    )
    create_command = command_for_container(
        image_id, name, data, output, scratch, telemetry_required=False
    )
    timed_out = False
    try:
        with stdout_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
            process = subprocess.Popen(
                ["docker", "start", "-a", container_id], stdout=stdout, stderr=stderr
            )
            try:
                process.wait(timeout=TIMEOUT_SECONDS)
            except subprocess.TimeoutExpired:
                timed_out = True
                run(["docker", "kill", container_id], check=False)
                process.wait(timeout=30)
        state = json.loads(run(["docker", "inspect", container_id]).stdout)[0]["State"]
        check = check_output(image_id, output)
        if (timed_out or state["ExitCode"] != 0 or state["OOMKilled"]
                or check.get("ok") is not True):
            raise RuntimeError("trusted V1 oracle failed on the generated dataset")
        manifest_sha = write_manifest(output, manifest_path)
        return output, {
            "source_sha": BASELINE_SHA,
            "image_id": image_id,
            "container_id": container_id,
            "create_command": create_command,
            "exit_code": state["ExitCode"],
            "oom_killed": state["OOMKilled"],
            "output_check": check,
            "output_manifest_sha256": manifest_sha,
            "artifacts": {
                "stdout": artifact_metadata(stdout_path, run_dir),
                "stderr": artifact_metadata(stderr_path, run_dir),
                "output_manifest": artifact_metadata(manifest_path, run_dir),
            },
        }
    finally:
        remove_container(container_id)
        shutil.rmtree(scratch, ignore_errors=True)


def qualification_outcome(*, timed_out: bool, wall_ns: int, exit_code: int,
                          oom_killed: bool, oom_event: bool,
                          memory_peak_bytes: int | None,
                          combined_peak_allocated_bytes: int,
                          disk_threshold_exceeded: bool, fixtures_ok: bool,
                          exact_oracle_match: bool,
                          telemetry_valid: bool) -> tuple[bool, str]:
    resource_gate = (
        memory_peak_bytes is not None
        and memory_peak_bytes < MEMORY_QUALIFY_BYTES
        and combined_peak_allocated_bytes < DISK_STOP_BYTES
        and not disk_threshold_exceeded
    )
    time_gate = not timed_out and wall_ns < TIMEOUT_NS
    success = (
        time_gate and exit_code == 0 and not oom_killed and not oom_event
        and resource_gate and fixtures_ok and exact_oracle_match and telemetry_valid
    )
    if not time_gate:
        return False, "timeout"
    if oom_killed or oom_event or not resource_gate:
        return False, "resource_fail"
    if exit_code != 0 or not exact_oracle_match:
        return False, "correctness_fail"
    if not telemetry_valid:
        return False, "telemetry_fail"
    return success, "pass"


def timed_run(root: Path, candidate_source: Path, baseline_source: Path,
              sha: str, tree: str, reference: dict[str, str], image_id: str,
              image_meta: dict[str, Any], oracle_image_id: str,
              oracle_image_meta: dict[str, Any], data: Path,
              generation: dict[str, Any], fixtures: dict[str, Any], host: dict[str, Any],
              seed: int, events: int, clicks: int) -> tuple[dict[str, Any], Path]:
    timestamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    run_id = f"{timestamp}-{sha[:12]}-s{seed}-e{events}-{image_id[7:19]}-r1"
    run_dir = root / "runs" / run_id
    run_dir.mkdir(parents=True)
    output, scratch = run_dir / "output", run_dir / "scratch"
    image_inspect_path = run_dir / "candidate-image-inspect.json"
    oracle_inspect_path = run_dir / "oracle-image-inspect.json"
    dataset_manifest_path = run_dir / "DATA_SHA256SUMS"
    generation_path = run_dir / "generation.json"
    atomic_json(image_inspect_path, image_meta["inspect"])
    atomic_json(oracle_inspect_path, oracle_image_meta["inspect"])
    atomic_json(generation_path, generation)
    verify_live_checkout(candidate_source, sha)
    verify_live_checkout(baseline_source, BASELINE_SHA)
    manifest = data.parent / "DATA_SHA256SUMS"
    manifest_sha = verify_manifest(data, manifest)
    atomic_bytes(dataset_manifest_path, manifest.read_bytes())
    if sha256_file(dataset_manifest_path) != manifest_sha:
        raise RuntimeError("dataset manifest changed while preparing run provenance")
    oracle_output, oracle = trusted_oracle_run(
        oracle_image_id, data, run_dir, run_id
    )
    idle = idle_preflight()
    name = f"shopping-bench-{run_id.lower()}"
    container_id = create_container(
        image_id, name, data, output, scratch, telemetry_required=True
    )
    create_command = command_for_container(
        image_id, name, data, output, scratch, telemetry_required=True
    )
    stdout_path, stderr_path = run_dir / "stdout.log", run_dir / "stderr.log"
    monitor = Monitor(container_id, output, scratch)
    timed_out = False
    try:
        with stdout_path.open("wb") as stdout, stderr_path.open("wb") as stderr:
            started_utc = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
            started_ns = time.monotonic_ns()
            process = subprocess.Popen(["docker", "start", "-a", container_id], stdout=stdout, stderr=stderr)
            monitor.start()
            try:
                process.wait(timeout=TIMEOUT_SECONDS)
            except subprocess.TimeoutExpired:
                timed_out = True
                run(["docker", "kill", container_id], check=False)
                process.wait(timeout=30)
            ended_ns = time.monotonic_ns()
        inspect = json.loads(run(["docker", "inspect", container_id]).stdout)[0]
        state = inspect["State"]
        wall_ns = ended_ns - started_ns
        resources = monitor.finish(wall_ns)
        self_check = check_output(oracle_image_id, output, oracle_output)
        telemetry, telemetry_artifact = ingest_candidate_telemetry(
            scratch, run_dir, events, clicks
        )
        oom_event = resources["memory_events"].get("oom_kill", 0) > 0
        success, outcome = qualification_outcome(
            timed_out=timed_out, wall_ns=wall_ns, exit_code=state["ExitCode"],
            oom_killed=state["OOMKilled"], oom_event=oom_event,
            memory_peak_bytes=resources["memory_peak_bytes"],
            combined_peak_allocated_bytes=resources["combined_peak_allocated_bytes"],
            disk_threshold_exceeded=monitor.disk_threshold_exceeded,
            fixtures_ok=fixtures["ok"],
            exact_oracle_match=self_check.get("ok") is True,
            telemetry_valid=telemetry_artifact["valid"],
        )
        result = {
            "schema_version": SCHEMA_VERSION, "run_id": run_id,
            "result_status": "eligible-v2-candidate" if success else "ineligible",
            "outcome": outcome, "eligible_timing": success, "started_utc": started_utc,
            "provenance": {
                "repository": REPO_URL, "git_sha": sha, "git_tree": tree, "git_tree_clean": True,
                "image_id": image_id, "image_revision_label": sha,
                "dockerfile": image_meta["dockerfile"],
                "dockerfile_sha256": image_meta["dockerfile_sha256"],
                "baseline_sha": BASELINE_SHA, "baseline_tree": reference["tree"],
                "fixtures_tree": reference["fixtures_tree"],
                "generator_tree": reference["generator_tree"],
                "oracle_image_id": oracle_image_id,
                "oracle_dockerfile": oracle_image_meta["dockerfile"],
                "oracle_dockerfile_sha256": oracle_image_meta["dockerfile_sha256"],
                "checker_program_sha256": hashlib.sha256(CHECKER.encode()).hexdigest(),
                "generator_sha": generation["generator_sha"],
                "generator_binary_sha256": generation["generator_binary_sha256"],
                "dataset_manifest_sha256": manifest_sha,
                "oracle_output_manifest_sha256": oracle["output_manifest_sha256"],
                "seed": seed, "events": events, "clicks": clicks,
            },
            "host": {**host, "idle_preflight": idle},
            "limits": {"cpus": 8, "memory_bytes": MEMORY_BYTES, "swap_bytes": 0,
                       "qualification_memory_bytes": MEMORY_QUALIFY_BYTES,
                       "pids": 4096, "tmpfs_bytes": TMPFS_BYTES,
                       "timeout_seconds": TIMEOUT_SECONDS},
            "container": {
                "id": container_id, "create_command": create_command,
                "flags_sha256": hashlib.sha256("\0".join(create_command).encode()).hexdigest(),
                "started_at": state["StartedAt"], "finished_at": state["FinishedAt"],
                "exit_code": state["ExitCode"], "oom_killed": state["OOMKilled"],
            },
            "timing": {"class": "warm", "repetition": 1, "wall_ns": wall_ns,
                       "docker_state_wall_ns": docker_timestamp_ns(state["FinishedAt"]) - docker_timestamp_ns(state["StartedAt"]),
                       "clock": "CLOCK_MONOTONIC", "boundary": "docker-start-request-to-observed-exit"},
            "resources": resources,
            "correctness": {
                "fixtures_passed": fixtures["passed"], "fixtures_total": fixtures["total"],
                "fixture_gate": fixtures["ok"],
                "trusted_oracle_gate": oracle["output_check"].get("ok") is True,
                "exact_oracle_match": self_check.get("ok") is True,
                "self_check": self_check.get("ok") is True,
                "self_check_details": self_check,
                "telemetry_gate": telemetry_artifact["valid"],
            },
            "telemetry": {"candidate": telemetry, "artifact": telemetry_artifact},
            "trusted_oracle": oracle,
            "parity": {
                "container_constraints": "official-equivalent-verified",
                "combined_disk_cap": "monitor-only-no-hard-cap",
                "cache_state": "warm-unspecified-official-cache",
                "counter_note": "null values make a result ineligible",
            },
            "artifacts": {
                "stdout": artifact_metadata(stdout_path, run_dir),
                "stderr": artifact_metadata(stderr_path, run_dir),
                "candidate_image_inspect": artifact_metadata(image_inspect_path, run_dir),
                "oracle_image_inspect": artifact_metadata(oracle_inspect_path, run_dir),
                "dataset_manifest": artifact_metadata(dataset_manifest_path, run_dir),
                "dataset_generation": artifact_metadata(generation_path, run_dir),
            },
        }
        finalize_result(run_dir, root / "results" / "results.csv", result)
        return result, run_dir
    finally:
        if monitor.is_alive():
            monitor.finish(1)
        remove_container(container_id)


def campaign(args: argparse.Namespace) -> int:
    sha = validate_sha(args.sha)
    validate_scale(args.events, args.clicks)
    root = Path(args.root).resolve()
    ensure_layout(root)
    with BenchLock(root / "locks" / "run.lock"):
        recover(root)
        ensure_progressive_gate(root, sha, args.seed, args.events)
        host = host_facts()
        candidate_source, tree = source_checkout(root, sha)
        baseline_source, baseline_tree = source_checkout(root, BASELINE_SHA)
        reference = {
            "tree": baseline_tree,
            "fixtures_tree": checkout_tree(baseline_source, "fixtures"),
            "generator_tree": checkout_tree(baseline_source, "generator"),
        }
        binary, binary_sha = build_generator(root, baseline_source, BASELINE_SHA)
        data, generation = prepare_dataset(
            root, binary, BASELINE_SHA, binary_sha,
            args.seed, args.events, args.clicks
        )
        image_id, image_meta = build_image(
            candidate_source, sha, "v2/Dockerfile", "candidate"
        )
        oracle_image_id, oracle_image_meta = build_image(
            baseline_source, BASELINE_SHA, "Dockerfile", "oracle"
        )
        fixture_result = fixture_gates(
            baseline_source, image_id, oracle_image_id, root / "tmp"
        )
        fixture_result.update({
            "candidate_image_id": image_id,
            "baseline_sha": BASELINE_SHA,
            "baseline_tree": baseline_tree,
            "fixtures_tree": reference["fixtures_tree"],
            "oracle_image_id": oracle_image_id,
        })
        atomic_json(root / "results" / f"fixtures-{sha}-{image_id[7:19]}.json", fixture_result)
        if not fixture_result["ok"]:
            raise RuntimeError(f"fixture gate failed after {fixture_result['passed']}/{fixture_result['total']}; stop")
        result, run_dir = timed_run(
            root, candidate_source, baseline_source, sha, tree, reference,
            image_id, image_meta, oracle_image_id, oracle_image_meta, data,
            generation, fixture_result, host, args.seed, args.events, args.clicks,
        )
        print(json.dumps({"run_id": result["run_id"], "outcome": result["outcome"],
                          "eligible_timing": result["eligible_timing"], "result": str(run_dir / "result.json")},
                         sort_keys=True))
        return 0 if result["eligible_timing"] else 2


def recover(root: Path) -> None:
    completed = run(["docker", "ps", "-a", "--filter", "label=org.shopping-bench.managed=true",
                     "--format", "{{.ID}} {{.State}} {{.Label \"org.shopping-bench.run\"}}"], check=False)
    for line in completed.stdout.splitlines():
        fields = line.split(maxsplit=2)
        if len(fields) >= 2:
            remove_container(fields[0])


def cleanup(root: Path, dry_run: bool) -> list[str]:
    actions: list[str] = []
    completed = run(["docker", "ps", "-a", "--filter", "label=org.shopping-bench.managed=true",
                     "--format", "{{.ID}} {{.State}} {{.Names}}"], check=False)
    for line in completed.stdout.splitlines():
        container_id, state, *_ = line.split()
        if state != "running":
            actions.append(f"remove container {container_id}")
            if not dry_run:
                remove_container(container_id)
    for partial in (root / "tmp").glob("*.partial.*") if (root / "tmp").exists() else []:
        actions.append(f"remove partial {partial}")
        if not dry_run:
            shutil.rmtree(partial)
    return actions


def plan(args: argparse.Namespace) -> int:
    validate_sha(args.sha)
    validate_scale(args.events, args.clicks)
    root = Path(args.root).resolve()
    data = root / "datasets" / f"g{BASELINE_SHA}-s{args.seed}-e{args.events}-c{args.clicks}" / "data"
    command = command_for_container(
        f"sha256:{'0' * 64}", "shopping-bench-dry-run", data,
        root / "runs" / "dry-run" / "output",
        root / "runs" / "dry-run" / "scratch", telemetry_required=True
    )
    print(json.dumps({
        "schema_version": SCHEMA_VERSION, "dry_run": True, "source_sha": args.sha,
        "baseline_sha": BASELINE_SHA, "generator_sha": BASELINE_SHA,
        "fixtures_sha": BASELINE_SHA, "oracle_sha": BASELINE_SHA,
        "dockerfile": "v2/Dockerfile", "events": args.events,
        "clicks": args.clicks, "idle_seconds": IDLE_SECONDS,
        "timeout_seconds": TIMEOUT_SECONDS, "container_create": command,
        "timed_boundary": "docker start -a",
    }, indent=2))
    return 0


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default="/srv/shopping-bench")
    sub = parser.add_subparsers(dest="command", required=True)
    for name in ("plan", "campaign"):
        item = sub.add_parser(name)
        item.add_argument("--sha", required=True)
        item.add_argument("--seed", type=int, default=1)
        item.add_argument("--events", type=int, required=True)
        item.add_argument("--clicks", type=int, required=True)
    cleanup_parser = sub.add_parser("cleanup")
    cleanup_parser.add_argument("--dry-run", action="store_true")
    sub.add_parser("recover")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv or sys.argv[1:])
    try:
        if args.command == "plan": return plan(args)
        if args.command == "campaign": return campaign(args)
        root = Path(args.root).resolve()
        ensure_layout(root)
        with BenchLock(root / "locks" / "run.lock"):
            if args.command == "recover": recover(root); return 0
            for action in cleanup(root, args.dry_run): print(action)
            return 0
    except (ValueError, RuntimeError, BusyError, subprocess.SubprocessError) as error:
        print(f"shopping-bench: {error}", file=sys.stderr)
        return 75 if isinstance(error, BusyError) else 2


if __name__ == "__main__":
    raise SystemExit(main())
