#!/usr/bin/env python3
"""Generate randomized small datasets and compare a V2 candidate with the V1 oracle."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import shlex
import subprocess
import sys
import tempfile


def run(arguments, *, environment=None, label):
    process = subprocess.run(arguments, env=environment, capture_output=True, text=True)
    if process.returncode:
        detail = (process.stdout + process.stderr)[-4000:]
        raise RuntimeError(f"{label} failed:\n{detail}")


def execute(command, data, output, label):
    output.mkdir(parents=True)
    with tempfile.TemporaryDirectory(prefix="v2-diff-scratch-") as scratch:
        environment = dict(
            os.environ,
            DATA_DIR=str(data),
            OUTPUT_DIR=str(output),
            SCRATCH_DIR=scratch,
        )
        run(command + [str(data), str(output)], environment=environment, label=label)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidate", required=True, help="candidate command, shell-quoted")
    parser.add_argument(
        "--oracle",
        default=f"{shlex.quote(sys.executable)} pipeline/pipeline.py",
        help="V1 oracle command, shell-quoted",
    )
    parser.add_argument("--generator", default="generate", help="generator command, shell-quoted")
    parser.add_argument("--seeds", type=int, default=10)
    parser.add_argument("--first-seed", type=int, default=1)
    parser.add_argument("--events", type=int, default=1_000)
    parser.add_argument("--clicks", type=int, default=250)
    args = parser.parse_args()
    if args.seeds < 1 or args.events < 1_000 or args.clicks < 0:
        parser.error("seeds must be positive, events at least 1000, and clicks nonnegative")

    candidate = shlex.split(args.candidate)
    oracle = shlex.split(args.oracle)
    generator = shlex.split(args.generator)
    compare = Path(__file__).with_name("compare.py")
    with tempfile.TemporaryDirectory(prefix="v2-differential-") as temporary:
        root = Path(temporary)
        for seed in range(args.first_seed, args.first_seed + args.seeds):
            data = root / f"seed-{seed}" / "data"
            data.parent.mkdir()
            run(
                generator + [
                    "--seed", str(seed), "--events", str(args.events),
                    "--clicks", str(args.clicks), "--threads", "1", "--output", str(data),
                ],
                label=f"generator seed {seed}",
            )
            oracle_output = root / f"seed-{seed}" / "oracle"
            candidate_output = root / f"seed-{seed}" / "candidate"
            execute(oracle, data, oracle_output, f"V1 oracle seed {seed}")
            execute(candidate, data, candidate_output, f"V2 candidate seed {seed}")
            run(
                [sys.executable, str(compare), str(oracle_output), str(candidate_output)],
                label=f"differential comparison seed {seed}",
            )
            print(f"ok   randomized seed {seed}")
    print(f"\n{args.seeds}/{args.seeds} randomized differential cases passed")


if __name__ == "__main__":
    try:
        main()
    except RuntimeError as error:
        print(error, file=sys.stderr)
        sys.exit(1)
