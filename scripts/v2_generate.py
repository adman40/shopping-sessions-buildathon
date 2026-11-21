#!/usr/bin/env python3
"""Run the published generator and add provenance used by the exact-scale gate."""

import argparse
import json
from pathlib import Path
import shlex
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--generator", default="generate", help="generator command, shell-quoted")
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--events", type=int, required=True)
    parser.add_argument("--clicks", type=int, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--threads", type=int)
    args = parser.parse_args()
    command = shlex.split(args.generator) + [
        "--seed", str(args.seed), "--events", str(args.events),
        "--clicks", str(args.clicks), "--output", str(args.output),
    ]
    if args.threads is not None:
        command += ["--threads", str(args.threads)]
    subprocess.run(command, check=True)
    provenance = {
        "format_version": 1,
        "generator": "shopping-sessions-published-v1",
        "seed": args.seed,
        "events": args.events,
        "clicks": args.clicks,
    }
    args.output.joinpath(".v2-generation.json").write_text(
        json.dumps(provenance, sort_keys=True) + "\n", encoding="utf-8"
    )


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as error:
        sys.exit(error.returncode)
