#!/usr/bin/env bash
set -euo pipefail
ROOT=$(mktemp -d)
trap 'rm -rf "$ROOT"' EXIT
SCRIPT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/shopping_bench.py
SHA=8a35b964aed48ecba5e44d061606b2a4bb9a2239

python3 "$SCRIPT" --root "$ROOT" plan --sha "$SHA" --events 100000 --clicks 25000 >"$ROOT/plan.json"
python3 - "$ROOT/plan.json" <<'PY'
import json, sys
value = json.load(open(sys.argv[1], encoding="utf-8"))
assert value["dry_run"] is True
assert value["source_sha"] == "8a35b964aed48ecba5e44d061606b2a4bb9a2239"
assert value["candidate_mode"] == "v2"
assert value["dockerfile"] == "v2/Dockerfile"
assert value["container_create"][0:2] == ["docker", "create"]
assert "V2_TELEMETRY_PATH=/scratch/v2-telemetry.json" in value["container_create"]
assert "docker start -a" == value["timed_boundary"]
PY

python3 "$SCRIPT" --root "$ROOT" plan --mode v1-smoke --sha "$SHA" --events 100000 --clicks 25000 >"$ROOT/v1-plan.json"
python3 - "$ROOT/v1-plan.json" <<'PY'
import json, sys
value = json.load(open(sys.argv[1], encoding="utf-8"))
assert value["candidate_mode"] == "v1-smoke"
assert value["dockerfile"] == "Dockerfile"
assert "V2_TELEMETRY_PATH=/scratch/v2-telemetry.json" not in value["container_create"]
PY

if python3 "$SCRIPT" --root "$ROOT" plan --sha main --events 100000 --clicks 25000 >/dev/null 2>&1; then
  echo "short/ref SHA unexpectedly accepted" >&2
  exit 1
fi
if python3 "$SCRIPT" --root "$ROOT" plan --sha "$SHA" --events 100000 --clicks 1 >/dev/null 2>&1; then
  echo "invalid click ratio unexpectedly accepted" >&2
  exit 1
fi

echo "CLI validation and dry-run command construction passed"
