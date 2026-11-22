#!/usr/bin/env bash
set -euo pipefail
ROOT=$(mktemp -d)
trap 'rm -rf "$ROOT"' EXIT
SCRIPT=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/shopping_bench.py
SHA=e1c9cc89ffaa51a4047372b1c3d39ceea4068fc0

python3 "$SCRIPT" --root "$ROOT" plan --sha "$SHA" --events 100000 --clicks 25000 >"$ROOT/plan.json"
python3 - "$ROOT/plan.json" <<'PY'
import json, sys
value = json.load(open(sys.argv[1], encoding="utf-8"))
assert value["dry_run"] is True
assert value["source_sha"] == "e1c9cc89ffaa51a4047372b1c3d39ceea4068fc0"
assert value["baseline_sha"] == "8a35b964aed48ecba5e44d061606b2a4bb9a2239"
assert value["source_sha"] != value["baseline_sha"]
assert value["generator_sha"] == value["baseline_sha"]
assert value["fixtures_sha"] == value["baseline_sha"]
assert value["oracle_sha"] == value["baseline_sha"]
assert value["dockerfile"] == "v2/Dockerfile"
assert value["idle_seconds"] == 5.0
assert value["timeout_seconds"] == 180.0
assert value["container_create"][0:2] == ["docker", "create"]
assert "V2_TELEMETRY_PATH=/scratch/v2-telemetry.json" in value["container_create"]
assert "docker start -a" == value["timed_boundary"]
PY

if python3 "$SCRIPT" --root "$ROOT" plan --sha main --events 100000 --clicks 25000 >/dev/null 2>&1; then
  echo "short/ref SHA unexpectedly accepted" >&2
  exit 1
fi
if python3 "$SCRIPT" --root "$ROOT" plan --sha "$SHA" --events 100000 --clicks 1 >/dev/null 2>&1; then
  echo "invalid click ratio unexpectedly accepted" >&2
  exit 1
fi
for removed in '--mode v1-smoke' '--idle-seconds 0' '--timeout-seconds 300' '--label manual'; do
  if python3 "$SCRIPT" --root "$ROOT" campaign --sha "$SHA" --events 100000 --clicks 25000 $removed >/dev/null 2>&1; then
    echo "removed campaign option unexpectedly accepted: $removed" >&2
    exit 1
  fi
done

echo "CLI validation and dry-run command construction passed"
