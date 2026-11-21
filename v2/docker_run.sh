#!/bin/sh
# Run the V2 image under scorer constraints and export its telemetry when requested.
set -eu
IMAGE="${IMAGE:-shopping-sessions-v2:latest}"
CPUS="${CPUS:-8}"
MEM="${MEM:-24g}"
PLATFORM="${PLATFORM:-linux/amd64}"
DATA=$(cd "$1" && pwd -P)
mkdir -p "$2"
OUT=$(cd "$2" && pwd -P)
REMOVE_SCRATCH=0
if [ -n "${SCRATCH_DIR:-}" ]; then
  mkdir -p "$SCRATCH_DIR"
  SCRATCH=$(cd "$SCRATCH_DIR" && pwd -P)
else
  SCRATCH=$(mktemp -d)
  REMOVE_SCRATCH=1
fi
TELEMETRY=
if [ -n "${V2_TELEMETRY_PATH:-}" ]; then
  TELEMETRY_DIR=$(cd "$(dirname "$V2_TELEMETRY_PATH")" && pwd -P)
  TELEMETRY="$TELEMETRY_DIR/$(basename "$V2_TELEMETRY_PATH")"
fi
chmod 777 "$OUT" "$SCRATCH"
release() {
  if [ -n "$(find "$OUT" "$SCRATCH" ! -user "$(id -u)" -print -quit 2>/dev/null)" ]; then
    docker run --rm --platform "$PLATFORM" --network none --user 1000:1000 \
      -v "$OUT":/output:z -v "$SCRATCH":/scratch:z --entrypoint chmod "$IMAGE" \
      -R a+rwX /output /scratch >/dev/null 2>&1 || true
  fi
  TELEMETRY_SOURCE="$SCRATCH/v2-telemetry.json"
  if [ -n "$TELEMETRY" ] && [ -f "$TELEMETRY_SOURCE" ] && \
    [ "$TELEMETRY_SOURCE" != "$TELEMETRY" ] && \
    { [ ! -f "$TELEMETRY" ] || [ ! "$TELEMETRY_SOURCE" -ef "$TELEMETRY" ]; }; then
    cp "$TELEMETRY_SOURCE" "$TELEMETRY"
  fi
  if [ "$REMOVE_SCRATCH" -eq 1 ]; then
    rm -rf "$SCRATCH" 2>/dev/null || true
  fi
}
trap release EXIT
docker run --rm --platform "$PLATFORM" \
  --network none --read-only --tmpfs /tmp:rw,size=64m,mode=1777 \
  --cap-drop ALL --security-opt no-new-privileges \
  --pids-limit 4096 --cpus "$CPUS" --memory "$MEM" --memory-swap "$MEM" \
  --user 1000:1000 \
  -v "$DATA":/data:ro,z -v "$OUT":/output:z -v "$SCRATCH":/scratch:z \
  -e DATA_DIR=/data -e OUTPUT_DIR=/output -e SCRATCH_DIR=/scratch \
  -e V2_TELEMETRY_PATH=/scratch/v2-telemetry.json -e HOME=/scratch \
  -e NCPU="${CPUS%.*}" -e MEM_GB="${MEM%g}" \
  "$IMAGE"
