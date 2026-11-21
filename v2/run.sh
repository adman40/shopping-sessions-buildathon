#!/bin/sh
# V2 scorer entry point. The binary owns output staging and telemetry emission.
set -eu
: "${DATA_DIR:=/data}" "${OUTPUT_DIR:=/output}" "${SCRATCH_DIR:=/scratch}"
: "${V2_TELEMETRY_PATH:=$SCRATCH_DIR/v2-telemetry.json}"
export DATA_DIR OUTPUT_DIR SCRATCH_DIR V2_TELEMETRY_PATH
export TMPDIR="$SCRATCH_DIR" HOME="$SCRATCH_DIR"
exec /app/shopping-sessions-v2
