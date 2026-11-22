# V2 Rust foundation

Slice 0 freezes the cross-slice contracts in `crates/domain`.
Slice 1 adds exact input discovery, CSV and timestamp parsing, and catalog and manifest loading in `crates/parser`.
Slice 2 implements the deterministic identity graph and canonicalization in `crates/identity`.
Slice 3 implements event reduction and compaction in `crates/reduce`.
Slice 4 implements the click reducer in `crates/reduce` against those contracts; see the [V2 implementation plan](../docs/V2_IMPLEMENTATION_PLAN.md) for its detailed boundary.
Slice 5 implements the ordered per-person session, attribution, assist, and aggregate kernel in `crates/sweep` against compact reducer batches.
Slice 6 provides the native Parquet writer and staged publication in `crates/output`.
The `app` orchestrates all integrated slices.
Exact source heads, rollback boundaries, and qualification caveats are recorded in the [integration provenance](../docs/V2_INTEGRATION_PROVENANCE.md).
Contract changes require tests and an entry in `../docs/V2_DECISION_LOG.md`.

## Ownership

- Parser-owned immutable arenas back `ByteSpan` values in raw records.
- Identity owns the canonical-name arena behind `ByteHandle` values.
- Reducers own compact batches and their immutable payload or click-ID sidecars.
- Sweep owns aggregates until it transfers fully owned output rows to output workers.
- The app owns phase timing and samples cgroup and scratch resource high-water marks.

## Validation commands

Run all integrated V2 crate and harness tests plus configured Rust format and lint checks:

```sh
make v2-test
```

Generate the provenance-certified 1M development data:

```sh
python3 scripts/v2_generate.py --seed 1 --events 1000000 --clicks 250000 --output dev
```

Run randomized differentials against V1 after a candidate exists:

```sh
make v2-differential
```

Build the scorer-facing repository-root Dockerfile and run the complete correctness-gated harness:

```sh
make v2-full
```

Candidate failures are written inside the container under `/scratch/v2-failures` and copied to `benchmark-results/v2-failures` by default.
Set `V2_FAILURE_LOG_DIR` to retain them elsewhere.
The benchmark prints no timing unless 36 fixtures, exact 1M output and schema, deterministic repeat output, timed-run correctness, telemetry validation, and the harness-overhead limit all pass.
The [Slice 6 output contract](../docs/V2_IMPLEMENTATION_PLAN.md#slice-6--parquet-output-and-publication-contract) is authoritative for physical Parquet field optionality.
