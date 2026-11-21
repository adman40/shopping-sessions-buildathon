# V2 Rust foundation

Slice 0 freezes the cross-slice contracts in `crates/domain` and provides a fail-closed `app` shell.
Slice 6 provides the native Parquet writer and staged publication in `crates/output`.
The app intentionally does not process data until the integration slices land.
Contract changes require tests and an entry in `../docs/V2_DECISION_LOG.md`.

## Ownership

- Parser-owned immutable arenas back `ByteSpan` values in raw records.
- Identity owns the canonical-name arena behind `ByteHandle` values.
- Reducers own compact batches and their immutable payload or click-ID sidecars.
- Sweep owns aggregates until it transfers fully owned output rows to output workers.
- The app owns phase timing and samples cgroup and scratch resource high-water marks.

## Validation commands

Run the frozen contracts, Slice 6 output checks, harness tests, and configured Rust format and lint checks:

```sh
make v2-test
```

Generate the provenance-certified 1M development data:

```sh
python3 scripts/v2_generate.py --seed 1 --events 1000000 --clicks 250000 --output dev
```

Run randomized differentials against V1 after a candidate exists:

```sh
python3 scripts/v2_differential.py --candidate 'v2/docker_run.sh' --seeds 10
```

Run the correctness-gated benchmark:

```sh
python3 scripts/v2_benchmark.py --candidate 'v2/docker_run.sh' --fixtures fixtures --dev-data dev --dev-answer dev-answer
```

The benchmark prints no timing unless 36 fixtures, exact 1M output and schema, deterministic repeat output, timed-run correctness, telemetry validation, and the harness-overhead limit all pass. The [Slice 6 output contract](../docs/V2_IMPLEMENTATION_PLAN.md#slice-6--parquet-output-and-publication-contract) is authoritative for physical Parquet field optionality.
