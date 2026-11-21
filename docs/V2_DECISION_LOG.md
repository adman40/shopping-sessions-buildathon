# V2 decision log

This file records changes and clarifications to frozen cross-slice contracts.
Every future contract change requires a focused contract test and an entry here.

## Slice 0 - 2026-11-20

- There are no semantic deviations from `V2_IMPLEMENTATION_PLAN.md`.
- `CompactEvent` and `CompactClick` are each exactly 16 bytes.
- Event payloads have invariant-preserving constructors and accessors. Click-ID tie values use index-aligned spans into an immutable, batch-owned contiguous byte arena so the records remain 16 bytes without truncating prices, category codes, campaign codes, or bytewise click-ID ties.
- Parser records contain `ByteSpan` values into parser-owned immutable arenas.
- Output rows own validated UTF-8 because they can outlive worker arenas and cross into writer threads.
- Telemetry version 1 is one JSON object with an outcome, seven ordered phase records, common counters, and resource high-water values. Timed-run validation requires a successful outcome, reconciled link/event/click row counters, and nonempty discovery/write counters.
- The benchmark requires exactly 36 fixtures, published-generator provenance certifying a 1,000,000-event development input, exact schema and row agreement, repeat agreement under the scorer's order-insensitive row semantics, canonical single-part output layout, and valid phase telemetry before it prints timing.
- The exact development scale and 0.5% harness-overhead ceiling are fixed acceptance criteria, not command-line options.
- Deterministic-repeat comparison is semantic rather than raw Parquet bytes because physical encodings and metadata are not scorer-visible and may vary while the required table is identical.
- Randomized differential tests use the published generator's smallest supported dataset of 1,000 events and compare against the unchanged V1 Polars pipeline.
- The `app` crate is a fail-closed submission-shell stub only.
- Parsing, identity construction, reduction, sweeping, output writing, orchestration, and optimization remain unimplemented for their later slices.
