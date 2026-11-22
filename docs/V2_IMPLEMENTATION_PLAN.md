# V2 Rust prototype implementation plan

## Goal and non-goals

V2 replaces the scored Polars data path with a compact Rust pipeline. It keeps the current `main` branch as the known-good, submit-ready Polars baseline. After Slice 0 was frozen on the published `v2/slice0-contracts` branch, each implementation slice branches from that contract baseline and is submitted only after it passes the same gates.

The objective is a correct pipeline with a measured x86-64 Linux runtime below 150 seconds, leaving margin below the 180-second limit. The research estimate is 114 seconds; that is a planning hypothesis, not a target we assume is achieved.

V2 will not begin with SIMD, a custom Parquet encoder, or a fully external algorithm. Those are measured follow-ups only if profiling proves they are needed.

## Research decisions frozen for V2

| Area | V2 decision | Research basis |
| --- | --- | --- |
| Execution engine | Native Rust semantic kernel; retain V1 Polars as a differential oracle | `research-03-engines` |
| CSV and time parsing | Scalar LF-only byte parser first; exact quote/field grammar; F1/F2/F3 timestamp parser | `research-04-parsing` |
| Identity | Bytewise-ordered IDs, sequential union-find, byte-minimum component name | `research-05-identity` |
| Deduplication | Exact event winner map (minimum `ingest_ts`) and exact click-ID dedup before validation | `research-01-rules`, `research-06-session` |
| Compute model | Compact numeric rows, person/time ordering, fused per-person session + attribution + assists sweep | `research-02-workload`, `research-06-session` |
| Concurrency and memory | Eight bounded workers, private aggregates, phase-owned arenas, 12 GiB working target | `research-07-memory` |
| Output | One `part-0.parquet` per output table; native writer, Zstd level 1, 128K row groups initially | `research-08-parquet` |
| Failure mode | In-memory fast path first; deterministic radix-partition spill fallback if memory thresholds fire | `research-09-spill` |
| Measurement | Exact correctness before timing; per-phase telemetry and x86-64 Linux runs are authoritative | `research-10-benchmark` |

## Architecture and contracts

V2 uses a Cargo workspace so independent workstreams have separate ownership. Each slice must expose stable, unit-tested types rather than call another slice's private implementation.

```text
v2/
  Cargo.toml                 # workspace
  crates/
    domain/                  # byte ordering, parsed/compact row types, errors, time constants
    parser/                  # file discovery, CSV grammar, F1/F2/F3, catalog/manifest readers
    identity/                # ID arena/index, DSU, canonical-person mapping
    reduce/                  # event/click dedup and compact surviving rows
    sweep/                   # person/time ordering, sessions, attribution, assists, aggregates
    output/                  # Parquet schemas and staged output writer
    app/                     # bounded orchestration, telemetry, spill transition, CLI
  tests/                     # fixture, differential, synthetic-scale, and property tests
```

### Boundary contracts

- **Parser -> identity/reduce:** parsed rows preserve exact byte spans and parse status. No trim, Unicode-digit acceptance, CRLF normalization, or validation-order shortcut is allowed.
- **Identity -> reduce:** `Canonicalize` in `v2/crates/domain/src/compact.rs` owns the frozen canonicalization contract.
- **Reduce -> sweep:** only validated, non-late winning events and valid deduplicated clicks become 16-byte compact records. Event and click records are sorted/grouped by `(person_code, timestamp)`.
- **Sweep -> output:** deterministic `UserDailyRow` and `CampaignDailyRow` aggregates with the exact required nullability and date types. Workers own local aggregates; reduction has no order-dependent floating point behavior.
- **Output:** writes only staged paths under `/output` and `/scratch`, verifies schema/readback, then makes exactly one Parquet file visible per required output directory.

Any contract change requires fixture tests and an updated entry in the decision log. The V1 Polars result remains the differential oracle for randomized tiny datasets and the published 1M development data.

## Parallel vertical slices

### Slice 0 — Contracts, oracle, and benchmark envelope

**Owns:** `domain`, fixture runner integration, randomized differential generator, telemetry format, Docker/Rust submission shell.

**Research applied:** correctness gates and phase budgets from research 01/03/10.

**Deliverables:**
- Canonical data types, error/status enums, bytewise comparator, and output schema definitions.
- A test command that requires 36/36 fixtures, exact 1M comparison, output schema validation, and deterministic repeat output.
- Phase counters/timers for discovery, links, events, clicks, sort, sweep, and write; cgroup memory and scratch high-water capture.

**Acceptance:** no timing is reported unless correctness passes; harness overhead under 0.5% at 1M.

**Dependency:** must land first. This is the only short serial gate before parallel implementation.

### Slice 1 — Exact input discovery and parser

**Owns:** `parser` and parser-focused tests only.

**Research applied:** research 04's scalar LF-line parser, per-file header rule, strict field grammar, ASCII-only integers, and F1/F2/F3 parser.

**Deliverables:**
- Deterministic glob/file discovery and manifest/catalog loading.
- Line parser that treats only `\n` as a line end and preserves `\r` as data.
- Typed parsed link/event/click rows with enough status retained to enforce event dedup-before-validation.
- Differential/property tests for malformed quotes, empty fields, offsets, leap dates, boundary timestamps, and overflow.

**Acceptance:** byte-for-byte field/status agreement with a simple reference parser on fixture and fuzz cases. No SIMD in this slice.

**Can run in parallel with:** slices 2, 4, 5, and 6 once Slice 0 types are frozen.

### Slice 2 — Identity graph and canonicalization

**Owns:** `identity` and identity-focused tests only.

**Research applied:** research 05's lexicographic raw-UTF-8 ID encoding, sequential union-find, and post-pass byte-minimum canonical names.

**Deliverables:**
- Immutable byte arena/index with exact byte equality after hashes.
- Link ingestion, union-by-size/path-compression DSU, canonical-name post-pass, and singleton behavior.
- A read-only canonicalization interface for event/click reduction.

**Acceptance:** L1/L2/L3 and randomized graph tests pass; result is independent of link order/file order; the capacity-accounted identity-construction peak stays below the 0.5 GiB planning envelope at representative scale.

**Can run in parallel with:** parser tests using raw link records, and sweep tests using synthetic person codes.

### Slice 3 — Event reducer and compaction

**Owns:** event dedup, event validation, catalog lookup, and compact-event emission in `reduce`.

**Research applied:** research 01/06/09's minimum-ingest winner rule, post-winner validation, 16-byte compact event layout, and phase memory release.

**Deliverables:**
- Exact event-ID winner selection with collision-safe equality.
- Validation only after the winner is selected, including late-event behavior.
- Canonical-person resolution for every survivor and category lookup for purchases, then compact event records.
- Explicit release of event-ID winner state before sorting.

**Acceptance:** all event fixtures pass, especially invalid-earliest duplicate cases; survivor counters reconcile with inputs; compact record layout is asserted in tests.

**Dependencies:** Slice 0 contract; uses Slice 1 and Slice 2 interfaces but may begin against fakes.

### Slice 4 — Click reducer and compaction

**Owns:** click dedup, validation, campaign lookup, and compact-click emission in `reduce`.

**Research applied:** research 06/09's exact click ID dedup, valid-click window, and retained click-ID bytes for same-time ties.

**Deliverables:**
- Exact click-ID dedup with collision-safe equality.
- Validation after dedup, canonical person mapping, campaign-code lookup, and compact click rows.
- Batch-owned click-ID bytes remain resolvable solely for tie resolution.

**Acceptance:** click window boundary, unknown campaign, quoted-duplicate, and same-timestamp tie tests pass.

**Dependencies:** Slice 0 contract; can develop independently of Slice 3.

### Slice 5 — Ordered per-person session, attribution, and assist sweep

**Owns:** `sweep` and synthetic compact-record tests.

**Research applied:** research 06's packed person/time ordering and fused scan; research 10's recommendation to replace range joins with a sliding click sweep.

**Deliverables:**
- Stable grouping/sort experiment behind one interface; begin with the simplest correct compact sort and retain counters for radix alternatives.
- One per-person scan that implements 30-minute sessions, UTC day of session start, category tie-breaking, latest qualifying click, and seven-day distinct-campaign assists.
- Worker-private user/campaign aggregates and deterministic reduction.

**Acceptance:** exact boundary behavior for equal event times, 30:00.000 vs 30:00.001, seven-day inclusivity, click-at-session-start, click-ID bytewise ties, organic, and assist de-duplication.

**Dependencies:** Slice 0 only for development; consumes Slice 3/4 outputs at integration.

### Slice 6 — Parquet output and publication contract

**Owns:** `output`, schema tests, and container output safety.

**Research applied:** research 08's exact schema and native Parquet guidance.

**Deliverables:**
- Staged output writer for `user_daily/part-0.parquet` and `campaign_daily/part-0.parquet`.
- `DATE`, `INT64`, and UTF-8 logical types; required physical fields except nullable `user_daily.top_category`; only `user_daily.top_category` values may be null.
- Zstd-1/128K-row-group initial configuration and readback/schema test.

**Acceptance:** empty and non-empty output cases pass every fixture; output contains no unexpected visible files; no writes occur outside `/output` and `/scratch`.

**Dependencies:** Slice 0 types; can use synthetic aggregates immediately.

### Slice 7 — Orchestration, bounded memory, and spill

**Owns:** `app`, bounded queues, phase lifecycle, scratch management, and the fallback path.

**Research applied:** research 07's eight-worker ownership model and research 09's adaptive radix spill design.

**Deliverables:**
- Phase scheduler with bounded queues and phase-owned buffer/arena release.
- Memory/scratch telemetry and thresholds: enter spill at projected/actual 12 GiB, drain at 16 GiB, fail closed before 20 GiB.
- Fast in-memory path first; 64-way dedup partition and 256-way person partition fallback behind the same contracts.

**Acceptance:** forced-low-memory test transitions without changing output; a corrupt/truncated spill file fails nonzero and never publishes partial output.

**Dependencies:** begins after Slice 0, but integrates after slices 3–6 have stable contracts. Spill optimization is not required for the first correct V2 submission unless measured memory requires it.

## Integration sequence

1. **Freeze Slice 0 interfaces and baseline test command.** Do not allow cross-slice type churn after this point.
2. **Parallel wave A:** parser, identity, sweep kernel, output writer, and event/click reducers against fixtures or synthetic records.
3. **Parallel wave B:** connect parser + identity + reducers; emit compact rows and prove event/click counters.
4. **Parallel wave C:** connect sorted compact rows to the sweep and writer; require 36/36 fixtures and 1M exact output.
5. **Performance wave:** run 100K, 1M, 5M, 15M, and 30M scale points; collect phase timings and memory before choosing parser/sort/spill optimizations.
6. **Release wave:** native x86-64 Linux runs at 60M across at least three seeds; submit only a commit with correctness evidence and at least 15–30 seconds timing margin.

## Initial allocation and merge discipline

| Workstream | First branch scope | Merge gate |
| --- | --- | --- |
| A | Slice 0 contracts/harness | fixture harness compiles and records phase metadata |
| B | Slice 1 parser | parser differential/property suite |
| C | Slice 2 identity | graph determinism + L1/L2/L3 |
| D | Slice 3 events | event fixture subset + compact-layout tests |
| E | Slice 4 clicks | click fixture subset + tie/window tests |
| F | Slice 5 sweep | synthetic session/attribution/assist suite |
| G | Slice 6 output | Parquet schema/readback suite |
| H | Slice 7 orchestration | forced-memory transition test |

Each branch changes only its owned crate/tests plus minimal workspace registration. It must rebase on the frozen Slice 0 contract rather than changing another slice's API. Integration commits are responsible for end-to-end fixture/dev evidence.

## Performance decision gates

- Do not optimize a phase before it has exact correctness evidence and a measured cost.
- If sweep/attribution is dominant, optimize the per-person sweep before parser SIMD or custom output.
- If parse/allocation/RSS dominates, prioritize interned IDs and the scalar specialized parser; SIMD requires differential tests and a measured x86 gain.
- If sorting/dedup crosses 12 GiB or has a 30M scaling cliff, implement the partitioned spill path before tuning minor constants.
- Keep the native Parquet writer unless it exceeds 5–10% of wall time.
- Do not remove or alter the V1 Polars baseline; use it for differential checks throughout V2.

## Definition of a submit-ready V2 iteration

A submission commit must record: 36/36 fixtures, exact 1M output match, output schema pass, a clean container run as uid 1000 with no network, phase timings, cgroup peak memory, scratch high-water, and an official or scorer-equivalent x86-64 Linux runtime. A faster run without these correctness gates is not eligible for submission.
