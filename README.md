# Shopping Sessions Buildathon

A complete Rust V2 candidate for the Shopping Sessions Buildathon. The repository-root Dockerfile builds a `linux/amd64` image, runs within the scorer's container limits, and writes both required Parquet output tables.

If this starter and `rules.md` ever disagree, `rules.md` wins. Ask the organizers if `rules.md` leaves you unsure.

The scorer-facing repository-root Dockerfile runs the Rust application under `v2/`. See `v2/README.md` for implementation status and validation commands; the unchanged Python pipeline remains the V1 differential oracle.

## Iteration workflow

- Read `rules.md` before changing behavior.
- Update the relevant Rust crate under `v2/crates/`.
- Run all 36 fixtures, randomized V1 differentials, and the 1M-event correctness gates before submitting a commit SHA.

## Run and test locally

You need Docker with buildx, and Python 3.10+ with the test-script dependencies. Install them with `python3 -m pip install -r requirements.txt`.

Build the image and run all 36 fixtures:

```sh
make build        # docker buildx build --platform linux/amd64 --load -t shopping-sessions:latest .
make fixtures     # runs the image on all 36 fixtures with scorer flags, diffs each against expected/
```

Build, run, and check the 1M-event dev set. Build the included generator first (Rust 1.85 or newer), then put it on your `PATH`.

```sh
cargo build --release --manifest-path generator/Cargo.toml
export PATH="$PWD/generator/target/release:$PATH"
make dev                                # generate --seed 1 --events 1000000 --clicks 250000 --output dev
make run-dev                            # runs your image on dev/ with scorer flags and times it
make check-dev                          # exact diff against dev-answer/, the track page's dev-answer.zip unzipped here
```

If your laptop cannot spare 8 cores and 24 GB, run with lower limits using `make run-dev CPUS=4 MEM=8g`. Your local times will not match the scorer anyway; only the board's times count.

- **x86 Linux and Windows machines.** Build and run the amd64 image natively.
- **Apple-silicon Macs.** Emulating amd64 is slow. For fast local loops, build a native image with `make build run-dev PLATFORM=linux/arm64`. The Dockerfile pins no platform, so it builds either way. Check the amd64 image before you submit.

To run the V1 differential oracle without Docker:

```sh
python3 -m pip install -r requirements.txt
python3 scripts/run_fixtures.py fixtures -- python3 pipeline/pipeline.py
```

## Submitting and scoring

Submit your public GitHub repository on the Buildathon track page. Enter a commit SHA to score a specific revision, or leave it empty to score the newest commit on the default branch.

The image's `CMD` runs with no arguments. Exit code 0 means "done, check my output". Build time is never timed; only the run is timed. The track page lists the current practice limits and how the final is scored.

## Container limits

Scorers are Google Cloud n2 Intel Xeon machines, Cascade Lake or newer, with AVX-512 available. Your container gets:

| Resource | Setting |
| --- | --- |
| `/data` | The dataset, read-only |
| `/output` | Empty, writable. Write `/output/user_daily/` and `/output/campaign_daily/` as Parquet |
| `/scratch` | Empty, writable, fast local disk for spills and temp files |
| CPU / RAM | 8 vCPUs, hyperthreads of an Intel Xeon, 24 GB, no swap |
| Network | None |
| Env vars | `DATA_DIR=/data`, `OUTPUT_DIR=/output`, `SCRATCH_DIR=/scratch`, `HOME=/scratch`, `NCPU=8`, `MEM_GB=24` |
| Filesystem | Root is read-only apart from `/tmp`, 64 MB in memory. The process runs as uid 1000, not root |

## Common pitfalls

- **CSV semantics.** The V1 Polars oracle's quoting and malformed-line handling can differ from `rules.md`; keep V2 parser changes covered by fixtures and differential checks.
- **V1 oracle threads.** `pipeline.py` sets `POLARS_MAX_THREADS` from `NCPU` before importing Polars. Otherwise Polars sizes its thread pool from the host machine's cores, which inside Docker on a Mac can differ from your CPU limit.
- **Memory.** The container has 24 GB and no swap. Qualify representative 60M-event runs rather than extrapolating memory use from the 1M development set.
- **`\d` is not `[0-9]`.** In several regex engines, `\d` matches Unicode digits. The rules mean ASCII digits only.
- **Extra files in output folders.** `_SUCCESS` and other non-Parquet files in the output folders are ignored.

## Self-checks

- Summed over all rows, `sessions` and `revenue_cents` are equal in both tables.
- `day` must be a Parquet `DATE`, not a string or timestamp.
- Row order and the number of Parquet files do not matter. Exact column names do, and there must be no extra columns.

## Files in this starter

- `v2/` holds the scorer-facing Rust workspace; see `v2/README.md`.
- `pipeline/pipeline.py` remains the V1 differential oracle.
- `Dockerfile` builds a `linux/amd64` image for the Intel Xeon scorers.
- `fixtures/` holds the 36 edge cases, each an `input/` folder and the `expected/` tables.
- `scripts/` runs your image the way the scorer does and diffs the output.
