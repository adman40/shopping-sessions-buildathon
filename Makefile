IMAGE ?= shopping-sessions:latest
CPUS  ?= 8
MEM   ?= 24g
# The scorer's platform. PLATFORM=linux/arm64 builds and runs natively on an Apple-silicon Mac, for fast local loops.
PLATFORM ?= linux/amd64
DEV   ?= dev
# In the monorepo the fixtures live two levels up; the published starter has its own copy.
FIXTURES ?= $(if $(wildcard fixtures),fixtures,../../fixtures)
export IMAGE CPUS MEM PLATFORM

.PHONY: build fixtures dev run-dev check-dev

## Build the image for the scorer's platform, linux/amd64 (emulated on Apple silicon)
build:
	docker buildx build --platform $(PLATFORM) --load -t $(IMAGE) .

## Run the image on all 36 edge-case fixtures and diff against the expected tables
fixtures: build
	python3 scripts/run_fixtures.py $(FIXTURES) -- scripts/docker_run.sh

## Make the 1M-event dev set with the published generator (needs `generate` on PATH)
dev:
	generate --seed 1 --events 1000000 --clicks 250000 --output $(DEV)

## Run on the dev set with scorer flags and time it
run-dev: build
	rm -rf out && time scripts/docker_run.sh $(DEV) out

## Compare your dev output with the published dev answer (dev-answer.zip, unzipped): make check-dev ANSWER=dev-answer
ANSWER ?= dev-answer
check-dev:
	python3 scripts/compare.py --reference-right out $(ANSWER)

V2_IMAGE ?= shopping-sessions-v2:latest
V2_CANDIDATE ?= v2/docker_run.sh
V2_DEV ?= dev
V2_ANSWER ?= dev-answer
V2_FAILURE_LOG_DIR ?= benchmark-results/v2-failures

.PHONY: v2-build v2-test v2-dev v2-differential v2-benchmark v2-full

## Build the Slice 0 V2 Rust submission shell
v2-build:
	docker buildx build --platform $(PLATFORM) --load -f v2/Dockerfile -t $(V2_IMAGE) .

## Compile, test, format-check, and lint all integrated V2 crates and the harness
v2-test:
	cargo test --locked --manifest-path v2/Cargo.toml
	cargo fmt --all --manifest-path v2/Cargo.toml --check
	cargo clippy --locked --manifest-path v2/Cargo.toml --all-targets -- -D warnings
	python3 -m unittest discover -s v2/tests -p 'test_*.py'

## Generate provenance-certified exact 1M development data
v2-dev:
	python3 scripts/v2_generate.py --seed 1 --events 1000000 --clicks 250000 --output $(V2_DEV)

## Compare randomized small candidate outputs with the V1 Polars oracle
v2-differential:
	mkdir -p "$(V2_FAILURE_LOG_DIR)"
	V2_FAILURE_LOG_DIR="$(abspath $(V2_FAILURE_LOG_DIR))" IMAGE=$(V2_IMAGE) python3 scripts/v2_differential.py --candidate '$(V2_CANDIDATE)'

## Report timing only after every V2 correctness and telemetry gate passes
v2-benchmark:
	mkdir -p "$(V2_FAILURE_LOG_DIR)"
	V2_FAILURE_LOG_DIR="$(abspath $(V2_FAILURE_LOG_DIR))" IMAGE=$(V2_IMAGE) python3 scripts/v2_benchmark.py --candidate '$(V2_CANDIDATE)' --fixtures $(FIXTURES) --dev-data $(V2_DEV) --dev-answer $(V2_ANSWER)

## Run workspace checks, build the candidate, then invoke the complete correctness-gated harness
v2-full: v2-test v2-build
	$(MAKE) v2-benchmark
