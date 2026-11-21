IMAGE ?= shopping-sessions:latest
CPUS  ?= 8
MEM   ?= 24g
# The scorer's platform. PLATFORM=linux/arm64 builds and runs natively on an Apple-silicon Mac, for fast local loops.
PLATFORM ?= linux/amd64
DEV   ?= dev
# In the monorepo the fixtures live two levels up; the published starter has its own copy.
FIXTURES ?= $(if $(wildcard fixtures),fixtures,../../fixtures)
export IMAGE CPUS MEM PLATFORM

.PHONY: build fixtures dev run-dev check-dev bench-vm-test bench-vm-plan

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
	python3 scripts/compare.py out $(ANSWER)

## Run the dedicated VM harness unit and shell tests (no cloud or Docker access)
bench-vm-test:
	python3 -m unittest discover -s bench/vm/tests -p 'test_*.py' -v
	bash bench/vm/tests/test_cli.sh

## Print the exact VM command plan without performing any work
BENCH_SHA ?= 8a35b964aed48ecba5e44d061606b2a4bb9a2239
BENCH_EVENTS ?= 100000
BENCH_CLICKS ?= 25000
bench-vm-plan:
	python3 bench/vm/shopping_bench.py plan --sha $(BENCH_SHA) --events $(BENCH_EVENTS) --clicks $(BENCH_CLICKS)
