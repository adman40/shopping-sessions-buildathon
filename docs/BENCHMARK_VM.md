# Dedicated GCP benchmark VM

This runbook operates `benchmark-x86` manually as a repeatable scorer-prediction environment.
It does not automate GCP lifecycle, change cloud policy, or publish an official score.
The runner accepts only a public full 40-character commit SHA and keeps benchmark source separate from the operator harness.

## Scope and parity

The runner builds the V2 candidate from its requested commit and `v2/Dockerfile`.
Generator source, all fixture inputs and expected outputs, and the V1 oracle/checker image are independently checked out from the immutable validated baseline `8a35b964aed48ecba5e44d061606b2a4bb9a2239`.
The runner builds images and the baseline generator outside the timer, verifies all 36 baseline fixtures, checks the complete generated-dataset manifest, and runs the baseline V1 image untimed on that exact dataset to authorize the expected output.
It then proves the VM is idle, creates the stopped candidate container, and times from immediately before `docker start -a` through observed exit using the host monotonic clock.
The candidate container is verified after creation to use linux/amd64, 8 CPUs, 24 GiB memory with equal memory/swap, no network, a read-only root, a 64 MiB `/tmp`, uid/gid 1000, 4096 PIDs, dropped capabilities, no-new-privileges, and the exact `/data`, `/output`, `/scratch`, and environment contract.
The candidate receives `V2_TELEMETRY_PATH=/scratch/v2-telemetry.json`.
A single nonblocking `flock` serializes recovery, fixture checks, and measurement.
Before any candidate can qualify, the runner verifies GCP metadata for the exact project, zone, instance, and `n2-standard-8` machine type, plus x86-64 architecture, CPU model, eight vCPUs, expected physical-memory range, and cgroup v2.
The result records Docker exit and OOM state, cgroup-v2 memory/CPU/I/O counters, output and scratch sampled high-water marks, exact semantic comparison against the untimed oracle output, candidate and baseline Git trees, baseline fixture and generator trees, source/generator/candidate-image/oracle-image/dataset/host provenance, and the exact container-create command.
A V2 timing is eligible only when fixtures pass, the trusted oracle succeeds, candidate rows exactly match the oracle, telemetry has the frozen successful reconciled format, cgroup memory is observed below 22 GiB, sampled output plus scratch stays below 45 GiB, and measured runtime stays below the fixed 180-second limit.
The exact candidate telemetry bytes are copied into the run bundle, checksummed, and embedded as parsed data in `result.json`.

The current implementation has two explicit scorer-parity gaps.
The combined output/scratch disk allowance is monitored at 45 GiB but is not a hard filesystem cap, so results say `monitor-only-no-hard-cap`.
The candidate run is warm with unspecified official cache state, so it is not a cold-cache scorer-equivalent campaign.
The dedicated VM uses persistent disk rather than known scorer-local scratch.

## Local tests and dry run

Run these before copying the harness to the VM:

```bash
make bench-vm-test
# Replace the all-zero value with the public full V2 commit SHA.
make bench-vm-plan \
  BENCH_SHA=0000000000000000000000000000000000000000 \
  BENCH_EVENTS=100000 BENCH_CLICKS=25000
```

The plan command does not access Docker, GitHub, or GCP.
It prints the exact container-create command and timing boundary.

## Start, connect, and minimally provision

Use the named configuration and explicit project and zone for every command.
Batch-mode SSH prevents prompts from being mistaken for successful unattended access.

```bash
GCLOUD=$(command -v gcloud)
PROJECT=buildathon-benchmarks-515401
ZONE=us-central1-a
VM=benchmark-x86

"$GCLOUD" --configuration=buildathon compute instances start "$VM" \
  --project="$PROJECT" --zone="$ZONE"
"$GCLOUD" --configuration=buildathon compute ssh "amanoni@$VM" \
  --project="$PROJECT" --zone="$ZONE" \
  --ssh-flag='-o BatchMode=yes' --command='id && docker version && flock --version'
```

Create only the dedicated layout, owned by the existing `amanoni` account.
Do not create a service, privileged benchmark user, token, firewall rule, or service-account change.

```bash
"$GCLOUD" --configuration=buildathon compute ssh "amanoni@$VM" \
  --project="$PROJECT" --zone="$ZONE" --ssh-flag='-o BatchMode=yes' \
  --command='sudo install -d -o amanoni -g amanoni -m 0750 /srv/shopping-bench /srv/shopping-bench/bin'

"$GCLOUD" --configuration=buildathon compute scp \
  bench/vm/shopping_bench.py "amanoni@$VM:/tmp/shopping_bench.py" \
  --project="$PROJECT" --zone="$ZONE" --scp-flag='-oBatchMode=yes'

"$GCLOUD" --configuration=buildathon compute ssh "amanoni@$VM" \
  --project="$PROJECT" --zone="$ZONE" --ssh-flag='-o BatchMode=yes' \
  --command='install -m 0755 /tmp/shopping_bench.py /srv/shopping-bench/bin/shopping-bench && rm /tmp/shopping_bench.py'
```

Docker and Cargo must already be available to `amanoni`.
The runner uses only Python's standard library on the host.
Output checking runs with the image's Polars installation.

## Run a V2 100K candidate measurement

Use an immutable public commit that contains an integrated V2 candidate.
The command builds that candidate while obtaining the generator, fixtures, expected outputs, and V1 oracle image only from the pinned baseline.
It generates seed 1 with 100,000 events and 25,000 clicks, verifies the dataset manifest has neither missing nor extra files, runs all baseline fixtures, produces an untimed trusted-oracle output on the same generated input, and only then attempts the measured run.
It stops without escalating if host provenance, fixtures, oracle generation, idle/resource checks, container verification, exit/OOM checks, exact oracle comparison, or V2 telemetry validation fail.

```bash
# Replace the all-zero value with the public full V2 commit SHA.
V2_SHA=0000000000000000000000000000000000000000
"$GCLOUD" --configuration=buildathon compute ssh "amanoni@$VM" \
  --project="$PROJECT" --zone="$ZONE" --ssh-flag='-o BatchMode=yes' \
  --command="/srv/shopping-bench/bin/shopping-bench --root /srv/shopping-bench campaign --sha $V2_SHA --seed 1 --events 100000 --clicks 25000"
```

The idle preflight refuses unrelated running containers, samples `/proc/stat`, and requires at least 90% idle CPU, at most 1% steal, and a one-minute load average no greater than 2.0.
Run on an otherwise idle VM and do not start package updates or another benchmark while it is active.
The kernel lock at `/srv/shopping-bench/locks/run.lock` is authoritative; its file contents are informational.

Every attempt that reaches measurement writes `runs/<run-id>/result.json`, a one-row `result.csv`, candidate and oracle stdout/stderr, retained oracle output and manifest, immutable image-inspect and dataset-provenance artifacts, and an append-only row in `results/results.csv`.
Runs retain `candidate-telemetry.json` with its checksum and validation metadata in the authoritative schema-version-2 JSON.
The results index accepts only the current V2 schema; legacy V1 CSV migration is intentionally unsupported.
`result_status` distinguishes an eligible V2 candidate from an ineligible result without claiming an official score.
Failed fixture gates are stored under `results/fixtures-*.json` and suppress measurement.

## Progressive size gates

Allowed event tiers are 100K, 1M, 5M, 15M, 30M, and 60M, with clicks fixed at one quarter of events.
Tier advancement has no human approval step: the next tier is enabled only by a prior result whose machine-verifiable correctness, exact-oracle, telemetry, resource, timing, baseline-provenance, and host-provenance gates all made it `eligible-v2-candidate`.
The runner automatically refuses advancement after fixture or exact-oracle failure, nonzero exit, timeout, OOM, missing required telemetry or cgroup memory, sampled combined disk at or above 45 GiB, memory at or above 22 GiB, or a linear next-tier projection at or above the fixed 180-second limit.
Use seeds 1, 2, and 3 only for an eventual 60M release campaign.
Do not describe any run as V2 until an integrated V2 image passes its own full correctness gates.

## Status, recovery, and cleanup

A killed client can leave a labeled stopped container.
Recovery and cleanup are exact-label scoped and never invoke `docker system prune`.
A dry run lists cleanup actions without changing state.

```bash
/srv/shopping-bench/bin/shopping-bench --root /srv/shopping-bench recover
/srv/shopping-bench/bin/shopping-bench --root /srv/shopping-bench cleanup --dry-run
/srv/shopping-bench/bin/shopping-bench --root /srv/shopping-bench cleanup
```

The runner removes only stopped containers labeled `org.shopping-bench.managed=true` and its own partial dataset directories.
Certified datasets, checkouts, images, completed results, output, and logs are retained until explicitly removed.

## Download and stop

Download results without deleting the VM copy.
Replace `<run-id>` with the identifier printed by the campaign command.

```bash
mkdir -p benchmark-results/<run-id>
"$GCLOUD" --configuration=buildathon compute scp --recurse \
  "amanoni@$VM:/srv/shopping-bench/runs/<run-id>" \
  benchmark-results/<run-id>/ \
  --project="$PROJECT" --zone="$ZONE" --scp-flag='-oBatchMode=yes'
"$GCLOUD" --configuration=buildathon compute scp \
  "amanoni@$VM:/srv/shopping-bench/results/results.csv" \
  benchmark-results/ \
  --project="$PROJECT" --zone="$ZONE" --scp-flag='-oBatchMode=yes'
```

The machine eligibility decision verifies the full candidate SHA and tree, pinned baseline SHA and tree, fixture and generator trees, generator binary checksum, candidate and oracle image IDs and inspect checksums, Dockerfile checksums, complete dataset and oracle-output manifest checksums, exact scorer-equivalent create command, fixture count, exact oracle match, V2 telemetry checksum and validation result, result status, host identity, and resource thresholds.

Confirm no benchmark container is running before stopping the VM:

```bash
"$GCLOUD" --configuration=buildathon compute ssh "amanoni@$VM" \
  --project="$PROJECT" --zone="$ZONE" --ssh-flag='-o BatchMode=yes' \
  --command='test -z "$(docker ps -q --filter label=org.shopping-bench.managed=true)"'
"$GCLOUD" --configuration=buildathon compute instances stop "$VM" \
  --project="$PROJECT" --zone="$ZONE"
```
