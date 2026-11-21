# Dedicated GCP benchmark VM

This runbook operates `benchmark-x86` manually as a repeatable scorer-prediction environment.
It does not automate GCP lifecycle, change cloud policy, or publish an official score.
The runner accepts only a public full 40-character commit SHA and keeps benchmark source separate from the operator harness.

## Scope and parity

The runner builds the image and generator outside the timer, verifies all 36 fixtures, checks the generated dataset checksum, proves the VM is idle, creates the stopped container, and times from immediately before `docker start -a` through observed exit using the host monotonic clock.
The container is verified after creation to use linux/amd64, 8 CPUs, 24 GiB memory with equal memory/swap, no network, a read-only root, a 64 MiB `/tmp`, uid/gid 1000, 4096 PIDs, dropped capabilities, no-new-privileges, and the exact `/data`, `/output`, `/scratch`, and environment contract.
A single nonblocking `flock` serializes recovery, fixture checks, and measurement.
The result records Docker exit and OOM state, cgroup-v2 memory/CPU/I/O counters when available, output and scratch sampled high-water marks, output self-checks, source/generator/image/dataset/host provenance, and unavailable-counter labels.

The current implementation has two explicit scorer-parity gaps.
The combined output/scratch disk allowance is monitored at 45 GiB but is not a hard filesystem cap, so results say `monitor-only-no-hard-cap`.
The smoke run is warm with unspecified official cache state, so it is not a cold-cache scorer-equivalent campaign.
The dedicated VM uses persistent disk rather than known scorer-local scratch.

## Local tests and dry run

Run these before copying the harness to the VM:

```bash
make bench-vm-test
make bench-vm-plan \
  BENCH_SHA=8a35b964aed48ecba5e44d061606b2a4bb9a2239 \
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

## Run the V1 100K smoke measurement

The published V1 baseline is the immutable public commit below.
The command builds the generator and image outside timing, generates seed 1 with 100,000 events and 25,000 clicks, verifies the dataset manifest, runs all fixtures, and only then attempts the measured run.
It stops without escalating if fixtures, idle/resource checks, container verification, exit/OOM checks, or output self-checks fail.

```bash
"$GCLOUD" --configuration=buildathon compute ssh "amanoni@$VM" \
  --project="$PROJECT" --zone="$ZONE" --ssh-flag='-o BatchMode=yes' \
  --command='/srv/shopping-bench/bin/shopping-bench --root /srv/shopping-bench campaign --sha 8a35b964aed48ecba5e44d061606b2a4bb9a2239 --seed 1 --events 100000 --clicks 25000 --label "V1 smoke measurement - not V2 or official"'
```

The idle preflight refuses unrelated running containers, samples `/proc/stat`, and requires at least 90% idle CPU, at most 1% steal, and a one-minute load average no greater than 2.0.
Run on an otherwise idle VM and do not start package updates or another benchmark while it is active.
The kernel lock at `/srv/shopping-bench/locks/run.lock` is authoritative; its file contents are informational.

Every attempt that reaches measurement writes `runs/<run-id>/result.json`, a one-row `result.csv`, stdout/stderr, and an append-only row in `results/results.csv`.
JSON is authoritative and uses schema version 1.
Failed fixture gates are stored under `results/fixtures-*.json` and suppress measurement.

## Progressive size gates

Allowed event tiers are 100K, 1M, 5M, 15M, 30M, and 60M, with clicks fixed at one quarter of events.
Advance one tier at a time only after correctness and resource review of the prior tier.
Stop immediately on fixture or self-check failure, nonzero exit, timeout, OOM, missing required telemetry, sampled combined disk above 45 GiB, memory at or above 22 GiB, or a projection above the 180-second limit.
Treat 20 GiB memory as a warning and preserve at least 15-30 seconds of runtime margin.
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
Certified datasets, checkouts, images, completed results, output, and logs are retained until the operator reviews them.

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

Verify the local JSON contains the expected full source SHA, generator SHA and binary checksum, image ID and revision label, dataset manifest checksum, fixture count, self-check, and parity labels before using the timing.

Confirm no benchmark container is running before stopping the VM:

```bash
"$GCLOUD" --configuration=buildathon compute ssh "amanoni@$VM" \
  --project="$PROJECT" --zone="$ZONE" --ssh-flag='-o BatchMode=yes' \
  --command='test -z "$(docker ps -q --filter label=org.shopping-bench.managed=true)"'
"$GCLOUD" --configuration=buildathon compute instances stop "$VM" \
  --project="$PROJECT" --zone="$ZONE"
```
