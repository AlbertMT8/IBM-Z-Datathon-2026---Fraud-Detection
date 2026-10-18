# Stage 1 — Transaction Producer

Streams the Sparkov `fraudTest.csv` dataset into the Redis stream `tx.raw`
at a paced, configurable rate. Every event validates against
the team's v1.0.0 contract, [`Albert/New stuff/event.schema.json`](../New%20stuff/event.schema.json)
(shared with the downstream validator / DLQ / scorer).

All commands below run from the `Albert/` folder.

## Event shape (v1.0.0)

Each `tx.raw` entry has these stream fields:

| Field | Meaning |
|-------|---------|
| `data` | The event JSON — exactly the contract, nothing else |
| `producer_id` | `--producer-id` (stream metadata: the contract forbids extra payload keys) |
| `seq` | Monotonic per-producer sequence number |
| `malformed` | Only on deliberately injected bad events: the kind (see below) |

Row → event mapping (identical to `Dhruv/stage3_scoring/demo_stream.make_event`):

- `emitted_at` — RFC3339 UTC with ms, stamped as the batch's `XADD`s are sent, not when
  the event is queued (latency clock start)
- `transaction.occurred_at` — `trans_date_trans_time` kept as-is with `Z` appended
  (hour-of-day preserved); `transaction.unix_time` is **derived** from it, not
  copied from the CSV
- `transaction.card_id` — `sha256(cc_num digits).hexdigest()[:16]`; `card_last4` for display
- `cardholder.zip` — zero-padded to 5 (New England ZIPs lose their leading zero in the CSV)
- `first`, `last`, `street` and the raw `cc_num` are **dropped** and never enter the pipeline
- `label.is_fraud` — ground truth, which the scorer must never read
- `run_id` — only when `--run-id` / `$RUN_ID` is set

## Platform

- **Target:** IBM LinuxONE, Linux on IBM Z (s390x, big-endian).
- **Dependencies:** `redis`, `jsonschema`. Pure-Python wheels / sdists only.
  **No** pandas / numpy / pyarrow — their s390x wheels are unreliable and
  compiling from source can cost hours.
- **Python:** 3.11+.
- **`jsonschema` is pinned to 4.17.3.** Every release from 4.18 on depends
  on `referencing` → `rpds-py`, a compiled Rust extension. 4.17.3 depends
  only on `attrs` and `pyrsistent`, both of which resolve to pure-Python
  `py3-none-any` wheels on s390x. Do not bump it without re-checking with
  `pip download --only-binary=:all: --platform manylinux2014_s390x --python-version 3.12 -r requirements.txt`.

> ⚠ **Build the Docker image on the LinuxONE VM itself.** Building on an
> x86 laptop produces an x86 image that will fail to run on the s390x host.
> Use `docker build --platform=linux/s390x` only if you have cross-build
> binfmt wired up; the simpler, less flaky path is to build on the VM.

## Install (bare metal)

```sh
python3.12 -m venv .venv
. .venv/bin/activate
pip install -r producer/requirements.txt
```

## Prepare the sorted CSV (one-time)

**Replay `fraudTest.csv`, never `fraudTrain.csv`.** The stage 3 scorer trained on fraudTrain and loads it as
card history at start-up, so replaying it would score data the models already saw and count every payment twice.

The raw file is not guaranteed sorted. External-merge-sort by `trans_date_trans_time`
into `data/sorted.csv` so the producer can stream row-by-row:

```sh
python stage1_producer/producer.py prepare \
    --input data/fraudTest.csv \
    --output data/sorted.csv
```

Memory is bounded by `--chunk-rows` (default 200 000). Expect a few minutes
for ~1.3 M rows.

## Run against local Redis

```sh
# Default REDIS_URL is redis://localhost:6379/0.
docker run -d --name redis -p 6379:6379 redis:7

python stage1_producer/producer.py run \
    --input data/sorted.csv \
    --rate 1000 --burst 2000 \
    --stream tx.raw \
    --maxlen 1000000 \
    --batch-size 200 --flush-interval 0.1 \
    --producer-id producer-1
```

**Run at 1000 events/sec (the one-liner the grader asked for):**

```sh
REDIS_URL=redis://localhost:6379/0 python stage1_producer/producer.py run \
    --input data/sorted.csv --rate 1000 --burst 2000 \
    --batch-size 200 --flush-interval 0.1
```

## Options

| Flag | Default | Purpose |
|------|---------|---------|
| `--rate` | required | Target events/sec (token-bucket paced, monotonic clock) |
| `--burst` | 200 | Token bucket capacity — short-term burst allowance |
| `--max-events` | ∞ | Stop after N events (handy for benchmarking) |
| `--loop` | off | Wrap back to row 0 when the file ends; new UUIDs + new `emitted_at` |
| `--malformed-rate` | 0 | Fraction (0–1) of events to deliberately break for DLQ demo |
| `--stream` | `tx.raw` | Target stream |
| `--maxlen` | 1 000 000 | `XADD MAXLEN ~` cap |
| `--batch-size` | 100 | `XADD`s per pipeline flush |
| `--flush-interval` | 0.25 s | Max time before a partial batch is flushed |
| `--producer-id` | `producer-1` | Stamped on every entry as the `producer_id` stream field |
| `--run-id` | unset (`$RUN_ID`) | Load-test tag carried in every event's `run_id` (per-run Postgres views) |
| `--resume` | off | Resume from `.producer_state.json` (seq + row offset) |
| `--state-file` | `.producer_state.json` | Checkpoint path |

## Resume

The producer checkpoints its sequence number and row offset every 1000 rows
and on shutdown. To resume:

```sh
python stage1_producer/producer.py run \
    --input data/sorted.csv --rate 1000 --resume
```

## Malformed injection (DLQ demo)

```sh
python stage1_producer/producer.py run \
    --input data/sorted.csv --rate 500 --malformed-rate 0.1
```

Injected events are tagged with a `malformed` **stream field** (a sibling
of `data`) naming the kind (`missing_required`, `wrong_type`,
`out_of_range_amount`, `bad_timestamp`), so the DLQ can be checked against
them exactly. The JSON payload itself carries no tag — it is broken in
exactly one way, and nothing else.

## Verify

```sh
# From another shell, tail the stream.
docker exec -it redis redis-cli XREAD COUNT 5 STREAMS tx.raw 0

# Length and MAXLEN behavior.
docker exec -it redis redis-cli XLEN tx.raw

# Run the test suite (test-only deps: tests/requirements.txt):
pip install -r tests/requirements.txt
pytest -q tests/

# Live end-to-end checks against a real Redis (loss, DLQ == injected,
# byte-identity, ordering, rate, SIGKILL recovery, two validators):
python tests/live/gen_sparkov.py live/fraudTest.csv 60000    # or use the real CSV
python stage1_producer/producer.py prepare --input live/fraudTest.csv --output live/sorted.csv
LIVE_DIR=live python tests/live/live_check.py basic          # kill-same | kill-new | two | backlog
```

## Shutdown

`SIGINT` / `SIGTERM` triggers an orderly shutdown: the current Redis
pipeline flushes, state is checkpointed, the final summary is printed, and
the process exits 0.

## Docker

```sh
# On the LinuxONE VM:
docker build -t fraud-producer:latest -f stage1_producer/Dockerfile .
docker network create fraud-net
docker run -d --name redis --network fraud-net redis:8
docker run --rm -e REDIS_URL=redis://redis:6379/0 \
    --network fraud-net \
    -v "$PWD/data:/app/data" \
    fraud-producer:latest \
    run --input /app/data/sorted.csv --rate 1000 --burst 2000
```
