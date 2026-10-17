# Stage 2 — Schema Validator / DLQ

All commands below run from the `Albert/` folder.

Consumes raw transaction events from the Redis stream `tx.raw` (produced
by Stage 1), validates each payload against
the team's v1.0.0 contract [`Albert/New stuff/event.schema.json`](../New%20stuff/event.schema.json)
(including its `uuid` / `date-time` / `date` formats, which jsonschema only
checks when told to), forwards valid events byte-identical to
`tx.valid`, and routes malformed / schema-violating events to `tx.dlq`
with a structured reason code so Stage 5 (dashboard) can chart why we
dropped them.

## Platform

- **Target:** IBM LinuxONE, Linux on IBM Z (s390x, big-endian).
- **Dependencies:** `redis`, `jsonschema`. Pure-Python only.
  **No** pandas / numpy / pydantic-core / pyarrow — their s390x wheels
  do not install cleanly and we don't have the hours to compile.
- **Python:** 3.11+.
- **`jsonschema` is pinned to 4.17.3.** Every release from 4.18 on depends
  on `referencing` → `rpds-py`, a compiled Rust extension. 4.17.3 depends
  only on `attrs` and `pyrsistent`, both of which resolve to pure-Python
  `py3-none-any` wheels on s390x. Do not bump it without re-checking with
  `pip download --only-binary=:all: --platform manylinux2014_s390x --python-version 3.12 -r requirements.txt`.

> ⚠ **Build the Docker image on the LinuxONE VM itself.** Building on an
> x86 laptop produces an x86 image that will fail to run on the s390x host.

## How events flow

```
producer ──XADD──▶ tx.raw ──XREADGROUP──▶ validator ──XADD──▶ tx.valid ──▶ scorer
                                           │
                                           └──XADD──▶ tx.dlq ──▶ dashboard
```

- Consumer group: `validators` (idempotent `XGROUP CREATE ... MKSTREAM`).
- Consumer name: `--consumer-id`, defaults to `$(hostname)`.
- Processing is **at-least-once** — `XACK` happens only after the event
  has been successfully XADDed to `tx.valid` or `tx.dlq`. The scorer
  (Stage 3) must therefore be idempotent on `event_id`.
- On startup, and then every `--min-idle-ms / 2`, `XAUTOCLAIM` reclaims
  pending entries older than `--min-idle-ms` from dead consumers — so a
  validator restarted under a new name (e.g. a new container hostname)
  still recovers a crashed consumer's in-flight entries.
- Each batch is forwarded with one pipelined round trip of `XADD`s and
  acked with one multi-ID `XACK` covering exactly the entries whose `XADD`
  succeeded. A Redis I/O error leaves the entries pending and is retried
  after a back-off; it is never dead-lettered.
- A poison event that triggers an unexpected exception goes to the DLQ
  with reason `INTERNAL_ERROR` — the loop never stalls.

## Install (bare metal)

```sh
python3.12 -m venv .venv
. .venv/bin/activate
pip install -r validator/requirements.txt
```

## Run against local Redis

```sh
# Start Redis if you don't have one running.
docker run -d --name redis -p 6379:6379 redis:7

# Run the producer in one shell (see producer/README.md for prep).
REDIS_URL=redis://localhost:6379/0 python stage1_producer/producer.py run \
    --input data/sorted.csv --rate 1000 --burst 2000 \
    --malformed-rate 0.1 --max-events 5000 \
    --batch-size 200 --flush-interval 0.1

# Run the validator in another shell.
REDIS_URL=redis://localhost:6379/0 python stage2_validator/validator.py \
    --consumer-id val-1 --block-ms 1000 --count 500 \
    --min-idle-ms 30000
```

## Options

| Flag | Default | Purpose |
|------|---------|---------|
| `--raw-stream` | `tx.raw` | Input stream |
| `--valid-stream` | `tx.valid` | Output stream for valid events (byte-identical payload) |
| `--dlq-stream` | `tx.dlq` | Output stream for invalid / malformed events |
| `--group` | `validators` | Consumer group name |
| `--consumer-id` | `$(hostname)` | Consumer name within the group |
| `--block-ms` | 1000 | `XREADGROUP BLOCK` in ms |
| `--count` | 500 | `XREADGROUP COUNT` batch size |
| `--min-idle-ms` | 30000 | `XAUTOCLAIM` idle threshold for recovery |
| `--max-events` | ∞ | Stop after N validated events (test runs) |
| `--once` | off | Drain pending, then exit (test runs) |
| `--log-interval` | 5 s | Rolling counter log cadence |
| `--metrics-interval` | 1 s | Hash-mirror cadence for the dashboard |
| `--valid-maxlen` | 1 000 000 | `XADD MAXLEN ~` on `tx.valid` |
| `--dlq-maxlen` | 1 000 000 | `XADD MAXLEN ~` on `tx.dlq` |
| `--metrics-key` | `metrics:validator` | Redis hash key for live metrics |

## DLQ record shape

Each entry on `tx.dlq` has these stream fields:

| Field | Meaning |
|-------|---------|
| `data` | **Raw payload, unmodified**, exactly as received from `tx.raw` |
| `reason` | One of `SCHEMA_VIOLATION`, `MALFORMED_JSON`, `MISSING_REQUIRED`, `TYPE_ERROR`, `RANGE_ERROR`, `INTERNAL_ERROR` |
| `event_id` | Present only if the payload parsed and contained one |
| `path` | JSON path to the failing field (`/` if unparseable) |
| `message` | Human-readable summary of all validation errors, concatenated (capped at 1000 chars). |
| `errors` | JSON array of **every** error: `[{"path": "/amt", "keyword": "type", "message": "..."}]`, uncapped in count |
| `dlq_ts` | Epoch ms when the DLQ record was written |
| `validator_id` | Which consumer DLQ'd it |
| `stage` | Always `validation` |
| `run_id` | The event's `run_id`, when it parsed and had one |
| `producer_id`, `seq`, `malformed` | Copied from the `tx.raw` entry when present (`malformed` = the injected kind) |

These map onto the `dead_letter_events` table in `Albert/New stuff/schema.sql`:
`reason` → `reason_code`, `message` → `reason_detail`, `data` → `raw_payload`,
`dlq_ts` → `rejected_at`, plus `event_id`, `run_id` and `stage` as-is.

## Valid record shape (`tx.valid`)

The producer's JSON payload passes through byte-identical in the `data`
field — the validator never mutates, reorders, or enriches it, so
`event_id` and `emitted_at` survive untouched (end-to-end latency is
measured from `emitted_at`), and the producer's `producer_id` / `seq` stream
fields are carried alongside.

| Field | Meaning |
|-------|---------|
| `data` | Byte-identical JSON from `tx.raw` |
| `validate_ts` | Epoch ms when validation finished |
| `validator_id` | Which consumer forwarded it |
| `producer_id`, `seq` | Copied from the `tx.raw` entry |

Stage 3 (`Dhruv/stage3_scoring/run.py`) reads one event JSON per line on
stdin; feed it the `data` field of each `tx.valid` entry, in stream order.

## Inspect

```sh
# How many valid events?
docker exec -it redis redis-cli XLEN tx.valid

# How many DLQ'd events?
docker exec -it redis redis-cli XLEN tx.dlq

# Peek at the first 5 valid events (full payload is in the `data` field).
docker exec -it redis redis-cli XRANGE tx.valid - + COUNT 5

# Peek at the first 5 DLQ records with reasons.
docker exec -it redis redis-cli XRANGE tx.dlq - + COUNT 5

# Count DLQ by reason (quick-and-dirty).
docker exec -it redis redis-cli --no-raw XRANGE tx.dlq - + \
  | grep -A1 '"reason"'

# Live validator metrics (for the dashboard).
docker exec -it redis redis-cli HGETALL metrics:validator

# Pending entries (anything the consumer group has delivered but not XACKed).
docker exec -it redis redis-cli XPENDING tx.raw validators

# Show consumers in the group.
docker exec -it redis redis-cli XINFO CONSUMERS tx.raw validators
```

## Tests

```sh
# Install test deps once:
pip install -r tests/requirements.txt

# Run the full suite:
pytest -q tests/
```

The suite proves against a real stream (fakeredis + a real producer):

- schema-passing events land on `tx.valid` byte-identical to what the
  producer XADDed, with `event_id` / `emitted_at` unchanged and `seq` carried;
- each malformed kind the producer injects (`missing_required`,
  `wrong_type`, `out_of_range_amount`, `bad_timestamp`) lands on
  `tx.dlq` with the correct reason code and the raw payload intact;
- the real producer with `--malformed-rate 0.1 --max-events 1000`
  produces a DLQ set that is exactly the events tagged with the
  `malformed` stream field;
- unparseable JSON is DLQ'd, never raises;
- an entry is XACKed only after it reaches `tx.valid` / `tx.dlq`;
  killing mid-flight and running the validator again with XAUTOCLAIM
  recovers it with no loss;
- two validators in the same group split the stream, no event is
  processed twice on the happy path;
- throughput ≥ 1000 events/sec (reported as a number in the output).

## Shutdown

`SIGINT` / `SIGTERM` triggers an orderly shutdown: the current read
loop stops after acking the in-flight batch, the final summary is
printed, metrics are pushed one last time, and the process exits 0.

## Docker

```sh
# On the LinuxONE VM:
docker build -t fraud-validator:latest -f stage2_validator/Dockerfile .
# Same network as Redis (see producer/README.md), so redis:6379 resolves.
docker run --rm -e REDIS_URL=redis://redis:6379/0 \
    --network fraud-net fraud-validator:latest \
    --consumer-id val-1 --block-ms 1000 --count 500
```
