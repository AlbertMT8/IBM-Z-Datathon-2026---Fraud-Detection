# Stage 1/2 audit — producer and validator

Audit of `Albert/stage1_producer` and `Albert/stage2_validator` at commit
`91bb2bc` against the team's audit brief. Date: 2026-10-17.

**Contract used:** `Albert/New stuff/event.schema.json` (v1.0.0), not edited. Where
the brief and the contract disagree, the contract wins. So the latency clock is
`emitted_at` (RFC 3339 with ms), not `emit_ts`, and `producer_id` / `seq` /
the injected-fault tag are Redis stream fields, never JSON keys.

**Environment:** macOS arm64, Python 3.12.15, Redis 8.10.2 built from the
official tarball (sha256 checked). Docker isn't available on this machine, so
step 6 wasn't run (see "Still to verify on the VM").

## Summary

- **7 code defects** were found. Each was proven by a test that failed on the
  original code, then fixed, and each fix was checked by the full suite and the
  live runs.
- **5 gaps in the existing tests** were found by planting defects (mutation
  testing). The code there was right but nothing pinned it down; all are
  covered now.
- **Real data:** all 1,296,675 rows of `fraudTrain.csv` produce events that pass
  the contract (0 invalid), so only injected faults reach the DLQ.
- **Tests:** 58 → 70, all passing (`results/pytest_final.txt`).
- **Requirements:** every requirement in the brief passes after the fixes.
  Docker is the exception: it isn't verified, because Docker isn't installed here.

## Audit table (final status)

Line numbers refer to the fixed files.

| # | Item | Status | Evidence |
|---|---|---|---|
| P1 | Emits in non-decreasing `unix_time`; streams the CSV; `prepare` is an external sort | PASS | `producer.py:154-225` chunked sort + `heapq.merge`; `:364` streams `csv.DictReader`. Full fraudTrain: 15.7 s, peak RSS 448 MB at 200k-row chunks. Live: 0 decreases. Memory bounds now tested |
| P2 | `event_id` UUIDv4, `emitted_at`, `producer_id`, `seq`; exact contract fit | PASS | `:273` uuid4; `:451` producer_id/seq as stream fields (contract forbids extra keys). 1,296,675/1,296,675 real rows valid |
| P3 | `emitted_at` stamped right before the XADD, not when queued | **FAIL → FIXED** | D2. Now `:447`, inside `_flush` |
| P4 | Monotonic rate limit, no drift; `--rate/--burst/--max-events/--loop/--resume` | **FAIL → FIXED** | Token bucket `:102-138` fine. Live 1009.5/s at 1000 (+0.95%), 5008.7/s at 5000 (+0.17%). `--resume` was broken by D1 |
| P5 | Pipelined XADDs with a flush interval; partial batch never stalls | PASS | `_sleep` `:483-489` flushes while waiting for tokens |
| P6 | `--malformed-rate` rotates 4 kinds and tags them | PASS | `:62`, `:305`, tag `:453`. Live: 1,988 tagged = 1,988 in DLQ |
| P7 | SIGINT/SIGTERM: flush, state file, summary, exit 0 | PASS | `:429`, `:576`, `:646-647`. Live exits 0 |
| V1 | Group `validators` created idempotently (MKSTREAM, BUSYGROUP ignored) | PASS | `validator.py:368-373`. Re-raise of other errors now tested |
| V2 | Schema compiled once | PASS | `:331` |
| V3 | Valid payload forwarded byte-identical; metadata in separate fields | PASS | `:515-516`, bytes client `:808`. Live 1000/1000 identical |
| V4 | `event_id`, `emitted_at`, `seq` survive | PASS | `:271-272` passthrough; live check |
| V5 | DLQ: raw payload, event_id, reason, all errors with paths, dlq_ts | PASS* | `:400-401`. *A contract-valid leap-second timestamp was DLQ'd: D6, fixed |
| V6 | XACK only after a successful forward or DLQ write | PASS* | `:557`. *A write that kept failing locked up the loop: D7, fixed `:759` |
| V7 | XAUTOCLAIM recovers pending entries on startup | **FAIL → FIXED** | D5, `:628` |
| V8 | Bad JSON / exceptions → DLQ (MALFORMED_JSON / INTERNAL_ERROR), never crash | PASS | `:444-466`, `:526` |
| V9 | p50/p95 in pure Python; counters mirrored to `metrics:validator` | **FAIL → FIXED** | Reservoir + nearest rank `:91-127`. Hash wrong with more than one validator: D3, `:698` |
| V10 | Graceful shutdown finishes, acks, prints summary, exits 0 | PASS | Live: all validators exit 0, pending 0 |
| X1 | Reason codes cover every injected kind | PASS | missing_required→MISSING_REQUIRED, wrong_type→TYPE_ERROR, out_of_range_amount→RANGE_ERROR, bad_timestamp→SCHEMA_VIOLATION (generic: there is no format-specific code) |
| X2 | Stream names, field names, Redis URL handling match | PASS | `data`/`producer_id`/`seq`/`malformed` on both sides; both read `REDIS_URL`, same default (`producer.py:50`, `validator.py:63`) |
| X3 | Nothing reads `emitted_at` from a re-serialized copy | PASS | Validator parses the received bytes once (`:463`) and reads that dict (`:512`) |
| X4 | Two validators on one host don't share a pending list *(added)* | **FAIL → FIXED** | D4, `:732` |
| S1 | Only pure-Python packages | PASS | `pip install --dry-run --report --only-binary=:all: --platform manylinux2014_s390x --python-version 3.12 -r stage2_validator/requirements.txt` → redis 8.1.0, jsonschema 4.17.3, attrs 26.1.0, pyrsistent 0.20.0, **all `py3-none-any`**. No rpds-py (it arrives with jsonschema ≥ 4.18) |
| S2 | No byte-order assumptions | PASS | No `struct` / `array` / `ctypes` / `to_bytes` / `byteorder` |
| S3 | `python:3.12-slim`; READMEs say to build on the LinuxONE VM | PASS | Both Dockerfiles; producer README `:47`, validator README `:27` |
| S4 | Both images built and run end to end on a docker network | **NOT VERIFIED** | No Docker on this machine. Commands below |

## Defects, root causes, fixes

Every test named below lives in `tests/test_audit_regressions.py`. Each one
fails on commit `91bb2bc` and passes now.

**D1 — Producer state can claim rows that never reached Redis** (`--resume` loses data)
- **Cause:** redis-py clears a pipeline even when `execute()` raises. Then
  `run()`'s `finally` force-flushed the now-empty pipeline, which succeeds, and
  saved a state file whose `rows_consumed` included the failed batch.
- **Effect:** measured state said 29 rows sent; 20 were in Redis. `--resume`
  would skip the rest silently.
- **Fix:** counters are updated before an event is queued. A failed flush takes
  the unsent batch back out of the state (`seq`, `rows_consumed`, `emitted`,
  `malformed`) and then re-raises.
- **Test:** `test_checkpoint_after_a_failed_flush_never_claims_unsent_rows`

**D2 — `emitted_at` was stamped when the event was queued, not when it was sent**
- **Cause:** the stamp was taken in `build_event`, and the XADD left only at the
  next flush (batch full or 250 ms).
- **Effect:** live at 1000/s, events waited p50 53 ms / p95 97 ms / max 221 ms
  between stamp and arrival in Redis. That was most of the reported "latency"
  (p50 65 / p95 102 ms).
- **Fix:** events are queued as dicts, and `_flush` stamps `emitted_at` as it
  builds each XADD.
- **After:** p50 5 / p95 8 / max 10 ms. Emit→validate latency at 1000/s is now
  p50 17 / p95 23 ms.
- **Test:** `test_emitted_at_is_stamped_when_the_xadd_is_sent_not_when_queued`
  (stamp precedes the send by up to 304 ms before; ≤ 20 ms required)

**D3 — `metrics:validator` was wrong with more than one validator**
- **Cause:** each validator HSET its own process-local counters into the shared
  hash, so only the last writer's counts survived.
- **Effect:** live, the hash showed 5,197 validated out of 10,000.
- **Fix:** counters are added with HINCRBY (only the change since the last push).
  Rate and latency fields stay last-writer, plus a per-validator copy
  `<field>:<validator_id>`. The README documents this.
- **Tests:** `test_metrics_hash_counts_every_validator_in_the_group`,
  `test_metrics_push_is_idempotent_between_ticks`

**D4 — Default consumer name was the bare hostname**
- **Cause:** two validators on one host became one consumer with one pending
  list. A starting validator could "recover" its twin's in-flight entries and
  forward them twice.
- **Effect:** live, two validators registered as a single consumer name.
- **Fix:** the default is now `hostname-pid`. That's unique per process, and
  stable across restarts in a container, where the pid is 1.
- **Test:** `test_two_validators_on_one_host_get_distinct_consumer_names`
  (spawns two real validator processes)

**D5 — XAUTOCLAIM gave up after one empty scan window**
- **Cause:** Redis scans at most COUNT×10 pending entries per call. When none of
  those are idle, it returns 0 claimed with a non-zero cursor, and the code
  `break`s on "nothing claimed".
- **Effect:** live on Redis 8.10.2: 0 of 10 dead-consumer entries reclaimed.
  fakeredis doesn't model the scan limit, which is why the original tests
  couldn't see this.
- **Fix:** keep walking until the cursor returns to `0-0`.
- **Test:** `test_xautoclaim_keeps_scanning_past_a_window_with_nothing_idle`
  (uses a client that copies Redis's scan limit). Real-Redis check:
  `live_audit.py xautoclaim`

**D6 — Leap-second timestamps were rejected**
- **Cause:** RFC 3339 (the contract's `date-time`) allows `:60`. The format check
  and the latency parser use `datetime.fromisoformat`, which rejects it, so a
  contract-valid event was DLQ'd.
- **Fix:** `_parse_rfc3339` reads `:60` as `:59` plus one second.
- **Test:** `test_leap_second_emitted_at_is_valid_rfc3339_and_forwarded`
- **Impact:** low. The producer can't emit `:60`.

**D7 — One entry whose write kept failing locked up the validator**
- **Cause:** a failed forward set `_own_pending_dirty`. The loop went straight
  back to draining its own pending list, failed again, and repeated forever with
  no backoff, never reading new entries.
- **Effect:** measured 18,692 write attempts in 3 s, and a valid event that
  arrived later was never forwarded. Any persistent per-command error triggers
  it: WRONGTYPE on `tx.dlq`, or OOM under `maxmemory noeviction`.
- **Fix:** after a failed retry, wait `retry_backoff_s` before retrying the
  pending list, and keep reading new entries meanwhile.
- **Test:** `test_a_write_that_keeps_failing_neither_spins_nor_blocks_new_entries`

### Gaps in the existing tests

None of Albert's 58 tests asserted anything false. Five behaviours were simply
untested: breaking each left all 58 green (`results/mutations_original_suite.txt`).

| Behaviour that no test pinned down | New test |
|---|---|
| An entry whose own write failed is not acked | `test_a_write_that_keeps_failing_…` |
| Group-creation errors other than BUSYGROUP propagate | `test_group_creation_errors_other_than_busygroup_are_raised` |
| Token-bucket burst cap after a stall | `test_token_bucket_burst_is_capped_after_a_long_stall` |
| `prepare` memory bounded by chunk size, not file size | `test_prepare_memory_is_bounded_by_chunk_rows_not_file_size` |
| `run` streams the CSV instead of loading it | `test_run_streams_the_csv_instead_of_loading_it` |

All five planted defects fail the 70-test suite.

## Live results (real Redis 8.10.2, real `fraudTrain.csv` after `prepare`)

Full logs: `results/live_before_fixes.txt` and `results/live_after_fixes.txt`.

| Check | Before fixes | After fixes |
|---|---|---|
| 20k @ 1000/s, 10% faults: valid+dlq = 20000, 0 loss, 0 dups | PASS | PASS |
| DLQ event_ids = injected set | PASS (1,988) | PASS (1,988) |
| 1,000 sampled tx.valid payloads byte-identical to tx.raw | 1000/1000 | 1000/1000 |
| `unix_time` non-decreasing in seq order | PASS | PASS |
| Producer rate within ±5% of 1000/s | 1009.7/s | 1009.5/s |
| SIGKILL validator mid-run, restart with the same name | — | PASS: 0 loss, pending 0 |
| SIGKILL, restart with a new name (XAUTOCLAIM) | — | PASS: 0 loss, pending 0 |
| Two validators, happy path, no duplicate event_id | — | PASS (10,197 + 9,803) |
| Two validators with the **default** name | FAIL: one consumer | PASS: 2 consumers, 0 dups ×5 runs |
| `metrics:validator` equals stream totals | FAIL: 5,197 vs 10,000 | PASS: 10,000 |
| XAUTOCLAIM behind a busy window | FAIL: 0/10 | PASS: 10/10 |
| Producer buffering (Redis arrival − `emitted_at`) | p50 53 / p95 97 / max 221 ms | p50 5 / p95 8 / max 10 ms |

### Throughput and latency (after fixes)

| Measurement | Result |
|---|---|
| Sustained at `--rate 5000` | 100,000 events offered at 5,008.7/s; one validator kept up (pending 0, 0 loss) |
| Emit→validate latency at 5000/s | p50 5 ms, p95 8 ms (max 17) |
| Emit→validate latency at 1000/s | p50 17 ms, p95 23 ms |
| Validator ceiling (100k backlog drained) | 9,808 ev/s wall clock, one process |

Throughput is far above 1000 ev/s, so no profiling was needed.

Commands (run from `Albert/`, with `REDIS_URL=redis://localhost:6379/0` and
`LIVE_DIR` pointing at the folder holding `sorted.csv`):

```
python stage1_producer/producer.py prepare --input ../Datasets/fraudTrain.csv --output $LIVE_DIR/sorted.csv
python tests/live/live_check.py basic --rate 1000 --events 20000 --malformed 0.1
python tests/live/live_check.py kill-same --rate 1000 --events 20000 --malformed 0.1
python tests/live/live_check.py kill-new --rate 1000 --events 20000 --malformed 0.1 --min-idle-ms 3000
python tests/live/live_check.py two --rate 1000 --events 20000 --malformed 0.1
python tests/live/live_check.py basic --rate 5000 --events 100000 --malformed 0.0
python tests/live/live_check.py backlog --events 100000
python audit_stage12/live_audit.py emit-gap | metrics | two-default | xautoclaim | fullscan
```

**One residual result I'm reporting as measured.** My own `emit-gap` bar
(p95 ≤ 5 ms from stamp to Redis arrival) still reads 8 ms after the fix. It
isn't buffering: all 100 entries of a batch land in the same Redis millisecond,
and the last-stamped event still shows ~4 ms. Timing `execute()` inside the
paced producer gives 4.1 ms p50 for a 100-event batch, against 0.9 ms when the
process is busy. The remainder is the XADD round trip itself, on a macOS process
waking from sleep. The stamp now sits immediately before the XADD, which is
what the requirement asks. I didn't loosen the bar; recheck it on the VM.

## Still to verify on the s390x VM

1. **Docker (step 6).** Build both images and run them end to end. From the repo root:
   ```
   docker network create fraudnet
   docker run -d --name audit-redis --network fraudnet redis:8
   docker build -t fraud-producer  -f Albert/stage1_producer/Dockerfile  Albert
   docker build -t fraud-validator -f Albert/stage2_validator/Dockerfile Albert
   docker run -d --name val1 --network fraudnet -e REDIS_URL=redis://audit-redis:6379/0 fraud-validator
   docker run --rm --network fraudnet -v "$PWD/Datasets:/data" fraud-producer prepare --input /data/fraudTrain.csv --output /data/sorted.csv
   docker run --rm --network fraudnet -v "$PWD/Datasets:/data" -e REDIS_URL=redis://audit-redis:6379/0 \
     fraud-producer run --input /data/sorted.csv --rate 1000 --max-events 20000 --malformed-rate 0.1 --state-file /data/.producer_state.json
   docker exec audit-redis redis-cli XLEN tx.valid   # + XLEN tx.dlq should be 20000
   ```
2. **Wheels.** pip on s390x should pick the `py3-none-any` wheels (no compiler in
   the slim image). The `docker build` output should list no "Building wheel"
   lines, for pyrsistent especially.
3. **Numbers.** Rate accuracy, throughput and p50/p95 depend on the machine. Rerun
   `live_check.py basic` at 1000/s and 5000/s, plus `backlog`, and rerun
   `live_audit.py emit-gap` (the residual above).
4. **Memory.** `prepare` peaked at 448 MB with the default 200k-row chunks. If
   the VM is small, lower `--chunk-rows`.

## Risks noted, not changed (outside the brief, or by design)

- **Trimming.** `tx.raw` is trimmed at about 1,000,000 entries (`--maxlen`), and
  `tx.valid` / `tx.dlq` likewise. If validators fall behind by more than that,
  unvalidated entries are dropped silently: about 200 s of backlog at 5000/s.
- **No order guard in `run`.** It doesn't check the input is sorted. It trusts
  `prepare`.
- **Bad CSV rows.** One would stop the producer with an exception. The real
  dataset has none.
- **Duplicate JSON keys** are accepted, and the last one wins. That matches
  Python and Postgres, but other parsers keep the first.
- **Consumer clean-up.** With `hostname-pid` names, restarted bare-metal
  validators leave idle consumers in the group (`XGROUP DELCONSUMER`).
- **`stubgen.py --validate`** (outside stages 1/2) builds its validator without a
  format checker, so it doesn't enforce `uuid`, `date-time` or `date`.

## Files changed

- `stage1_producer/producer.py`: D1, D2
- `stage2_validator/validator.py`: D3–D7
- `stage1_producer/README.md`, `stage2_validator/README.md`: documented behaviour changes
- `tests/test_audit_regressions.py` (new): 12 tests
- `audit_stage12/` (new): this report, `live_audit.py`, `results/`
