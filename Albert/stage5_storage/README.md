# Stage 5 — Storage (Postgres writer)

One small program, `storer.py`, run as two containers from one image:

| Mode | Reads | Group | Default consumer | Writes |
| --- | --- | --- | --- | --- |
| `storer --mode explained` | `tx.explained` | `storers` | `storer-1` | `scored_transactions` |
| `storer --mode dlq` | `tx.dlq` | `dlq-storers` | `dlq-storer-1` | `dead_letter_events` |

Table and view shapes come from [`Albert/New stuff/schema.sql`](../New%20stuff/schema.sql); the dashboard
(`Albert/dashboard/`) reads them. All commands below run from the `Albert/` folder.

## What it does, exactly

**Consumer groups.** Both groups are created with `XGROUP CREATE … 0 MKSTREAM` (so nothing written before the
storer first started is missed; `BUSYGROUP` is ignored). The consumer name is fixed (`--consumer`), so a
restarted storer is the same consumer. On start it:
1. processes **its own pending entries** first (`XREADGROUP … 0`, walked to the end of its pending list), then
2. reads new entries (`XREADGROUP … >`, up to **500** per call, `BLOCK 200 ms`), and
3. every 15 s runs `XAUTOCLAIM` for entries **idle > 30 s** left by a consumer that died.

**At-least-once, ack after commit.** Each batch is inserted in **one statement inside one transaction**, then
`COMMIT`, and only then `XACK`ed. If Postgres is down, nothing is acked: the batch stays in the pending list and is
retried after a back-off. If the process dies between COMMIT and XACK, the batch is replayed on restart and the
repeat is harmless (below).

**Explained rows.** The `data` field is parsed and mapped with **`to_row()` imported from
`Albert/New stuff/stubgen.py`** (loaded by file path, so the folder keeps its space; the image copies it to the same
relative path). Inserted with `ON CONFLICT (event_id) DO NOTHING`, so a repeated `event_id` (an upstream
at-least-once repeat or a replay) is counted in `duplicates_ignored` and never stored twice.

**The clocks.** `committed_at` is never sent: the column default `clock_timestamp()` stamps it on the database clock,
and `latency_ms` is a generated column (`committed_at − emitted_at`), never written. `clock_timestamp()` rather than
`now()` because inside one batch transaction `now()` is frozen at the transaction's start, while `clock_timestamp()`
is read for each row as it is inserted, i.e. a few ms before the COMMIT that makes the batch visible. That's the
closest per-row commit time Postgres exposes (tested: `test_pg_clock_timestamp_is_per_row_now_is_per_transaction`).

**DLQ rows.** `reason → reason_code`, `message → reason_detail`, `data → raw_payload`, `dlq_ts → rejected_at`, plus
`stage` and `event_id`:
- `raw_payload` is `TEXT` in schema.sql, so invalid JSON is stored as-is. Bytes that aren't UTF-8 are kept as
  backslash escapes, and NUL (never allowed in `TEXT`) is written as `\x00`.
- `event_id` is stored only if it is a UUID; otherwise `NULL` (the column allows it).
- `run_id` comes from the `run_id` stream field (stage 2 adds it), else from the payload's `run_id` if the payload
  parses (stages 3 and 4 send no field), else the column's own default `'default'`. The column is
  `NOT NULL DEFAULT 'default'`, so `NULL` is not possible.
- A missing `stage` gets the column default `validation`; a missing `dlq_ts` gets `clock_timestamp()`; a missing
  `reason` is stored as `UNKNOWN`.
- **Duplicates.** `dead_letter_events` has no unique key and schema.sql is a frozen contract, so stream-entry dedupe
  is done in the storer: the last **500,000** `tx.dlq` entry IDs are remembered in memory. After a restart, entries
  replayed from the pending list are instead inserted only if no identical row (`rejected_at`, `stage`, `reason_code`,
  `raw_payload`) exists. *Limitation:* two different stream entries that are identical in all four fields would be
  stored once after a crash between COMMIT and XACK.

**Bad rows never fail a batch.** If Postgres rejects the batch's data (e.g. a CHECK constraint), the batch is retried
row by row under savepoints; a rejected row goes to `tx.dlq` with `stage: storage`, reason `DB_REJECTED`. A
`tx.explained` entry that can't be mapped at all goes there with reason `MALFORMED_SCORED_EVENT`.

**Metrics.** Redis hash `metrics:storer`, like `metrics:scorer`: `stored`, `duplicates_ignored`, `rejected`, `batches`,
`rows_per_sec` (last 10 s), `consumer`, `updated_ms` from the explained storer, and `dlq_stored`,
`dlq_duplicates_ignored`, `dlq_batches`, `dlq_rows_per_sec`, `dlq_consumer` from the DLQ storer (separate names so
the two never overwrite each other). Counters are per process since it started.

**Shutdown.** `SIGTERM`/`SIGINT` finish the current batch, commit, ack, print a summary and exit 0.

## Configuration (environment only)

| Variable | Meaning | Image default |
| --- | --- | --- |
| `REDIS_URL` | Redis | `redis://redis:6379/0` |
| `PGHOST`, `PGPORT`, `PGDATABASE` | Postgres | `postgres`, `5432`, `fraud` |
| `PGUSER`, `PGPASSWORD` | Postgres login | none: put them in `~/fraud/.env` |

Copy [`.env.example`](../../.env.example) to `.env` (git-ignored) and set real passwords. Never commit `.env`.

## Dependencies (no compiled PyPI wheels)

Image: `debian:trixie-slim` + Debian's `python3` 3.13.5, `python3-psycopg2` 2.9.10 and `python3-redis` 6.1.0,
which Debian builds for s390x (same approach as Dhruv's stage 3). No pip in the image. Check with:
`docker run --rm --entrypoint dpkg-query fraud-storer -W python3 python3-psycopg2 python3-redis libpq5`.
Postgres server: the official `postgres:17.11-trixie` image (Docker Hub lists a `linux/s390x` build for this tag).

## Build and run

On the LinuxONE VM, build **on the VM** and use [`Albert/linuxone/deploy5.sh`](../linuxone/deploy5.sh) (Postgres with
the `fraud-pg` volume, schema applied on first start, storer, dlq-storer, dashboard). By hand:

```bash
docker build -t fraud-storer:latest -f stage5_storage/Dockerfile .
docker run -d --name storer     --network fraud-net --env-file ~/fraud/.env fraud-storer:latest --mode explained
docker run -d --name dlq-storer --network fraud-net --env-file ~/fraud/.env fraud-storer:latest --mode dlq
```

Locally (Docker Desktop) the same commands work; Postgres:

```bash
docker volume create fraud-pg
docker run -d --name postgres --network fraud-net --env-file .env -v fraud-pg:/var/lib/postgresql/data \
  -v "$PWD/New stuff/schema.sql:/docker-entrypoint-initdb.d/schema.sql:ro" postgres:17.11-trixie
```

## Tests

`tests/test_storer.py`: mapping via `to_row`, `ON CONFLICT` dedupe, ack only after commit (DB outage: nothing acked,
batch retried), malformed DLQ payloads, own-pending recovery, `XAUTOCLAIM` of a dead consumer, DLQ dedupe across a
restart, bad-row isolation, metrics and shutdown. The `pg` tests need a real Postgres and are skipped without one.

```bash
# unit tests only (fakeredis), any Python >= 3.10:
python3 -m pytest -q stage5_storage/tests
# everything, inside the real image, against Postgres 17:
docker build -t fraud-storer:latest      -f stage5_storage/Dockerfile .
docker build -t fraud-storer-test:latest -f stage5_storage/Dockerfile.test .
docker network create s5-test
docker run -d --name s5-pg --network s5-test -e POSTGRES_DB=fraud -e POSTGRES_USER=fraud -e POSTGRES_PASSWORD=test \
  -v "$PWD/New stuff/schema.sql:/docker-entrypoint-initdb.d/schema.sql:ro" postgres:17.11-trixie
docker run --rm --network s5-test -e PGHOST=s5-pg -e PGUSER=fraud -e PGPASSWORD=test -e PGDATABASE=fraud fraud-storer-test
```
