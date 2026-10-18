# ZSCORE — IBM Z Datathon 2026 (Real-Time AI for Critical Decisions)

Most card fraud is caught after settlement: the money is already gone, and cardholders with little cushion wait weeks
for provisional credit. **ZSCORE scores every card payment the moment it arrives**, on IBM LinuxONE. A stream of
transactions is schema-checked, scored by a three-model ensemble, explained in one plain sentence an analyst can act
on, and committed to PostgreSQL, where a live dashboard shows alerts, end-to-end latency, throughput and every
rejected event. The point is a measured system rather than a classifier: what it does under load, that it never
loses or double-counts a payment, and that broken input goes to a dead-letter queue instead of crashing anything.

## Architecture

```
  fraudTest.csv
       │
  ┌────▼──────────┐ tx.raw  ┌───────────────┐ tx.valid ┌──────────────┐ tx.scored ┌──────────────┐
  │ 1  producer   ├────────►│ 2  validator  ├─────────►│ 3  scorer    ├──────────►│ 4  explainer │
  └───────────────┘         │    (x2)       │          │  LR + RF +   │           │  one-sentence│
                            └───────┬───────┘          │  XGBoost     │           │  reason      │
                                    │                  └──────┬───────┘           └──────┬───────┘
                         broken     │                         │                          │ tx.explained
                         events     ▼                         ▼                          ▼
                              ┌──────────────── tx.dlq ◄──────┴──────┐           ┌──────────────┐
                              │  (dead-letter queue, every stage)    └───────────┤ 5  storer    │
                              ▼                                                  └──────┬───────┘
                       ┌──────────────┐                                                 │
                       │ 5 dlq-storer │                                                 │
                       └──────┬───────┘                                                 │
                              ▼                                                         ▼
                     ┌─────────────────────────────────────────────────────────────────────────┐
                     │ PostgreSQL 17: dead_letter_events · scored_transactions · SQL views     │
                     └────────────────────────────────────┬────────────────────────────────────┘
                                                          ▼
                                     dashboard (loopback :8080, via SSH tunnel)

  Every arrow is a Redis stream read by a consumer group: write the output, then XACK the input (at-least-once).
```

| Stream | Written by | Read by (group) |
| --- | --- | --- |
| `tx.raw` | stage 1 producer | stage 2 validators (`validators`) |
| `tx.valid` | stage 2 | stage 3 scorer (`scorers`) |
| `tx.scored` | stage 3 | stage 4 explainer (`explainers`) |
| `tx.explained` | stage 4 | stage 5 storer (`storers`) |
| `tx.dlq` | stages 2, 3, 4, 5 | stage 5 dlq-storer (`dlq-storers`) |

Field-level detail per stream: [`Albert/New stuff/README.md`](Albert/New%20stuff/README.md). Data contracts (event
JSON schema, scored-event schema, Postgres tables and views):
[`Albert/New stuff/`](Albert/New%20stuff/).

## How each stage works

1. **Producer** ([`Albert/stage1_producer/`](Albert/stage1_producer/README.md)). Replays the Sparkov
   `fraudTest.csv` in true time order (external merge sort, then streaming) at a set rate with a monotonic token
   bucket. Each payment becomes a contract event with a fresh UUID and an `emitted_at` stamp taken when its batch is
   sent. The card number is hashed and names and street are dropped. `--malformed-rate` injects broken events,
   tagged in a separate stream field.
2. **Validator** ([`Albert/stage2_validator/`](Albert/stage2_validator/README.md)). Checks every event against
   the JSON schema, formats included. Valid events go to `tx.valid` byte-for-byte; anything else goes to `tx.dlq`
   with a reason code and every error with its JSON path. Batched round trips, a Redis error is retried rather than
   dead-lettered, and dead consumers' work is reclaimed. Two run in parallel.
3. **Scorer** ([`Dhruv/stage3_scoring/`](Dhruv/stage3_scoring/README.md)). 27 features per payment, including
   per-card history kept in memory and warmed up from `fraudTrain.csv`. Ensemble 0.1·logistic regression +
   0.1·random forest + 0.8·XGBoost, scored in batches. Decision ALLOW / REVIEW / BLOCK, plus the top 3 reasons
   (XGBoost feature contributions) for alerts. Skips repeated `event_id`s.
4. **Explainer** ([`Dhruv/stage4_explanation/`](Dhruv/stage4_explanation/README.md)). Turns each alert's reasons
   into one sentence. The default `template` backend is instant. A local IBM Granite backend (llama.cpp) and a
   watsonx.ai backend exist behind the same interface, with a timeout and a check that the sentence invents no
   numbers.
5. **Storage** ([`Albert/stage5_storage/`](Albert/stage5_storage/README.md)). Two consumers of one program:
   `tx.explained → scored_transactions` (mapped with the contract's own `to_row()`, `ON CONFLICT (event_id) DO
   NOTHING`) and `tx.dlq → dead_letter_events`. Batches of 500, committed and then acked. `committed_at` comes
   from the database clock, so `latency_ms` (emit → commit) is computed by Postgres.
6. **Dashboard** ([`Albert/dashboard/`](Albert/dashboard/README.md)). Standard-library HTTP server and a
   one-page UI polling every second. Shows the alert feed with explanations, p50/p95/p99 latency, rows per second,
   decisions, DLQ by stage and reason, and every stage's live Redis metrics.

Every image is built on the target machine. All Python dependencies are pure Python (stages 1, 2, 4) or Debian's own
s390x packages (stages 3, 5, dashboard); nothing is compiled from PyPI.

## Run it

**On LinuxONE** (all images built on the VM, s390x):
1. Stages 1–4: [`Dhruv/README.md`](Dhruv/README.md) (network `fraud-net`, Redis, validators, scorer with models
   trained on the VM, explainer).
2. Stage 5 + dashboard: [`Albert/linuxone/deploy5.sh`](Albert/linuxone/deploy5.sh) (reads secrets from the
   git-ignored `~/fraud/.env`; see [`.env.example`](.env.example)).
3. A measured run: [`Albert/linuxone/demo5.sh RATE EVENTS`](Albert/linuxone/demo5.sh) (e.g. `1000 60000`). It
   wraps [`Dhruv/linuxone/demo.sh`](Dhruv/linuxone/demo.sh) and adds the Postgres-inclusive numbers from
   [`measure_db.py`](Albert/linuxone/measure_db.py).
4. Copy-paste steps: [`Albert/linuxone/VM_RUNBOOK.md`](Albert/linuxone/VM_RUNBOOK.md). Dashboard via
   `ssh -i "$L1CC_KEY" -L 8080:127.0.0.1:8080 linux1@"$L1CC_HOST"`, then http://localhost:8080.

**Locally** (Docker): put `fraudTrain.csv` and `fraudTest.csv` in `Datasets/` (git-ignored), run the producer's
`prepare` on fraudTest (output `Datasets/sorted_test.csv`), then the same commands as on the VM. The scorer can use
the Mac-trained models in `Dhruv/stage3_scoring/models/`. Tests: `pytest Albert/tests`,
`pytest Albert/stage5_storage/tests` (and the in-image Postgres run in the stage 5 README).

## Results

Measured on a LinuxONE Community Cloud VM (3 cores, 8 GB; `uname -m` = `s390x`), replaying `fraudTest.csv` with 1%
deliberately broken events. Details: [`Dhruv/LINUXONE_RESULTS.md`](Dhruv/LINUXONE_RESULTS.md).

| Measured to | Target rate | Events | Lost / written twice | Sustained | p50 | p95 | p99 | Broken → DLQ |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| stage 4 output | 1,000/s | 60,000 | 0 / 0 | 1,024/s | 48 ms | 68 ms | 456 ms | 595 / 595 |
| stage 4 output | 3,000/s | 90,000 | 0 / 0 | 3,175/s | 81 ms | 1,378 ms | 1,557 ms | 882 / 882 |
| stage 4 output | 5,000/s (2 validators) | 150,000 | 0 / 0 | 4,944/s | 1,746 ms | 2,220 ms | 2,496 ms | 1,487 / 1,487 |
| **Postgres commit (stage 5 included)** | 1,000/s and 5,000/s | — | — | — | — | — | — | **pending VM run** |

"p50/p95/p99" = latency from the producer's `emitted_at` to the stage named in the first column. About
3,000–5,000 events/s is where this 3-core VM saturates: above that, events queue but none are lost.

**Model quality** (models trained on the VM, graded once on fraudTest: 555,719 payments, 2,145 fraud): ensemble
PR-AUC **0.960**, precision **0.950**, recall **0.902** at the alert cut-off. Live on the stream at 1,000/s: 90% of
fraud caught, 93% of alerts real.

Stage 5 was verified locally (Docker Desktop, arm64, synthetic Sparkov-format data, Mac-trained models) for
correctness, not speed: 19,799 of 19,799 valid events stored, 0 lost, 0 duplicate rows; DLQ 206 injected = 206 in
`tx.dlq` = 206 stored; every alert row has its explanation and reasons. Also no loss after a SIGKILL of the storer,
and an unchanged row count when the same events were replayed again.

## Honest limits

- The explainer runs the **template** backend on LinuxONE. The local IBM Granite model was measured **on a Mac
  only** (about 2–5 s per sentence). The watsonx.ai backend is written but **was never run** (no access). So no
  generative explanation has run on LinuxONE.
- **Card history is lost when the scorer restarts** (it lives in one process's memory and is rebuilt from the
  training file). Fine for a demo, and why exactly one scorer runs; the production fix is keeping it in Redis.
- **Single VM.** Every container shares 3 cores; there is no replication or failover, and Redis runs without
  persistence.
- The data is **synthetic** (Sparkov). Fraud patterns are cleaner than real life, so accuracy is higher than it
  would be on real cards.
- `dead_letter_events` has no unique key (the schema is a frozen contract). The DLQ storer dedupes by stream entry
  in memory, and after a crash by row content (see the stage 5 README).
- The stage-5-inclusive LinuxONE numbers are not measured yet (row above).

## Open source and data

Data:
- Sparkov credit card transactions dataset (Kaggle, kartik2112): https://www.kaggle.com/datasets/kartik2112/fraud-detection
- Its generator, Sparkov Data Generation: https://github.com/namebrandon/Sparkov_Data_Generation

Software:

| Used for | Project |
| --- | --- |
| Models | scikit-learn https://scikit-learn.org · XGBoost https://xgboost.ai · pandas https://pandas.pydata.org · NumPy https://numpy.org · joblib https://joblib.readthedocs.io |
| Streams | Redis https://redis.io (image https://hub.docker.com/_/redis) · redis-py https://github.com/redis/redis-py |
| Contracts | jsonschema https://github.com/python-jsonschema/jsonschema (with attrs https://www.attrs.org and pyrsistent https://github.com/tobgu/pyrsistent) |
| Storage | PostgreSQL https://www.postgresql.org (image https://hub.docker.com/_/postgres) · psycopg2 https://www.psycopg.org |
| Explanations | llama.cpp https://github.com/ggml-org/llama.cpp · IBM Granite https://huggingface.co/ibm-granite |
| s390x packages | Debian (trixie) https://packages.debian.org/trixie/ |
| Tests | pytest https://docs.pytest.org · fakeredis https://github.com/cunla/fakeredis-py |

No Chart.js or other front-end library: the dashboard's chart is inline SVG.
