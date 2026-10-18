# Stage 3 — Fraud scoring container (Dhruv)

One event in (Albert's `event.schema.json`) → one scored event out
(Albert's `scored_event.schema.json`, with top 3 reasons in `scoring.reasons`).

In the pipeline: **Redis `tx.valid` → `stream_worker.py` → Redis `tx.scored`** (broken events → `tx.dlq`).

## Files
| File | What it does |
| --- | --- |
| `features.py` | Turns one payment into 27 numbers. Holds `CardMemory` (each card's recent history). Same code for training and live. |
| `train.py` | Trains logistic regression, random forest, XGBoost on `fraudTrain.csv`, picks ensemble weights and cut-offs on a validation slice, grades on `fraudTest.csv`, saves to `models/`. |
| `scorer.py` | `FraudScorer`: `score(event)` for one, `score_batch(events)` for many at once. Skips repeated `event_id`s. |
| `dedupe.py` | Remembers the last 500,000 `event_id`s so a repeated event isn't counted twice. |
| `stream_worker.py` | **The container's entrypoint.** Reads Redis `tx.valid`, scores in batches, writes `tx.scored`. |
| `run.py` | Same scoring over stdin/stdout (one JSON per line), for quick local tests without Redis. |
| `Dockerfile` | The container. Build it on the LinuxONE VM. |
| `make_events.py` | Test helper: writes fraudTest rows as schema events, like Albert's producer will. |
| `demo_stream.py` | Fakes the producer: builds schema-format events from `fraudTest.csv`, scores them one by one, validates every output against the schema, reports latency. |

## Setup (from this folder; the `.venv` is shared one level up in `Dhruv/`)
```bash
python3 -m venv ../.venv && ../.venv/bin/pip install -r requirements.txt
../.venv/bin/python train.py                 # ~1.5 min, writes models/
../.venv/bin/python demo_stream.py           # speed + accuracy check on 50k events
../.venv/bin/python make_events.py 1000 | ../.venv/bin/python run.py --warm-up ../../Datasets/fraudTrain.csv > scored.jsonl
```
(On this Mac xgboost needed OpenMP; we copied scikit-learn's bundled `libomp.dylib` next to `libxgboost.dylib`. On LinuxONE use the Dockerfile below.)

## Docker on LinuxONE (build on the VM itself)
The image installs numpy / pandas / scikit-learn / xgboost from **Debian's own s390x builds** (apt), because PyPI
has no reliable s390x builds for them. Nothing compiles. The models are **not** copied from a laptop: they're
trained inside the image, because byte order and library versions must match.
```bash
# from the repo root, on the VM, after Albert's: docker network create fraud-net + redis container
docker build -t fraud-scorer:latest Dhruv/stage3_scoring
docker volume create fraud-models
# 1) train once (~2 min; needs Datasets/fraudTrain.csv + fraudTest.csv)
docker run --rm -v "$PWD/Datasets:/data:ro" -v fraud-models:/models fraud-scorer:latest /app/train.py
# 2) run the worker (loads card history from fraudTrain.csv first, ~15 s)
docker run -d --name scorer --network fraud-net -v "$PWD/Datasets:/data:ro" -v fraud-models:/models \
    fraud-scorer:latest
```
Worker options: `--count` (max batch, 100), `--block-ms`, `--in-stream`, `--out-stream`, `--group` (`scorers`),
`--consumer` (`scorer-1`), `--warm-up CSV`. Live numbers go to the Redis hash `metrics:scorer`
(scored, allow/review/block, duplicates, dlq, events_per_sec, p50_ms, p95_ms, cards_tracked).

**Not verified yet:** no Docker or LinuxONE machine was available. The Debian packages were checked to exist for s390x
(trixie: xgboost 3.0.0, scikit-learn 1.4.2, pandas 2.2.3, numpy 2.2.4, redis 6.1.0).

## Rules the worker follows
- **Run ONE scorer.** Card history lives in the worker's memory. Two scorers would each see half of every card's
  payments. The worker logs a warning if it sees another active scorer in its group.
- **Replay `fraudTest.csv`, warm up from `fraudTrain.csv`.** The models learned from fraudTrain, so replaying it
  would mean scoring data the models have already seen (and counting every payment twice in card history).
  The worker warns once if events are older than its warm-up history.
- **At-least-once, like Albert's validator.** It writes to `tx.scored`, then XACKs `tx.valid`. A crash in between means
  a few events can be written twice: stage 5 must ignore a repeated `event_id` (`ON CONFLICT (event_id) DO NOTHING`).
- **Repeated `event_id`s are skipped** (counted as `duplicates`), so card history stays right.
- **The original event is pasted in byte-for-byte**, as the contract asks.
- **After a restart**, card history is rebuilt from the warm-up file only: payments scored before the crash
  are no longer in memory. Fine for a demo; a production version would save card history to Redis.

## How to call it
```python
from scorer import FraudScorer
scorer = FraudScorer()
scorer.warm_up_from_csv("../Datasets/fraudTrain.csv")   # optional, gives cards history
scored = scorer.score(event)                            # scored_event.schema.json dict
scored, reasons = scorer.score_with_reasons(event)      # same, plus the reasons list on its own
```
- Feed events **in time order** (`occurred_at`), because features depend on each card's history.
- The scorer **never reads `event["label"]`**.
- A broken event raises `ValueError` (stage 2 should already have dead-lettered it).

## How the score is made
- `model_scores`: `logistic_regression`, `random_forest`, `xgboost`, each a 0–1 fraud probability.
- `ensemble_score` = weighted average, **0.1 × LR + 0.1 × RF + 0.8 × XGBoost**. The weights were picked on validation (last 15% of fraudTrain) by best PR-AUC, with each model kept at ≥ 0.1 so it stays an ensemble.
- `decision`:
  - `ALLOW` if score < `threshold` (0.174, best F1 on validation)
  - `REVIEW` if `threshold` ≤ score < block cut-off (0.606, the lowest score with ≥ 99% precision on validation)
  - `BLOCK` above that
- `scoring_duration_ms` = feature build + inference **for the batch the event was in** (every event in a batch waited
  for the whole batch, so each reports the batch's time). Reasons are computed after the clock stops, and only for REVIEW/BLOCK.

## Batch scoring (why one scorer keeps up)
The models cost about the same to run on 1 event or 50, so the worker scores whatever is waiting in one go.
At low traffic a batch is 1 event (no extra delay); at high traffic batches fill up. Results are identical
to scoring one at a time (checked on 3,000 events). Measured on the Mac:

| Events per batch | Time per event | One scorer can handle |
| --- | --- | --- |
| 1 | 4.1 ms | ~245 events/s |
| 10 | 0.43 ms | ~2,300 events/s |
| 50 | 0.10 ms | ~9,600 events/s |
- `model_version` = `ens-v1`. Bump `MODEL_VERSION` in `train.py` when retraining.

## Reasons (`scoring.reasons`, REVIEW/BLOCK only)
```json
[{"feature": "recent_activity", "contribution": 5.995, "value": 4,
  "detail": "4 other payments totalling $1,935 on this card in the last 24h"},
 {"feature": "time_of_day", "contribution": 2.274, "value": 22, "detail": "made at 22:00 (late night)"}]
```
`contribution` = how much that feature pushed XGBoost's score toward fraud (same idea as SHAP values). Left out entirely for ALLOW (the field is optional).

## Assumptions the producer must match
- `card_id` = `sha256(str(cc_num))[:16]`, using the decimal digits of the raw card number (same as `features.card_id_from_cc_num`).
- Read `cc_num` with `dtype=str` before hashing.
- `occurred_at` keeps the dataset's clock time and just labels it UTC (`2019-01-01 00:00:18` → `2019-01-01T00:00:18Z`). Shifting the hours would break the time-of-day feature.

## Results (fraudTest, 555,719 payments never seen, 2,145 fraud)
Ensemble PR-AUC 0.964. At the REVIEW cut-off, precision 0.94 and recall 0.91. Latency on a MacBook: p50 ~4 ms, p95 ~5 ms per event. Not yet measured on LinuxONE.
Also checked: 500 events from Albert's `stubgen.py` score with 0 errors under his own validators and map cleanly through his `to_row()`.

**Pipeline test (Oct 17, Mac, fake Redis server instead of real Redis, no Docker):** Albert's real producer (1,000 events/s,
1% deliberately broken) → his validator → `stream_worker.py` → stage 4 worker.
- 20,000 events: every valid one scored and explained once, broken ones all in `tx.dlq`, 0 schema errors, original event text unchanged.
- 100 re-sent events: all skipped as duplicates.
- Stage 3 `kill -9`'d mid-stream and restarted: 0 lost, 0 written twice.
- Stage 3 **and** stage 4 `kill -9`'d mid-stream: 0 lost, 2 written twice (stage 5 drops those by `event_id`).
- Latency numbers from this test are **not meaningful**: the fake Redis server is far slower than real Redis.

## Open-source used
- Sparkov dataset: https://www.kaggle.com/datasets/kartik2112/fraud-detection
- scikit-learn https://scikit-learn.org · XGBoost https://xgboost.ai · pandas https://pandas.pydata.org · NumPy https://numpy.org · joblib https://joblib.readthedocs.io · jsonschema https://python-jsonschema.readthedocs.io · redis-py https://github.com/redis/redis-py · Debian packages https://packages.debian.org
