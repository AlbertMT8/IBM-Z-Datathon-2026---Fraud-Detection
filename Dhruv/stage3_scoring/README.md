# Stage 3 — Fraud scoring container (Dhruv)

One event in (Albert's `event.schema.json`) → one scored event out
(Albert's `scored_event.schema.json`, with top 3 reasons in `scoring.reasons`).

## Files
| File | What it does |
| --- | --- |
| `features.py` | Turns one payment into 27 numbers. Holds `CardMemory` (each card's recent history). Same code for training and live. |
| `train.py` | Trains logistic regression, random forest, XGBoost on `fraudTrain.csv`, picks ensemble weights and cut-offs on a validation slice, grades on `fraudTest.csv`, saves to `models/`. |
| `scorer.py` | `FraudScorer().score(event)` — what the pipeline calls. |
| `run.py` | **The container's entrypoint.** Events in on stdin (one JSON per line), scored events out on stdout. Bad lines go to stderr as dead-letter records. |
| `make_events.py` | Test helper: writes fraudTest rows as schema events, like Albert's producer will. |
| `demo_stream.py` | Fakes the producer: builds schema-format events from `fraudTest.csv`, scores them one by one, validates every output against the schema, reports latency. |

## Setup (from this folder; the `.venv` is shared one level up in `Dhruv/`)
```bash
python3 -m venv ../.venv && ../.venv/bin/pip install -r requirements.txt
../.venv/bin/python train.py                 # ~1.5 min, writes models/
../.venv/bin/python demo_stream.py           # speed + accuracy check on 50k events
../.venv/bin/python make_events.py 1000 | ../.venv/bin/python run.py --warm-up ../../Datasets/fraudTrain.csv > scored.jsonl
```
(On this Mac xgboost needed OpenMP; we copied scikit-learn's bundled `libomp.dylib` next to `libxgboost.dylib`. On LinuxONE use IBM's AI/ML container instead.)

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
- `scoring_duration_ms` = feature build + inference. Reasons are computed after the clock stops, and only for REVIEW/BLOCK.
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

## Open-source used
- Sparkov dataset: https://www.kaggle.com/datasets/kartik2112/fraud-detection
- scikit-learn https://scikit-learn.org · XGBoost https://xgboost.ai · pandas https://pandas.pydata.org · NumPy https://numpy.org · joblib https://joblib.readthedocs.io · jsonschema https://python-jsonschema.readthedocs.io
