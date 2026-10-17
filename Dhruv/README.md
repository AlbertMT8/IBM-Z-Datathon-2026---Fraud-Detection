# Stage 3 — Fraud scoring (Dhruv)

One payment in → fraud probability + top 3 reasons out.

## Files
| File | What it does |
| --- | --- |
| `features.py` | Turns one payment into 27 numbers. Holds `CardMemory` (each card's recent history). Same code for training and live. |
| `train.py` | Trains logistic regression, random forest, XGBoost on `fraudTrain.csv`, grades on `fraudTest.csv`, saves to `models/`. |
| `scorer.py` | `FraudScorer().score(payment)` — what the pipeline calls. |
| `demo_stream.py` | Fakes the live stream: scores test payments one by one, reports speed and alerts. |

## Setup
```bash
python3 -m venv .venv && .venv/bin/pip install pandas scikit-learn xgboost joblib
.venv/bin/python train.py
.venv/bin/python demo_stream.py
```
(On this Mac xgboost needed OpenMP; we copied scikit-learn's bundled `libomp.dylib` next to `libxgboost.dylib`. On LinuxONE use IBM's AI/ML container instead.)

## Contract with the pipeline
**Input** — a dict per payment with the Sparkov fields (extras ignored), sent **in time order**:
`trans_num, trans_date_trans_time, cc_num, category, amt, gender, lat, long, city_pop, dob, merch_lat, merch_long`

**Output**
```json
{
  "trans_num": "2da90c7d...",
  "fraud_probability": 0.93,
  "is_alert": true,
  "model_scores": {"lr": 0.41, "rf": 0.97, "xgb": 0.99},
  "reasons": [
    {"feature": "amount", "impact": 3.1, "text": "amount $912.40 is 14.2x this card's usual $64"},
    {"feature": "time_of_day", "impact": 2.0, "text": "made at 23:00 (late night)"},
    {"feature": "category", "impact": 0.9, "text": "merchant category 'shopping_net'"}
  ],
  "latency_ms": 1.8
}
```
`reasons` is filled only when `is_alert` is true (saves ~1 ms per payment); pass `explain_all=True` to get them for every payment. Call `scorer.warm_up(past_payments)` once at startup so cards have history.

A missing or broken field raises `ValueError` → pipeline should send it to the dead-letter queue.

## Open-source used
- Sparkov dataset: https://www.kaggle.com/datasets/kartik2112/fraud-detection
- scikit-learn https://scikit-learn.org · XGBoost https://xgboost.ai · pandas https://pandas.pydata.org · NumPy https://numpy.org · joblib https://joblib.readthedocs.io
