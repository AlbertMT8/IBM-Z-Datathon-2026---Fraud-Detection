"""
demo_stream.py — pretends to be the live pipeline: feeds fraudTest payments to
score() one at a time, in time order, and reports speed + what got flagged.

Run:  .venv/bin/python demo_stream.py            (first 50,000 payments)
      .venv/bin/python demo_stream.py 200000     (more)
"""

import json
import sys
import time

import numpy as np
import pandas as pd

from scorer import FraudScorer

n = int(sys.argv[1]) if len(sys.argv) > 1 else 50_000
df = pd.read_csv("../Datasets/fraudTest.csv", nrows=n).sort_values("trans_date_trans_time", kind="stable")
payments = df.drop(columns=["Unnamed: 0"]).to_dict("records")

scorer = FraudScorer()
# Give the scorer each card's history from the older file first, like a real bank would have.
history = pd.read_csv("../Datasets/fraudTrain.csv").sort_values("trans_date_trans_time", kind="stable")
scorer.warm_up(history.to_dict("records"))
print(f"Warmed up card memory with {len(history):,} past payments")
lat, alerts, caught = [], 0, 0
first_alert = None
t0 = time.time()
for p in payments:
    truth = p.pop("is_fraud")  # the scorer never sees the answer
    r = scorer.score(p)
    lat.append(r["latency_ms"])
    if r["is_alert"]:
        alerts += 1
        caught += truth
        if first_alert is None and truth:
            first_alert = r
wall = time.time() - t0

frauds = int(df.is_fraud.sum())
print(f"Scored {len(payments):,} payments in {wall:.1f}s = {len(payments)/wall:,.0f} payments/sec (one at a time)")
print(f"Latency per payment: p50 {np.percentile(lat,50):.2f} ms | p95 {np.percentile(lat,95):.2f} ms | p99 {np.percentile(lat,99):.2f} ms")
print(f"Alerts: {alerts}  |  real fraud among alerts: {caught} ({caught/max(alerts,1):.0%})  |  fraud caught: {caught}/{frauds} ({caught/max(frauds,1):.0%})")
print("\nExample alert output:")
print(json.dumps(first_alert, indent=2, default=bool))
