"""
demo_stream.py — pretends to be the live pipeline: turns fraudTest rows into events
in Albert's event.schema.json format, feeds them to the scorer one at a time in time
order, checks every output against scored_event.schema.json, and reports speed.

Run:  .venv/bin/python demo_stream.py            (first 50,000 payments)
      .venv/bin/python demo_stream.py 200000     (more)
"""

import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone

import jsonschema
import numpy as np
import pandas as pd
from referencing import Registry, Resource

from features import card_id_from_cc_num
from scorer import FraudScorer

HERE = os.path.dirname(os.path.abspath(__file__))
# Albert's latest contract (the one with scoring.reasons). It lives in Albert/ on GitHub;
# locally it may sit in Albert/New stuff/. Use whichever copy has the reasons field.
SCHEMA_DIRS = [os.path.join(HERE, "..", "Albert"), os.path.join(HERE, "..", "Albert", "New stuff")]
SCHEMAS = next(d for d in SCHEMA_DIRS
               if os.path.exists(os.path.join(d, "scored_event.schema.json"))
               and '"reasons"' in open(os.path.join(d, "scored_event.schema.json")).read())


def load_validator():
    """Validators for both schemas; scored_event refers to event.schema.json by name."""
    ev = json.load(open(os.path.join(SCHEMAS, "event.schema.json")))
    sc = json.load(open(os.path.join(SCHEMAS, "scored_event.schema.json")))
    reg = Registry().with_resource("event.schema.json", Resource.from_contents(ev))
    return jsonschema.Draft202012Validator(ev), jsonschema.Draft202012Validator(sc, registry=reg)


def make_event(r):
    """A CSV row -> an event, the way Albert's producer builds it (our stand-in for testing)."""
    occurred = datetime.fromisoformat(r["trans_date_trans_time"]).replace(tzinfo=timezone.utc)
    return {
        "schema_version": "1.0.0",
        "event_id": str(uuid.uuid4()),
        "emitted_at": datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z"),
        "run_id": "dhruv-demo",
        "source_trans_num": r["trans_num"],
        "transaction": {
            "occurred_at": occurred.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            "unix_time": int(occurred.timestamp()),  # derived from occurred_at, per the schema
            "card_id": card_id_from_cc_num(r["cc_num"]),
            "card_last4": str(r["cc_num"])[-4:],
            "amount": round(float(r["amt"]), 2),
            "merchant": r["merchant"],
            "category": r["category"],
            "merchant_lat": float(r["merch_lat"]),
            "merchant_lon": float(r["merch_long"]),
        },
        "cardholder": {
            "city": r["city"], "state": r["state"], "zip": str(r["zip"]).zfill(5),
            "lat": float(r["lat"]), "lon": float(r["long"]), "city_pop": int(r["city_pop"]),
            "job": r["job"], "gender": r["gender"], "dob": r["dob"],
        },
        "label": {"is_fraud": int(r["is_fraud"])},  # the scorer must never read this
    }


n = int(sys.argv[1]) if len(sys.argv) > 1 else 50_000
event_check, scored_check = load_validator()
df = pd.read_csv(os.path.join(HERE, "..", "Datasets", "fraudTest.csv"), nrows=n,
                 dtype={"cc_num": str, "zip": str})
df = df.sort_values("trans_date_trans_time", kind="stable")

scorer = FraudScorer()
warm = scorer.warm_up_from_csv(os.path.join(HERE, "..", "Datasets", "fraudTrain.csv"))
print(f"Warmed up card memory with {warm:,} past payments")

lat, decisions, caught, frauds, bad = [], {"ALLOW": 0, "REVIEW": 0, "BLOCK": 0}, 0, 0, 0
example = None
t0 = time.time()
for r in df.to_dict("records"):
    event = make_event(r)
    scored, reasons = scorer.score_with_reasons(event)
    s = scored["scoring"]
    lat.append(s["scoring_duration_ms"])
    decisions[s["decision"]] += 1
    truth = event["label"]["is_fraud"]
    frauds += truth
    if s["decision"] != "ALLOW":
        caught += truth
        if example is None and truth:
            example = (scored, reasons)
    # Check every output against the contract (slow, so it's not counted in latency).
    if len(lat) <= 5000 or s["decision"] != "ALLOW":
        bad += sum(1 for _ in event_check.iter_errors(event))
        bad += sum(1 for _ in scored_check.iter_errors(scored))
wall = time.time() - t0

alerts = decisions["REVIEW"] + decisions["BLOCK"]
print(f"Schemas used: {os.path.relpath(SCHEMAS, HERE)}")
print(f"Scored {len(lat):,} events (one at a time). Schema errors in outputs checked: {bad}")
print(f"scoring_duration_ms: p50 {np.percentile(lat,50):.2f} | p95 {np.percentile(lat,95):.2f} | p99 {np.percentile(lat,99):.2f}  (budget p95 <= 25)")
print(f"Decisions: {decisions}")
print(f"Real fraud among REVIEW+BLOCK: {caught}/{alerts} ({caught/max(alerts,1):.0%})  |  fraud caught: {caught}/{frauds} ({caught/max(frauds,1):.0%})")
print("\nExample alert (scored event):")
print(json.dumps(example[0], indent=2))
