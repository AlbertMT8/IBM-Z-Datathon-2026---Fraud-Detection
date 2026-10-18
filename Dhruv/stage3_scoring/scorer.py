"""
scorer.py — stage 3. One event in (Albert's event.schema.json), one scored event
out (Albert's scored_event.schema.json).

    from scorer import FraudScorer
    scorer = FraudScorer()                        # loads models once (~1s)
    scorer.warm_up_from_csv("fraudTrain.csv")     # optional: give cards their history
    scored = scorer.score(event)                  # every event (~ms)
    scored, reasons = scorer.score_with_reasons(event)   # same, plus reasons for stage 4

OUTPUT (scored_event.schema.json):
    {
      "schema_version": "1.0.0",
      "event": { ...the input event, untouched... },
      "scoring": {
        "scored_at": "2026-10-17T15:04:05.123Z",
        "model_scores": {"xgboost": 0.99, "random_forest": 0.71, "logistic_regression": 0.40},
        "ensemble_score": 0.90,          # weighted average (weights in models/meta.json)
        "threshold": 0.17,               # REVIEW cut-off
        "decision": "BLOCK",             # ALLOW / REVIEW / BLOCK
        "model_version": "ens-v1",
        "scoring_duration_ms": 4.2,      # feature build + inference
        "feature_count": 27,
        "reasons": [...]                 # only on REVIEW/BLOCK
      },
      "explanation": null                # stage 4 fills this
    }

REASONS go inside scoring.reasons (optional field Albert added), only for REVIEW/BLOCK:
    [{"feature": "amount", "contribution": 3.1, "value": 912.4,
      "detail": "amount $912.40 is 14.2x this card's usual $64"}, ...]
  contribution = how much it pushed XGBoost's score toward fraud (positive = more fraud-like).
  score_with_reasons() also returns the list on its own, for convenience.

A broken event raises ValueError — stage 2 should already have sent it to the
dead-letter queue, this is a second safety net.

The scorer never reads event["label"]. Feed events in time order (CardMemory).
"""

import json
import os
import time
from datetime import datetime, timezone

import joblib
import numpy as np
import pandas as pd
import xgboost as xgb

from dedupe import RecentIds
from features import FEATURE_NAMES, CardMemory, flatten_event, rows_from_csv

HERE = os.path.dirname(os.path.abspath(__file__))

# Several features describe the same idea; group them so reasons aren't repetitive.
GROUP = {"amt": "amount", "log_amt": "amount", "amt_vs_card_avg": "amount",
         "hour": "time_of_day", "is_night": "time_of_day",
         "card_n_24h": "recent_activity", "card_amt_24h": "recent_activity",
         "log_secs_since_last": "time_since_last", "age": "age", "is_male": "gender",
         "log_city_pop": "city_size", "distance_km": "distance", "card_n_prev": "card_history"}


def _now_rfc3339():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class DuplicateEvent(Exception):
    """This event_id was already scored. Not an error: just skip it (and ack it)."""


class FraudScorer:
    def __init__(self, model_dir=None):
        model_dir = model_dir or os.environ.get("MODELS_DIR") or os.path.join(HERE, "models")
        with open(os.path.join(model_dir, "meta.json")) as f:
            meta = json.load(f)
        assert meta["feature_names"] == FEATURE_NAMES, "models were trained with different features"
        self.model_version = meta["model_version"]
        self.threshold = meta["threshold"]
        self.block_threshold = meta["block_threshold"]
        self.weights = meta["weights"]
        self.lr = joblib.load(os.path.join(model_dir, "lr.joblib"))
        self.rf = joblib.load(os.path.join(model_dir, "rf.joblib"))
        self.xgb = xgb.Booster()
        self.xgb.load_model(os.path.join(model_dir, "xgb.json"))
        self.xgb.set_param({"nthread": 1})
        self.memory = CardMemory()
        self.seen = RecentIds()  # event_ids already scored, so repeats don't count twice
        self.history_until = None  # latest warm-up time, "YYYY-MM-DDTHH:MM:SS"

    def warm_up_from_csv(self, csv_path):
        """Load card history without scoring (a real bank already knows its cards' past)."""
        df = pd.read_csv(csv_path).sort_values("trans_date_trans_time", kind="stable")
        for p in rows_from_csv(df):
            self.memory.compute(p)
        if len(df):
            self.history_until = str(df["trans_date_trans_time"].iloc[-1]).replace(" ", "T")[:19]
        return len(df)

    def score(self, event):
        return self.score_with_reasons(event)[0]

    def score_with_reasons(self, event):
        """One event -> (scored_event, reasons). Raises ValueError for a broken event,
        DuplicateEvent for an event_id we've already scored."""
        result = self.score_batch([event])[0]
        if isinstance(result, Exception):
            raise result
        return result

    def score_batch(self, events):
        """Several events (in time order) -> one result per event, same order. Each result is
        (scored_event, reasons), or a ValueError / DuplicateEvent instance for that event.

        Why batches: the models cost about the same to run on 1 row or 50, so scoring
        whatever is waiting in one go lets one scorer keep up with a fast stream.
        Card memory is still updated one event at a time, in order."""
        start = time.perf_counter()
        out = [None] * len(events)
        rows, where, facts_list = [], [], []
        for i, event in enumerate(events):
            event_id = event.get("event_id") if isinstance(event, dict) else None
            if event_id is not None and event_id in self.seen:
                out[i] = DuplicateEvent(event_id)
                continue
            try:
                feats, facts = self.memory.compute(flatten_event(event))
            except (KeyError, TypeError, ValueError, AttributeError) as e:
                out[i] = ValueError(f"bad event: {type(e).__name__}: {e}")
                continue
            if event_id is not None:
                self.seen.add(event_id)
            rows.append(feats)
            where.append(i)
            facts_list.append(facts)
        if not rows:
            return out

        x = np.asarray(rows, dtype=np.float32)
        dm = xgb.DMatrix(x, feature_names=FEATURE_NAMES)
        p_lr = self.lr.predict_proba(x)[:, 1]
        p_rf = self.rf.predict_proba(x)[:, 1]
        p_xgb = self.xgb.predict(dm)
        w = self.weights
        prob = np.clip(w["lr"] * p_lr + w["rf"] * p_rf + w["xgb"] * p_xgb, 0.0, 1.0)
        decisions = np.where(prob >= self.block_threshold, "BLOCK",
                             np.where(prob >= self.threshold, "REVIEW", "ALLOW"))
        # Every event in the batch waited for the whole batch, so each reports the batch's time
        # (honest, slightly pessimistic). Reasons below are outside the clock: "feature build + inference".
        duration_ms = round((time.perf_counter() - start) * 1000, 3)

        alerts = [k for k, d in enumerate(decisions) if d != "ALLOW"]
        contribs = {}
        if alerts:
            # How much each feature pushed XGBoost's score up (+) or down (-). Alerts only.
            c = self.xgb.predict(xgb.DMatrix(x[alerts], feature_names=FEATURE_NAMES), pred_contribs=True)
            contribs = {k: c[j][:-1] for j, k in enumerate(alerts)}  # last value is the baseline

        scored_at = _now_rfc3339()
        for k, i in enumerate(where):
            reasons = self._reasons(contribs[k], rows[k], facts_list[k]) if k in contribs else []
            scored = {
                "schema_version": "1.0.0",
                "event": events[i],  # passed through untouched
                "scoring": {
                    "scored_at": scored_at,
                    "model_scores": {"logistic_regression": round(float(p_lr[k]), 6),
                                     "random_forest": round(float(p_rf[k]), 6),
                                     "xgboost": round(float(p_xgb[k]), 6)},
                    "ensemble_score": round(float(prob[k]), 6),
                    "threshold": self.threshold,
                    "decision": str(decisions[k]),
                    "model_version": self.model_version,
                    "scoring_duration_ms": duration_ms,
                    "feature_count": len(FEATURE_NAMES),
                },
                "explanation": None,
            }
            if reasons:  # optional field; left out for ALLOW
                scored["scoring"]["reasons"] = reasons
            out[i] = (scored, reasons)
        return out

    def _reasons(self, contribs, feats, facts, top=3):
        impact = {}
        for name, c in zip(FEATURE_NAMES, contribs):
            g = "category" if name.startswith("cat_") else GROUP[name]
            impact[g] = impact.get(g, 0.0) + float(c)
        ranked = sorted((kv for kv in impact.items() if kv[1] > 0), key=lambda kv: -kv[1])[:top]
        f = dict(zip(FEATURE_NAMES, feats))
        out = []
        for g, v in ranked:
            r = {"feature": g, "contribution": round(v, 3)}
            val = _value(g, f, facts)
            if val is not None:
                r["value"] = val
            r["detail"] = _describe(g, f, facts)[:200]
            out.append(r)
        return out


def _value(group, f, facts):
    """The one raw number/label behind each reason, for the explanation layer."""
    return {
        "amount": facts["amt"],
        "time_of_day": facts["hour"],
        "category": facts["category"],
        "recent_activity": facts["card_n_24h"],
        "time_since_last": round(facts["secs_since_last"] / 60, 1) if facts["secs_since_last"] is not None else None,
        "age": round(f["age"], 1),
        "distance": round(f["distance_km"], 1),
        "city_size": int(round(np.expm1(f["log_city_pop"]))),
        "gender": "M" if f["is_male"] else "F",
        "card_history": int(f["card_n_prev"]),
    }.get(group)


def _describe(group, f, facts):
    """Plain-English fact for one reason. The explanation teammate can polish these."""
    if group == "amount":
        s = f"amount ${facts['amt']:,.2f}"
        if facts["card_avg_amt"]:
            s += f" is {f['amt_vs_card_avg']:.1f}x this card's usual ${facts['card_avg_amt']:,.0f}"
        return s
    if group == "time_of_day":
        return f"made at {facts['hour']:02d}:00" + (" (late night)" if f["is_night"] else "")
    if group == "category":
        return f"merchant category '{facts['category']}'"
    if group == "recent_activity":
        return f"{facts['card_n_24h']} other payments totalling ${facts['card_amt_24h']:,.0f} on this card in the last 24h"
    if group == "time_since_last":
        s = facts["secs_since_last"]
        return "first payment seen from this card" if s is None else f"{s/60:,.0f} minutes since this card's previous payment"
    if group == "age":
        return f"cardholder age {f['age']:.0f}"
    if group == "distance":
        return f"merchant {f['distance_km']:.0f} km from cardholder's home"
    if group == "city_size":
        return f"cardholder city population {np.expm1(f['log_city_pop']):,.0f}"
    if group == "card_history":
        return f"{f['card_n_prev']:.0f} earlier payments seen from this card"
    return group


# Simple module-level helper so teammates can just `from scorer import score`.
_default = None


def score(event):
    global _default
    if _default is None:
        _default = FraudScorer()
    return _default.score(event)
