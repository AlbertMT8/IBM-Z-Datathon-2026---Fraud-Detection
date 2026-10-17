"""
scorer.py — the thing teammates call. One payment in, one verdict out.

    from scorer import FraudScorer
    scorer = FraudScorer()           # loads models once (slow part, ~1s)
    result = scorer.score(payment)   # call for every payment (fast part, ~ms)

INPUT  `payment`: a dict with the Sparkov fields (extra fields are ignored):
    trans_num, trans_date_trans_time ("2020-06-21 12:14:25"), cc_num, category,
    amt, gender, lat, long, city_pop, dob ("1968-03-19"), merch_lat, merch_long

OUTPUT: a dict
    {
      "trans_num": "2da90c7d...",
      "fraud_probability": 0.93,           # 0-1, weighted average of the 3 models
      "is_alert": True,                    # probability >= the cut-off chosen in training
      "model_scores": {"lr": .., "rf": .., "xgb": ..},
      "reasons": [                         # top 3 things that pushed the score UP
          {"feature": "amount", "impact": 3.1, "text": "amount $912.40 is 14.2x this card's usual $64"},
          ...
      ],
      "latency_ms": 1.8
    }

A missing/broken field raises ValueError — the pipeline should catch that and send the
payment to the dead-letter queue instead of crashing.

The scorer keeps CardMemory (each card's recent history), so feed payments in time order.
"""

import json
import os
import time

import joblib
import numpy as np
import xgboost as xgb

from features import FEATURE_NAMES, CardMemory

HERE = os.path.dirname(os.path.abspath(__file__))
REQUIRED = ["trans_date_trans_time", "cc_num", "category", "amt", "lat", "long",
            "city_pop", "dob", "merch_lat", "merch_long"]

# Several features describe the same idea; group them so reasons aren't repetitive.
GROUP = {"amt": "amount", "log_amt": "amount", "amt_vs_card_avg": "amount",
         "hour": "time_of_day", "is_night": "time_of_day",
         "card_n_24h": "recent_activity", "card_amt_24h": "recent_activity",
         "log_secs_since_last": "time_since_last", "age": "age", "is_male": "gender",
         "log_city_pop": "city_size", "distance_km": "distance", "card_n_prev": "card_history"}


class FraudScorer:
    def __init__(self, model_dir=os.path.join(HERE, "models")):
        with open(os.path.join(model_dir, "meta.json")) as f:
            meta = json.load(f)
        assert meta["feature_names"] == FEATURE_NAMES, "models were trained with different features"
        self.threshold = meta["threshold"]
        self.weights = meta.get("weights", {"lr": 1 / 3, "rf": 1 / 3, "xgb": 1 / 3})
        self.lr = joblib.load(os.path.join(model_dir, "lr.joblib"))
        self.rf = joblib.load(os.path.join(model_dir, "rf.joblib"))
        self.xgb = xgb.Booster()
        self.xgb.load_model(os.path.join(model_dir, "xgb.json"))
        self.xgb.set_param({"nthread": 1})
        self.memory = CardMemory()

    def warm_up(self, past_payments):
        """Load card history without scoring (a real bank already knows its cards' past)."""
        for p in past_payments:
            self.memory.compute(p)

    def score(self, payment, explain_all=False):
        start = time.perf_counter()
        missing = [k for k in REQUIRED if payment.get(k) in (None, "")]
        if missing:
            raise ValueError(f"payment missing fields: {missing}")
        try:
            feats, facts = self.memory.compute(payment)
        except (TypeError, ValueError) as e:
            raise ValueError(f"bad payment field: {e}") from e

        x = np.asarray([feats], dtype=np.float32)
        dm = xgb.DMatrix(x, feature_names=FEATURE_NAMES)
        p_lr = float(self.lr.predict_proba(x)[0, 1])
        p_rf = float(self.rf.predict_proba(x)[0, 1])
        p_xgb = float(self.xgb.predict(dm)[0])
        w = self.weights
        prob = w["lr"] * p_lr + w["rf"] * p_rf + w["xgb"] * p_xgb

        is_alert = prob >= self.threshold
        reasons = []
        if is_alert or explain_all:
            # How much each feature pushed XGBoost's score up (+) or down (-).
            # Costs ~1 ms, so by default only alerts get reasons.
            contribs = self.xgb.predict(dm, pred_contribs=True)[0][:-1]  # last value is the baseline
            reasons = self._reasons(contribs, feats, facts)

        return {
            "trans_num": payment.get("trans_num"),
            "fraud_probability": round(prob, 4),
            "is_alert": bool(is_alert),
            "model_scores": {"lr": round(p_lr, 4), "rf": round(p_rf, 4), "xgb": round(p_xgb, 4)},
            "reasons": reasons,
            "latency_ms": round((time.perf_counter() - start) * 1000, 3),
        }

    def _reasons(self, contribs, feats, facts, top=3):
        impact = {}
        for name, c in zip(FEATURE_NAMES, contribs):
            g = "category" if name.startswith("cat_") else GROUP[name]
            impact[g] = impact.get(g, 0.0) + float(c)
        ranked = sorted((kv for kv in impact.items() if kv[1] > 0), key=lambda kv: -kv[1])[:top]
        f = dict(zip(FEATURE_NAMES, feats))
        return [{"feature": g, "impact": round(v, 3), "text": _describe(g, f, facts)} for g, v in ranked]


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


def score(payment):
    global _default
    if _default is None:
        _default = FraudScorer()
    return _default.score(payment)
