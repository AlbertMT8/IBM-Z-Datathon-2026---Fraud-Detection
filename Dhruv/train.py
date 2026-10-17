"""
train.py — teaches the three models what fraud looks like, then grades them
on payments they have never seen.

Run:  .venv/bin/python train.py
Makes: models/lr.joblib, models/rf.joblib, models/xgb.json, models/meta.json

Steps:
 1. Load both files and sort everything by time (oldest first).
 2. Replay every payment through CardMemory, exactly like the live stream will.
 3. Split by time:  fit   = first 85% of fraudTrain   (models learn here)
                    valid = last 15% of fraudTrain    (we pick the alert cut-off here)
                    test  = all of fraudTest          (final grade; touched once)
 4. Train logistic regression, random forest, XGBoost. Ensemble = weighted average,
    weights picked on valid.
 5. Print honest metrics and save everything.
"""

import json
import os
import time

import joblib
import numpy as np
import pandas as pd
import xgboost as xgb
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import average_precision_score, precision_recall_curve, roc_auc_score
from sklearn.pipeline import make_pipeline
from sklearn.preprocessing import StandardScaler

from features import FEATURE_NAMES, CardMemory, rows_from_csv

HERE = os.path.dirname(os.path.abspath(__file__))
DATA = os.path.join(HERE, "..", "Datasets")
OUT = os.path.join(HERE, "models")
MODEL_VERSION = "ens-v1"  # bump whenever you retrain with changes, so the dashboard can tell runs apart
COLS = ["trans_date_trans_time", "cc_num", "category", "amt", "gender", "lat", "long",
        "city_pop", "dob", "merch_lat", "merch_long", "is_fraud"]


def build_features(df):
    """Replay payments in time order through CardMemory -> feature matrix."""
    mem = CardMemory()
    rows = rows_from_csv(df)
    X = np.empty((len(rows), len(FEATURE_NAMES)), dtype=np.float32)
    for i, p in enumerate(rows):
        X[i], _ = mem.compute(p)
    return X


def report(name, y, p, threshold):
    """Print the numbers that matter for rare events (not accuracy!)."""
    flagged = p >= threshold
    tp = int((flagged & (y == 1)).sum())
    precision = tp / max(flagged.sum(), 1)
    recall = tp / max((y == 1).sum(), 1)
    order = np.argsort(-p)
    top = {n: y[order[:n]].mean() for n in (100, 500, 1000)}
    print(f"  {name:9s} PR-AUC {average_precision_score(y, p):.3f} | ROC-AUC {roc_auc_score(y, p):.3f} | "
          f"at cut-off {threshold:.2f}: precision {precision:.3f}, recall {recall:.3f} | "
          f"precision in top 100/500/1000: {top[100]:.2f}/{top[500]:.2f}/{top[1000]:.2f}")


def main():
    t0 = time.time()
    train = pd.read_csv(os.path.join(DATA, "fraudTrain.csv"), usecols=COLS)
    test = pd.read_csv(os.path.join(DATA, "fraudTest.csv"), usecols=COLS)
    train["split"], test["split"] = "train", "test"
    # One timeline: fraudTest picks up right where fraudTrain ends, so card memory carries over.
    df = pd.concat([train, test], ignore_index=True)
    df = df.sort_values("trans_date_trans_time", kind="stable").reset_index(drop=True)
    print(f"Loaded {len(df):,} payments in {time.time()-t0:.0f}s. Building features...")

    t0 = time.time()
    X = build_features(df)
    y = df["is_fraud"].to_numpy()
    print(f"Features built in {time.time()-t0:.0f}s ({len(FEATURE_NAMES)} features per payment).")

    is_train = (df["split"] == "train").to_numpy()
    train_idx = np.where(is_train)[0]
    cut = int(len(train_idx) * 0.85)
    fit_i, val_i, test_i = train_idx[:cut], train_idx[cut:], np.where(~is_train)[0]
    Xf, yf = X[fit_i], y[fit_i]

    models = {
        "lr": make_pipeline(StandardScaler(), LogisticRegression(max_iter=1000)),
        "rf": RandomForestClassifier(n_estimators=100, max_depth=12, min_samples_leaf=20,
                                     n_jobs=-1, random_state=0),
        "xgb": xgb.XGBClassifier(n_estimators=300, max_depth=6, learning_rate=0.1,
                                 subsample=0.8, colsample_bytree=0.8, tree_method="hist",
                                 n_jobs=-1, random_state=0),
    }
    for name, m in models.items():
        t0 = time.time()
        m.fit(Xf, yf)
        print(f"Trained {name} in {time.time()-t0:.0f}s")

    # Pick how much each model counts, on VALID (never on test). Try every mix in
    # steps of 0.1 that adds up to 1 and keep the one with the best PR-AUC.
    # Every model keeps at least 0.1, otherwise the search just picks XGBoost alone
    # and it stops being an ensemble.
    raw_val = {n: m.predict_proba(X[val_i])[:, 1] for n, m in models.items()}
    best = (-1, None)
    for w_lr in np.arange(0.1, 0.81, 0.1):
        for w_rf in np.arange(0.1, 0.91 - w_lr, 0.1):
            w = {"lr": w_lr, "rf": w_rf, "xgb": 1 - w_lr - w_rf}
            p = sum(w[n] * raw_val[n] for n in w)
            ap = average_precision_score(y[val_i], p)
            if ap > best[0]:
                best = (ap, w)
    weights = {n: round(float(v), 2) for n, v in best[1].items()}
    print(f"\nEnsemble weights chosen on validation: {weights}")

    def all_probs(idx):
        ps = {n: m.predict_proba(X[idx])[:, 1] for n, m in models.items()}
        ps["equal_avg"] = (ps["lr"] + ps["rf"] + ps["xgb"]) / 3
        ps["ensemble"] = sum(weights[n] * ps[n] for n in weights)
        return ps

    # Pick the alert cut-off on VALID (never on test): the one with the best F1,
    # i.e. the best balance of "alerts are real" and "we catch most fraud".
    pv = all_probs(val_i)
    prec, rec, thr = precision_recall_curve(y[val_i], pv["ensemble"])
    f1 = 2 * prec * rec / np.maximum(prec + rec, 1e-9)
    threshold = float(thr[np.argmax(f1[:-1])])
    # BLOCK cut-off: the lowest score where, on validation, at least 99% of payments
    # above it are real fraud. Between the two cut-offs -> REVIEW (a human looks).
    ok = np.where(prec[:-1] >= 0.99)[0]
    block_threshold = float(thr[ok[0]]) if len(ok) else 0.99
    block_threshold = max(block_threshold, threshold)
    print(f"\nAlert (REVIEW) cut-off chosen on validation: {threshold:.3f} | BLOCK cut-off: {block_threshold:.3f}")

    print(f"\nVALIDATION (last 15% of fraudTrain, {len(val_i):,} payments, {y[val_i].sum()} fraud):")
    for n, p in pv.items():
        report(n, y[val_i], p, threshold)

    pt = all_probs(test_i)
    print(f"\nTEST (fraudTest, never seen, {len(test_i):,} payments, {y[test_i].sum()} fraud):")
    for n, p in pt.items():
        report(n, y[test_i], p, threshold)

    os.makedirs(OUT, exist_ok=True)
    for m in (models["lr"], models["rf"]):
        if hasattr(m, "n_jobs"):
            m.n_jobs = 1  # scoring one payment at a time: extra threads only add overhead
    joblib.dump(models["lr"], os.path.join(OUT, "lr.joblib"))
    joblib.dump(models["rf"], os.path.join(OUT, "rf.joblib"))
    models["xgb"].get_booster().save_model(os.path.join(OUT, "xgb.json"))
    with open(os.path.join(OUT, "meta.json"), "w") as f:
        json.dump({"model_version": MODEL_VERSION, "feature_names": FEATURE_NAMES, "threshold": threshold,
                   "block_threshold": block_threshold, "weights": weights}, f, indent=2)
    print(f"\nSaved models to {OUT}")


if __name__ == "__main__":
    main()
