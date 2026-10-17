# Context for Claude Code — IBM Z Datathon 2026 (Oct 17–18)

Full team context is in `IBM-Z-Datathon-2026-Team-Context-Handoff.md` in this folder. Read it first.

## Who I am and how to talk to me
- Dhruv, Math + CS student at NYU, wants to go into quant finance.
- I am new to most of this. Explain things very simply, one step at a time, with everyday analogies. Avoid jargon; if a technical word is needed, explain it in one plain sentence.
- I want to understand, not just copy code. Explain what each piece of code does and why.
- Give honest, practical opinions even if they disagree with me.

## My role on the team: the ML scoring part (stage 3)
Goal: a function `score(payment) -> (fraud_probability 0–1, top 2–3 reasons)` that teammates call.

Plan:
1. Get data: Sparkov dataset from Kaggle (kartik2112) — `fraudTrain.csv` and `fraudTest.csv`. Put them in this folder.
2. Explore: how fraud differs from normal payments (amount, hour, category, age, etc.).
3. Features: amount, hour, category, age, distance, time since this card's last payment, amount vs this card's usual amount. Same feature code must be used in training and in the live stream (per-card state).
4. Train on fraudTrain (older), test on fraudTest (newer) — this is already a time-based split. Never test on data the model trained on.
5. Models: logistic regression, random forest, XGBoost; average their probabilities (ensemble).
6. Evaluate with precision, recall, PR-AUC, and "precision in the top N alerts". Do NOT lead with accuracy (fraud is ~0.5%, so accuracy is misleading).
7. Reasons: use XGBoost `pred_contribs` (SHAP-style) to get the top contributing features per payment; the explanation teammate turns these into a sentence.
8. Speed: one score should take a few milliseconds. Measure it.
9. Save models + `score()` function; agree the input/output format with the team early.
10. Final runtime is IBM LinuxONE (s390x, big-endian) — use IBM's AI/ML Jupyter container which has XGBoost/scikit-learn prebuilt for s390x.

## Rules to respect
- All code must be written fresh during Oct 17–18 (no old repos).
- Link every open-source library/dataset used in the submission.

## Status
- As of 10:10 AM Oct 17: plan agreed, next step is step 1 (get the data) then step 2 (explore together).
