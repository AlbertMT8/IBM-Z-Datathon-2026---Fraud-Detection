# System context: fraud pipeline, as of Oct 18, 2026 (repo commit `0185fdb`)

Written for **Albert** and **Albert's Claude** by Dhruv's side of the team. It describes what exists now, how the
pieces fit, what was measured on LinuxONE, and what is still open. It complements
`IBM-Z-Datathon-2026-Team-Context-Handoff.md` (the event, rubric and original plan), which is still accurate for the "why".

> **For Claude:** treat this file as the current state of the system. Albert's contracts in `Albert/New stuff/` are the
> source of truth for data shapes; this file says how stages 3–4 use them. Verify a detail in the code before relying
> on it if it matters, because the code may have moved on after this snapshot. Never commit secrets: `*.pem`, `*.key`
> and `.env` are git-ignored, and **the repo is public**.

---

## 1. The pipeline in one picture

```
 stage 1 producer ──XADD──▶ tx.raw ──▶ stage 2 validator ──▶ tx.valid ──▶ STAGE 3 scorer ──▶ tx.scored ──▶ STAGE 4 explainer ──▶ tx.explained ──▶ stage 5 (NOT BUILT)
 Albert/stage1_producer             Albert/stage2_validator      │        Dhruv/stage3_scoring     │       Dhruv/stage4_explanation                 Postgres + dashboard
                                          └──────────────▶ tx.dlq ◀────────────────────────────────┘
                                                      (broken events, all stages)
```

Every arrow is a **Redis stream**. Every consumer uses a **consumer group** and is **at-least-once**: it writes its output,
then XACKs its input. A crash between those two steps can repeat an event downstream, so **stage 5 must ignore a repeated
`event_id`** (`ON CONFLICT (event_id) DO NOTHING`; `event_id` is already the primary key in `schema.sql`).

| Stream | Written by | Read by (group) | Stream fields |
| --- | --- | --- | --- |
| `tx.raw` | stage 1 | stage 2 (`validators`) | `data`, `producer_id`, `seq`, `malformed` (only on injected faults) |
| `tx.valid` | stage 2 | stage 3 (`scorers`) | `data` (byte-identical event), `validate_ts`, `validator_id`, `producer_id`, `seq` |
| `tx.scored` | stage 3 | stage 4 (`explainers`) | `data` (scored event), `event_id`, `scored_ts`, `scorer_id`, `producer_id`, `seq` |
| `tx.explained` | stage 4 | **stage 5 (to build)** | `data` (scored event with `explanation` filled), `event_id`, `explained_ts`, `explainer_id`, `scored_ts`, `producer_id`, `seq` |
| `tx.dlq` | stages 2, 3, 4 | dashboard / stage 5 | `data` (raw payload), `reason`, `event_id`, `path`, `message`, `errors`, `dlq_ts`, `stage`, plus `validator_id` (stage 2) or `worker_id` (stages 3–4) |

DLQ `stage` values: `validation` (stage 2), `scoring` (stage 3: reasons `MALFORMED_JSON`, `SCORING_ERROR`),
`explanation` (stage 4: reason `MALFORMED_JSON`). In practice only stage 2 dead-letters anything, because stage 3 only
sees events that already passed validation.

**Live metrics for the dashboard (Redis hashes):**
- `metrics:validator`: Albert's.
- `metrics:scorer`: `scored, allow, review, block, duplicates, dlq, events_per_sec, p50_ms, p95_ms, cards_tracked, model_version, consumer, updated_ms`.
- `metrics:explainer`: `explained, ok, skipped, failed, timeout, ai_busy_used_template, dlq, ai_waiting, backend, alert_p50_ms, alert_p95_ms, consumer, updated_ms`.

---

## 2. Data contracts

- **Source of truth:** `Albert/New stuff/event.schema.json`, `scored_event.schema.json`, `schema.sql` (v1.0.0 plus the
  optional `scoring.reasons` field and the `scoring_reasons` column).
- `Albert/Formatting/` is an **older copy without `reasons`**. Nothing uses it; recommend deleting it.
- Stage 3 copies the original event into the scored event **byte-for-byte** (it pastes the received JSON text rather than
  re-serialising it). Stage 4 keeps that text and only replaces the trailing `"explanation": null`.
- `scoring.reasons` (REVIEW/BLOCK only, up to 3 items):
  `{"feature": "amount", "contribution": 3.1, "value": 912.4, "detail": "amount $912.40 is 14.2x this card's usual $64"}`.
  `contribution` is XGBoost's per-feature contribution toward fraud (SHAP-style; positive means more fraud-like).
- `model_scores` keys: `logistic_regression`, `random_forest`, `xgboost`.
- `decision`: `ALLOW` < REVIEW cut-off ≤ `REVIEW` < BLOCK cut-off ≤ `BLOCK`. Only the REVIEW cut-off is stored, in `scoring.threshold`.
- The producer's mapping and the scorer agree (tested on 20,000 rows, 0 mismatches):
  `card_id = sha256(cc_num digits)[:16]`, `occurred_at` = dataset clock time + `Z` (no time-zone shift), `unix_time`
  derived from `occurred_at`. The scorer never reads `label`.

---

## 3. Stage 3: scoring (`Dhruv/stage3_scoring/`)

**Entry point:** `stream_worker.py` (container `fraud-scorer`). `run.py` does the same over stdin/stdout for local tests.

- **Features** (`features.py`, 27 numbers): amount, log amount, hour, is-night (22:00–04:00), age, gender, log city
  population, home→merchant distance, time since the card's last payment, the card's payment count, amount ÷ the card's
  average, the card's payment count and spend in the last 24 h, plus 14 category flags. Per-card history lives in
  `CardMemory` **in the worker's memory**. The same code is used for training (replaying history) and live scoring.
- **Models** (`train.py`): logistic regression, random forest, XGBoost, trained on fraudTrain (fit on the first 85%,
  cut-offs and weights picked on the last 15%), graded once on fraudTest.
  **Ensemble = 0.1·LR + 0.1·RF + 0.8·XGBoost** (weights searched on validation, each model kept ≥ 0.1 so it stays a
  multi-model ensemble). The REVIEW cut-off is the best F1 on validation; the BLOCK cut-off is the lowest score with
  ≥ 99% precision on validation.
- **Batch scoring:** the worker scores whatever is waiting (up to 100) in one call. Results are identical to scoring one
  at a time. Throughput on a Mac: ~245 events/s one at a time vs ~9,600 events/s in batches of 50.
  `scoring_duration_ms` = feature build + inference for the event's batch.
- **Duplicates:** the last 500,000 `event_id`s are remembered (`dedupe.py`); repeats are skipped (counted in
  `metrics:scorer.duplicates`) so card history isn't double-counted.
- **Warm-up:** at start it loads card history from `fraudTrain.csv` (~14 s on LinuxONE). **So the producer must replay
  `fraudTest.csv`.** Replaying fraudTrain would score data the models trained on and double-count history; the worker
  logs a warning if events are older than its warm-up history.
- **Run exactly one scorer** (card history is per-process). It warns if it sees another active consumer in `scorers`.
- **Restart behaviour:** it resumes its own unacked entries (fixed consumer name `scorer-1`), but card history is rebuilt
  from the warm-up file only, so payments scored before the restart are forgotten. **Restart the scorer before every
  fresh replay** (`docker restart scorer`), otherwise a second replay of the same rows double-counts card history.
- **Docker:** `debian:trixie-slim` + Debian's own s390x packages (python3-numpy/pandas/sklearn/xgboost/joblib/redis),
  because PyPI has no reliable s390x builds for them. Models are **not** baked in: they're trained inside the image on
  the VM into the `fraud-models` volume. `Dhruv/stage3_scoring/models/` in git holds **Mac-trained** models
  (scikit-learn 1.6.1) for local runs only.

| Models | REVIEW cut-off | BLOCK cut-off | Where |
| --- | --- | --- | --- |
| Mac-trained (in git) | 0.174 | 0.606 | local runs |
| VM-trained (`fraud-models` volume) | 0.232 | 0.720 | LinuxONE (XGBoost 3.0, sklearn 1.4.2) |

---

## 4. Stage 4: explanation (`Dhruv/stage4_explanation/`)

**Entry point:** `stream_worker.py` (container `fraud-explainer`; only depends on `redis`, which is pure Python).

- ALLOW → `explanation: {"status": "skipped", ...}`. REVIEW/BLOCK → one sentence (`status: "ok"`).
- **Backends** (`BACKEND` env var):
  - `template` (default, instant): fills a sentence from `scoring.reasons`.
    Example: *"BLOCK: $780.52 at Rodriguez, Yost and Jenkins (misc online), cardholder in Benton, WI. Why: amount $780.52 is 13.0x this card's usual $60; …"*
  - `local`: an IBM Granite 3.3 2B model served by llama.cpp `llama-server` (`LLAMA_URL`). Measured **on a Mac only**:
    ~2.3 s per sentence with the GPU, ~5 s CPU-only. Not yet run on LinuxONE (that needs a big-endian GGUF; see the stage 4 README).
  - `watsonx`: watsonx.ai text generation (`WATSONX_APIKEY`, `WATSONX_PROJECT_ID`). **Written but never run:** the
    team couldn't get watsonx access. The team chose watsonx.ai over watsonx Assistant (a chatbot builder), since this
    stage is "facts in, one sentence out".
- **Safety nets:** if the AI is slower than `TIMEOUT_MS` (default 800) or errors, or **its sentence contains a number
  that isn't in the facts** (a small-model habit, e.g. "130x" for "13.0x"), the event gets the template sentence with
  `status: "timeout"` or `"failed"` and `model: "template-fallback"`. A row is always written.
- **Doesn't slow the pipeline:** ALLOW and template events pass straight through; AI calls run on background threads
  (`--ai-threads 2`). If more than `--max-ai-waiting 8` alerts are already waiting, new alerts get the template
  immediately (`ai_busy_used_template`).
- Stateless, so you can run several (each with its own `--consumer`); a restarted worker takes over a dead worker's entries.
- The AI is never shown `label`, `card_id` or `card_last4`.

---

## 5. Stage 5: NOT BUILT (the main gap)

What it needs to do, to fit everything above:
1. Consume `tx.explained` with its own consumer group (at-least-once: write to Postgres, then XACK).
2. Map each `data` JSON to a row with **`to_row()` in `Albert/New stuff/stubgen.py`** (it already maps
   `scoring.reasons` → `scoring_reasons`). Insert with **`ON CONFLICT (event_id) DO NOTHING`**.
   `committed_at` (DB clock) − `emitted_at` = the headline end-to-end latency (the generated `latency_ms` column).
3. Consume `tx.dlq` into `dead_letter_events` (`reason`→`reason_code`, `message`→`reason_detail`, `data`→`raw_payload`,
   `dlq_ts`→`rejected_at`, plus `stage`, `event_id`, `run_id`).
4. Dashboard: the SQL views in `schema.sql`, plus the three `metrics:*` Redis hashes for live per-stage numbers.
5. Note: `stubgen.py` needs Python ≥ 3.10 (it uses `X | Y` type syntax).

---

## 6. LinuxONE: what's deployed right now

- **VM:** LinuxONE Community Cloud, image `Ubuntu24.04-Datathon`, flavor `AI_LAB` (3 cores, 8 GB, 50 GB), user `linux1`.
  `uname -m` = **s390x**, machine type 9175 (IBM z17, to our knowledge). **Ask Dhruv for the IP and SSH key privately.
  Never put them in the repo, which is public.**
- Docker 29.1.3 (Ubuntu's `docker.io`). Repo cloned at `~/fraud` (at commit `346d3b4`; `git pull` for later files).
  Datasets in `~/fraud/Datasets/` (fraudTrain, fraudTest, plus `sorted_test.csv` made by the producer's `prepare`).
- **Running containers** on Docker network `fraud-net`: `redis` (redis:8, no persistence), `validator` (`val-1`),
  `validator2` (`val-2`), `scorer`, `explainer` (template backend). Volume `fraud-models` holds the VM-trained models.
- **Demo:** `~/demo.sh RATE EVENTS` (e.g. `~/demo.sh 1000 60000`) restarts the scorer, replays fraudTest with 1% broken
  events, waits for the pipeline to drain, and prints the results via `~/measure.py`. Copies are in `Dhruv/linuxone/`.
- Full build/run commands: `Dhruv/README.md`.

---

## 7. Measured on LinuxONE (details: `Dhruv/LINUXONE_RESULTS.md`)

"End to end" = producer `emitted_at` → stage 4 output (stage 5 not included yet).

| Target rate | Events | Lost / written twice | Sustained | End-to-end p50 / p95 / p99 | Broken → DLQ |
| --- | --- | --- | --- | --- | --- |
| 1,000/s | 60,000 | 0 / 0 | 1,024/s | **48 / 68** / 456 ms | 595 / 595 |
| 3,000/s | 90,000 | 0 / 0 | 3,175/s | 81 / 1,378 / 1,557 ms | 882 / 882 |
| 5,000/s (2 validators) | 150,000 | 0 / 0 | 4,944/s | 1,746 / 2,220 / 2,496 ms | 1,487 / 1,487 |

- Per-stage p50 at 1,000/s: validate 16 ms, score 26 ms, explain 6 ms. The scorer's own batch time is 5–6 ms at every rate.
- **At 3,000/s the single validator was the slowest stage** (p95 1,260 ms). A second validator was added for the 5,000/s run.
  ~3,000–5,000/s is where this 3-core VM saturates; beyond that events queue but none are lost.
- Model quality (VM-trained, fraudTest): ensemble PR-AUC **0.960**, precision 0.950, recall 0.902.
  Live on the stream: 90% of fraud caught, 93–95% of alerts real.
- The 3,000/s run's **accuracy** numbers are invalid (it replayed rows the scorer had already seen without a restart);
  its speed numbers are fine.

---

## 8. Decisions and open items for Albert

| # | Item | Status / proposal |
| --- | --- | --- |
| 1 | New stream names `tx.scored`, `tx.explained` | Used by stages 3–4; please confirm |
| 2 | Producer replays `fraudTest.csv` | Needed (`prepare --input …/fraudTest.csv`); stages 1–2 docs still say fraudTrain |
| 3 | Stage 5 ignores repeated `event_id` | Required by at-least-once; `ON CONFLICT DO NOTHING` |
| 4 | Stage 4 timeout for the local LLM | Contract says 800 ms; the local model needs ~10 s (`TIMEOUT_MS=10000`). Agree before using it |
| 5 | Delete `Albert/Formatting/` (old contract) | Avoids building against the wrong copy |
| 6 | Validator throughput | ~3,000/s per validator on this VM; run 2 for higher rates |
| 7 | Who builds stage 5 + dashboard | Not started |
| 8 | watsonx access | Not available; template is the default, local Granite is the backup |
| 9 | Card history across scorer restarts | Lost on restart (fine for the demo); production fix = keep `CardMemory` in Redis |

## 9. Where to look

| Need | File |
| --- | --- |
| Event/scored/table shapes | `Albert/New stuff/*.schema.json`, `schema.sql` |
| Stage 1/2 behaviour, audit | `Albert/stage1_producer/README.md`, `Albert/stage2_validator/README.md`, `Albert/audit_stage12/REPORT.md` |
| Stage 3 details, Docker, rules | `Dhruv/stage3_scoring/README.md` |
| Stage 4 backends, LLM setup, Docker | `Dhruv/stage4_explanation/README.md` |
| Full LinuxONE run commands | `Dhruv/README.md` |
| LinuxONE numbers | `Dhruv/LINUXONE_RESULTS.md` |
| Original plan, rubric, rules | `IBM-Z-Datathon-2026-Team-Context-Handoff.md` |

## 10. Hard rules still in force
All code is written during Oct 17–18. Link every open-source resource in the submission: Sparkov dataset (Kaggle,
kartik2112), scikit-learn, XGBoost, pandas, NumPy, joblib, redis-py, jsonschema, Redis, llama.cpp, IBM Granite,
Debian packages. No secrets in the public repo.
