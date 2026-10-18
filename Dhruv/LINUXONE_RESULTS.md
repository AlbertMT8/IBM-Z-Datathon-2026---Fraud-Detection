# Pipeline results on IBM LinuxONE (Oct 18, 2026)

## The machine
- LinuxONE Community Cloud VM `best-betas`, image **Ubuntu24.04-Datathon**, flavor **AI_LAB**: 3 cores, 8 GB, 50 GB.
- `uname -m` → **s390x**, machine type **9175** (IBM z17, to our knowledge). Inside the scorer container, Python reports byte order **big**.
- Docker 29.1.3 (s390x). Six containers on one Docker network: `redis:8`, `fraud-validator` ×2, `fraud-scorer`, `fraud-explainer`, plus `fraud-producer` while replaying.
- Every image was **built on the VM**. Stage 3's models were **trained on the VM** inside the scorer image
  (Python 3.13.5, scikit-learn 1.4.2, XGBoost 3.0.0, from Debian's s390x packages).

## Model quality (trained on LinuxONE, graded on fraudTest: 555,719 payments never seen, 2,145 fraud)
| | PR-AUC | Precision at alert cut-off | Recall at alert cut-off |
| --- | --- | --- | --- |
| Ensemble (0.1 LR + 0.1 RF + 0.8 XGBoost) | **0.960** | **0.950** | **0.902** |
| XGBoost alone | 0.965 | 0.949 | 0.905 |
| Random forest | 0.916 | 0.816 | 0.876 |
| Logistic regression | 0.517 | 0.783 | 0.438 |

Training all three models took about 1.5 minutes on the VM. The alert (REVIEW) cut-off is 0.232 and the BLOCK cut-off is 0.720, both chosen on validation data.

## Live pipeline (Albert's producer replaying fraudTest.csv, 1% deliberately broken events)
"End to end" = producer emits the event → stage 4 writes the explained event to `tx.explained`.
(Stage 5, the Postgres writer, isn't built yet, so its write time isn't included.)

| Target rate | Events | Lost | Written twice | Sustained | End-to-end p50 | p95 | p99 | Broken → DLQ |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1,000/s | 60,000 | 0 | 0 | **1,024/s** | **48 ms** | **68 ms** | 456 ms | 595 / 595 |
| 3,000/s | 90,000 | 0 | 0 | 3,175/s | 81 ms | 1,378 ms | 1,557 ms | 882 / 882 |
| 5,000/s (2 validators) | 150,000 | 0 | 0 | 4,944/s | 1,746 ms | 2,220 ms | 2,496 ms | 1,487 / 1,487 |

Delay by stage (p50 / p95):

| Rate | Stage 2 validate | Stage 3 score | Stage 4 explain |
| --- | --- | --- | --- |
| 1,000/s | 16 / 29 ms | 26 / 48 ms | 6 / 9 ms |
| 3,000/s | 34 / 1,260 ms | 48 / 123 ms | 6 / 11 ms |
| 5,000/s | 690 / 1,671 ms | 576 / 1,375 ms | 13 / 198 ms |

The scorer's own batch scoring time stayed at **5–6 ms (p50)** in every run.

Live fraud detection on the replayed stream (fresh scorer):
- at 1,000/s: 90% of fraud caught (226/251), 93% of alerts were real fraud (226/242)
- at 5,000/s: 90% of fraud caught (585/652), 95% of alerts were real fraud (585/617)

## What this means
- **At 1,000 events/s the whole pipeline answers in under 70 ms for 95% of payments**, on 3 cores.
- **It never lost or duplicated an event**, even at 5,000/s and with 1% broken input.
- **About 3,000–5,000 events/s is where this 3-core VM saturates.** Above that, events queue and delay grows to seconds,
  but nothing is dropped. At 3,000/s the single validator was the slow step; a second validator helped.
  All six containers share the same 3 cores.

## Caveats (say these if asked)
- The 3,000/s run's accuracy numbers are **not valid**: it replayed the same payments as the 1,000/s run without
  restarting the scorer, so card history counted them twice. Its throughput and latency numbers are fine.
  Always `docker restart scorer` before a fresh replay.
- Synthetic data (Sparkov): fraud patterns are cleaner than real life, so accuracy is higher than on real cards.
- Explanations here used the **template** backend. The local Granite model was measured separately on a Mac
  (2–5 s per sentence). It hasn't been run on LinuxONE yet.
