# Stage 5 on LinuxONE: results (Oct 18, 2026)

The whole pipeline, stages 1–5, on the LinuxONE Community Cloud VM (3 cores, 8 GB; `uname -m` = `s390x`; Python
in the stage 5 containers reports byte order **big**). Every image was built on the VM. Nine containers share the
3 cores: `redis`, `validator` ×2, `scorer`, `explainer`, `postgres`, `storer`, `dlq-storer`, `dashboard` (plus the
producer while replaying). Each run: `Albert/linuxone/demo5.sh RATE EVENTS` (empties the stage 5 tables, runs
`Dhruv/linuxone/demo.sh` unchanged, which restarts the scorer and replays `fraudTest.csv` with 1% broken events, waits
for stage 5 to drain, then runs `Albert/linuxone/measure_db.py`). VM-trained models, template explainer.

"End to end" = producer `emitted_at` → the row's Postgres commit (`latency_ms`, computed by the database).

| Target rate | Events | Stored / valid | Lost | Duplicate rows | Sustained | p50 | p95 | p99 | Broken → DLQ (stored) | Checks |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| 1,000/s | 60,000 | 59,376 / 59,376 | 0 | 0 | **1,023 rows/s** | **36 ms** | **105 ms** | 476 ms | 624 / 624 | 6/6 PASS |
| 5,000/s (2 validators) | 150,000 | 148,559 / 148,559 | 0 | 0 | 4,141 rows/s | 6,278 ms | 8,846 ms | 9,058 ms | 1,441 / 1,441 | 6/6 PASS |

Same runs, measured to stage 4 (by `measure.py`) vs to Postgres (by `measure_db.py`):

| Rate | to stage 4: p50 / p95 / p99 | to Postgres: p50 / p95 / p99 | Stage 5 adds (p50) |
| --- | --- | --- | --- |
| 1,000/s | 27 / 44 / 325 ms | 36 / 105 / 476 ms | ~9 ms |
| 5,000/s | 5,911 / 8,794 / 8,984 ms | 6,278 / 8,846 / 9,058 ms | ~370 ms |

- **At 1,000/s the whole pipeline, including the Postgres commit, answers in 105 ms for 95% of payments.** Nothing
  lost, nothing stored twice, every broken event in `dead_letter_events`, every alert stored with its explanation
  and reasons.
- **5,000/s is past this VM's capacity with all nine containers.** Events queue in stages 2 and 3 (validate p50
  3.1 s, score p50 2.8 s) and the delay grows to seconds, but still nothing is lost or duplicated. This is slower
  than Dhruv's earlier 5,000/s run without stage 5 (4,944/s, p50 1,746 ms, `Dhruv/LINUXONE_RESULTS.md`), because
  Postgres and the storers now share the same 3 cores.
- **Stage 5 alone keeps up with more than the pipeline produces.** On first start the storer imported the 297,036
  explained events already in `tx.explained` from earlier runs at about 6,000–6,600 rows/s (`metrics:storer`
  `rows_per_sec`), in about 45 s, with 0 duplicates. The per-run row counts it imported (59,405, 89,118 and
  148,513) match `Dhruv/LINUXONE_RESULTS.md` exactly.
- Model quality on the stream (REVIEW+BLOCK vs the label, evaluation only): 1,000/s recall 89.5% (221/247),
  precision 93.6% (221/236); 5,000/s recall 90.1% (591/656), precision 94.0% (591/629).
- The dashboard on the VM (`127.0.0.1:8080` only, via SSH tunnel) returned every panel with live data and no errors.

## Raw output

IP addresses and the VM hostname are replaced with `<ip>` / `<vm>`.

```
## uname -m
s390x

## docker ps
dashboard (fraud-dashboard:latest) Up 52 seconds
dlq-storer (fraud-storer:latest) Up 53 seconds
storer (fraud-storer:latest) Up 53 seconds
postgres (postgres:17.11-trixie) Up 56 seconds
validator2 (fraud-validator) Up 4 hours
explainer (fraud-explainer) Up 4 hours
scorer (fraud-scorer) Up 4 hours
validator (fraud-validator) Up 4 hours
redis (redis:8) Up 4 hours

## demo5.sh 1000 60000
== emptying stage 5 tables
== machine: s390x, 3 cores | containers:
dashboard (fraud-dashboard:latest) Up 53 seconds
dlq-storer (fraud-storer:latest) Up 53 seconds
storer (fraud-storer:latest) Up 53 seconds
postgres (postgres:17.11-trixie) Up 56 seconds
validator2 (fraud-validator) Up 4 hours
explainer (fraud-explainer) Up 4 hours
scorer (fraud-scorer) Up 4 hours
validator (fraud-validator) Up 4 hours
redis (redis:8) Up 4 hours
== restarting scorer for fresh card memory...
== replaying 60000 payments at 1000/s (run id demo-1000eps-043204)
"emitted": 60000, "malformed": 624
run demo-1000eps-043204: produced 60,000 (deliberately broken 624) | dead-lettered 624 | valid 59,376
explained 59,376 of 59,376 valid | lost 0 | written twice 0
sustained throughput: 1,023 events/s end to end (59,376 events in 58.0s)
END-TO-END latency (producer emit -> stage 4 output): p50 27.0 ms | p95 44.0 ms | p99 325.0 ms | max 505 ms
   stage 2 validate: p50    9.0 ms | p95   18.0 ms
   stage 3 score   : p50   16.0 ms | p95   30.0 ms
   stage 4 explain : p50    3.0 ms | p95    7.0 ms
   scorer's own scoring_duration_ms (per batch): p50 3.95 | p95 5.34
decisions {'ALLOW': 59140, 'REVIEW': 39, 'BLOCK': 197} | explanations {'skipped': 59140, 'ok': 236}
fraud caught 221/247 (89%) | real fraud among alerts 221/236 (94%)
== waiting for stage 5 (run demo-1000eps-043204): storer pending 0 and row counts stable
   stored rows/dlq rows: 59376/624
run demo-1000eps-043204
produced 60,000 (deliberately broken 624) | valid 59,376 | tx.explained entries 59,376 (59,376 distinct event_ids)
STORED in Postgres: 59,376 rows (59,376 distinct) | lost 0 | duplicate rows 0 | upstream repeats ignored 0
DLQ: stored 624 | in tx.dlq 624 | injected 624 | by stage/reason [('validation', 'SCHEMA_VIOLATION', 156), ('validation', 'RANGE_ERROR', 156), ('validation', 'TYPE_ERROR', 156), ('validation', 'MISSING_REQUIRED', 156)]
sustained throughput (emit of first -> commit of last): 1,023 rows/s (59,376 rows in 58.0 s)
END-TO-END latency (producer emitted_at -> Postgres commit, latency_ms): p50 36.4 ms | p95 104.6 ms | p99 476.3 ms | max 526 ms (n=59,376)
decisions {'ALLOW': 59140, 'BLOCK': 197, 'REVIEW': 39} | explanation_status {'ok': 236, 'skipped': 59140}
REVIEW/BLOCK rows: 236 | with explanation 236 | with scoring_reasons 236
model quality on the stream (REVIEW+BLOCK vs label): recall 221/247 (89.5%) | precision 221/236 (93.6%)
CHECK PASS: every valid event stored once
CHECK PASS: no duplicate rows
CHECK PASS: DLQ stored == injected == tx.dlq
CHECK PASS: every alert explained
CHECK PASS: every alert has scoring_reasons
CHECK PASS: latency for every row

## demo5.sh 5000 150000
== emptying stage 5 tables
== machine: s390x, 3 cores | containers:
dashboard (fraud-dashboard:latest) Up 3 minutes
dlq-storer (fraud-storer:latest) Up 3 minutes
storer (fraud-storer:latest) Up 3 minutes
postgres (postgres:17.11-trixie) Up 3 minutes
validator2 (fraud-validator) Up 4 hours
explainer (fraud-explainer) Up 4 hours
scorer (fraud-scorer) Up 2 minutes
validator (fraud-validator) Up 4 hours
redis (redis:8) Up 4 hours
== restarting scorer for fresh card memory...
== replaying 150000 payments at 5000/s (run id demo-5000eps-043431)
"emitted": 150000, "malformed": 1441
run demo-5000eps-043431: produced 150,000 (deliberately broken 1441) | dead-lettered 1441 | valid 148,559
explained 148,559 of 148,559 valid | lost 0 | written twice 0
sustained throughput: 4,148 events/s end to end (148,559 events in 35.8s)
END-TO-END latency (producer emit -> stage 4 output): p50 5911.0 ms | p95 8794.0 ms | p99 8984.0 ms | max 9038 ms
   stage 2 validate: p50 3116.0 ms | p95 3791.0 ms
   stage 3 score   : p50 2780.0 ms | p95 5219.0 ms
   stage 4 explain : p50   10.0 ms | p95   74.0 ms
   scorer's own scoring_duration_ms (per batch): p50 7.71 | p95 18.19
decisions {'ALLOW': 147930, 'REVIEW': 116, 'BLOCK': 513} | explanations {'skipped': 147930, 'ok': 629}
fraud caught 591/656 (90%) | real fraud among alerts 591/629 (94%)
== waiting for stage 5 (run demo-5000eps-043431): storer pending 0 and row counts stable
   stored rows/dlq rows: 148559/1441
run demo-5000eps-043431
produced 150,000 (deliberately broken 1441) | valid 148,559 | tx.explained entries 148,559 (148,559 distinct event_ids)
STORED in Postgres: 148,559 rows (148,559 distinct) | lost 0 | duplicate rows 0 | upstream repeats ignored 0
DLQ: stored 1441 | in tx.dlq 1441 | injected 1441 | by stage/reason [('validation', 'MISSING_REQUIRED', 361), ('validation', 'SCHEMA_VIOLATION', 360), ('validation', 'RANGE_ERROR', 360), ('validation', 'TYPE_ERROR', 360)]
sustained throughput (emit of first -> commit of last): 4,141 rows/s (148,559 rows in 35.9 s)
END-TO-END latency (producer emitted_at -> Postgres commit, latency_ms): p50 6277.6 ms | p95 8846.4 ms | p99 9057.5 ms | max 9110 ms (n=148,559)
decisions {'ALLOW': 147930, 'BLOCK': 513, 'REVIEW': 116} | explanation_status {'ok': 629, 'skipped': 147930}
REVIEW/BLOCK rows: 629 | with explanation 629 | with scoring_reasons 629
model quality on the stream (REVIEW+BLOCK vs label): recall 591/656 (90.1%) | precision 591/629 (94.0%)
CHECK PASS: every valid event stored once
CHECK PASS: no duplicate rows
CHECK PASS: DLQ stored == injected == tx.dlq
CHECK PASS: every alert explained
CHECK PASS: every alert has scoring_reasons
CHECK PASS: latency for every row
```
