"""Stage-5-inclusive results for one producer run (--run-id), read from Postgres
and checked against the Redis streams.

Runs inside the fraud-storer image (it has python3, redis and psycopg2):
  docker run --rm --network fraud-net --env-file ~/fraud/.env \
    -v ~/fraud/Albert/linuxone/measure_db.py:/m.py:ro --entrypoint python3 fraud-storer /m.py --run-id RUN

"End to end" here = producer emitted_at -> Postgres commit (latency_ms, computed by
the database). Model quality reads the dataset label: that is evaluation, never
an input to scoring. --assert exits 1 if any pipeline check fails.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

import psycopg2
import redis

ap = argparse.ArgumentParser()
ap.add_argument("--run-id", required=True)
ap.add_argument("--assert", dest="check", action="store_true", help="exit 1 if a check fails")
a = ap.parse_args()
r = redis.Redis.from_url(os.environ.get("REDIS_URL", "redis://redis:6379/0"))
db = psycopg2.connect("")


def page(stream: str):
    start, first = "-", True
    while True:
        p = r.xrange(stream, min=start, count=2000)
        if not first:
            p = p[1:]
        if not p:
            return
        yield from p
        start, first = p[-1][0], False


def run_of(raw: bytes):
    try:
        doc = json.loads(raw)
        return doc.get("run_id") or (doc.get("event") or {}).get("run_id")
    except Exception:  # noqa: BLE001
        return None


def q(sql: str, *params):
    with db.cursor() as c:
        c.execute(sql, params)
        return c.fetchall()


# ---- what the streams say -------------------------------------------------
produced = injected = 0
for _, f in page("tx.raw"):
    if run_of(f[b"data"]) == a.run_id:
        produced += 1
        injected += b"malformed" in f
valid_ids = set()
for _, f in page("tx.valid"):
    if run_of(f[b"data"]) == a.run_id:
        valid_ids.add(json.loads(f[b"data"])["event_id"].encode())
valid = len(valid_ids)
explained_entries, explained_ids = 0, set()
for _, f in page("tx.explained"):
    if run_of(f[b"data"]) == a.run_id:
        explained_entries += 1
        explained_ids.add(f[b"event_id"])
dlq_stream = sum(1 for _, f in page("tx.dlq")
                 if (f.get(b"run_id", b"").decode() or run_of(f.get(b"data", b"{}"))) == a.run_id)

# ---- what Postgres says ---------------------------------------------------
(rows, distinct, t0, t1, alerts, alerts_explained, alerts_reasons), = q("""
    SELECT count(*), count(DISTINCT event_id), min(emitted_at), max(committed_at),
           count(*) FILTER (WHERE decision <> 'ALLOW'),
           count(*) FILTER (WHERE decision <> 'ALLOW' AND explanation_text IS NOT NULL),
           count(*) FILTER (WHERE decision <> 'ALLOW' AND scoring_reasons IS NOT NULL
                            AND jsonb_array_length(scoring_reasons) > 0)
    FROM scored_transactions WHERE run_id = %s""", a.run_id)
(n_lat, p50, p95, p99, pmax), = q("""
    SELECT count(latency_ms),
           PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY latency_ms),
           PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms),
           PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency_ms), max(latency_ms)
    FROM scored_transactions WHERE run_id = %s""", a.run_id)
decisions = dict(q("SELECT decision, count(*) FROM scored_transactions WHERE run_id = %s GROUP BY 1 ORDER BY 1",
                   a.run_id))
statuses = dict(q("SELECT coalesce(explanation_status, 'null'), count(*) FROM scored_transactions "
                  "WHERE run_id = %s GROUP BY 1 ORDER BY 1", a.run_id))
(tp, fp, fn), = q("""
    SELECT count(*) FILTER (WHERE decision <> 'ALLOW' AND is_fraud = 1),
           count(*) FILTER (WHERE decision <> 'ALLOW' AND is_fraud = 0),
           count(*) FILTER (WHERE decision = 'ALLOW' AND is_fraud = 1)
    FROM scored_transactions WHERE run_id = %s""", a.run_id)
dlq_rows = q("SELECT stage, reason_code, count(*) FROM dead_letter_events WHERE run_id = %s "
             "GROUP BY 1, 2 ORDER BY 3 DESC", a.run_id)
dlq_db = sum(n for *_, n in dlq_rows)
span = (t1 - t0).total_seconds() if rows else float("nan")

# ---- report ----------------------------------------------------------------
print(f"run {a.run_id}")
print(f"produced {produced:,} (deliberately broken {injected}) | valid {valid:,} | "
      f"tx.explained entries {explained_entries:,} ({len(explained_ids):,} distinct event_ids)")
print(f"STORED in Postgres: {rows:,} rows ({distinct:,} distinct) | lost {valid - distinct} | "
      f"duplicate rows {rows - distinct} | upstream repeats ignored {explained_entries - len(explained_ids)}")
print(f"DLQ: stored {dlq_db} | in tx.dlq {dlq_stream} | injected {injected} | by stage/reason {dlq_rows}")
print(f"sustained throughput (emit of first -> commit of last): {rows / span:,.0f} rows/s "
      f"({rows:,} rows in {span:.1f} s)")
print(f"END-TO-END latency (producer emitted_at -> Postgres commit, latency_ms): "
      f"p50 {p50:.1f} ms | p95 {p95:.1f} ms | p99 {p99:.1f} ms | max {pmax:.0f} ms (n={n_lat:,})")
print(f"decisions {decisions} | explanation_status {statuses}")
print(f"REVIEW/BLOCK rows: {alerts:,} | with explanation {alerts_explained:,} | with scoring_reasons {alerts_reasons:,}")
print(f"model quality on the stream (REVIEW+BLOCK vs label): recall {tp}/{tp + fn} "
      f"({tp / max(tp + fn, 1):.1%}) | precision {tp}/{tp + fp} ({tp / max(tp + fp, 1):.1%})")

checks = {
    "every valid event stored once": distinct == valid == len(explained_ids) and valid_ids == explained_ids,
    "no duplicate rows": rows == distinct,
    "DLQ stored == injected == tx.dlq": dlq_db == injected == dlq_stream,
    "every alert explained": alerts_explained == alerts,
    "every alert has scoring_reasons": alerts_reasons == alerts,
    "latency for every row": n_lat == rows,
}
for name, ok in checks.items():
    print(f"CHECK {'PASS' if ok else 'FAIL'}: {name}")
sys.exit(1 if a.check and not all(checks.values()) else 0)
