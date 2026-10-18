"""Reads the Redis streams for one producer run (--run-id) and reports what the pipeline achieved.
Runs inside the fraud-explainer image on the fraud-net network (it has python + redis)."""
import argparse
import collections
import json
import os
from datetime import datetime

import redis

ap = argparse.ArgumentParser()
ap.add_argument("--run-id", required=True)
a = ap.parse_args()
r = redis.Redis.from_url(os.environ.get("REDIS_URL", "redis://redis:6379/0"))


def page(stream):
    start, first = "-", True
    while True:
        p = r.xrange(stream, min=start, count=2000)
        if not first:
            p = p[1:]
        if not p:
            return
        yield from p
        start, first = p[-1][0], False


def ms(iso):
    return datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp() * 1000


def pct(v, q):
    v = sorted(v)
    return v[min(len(v) - 1, int(q * len(v)))] if v else float("nan")


def run_of(raw_bytes):
    try:
        return json.loads(raw_bytes).get("run_id")
    except Exception:
        return None


raw_n = injected = 0
emit_first = None
for _, f in page("tx.raw"):
    if run_of(f[b"data"]) == a.run_id:
        raw_n += 1
        injected += b"malformed" in f
dlq = sum(1 for _, f in page("tx.dlq") if f.get(b"run_id", b"").decode() == a.run_id)

valid_ts = {}
for _, f in page("tx.valid"):
    ev = json.loads(f[b"data"])
    if ev.get("run_id") == a.run_id:
        valid_ts[ev["event_id"]] = int(f[b"validate_ts"])
scored_ts, score_ms = {}, []
for _, f in page("tx.scored"):
    if f[b"event_id"].decode() in valid_ts:
        scored_ts[f[b"event_id"].decode()] = int(f[b"scored_ts"])
        score_ms.append(json.loads(f[b"data"])["scoring"]["scoring_duration_ms"])

seen = collections.Counter()
e2e, s2, s3, s4, emits, dones = [], [], [], [], [], []
dec, status = collections.Counter(), collections.Counter()
caught = frauds = alerts = 0
for _, f in page("tx.explained"):
    eid = f[b"event_id"].decode()
    if eid not in valid_ts:
        continue
    seen[eid] += 1
    if seen[eid] > 1:
        continue
    doc = json.loads(f[b"data"])
    emit, done = ms(doc["event"]["emitted_at"]), int(f[b"explained_ts"])
    emits.append(emit)
    dones.append(done)
    e2e.append(done - emit)
    s2.append(valid_ts[eid] - emit)
    s3.append(scored_ts[eid] - valid_ts[eid])
    s4.append(done - scored_ts[eid])
    d = doc["scoring"]["decision"]
    dec[d] += 1
    status[doc["explanation"]["status"]] += 1
    y = doc["event"]["label"]["is_fraud"]
    frauds += y
    if d != "ALLOW":
        alerts += 1
        caught += y

n = len(e2e)
span_s = (max(dones) - min(emits)) / 1000 if n else float("nan")
print(f"run {a.run_id}: produced {raw_n:,} (deliberately broken {injected}) | dead-lettered {dlq} | valid {len(valid_ts):,}")
print(f"explained {n:,} of {len(valid_ts):,} valid | lost {len(valid_ts) - n} | written twice {sum(c - 1 for c in seen.values())}")
print(f"sustained throughput: {n / span_s:,.0f} events/s end to end ({n:,} events in {span_s:.1f}s)")
print(f"END-TO-END latency (producer emit -> stage 4 output): p50 {pct(e2e, .5):.1f} ms | p95 {pct(e2e, .95):.1f} ms | p99 {pct(e2e, .99):.1f} ms | max {max(e2e):.0f} ms")
for name, v in (("stage 2 validate", s2), ("stage 3 score   ", s3), ("stage 4 explain ", s4)):
    print(f"   {name}: p50 {pct(v, .5):6.1f} ms | p95 {pct(v, .95):6.1f} ms")
print(f"   scorer's own scoring_duration_ms (per batch): p50 {pct(score_ms, .5):.2f} | p95 {pct(score_ms, .95):.2f}")
print(f"decisions {dict(dec)} | explanations {dict(status)}")
print(f"fraud caught {caught}/{frauds} ({caught / max(frauds, 1):.0%}) | real fraud among alerts {caught}/{alerts} ({caught / max(alerts, 1):.0%})")
