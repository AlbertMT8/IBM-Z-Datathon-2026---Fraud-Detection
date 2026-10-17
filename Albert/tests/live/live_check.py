"""Live Step-2 harness: real producer + real validator processes against a real Redis.

usage: live_check.py MODE [--project DIR] [--rate R] [--events N]
MODE: basic | kill-same | kill-new | two | throughput | backlog
Exit code 0 iff every assertion for the mode passes.
"""
from __future__ import annotations

import argparse, json, os, random, signal, statistics, subprocess, sys, time
from datetime import datetime
from pathlib import Path

import redis

S = Path(os.environ.get("LIVE_DIR", "live")).resolve()  # holds sorted.csv; logs land here
URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
r = redis.Redis.from_url(URL)  # bytes mode: compare raw bytes


def pct(vals, p):
    s = sorted(vals)
    k = max(0, -(-len(s) * p // 100) - 1)  # nearest rank
    return s[int(k)]


def start_validator(proj, cid, extra=()):
    log = open(S / f"validator_{cid}_{time.time_ns()}.log", "w")
    prof = ["-m", "cProfile", "-o", str(S / f"prof_{cid}.out")] if os.environ.get("PROFILE") else []
    return subprocess.Popen([sys.executable, *prof, str(proj / "stage2_validator/validator.py"), "--consumer-id", cid,
                             "--log-interval", "2", *extra], stdout=subprocess.PIPE, stderr=log, text=True)


def start_producer(proj, rate, n, malformed):
    log = open(S / f"producer_{time.time_ns()}.log", "w")
    return subprocess.Popen([sys.executable, str(proj / "stage1_producer/producer.py"), "run", "--input", str(S / "sorted.csv"),
                             "--rate", str(rate), "--max-events", str(n), "--malformed-rate", str(malformed),
                             "--state-file", str(S / ".producer_state.json"), "--seed", "99", "--log-interval", "2"],
                            stdout=subprocess.PIPE, stderr=log, text=True)


def last_json(text):
    for line in reversed(text.strip().splitlines()):
        try:
            return json.loads(line)
        except ValueError:
            pass
    return None


def stop(p, sig=signal.SIGTERM, timeout=30):
    p.send_signal(sig)
    out, _ = p.communicate(timeout=timeout)
    return p.returncode, last_json(out or "")


def pending():
    try:
        return r.xpending("tx.raw", "validators")["pending"]
    except redis.ResponseError:
        return -1


def wait_drained(n_expected_unique, timeout):
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        if r.xlen("tx.valid") + r.xlen("tx.dlq") >= n_expected_unique and pending() == 0:
            return True
        time.sleep(0.25)
    return False


def evaluate(n, producer_summary, rate, allow_dups=False):
    fails = []
    raw = r.xrange("tx.raw", "-", "+")
    valid = r.xrange("tx.valid", "-", "+")
    dlq = r.xrange("tx.dlq", "-", "+")
    print(f"XLEN raw={len(raw)} valid={len(valid)} dlq={len(dlq)} pending={pending()}")

    raw_by_id, tagged, seq_of = {}, set(), {}
    for _, f in raw:
        ev = json.loads(f[b"data"])
        raw_by_id[ev["event_id"]] = f[b"data"]
        seq_of[ev["event_id"]] = int(f[b"seq"])
        if b"malformed" in f:  # v1.0.0: the tag is a stream field, never in the payload
            tagged.add(ev["event_id"])
    if len(raw) != n:
        fails.append(f"tx.raw has {len(raw)} != {n}")

    valid_ids = [json.loads(f[b"data"])["event_id"] for _, f in valid]
    dlq_ids = [f[b"event_id"].decode() for _, f in dlq if b"event_id" in f]
    all_ids = valid_ids + dlq_ids
    dups = len(all_ids) - len(set(all_ids))
    print(f"unique routed={len(set(all_ids))} duplicates={dups} tagged={len(tagged)}")
    if set(all_ids) != set(raw_by_id):
        fails.append(f"LOSS: {len(set(raw_by_id) - set(all_ids))} raw events never routed; "
                     f"{len(set(all_ids) - set(raw_by_id))} unknown")
    if not allow_dups:
        if len(valid) + len(dlq) != n:
            fails.append(f"XLEN(valid)+XLEN(dlq)={len(valid) + len(dlq)} != {n}")
        if dups:
            fails.append(f"{dups} duplicate event_ids")
    if set(dlq_ids) != tagged:
        fails.append(f"DLQ set != tagged set: dlq_only={len(set(dlq_ids) - tagged)} tagged_only={len(tagged - set(dlq_ids))}")
    if set(valid_ids) & tagged:
        fails.append(f"{len(set(valid_ids) & tagged)} tagged events reached tx.valid")
    # DLQ records must carry structured errors
    if dlq and b"errors" not in dlq[0][1]:
        fails.append("DLQ entries carry no structured 'errors' field (all errors w/ JSON paths)")

    sample = random.Random(1).sample(valid, min(1000, len(valid)))
    mism = sum(1 for _, f in sample if raw_by_id[json.loads(f[b"data"])["event_id"]] != f[b"data"])
    print(f"byte-identical sample: {len(sample) - mism}/{len(sample)}")
    if mism:
        fails.append(f"{mism} sampled tx.valid payloads differ from tx.raw")

    # Ordering: within the producer's seq order, unix_time non-decreasing
    evs = sorted((json.loads(f[b"data"]) for _, f in valid), key=lambda e: seq_of[e["event_id"]])
    bad = sum(1 for a, b in zip(evs, evs[1:]) if a["transaction"]["unix_time"] > b["transaction"]["unix_time"])
    seq_lost = sum(1 for _, f in valid if f.get(b"seq") is None)
    if seq_lost:
        fails.append(f"{seq_lost} tx.valid entries lost the producer seq field")
    # And stream order on tx.valid for a single validator
    if bad:
        fails.append(f"{bad} unix_time decreases in tx.valid (seq order)")

    def emit_ms(ev):
        return int(datetime.fromisoformat(ev["emitted_at"].replace("Z", "+00:00")).timestamp() * 1000 + 0.5)

    lat = [int(f[b"validate_ts"]) - emit_ms(json.loads(f[b"data"])) for _, f in valid]
    if lat:
        print(f"emit->validate latency from stream fields: p50={pct(lat, 50)}ms p95={pct(lat, 95)}ms max={max(lat)}ms")

    if producer_summary:
        ach = producer_summary["achieved_rate"]
        print(f"producer achieved_rate={ach}/s target={rate}")
        if rate and not (0.95 * rate <= ach <= 1.05 * rate):
            fails.append(f"producer rate {ach} outside ±5% of {rate}")
    return fails


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("mode")
    ap.add_argument("--project", default=str(Path(__file__).resolve().parents[2]))
    ap.add_argument("--rate", type=float, default=1000)
    ap.add_argument("--events", type=int, default=20000)
    ap.add_argument("--malformed", type=float, default=0.1)
    ap.add_argument("--min-idle-ms", default="30000")
    a = ap.parse_args()
    proj = Path(a.project)
    r.flushdb()
    (S / ".producer_state.json").unlink(missing_ok=True)
    fails = []
    vargs = ("--min-idle-ms", a.min_idle_ms)

    if a.mode == "backlog":
        p = start_producer(proj, 1_000_000, a.events, 0.0)
        rc, ps = stop(p, signal.SIGCONT, timeout=300)  # SIGCONT is a no-op: just wait
        t0 = time.monotonic()
        v = start_validator(proj, "bench", vargs)
        ok = wait_drained(a.events, 600)
        dt = time.monotonic() - t0
        vrc, vs = stop(v)
        print(f"BACKLOG drained={ok} n={a.events} wall={dt:.2f}s wall_rate={a.events / dt:.0f} ev/s "
              f"validator_reported={vs and vs['events_per_sec']} ev/s")
        return 0 if ok else 1

    vs_list = []
    v = start_validator(proj, "val-1", vargs)
    v2 = start_validator(proj, "val-2", vargs) if a.mode == "two" else None
    time.sleep(1.0)
    p = start_producer(proj, a.rate, a.events, a.malformed)
    if a.mode.startswith("kill"):
        time.sleep(a.events / a.rate / 3)
        # Freeze first: Redis keeps serving the blocked XREADGROUP, so entries
        # pile up in this consumer's pending list. Then kill it mid-flight.
        v.send_signal(signal.SIGSTOP); time.sleep(0.5)
        v.send_signal(signal.SIGKILL); v.wait()
        print(f"SIGKILLed validator at XLEN(valid)={r.xlen('tx.valid')} pending={pending()}")
        time.sleep(1.0)
        v = start_validator(proj, "val-1" if a.mode == "kill-same" else "val-1b", vargs)
    pout, _ = p.communicate(timeout=a.events / a.rate * 3 + 60)
    ps = last_json(pout)
    if p.returncode != 0:
        fails.append(f"producer exit {p.returncode}")
    drained = wait_drained(a.events, 90)
    if not drained:
        fails.append(f"not drained within 90s: valid+dlq={r.xlen('tx.valid') + r.xlen('tx.dlq')} pending={pending()}")
    for vv in [v, v2]:
        if vv is None:
            continue
        rc, vs = stop(vv)
        vs_list.append(vs)
        if rc != 0:
            fails.append(f"validator exit {rc}")
    for vs in vs_list:
        print("validator summary:", json.dumps(vs))
    if a.mode == "two":
        per = [vs["validated"] for vs in vs_list if vs]
        print(f"per-validator validated={per}")
    fails += evaluate(a.events, ps, a.rate, allow_dups=a.mode.startswith("kill"))
    if a.mode.startswith("kill") and pending() != 0:
        fails.append(f"pending list not drained: {pending()}")
    print("RESULT:", "PASS" if not fails else "FAIL")
    for f in fails:
        print("  FAIL:", f)
    return 0 if not fails else 1


if __name__ == "__main__":
    sys.exit(main())
