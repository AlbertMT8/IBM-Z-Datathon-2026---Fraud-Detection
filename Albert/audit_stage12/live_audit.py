"""Live checks for the stage 1/2 audit that tests/live/live_check.py does not cover.

Runs the real producer/validator processes against a real Redis. Each mode
prints its numbers and exits 0 only if its assertions hold.

    REDIS_URL=redis://localhost:6379/0 LIVE_DIR=/path/with/sorted.csv \
        python audit_stage12/live_audit.py MODE

MODE
  emit-gap       1000 ev/s run; how long each event sat in the producer before
                 its XADD reached Redis (stream entry id ms - emitted_at)
  metrics        two validators (distinct names); metrics:validator vs real totals
  two-default    two validators with the DEFAULT consumer name; duplicates?
  xautoclaim     dead-consumer entries behind a busy window are recovered (real Redis)
  fullscan       every row of sorted.csv -> build_event -> contract; nothing invalid
"""
from __future__ import annotations

import json
import os
import signal
import subprocess
import sys
import time
from datetime import datetime
from pathlib import Path

import redis

ALBERT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ALBERT / "stage1_producer"))
sys.path.insert(0, str(ALBERT / "stage2_validator"))
sys.path.insert(0, str(ALBERT / "tests" / "live"))

import live_check as lc  # noqa: E402  (shares REDIS_URL / LIVE_DIR handling)

r = lc.r
S = lc.S


def ms(iso: str) -> int:
    return int(round(datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp() * 1000))


def validator(cid: str | None, *extra: str) -> subprocess.Popen:
    log = open(S / f"audit_validator_{cid or 'default'}_{time.time_ns()}.log", "w")
    args = [sys.executable, str(ALBERT / "stage2_validator/validator.py"), "--log-interval", "2", *extra]
    if cid:
        args += ["--consumer-id", cid]
    env = {k: v for k, v in os.environ.items() if k != "VALIDATOR_ID"}
    return subprocess.Popen(args, stdout=subprocess.PIPE, stderr=log, text=True, env=env)


def run_producer(rate: float, n: int, malformed: float = 0.0) -> dict:
    p = lc.start_producer(ALBERT, rate, n, malformed)
    out, _ = p.communicate(timeout=n / rate * 3 + 60)
    assert p.returncode == 0, p.returncode
    return lc.last_json(out)


def stop_all(*procs: subprocess.Popen) -> list:
    return [lc.stop(p) for p in procs]


def mode_emit_gap() -> list[str]:
    run_producer(1000, 20000)
    gaps = []
    for eid, f in r.xrange("tx.raw", "-", "+"):
        gaps.append(int(eid.split(b"-")[0]) - ms(json.loads(f[b"data"])["emitted_at"]))
    print(f"producer buffering (XADD arrival - emitted_at) over {len(gaps)} events: "
          f"p50={lc.pct(gaps, 50)}ms p95={lc.pct(gaps, 95)}ms max={max(gaps)}ms")
    return [] if lc.pct(gaps, 95) <= 5 else [f"emitted_at precedes the XADD by p95={lc.pct(gaps, 95)}ms"]


def mode_metrics() -> list[str]:
    a, b = validator("m-a", "--metrics-interval", "0.2"), validator("m-b", "--metrics-interval", "0.2")
    time.sleep(1.0)
    run_producer(2000, 10000, 0.1)
    lc.wait_drained(10000, 60)
    time.sleep(0.5)
    stop_all(a, b)
    m = {k.decode(): v.decode() for k, v in r.hgetall("metrics:validator").items()}
    total = r.xlen("tx.valid") + r.xlen("tx.dlq")
    print(f"XLEN valid+dlq={total}  metrics:validator validated={m.get('validated')} "
          f"passed={m.get('passed')} dlq={m.get('dlq')}")
    return [] if int(m.get("validated", -1)) == total else [f"metrics hash says {m.get('validated')} validated, streams hold {total}"]


def mode_two_default() -> list[str]:
    fails = []
    for attempt in range(5):
        r.flushdb()
        a = validator(None, "--count", "50")
        p = lc.start_producer(ALBERT, 2000, 6000, 0.0)
        time.sleep(1.0)
        b = validator(None, "--count", "50")        # joins while a has work in flight
        p.communicate(timeout=60)
        lc.wait_drained(6000, 60)
        stop_all(a, b)
        names = [c["name"].decode() for c in r.xinfo_consumers("tx.raw", "validators")]
        ids = [json.loads(f[b"data"])["event_id"] for _, f in r.xrange("tx.valid", "-", "+")]
        dups = len(ids) - len(set(ids))
        print(f"attempt {attempt}: consumers={names} tx.valid={len(ids)} duplicates={dups}")
        if len(names) != 2:
            fails.append(f"attempt {attempt}: two validators registered as {names}")
        if dups:
            fails.append(f"attempt {attempt}: {dups} duplicate event_ids on tx.valid")
        if fails:
            break
    return fails


def mode_xautoclaim() -> list[str]:
    import validator as val
    for i in range(60):
        r.xadd("tx.raw", {"data": json.dumps({"i": i})})
    r.xgroup_create("tx.raw", "validators", id="0")
    r.xreadgroup("validators", "dead", {"tx.raw": ">"}, count=60)
    time.sleep(0.3)
    busy = [e[0] for e in r.xrange("tx.raw", "-", "+")][:50]
    r.xclaim("tx.raw", "validators", "busy", 0, busy)
    cfg = val.ValidatorConfig(consumer_id="rec", count=4, min_idle_ms=200, once=True,
                              log_interval=1e9, metrics_interval=1e9)
    v = val.Validator(r, cfg)
    n = v._reclaim_pending()
    left = {c["name"].decode(): c["pending"] for c in r.xinfo_consumers("tx.raw", "validators")}
    print(f"reclaimed={n} pending by consumer={left}")
    return [] if n == 10 and left.get("dead", 0) == 0 else [f"dead consumer still holds {left.get('dead')} entries"]


def mode_fullscan() -> list[str]:
    import csv
    import producer as prod
    import validator as val
    v = val.build_schema_validator()
    bad, n, t0 = [], 0, time.monotonic()
    with open(S / "sorted.csv", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            n += 1
            errs = list(v.iter_errors(prod.build_event(row, now_ms=1_792_000_000_000)))
            if errs and len(bad) < 10:
                bad.append((row["trans_num"], [e.message for e in errs]))
    print(f"fullscan: {n} rows, {len(bad)} invalid, {time.monotonic() - t0:.0f}s")
    return [f"{b}" for b in bad]


def main() -> int:
    mode = sys.argv[1]
    r.flushdb()
    (S / ".producer_state.json").unlink(missing_ok=True)
    fails = globals()["mode_" + mode.replace("-", "_")]()
    print("RESULT:", "PASS" if not fails else "FAIL")
    for f in fails:
        print("  FAIL:", f)
    return 0 if not fails else 1


if __name__ == "__main__":
    sys.exit(main())
