"""Regression tests from the stage 1/2 audit (Albert/audit_stage12/REPORT.md).

Each test failed against the code at commit 91bb2bc and passes after the fix
it names. Same style as the other suites: fakeredis, real producer and
validator code, claims about what lands in the streams.
"""

from __future__ import annotations

import csv
import json
import os
import socket
import subprocess
import sys
import threading
import time
from pathlib import Path
from typing import Any

import fakeredis
import pytest
import redis as _redis

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent  # Albert/
sys.path.insert(0, str(ROOT / "stage1_producer"))
sys.path.insert(0, str(ROOT / "stage2_validator"))

import producer as prod  # noqa: E402
import validator as val  # noqa: E402

_ROW = {
    "trans_date_trans_time": "2019-01-01 00:00:18", "cc_num": "2703186189652095",
    "merchant": "fraud_Rippin, Kub and Mann", "category": "misc_net", "amt": "4.97",
    "first": "Jennifer", "last": "Banks", "gender": "F", "street": "561 Perry Cove",
    "city": "Moravian Falls", "state": "NC", "zip": "28654", "lat": "36.0788",
    "long": "-81.1781", "city_pop": "3495", "job": "Psychologist, counselling",
    "dob": "1988-03-09", "trans_num": "0b242abb623afc578575680df30655b9",
    "unix_time": "1325376018", "merch_lat": "36.011293", "merch_long": "-82.048315",
    "is_fraud": "0",
}


def _sorted_csv(path: Path, n: int) -> Path:
    base = 1_546_300_800
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=prod.CSV_COLUMNS)
        w.writeheader()
        for i in range(n):
            ts = time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(base + i))
            w.writerow({**_ROW, "trans_date_trans_time": ts, "trans_num": f"{i:032x}"})
    return path


def _cfg(**kw: Any) -> val.ValidatorConfig:
    base = dict(consumer_id="v1", once=True, min_idle_ms=0, block_ms=10, count=500,
                log_interval=1e9, metrics_interval=1e9)
    base.update(kw)
    cfg = val.ValidatorConfig(**base)
    cfg.retry_backoff_s = 0.01
    return cfg


def _good(seq: int = 0, emit_ms: int | None = None) -> str:
    row = {**_ROW, "trans_num": f"{seq:032x}"}
    return json.dumps(prod.build_event(row, now_ms=emit_ms or int(time.time() * 1000)),
                      separators=(",", ":"))


# ---------------------------------------------------------------------------
# Producer
# ---------------------------------------------------------------------------


def test_checkpoint_after_a_failed_flush_never_claims_unsent_rows(tmp_path: Path) -> None:
    """redis-py resets a pipeline even when execute() raises. The run() finally
    block then force-flushed the now-empty pipeline (no error), and saved a
    state whose rows_consumed included the batch that never reached Redis, so
    --resume silently skipped those rows."""
    server = fakeredis.FakeServer()
    client = fakeredis.FakeRedis(server=server, decode_responses=True)
    real_pipeline = client.pipeline
    executes = {"n": 0}

    def pipeline(*a: Any, **kw: Any) -> Any:
        pipe = real_pipeline(*a, **kw)
        real_execute = pipe.execute

        def execute(*ea: Any, **ekw: Any) -> Any:
            executes["n"] += 1
            if executes["n"] == 3:
                server.connected = False      # Redis goes away mid-run
            return real_execute(*ea, **ekw)
        pipe.execute = execute
        return pipe

    client.pipeline = pipeline
    state_path = tmp_path / "state.json"
    cfg = prod.ProducerConfig(
        sorted_csv=_sorted_csv(tmp_path / "s.csv", 100), rate=1_000_000, burst=10_000,
        max_events=None, loop=False, malformed_rate=0.0, state_path=state_path,
        batch_size=10, flush_interval=1e9, log_interval=1e9,
    )
    with pytest.raises(_redis.exceptions.ConnectionError):
        prod.Producer(client, cfg).run()
    server.connected = True
    in_redis = client.xlen("tx.raw")
    state = json.loads(state_path.read_text())
    assert in_redis == 20
    assert state["rows_consumed"] <= in_redis, (
        f"state claims {state['rows_consumed']} rows sent but only {in_redis} reached Redis")
    assert state["seq"] == state["rows_consumed"]


@pytest.mark.timeout(20)
def test_emitted_at_is_stamped_when_the_xadd_is_sent_not_when_queued(tmp_path: Path) -> None:
    """emitted_at is the latency clock start (contract: 'the instant of emit').
    It was stamped when the event was queued in the pipeline, so every event
    carried up to flush_interval (default 250 ms) of producer buffering."""
    client = fakeredis.FakeRedis(decode_responses=True)
    real_pipeline = client.pipeline
    sent_at: list[int] = []

    def pipeline(*a: Any, **kw: Any) -> Any:
        pipe = real_pipeline(*a, **kw)
        real_execute = pipe.execute

        def execute(*ea: Any, **ekw: Any) -> Any:
            n = len(pipe.command_stack)
            sent_at.extend([time.time_ns() // 1_000_000] * n)
            return real_execute(*ea, **ekw)
        pipe.execute = execute
        return pipe

    client.pipeline = pipeline
    cfg = prod.ProducerConfig(
        sorted_csv=_sorted_csv(tmp_path / "s.csv", 50), rate=50, burst=1,
        max_events=20, loop=False, malformed_rate=0.0, state_path=tmp_path / "st.json",
        batch_size=1000, flush_interval=0.3, log_interval=1e9,
    )
    prod.Producer(client, cfg).run()
    entries = client.xrange("tx.raw", "-", "+")
    assert len(entries) == len(sent_at) == 20
    gaps = []
    for (_, f), sent in zip(entries, sent_at):
        emitted = prod_ms(json.loads(f["data"])["emitted_at"])
        gaps.append(sent - emitted)
    assert max(gaps) <= 20, f"emitted_at precedes the XADD send by up to {max(gaps)} ms: {gaps}"


def prod_ms(iso: str) -> int:
    from datetime import datetime
    return int(round(datetime.fromisoformat(iso.replace("Z", "+00:00")).timestamp() * 1000))


# ---------------------------------------------------------------------------
# Validator
# ---------------------------------------------------------------------------


def test_metrics_hash_counts_every_validator_in_the_group() -> None:
    """Each validator HSET its own process-local counters into the shared
    metrics:validator hash, so with two validators the hash showed only
    whichever pushed last."""
    fake = fakeredis.FakeRedis(decode_responses=True)
    for i in range(30):
        fake.xadd(val.RAW_STREAM, {"data": _good(i)})
    fake.xadd(val.RAW_STREAM, {"data": "{bad"})
    a = val.Validator(fake, _cfg(consumer_id="a", count=10, max_events=10, metrics_interval=0))
    a.run()
    b = val.Validator(fake, _cfg(consumer_id="b", metrics_interval=0))
    b.run()
    a._push_metrics()  # a pushes again last, as it would on its next tick
    m = fake.hgetall(val.METRICS_KEY)
    assert a.counters.validated + b.counters.validated == 31
    assert int(m["validated"]) == 31, m
    assert int(m["passed"]) == 30 and int(m["dlq"]) == 1 and int(m["dlq_MALFORMED_JSON"]) == 1


def test_metrics_push_is_idempotent_between_ticks() -> None:
    fake = fakeredis.FakeRedis(decode_responses=True)
    for i in range(5):
        fake.xadd(val.RAW_STREAM, {"data": _good(i)})
    v = val.Validator(fake, _cfg())
    v.run()
    v._push_metrics()
    v._push_metrics()
    assert int(fake.hget(val.METRICS_KEY, "validated")) == 5


class _ScanLimitedClient:
    """fakeredis, but XAUTOCLAIM scans at most COUNT*10 pending entries per
    call, as real Redis >= 7 does (checked against Redis 8.10.2 in the audit:
    0 entries claimed, non-zero cursor). fakeredis claims past the window."""

    def __init__(self, inner: Any) -> None:
        self._inner = inner

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def xautoclaim(self, name: str, groupname: str, consumername: str, min_idle_time: int,
                   start_id: Any = "0-0", count: int = 100, **kw: Any) -> Any:
        start = start_id.decode() if isinstance(start_id, bytes) else str(start_id)
        pel = self._inner.xpending_range(name, groupname, min="-", max="+", count=1_000_000)
        pel = [p for p in pel if _id_key(p["message_id"]) >= _id_key(start)]
        attempts, idle, examined = count * 10, [], 0
        for p in pel:                      # stop at COUNT claims or COUNT*10 entries scanned
            if attempts == 0 or len(idle) == count:
                break
            attempts -= 1
            examined += 1
            if p["time_since_delivered"] >= min_idle_time:
                idle.append(p["message_id"])
        claimed = self._inner.xclaim(name, groupname, consumername, min_idle_time, idle) if idle else []
        cursor = pel[examined]["message_id"] if examined < len(pel) else "0-0"
        return [cursor, claimed, []]


def _id_key(i: Any) -> tuple[int, int]:
    s = i.decode() if isinstance(i, bytes) else str(i)
    a, b = s.split("-")
    return int(a), int(b)


def test_xautoclaim_keeps_scanning_past_a_window_with_nothing_idle() -> None:
    """With more than COUNT*10 busy pending entries ahead of a dead consumer's
    entries, XAUTOCLAIM returns nothing claimed and a non-zero cursor. The
    loop stopped there, so the dead consumer's entries were never recovered."""
    fake = fakeredis.FakeRedis(decode_responses=True)
    for i in range(60):
        fake.xadd(val.RAW_STREAM, {"data": _good(i)})
    val.Validator(fake, _cfg()).ensure_group()
    fake.xreadgroup(val.GROUP, "dead", {val.RAW_STREAM: ">"}, count=60)
    time.sleep(0.25)
    busy = [e[0] for e in fake.xrange(val.RAW_STREAM, "-", "+")][:50]
    fake.xclaim(val.RAW_STREAM, val.GROUP, "busy", 0, busy)   # a live peer's in-flight work
    v = val.Validator(_ScanLimitedClient(fake), _cfg(consumer_id="rec", count=4, min_idle_ms=200))
    v.ensure_group()
    assert v._reclaim_pending() == 10
    owners = {c["name"]: c["pending"] for c in fake.xinfo_consumers(val.RAW_STREAM, val.GROUP)}
    assert owners.get("dead", 0) == 0 and owners["busy"] == 50


@pytest.fixture
def tcp_redis_url() -> Any:
    from fakeredis import TcpFakeServer
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port = s.getsockname()[1]
    server = TcpFakeServer(("127.0.0.1", port), server_type="redis")
    t = threading.Thread(target=server.serve_forever, daemon=True)
    t.start()
    yield f"redis://127.0.0.1:{port}/0"
    server.shutdown()
    server.server_close()


@pytest.mark.timeout(30)
def test_two_validators_on_one_host_get_distinct_consumer_names(tcp_redis_url: str) -> None:
    """The default consumer name was the bare hostname, so two validators on
    one host shared one pending list: a starting validator 'recovered' the
    other's in-flight entries and both forwarded them."""
    c = _redis.Redis.from_url(tcp_redis_url)
    for i in range(20):
        c.xadd(val.RAW_STREAM, {"data": _good(i)})
    env = {**os.environ, "REDIS_URL": tcp_redis_url}
    env.pop("VALIDATOR_ID", None)
    procs = [subprocess.Popen([sys.executable, str(ROOT / "stage2_validator" / "validator.py"),
                               "--once", "--block-ms", "50", "--log-interval", "1e9"],
                              env=env, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
             for _ in range(2)]
    for p in procs:
        assert p.wait(timeout=20) == 0, p.stderr.read()
    names = {x["name"] for x in c.xinfo_consumers(val.RAW_STREAM, val.GROUP)}
    assert len(names) == 2, f"two validator processes registered as {names}"


def test_leap_second_emitted_at_is_valid_rfc3339_and_forwarded() -> None:
    """The contract's date-time is RFC 3339, which allows second 60. The
    format check rejected it (datetime can't hold :60), and the latency
    parser would raise on it, so a contract-valid event went to the DLQ."""
    fake = fakeredis.FakeRedis(decode_responses=True)
    ev = json.loads(_good(1))
    ev["emitted_at"] = "2016-12-31T23:59:60.500Z"
    raw = json.dumps(ev, separators=(",", ":"))
    fake.xadd(val.RAW_STREAM, {"data": raw})
    v = val.Validator(fake, _cfg(), now_ms=lambda: 1_483_228_801_000)   # 2017-01-01T00:00:01Z
    v.run()
    assert fake.xrange(val.DLQ_STREAM, "-", "+") == []
    (_, f), = fake.xrange(val.VALID_STREAM, "-", "+")
    assert f["data"] == raw
    assert v.counters.latency.percentile(50) == 500.0
    fc = val.build_schema_validator().format_checker
    assert not fc.conforms("2016-12-31T23:59:61Z", "date-time")
    assert not fc.conforms("2019-02-30T00:00:00Z", "date-time")


# ---------------------------------------------------------------------------
# Coverage gaps: behaviour that was already correct but that no test pinned
# down (mutation testing: breaking it left the original 58 tests green).
# ---------------------------------------------------------------------------


@pytest.mark.timeout(20)
def test_a_write_that_keeps_failing_neither_spins_nor_blocks_new_entries() -> None:
    """A failed forward set _own_pending_dirty; the loop went straight back to
    draining its own pending list, failed again, and repeated - with no
    backoff and without ever reading new entries again. Measured on the
    original code: ~18,700 write attempts in 3 s and a later valid event
    never forwarded. Any persistent per-command error (here WRONGTYPE on
    tx.dlq; also OOM under maxmemory) froze the whole validator."""
    fake = fakeredis.FakeRedis(decode_responses=True)
    fake.set(val.DLQ_STREAM, "not a stream")          # every XADD to tx.dlq fails
    bad_id = fake.xadd(val.RAW_STREAM, {"data": "{bad"})
    attempts = {"n": 0}
    real_pipeline = fake.pipeline

    def counting(*a: Any, **kw: Any) -> Any:
        attempts["n"] += 1
        return real_pipeline(*a, **kw)
    fake.pipeline = counting
    cfg = _cfg(once=False, min_idle_ms=60_000)
    cfg.retry_backoff_s = 0.2
    v = val.Validator(fake, cfg)
    t = threading.Thread(target=v.run, daemon=True)
    t.start()
    try:
        time.sleep(0.5)
        good = _good(1)
        fake.xadd(val.RAW_STREAM, {"data": good})
        deadline = time.monotonic() + 3
        while time.monotonic() < deadline and not fake.xlen(val.VALID_STREAM):
            time.sleep(0.02)
        assert [f["data"] for _, f in fake.xrange(val.VALID_STREAM, "-", "+")] == [good], \
            "a new valid event was never forwarded while another entry's write kept failing"
        time.sleep(0.5)
        pending = fake.xpending_range(val.RAW_STREAM, val.GROUP, min="-", max="+", count=10)
        assert [p["message_id"] for p in pending] == [bad_id], "the entry whose write failed was acked"
        assert attempts["n"] < 40, f"{attempts['n']} write attempts in ~1.5 s: retry has no backoff"
    finally:
        v.stop()
        t.join(timeout=5)


def test_group_creation_errors_other_than_busygroup_are_raised() -> None:
    fake = fakeredis.FakeRedis(decode_responses=True)
    fake.set(val.RAW_STREAM, "not a stream")
    with pytest.raises(_redis.exceptions.ResponseError, match="WRONGTYPE"):
        val.Validator(fake, _cfg()).ensure_group()


def test_token_bucket_burst_is_capped_after_a_long_stall() -> None:
    now = [0.0]
    bucket = prod.TokenBucket(rate=100.0, burst=5, clock=lambda: now[0])
    now[0] += 60.0                                 # a minute with nothing sent
    free = 0
    while bucket.tokens >= 1.0 - bucket._EPS:
        bucket.acquire(sleeper=lambda dt: pytest.fail("burst token should be free"))
        free += 1
        bucket._refill()
    assert free == 5, f"{free} events sent back-to-back after a stall; burst is 5"


def _peak_bytes(fn: Any) -> int:
    import tracemalloc
    tracemalloc.start()
    try:
        fn()
        return tracemalloc.get_traced_memory()[1]
    finally:
        tracemalloc.stop()


def test_prepare_memory_is_bounded_by_chunk_rows_not_file_size(tmp_path: Path) -> None:
    small, big = _sorted_csv(tmp_path / "small.csv", 2_000), _sorted_csv(tmp_path / "big.csv", 20_000)
    p_small = _peak_bytes(lambda: prod.external_sort(small, tmp_path / "o1.csv", chunk_rows=500))
    p_big = _peak_bytes(lambda: prod.external_sort(big, tmp_path / "o2.csv", chunk_rows=500))
    assert p_big < 2.5 * p_small, f"10x the rows took {p_big / p_small:.1f}x the memory"


def test_run_streams_the_csv_instead_of_loading_it(tmp_path: Path) -> None:
    def emit(n_rows: int) -> int:
        csv_path = _sorted_csv(tmp_path / f"in{n_rows}.csv", n_rows)
        cfg = prod.ProducerConfig(sorted_csv=csv_path, rate=1_000_000, burst=10_000, max_events=50,
                                  loop=False, malformed_rate=0.0, state_path=tmp_path / f"s{n_rows}.json",
                                  log_interval=1e9)
        return _peak_bytes(lambda: prod.Producer(fakeredis.FakeRedis(decode_responses=True), cfg).run())
    p_small, p_big = emit(1_000), emit(20_000)
    assert p_big < 2 * p_small, f"emitting 50 events from a 20x larger file took {p_big / p_small:.1f}x the memory"
