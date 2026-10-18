"""Tests for the Stage 1 producer.

These are real end-to-end tests against ``fakeredis`` — not mocks asserting
on themselves. Each test writes a tiny CSV, runs the producer, reads back
the stream, and makes claims about the bytes that landed there.
"""

from __future__ import annotations

import calendar
import csv
import hashlib
import json
import os
import random
import signal
import subprocess
import sys
import time
from pathlib import Path
from typing import Any, Iterator

import pytest
import fakeredis

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent  # Albert/
sys.path.insert(0, str(ROOT / "stage1_producer"))
sys.path.insert(0, str(ROOT / "stage2_validator"))

import producer as prod  # noqa: E402
import validator as val  # noqa: E402


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


SCHEMA_PATH = ROOT / "New stuff" / "event.schema.json"  # the published v1.0.0 contract
EVENT_SCHEMA = json.loads(SCHEMA_PATH.read_text())
# Stage 2's validator, formats included: the contract is only real if it is
# enforced the way the pipeline enforces it.
VALIDATOR = val.build_schema_validator(SCHEMA_PATH)


SAMPLE_ROWS: list[dict[str, Any]] = [
    {
        "trans_date_trans_time": "2019-01-01 00:00:18",
        "cc_num": "2703186189652095",
        "merchant": "fraud_Rippin, Kub and Mann",
        "category": "misc_net",
        "amt": 4.97,
        "first": "Jennifer", "last": "Banks", "gender": "F",
        "street": "561 Perry Cove", "city": "Moravian Falls", "state": "NC",
        "zip": "28654", "lat": 36.0788, "long": -81.1781, "city_pop": 3495,
        "job": "Psychologist, counselling", "dob": "1988-03-09",
        "trans_num": "0b242abb623afc578575680df30655b9",
        "unix_time": 1325376018,
        "merch_lat": 36.011293, "merch_long": -82.048315, "is_fraud": 0,
    },
    {
        "trans_date_trans_time": "2019-01-01 00:00:44",
        "cc_num": "630423337322",
        "merchant": "fraud_Heller, Gutmann and Zieme",
        "category": "grocery_pos",
        "amt": 107.23,
        "first": "Stephanie", "last": "Gill", "gender": "F",
        "street": "43039 Riley Greens Suite 393", "city": "Orient", "state": "WA",
        "zip": "99160", "lat": 48.8878, "long": -118.2105, "city_pop": 149,
        "job": "Special educational needs teacher", "dob": "1978-06-21",
        "trans_num": "1f76529f8574734946361c461b024d99",
        "unix_time": 1325376044,
        "merch_lat": 49.159047, "merch_long": -118.186462, "is_fraud": 0,
    },
    {
        "trans_date_trans_time": "2019-01-01 00:00:51",
        "cc_num": "38859492057661",
        "merchant": "fraud_Lind-Buckridge",
        "category": "entertainment",
        "amt": 220.11,
        "first": "Edward", "last": "Sanchez", "gender": "M",
        "street": "594 White Dale Suite 530", "city": "Malad City", "state": "ID",
        "zip": "83252", "lat": 42.1808, "long": -112.262, "city_pop": 4154,
        "job": "Nature conservation officer", "dob": "1962-01-19",
        "trans_num": "a1a22d70485983eac12b5b88dad1cf95",
        "unix_time": 1325376051,
        "merch_lat": 43.150704, "merch_long": -112.154481, "is_fraud": 0,
    },
    {
        "trans_date_trans_time": "2019-01-01 00:01:16",
        "cc_num": "3534093764340240",
        "merchant": "fraud_Kutch, Hermiston and Farrell",
        "category": "gas_transport",
        "amt": 45.00,
        "first": "Jeremy", "last": "White", "gender": "M",
        "street": "9443 Cynthia Court Apt. 038", "city": "Boulder", "state": "MT",
        "zip": "59632", "lat": 46.2306, "long": -112.1138, "city_pop": 1939,
        "job": "Patent attorney", "dob": "1967-01-12",
        "trans_num": "6b849c168bdad6f867558c3793159a81",
        "unix_time": 1325376076,
        "merch_lat": 47.034331, "merch_long": -112.561071, "is_fraud": 0,
    },
    {
        "trans_date_trans_time": "2019-01-01 00:03:06",
        "cc_num": "375534208663984",
        "merchant": "fraud_Keeling-Crist",
        "category": "misc_pos",
        "amt": 41.96,
        "first": "Tyler", "last": "Garcia", "gender": "M",
        "street": "408 Bradley Rest", "city": "Doe Hill", "state": "VA",
        "zip": "24433", "lat": 38.4207, "long": -79.4629, "city_pop": 99,
        "job": "Dance movement psychotherapist", "dob": "1986-03-28",
        "trans_num": "a41d7549acf90789359a9aa5346dcb46",
        "unix_time": 1325376186,
        "merch_lat": 38.674999, "merch_long": -78.632459, "is_fraud": 0,
    },
]


def _write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=prod.CSV_COLUMNS)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def _write_unsorted_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    scrambled = list(rows)
    random.Random(42).shuffle(scrambled)
    _write_csv(path, scrambled)


def _ts(unix: int) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.gmtime(unix))


def _at(row: dict[str, Any], unix: int) -> dict[str, Any]:
    """Copy of row whose dataset clock is `unix`. trans_date_trans_time is the
    sort key and the source of transaction.unix_time; the CSV column is kept
    consistent so expectations can be read back from the file."""
    return {**row, "unix_time": unix, "trans_date_trans_time": _ts(unix)}


def _make_big_sorted(path: Path, n: int) -> None:
    """Repeat SAMPLE_ROWS to generate n sorted rows with strictly increasing time."""
    base = SAMPLE_ROWS[0]["unix_time"]
    _write_csv(path, [_at(SAMPLE_ROWS[i % len(SAMPLE_ROWS)], base + i) for i in range(n)])


def _row_unix(row: dict[str, str]) -> int:
    return calendar.timegm(time.strptime(row["trans_date_trans_time"], "%Y-%m-%d %H:%M:%S"))


def _read_stream(client: Any, stream: str) -> list[dict[str, Any]]:
    entries = client.xrange(stream, "-", "+")
    out: list[dict[str, Any]] = []
    for _entry_id, fields in entries:
        data = fields.get("data") if isinstance(fields, dict) else fields[b"data"].decode()
        if isinstance(data, bytes):
            data = data.decode()
        out.append(json.loads(data))
    return out


@pytest.fixture
def fake_client() -> fakeredis.FakeRedis:
    return fakeredis.FakeRedis(decode_responses=True)


@pytest.fixture
def sorted_csv(tmp_path: Path) -> Path:
    p = tmp_path / "sorted.csv"
    _write_csv(p, SAMPLE_ROWS)
    return p


# ---------------------------------------------------------------------------
# External sort
# ---------------------------------------------------------------------------


def test_external_sort_produces_nondecreasing(tmp_path: Path) -> None:
    src = tmp_path / "raw.csv"
    dst = tmp_path / "sorted.csv"
    _write_unsorted_csv(src, SAMPLE_ROWS)

    n = prod.external_sort(src, dst, chunk_rows=2)  # tiny chunks → forces merging
    assert n == len(SAMPLE_ROWS)

    times = []
    with dst.open() as f:
        for row in csv.DictReader(f):
            times.append(_row_unix(row))
    assert times == sorted(times), f"expected sorted, got {times}"


# ---------------------------------------------------------------------------
# Core: schema conformance + chronological order
# ---------------------------------------------------------------------------


def test_all_emitted_events_pass_schema(fake_client: Any, sorted_csv: Path, tmp_path: Path) -> None:
    cfg = prod.ProducerConfig(
        sorted_csv=sorted_csv, rate=10_000, burst=1000,
        max_events=len(SAMPLE_ROWS), loop=False, malformed_rate=0.0,
        state_path=tmp_path / "state.json",
    )
    p = prod.Producer(fake_client, cfg)
    p.run()

    entries = fake_client.xrange(cfg.stream, "-", "+")
    assert len(entries) == len(SAMPLE_ROWS)
    for i, (_id, fields) in enumerate(entries):
        ev = json.loads(fields["data"])
        errors = sorted(VALIDATOR.iter_errors(ev), key=lambda e: list(e.path))
        assert not errors, f"event failed schema: {[e.message for e in errors]}"
        # Required stamped fields present:
        assert "event_id" in ev and len(ev["event_id"]) == 36
        assert ev["emitted_at"].endswith("Z") and len(ev["emitted_at"]) == 24
        # Producer metadata rides in sibling stream fields, not the payload.
        assert fields["producer_id"] == cfg.producer_id
        assert fields["seq"] == str(i)


def test_output_strictly_nondecreasing_unix_time(fake_client: Any, tmp_path: Path) -> None:
    # Build a dataset with duplicate timestamps so we can prove non-DECREASING,
    # not just strictly increasing (producer must preserve input order).
    rows = []
    base = 1_600_000_000
    for i in range(50):
        rows.append(_at(SAMPLE_ROWS[i % len(SAMPLE_ROWS)], base + (i // 2)))  # pairs share a timestamp
    src = tmp_path / "raw.csv"
    dst = tmp_path / "sorted.csv"
    _write_unsorted_csv(src, rows)
    prod.external_sort(src, dst, chunk_rows=7)

    cfg = prod.ProducerConfig(sorted_csv=dst, rate=10_000, burst=1000,
                              max_events=None, loop=False, malformed_rate=0.0,
                              state_path=tmp_path / "state.json")
    prod.Producer(fake_client, cfg).run()
    events = _read_stream(fake_client, cfg.stream)
    assert len(events) == 50
    times = [ev["transaction"]["unix_time"] for ev in events]
    assert all(a <= b for a, b in zip(times, times[1:])), f"not non-decreasing: {times}"


# ---------------------------------------------------------------------------
# Rate accuracy
# ---------------------------------------------------------------------------


@pytest.mark.timeout(30)
def test_rate_500_over_10_seconds(fake_client: Any, tmp_path: Path) -> None:
    sorted_csv = tmp_path / "sorted.csv"
    _make_big_sorted(sorted_csv, 10_000)  # plenty of rows for 10 s @ 500/s

    target = 500.0
    duration = 10.0
    cfg = prod.ProducerConfig(
        sorted_csv=sorted_csv, rate=target, burst=50,
        max_events=int(target * duration),
        loop=False, malformed_rate=0.0,
        state_path=tmp_path / "state.json",
        batch_size=50, flush_interval=0.05,
        log_interval=1e9,
    )
    p = prod.Producer(fake_client, cfg)
    t0 = time.monotonic()
    summary = p.run()
    elapsed = time.monotonic() - t0

    # 500 ± 5% over the full window.
    assert summary["emitted"] == int(target * duration)
    achieved = summary["emitted"] / elapsed
    tol = 0.05
    assert target * (1 - tol) <= achieved <= target * (1 + tol), (
        f"achieved {achieved:.1f}/s outside 500 ± 5% (elapsed={elapsed:.2f}s)"
    )


# ---------------------------------------------------------------------------
# Malformed injection
# ---------------------------------------------------------------------------


def test_malformed_rate_approximately_matches(fake_client: Any, tmp_path: Path) -> None:
    sorted_csv = tmp_path / "sorted.csv"
    _make_big_sorted(sorted_csv, 2000)

    cfg = prod.ProducerConfig(
        sorted_csv=sorted_csv, rate=100_000, burst=10_000,
        max_events=2000, loop=False, malformed_rate=0.1,
        state_path=tmp_path / "state.json",
        seed=1234,  # deterministic
        log_interval=1e9,
    )
    p = prod.Producer(fake_client, cfg)
    summary = p.run()

    entries = fake_client.xrange(cfg.stream, "-", "+")
    assert len(entries) == 2000

    passing = 0
    failing = 0
    tagged = 0
    for _entry_id, fields in entries:
        ev = json.loads(fields["data"])
        # Injected events are tagged in a sibling stream field, never in the
        # payload, so the payload alone must fail the schema.
        if "malformed" in fields:
            tagged += 1
            errors = list(VALIDATOR.iter_errors(ev))
            assert errors, f"tagged malformed event unexpectedly validated: {ev}"
        else:
            errors = list(VALIDATOR.iter_errors(ev))
            if errors:
                failing += 1
            else:
                passing += 1

    assert tagged == summary["malformed"], "counter and payload tags disagree"
    # ~10% malformed; allow ±2.5pp noise either way given 2000 trials.
    frac = tagged / 2000.0
    assert 0.075 <= frac <= 0.125, f"malformed fraction {frac:.3f} outside 7.5–12.5%"
    # The non-tagged events must all pass.
    assert failing == 0
    # Sanity: passing count ≈ 90%.
    assert passing + tagged == 2000


# ---------------------------------------------------------------------------
# Resume
# ---------------------------------------------------------------------------


def test_resume_continues_at_correct_seq_and_offset(fake_client: Any, tmp_path: Path) -> None:
    sorted_csv = tmp_path / "sorted.csv"
    _make_big_sorted(sorted_csv, 100)
    state_path = tmp_path / "state.json"

    cfg1 = prod.ProducerConfig(
        sorted_csv=sorted_csv, rate=100_000, burst=1000,
        max_events=40, loop=False, malformed_rate=0.0,
        state_path=state_path, log_interval=1e9,
    )
    s1 = prod.Producer(fake_client, cfg1).run()
    assert s1["emitted"] == 40
    assert s1["final_seq"] == 40
    assert s1["rows_consumed"] == 40

    # Fresh client — a resume should pick up at seq=40 and skip the first 40 rows.
    fake2 = fakeredis.FakeRedis(decode_responses=True)
    cfg2 = prod.ProducerConfig(
        sorted_csv=sorted_csv, rate=100_000, burst=1000,
        max_events=30, loop=False, malformed_rate=0.0,
        state_path=state_path, resume=True, log_interval=1e9,
    )
    s2 = prod.Producer(fake2, cfg2).run()
    assert s2["emitted"] == 30
    assert s2["final_seq"] == 70  # 40 carried + 30 new
    assert s2["rows_consumed"] == 70

    entries = fake2.xrange(cfg2.stream, "-", "+")
    assert [int(f["seq"]) for _, f in entries] == list(range(40, 70))

    # Row content should match rows 40..69 of the sorted CSV.
    events = [json.loads(f["data"]) for _, f in entries]
    with sorted_csv.open() as f:
        all_rows = list(csv.DictReader(f))
    assert [ev["source_trans_num"] for ev in events] == [r["trans_num"] for r in all_rows[40:70]]
    assert [ev["transaction"]["unix_time"] for ev in events] == [_row_unix(r) for r in all_rows[40:70]]


# ---------------------------------------------------------------------------
# SIGTERM / graceful shutdown
# ---------------------------------------------------------------------------


@pytest.mark.timeout(30)
def test_sigterm_flushes_and_exits_zero(tmp_path: Path) -> None:
    sorted_csv = tmp_path / "sorted.csv"
    _make_big_sorted(sorted_csv, 50_000)
    state_path = tmp_path / "state.json"
    summary_path = tmp_path / "summary.json"

    runner = tmp_path / "runner.py"
    runner.write_text(
        "import json, sys, os\n"
        f"sys.path.insert(0, {str(ROOT / 'stage1_producer')!r})\n"
        "import fakeredis\n"
        "import producer as prod\n"
        "client = fakeredis.FakeRedis(decode_responses=True)\n"
        "cfg = prod.ProducerConfig(\n"
        f"    sorted_csv=__import__('pathlib').Path({str(sorted_csv)!r}),\n"
        "    rate=200.0, burst=50,\n"
        "    max_events=None, loop=True, malformed_rate=0.0,\n"
        f"    state_path=__import__('pathlib').Path({str(state_path)!r}),\n"
        "    log_interval=1e9,\n"
        ")\n"
        "p = prod.Producer(client, cfg)\n"
        "summary = p.run()\n"
        f"open({str(summary_path)!r}, 'w').write(json.dumps(summary))\n"
        f"open({str(summary_path)!r} + '.len', 'w').write(str(client.xlen('tx.raw')))\n"
    )

    proc = subprocess.Popen(
        [sys.executable, str(runner)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        env={**os.environ},
    )
    # Let the producer actually emit some events before signaling.
    time.sleep(2.0)
    assert proc.poll() is None, "producer exited before signal"
    proc.send_signal(signal.SIGTERM)
    rc = proc.wait(timeout=15)
    stdout, stderr = proc.communicate(timeout=5) if proc.stdout else (b"", b"")
    assert rc == 0, f"exit code {rc}; stderr=\n{stderr.decode(errors='replace')}"

    assert summary_path.is_file(), "summary never written — producer did not finalize"
    summary = json.loads(summary_path.read_text())
    assert summary["emitted"] > 0

    xlen = int((summary_path.parent / (summary_path.name + ".len")).read_text())
    # The post-shutdown stream length must equal the counter (nothing left in a dangling pipeline).
    assert xlen == summary["emitted"], (
        f"XLEN={xlen} but counter={summary['emitted']} — pipeline not flushed on SIGTERM"
    )


# ---------------------------------------------------------------------------
# Token bucket — direct unit tests
# ---------------------------------------------------------------------------


def test_token_bucket_paces_correctly() -> None:
    # Virtual clock makes this deterministic.
    now = [0.0]
    sleeps: list[float] = []

    def clock() -> float:
        return now[0]

    def sleeper(dt: float) -> None:
        sleeps.append(dt)
        now[0] += dt

    bucket = prod.TokenBucket(rate=100.0, burst=10, clock=clock)
    # First 10 should be free (burst).
    for _ in range(10):
        bucket.acquire(sleeper=sleeper)
    assert not sleeps, "burst should not require sleeping"
    # Next 100 should average ~1/100 s each → total ~1 s.
    for _ in range(100):
        bucket.acquire(sleeper=sleeper)
    assert 0.9 <= now[0] <= 1.1, f"expected ~1.0 s elapsed, got {now[0]:.3f}"


# ---------------------------------------------------------------------------
# Audit regressions (each failed against the pre-audit code)
# ---------------------------------------------------------------------------


@pytest.mark.timeout(10)
def test_token_bucket_long_run_does_not_drift_or_livelock() -> None:
    # Virtual clock that advances exactly by what is slept. Pre-audit,
    # float rounding left tokens at 0.999…, the next wait (~1e-18 s) did not
    # move the clock, and acquire() spun forever.
    now = [0.0]
    bucket = prod.TokenBucket(rate=1000.0, burst=1, clock=lambda: now[0])

    def sleeper(dt: float) -> None:
        now[0] += dt

    for _ in range(100_000):
        bucket.acquire(sleeper=sleeper)
    # 1 free token + 99 999 paced at 1 ms each.
    assert abs(now[0] - 99.999) < 0.01, f"drifted: {now[0]:.4f}s"


@pytest.mark.timeout(10)
def test_partial_batch_is_flushed_while_producer_waits_for_tokens(tmp_path: Path) -> None:
    # At 2 ev/s with batch_size=100, the first event must reach Redis within
    # ~flush_interval, not when the next token arrives 0.5 s later.
    import threading

    client = fakeredis.FakeRedis(decode_responses=True)
    sorted_csv = tmp_path / "sorted.csv"
    _make_big_sorted(sorted_csv, 10)
    cfg = prod.ProducerConfig(
        sorted_csv=sorted_csv, rate=2.0, burst=1, max_events=3, loop=False,
        malformed_rate=0.0, state_path=tmp_path / "state.json",
        batch_size=100, flush_interval=0.05, log_interval=1e9,
    )
    p = prod.Producer(client, cfg)
    t = threading.Thread(target=p.run, daemon=True)
    t.start()
    time.sleep(0.3)
    try:
        assert client.xlen(cfg.stream) >= 1, "partial batch stalled past flush_interval"
    finally:
        p.stop()
        t.join(timeout=5)


def test_checkpoint_never_ahead_of_what_reached_redis(fake_client: Any, tmp_path: Path, monkeypatch: Any) -> None:
    # If the process dies right after a checkpoint, --resume skips
    # rows_consumed rows. Those rows must already be in Redis.
    sorted_csv = tmp_path / "sorted.csv"
    _make_big_sorted(sorted_csv, 2500)
    observed: list[tuple[int, int]] = []
    real_save = prod.ProducerState.save

    def spy(self: Any, path: Path) -> None:
        observed.append((self.rows_consumed, fake_client.xlen("tx.raw")))
        real_save(self, path)

    monkeypatch.setattr(prod.ProducerState, "save", spy)
    cfg = prod.ProducerConfig(
        sorted_csv=sorted_csv, rate=1_000_000, burst=10_000, max_events=2500,
        loop=False, malformed_rate=0.0, state_path=tmp_path / "state.json",
        batch_size=5000, flush_interval=1e9, log_interval=1e9,
    )
    prod.Producer(fake_client, cfg).run()
    assert observed, "no checkpoints written"
    for rows, xlen in observed:
        assert xlen >= rows, f"checkpoint rows_consumed={rows} but only {xlen} in Redis"


def test_resume_after_loop_wrap_continues_at_correct_row(fake_client: Any, tmp_path: Path) -> None:
    sorted_csv = tmp_path / "sorted.csv"
    _make_big_sorted(sorted_csv, 100)
    state_path = tmp_path / "state.json"
    common = dict(sorted_csv=sorted_csv, rate=1_000_000, burst=10_000, loop=True,
                  malformed_rate=0.0, state_path=state_path, log_interval=1e9)
    s1 = prod.Producer(fake_client, prod.ProducerConfig(max_events=250, **common)).run()
    assert s1["rows_consumed"] == 250

    fake2 = fakeredis.FakeRedis(decode_responses=True)
    prod.Producer(fake2, prod.ProducerConfig(max_events=10, resume=True, **common)).run()
    entries = fake2.xrange("tx.raw", "-", "+")
    events = [json.loads(f["data"]) for _, f in entries]
    with sorted_csv.open() as f:
        all_rows = list(csv.DictReader(f))
    # 250 rows consumed over a 100-row file → next row is index 50.
    assert [ev["transaction"]["unix_time"] for ev in events] == [_row_unix(r) for r in all_rows[50:60]]
    assert [int(f["seq"]) for _, f in entries] == list(range(250, 260))


@pytest.mark.parametrize("kind, keyword, path", [
    ("missing_required", "required", ["transaction"]),
    ("wrong_type", "type", ["transaction", "amount"]),
    ("out_of_range_amount", "minimum", ["transaction", "amount"]),
    ("bad_timestamp", "format", ["transaction", "occurred_at"]),
])
def test_injected_payload_is_broken_in_exactly_the_one_intended_way(kind: str, keyword: str, path: list) -> None:
    # Pre-audit, corrupt_event added a `_malformed` key, so every injected
    # payload failed additionalProperties even if the intended corruption was
    # a no-op — the DLQ assertions could not tell the difference.
    ev = prod.build_event({**SAMPLE_ROWS[0]}, now_ms=1)
    assert not list(VALIDATOR.iter_errors(ev))
    bad = prod.corrupt_event(ev, kind)
    errors = list(VALIDATOR.iter_errors(bad))
    assert [(e.validator, list(e.absolute_path)) for e in errors] == [(keyword, path)], (
        [(e.validator, list(e.absolute_path), e.message) for e in errors])
    assert set(bad) <= set(EVENT_SCHEMA["properties"]), "payload carries keys outside the contract"


def test_injected_events_are_tagged_in_a_stream_field(fake_client: Any, tmp_path: Path) -> None:
    sorted_csv = tmp_path / "sorted.csv"
    _make_big_sorted(sorted_csv, 400)
    cfg = prod.ProducerConfig(
        sorted_csv=sorted_csv, rate=1_000_000, burst=10_000, max_events=400, loop=False,
        malformed_rate=0.25, state_path=tmp_path / "state.json", seed=7, log_interval=1e9,
    )
    summary = prod.Producer(fake_client, cfg).run()
    kinds = []
    for _, fields in fake_client.xrange("tx.raw", "-", "+"):
        payload = json.loads(fields["data"])
        assert "_malformed" not in payload
        if "malformed" in fields:
            kinds.append(fields["malformed"])
            assert list(VALIDATOR.iter_errors(payload)), "tagged event validates"
        else:
            assert not list(VALIDATOR.iter_errors(payload)), "untagged event fails schema"
    assert len(kinds) == summary["malformed"] > 0
    # Rotation: kinds cycle in order.
    assert kinds == [prod.MALFORMED_KINDS[i % 4] for i in range(len(kinds))]


@pytest.mark.timeout(30)
def test_cli_sigint_flushes_writes_state_prints_summary_exits_zero(tmp_path: Path) -> None:
    sorted_csv = tmp_path / "sorted.csv"
    _make_big_sorted(sorted_csv, 20_000)
    state_path = tmp_path / ".producer_state.json"
    runner = tmp_path / "cli_runner.py"
    runner.write_text(
        "import sys, fakeredis\n"
        f"sys.path.insert(0, {str(ROOT / 'stage1_producer')!r})\n"
        "import producer as prod\n"
        "client = fakeredis.FakeRedis(decode_responses=True)\n"
        "prod._open_client = lambda url: client\n"
        "rc = prod.main(sys.argv[1:])\n"
        "sys.stderr.write('XLEN=%d\\n' % client.xlen('tx.raw'))\n"
        "sys.exit(rc)\n"
    )
    proc = subprocess.Popen(
        [sys.executable, str(runner), "run", "--input", str(sorted_csv), "--rate", "500",
         "--batch-size", "1000", "--flush-interval", "100", "--state-file", str(state_path)],
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True,
    )
    time.sleep(2.0)
    proc.send_signal(signal.SIGINT)
    out, err = proc.communicate(timeout=15)
    assert proc.returncode == 0, err
    summary = json.loads(out.strip().splitlines()[-1])
    xlen = int([l for l in err.splitlines() if l.startswith("XLEN=")][-1][5:])
    assert summary["emitted"] > 0 and xlen == summary["emitted"]
    state = json.loads(state_path.read_text())
    assert state["seq"] == state["rows_consumed"] == summary["emitted"]


# ---------------------------------------------------------------------------
# v1.0.0 contract mapping (Albert/New stuff/event.schema.json)
# ---------------------------------------------------------------------------


def test_row_maps_to_v1_event_exactly_as_the_contract_specifies() -> None:
    row = {**SAMPLE_ROWS[0], "trans_date_trans_time": "2019-03-02 14:17:00",
           "cc_num": "4999891557932452.0", "zip": "2108",
           "unix_time": 1330000000}  # deliberately inconsistent CSV unix_time
    ev = prod.build_event(row, now_ms=1_792_256_880_136, run_id="loadtest-1000eps")
    assert not list(VALIDATOR.iter_errors(ev))
    assert ev["schema_version"] == "1.0.0"
    assert ev["emitted_at"] == "2026-10-17T17:08:00.136Z"
    assert ev["run_id"] == "loadtest-1000eps"
    assert ev["source_trans_num"] == row["trans_num"]
    tx, ch = ev["transaction"], ev["cardholder"]
    # Dataset clock kept as-is and labelled Z; unix_time derived from it.
    assert tx["occurred_at"] == "2019-03-02T14:17:00.000Z"
    assert tx["unix_time"] == calendar.timegm((2019, 3, 2, 14, 17, 0)) != row["unix_time"]
    # card_id = sha256(plain digit string)[:16]; float-ish cc_num normalized first.
    assert tx["card_id"] == hashlib.sha256(b"4999891557932452").hexdigest()[:16]
    assert tx["card_last4"] == "2452"
    assert ch["zip"] == "02108"  # leading zero restored
    assert (tx["merchant_lat"], tx["merchant_lon"]) == (row["merch_lat"], row["merch_long"])
    assert (ch["lat"], ch["lon"]) == (row["lat"], row["long"])
    assert ev["label"] == {"is_fraud": row["is_fraud"]}
    # Name and street never enter the pipeline, nor does the raw card number.
    body = json.dumps(ev)
    for dropped in (row["first"], row["last"], row["street"], "4999891557932452"):
        assert dropped not in body


def test_run_id_is_omitted_when_not_set() -> None:
    ev = prod.build_event({**SAMPLE_ROWS[0]}, now_ms=1)
    assert "run_id" not in ev
    assert not list(VALIDATOR.iter_errors(ev))


def test_prepare_defaults_to_fraud_test_not_the_scorers_training_file() -> None:
    # The scorer warms up on fraudTrain.csv; the producer must replay fraudTest.csv.
    args = prod.build_parser().parse_args(["prepare"])
    assert args.input == "data/fraudTest.csv"
