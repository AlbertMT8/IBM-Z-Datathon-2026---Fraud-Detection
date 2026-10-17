"""Stage 1 producer for the IBM Z Datathon 2026 fraud-scoring pipeline.

Reads chronologically sorted Sparkov transactions and publishes them to the
Redis stream ``tx.raw`` at a paced rate, as ``RawTransactionEvent`` v1.0.0
(``Albert/New stuff/event.schema.json``): a fresh UUID ``event_id`` and an
RFC3339 millisecond ``emitted_at`` on every emit. The producer id and a
monotonic sequence number travel as sibling stream fields, since the contract
allows no extra payload keys.

Target: IBM LinuxONE (s390x). Pure Python only — stdlib + redis + jsonschema.
No pandas, numpy, or pyarrow.
"""

from __future__ import annotations

import argparse
import calendar
import csv
import hashlib
import heapq
import json
import logging
import os
import random
import signal
import sys
import tempfile
import threading
import time
import uuid
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator, Optional

__all__ = [
    "Counters",
    "TokenBucket",
    "ProducerState",
    "Producer",
    "build_event",
    "corrupt_event",
    "iter_sorted_rows",
    "external_sort",
    "MALFORMED_KINDS",
    "STREAM_NAME",
]

STREAM_NAME = "tx.raw"
DEFAULT_REDIS_URL = "redis://localhost:6379/0"
STATE_FILE_DEFAULT = ".producer_state.json"

CSV_COLUMNS = [
    "trans_date_trans_time", "cc_num", "merchant", "category", "amt",
    "first", "last", "gender", "street", "city", "state", "zip",
    "lat", "long", "city_pop", "job", "dob", "trans_num", "unix_time",
    "merch_lat", "merch_long", "is_fraud",
]

SCHEMA_VERSION = "1.0.0"

MALFORMED_KINDS = (
    "missing_required",
    "wrong_type",
    "out_of_range_amount",
    "bad_timestamp",
)

log = logging.getLogger("producer")


# ---------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------


@dataclass
class Counters:
    """Thread-safe-ish counters. The dashboard stage will scrape these."""

    emitted: int = 0
    malformed: int = 0
    bytes_written: int = 0
    start_monotonic: float = field(default_factory=time.monotonic)

    def snapshot(self) -> dict[str, float | int]:
        elapsed = max(time.monotonic() - self.start_monotonic, 1e-9)
        return {
            "emitted": self.emitted,
            "malformed": self.malformed,
            "bytes_written": self.bytes_written,
            "elapsed_s": elapsed,
            "achieved_rate": self.emitted / elapsed,
        }


# ---------------------------------------------------------------------------
# Token bucket
# ---------------------------------------------------------------------------


class TokenBucket:
    """Monotonic-clock token bucket. No drift because tokens are refilled
    from the elapsed wall-ish time since the last refill, not accumulated
    from a schedule."""

    # Refilling by exactly ``needed / rate`` can land on 0.999… due to float
    # rounding; without a tolerance the next wait is ~1e-18 s, the clock does
    # not move, and acquire() spins forever.
    _EPS = 1e-9

    def __init__(self, rate: float, burst: int, clock: Callable[[], float] = time.monotonic) -> None:
        if rate <= 0:
            raise ValueError("rate must be positive")
        if burst <= 0:
            raise ValueError("burst must be positive")
        self.rate = float(rate)
        self.capacity = float(burst)
        self.tokens = float(burst)
        self._clock = clock
        self._last = clock()

    def _refill(self) -> None:
        now = self._clock()
        delta = now - self._last
        if delta > 0:
            self.tokens = min(self.capacity, self.tokens + delta * self.rate)
            self._last = now

    def acquire(self, sleeper: Callable[[float], None] = time.sleep) -> None:
        """Block until one token is available, then consume it."""
        self._refill()
        while self.tokens < 1.0 - self._EPS:
            needed = 1.0 - self.tokens
            wait = needed / self.rate
            sleeper(wait)
            self._refill()
        self.tokens -= 1.0


# ---------------------------------------------------------------------------
# External merge sort
# ---------------------------------------------------------------------------


def _sort_key(row: dict[str, str]) -> str:
    # Order by the dataset clock, which is what occurred_at (and the derived
    # unix_time) are built from. "YYYY-MM-DD HH:MM:SS" sorts correctly as a
    # string. The CSV's own unix_time column is not used: per the contract it
    # is offset from trans_date_trans_time.
    return row.get("trans_date_trans_time") or ""


def external_sort(src: Path, dst: Path, chunk_rows: int = 200_000, tmp_dir: Optional[Path] = None) -> int:
    """External-merge-sort ``src`` CSV by ``trans_date_trans_time`` into ``dst``.

    Returns the number of data rows written. Memory bounded by ``chunk_rows``.
    """
    src = Path(src)
    dst = Path(dst)
    if not src.is_file():
        raise FileNotFoundError(src)
    tmp_root = Path(tmp_dir) if tmp_dir else dst.parent
    tmp_root.mkdir(parents=True, exist_ok=True)

    chunk_paths: list[Path] = []
    total_rows = 0
    with tempfile.TemporaryDirectory(dir=str(tmp_root), prefix=".sortchunks_") as tdir:
        tdir_path = Path(tdir)
        with src.open("r", newline="", encoding="utf-8") as fin:
            reader = csv.DictReader(fin)
            if reader.fieldnames is None:
                raise ValueError(f"{src} has no header row")
            fieldnames = list(reader.fieldnames)
            # Validate we at least have the sort column.
            if "trans_date_trans_time" not in fieldnames:
                raise ValueError("input CSV missing required 'trans_date_trans_time' column")

            buf: list[dict[str, str]] = []
            idx = 0
            for row in reader:
                buf.append(row)
                total_rows += 1
                if len(buf) >= chunk_rows:
                    buf.sort(key=_sort_key)
                    cp = tdir_path / f"chunk_{idx:06d}.csv"
                    _write_chunk(cp, fieldnames, buf)
                    chunk_paths.append(cp)
                    buf.clear()
                    idx += 1
            if buf:
                buf.sort(key=_sort_key)
                cp = tdir_path / f"chunk_{idx:06d}.csv"
                _write_chunk(cp, fieldnames, buf)
                chunk_paths.append(cp)
                buf.clear()

        # Merge chunks.
        dst.parent.mkdir(parents=True, exist_ok=True)
        _merge_chunks(chunk_paths, dst, fieldnames)

    return total_rows


def _write_chunk(path: Path, fieldnames: list[str], rows: list[dict[str, str]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()
        w.writerows(rows)


def _merge_chunks(chunk_paths: list[Path], dst: Path, fieldnames: list[str]) -> None:
    files = [p.open("r", newline="", encoding="utf-8") for p in chunk_paths]
    try:
        readers = [csv.DictReader(f) for f in files]
        iters: list[Iterator[dict[str, str]]] = [iter(r) for r in readers]
        with dst.open("w", newline="", encoding="utf-8") as fout:
            writer = csv.DictWriter(fout, fieldnames=fieldnames)
            writer.writeheader()
            # heapq.merge needs comparable keys — key via a wrapper tuple.
            def keyed(it: Iterator[dict[str, str]]) -> Iterator[tuple[str, dict[str, str]]]:
                for r in it:
                    yield (_sort_key(r), r)

            merged = heapq.merge(*(keyed(it) for it in iters), key=lambda t: t[0])
            for _, row in merged:
                writer.writerow(row)
    finally:
        for f in files:
            f.close()


# ---------------------------------------------------------------------------
# Row → event
# ---------------------------------------------------------------------------


def _iso_ms(epoch_ms: int) -> str:
    """Epoch milliseconds -> RFC3339 UTC with milliseconds, exact (no float)."""
    secs, ms = divmod(int(epoch_ms), 1000)
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(secs)) + f".{ms:03d}Z"


def card_id_from_cc_num(cc_num: str) -> str:
    """EXACTLY sha256(<cc_num digits>)[:16] — the scorer keys card history on it."""
    return hashlib.sha256(cc_num.encode()).hexdigest()[:16]


def _cc_digits(raw: str) -> str:
    # Normalize cc_num: Sparkov stores it as a float-ish string sometimes.
    cc = str(raw).strip()
    if cc.endswith(".0"):
        cc = cc[:-2]
    return cc


def build_event(row: dict[str, str], now_ms: Optional[int] = None, run_id: Optional[str] = None) -> dict[str, Any]:
    """Construct a v1.0.0 RawTransactionEvent from a sorted-CSV row.

    ``emitted_at`` is stamped by the caller just before publish, so pass a
    fresh ``now_ms`` for every emission (do NOT reuse a stale value). The
    dataset's first, last and street columns are dropped here and never
    enter the pipeline.
    """
    # The dataset clock is naive; keep the wall time and label it UTC so
    # hour-of-day survives (contract: occurred_at).
    occurred = datetime.strptime(row["trans_date_trans_time"], "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
    unix_time = calendar.timegm(occurred.timetuple())  # derived, not the CSV column
    cc = _cc_digits(row["cc_num"])
    emitted_ms = int(time.time_ns() // 1_000_000) if now_ms is None else int(now_ms)
    event: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "event_id": str(uuid.uuid4()),
        "emitted_at": _iso_ms(emitted_ms),
        "source_trans_num": row["trans_num"],
        "transaction": {
            "occurred_at": _iso_ms(unix_time * 1000),
            "unix_time": unix_time,
            "card_id": card_id_from_cc_num(cc),
            "card_last4": cc[-4:],
            "amount": round(float(row["amt"]), 2),
            "merchant": row["merchant"],
            "category": row["category"],
            "merchant_lat": float(row["merch_lat"]),
            "merchant_lon": float(row["merch_long"]),
        },
        "cardholder": {
            "city": row["city"],
            "state": row["state"],
            "zip": str(row["zip"]).strip().zfill(5),  # leading zeros (New England)
            "lat": float(row["lat"]),
            "lon": float(row["long"]),
            "city_pop": int(row["city_pop"]),
            "job": row["job"],
            "gender": row["gender"],
            "dob": row["dob"],
        },
        "label": {"is_fraud": int(row["is_fraud"])},
    }
    if run_id:
        event["run_id"] = run_id
    return event


def corrupt_event(event: dict[str, Any], kind: str) -> dict[str, Any]:
    """Return a copy of ``event`` broken in exactly one way. The downstream
    DLQ should catch this and only this. The payload is not tagged — an
    in-payload tag would itself violate additionalProperties and mask a
    corruption that failed to break anything. The producer tags injected
    events with a separate ``malformed`` stream field instead."""
    bad = {k: (dict(v) if isinstance(v, dict) else v) for k, v in event.items()}
    tx = bad["transaction"]
    if kind == "missing_required":
        tx.pop("merchant", None)
    elif kind == "wrong_type":
        tx["amount"] = "not-a-number"
    elif kind == "out_of_range_amount":
        tx["amount"] = -1.0
    elif kind == "bad_timestamp":
        tx["occurred_at"] = "2019-13-45 99:99"  # not RFC3339 date-time
    else:
        raise ValueError(f"unknown malformed kind: {kind}")
    return bad


# ---------------------------------------------------------------------------
# State / checkpoint
# ---------------------------------------------------------------------------


@dataclass
class ProducerState:
    seq: int = 0
    rows_consumed: int = 0
    emitted: int = 0
    malformed: int = 0

    @classmethod
    def load(cls, path: Path) -> "ProducerState":
        if not path.is_file():
            return cls()
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return cls()
        return cls(
            seq=int(data.get("seq", 0)),
            rows_consumed=int(data.get("rows_consumed", 0)),
            emitted=int(data.get("emitted", 0)),
            malformed=int(data.get("malformed", 0)),
        )

    def save(self, path: Path) -> None:
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(asdict(self)), encoding="utf-8")
        os.replace(tmp, path)


# ---------------------------------------------------------------------------
# Row iterator with skip support (for --resume)
# ---------------------------------------------------------------------------


def iter_sorted_rows(path: Path, skip: int = 0) -> Iterator[dict[str, str]]:
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            if i < skip:
                continue
            yield row


# ---------------------------------------------------------------------------
# Producer
# ---------------------------------------------------------------------------


@dataclass
class ProducerConfig:
    sorted_csv: Path
    rate: float
    burst: int
    max_events: Optional[int]
    loop: bool
    malformed_rate: float
    stream: str = STREAM_NAME
    maxlen: int = 1_000_000
    batch_size: int = 100
    flush_interval: float = 0.25
    producer_id: str = "producer-1"
    run_id: Optional[str] = None
    state_path: Path = Path(STATE_FILE_DEFAULT)
    resume: bool = False
    log_interval: float = 5.0
    seed: Optional[int] = None


class Producer:
    """Core producer loop. Decoupled from Redis via the ``client`` arg so tests
    can inject fakeredis or a monkeypatched stub."""

    def __init__(self, client: Any, cfg: ProducerConfig, now_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000) -> None:
        self.client = client
        self.cfg = cfg
        self._now_ms = now_ms
        self.counters = Counters()
        self.state = ProducerState.load(cfg.state_path) if cfg.resume else ProducerState()
        self.bucket = TokenBucket(cfg.rate, cfg.burst)
        self._stop = threading.Event()
        self._rng = random.Random(cfg.seed)
        self._malformed_cycle = 0
        self._pipeline: Any = None
        self._pending = 0
        self._last_flush = time.monotonic()
        self._last_log = time.monotonic()

    # ----- lifecycle -----

    def stop(self) -> None:
        self._stop.set()

    def _install_signal_handlers(self) -> None:
        def handler(signum: int, _frame: Any) -> None:
            log.info("received signal %d — stopping", signum)
            self.stop()

        try:
            signal.signal(signal.SIGINT, handler)
            signal.signal(signal.SIGTERM, handler)
        except ValueError:
            # Not on main thread (tests may invoke run() off-main).
            pass

    # ----- publish path -----

    def _pipeline_open(self) -> Any:
        if self._pipeline is None:
            # transaction=False → plain command batching, no MULTI/EXEC.
            self._pipeline = self.client.pipeline(transaction=False)
            self._pending = 0
            self._last_flush = time.monotonic()
        return self._pipeline

    def _flush(self, force: bool = False) -> None:
        if self._pipeline is None or self._pending == 0:
            if force and self._pipeline is not None:
                self._pipeline.execute()
                self._pipeline = None
                self._pending = 0
            return
        self._pipeline.execute()
        self._pipeline = None
        self._pending = 0
        self._last_flush = time.monotonic()

    def _publish(self, payload: dict[str, Any], seq: int, malformed_kind: Optional[str] = None) -> int:
        body = json.dumps(payload, separators=(",", ":"))
        # producer_id / seq are stream metadata: the contract forbids extra
        # payload keys.
        fields = {"data": body, "producer_id": self.cfg.producer_id, "seq": str(seq)}
        if malformed_kind is not None:
            fields["malformed"] = malformed_kind
        pipe = self._pipeline_open()
        pipe.xadd(self.cfg.stream, fields, maxlen=self.cfg.maxlen, approximate=True)
        self._pending += 1
        if self._pending >= self.cfg.batch_size:
            self._flush()
        return len(body)

    def _sleep(self, seconds: float) -> None:
        """Token-bucket sleeper that flushes a partial batch once
        ``flush_interval`` has elapsed, so a slow trickle never leaves events
        sitting in the pipeline while we wait for the next token."""
        if self._pending:
            until_flush = self._last_flush + self.cfg.flush_interval - time.monotonic()
            if until_flush <= seconds:
                if until_flush > 0:
                    time.sleep(until_flush)
                self._flush()
                return  # acquire() re-checks the bucket and sleeps the remainder
        time.sleep(seconds)

    # ----- logging -----

    def _maybe_log(self) -> None:
        now = time.monotonic()
        if now - self._last_log < self.cfg.log_interval:
            return
        snap = self.counters.snapshot()
        log.info(
            "emitted=%d malformed=%d rate=%.1f/s target=%.1f/s bytes=%d",
            snap["emitted"], snap["malformed"], snap["achieved_rate"],
            self.cfg.rate, snap["bytes_written"],
        )
        self._last_log = now

    # ----- main loop -----

    def _rows(self) -> Iterator[dict[str, str]]:
        skip = self.state.rows_consumed if self.cfg.resume else 0
        if skip and self.cfg.loop:
            # rows_consumed keeps counting across wraps; map it back into the file.
            n_rows = sum(1 for _ in iter_sorted_rows(self.cfg.sorted_csv))
            if n_rows:
                skip %= n_rows
        first_pass = True
        while True:
            yield from iter_sorted_rows(self.cfg.sorted_csv, skip=skip if first_pass else 0)
            if not self.cfg.loop:
                return
            first_pass = False

    def run(self) -> dict[str, Any]:
        self._install_signal_handlers()
        self.counters = Counters()
        self.counters.emitted = 0  # per-run; state.emitted is cumulative across runs
        self.counters.start_monotonic = time.monotonic()
        try:
            for row in self._rows():
                if self._stop.is_set():
                    break
                if self.cfg.max_events is not None and self.counters.emitted >= self.cfg.max_events:
                    break

                self.bucket.acquire(sleeper=self._sleep)

                # Build event stamped NOW — emitted_at must be close to publish.
                event = build_event(row, now_ms=self._now_ms(), run_id=self.cfg.run_id)

                kind: Optional[str] = None
                if self.cfg.malformed_rate > 0 and self._rng.random() < self.cfg.malformed_rate:
                    kind = MALFORMED_KINDS[self._malformed_cycle % len(MALFORMED_KINDS)]
                    self._malformed_cycle += 1
                    payload = corrupt_event(event, kind)
                    self.counters.malformed += 1
                    self.state.malformed += 1
                else:
                    payload = event

                nbytes = self._publish(payload, self.state.seq, kind)
                self.counters.bytes_written += nbytes
                self.counters.emitted += 1
                self.state.emitted += 1
                self.state.seq += 1
                self.state.rows_consumed += 1

                # Time-based flush so a slow trickle never stalls.
                if time.monotonic() - self._last_flush >= self.cfg.flush_interval:
                    self._flush()

                # Checkpoint periodically (every ~1000 rows). Flush first so a
                # checkpoint never claims rows that are still in the pipeline.
                if self.state.rows_consumed % 1000 == 0:
                    self._flush()
                    self.state.save(self.cfg.state_path)

                self._maybe_log()
        finally:
            self._flush(force=True)
            try:
                self.state.save(self.cfg.state_path)
            except OSError:
                log.exception("failed to save state")

        return self._summary()

    def _summary(self) -> dict[str, Any]:
        snap = self.counters.snapshot()
        summary = {
            "emitted": self.counters.emitted,
            "malformed": self.counters.malformed,
            "bytes_written": self.counters.bytes_written,
            "elapsed_s": round(snap["elapsed_s"], 3),
            "achieved_rate": round(snap["achieved_rate"], 2),
            "target_rate": self.cfg.rate,
            "producer_id": self.cfg.producer_id,
            "run_id": self.cfg.run_id,
            "final_seq": self.state.seq,
            "rows_consumed": self.state.rows_consumed,
        }
        log.info("FINAL %s", json.dumps(summary))
        return summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _open_client(url: str) -> Any:
    import redis  # local import so tests that monkeypatch don't need the real server
    return redis.Redis.from_url(url, decode_responses=True)


def _cmd_prepare(args: argparse.Namespace) -> int:
    src = Path(args.input)
    dst = Path(args.output)
    log.info("sorting %s -> %s (chunk_rows=%d)", src, dst, args.chunk_rows)
    t0 = time.monotonic()
    n = external_sort(src, dst, chunk_rows=args.chunk_rows)
    dt = time.monotonic() - t0
    log.info("sorted %d rows in %.2fs", n, dt)
    print(json.dumps({"rows": n, "elapsed_s": round(dt, 3), "output": str(dst)}))
    return 0


def _cmd_run(args: argparse.Namespace) -> int:
    url = os.environ.get("REDIS_URL", DEFAULT_REDIS_URL)
    client = _open_client(url)
    cfg = ProducerConfig(
        sorted_csv=Path(args.input),
        rate=args.rate,
        burst=args.burst,
        max_events=args.max_events,
        loop=args.loop,
        malformed_rate=args.malformed_rate,
        stream=args.stream,
        maxlen=args.maxlen,
        batch_size=args.batch_size,
        flush_interval=args.flush_interval,
        producer_id=args.producer_id,
        run_id=args.run_id,
        state_path=Path(args.state_file),
        resume=args.resume,
        log_interval=args.log_interval,
        seed=args.seed,
    )
    log.info("connecting to %s stream=%s rate=%.1f/s", url, cfg.stream, cfg.rate)
    producer = Producer(client, cfg)
    summary = producer.run()
    print(json.dumps(summary))
    return 0


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="producer", description="Stage 1 producer for the IBM Z Datathon 2026 fraud pipeline.")
    p.add_argument("--log-level", default="INFO")
    sub = p.add_subparsers(dest="cmd", required=True)

    pp = sub.add_parser("prepare", help="external-merge-sort the raw CSV by unix_time")
    pp.add_argument("--input", default="data/fraudTrain.csv")
    pp.add_argument("--output", default="data/sorted.csv")
    pp.add_argument("--chunk-rows", type=int, default=200_000)
    pp.set_defaults(func=_cmd_prepare)

    pr = sub.add_parser("run", help="stream the sorted CSV to Redis Streams")
    pr.add_argument("--input", default="data/sorted.csv")
    pr.add_argument("--rate", type=float, required=True, help="events per second (target)")
    pr.add_argument("--burst", type=int, default=200)
    pr.add_argument("--max-events", type=int, default=None)
    pr.add_argument("--loop", action="store_true")
    pr.add_argument("--malformed-rate", type=float, default=0.0)
    pr.add_argument("--stream", default=STREAM_NAME)
    pr.add_argument("--maxlen", type=int, default=1_000_000)
    pr.add_argument("--batch-size", type=int, default=100)
    pr.add_argument("--flush-interval", type=float, default=0.25)
    pr.add_argument("--producer-id", default=os.environ.get("PRODUCER_ID", "producer-1"))
    pr.add_argument("--run-id", default=os.environ.get("RUN_ID") or None,
                    help="optional load-test tag, carried in every event's run_id")
    pr.add_argument("--state-file", default=STATE_FILE_DEFAULT)
    pr.add_argument("--resume", action="store_true")
    pr.add_argument("--log-interval", type=float, default=5.0)
    pr.add_argument("--seed", type=int, default=None)
    pr.set_defaults(func=_cmd_run)

    return p


def main(argv: Optional[list[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
