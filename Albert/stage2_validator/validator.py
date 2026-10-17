"""Stage 2 validator for the IBM Z Datathon 2026 fraud-scoring pipeline.

Consumes raw transaction events from Redis stream ``tx.raw`` via a consumer
group, validates each payload against the v1.0.0 ``event.schema.json``
contract (``Albert/New stuff/`` in the team repo, ``/app/contracts/`` in the
image), including its ``uuid`` / ``date-time`` / ``date`` formats,
forwards valid events to ``tx.valid`` (byte-identical) and invalid or
unparseable events to the dead-letter queue ``tx.dlq`` with structured
reason metadata.

Processing is at-least-once: XACK happens only after a successful forward
to ``tx.valid`` or ``tx.dlq``. On startup, XAUTOCLAIM reclaims pending
entries older than ``--min-idle-ms`` from dead consumers.

Target: IBM LinuxONE (s390x). Pure Python only — stdlib + redis +
jsonschema. No numpy, no pandas, no pydantic-core.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import random
import re
import signal
import socket
import sys
import threading
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

from jsonschema import Draft202012Validator, FormatChecker
from jsonschema.exceptions import ValidationError
from redis.exceptions import RedisError

__all__ = [
    "RAW_STREAM",
    "VALID_STREAM",
    "DLQ_STREAM",
    "GROUP",
    "METRICS_KEY",
    "REASON_CODES",
    "Counters",
    "ReservoirSampler",
    "Validator",
    "ValidatorConfig",
    "classify_error",
    "build_schema_validator",
]

RAW_STREAM = "tx.raw"
VALID_STREAM = "tx.valid"
DLQ_STREAM = "tx.dlq"
GROUP = "validators"
METRICS_KEY = "metrics:validator"

DEFAULT_REDIS_URL = "redis://localhost:6379/0"
_HERE = Path(__file__).resolve().parent
# EVENT_SCHEMA_PATH wins; otherwise the image layout (/app/contracts), then the
# team-repo layout (Albert/New stuff, the published v1.0.0 contracts).
_SCHEMA_CANDIDATES = [
    _HERE.parent / "contracts" / "event.schema.json",
    _HERE.parent / "New stuff" / "event.schema.json",
]
SCHEMA_PATH = Path(os.environ["EVENT_SCHEMA_PATH"]) if os.environ.get("EVENT_SCHEMA_PATH") else next(
    (p for p in _SCHEMA_CANDIDATES if p.is_file()), _SCHEMA_CANDIDATES[0])

REASON_CODES = (
    "SCHEMA_VIOLATION",
    "MALFORMED_JSON",
    "MISSING_REQUIRED",
    "TYPE_ERROR",
    "RANGE_ERROR",
    "INTERNAL_ERROR",
)

log = logging.getLogger("validator")


# ---------------------------------------------------------------------------
# Reservoir / latency histogram
# ---------------------------------------------------------------------------


class ReservoirSampler:
    """Vitter's Algorithm R — fixed-size uniform sample of a stream.

    Pure Python, no numpy. Percentiles are computed via nearest-rank over
    the sorted reservoir, which is accurate enough for p50/p95 at the
    sample sizes we actually hit (default 2048).
    """

    __slots__ = ("_cap", "_buf", "_n", "_rng")

    def __init__(self, capacity: int = 2048, seed: Optional[int] = None) -> None:
        if capacity <= 0:
            raise ValueError("capacity must be positive")
        self._cap = capacity
        self._buf: list[float] = []
        self._n = 0
        self._rng = random.Random(seed)

    def add(self, value: float) -> None:
        self._n += 1
        if len(self._buf) < self._cap:
            self._buf.append(value)
        else:
            j = self._rng.randint(0, self._n - 1)
            if j < self._cap:
                self._buf[j] = value

    def percentile(self, p: float) -> Optional[float]:
        if not self._buf:
            return None
        if not 0.0 <= p <= 100.0:
            raise ValueError("p must be in [0, 100]")
        s = sorted(self._buf)
        if p == 0.0:
            return s[0]
        # Nearest-rank: ceil(p/100 * N) - 1 (0-indexed).
        k = max(0, min(len(s) - 1, math.ceil((p / 100.0) * len(s)) - 1))
        return s[k]

    def __len__(self) -> int:
        return len(self._buf)

    @property
    def total(self) -> int:
        return self._n


# ---------------------------------------------------------------------------
# Counters
# ---------------------------------------------------------------------------


@dataclass
class Counters:
    validated: int = 0
    passed: int = 0
    dlq: int = 0
    by_reason: dict[str, int] = field(default_factory=lambda: {r: 0 for r in REASON_CODES})
    start_monotonic: float = field(default_factory=time.monotonic)
    latency: ReservoirSampler = field(default_factory=ReservoirSampler)

    def record_pass(self, latency_ms: float) -> None:
        self.validated += 1
        self.passed += 1
        self.latency.add(latency_ms)

    def record_dlq(self, reason: str, latency_ms: Optional[float] = None) -> None:
        self.validated += 1
        self.dlq += 1
        self.by_reason[reason] = self.by_reason.get(reason, 0) + 1
        if latency_ms is not None:
            self.latency.add(latency_ms)

    def snapshot(self) -> dict[str, Any]:
        elapsed = max(time.monotonic() - self.start_monotonic, 1e-9)
        return {
            "validated": self.validated,
            "passed": self.passed,
            "dlq": self.dlq,
            "elapsed_s": elapsed,
            "events_per_sec": self.validated / elapsed,
            "by_reason": dict(self.by_reason),
            "p50_ms": self.latency.percentile(50.0),
            "p95_ms": self.latency.percentile(95.0),
            "latency_samples": len(self.latency),
        }


# ---------------------------------------------------------------------------
# Schema loading / error classification
# ---------------------------------------------------------------------------


_RFC3339 = re.compile(r"^\d{4}-\d{2}-\d{2}[Tt]\d{2}:\d{2}:\d{2}(\.\d+)?([Zz]|[+-]\d{2}:\d{2})$")
_UUID = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")
_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _parse_rfc3339(value: str) -> datetime:
    iso = value[:-1] + "+00:00" if value[-1] in "Zz" else value
    if iso[17:19] == "60":
        # RFC 3339 allows a leap second (:60); datetime cannot hold one.
        return datetime.fromisoformat(iso[:17] + "59" + iso[19:]) + timedelta(seconds=1)
    return datetime.fromisoformat(iso)


def _format_checker() -> FormatChecker:
    """The contract's formats, checked in pure Python. jsonschema treats
    ``format`` as annotation-only by default, and 4.17 only checks
    ``date-time`` if the optional rfc3339-validator package is installed —
    so the three formats the contract uses are registered explicitly."""
    fc = FormatChecker(formats=())

    @fc.checks("uuid")
    def _uuid(value: object) -> bool:
        return not isinstance(value, str) or bool(_UUID.match(value))

    @fc.checks("date-time", raises=ValueError)
    def _date_time(value: object) -> bool:
        if not isinstance(value, str):
            return True
        if not _RFC3339.match(value):
            return False
        _parse_rfc3339(value)  # rejects month 13, hour 25, ...
        return True

    @fc.checks("date", raises=ValueError)
    def _date(value: object) -> bool:
        if not isinstance(value, str):
            return True
        if not _DATE.match(value):
            return False
        date.fromisoformat(value)
        return True

    return fc


def build_schema_validator(schema_path: Path = SCHEMA_PATH) -> Draft202012Validator:
    schema = json.loads(schema_path.read_text(encoding="utf-8"))
    Draft202012Validator.check_schema(schema)
    return Draft202012Validator(schema, format_checker=_format_checker())


def classify_error(err: ValidationError) -> str:
    """Map a jsonschema ValidationError to one of our reason codes.

    More specific than SCHEMA_VIOLATION where possible so Part 4 can
    chart DLQ reasons meaningfully.
    """
    v = err.validator
    if v == "required":
        return "MISSING_REQUIRED"
    if v == "type":
        return "TYPE_ERROR"
    if v in ("minimum", "maximum", "exclusiveMinimum", "exclusiveMaximum"):
        return "RANGE_ERROR"
    # pattern / enum / minLength / maxLength / additionalProperties → generic
    return "SCHEMA_VIOLATION"


def _path_str(err: ValidationError) -> str:
    parts: list[str] = []
    for p in err.absolute_path:
        parts.append(str(p))
    return "/" + "/".join(parts) if parts else "/"


def _field(fields: Any, name: str) -> Any:
    """Look up a stream field whether the client decodes responses or not."""
    if not isinstance(fields, dict):
        return None
    val = fields.get(name)
    if val is None:
        val = fields.get(name.encode())
    return val


# Producer metadata on tx.raw entries, copied through as sibling fields so
# seq / producer_id survive to tx.valid and the injected-kind tag reaches the DLQ.
_PASSTHROUGH_VALID = ("producer_id", "seq")
_PASSTHROUGH_DLQ = ("producer_id", "seq", "malformed")


def _passthrough(fields: Any, names: tuple[str, ...]) -> dict[str, Any]:
    out = {}
    for name in names:
        val = _field(fields, name)
        if val is not None:
            out[name] = val
    return out


def _id_str(entry_id: Any) -> str:
    return entry_id.decode("utf-8") if isinstance(entry_id, bytes) else entry_id


def _reject_constant(token: str) -> Any:
    raise ValueError(f"non-standard JSON constant {token!r}")


# ---------------------------------------------------------------------------
# Validator
# ---------------------------------------------------------------------------


@dataclass
class ValidatorConfig:
    raw_stream: str = RAW_STREAM
    valid_stream: str = VALID_STREAM
    dlq_stream: str = DLQ_STREAM
    group: str = GROUP
    consumer_id: str = ""
    block_ms: int = 1000
    count: int = 500
    min_idle_ms: int = 30_000
    max_events: Optional[int] = None
    once: bool = False
    log_interval: float = 5.0
    valid_maxlen: int = 1_000_000
    dlq_maxlen: int = 1_000_000
    metrics_key: str = METRICS_KEY
    metrics_interval: float = 1.0
    retry_backoff_s: float = 0.5


class Validator:
    """Stream validator. ``client`` is any object exposing the subset of
    redis-py we need (xgroup_create, xreadgroup, xautoclaim, xadd, xack,
    hset, pipeline). Tests pass fakeredis; prod passes redis.Redis."""

    def __init__(
        self,
        client: Any,
        cfg: ValidatorConfig,
        validator: Optional[Draft202012Validator] = None,
        now_ms: Callable[[], int] = lambda: time.time_ns() // 1_000_000,
    ) -> None:
        self.client = client
        self.cfg = cfg
        self._validator = validator if validator is not None else build_schema_validator()
        self._now_ms = now_ms
        self.counters = Counters()
        self._stop = threading.Event()
        self._last_log = time.monotonic()
        self._last_metrics_push = 0.0
        self._pushed: dict[str, int] = {}  # counter values already added to the metrics hash
        self._in_signal_handler = False
        self._own_pending_dirty = True
        self._next_pending_retry = 0.0
        self._last_reclaim = float("-inf")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

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
            # Not on main thread — tests may invoke run() from a thread.
            pass

    def ensure_group(self) -> None:
        """Create the consumer group on tx.raw if it doesn't exist."""
        try:
            self.client.xgroup_create(
                name=self.cfg.raw_stream,
                groupname=self.cfg.group,
                id="0",
                mkstream=True,
            )
        except Exception as e:  # BUSYGROUP → already exists; swallow
            msg = str(e)
            if "BUSYGROUP" in msg:
                return
            raise

    # ------------------------------------------------------------------
    # Core processing
    # ------------------------------------------------------------------

    def _dlq_entry(
        self,
        raw: Any,
        reason: str,
        event_id: Optional[str],
        path: str,
        message: str,
        errors: Optional[list[dict[str, str]]] = None,
        run_id: Optional[str] = None,
    ) -> dict[str, Any]:
        if errors is None:
            errors = [{"path": path, "keyword": reason.lower(), "message": message}]
        entry = {
            # Raw payload exactly as received (str or bytes), never re-encoded.
            "data": raw if isinstance(raw, (str, bytes)) else json.dumps(raw, separators=(",", ":")),
            "reason": reason,
            "path": path,
            "message": message[:1000],  # cap in case of a very long jsonschema message
            # Every error with its JSON path; `message` above is the capped summary.
            "errors": json.dumps(errors, separators=(",", ":")),
            "dlq_ts": str(self._now_ms()),
            "stage": "validation",
            "validator_id": self.cfg.consumer_id,
        }
        if event_id is not None:
            entry["event_id"] = event_id
        if run_id is not None:
            entry["run_id"] = run_id
        return entry

    def _try_extract_event_id(self, raw: str) -> Optional[str]:
        """If the raw string parses as a JSON object with event_id, return it."""
        try:
            obj = json.loads(raw)
        except (ValueError, TypeError):
            return None
        if isinstance(obj, dict):
            eid = obj.get("event_id")
            if isinstance(eid, str):
                return eid
        return None

    def _route(self, entry_id: str, fields: Any) -> tuple[str, dict[str, Any], Optional[str], Optional[float]]:
        """Decide where one stream entry goes, without touching Redis, and
        carry the producer's sibling metadata fields along."""
        stream, out, reason, latency = self._route_payload(entry_id, fields)
        meta = _passthrough(fields, _PASSTHROUGH_VALID if reason is None else _PASSTHROUGH_DLQ)
        return stream, {**out, **meta}, reason, latency

    def _route_payload(self, entry_id: str, fields: Any) -> tuple[str, dict[str, Any], Optional[str], Optional[float]]:
        """Decide where one stream entry goes, without touching Redis.

        Returns ``(stream, stream_fields, dlq_reason, latency_ms)``;
        ``dlq_reason`` is None for a valid event. Must never raise — any
        uncaught exception becomes an INTERNAL_ERROR DLQ record so a single
        poison entry can't stall the stream.
        """
        dlq = self.cfg.dlq_stream
        try:
            raw = _field(fields, "data")
            if raw is None:
                # Entry missing the `data` field entirely — treat as malformed.
                return dlq, self._dlq_entry(
                    raw="", reason="MALFORMED_JSON", event_id=None, path="/",
                    message="stream entry has no 'data' field",
                ), "MALFORMED_JSON", None

            if isinstance(raw, bytes):
                try:
                    raw_str = raw.decode("utf-8")
                except UnicodeDecodeError:
                    # Not valid UTF-8 → can't be valid JSON per RFC 8259.
                    return dlq, self._dlq_entry(
                        raw=raw, reason="MALFORMED_JSON", event_id=None, path="/",
                        message="payload is not valid UTF-8",
                    ), "MALFORMED_JSON", None
            else:
                raw_str = raw  # already a decoded str

            # Try to parse. NaN/Infinity are not JSON (RFC 8259) and NaN would
            # slip past every minimum/maximum check, so reject them here.
            try:
                event = json.loads(raw_str, parse_constant=_reject_constant)
            except (ValueError, TypeError) as e:
                return dlq, self._dlq_entry(
                    raw=raw, reason="MALFORMED_JSON", event_id=None, path="/",
                    message=f"json parse error: {e}",
                ), "MALFORMED_JSON", None

            # Validate against the schema. Collect ALL errors.
            errors = sorted(self._validator.iter_errors(event), key=lambda e: list(e.absolute_path))
            if errors:
                event_id = event.get("event_id") if isinstance(event, dict) else None
                if not isinstance(event_id, str):
                    event_id = None
                run_id = event.get("run_id") if isinstance(event, dict) else None
                if not isinstance(run_id, str):
                    run_id = None
                # Prefer the most specific reason: scan all errors and pick the
                # highest-priority code (MISSING_REQUIRED > TYPE_ERROR > RANGE_ERROR > SCHEMA_VIOLATION).
                priority = {
                    "MISSING_REQUIRED": 3,
                    "TYPE_ERROR": 2,
                    "RANGE_ERROR": 1,
                    "SCHEMA_VIOLATION": 0,
                }
                best = errors[0]
                best_code = classify_error(best)
                for err in errors[1:]:
                    code = classify_error(err)
                    if priority[code] > priority[best_code]:
                        best = err
                        best_code = code
                # Compose a message that includes every error for debugging.
                combined = "; ".join(f"{_path_str(e)}: {e.message}" for e in errors)
                detail = [
                    {"path": _path_str(e), "keyword": str(e.validator), "message": e.message[:1000]}
                    for e in errors
                ]
                return dlq, self._dlq_entry(
                    raw=raw, reason=best_code, event_id=event_id, path=_path_str(best),
                    message=combined, errors=detail, run_id=run_id,
                ), best_code, None

            # Valid. Forward the raw payload byte-identical. Stage metadata
            # lives in sibling fields, NOT inside the JSON payload (that would
            # corrupt the contract).
            now = self._now_ms()
            latency_ms: Optional[float] = None
            emitted_at = event.get("emitted_at") if isinstance(event, dict) else None
            if isinstance(emitted_at, str):
                emit_ms = int(_parse_rfc3339(emitted_at).timestamp() * 1000 + 0.5)
                latency_ms = max(0.0, float(now - emit_ms))
            return self.cfg.valid_stream, {
                "data": raw,
                "validate_ts": str(now),
                "validator_id": self.cfg.consumer_id,
            }, None, latency_ms
        except Exception as e:  # noqa: BLE001 — poison-pill safety net
            log.exception("internal error processing entry %s", entry_id)
            raw = _field(fields, "data")
            text = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else raw
            event_id = self._try_extract_event_id(text) if isinstance(text, str) else None
            return dlq, self._dlq_entry(
                raw=raw if isinstance(raw, (str, bytes)) else "",
                reason="INTERNAL_ERROR", event_id=event_id, path="/",
                message=f"{type(e).__name__}: {e}",
            ), "INTERNAL_ERROR", None

    def _process_batch(self, batch: list[tuple[str, Any]]) -> None:
        """Route a batch: one pipelined round trip of XADDs to tx.valid /
        tx.dlq, then one XACK for exactly the entries whose XADD succeeded.

        Redis I/O errors propagate to the caller with the affected entries
        still pending — an infrastructure failure is retried, never
        dead-lettered.
        """
        if not batch:
            return
        routed = [(entry_id, *self._route(entry_id, fields)) for entry_id, fields in batch]
        pipe = self.client.pipeline(transaction=False)
        for _entry_id, stream, out, _reason, _latency in routed:
            maxlen = self.cfg.valid_maxlen if stream == self.cfg.valid_stream else self.cfg.dlq_maxlen
            pipe.xadd(stream, out, maxlen=maxlen, approximate=True)
        results = pipe.execute(raise_on_error=False)

        done = []
        for item, res in zip(routed, results):
            if isinstance(res, Exception):
                # Left pending; retried from this consumer's pending list.
                log.error("forward of %s failed, leaving pending: %s", item[0], res)
                self._own_pending_dirty = True
                continue
            done.append(item)
        if not done:
            return
        self._xack(*(item[0] for item in done))
        for _entry_id, _stream, _out, reason, latency_ms in done:
            if reason is not None:
                self.counters.record_dlq(reason)
            elif latency_ms is not None:
                self.counters.record_pass(latency_ms)
            else:
                self.counters.validated += 1
                self.counters.passed += 1

    def _process_one(self, entry_id: str, fields: dict[str, Any]) -> None:
        """Validate one stream entry and route it to tx.valid or tx.dlq."""
        self._process_batch([(entry_id, fields)])

    # ------------------------------------------------------------------
    # Redis I/O
    # ------------------------------------------------------------------

    def _xack(self, *entry_ids: str) -> None:
        self.client.xack(self.cfg.raw_stream, self.cfg.group, *entry_ids)

    # ------------------------------------------------------------------
    # Read loop
    # ------------------------------------------------------------------

    def _remaining(self) -> Optional[int]:
        if self.cfg.max_events is None:
            return None
        return max(0, self.cfg.max_events - self.counters.validated)

    def _batch_count(self) -> int:
        # Never take delivery of more entries than --max-events will process;
        # extras would sit in the pending list after we exit.
        remaining = self._remaining()
        return self.cfg.count if remaining is None else max(1, min(self.cfg.count, remaining))

    def _reclaim_pending(self) -> int:
        """XAUTOCLAIM pending entries older than min_idle_ms. Processes them
        inline (same code path as fresh entries) and returns the number
        reclaimed and processed."""
        processed = 0
        cursor = "0-0"
        while not self._stop.is_set() and self._remaining() != 0:
            try:
                res = self.client.xautoclaim(
                    name=self.cfg.raw_stream,
                    groupname=self.cfg.group,
                    consumername=self.cfg.consumer_id,
                    min_idle_time=self.cfg.min_idle_ms,
                    start_id=cursor,
                    count=self._batch_count(),
                )
            except Exception as e:  # noqa: BLE001
                # Stream may not exist yet on a cold start — ensure_group
                # already created it, so this is unexpected but non-fatal.
                log.warning("xautoclaim failed: %s", e)
                return processed
            # redis-py 5+ returns (next_cursor, claimed_entries, deleted_ids)
            # Older variants return (next_cursor, claimed_entries). Handle both.
            if not isinstance(res, (list, tuple)):
                break
            if len(res) >= 2:
                cursor = res[0]
                claimed = res[1]
            else:
                break
            if isinstance(cursor, bytes):
                cursor = cursor.decode("utf-8")
            # Redis scans at most COUNT*10 pending entries per call, so an
            # empty claim with a non-zero cursor only means nothing in that
            # window was idle: keep walking until the cursor wraps to 0-0.
            if claimed:
                batch = [(_id_str(entry_id), fields) for entry_id, fields in claimed]
                self._process_batch(batch)
                processed += len(batch)
            if cursor == "0-0":
                break
        if processed:
            log.info("xautoclaim reclaimed %d entries from stale consumers", processed)
        return processed

    def _xreadgroup(self, start_id: str, block: Optional[int]) -> list[tuple[str, Any]]:
        res = self.client.xreadgroup(
            groupname=self.cfg.group,
            consumername=self.cfg.consumer_id,
            streams={self.cfg.raw_stream: start_id},
            count=self._batch_count(),
            block=block,
        )
        batch: list[tuple[str, Any]] = []
        if not res:
            return batch
        for _stream, entries in res:
            for entry_id, fields in entries:
                batch.append((_id_str(entry_id), fields))
        return batch

    def _read_batch(self) -> list[tuple[str, Any]]:
        return self._xreadgroup(">", self.cfg.block_ms)

    def _drain_own_pending(self) -> int:
        """Process every entry already delivered to THIS consumer but not
        yet acked (e.g. a previous crash with the same consumer name, or a
        forward that failed), walking the pending list to its end."""
        processed = 0
        cursor = "0"
        while not self._stop.is_set() and self._remaining() != 0:
            batch = self._xreadgroup(cursor, None)
            if not batch:
                break
            self._process_batch(batch)
            processed += len(batch)
            cursor = batch[-1][0]
        return processed

    # ------------------------------------------------------------------
    # Metrics
    # ------------------------------------------------------------------

    def _push_metrics(self) -> None:
        """Counters go in with HINCRBY (only what changed since the last
        push), so the hash holds the whole group's totals however many
        validators run; HSET from each one would leave only the last
        writer's. Rate and latency are per validator: the plain fields are
        the last writer's, and each validator also writes its own copy
        suffixed with ``:<validator_id>``."""
        snap = self.counters.snapshot()
        counts = {"validated": snap["validated"], "passed": snap["passed"], "dlq": snap["dlq"]}
        for code, n in snap["by_reason"].items():
            counts[f"dlq_{code}"] = n
        gauges = {
            "events_per_sec": f"{snap['events_per_sec']:.2f}",
            "p50_ms": "" if snap["p50_ms"] is None else f"{snap['p50_ms']:.2f}",
            "p95_ms": "" if snap["p95_ms"] is None else f"{snap['p95_ms']:.2f}",
            "elapsed_s": f"{snap['elapsed_s']:.3f}",
        }
        payload = {**gauges, **{f"{k}:{self.cfg.consumer_id}": v for k, v in gauges.items()},
                   "validator_id": self.cfg.consumer_id, "updated_ts": str(self._now_ms())}
        try:
            pipe = self.client.pipeline(transaction=False)
            for k, n in counts.items():
                pipe.hincrby(self.cfg.metrics_key, k, n - self._pushed.get(k, 0))
            pipe.hset(self.cfg.metrics_key, mapping=payload)
            pipe.execute()
            self._pushed = counts
        except Exception:  # noqa: BLE001
            log.exception("failed to push metrics")

    def _maybe_log_and_push(self, force: bool = False) -> None:
        now = time.monotonic()
        if force or (now - self._last_metrics_push) >= self.cfg.metrics_interval:
            self._push_metrics()
            self._last_metrics_push = now
        if force or (now - self._last_log) >= self.cfg.log_interval:
            snap = self.counters.snapshot()
            log.info(
                "validated=%d passed=%d dlq=%d rate=%.1f/s p50=%s ms p95=%s ms by=%s",
                snap["validated"], snap["passed"], snap["dlq"],
                snap["events_per_sec"],
                "n/a" if snap["p50_ms"] is None else f"{snap['p50_ms']:.1f}",
                "n/a" if snap["p95_ms"] is None else f"{snap['p95_ms']:.1f}",
                {k: v for k, v in snap["by_reason"].items() if v},
            )
            self._last_log = now

    # ------------------------------------------------------------------
    # Public entrypoint
    # ------------------------------------------------------------------

    def run(self) -> dict[str, Any]:
        self._install_signal_handlers()
        if not self.cfg.consumer_id:
            # hostname-pid: unique per process on a host (two validators on one
            # host must not share a pending list), and stable across restarts
            # of a container, where the pid is always 1.
            self.cfg.consumer_id = f"{socket.gethostname() or 'validator'}-{os.getpid()}"
        self.counters = Counters()
        self._pushed = {}

        self.ensure_group()
        self._own_pending_dirty = True
        self._last_reclaim = float("-inf")
        # Reclaim dead consumers' entries periodically, not just at startup:
        # a consumer restarted under a new name (e.g. a new container
        # hostname) within min_idle_ms would otherwise never recover them.
        reclaim_every_s = max(0.1, self.cfg.min_idle_ms / 2000.0)

        try:
            while not self._stop.is_set() and self._remaining() != 0:
                try:
                    if time.monotonic() - self._last_reclaim >= reclaim_every_s:
                        self._reclaim_pending()
                        self._last_reclaim = time.monotonic()
                    if self._own_pending_dirty and time.monotonic() >= self._next_pending_retry:
                        # Entries already delivered to this consumer but not
                        # acked: a previous crash or a failed forward.
                        self._own_pending_dirty = False
                        self._drain_own_pending()
                        if self._own_pending_dirty:
                            # A write failed again. Retry after a backoff and keep
                            # reading new entries meanwhile, rather than spinning
                            # on the same failure and starving the stream.
                            self._next_pending_retry = time.monotonic() + self.cfg.retry_backoff_s
                        continue
                    batch = self._read_batch()
                    if not batch:
                        if self.cfg.once:
                            break
                        self._maybe_log_and_push()
                        continue
                    # A batch is always finished and acked, even if a stop
                    # signal arrives part-way through it.
                    self._process_batch(batch)
                except RedisError as e:
                    log.warning("redis error, retrying in %.2fs: %s", self.cfg.retry_backoff_s, e)
                    self._own_pending_dirty = True
                    self._stop.wait(self.cfg.retry_backoff_s)
                    continue
                self._maybe_log_and_push()
        finally:
            self._maybe_log_and_push(force=True)

        return self._summary()

    def _summary(self) -> dict[str, Any]:
        snap = self.counters.snapshot()
        summary = {
            "validated": snap["validated"],
            "passed": snap["passed"],
            "dlq": snap["dlq"],
            "by_reason": {k: v for k, v in snap["by_reason"].items() if v},
            "events_per_sec": round(snap["events_per_sec"], 2),
            "elapsed_s": round(snap["elapsed_s"], 3),
            "p50_ms": snap["p50_ms"],
            "p95_ms": snap["p95_ms"],
            "validator_id": self.cfg.consumer_id,
        }
        log.info("FINAL %s", json.dumps(summary))
        return summary


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _open_client(url: str) -> Any:
    import redis  # local import so tests that monkeypatch don't need the real server
    # Raw bytes: a non-UTF-8 payload must reach the DLQ, not make XREADGROUP
    # raise UnicodeDecodeError, and valid payloads are forwarded as the exact
    # bytes received.
    return redis.Redis.from_url(url, decode_responses=False)


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="validator",
        description="Stage 2 validator for the IBM Z Datathon 2026 fraud pipeline.",
    )
    p.add_argument("--log-level", default="INFO")
    p.add_argument("--raw-stream", default=RAW_STREAM)
    p.add_argument("--valid-stream", default=VALID_STREAM)
    p.add_argument("--dlq-stream", default=DLQ_STREAM)
    p.add_argument("--group", default=GROUP)
    p.add_argument("--consumer-id", default=os.environ.get("VALIDATOR_ID", ""),
                   help="consumer name within the group (default: hostname-pid)")
    p.add_argument("--block-ms", type=int, default=1000,
                   help="XREADGROUP BLOCK in ms")
    p.add_argument("--count", type=int, default=500,
                   help="XREADGROUP batch size")
    p.add_argument("--min-idle-ms", type=int, default=30_000,
                   help="XAUTOCLAIM min idle time for recovery")
    p.add_argument("--max-events", type=int, default=None)
    p.add_argument("--once", action="store_true",
                   help="drain what's pending, then exit")
    p.add_argument("--log-interval", type=float, default=5.0)
    p.add_argument("--metrics-interval", type=float, default=1.0)
    p.add_argument("--valid-maxlen", type=int, default=1_000_000)
    p.add_argument("--dlq-maxlen", type=int, default=1_000_000)
    p.add_argument("--metrics-key", default=METRICS_KEY)
    return p


def main(argv: Optional[Iterable[str]] = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    url = os.environ.get("REDIS_URL", DEFAULT_REDIS_URL)
    client = _open_client(url)
    cfg = ValidatorConfig(
        raw_stream=args.raw_stream,
        valid_stream=args.valid_stream,
        dlq_stream=args.dlq_stream,
        group=args.group,
        consumer_id=args.consumer_id,
        block_ms=args.block_ms,
        count=args.count,
        min_idle_ms=args.min_idle_ms,
        max_events=args.max_events,
        once=args.once,
        log_interval=args.log_interval,
        metrics_interval=args.metrics_interval,
        valid_maxlen=args.valid_maxlen,
        dlq_maxlen=args.dlq_maxlen,
        metrics_key=args.metrics_key,
    )
    v = Validator(client, cfg)
    log.info("connecting to %s raw=%s valid=%s dlq=%s group=%s consumer=%s",
             url, cfg.raw_stream, cfg.valid_stream, cfg.dlq_stream, cfg.group,
             cfg.consumer_id or "<hostname>")
    summary = v.run()
    print(json.dumps(summary))
    return 0


if __name__ == "__main__":
    sys.exit(main())
