"""Stage 5 storer for the IBM Z Datathon 2026 fraud pipeline.

Two consumers in one program, picked with ``--mode``:

  storer --mode explained   tx.explained -> scored_transactions   (group ``storers``)
  storer --mode dlq         tx.dlq       -> dead_letter_events    (group ``dlq-storers``)

At-least-once: a batch is written to Postgres and COMMITTED, and only then
XACKed. A crash between the two replays the batch; ``scored_transactions``
ignores a repeated ``event_id`` (ON CONFLICT DO NOTHING) and the DLQ path
skips stream entries it has already stored (see ``DlqDeduper``).

Explained rows are mapped with ``to_row()`` from ``Albert/New stuff/stubgen.py``
(the contract's own mapping, imported by file path because the folder name
has a space). ``committed_at`` is never sent: the column default
``clock_timestamp()`` stamps it on the database clock, and ``latency_ms`` is a
generated column.

Config comes only from environment variables: REDIS_URL and libpq's
PGHOST / PGPORT / PGDATABASE / PGUSER / PGPASSWORD.

Target: IBM LinuxONE (s390x). Python >= 3.10 (stubgen.py needs it), redis-py
and psycopg2 from Debian's own packages, nothing compiled from PyPI.
"""

from __future__ import annotations

import argparse
import collections
import importlib.util
import json
import logging
import os
import re
import signal
import sys
import time
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

import redis

log = logging.getLogger("storer")

HERE = Path(__file__).resolve().parent
# Same relative path in the repo (Albert/stage5_storage -> Albert/New stuff)
# and in the image (/app/stage5_storage -> /app/New stuff).
STUBGEN_PATH = Path(os.environ.get("STUBGEN_PATH") or HERE.parent / "New stuff" / "stubgen.py")

MODES = {
    # mode: (input stream, group, default consumer)
    "explained": ("tx.explained", "storers", "storer-1"),
    "dlq": ("tx.dlq", "dlq-storers", "dlq-storer-1"),
}
DLQ_STREAM = "tx.dlq"
METRICS_KEY = "metrics:storer"
RECLAIM_IDLE_MS = 30_000

_UUID = re.compile(r"^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$")


def load_to_row(path: Path = STUBGEN_PATH) -> Callable[[dict], dict]:
    """Import ``to_row`` from stubgen.py without renaming its folder."""
    spec = importlib.util.spec_from_file_location("stubgen", path)
    if spec is None or spec.loader is None:
        raise ImportError(f"cannot load stubgen from {path}")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module.to_row


# ---------------------------------------------------------------------------
# Field helpers
# ---------------------------------------------------------------------------


def _field(fields: dict, name: str) -> Any:
    if not fields:
        return None
    val = fields.get(name.encode())
    return fields.get(name) if val is None else val


def _text(val: Any) -> Optional[str]:
    """Stream bytes -> str that Postgres TEXT accepts: invalid UTF-8 is kept
    as backslash escapes, and NUL (never allowed in TEXT) becomes ``\\x00``."""
    if val is None:
        return None
    s = val.decode("utf-8", errors="backslashreplace") if isinstance(val, bytes) else str(val)
    return s.replace("\x00", "\\x00")


def _id(entry_id: Any) -> str:
    return entry_id.decode() if isinstance(entry_id, bytes) else str(entry_id)


def dlq_row(entry_id: str, fields: dict) -> dict:
    """tx.dlq entry -> dead_letter_events row (schema.sql column names)."""
    raw = _text(_field(fields, "data"))
    event_id = _text(_field(fields, "event_id")) or ""
    run_id = _text(_field(fields, "run_id")) or None
    if run_id is None and raw:
        # Stage 2 sends run_id as a stream field; stages 3 and 4 don't, but
        # the payload carries it whenever it parses.
        try:
            doc = json.loads(raw)
            if isinstance(doc, dict):
                cand = doc.get("run_id") or (doc.get("event") or {}).get("run_id")
                run_id = cand if isinstance(cand, str) and cand else None
                if not event_id:
                    eid = doc.get("event_id") or (doc.get("event") or {}).get("event_id")
                    event_id = eid if isinstance(eid, str) else ""
        except (ValueError, TypeError, AttributeError):
            pass
    dlq_ts = _text(_field(fields, "dlq_ts")) or ""
    return {
        "entry_id": entry_id,
        "run_id": run_id,  # None -> the column default 'default'
        "event_id": event_id if _UUID.match(event_id) else None,
        "rejected_ms": int(dlq_ts) if dlq_ts.isdigit() else None,  # None -> clock_timestamp()
        "stage": _text(_field(fields, "stage")) or None,
        "reason_code": _text(_field(fields, "reason")) or "UNKNOWN",
        "reason_detail": _text(_field(fields, "message")),
        "raw_payload": raw if raw is not None else "",
    }


class DlqDeduper:
    """Remembers the last N tx.dlq entry IDs already stored.

    dead_letter_events has no unique key and schema.sql is a frozen contract,
    so stream-entry dedupe can't live in the table. Within a process this set
    catches every repeat. Across a restart, entries replayed from the pending
    list are checked against the table instead (``recovered=True`` in
    ``PgSink.insert_dlq``)."""

    def __init__(self, size: int = 500_000) -> None:
        self.size = size
        self._seen: collections.OrderedDict[str, None] = collections.OrderedDict()

    def __contains__(self, entry_id: str) -> bool:
        return entry_id in self._seen

    def add(self, entry_id: str) -> None:
        self._seen[entry_id] = None
        if len(self._seen) > self.size:
            self._seen.popitem(last=False)


# ---------------------------------------------------------------------------
# Postgres
# ---------------------------------------------------------------------------


class DbUnavailable(Exception):
    """The database can't be reached; the batch stays unacked and is retried."""


class PgSink:
    """The only code that talks to Postgres. One open transaction per batch."""

    def __init__(self, connect: Optional[Callable[[], Any]] = None) -> None:
        import psycopg2  # Debian python3-psycopg2
        import psycopg2.extras

        self._pg = psycopg2
        self._extras = psycopg2.extras
        self._connect = connect or (lambda: psycopg2.connect(""))  # libpq reads PG* env vars
        self.conn = None

    def _cur(self):
        try:
            if self.conn is None or self.conn.closed:
                self.conn = self._connect()
            return self.conn.cursor()
        except self._pg.OperationalError as e:
            self.conn = None
            raise DbUnavailable(str(e)) from e

    def _run(self, fn):
        """Run fn(cursor) inside the open transaction. Bad data -> ValueError
        (caller isolates the row); a dead connection -> DbUnavailable."""
        cur = self._cur()
        try:
            return fn(cur)
        except (self._pg.OperationalError, self._pg.InterfaceError) as e:
            self._drop()
            raise DbUnavailable(str(e)) from e
        except self._pg.Error as e:
            raise ValueError(f"{type(e).__name__}: {e}".strip()) from e
        finally:
            try:
                cur.close()
            except Exception:  # noqa: BLE001
                pass

    def _drop(self) -> None:
        try:
            if self.conn is not None:
                self.conn.close()
        except Exception:  # noqa: BLE001
            pass
        self.conn = None

    def savepoint(self, name: str) -> None:
        self._run(lambda c: c.execute(f"SAVEPOINT {name}"))

    def rollback_to(self, name: str) -> None:
        self._run(lambda c: c.execute(f"ROLLBACK TO SAVEPOINT {name}"))

    def insert_explained(self, rows: list[dict]) -> int:
        """Multi-row INSERT ... ON CONFLICT (event_id) DO NOTHING. Columns are
        exactly to_row()'s keys; committed_at and latency_ms are left to the
        database. Returns how many rows were new."""
        cols = list(rows[0])
        sql = (f"INSERT INTO scored_transactions ({', '.join(cols)}) VALUES %s "
               f"ON CONFLICT (event_id) DO NOTHING RETURNING event_id")
        template = "(" + ", ".join(f"%({c})s" for c in cols) + ")"
        return self._run(lambda c: len(self._extras.execute_values(
            c, sql, rows, template=template, page_size=len(rows), fetch=True)))

    _DLQ_VALUES = ("(COALESCE(%(run_id)s, 'default'), %(event_id)s::uuid, "
                   "COALESCE(to_timestamp(%(rejected_ms)s::double precision / 1000.0), clock_timestamp()), "
                   "COALESCE(%(stage)s, 'validation'), %(reason_code)s, %(reason_detail)s, %(raw_payload)s)")

    def insert_dlq(self, rows: list[dict], recovered: bool) -> int:
        """Insert DLQ rows. COALESCE fills run_id / stage / rejected_at with
        the same defaults schema.sql declares. For entries replayed after a
        restart (``recovered``), a row is skipped if the identical row
        (rejected_at, stage, reason, payload) is already stored: that is a
        commit whose XACK never happened."""
        cols = "run_id, event_id, rejected_at, stage, reason_code, reason_detail, raw_payload"
        if not recovered:
            sql = f"INSERT INTO dead_letter_events ({cols}) VALUES %s"
            return self._run(lambda c: (self._extras.execute_values(
                c, sql, rows, template=self._DLQ_VALUES, page_size=len(rows)), c.rowcount)[1])
        inserted = 0
        for row in rows:
            sql = (f"INSERT INTO dead_letter_events ({cols}) SELECT * FROM (VALUES {self._DLQ_VALUES}) AS v({cols}) "
                   f"WHERE NOT EXISTS (SELECT 1 FROM dead_letter_events d WHERE d.rejected_at = v.rejected_at "
                   f"AND d.stage = v.stage AND d.reason_code = v.reason_code AND d.raw_payload = v.raw_payload)")
            inserted += self._run(lambda c: (c.execute(sql, row), c.rowcount)[1])
        return inserted

    def commit(self) -> None:
        if self.conn is None:
            raise DbUnavailable("no connection")
        try:
            self.conn.commit()
        except (self._pg.OperationalError, self._pg.InterfaceError) as e:
            self._drop()
            raise DbUnavailable(str(e)) from e

    def rollback(self) -> None:
        try:
            if self.conn is not None and not self.conn.closed:
                self.conn.rollback()
        except Exception:  # noqa: BLE001
            self._drop()


# ---------------------------------------------------------------------------
# Storer
# ---------------------------------------------------------------------------


class Storer:
    def __init__(self, r: redis.Redis, sink: Any, args: argparse.Namespace,
                 to_row: Optional[Callable[[dict], dict]] = None) -> None:
        self.r, self.sink, self.a = r, sink, args
        self.stream, self.group, _ = MODES[args.mode]
        self.to_row = to_row or (load_to_row() if args.mode == "explained" else None)
        self.stop = False
        self.counts = {"stored": 0, "duplicates_ignored": 0, "batches": 0, "rejected": 0}
        self.dedupe = DlqDeduper(args.dedupe_size)
        self.recent: collections.deque[tuple[float, int]] = collections.deque()
        self.last_metrics = 0.0
        self.last_reclaim = float("-inf")
        self.need_pending = True

    # ---- setup -------------------------------------------------------------

    def ensure_group(self) -> None:
        try:  # "0": nothing written before the group existed is missed
            self.r.xgroup_create(self.stream, self.group, id="0", mkstream=True)
            log.info("created consumer group %s on %s", self.group, self.stream)
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    # ---- reading -----------------------------------------------------------

    def _read(self, start_id: str, block: Optional[int]) -> list[tuple[str, dict]]:
        resp = self.r.xreadgroup(self.group, self.a.consumer, {self.stream: start_id},
                                 count=self.a.count, block=block)
        return [(_id(i), f) for i, f in (resp[0][1] if resp else [])]

    def drain_own_pending(self) -> int:
        """Entries this consumer took before a crash/restart (XREADGROUP 0)."""
        done, cursor = 0, "0"
        while not self.stop:
            entries = self._read(cursor, None)
            if not entries:
                break
            self.handle(entries, recovered=True)
            done += len(entries)
            cursor = entries[-1][0]
        return done

    def reclaim(self) -> int:
        """XAUTOCLAIM entries idle > 30 s from consumers that died."""
        done, cursor = 0, "0-0"
        while not self.stop:
            res = self.r.xautoclaim(self.stream, self.group, self.a.consumer,
                                    min_idle_time=RECLAIM_IDLE_MS, start_id=cursor, count=self.a.count)
            cursor, claimed = _id(res[0]), res[1]
            entries = [(_id(i), f) for i, f in claimed]
            if entries:
                self.handle(entries, recovered=True)
                done += len(entries)
            if cursor == "0-0":  # walked the whole pending list
                break
        if done:
            log.info("reclaimed %d entries from dead consumers", done)
        return done

    # ---- writing -----------------------------------------------------------

    def _map(self, entries: list[tuple[str, dict]], recovered: bool):
        """-> (rows, ids_to_ack, rejects). rejects = (entry_id, fields, reason, detail)."""
        rows, ack, rejects = [], [], []
        for entry_id, fields in entries:
            ack.append(entry_id)
            if not fields:  # trimmed from the stream while pending: nothing to store
                continue
            if self.a.mode == "dlq":
                if entry_id in self.dedupe:
                    self.counts["duplicates_ignored"] += 1
                    continue
                rows.append(dlq_row(entry_id, fields))
                continue
            raw = _field(fields, "data")
            try:
                rows.append({"_entry": entry_id, **self.to_row(json.loads(raw))})
            except Exception as e:  # noqa: BLE001 — a broken event must not stop the batch
                rejects.append((entry_id, fields, "MALFORMED_SCORED_EVENT", f"{type(e).__name__}: {e}"))
        return rows, ack, rejects

    def _insert(self, rows: list[dict], recovered: bool) -> int:
        if self.a.mode == "dlq":
            return self.sink.insert_dlq([{k: v for k, v in r.items() if k != "entry_id"} for r in rows], recovered)
        return self.sink.insert_explained([{k: v for k, v in r.items() if k != "_entry"} for r in rows])

    def _insert_isolating(self, rows: list[dict], recovered: bool, rejects: list) -> int:
        """Insert the whole batch in one statement; if Postgres rejects the data,
        retry row by row under savepoints so one bad row can't fail the rest."""
        self.sink.savepoint("batch")
        try:
            return self._insert(rows, recovered)
        except ValueError:
            self.sink.rollback_to("batch")
        inserted = 0
        key = "entry_id" if self.a.mode == "dlq" else "_entry"
        for row in rows:
            self.sink.savepoint("one")
            try:
                inserted += self._insert([row], recovered)
            except ValueError as e:
                self.sink.rollback_to("one")
                rejects.append((row[key], None, "DB_REJECTED", str(e)))
        return inserted

    def handle(self, entries: list[tuple[str, dict]], recovered: bool = False) -> None:
        """Map -> INSERT -> COMMIT -> (DLQ rejects + XACK). Raises DbUnavailable
        with nothing acked if Postgres is down."""
        rows, ack, rejects = self._map(entries, recovered)
        mapped_rejects = len(rejects)
        inserted = 0
        if rows:
            try:
                inserted = self._insert_isolating(rows, recovered, rejects)
                self.sink.commit()
            except DbUnavailable:
                self.sink.rollback()
                raise
        # Committed. Now dead-letter anything Postgres couldn't take, and ack.
        by_id = dict(entries)
        pipe = self.r.pipeline(transaction=False)
        for entry_id, fields, reason, detail in rejects:
            if self.a.mode == "dlq":
                log.error("dead_letter_events rejected tx.dlq entry %s: %s", entry_id, detail)
                continue  # never loop a DLQ row back into the DLQ
            fields = fields or by_id.get(entry_id) or {}
            pipe.xadd(DLQ_STREAM, {"data": _field(fields, "data") or b"", "reason": reason,
                                   "event_id": _field(fields, "event_id") or b"", "path": "/",
                                   "message": detail[:1000], "errors": "[]",
                                   "dlq_ts": str(int(time.time() * 1000)), "stage": "storage",
                                   "worker_id": self.a.consumer},
                      maxlen=1_000_000, approximate=True)
        pipe.xack(self.stream, self.group, *ack)
        self._retry_redis(pipe.execute)
        if self.a.mode == "dlq":
            for row in rows:
                self.dedupe.add(row["entry_id"])
        self.counts["stored"] += inserted
        db_rejects = len(rejects) - mapped_rejects
        self.counts["duplicates_ignored"] += len(rows) - inserted - db_rejects
        self.counts["rejected"] += len(rejects)
        self.counts["batches"] += 1
        self.recent.append((time.time(), inserted))

    def _retry_redis(self, fn: Callable[[], Any]) -> None:
        """After a commit the rows are safe; keep trying the XACK. (If we die
        here instead, the replay is harmless: see the class docstring.)"""
        delay = 0.1
        while True:
            try:
                fn()
                return
            except (redis.ConnectionError, redis.TimeoutError) as e:
                if self.stop:
                    log.error("stopping with a committed but unacked batch (replayed safely): %s", e)
                    return
                log.warning("Redis write failed after commit (%s); retrying in %.1fs", e, delay)
                time.sleep(delay)
                delay = min(delay * 2, 5.0)

    # ---- metrics -----------------------------------------------------------

    def push_metrics(self, force: bool = False) -> None:
        now = time.time()
        if not force and now - self.last_metrics < self.a.metrics_interval:
            return
        self.last_metrics = now
        while self.recent and now - self.recent[0][0] > 10:
            self.recent.popleft()
        rate = round(sum(n for _, n in self.recent) / 10, 2)
        c = self.counts
        if self.a.mode == "explained":
            payload = {"stored": c["stored"], "duplicates_ignored": c["duplicates_ignored"],
                       "rejected": c["rejected"], "batches": c["batches"], "rows_per_sec": rate,
                       "consumer": self.a.consumer}
        else:  # separate field names: both modes share the one hash
            payload = {"dlq_stored": c["stored"], "dlq_duplicates_ignored": c["duplicates_ignored"],
                       "dlq_batches": c["batches"], "dlq_rows_per_sec": rate,
                       "dlq_consumer": self.a.consumer}
        payload["updated_ms"] = int(now * 1000)
        try:
            self.r.hset(METRICS_KEY, mapping={k: str(v) for k, v in payload.items()})
        except (redis.ConnectionError, redis.TimeoutError):
            pass  # best-effort, like metrics:scorer

    # ---- main loop ---------------------------------------------------------

    def _wait(self, what: str, step: Callable[[], Any]) -> Any:
        delay = 0.5
        while not self.stop:
            try:
                return step()
            except (redis.ConnectionError, redis.TimeoutError, DbUnavailable) as e:
                log.warning("%s failed (%s); retrying in %.1fs", what, e, delay)
                time.sleep(delay)
                delay = min(delay * 2, 5.0)

    def run(self) -> dict:
        self._wait("Redis connect", self.r.ping)
        self._wait("create group", self.ensure_group)
        delay = 0.5
        while not self.stop:
            try:
                if self.need_pending:
                    self.need_pending = False
                    self.drain_own_pending()
                    continue
                if time.monotonic() - self.last_reclaim >= self.a.reclaim_interval:
                    self.last_reclaim = time.monotonic()
                    self.reclaim()
                entries = self._read(">", self.a.block_ms)
                if entries:
                    self.handle(entries)
                elif self.a.once:
                    break
                delay = 0.5
            except (redis.ConnectionError, redis.TimeoutError, DbUnavailable) as e:
                # Nothing was acked: the batch is still in our pending list.
                log.warning("%s; batch left pending, retrying in %.1fs", e, delay)
                self.need_pending = True
                time.sleep(delay)
                delay = min(delay * 2, 5.0)
            self.push_metrics()
        self.push_metrics(force=True)
        summary = {"mode": self.a.mode, "consumer": self.a.consumer, **self.counts}
        log.info("FINAL %s", json.dumps(summary))
        return summary


def build_parser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(prog="storer", description="Stage 5: store tx.explained / tx.dlq in Postgres")
    ap.add_argument("--mode", choices=sorted(MODES), required=True)
    ap.add_argument("--consumer", default=None, help="fixed consumer name (default storer-1 / dlq-storer-1)")
    ap.add_argument("--count", type=int, default=500, help="entries per XREADGROUP")
    ap.add_argument("--block-ms", type=int, default=200)
    ap.add_argument("--reclaim-interval", type=float, default=15.0, help="seconds between XAUTOCLAIM sweeps")
    ap.add_argument("--metrics-interval", type=float, default=1.0)
    ap.add_argument("--dedupe-size", type=int, default=500_000, help="DLQ entry IDs remembered")
    ap.add_argument("--once", action="store_true", help="exit when the stream is drained (tests)")
    ap.add_argument("--log-level", default="INFO")
    return ap


def main(argv: Optional[Iterable[str]] = None) -> int:
    args = build_parser().parse_args(argv)
    args.consumer = args.consumer or MODES[args.mode][2]
    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO),
                        format="%(asctime)s %(levelname)s %(name)s %(message)s")
    r = redis.Redis.from_url(os.environ.get("REDIS_URL", "redis://localhost:6379/0"))
    storer = Storer(r, PgSink(), args)

    def on_signal(signum: int, _frame: Any) -> None:
        log.info("signal %d: finishing the current batch, then exiting", signum)
        storer.stop = True

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)
    log.info("stage 5 %s: %s (group %s) as %s -> postgres %s/%s", args.mode, storer.stream, storer.group,
             args.consumer, os.environ.get("PGHOST", "localhost"), os.environ.get("PGDATABASE", ""))
    print(json.dumps(storer.run()))
    return 0


if __name__ == "__main__":
    sys.exit(main())
