"""Stage 5 storer tests.

Unit tests use fakeredis and an in-memory sink. Tests marked ``pg`` run
against a real PostgreSQL with Albert/New stuff/schema.sql applied; they need
the PG* environment variables (see README "Tests") and are skipped without
them so the unit tests still run anywhere.
"""

from __future__ import annotations

import argparse
import importlib.util
import json
import os
import random
import sys
import time
from pathlib import Path
from typing import Any

import fakeredis
import pytest
import redis as _redis

HERE = Path(__file__).resolve().parent
STAGE = HERE.parent
sys.path.insert(0, str(STAGE))

import storer as st  # noqa: E402

_spec = importlib.util.spec_from_file_location("stubgen_t", st.STUBGEN_PATH)
stubgen = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(stubgen)
SCHEMA_SQL = st.STUBGEN_PATH.parent / "schema.sql"


def _args(mode: str = "explained", **kw: Any) -> argparse.Namespace:
    a = st.build_parser().parse_args(["--mode", mode, "--once"])
    a.consumer = kw.pop("consumer", st.MODES[mode][2])
    a.block_ms = 10
    a.reclaim_interval = 0
    for k, v in kw.items():
        setattr(a, k, v)
    return a


def _scored(rng: random.Random, run_id: str = "t-run") -> dict:
    return stubgen.make_scored(rng, stubgen.make_event(rng, run_id=run_id))


def _publish_explained(r: Any, docs: list[dict]) -> list[str]:
    return [r.xadd("tx.explained", {"data": json.dumps(d), "event_id": d["event"]["event_id"]}) for d in docs]


def _pending(r: Any, stream: str, group: str) -> int:
    try:
        return r.xpending(stream, group)["pending"]
    except _redis.ResponseError:
        return 0


class FakeSink:
    """In-memory stand-in for PgSink with ON CONFLICT semantics and a switch
    that makes the database 'go down'."""

    def __init__(self) -> None:
        self.table: dict[str, dict] = {}
        self.dlq: list[dict] = []
        self.staged: list[tuple[str, dict]] = []
        self.down = 0  # how many upcoming commits fail
        self.commits = 0

    def savepoint(self, name: str) -> None:
        pass

    def rollback_to(self, name: str) -> None:
        pass

    def insert_explained(self, rows: list[dict]) -> int:
        new = 0
        staged_ids = {k for kind, k in ((s[0], s[1].get("event_id")) for s in self.staged) if kind == "x"}
        for row in rows:
            if row["event_id"] in self.table or row["event_id"] in staged_ids:
                continue
            staged_ids.add(row["event_id"])
            self.staged.append(("x", row))
            new += 1
        return new

    def insert_dlq(self, rows: list[dict], recovered: bool) -> int:
        for row in rows:
            self.staged.append(("d", row))
        return len(rows)

    def commit(self) -> None:
        if self.down:
            self.down -= 1
            raise st.DbUnavailable("simulated outage")
        for kind, row in self.staged:
            if kind == "x":
                self.table[row["event_id"]] = row
            else:
                self.dlq.append(row)
        self.staged = []
        self.commits += 1

    def rollback(self) -> None:
        self.staged = []


@pytest.fixture
def r() -> Any:
    return fakeredis.FakeRedis()


# ---------------------------------------------------------------------------
# Unit tests (no Postgres)
# ---------------------------------------------------------------------------


def test_rows_are_exactly_stubgen_to_row(r: Any) -> None:
    rng = random.Random(1)
    docs = [_scored(rng) for _ in range(20)]
    _publish_explained(r, docs)
    sink = FakeSink()
    st.Storer(r, sink, _args()).run()
    assert len(sink.table) == 20
    for d in docs:
        assert sink.table[d["event"]["event_id"]] == stubgen.to_row(d)
    # committed_at / latency_ms are the database's job, never sent.
    assert not {"committed_at", "latency_ms"} & set(next(iter(sink.table.values())))
    assert _pending(r, "tx.explained", "storers") == 0


def test_storer_imports_to_row_from_the_contract_folder() -> None:
    assert st.STUBGEN_PATH.parent.name == "New stuff"
    assert st.load_to_row().__code__.co_filename == str(st.STUBGEN_PATH)


def test_nothing_is_acked_until_commit_and_failed_batch_is_retried(r: Any) -> None:
    rng = random.Random(2)
    docs = [_scored(rng) for _ in range(30)]
    _publish_explained(r, docs)
    sink = FakeSink()
    sink.down = 1  # the first commit fails
    s = st.Storer(r, sink, _args())
    s.ensure_group()
    entries = s._read(">", None)
    with pytest.raises(st.DbUnavailable):
        s.handle(entries)
    assert sink.table == {}
    assert _pending(r, "tx.explained", "storers") == 30, "acked before the commit succeeded"
    # The run loop picks the batch up again from its own pending list.
    st.Storer(r, sink, _args()).run()
    assert set(sink.table) == {d["event"]["event_id"] for d in docs}
    assert _pending(r, "tx.explained", "storers") == 0


def test_db_outage_in_run_loop_retries_without_loss(r: Any, monkeypatch: Any) -> None:
    monkeypatch.setattr(st.time, "sleep", lambda s: None)
    rng = random.Random(3)
    docs = [_scored(rng) for _ in range(10)]
    _publish_explained(r, docs)
    sink = FakeSink()
    sink.down = 3
    st.Storer(r, sink, _args()).run()
    assert len(sink.table) == 10 and sink.commits >= 1
    assert _pending(r, "tx.explained", "storers") == 0


def test_own_pending_entries_are_processed_first_after_restart(r: Any) -> None:
    rng = random.Random(4)
    old = [_scored(rng) for _ in range(5)]
    _publish_explained(r, old)
    s = st.Storer(r, FakeSink(), _args())
    s.ensure_group()
    s._read(">", None)  # delivered to storer-1, then the process "dies"
    new = [_scored(rng) for _ in range(5)]
    _publish_explained(r, new)
    sink = FakeSink()
    order: list[bool] = []
    s2 = st.Storer(r, sink, _args())
    real = s2.handle
    s2.handle = lambda e, recovered=False: (order.append(recovered), real(e, recovered))[1]  # type: ignore
    s2.run()
    assert len(sink.table) == 10
    assert order[0] is True, "pending entries must be handled before new ones"
    assert _pending(r, "tx.explained", "storers") == 0


def test_dead_consumers_entries_are_reclaimed(r: Any, monkeypatch: Any) -> None:
    monkeypatch.setattr(st, "RECLAIM_IDLE_MS", 0)
    rng = random.Random(5)
    docs = [_scored(rng) for _ in range(8)]
    _publish_explained(r, docs)
    dead = st.Storer(r, FakeSink(), _args(consumer="storer-dead"))
    dead.ensure_group()
    dead._read(">", None)
    sink = FakeSink()
    st.Storer(r, sink, _args(consumer="storer-1")).run()
    assert len(sink.table) == 8
    assert _pending(r, "tx.explained", "storers") == 0


def test_repeated_event_id_is_ignored(r: Any) -> None:
    rng = random.Random(6)
    docs = [_scored(rng) for _ in range(5)]
    _publish_explained(r, docs + docs[:3])  # at-least-once upstream repeats
    sink = FakeSink()
    s = st.Storer(r, sink, _args())
    s.run()
    assert len(sink.table) == 5
    assert s.counts["stored"] == 5 and s.counts["duplicates_ignored"] == 3


def test_broken_scored_event_goes_to_dlq_and_batch_continues(r: Any) -> None:
    rng = random.Random(7)
    good = _scored(rng)
    r.xadd("tx.explained", {"data": b"{not json", "event_id": b""})
    r.xadd("tx.explained", {"data": json.dumps({"event": {}}), "event_id": b""})
    _publish_explained(r, [good])
    sink = FakeSink()
    st.Storer(r, sink, _args()).run()
    assert list(sink.table) == [good["event"]["event_id"]]
    dlq = r.xrange("tx.dlq")
    assert [f[b"stage"] for _, f in dlq] == [b"storage", b"storage"]
    assert {f[b"reason"] for _, f in dlq} == {b"MALFORMED_SCORED_EVENT"}
    assert dlq[0][1][b"data"] == b"{not json"
    assert _pending(r, "tx.explained", "storers") == 0


def test_dlq_row_mapping() -> None:
    ev = stubgen.make_event(random.Random(8), run_id="loadtest-1")
    raw = json.dumps(ev)
    row = st.dlq_row("1-0", {b"data": raw.encode(), b"reason": b"TYPE_ERROR", b"message": b"bad amount",
                             b"event_id": ev["event_id"].encode(), b"dlq_ts": b"1792256880136",
                             b"stage": b"validation", b"run_id": b"loadtest-1"})
    assert row == {"entry_id": "1-0", "run_id": "loadtest-1", "event_id": ev["event_id"],
                   "rejected_ms": 1792256880136, "stage": "validation", "reason_code": "TYPE_ERROR",
                   "reason_detail": "bad amount", "raw_payload": raw}
    # Stages 3/4 send no run_id field: it comes from the payload when it parses.
    row = st.dlq_row("2-0", {b"data": raw.encode(), b"reason": b"SCORING_ERROR", b"stage": b"scoring"})
    assert row["run_id"] == "loadtest-1" and row["event_id"] == ev["event_id"]


@pytest.mark.parametrize("data, event_id", [
    (b"{not json", b""),
    (b"\xff\xfe broken \x00 bytes", b""),
    (b'{"event_id": "not-a-uuid"}', b"not-a-uuid"),
    (b"", b""),
])
def test_malformed_dlq_payloads_map_safely(data: bytes, event_id: bytes) -> None:
    row = st.dlq_row("3-0", {b"data": data, b"reason": b"MALFORMED_JSON", b"event_id": event_id})
    assert row["event_id"] is None  # never a value the UUID column would reject
    assert row["run_id"] is None  # not derivable -> the column default
    assert "\x00" not in row["raw_payload"]  # Postgres TEXT can't hold NUL
    assert isinstance(row["raw_payload"], str)


def test_dlq_entry_ids_are_deduped_in_process(r: Any) -> None:
    for i in range(4):
        r.xadd("tx.dlq", {"data": f"bad-{i}", "reason": "MALFORMED_JSON", "stage": "validation",
                          "dlq_ts": str(1792256880000 + i)})
    sink = FakeSink()
    s = st.Storer(r, sink, _args("dlq"))
    s.ensure_group()
    entries = s._read(">", None)
    s.handle(entries)
    s.handle(entries)  # same stream entries again (e.g. an XACK that never landed)
    assert len(sink.dlq) == 4
    assert s.counts["duplicates_ignored"] == 4


def test_metrics_hash_has_both_modes_without_clobbering(r: Any) -> None:
    _publish_explained(r, [_scored(random.Random(9))])
    r.xadd("tx.dlq", {"data": "x", "reason": "MALFORMED_JSON", "stage": "validation"})
    st.Storer(r, FakeSink(), _args()).run()
    st.Storer(r, FakeSink(), _args("dlq")).run()
    m = {k.decode(): v.decode() for k, v in r.hgetall(st.METRICS_KEY).items()}
    assert m["stored"] == "1" and m["dlq_stored"] == "1"
    assert m["consumer"] == "storer-1" and m["dlq_consumer"] == "dlq-storer-1"
    assert {"duplicates_ignored", "batches", "rows_per_sec", "updated_ms"} <= set(m)


def test_sigterm_flag_finishes_current_batch(r: Any) -> None:
    rng = random.Random(10)
    _publish_explained(r, [_scored(rng) for _ in range(50)])
    sink = FakeSink()
    s = st.Storer(r, sink, _args(once=False))
    real = s.handle

    def handle_then_stop(entries: Any, recovered: bool = False) -> None:
        s.stop = True  # SIGTERM arrives while this batch is in flight
        real(entries, recovered)

    s.handle = handle_then_stop  # type: ignore[assignment]
    s.run()
    assert len(sink.table) == 50
    assert _pending(r, "tx.explained", "storers") == 0


# ---------------------------------------------------------------------------
# Real PostgreSQL
# ---------------------------------------------------------------------------

pg = pytest.mark.skipif(not os.environ.get("PGHOST"), reason="needs a PostgreSQL (set PG* env vars)")


@pytest.fixture
def db() -> Any:
    import psycopg2

    conn = psycopg2.connect("")
    conn.autocommit = True
    with conn.cursor() as c:
        c.execute(SCHEMA_SQL.read_text())
        c.execute("TRUNCATE scored_transactions, dead_letter_events")
    yield conn
    conn.close()


def _q(conn: Any, sql: str) -> Any:
    with conn.cursor() as c:
        c.execute(sql)
        return c.fetchall()


@pg
def test_pg_on_conflict_ignores_repeats_and_db_clock_sets_committed_at(r: Any, db: Any) -> None:
    rng = random.Random(11)
    docs = [_scored(rng) for _ in range(25)]
    _publish_explained(r, docs)
    st.Storer(r, st.PgSink(), _args()).run()
    _publish_explained(r, docs)  # the same events again
    s = st.Storer(r, st.PgSink(), _args())
    s.run()
    assert s.counts == {"stored": 0, "duplicates_ignored": 25, "batches": 1, "rejected": 0}
    (n, distinct, bad_clock, null_latency), = _q(db, """
        SELECT count(*), count(DISTINCT event_id),
               count(*) FILTER (WHERE committed_at < emitted_at),
               count(*) FILTER (WHERE latency_ms IS NULL) FROM scored_transactions""")
    assert (n, distinct, bad_clock, null_latency) == (25, 25, 0, 0)
    # A whole stored row round-trips through to_row's mapping.
    (decision, reasons), = _q(db, f"SELECT decision, scoring_reasons FROM scored_transactions "
                                  f"WHERE event_id = '{docs[0]['event']['event_id']}'")
    assert decision == docs[0]["scoring"]["decision"]
    assert reasons == docs[0]["scoring"].get("reasons")


@pg
def test_pg_clock_timestamp_is_per_row_now_is_per_transaction(db: Any) -> None:
    # Why committed_at relies on the column default clock_timestamp(): in one
    # batch transaction now() is frozen at BEGIN, clock_timestamp() advances.
    db.autocommit = False
    with db.cursor() as c:
        c.execute("SELECT now(), clock_timestamp(), pg_sleep(0.05), now(), clock_timestamp()")
        n1, c1, _, n2, c2 = c.fetchone()
    db.rollback()
    db.autocommit = True
    assert n1 == n2
    assert (c2 - c1).total_seconds() >= 0.05


@pg
def test_pg_bad_row_is_isolated_and_dead_lettered(r: Any, db: Any) -> None:
    rng = random.Random(12)
    docs = [_scored(rng) for _ in range(5)]
    bad = _scored(rng)
    bad["scoring"]["decision"] = "MAYBE"  # violates the CHECK constraint
    _publish_explained(r, docs[:2] + [bad] + docs[2:])
    s = st.Storer(r, st.PgSink(), _args())
    s.run()
    assert _q(db, "SELECT count(*) FROM scored_transactions")[0][0] == 5
    assert s.counts["rejected"] == 1
    (_, f), = r.xrange("tx.dlq")
    assert f[b"reason"] == b"DB_REJECTED" and f[b"stage"] == b"storage"
    assert _pending(r, "tx.explained", "storers") == 0


@pg
def test_pg_malformed_dlq_payloads_never_fail_the_batch(r: Any, db: Any) -> None:
    payloads = [b"{not json", b"\xff\xfe broken \x00 bytes", b'{"event_id": "not-a-uuid"}', b"",
                json.dumps(stubgen.make_event(random.Random(13), run_id="r-13")).encode()]
    for i, p in enumerate(payloads):
        r.xadd("tx.dlq", {"data": p, "reason": "MALFORMED_JSON", "event_id": b"not-a-uuid" if i == 2 else b"",
                          "message": "m", "dlq_ts": str(1792256880000 + i), "stage": "validation"})
    r.xadd("tx.dlq", {"data": b"no dlq_ts / stage / reason fields"})
    st.Storer(r, st.PgSink(), _args("dlq")).run()
    rows = _q(db, "SELECT run_id, event_id, stage, reason_code, raw_payload FROM dead_letter_events ORDER BY dlq_id")
    assert len(rows) == 6
    assert rows[1][4] == "\\xff\\xfe broken \\x00 bytes"
    assert rows[2][1] is None
    assert rows[4][0] == "r-13" and rows[4][1] is not None
    assert rows[5][:4] == ("default", None, "validation", "UNKNOWN")
    assert _pending(r, "tx.dlq", "dlq-storers") == 0


@pg
def test_pg_dlq_commit_without_ack_is_not_stored_twice_after_restart(r: Any, db: Any) -> None:
    for i in range(3):
        r.xadd("tx.dlq", {"data": f"bad-{i}", "reason": "MALFORMED_JSON", "stage": "validation",
                          "dlq_ts": str(1792256880000 + i)})
    s = st.Storer(r, st.PgSink(), _args("dlq"))
    s.ensure_group()
    entries = s._read(">", None)
    rows, _, _ = s._map(entries, False)
    s.sink.insert_dlq([{k: v for k, v in x.items() if k != "entry_id"} for x in rows], False)
    s.sink.commit()  # committed, then the process dies before XACK
    st.Storer(r, st.PgSink(), _args("dlq")).run()  # fresh process: replays its pending list
    assert _q(db, "SELECT count(*) FROM dead_letter_events")[0][0] == 3
    assert _pending(r, "tx.dlq", "dlq-storers") == 0


@pg
def test_pg_outage_then_recovery_loses_nothing(r: Any, db: Any, monkeypatch: Any) -> None:
    import psycopg2

    monkeypatch.setattr(st.time, "sleep", lambda s: None)
    rng = random.Random(14)
    _publish_explained(r, [_scored(rng) for _ in range(40)])
    fails = {"n": 2}

    def flaky_connect() -> Any:
        if fails["n"]:
            fails["n"] -= 1
            raise psycopg2.OperationalError("simulated: connection refused")
        return psycopg2.connect("")

    st.Storer(r, st.PgSink(connect=flaky_connect), _args()).run()
    assert _q(db, "SELECT count(*) FROM scored_transactions")[0][0] == 40
    assert _pending(r, "tx.explained", "storers") == 0
