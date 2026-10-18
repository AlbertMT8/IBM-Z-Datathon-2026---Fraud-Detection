"""ZSCORE live dashboard: one HTML page plus GET /api/state (JSON).

Pure Python server side: the standard library's http.server, redis-py and
psycopg2 (Debian packages on s390x). Reads Postgres (the views in
Albert/New stuff/schema.sql wherever one fits) and the per-stage Redis hashes
metrics:validator / scorer / explainer / storer, exactly as they are.

Config only via env: REDIS_URL, PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD,
and PORT (default 8080). Inside the container it binds 0.0.0.0; publish it on
the host's loopback only (-p 127.0.0.1:8080:8080).
"""

from __future__ import annotations

import json
import logging
import os
import signal
import threading
import time
from datetime import date, datetime
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable

import redis

log = logging.getLogger("dashboard")
HERE = Path(__file__).resolve().parent
METRICS_KEYS = ("metrics:validator", "metrics:scorer", "metrics:explainer", "metrics:storer")


def _json_default(o: Any) -> Any:
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    if isinstance(o, Decimal):
        return float(o)
    return str(o)


class Db:
    """One autocommit connection, reopened after any failure."""

    def __init__(self) -> None:
        import psycopg2  # Debian python3-psycopg2
        import psycopg2.extras

        self._pg, self._extras = psycopg2, psycopg2.extras
        self._conn = None
        self._lock = threading.Lock()

    def query(self, sql: str, params: Any = None) -> list[dict]:
        with self._lock:
            try:
                if self._conn is None or self._conn.closed:
                    self._conn = self._pg.connect("")  # libpq reads the PG* env vars
                    self._conn.autocommit = True
                with self._conn.cursor(cursor_factory=self._extras.RealDictCursor) as cur:
                    cur.execute(sql, params)
                    return [dict(r) for r in cur.fetchall()]
            except self._pg.Error:
                try:
                    if self._conn is not None:
                        self._conn.close()
                finally:
                    self._conn = None
                raise


# Queries. View names come from schema.sql; the rest exist because no view
# gives them (per-second series, p99 for a whole run, DLQ by stage).
SQL = {
    "alerts": "SELECT committed_at, amount, merchant, category, holder_city, holder_state, card_last4, "
              "decision, ensemble_score, explanation_text, explanation_status, is_fraud "
              "FROM v_alert_feed LIMIT 20",
    "latency_60s": "SELECT * FROM v_latency",
    "throughput_60s": "SELECT * FROM v_throughput",
    "series": "SELECT floor(extract(epoch FROM committed_at))::bigint AS t, count(*) AS n "
              "FROM scored_transactions WHERE committed_at > clock_timestamp() - INTERVAL '60 seconds' "
              "GROUP BY 1 ORDER BY 1",
    "current_run": "SELECT run_id FROM scored_transactions ORDER BY committed_at DESC LIMIT 1",
    "run_summary": "SELECT * FROM v_run_summary WHERE run_id = %(run)s",
    "run_latency": "SELECT count(*) AS n, "
                   "ROUND(PERCENTILE_CONT(0.50) WITHIN GROUP (ORDER BY latency_ms)::NUMERIC, 2) AS p50_ms, "
                   "ROUND(PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY latency_ms)::NUMERIC, 2) AS p95_ms, "
                   "ROUND(PERCENTILE_CONT(0.99) WITHIN GROUP (ORDER BY latency_ms)::NUMERIC, 2) AS p99_ms, "
                   "ROUND(MAX(latency_ms)::NUMERIC, 2) AS max_ms "
                   "FROM scored_transactions WHERE run_id = %(run)s",
    "decisions": "SELECT decision, count(*) AS n FROM scored_transactions WHERE run_id = %(run)s GROUP BY 1",
    "totals": "SELECT (SELECT count(*) FROM scored_transactions) AS rows_stored, "
              "(SELECT count(*) FROM dead_letter_events) AS dlq_stored",
    "dlq": "SELECT stage, reason_code, count(*) AS total, "
           "count(*) FILTER (WHERE rejected_at > clock_timestamp() - INTERVAL '60 seconds') AS last_60s, "
           "max(rejected_at) AS most_recent FROM dead_letter_events GROUP BY 1, 2 ORDER BY 3 DESC",
    "quality": "SELECT * FROM v_model_quality",
}


class State:
    def __init__(self, db: Any, r: Any) -> None:
        self.db, self.r = db, r
        self._cache: dict[str, tuple[float, Any]] = {}

    def _cached(self, key: str, ttl: float, fn: Callable[[], Any]) -> Any:
        hit = self._cache.get(key)
        if hit and time.monotonic() - hit[0] < ttl:
            return hit[1]
        val = fn()
        self._cache[key] = (time.monotonic(), val)
        return val

    def _section(self, out: dict, errors: dict, name: str, fn: Callable[[], Any]) -> None:
        try:
            out[name] = fn()
        except Exception as e:  # noqa: BLE001 — one failing panel must not blank the page
            errors[name] = f"{type(e).__name__}: {e}".strip()[:300]

    def build(self) -> dict:
        out: dict[str, Any] = {"server_time_ms": int(time.time() * 1000)}
        errors: dict[str, str] = {}
        q = self.db.query
        self._section(out, errors, "totals", lambda: q(SQL["totals"])[0])
        self._section(out, errors, "alerts", lambda: q(SQL["alerts"]))
        self._section(out, errors, "latency_60s", lambda: q(SQL["latency_60s"])[0])
        self._section(out, errors, "throughput_60s", lambda: q(SQL["throughput_60s"])[0])

        def series() -> list[dict]:
            now = int(time.time())
            got = {int(r["t"]): int(r["n"]) for r in q(SQL["series"])}
            return [{"t": t, "n": got.get(t, 0)} for t in range(now - 59, now + 1)]

        self._section(out, errors, "throughput_series", series)
        run = None
        try:
            rows = q(SQL["current_run"])
            run = rows[0]["run_id"] if rows else None
        except Exception as e:  # noqa: BLE001
            errors["current_run"] = str(e)[:300]
        out["run_id"] = run
        if run is not None:
            p = {"run": run}
            self._section(out, errors, "run_summary",
                          lambda: (q(SQL["run_summary"], p) or [None])[0])
            # Percentiles over a whole run sort every row: refresh every 5 s, not every poll.
            self._section(out, errors, "run_latency",
                          lambda: self._cached(f"rl:{run}", 5.0, lambda: q(SQL["run_latency"], p)[0]))
            self._section(out, errors, "decisions",
                          lambda: {d["decision"]: int(d["n"]) for d in q(SQL["decisions"], p)})
        self._section(out, errors, "dlq", lambda: q(SQL["dlq"]))
        self._section(out, errors, "quality", lambda: self._cached("quality", 5.0, lambda: q(SQL["quality"])[0]))

        def metrics() -> dict:
            pipe = self.r.pipeline(transaction=False)
            for k in METRICS_KEYS:
                pipe.hgetall(k)
            return {k: {f.decode(): v.decode() for f, v in sorted(h.items())}
                    for k, h in zip(METRICS_KEYS, pipe.execute())}

        self._section(out, errors, "stages", metrics)
        out["errors"] = errors
        return out


def make_handler(state: State, page: bytes) -> type:
    class Handler(BaseHTTPRequestHandler):
        server_version = "zscore-dashboard"

        def _send(self, code: int, body: bytes, ctype: str) -> None:
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def do_GET(self) -> None:  # noqa: N802
            path = self.path.split("?", 1)[0]
            if path == "/api/state":
                body = json.dumps(state.build(), default=_json_default).encode()
                self._send(200, body, "application/json")
            elif path in ("/", "/index.html"):
                self._send(200, page, "text/html; charset=utf-8")
            elif path == "/healthz":
                self._send(200, b"ok\n", "text/plain")
            else:
                self._send(404, b"not found\n", "text/plain")

        def log_message(self, fmt: str, *args: Any) -> None:
            log.debug("%s " + fmt, self.address_string(), *args)

    return Handler


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    r = redis.Redis.from_url(os.environ.get("REDIS_URL", "redis://localhost:6379/0"))
    state = State(Db(), r)
    page = (HERE / "index.html").read_bytes()
    port = int(os.environ.get("PORT", "8080"))
    server = ThreadingHTTPServer(("0.0.0.0", port), make_handler(state, page))
    log.info("dashboard on :%d (publish on the host's 127.0.0.1 only)", port)
    # PID 1 in a container ignores SIGTERM unless a handler is installed.
    signal.signal(signal.SIGTERM, lambda *_: threading.Thread(target=server.shutdown, daemon=True).start())
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
