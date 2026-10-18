"""
stream_worker.py — stage 4 connected to the pipeline (what the container runs).

    Redis tx.scored  ──▶  this worker (fill `explanation`)  ──▶  Redis tx.explained  ──▶  stage 5

How it keeps the pipeline fast:
  - ALLOW events (~99.6%) and template sentences are done instantly and passed on.
  - Alerts that need the AI (watsonx / local) go to a few background threads, so a
    2-5 s AI call never holds up the payments behind it.
  - If too many alerts are already waiting for the AI (--max-ai-waiting), new alerts get
    the template sentence straight away (model "template") instead of queueing forever.
  - Each input entry is XACKed only after its explained event is written to tx.explained.
  - The original event text is kept byte-for-byte: only the trailing `"explanation": null`
    written by stage 3 is replaced.

Stage 4 has no memory between events, so you can run several copies (give each its own
--consumer name). Live numbers go to the Redis hash `metrics:explainer`.

    REDIS_URL=redis://localhost:6379/0 python stream_worker.py --backend template
    REDIS_URL=... LLAMA_URL=http://llm:8080 python stream_worker.py --backend local --timeout-ms 10000
"""

import argparse
import json
import logging
import os
import signal
import time
from concurrent.futures import ThreadPoolExecutor

import redis

from explainer import Explainer

log = logging.getLogger("stage4")
NULL_TAIL = ', "explanation": null}'  # how stage 3's stream_worker ends every scored event


def with_explanation(raw_scored_text, scored, explanation):
    """The explained event as JSON text, keeping stage 3's text (and so the original event) unchanged."""
    if raw_scored_text.endswith(NULL_TAIL):
        return raw_scored_text[: -len(NULL_TAIL)] + ', "explanation": ' + json.dumps(explanation) + "}"
    out = dict(scored)
    out["explanation"] = explanation
    return json.dumps(out)


def percentile(values, q):
    if not values:
        return None
    s = sorted(values)
    return s[min(len(s) - 1, int(q * len(s)))]


class Stage4Worker:
    def __init__(self, r, args):
        self.r, self.a = r, args
        self.stop = False
        self.ex = Explainer(backend=args.backend, timeout_ms=args.timeout_ms)
        self.template = Explainer(backend="template")
        self.uses_ai = args.backend != "template"
        self.pool = ThreadPoolExecutor(max_workers=args.ai_threads) if self.uses_ai else None
        self.waiting = []  # (future, entry_id, raw_text, scored, meta)
        self.counts = {"explained": 0, "ok": 0, "skipped": 0, "failed": 0, "timeout": 0,
                       "ai_busy_used_template": 0, "dlq": 0}
        self.recent = []  # (time, duration_ms) of alert explanations
        self.last_metrics = 0.0

    def ensure_group(self):
        try:
            self.r.xgroup_create(self.a.in_stream, self.a.group, id="0", mkstream=True)
            log.info("created consumer group %s on %s", self.a.group, self.a.in_stream)
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    def claim_from_dead_workers(self):
        """Take over entries another stage 4 worker took but never finished (it crashed).
        They become our own pending entries, which run() reads first. Small pages keep replies small."""
        start = "0-0"
        while True:
            resp = self.r.xautoclaim(self.a.in_stream, self.a.group, self.a.consumer,
                                     min_idle_time=self.a.min_idle_ms, start_id=start, count=50)
            nxt, claimed = resp[0], resp[1]
            start = nxt.decode() if isinstance(nxt, bytes) else str(nxt)
            if claimed:
                log.info("took over %d unfinished entries from a stopped worker", len(claimed))
            if start == "0-0":
                return

    def retry_setup(self, step):
        """Startup steps (create group, claim) wait out a Redis blip instead of crashing."""
        wait = 0.5
        while not self.stop:
            try:
                return step()
            except (redis.ConnectionError, redis.TimeoutError) as e:
                log.warning("Redis setup step failed (%s); retrying in %.1fs", e, wait)
                time.sleep(wait)
                wait = min(wait * 2, 5.0)

    def run(self):
        wait = 0.5
        while not self.stop:
            try:
                self.r.ping()
                break
            except (redis.ConnectionError, redis.TimeoutError) as e:
                log.warning("Redis not reachable yet (%s); retrying in %.1fs", e, wait)
                time.sleep(wait)
                wait = min(wait * 2, 5.0)
        self.retry_setup(self.ensure_group)
        self.retry_setup(self.claim_from_dead_workers)
        reading_pending, pending_cursor, handled = True, "0", 0  # first finish our unfinished entries
        read_delay = 0.1
        while True:
            self.collect_finished_ai()
            if self.stop or (self.a.max_events and handled >= self.a.max_events):
                break
            try:
                resp = self.r.xreadgroup(self.a.group, self.a.consumer,
                                         {self.a.in_stream: pending_cursor if reading_pending else ">"},
                                         count=self.a.count,
                                         block=None if reading_pending else self.a.block_ms)
                read_delay = 0.1
            except (redis.ConnectionError, redis.TimeoutError) as e:
                # Redis blipped: wait and try again. Nothing is lost, unacked entries stay in Redis.
                log.warning("Redis read failed (%s); retrying in %.1fs", e, read_delay)
                time.sleep(read_delay)
                read_delay = min(read_delay * 2, 5.0)
                continue
            entries = resp[0][1] if resp else []
            if reading_pending:
                if not entries:
                    reading_pending = False
                    continue
                pending_cursor = entries[-1][0]  # walk through our unfinished entries once, in order
            if entries:
                handled += len(entries)
                self.handle(entries)
            self.push_metrics()
        # finish alerts still with the AI before exiting (each has its own timeout)
        deadline = time.time() + self.a.timeout_ms / 1000 + 2
        while self.waiting and time.time() < deadline:
            self.collect_finished_ai()
            time.sleep(0.05)
        if self.pool:
            self.pool.shutdown(wait=False)
        self.push_metrics(force=True)
        log.info("stopped: %s", self.counts)

    def handle(self, entries):
        pipe = self.r.pipeline(transaction=False)
        acks = []  # entries finished in this batch; acked with ONE XACK after their results are queued
        for entry_id, fields in entries:
            if not fields:  # entry was trimmed from the stream while pending: nothing to explain, just ack
                acks.append(entry_id)
                continue
            raw = fields.get(b"data", b"")
            raw_text = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else str(raw)
            meta = {k: v for k, v in fields.items() if k in (b"producer_id", b"seq", b"scored_ts")}
            try:
                scored = json.loads(raw_text)
                if not isinstance(scored, dict):
                    raise ValueError("scored event is not a JSON object")
            except ValueError as e:
                self.counts["dlq"] += 1
                pipe.xadd(self.a.dlq_stream,
                          {"data": raw_text, "reason": "MALFORMED_JSON", "event_id": "", "path": "/",
                           "message": str(e)[:1000], "errors": "[]", "dlq_ts": str(int(time.time() * 1000)),
                           "stage": "explanation", "worker_id": self.a.consumer},
                          maxlen=self.a.out_maxlen, approximate=True)
                acks.append(entry_id)
                continue

            decision = (scored.get("scoring") or {}).get("decision")
            if not self.uses_ai or decision == "ALLOW":
                self.emit(pipe, acks, entry_id, raw_text, scored, self.ex.explain(scored), meta)
            elif len(self.waiting) < self.a.max_ai_waiting:
                fut = self.pool.submit(self.ex.explain, scored)
                self.waiting.append((fut, entry_id, raw_text, scored, meta))
            else:  # AI is backed up: don't let alerts pile up, use the instant template sentence
                self.counts["ai_busy_used_template"] += 1
                self.emit(pipe, acks, entry_id, raw_text, scored, self.template.explain(scored), meta)
        if acks:
            pipe.xack(self.a.in_stream, self.a.group, *acks)
        self.execute(pipe)

    def collect_finished_ai(self):
        done = [w for w in self.waiting if w[0].done()]
        if not done:
            return
        self.waiting = [w for w in self.waiting if not w[0].done()]
        pipe = self.r.pipeline(transaction=False)
        acks = []
        for fut, entry_id, raw_text, scored, meta in done:
            try:
                explained = fut.result()
            except Exception as e:  # explain() shouldn't raise, but a row must still be written
                log.error("explain crashed: %s", e)
                explained = self.template.explain(scored)
            self.emit(pipe, acks, entry_id, raw_text, scored, explained, meta)
        pipe.xack(self.a.in_stream, self.a.group, *acks)
        self.execute(pipe)

    def emit(self, pipe, acks, entry_id, raw_text, scored, explained, meta):
        expl = explained["explanation"]
        event_id = ((scored.get("event") or {}).get("event_id")) or ""
        pipe.xadd(self.a.out_stream,
                  {"data": with_explanation(raw_text, scored, expl), "event_id": event_id,
                   "explained_ts": str(int(time.time() * 1000)), "explainer_id": self.a.consumer, **meta},
                  maxlen=self.a.out_maxlen, approximate=True)
        acks.append(entry_id)
        self.counts["explained"] += 1
        self.counts[expl["status"]] = self.counts.get(expl["status"], 0) + 1
        if expl["status"] != "skipped":
            self.recent.append((time.time(), expl.get("duration_ms", 0)))

    def execute(self, pipe):
        delay = 0.1
        while True:
            try:
                pipe.execute()
                return
            except (redis.ConnectionError, redis.TimeoutError) as e:
                if self.stop:
                    log.error("shutting down with unwritten results (inputs stay pending in Redis): %s", e)
                    return
                log.warning("Redis write failed (%s); retrying in %.1fs", e, delay)
                time.sleep(delay)
                delay = min(delay * 2, 5.0)

    def push_metrics(self, force=False):
        now = time.time()
        if not force and now - self.last_metrics < self.a.metrics_interval:
            return
        self.last_metrics = now
        self.recent = [x for x in self.recent if now - x[0] <= 60]
        d = [x for _, x in self.recent]
        payload = {**self.counts, "ai_waiting": len(self.waiting), "backend": self.ex.backend.name,
                   "alert_p50_ms": percentile(d, 0.50) or 0, "alert_p95_ms": percentile(d, 0.95) or 0,
                   "consumer": self.a.consumer, "updated_ms": int(now * 1000)}
        try:
            self.r.hset(self.a.metrics_key, mapping={k: str(v) for k, v in payload.items()})
        except (redis.ConnectionError, redis.TimeoutError):
            pass


def build_parser():
    ap = argparse.ArgumentParser(description="Stage 4: explain scored events from Redis tx.scored into tx.explained")
    ap.add_argument("--redis-url", default=os.environ.get("REDIS_URL", "redis://localhost:6379/0"))
    ap.add_argument("--in-stream", default="tx.scored")
    ap.add_argument("--out-stream", default="tx.explained")
    ap.add_argument("--dlq-stream", default="tx.dlq")
    ap.add_argument("--group", default="explainers")
    ap.add_argument("--consumer", default=os.environ.get("CONSUMER", "explainer-1"))
    ap.add_argument("--backend", choices=["template", "watsonx", "local"], default=os.environ.get("BACKEND", "template"))
    ap.add_argument("--timeout-ms", type=int, default=int(os.environ.get("TIMEOUT_MS", "800")))
    ap.add_argument("--ai-threads", type=int, default=2, help="AI calls in flight at once")
    ap.add_argument("--max-ai-waiting", type=int, default=8, help="beyond this, alerts get the template sentence")
    ap.add_argument("--count", type=int, default=100)
    ap.add_argument("--block-ms", type=int, default=100, help="short, so finished AI sentences are sent on quickly")
    ap.add_argument("--min-idle-ms", type=int, default=30_000)
    ap.add_argument("--out-maxlen", type=int, default=1_000_000)
    ap.add_argument("--metrics-key", default="metrics:explainer")
    ap.add_argument("--metrics-interval", type=float, default=1.0)
    ap.add_argument("--max-events", type=int, default=0, help="stop after N input entries (tests)")
    return ap


def main(argv=None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [stage4] %(levelname)s %(message)s")
    args = build_parser().parse_args(argv)
    r = redis.Redis.from_url(args.redis_url, decode_responses=False)
    worker = Stage4Worker(r, args)  # fails loudly here if watsonx/local is misconfigured

    def _stop(signum, frame):
        log.info("signal %s: finishing in-flight work, then exiting", signum)
        worker.stop = True
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    log.info("reading %s (group %s, consumer %s) -> %s, backend %s",
             args.in_stream, args.group, args.consumer, args.out_stream, worker.ex.backend.name)
    worker.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
