"""
stream_worker.py — stage 3 connected to the pipeline (what the container runs).

    Redis tx.valid  ──▶  this worker (score in batches)  ──▶  Redis tx.scored
                                      └── broken events ──▶  Redis tx.dlq

How it talks to Redis (same rules as Albert's validator):
  - Reads tx.valid as consumer group "scorers". Each stream entry's `data` field is
    one event (event.schema.json).
  - Scores everything that's waiting in one batch (up to --count). At low traffic a
    batch is 1 event (low delay); at high traffic batches fill up (high throughput).
  - Writes each scored event to tx.scored, then XACKs ("done") the input entries.
    If the worker dies before XACK, Redis hands those entries back on restart.
  - Repeated event_ids are skipped (they'd double-count card history) and acked.
  - The original event is copied into the scored event byte-for-byte, as the
    contract asks: we paste the received text in rather than re-writing it.

ONLY RUN ONE SCORER. Card history lives in this process's memory; two scorers would
each see half of every card's payments and compute wrong features.

    REDIS_URL=redis://localhost:6379/0 python stream_worker.py --warm-up /data/fraudTrain.csv

Live numbers for the dashboard go to the Redis hash `metrics:scorer`.
"""

import argparse
import json
import logging
import os
import signal
import socket
import time

import redis

from scorer import DuplicateEvent, FraudScorer

log = logging.getLogger("stage3")


def wrap_scored(raw_event_text, scoring):
    """The scored event as JSON text, with the original event pasted in unchanged.
    Same shape as scored_event.schema.json: schema_version, event, scoring, explanation."""
    return ('{"schema_version": "1.0.0", "event": ' + raw_event_text
            + ', "scoring": ' + json.dumps(scoring) + ', "explanation": null}')


def percentile(values, q):
    if not values:
        return None
    s = sorted(values)
    return s[min(len(s) - 1, int(q * len(s)))]


class Stage3Worker:
    def __init__(self, r, scorer, args):
        self.r, self.scorer, self.a = r, scorer, args
        self.stop = False
        self.counts = {"scored": 0, "allow": 0, "review": 0, "block": 0, "duplicates": 0, "dlq": 0}
        self.recent = []            # (time, scoring_duration_ms) for live p50/p95 and events/s
        self.last_metrics = 0.0
        self.warned_old = False

    # ---- setup ---------------------------------------------------------------
    def ensure_group(self):
        try:  # "0" = start from the beginning of tx.valid the first time the group is made
            self.r.xgroup_create(self.a.in_stream, self.a.group, id="0", mkstream=True)
            log.info("created consumer group %s on %s", self.a.group, self.a.in_stream)
        except redis.ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise

    def warn_if_other_scorers(self):
        try:
            consumers = self.r.xinfo_consumers(self.a.in_stream, self.a.group)
        except redis.ResponseError:
            return
        for c in consumers:
            name = c["name"].decode() if isinstance(c["name"], bytes) else c["name"]
            if name != self.a.consumer and (c.get("pending", 0) > 0 or c.get("idle", 10**9) < 60_000):
                log.warning("another scorer '%s' is active in group %s. Run only ONE scorer: "
                            "card history is kept in memory and would be split.", name, self.a.group)

    # ---- main loop -----------------------------------------------------------
    def retry_setup(self, step):
        """Startup steps (create group, check consumers) wait out a Redis blip instead of crashing."""
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
        self.retry_setup(self.warn_if_other_scorers)
        reading_pending, pending_cursor = True, "0"  # first finish anything we took before a crash/restart
        read_delay = 0.1
        handled = 0
        while not self.stop:
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
                handled += self.handle(entries)
            self.push_metrics()
            if self.a.max_events and handled >= self.a.max_events:
                break
        self.push_metrics(force=True)
        log.info("stopped: %s", self.counts)

    def handle(self, entries):
        ids, raws, events, metas = [], [], [], []
        pipe = self.r.pipeline(transaction=False)
        now_ms = int(time.time() * 1000)
        for entry_id, fields in entries:
            ids.append(entry_id)
            if not fields:  # entry was trimmed from the stream while pending: nothing to score, just ack
                continue
            raw = fields.get(b"data", b"")
            raw_text = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else str(raw)
            meta = {k: v for k, v in fields.items() if k in (b"producer_id", b"seq")}
            try:
                event = json.loads(raw_text)
                if not isinstance(event, dict):
                    raise ValueError("event is not a JSON object")
            except ValueError as e:
                self.dead_letter(pipe, raw_text, None, "MALFORMED_JSON", e, now_ms)
                continue
            raws.append(raw_text)
            events.append(event)
            metas.append(meta)

        results = self.scorer.score_batch(events) if events else []
        for raw_text, event, meta, result in zip(raws, events, metas, results):
            event_id = event.get("event_id")
            if isinstance(result, DuplicateEvent):
                self.counts["duplicates"] += 1
                continue
            if isinstance(result, Exception):
                self.dead_letter(pipe, raw_text, event_id, "SCORING_ERROR", result, now_ms)
                continue
            scoring = result[0]["scoring"]
            self.check_not_replaying_history(event)
            pipe.xadd(self.a.out_stream,
                      {"data": wrap_scored(raw_text, scoring), "event_id": event_id or "",
                       "scored_ts": str(int(time.time() * 1000)), "scorer_id": self.a.consumer, **meta},
                      maxlen=self.a.out_maxlen, approximate=True)
            self.counts["scored"] += 1
            self.counts[scoring["decision"].lower()] += 1
            self.recent.append((time.time(), scoring["scoring_duration_ms"]))

        # Write results, THEN mark the inputs done. If Redis hiccups, retry the same writes:
        # the events are already in card memory, so they must not be scored a second time.
        pipe.xack(self.a.in_stream, self.a.group, *ids)
        delay = 0.1
        while True:
            try:
                pipe.execute()
                break
            except (redis.ConnectionError, redis.TimeoutError) as e:
                if self.stop:
                    log.error("shutting down with an unwritten batch (it stays pending in Redis): %s", e)
                    break
                log.warning("Redis write failed (%s); retrying in %.1fs", e, delay)
                time.sleep(delay)
                delay = min(delay * 2, 5.0)
        return len(ids)

    def dead_letter(self, pipe, raw_text, event_id, reason, err, now_ms):
        self.counts["dlq"] += 1
        pipe.xadd(self.a.dlq_stream,
                  {"data": raw_text, "reason": reason, "event_id": event_id or "", "path": "/",
                   "message": str(err)[:1000], "errors": "[]", "dlq_ts": str(now_ms),
                   "stage": "scoring", "worker_id": self.a.consumer},
                  maxlen=self.a.out_maxlen, approximate=True)

    def check_not_replaying_history(self, event):
        """Warn once if the stream is older than the warm-up history: that usually means the
        producer is replaying the training file we warmed up from, so every payment would be
        counted twice in card history (and the model would be scoring data it trained on)."""
        until = getattr(self.scorer, "history_until", None)
        if self.warned_old or not until:
            return
        occurred = str(event.get("transaction", {}).get("occurred_at", ""))[:19]
        if occurred and occurred < until:
            self.warned_old = True
            log.warning("event at %s is older than the warm-up history (ends %s). Is the producer "
                        "replaying the warm-up/training file? Replay fraudTest.csv instead.", occurred, until)

    def push_metrics(self, force=False):
        now = time.time()
        if not force and now - self.last_metrics < self.a.metrics_interval:
            return
        self.last_metrics = now
        self.recent = [x for x in self.recent if now - x[0] <= 60]
        durations = [d for _, d in self.recent]
        last10 = sum(1 for t, _ in self.recent if now - t <= 10)
        payload = {**self.counts, "events_per_sec": round(last10 / 10, 2),
                   "p50_ms": percentile(durations, 0.50) or 0, "p95_ms": percentile(durations, 0.95) or 0,
                   "cards_tracked": len(self.scorer.memory.cards), "model_version": self.scorer.model_version,
                   "consumer": self.a.consumer, "updated_ms": int(now * 1000)}
        try:
            self.r.hset(self.a.metrics_key, mapping={k: str(v) for k, v in payload.items()})
        except (redis.ConnectionError, redis.TimeoutError):
            pass  # metrics are best-effort; never stop scoring for them


def build_parser():
    ap = argparse.ArgumentParser(description="Stage 3: score events from Redis tx.valid into tx.scored")
    ap.add_argument("--redis-url", default=os.environ.get("REDIS_URL", "redis://localhost:6379/0"))
    ap.add_argument("--in-stream", default="tx.valid")
    ap.add_argument("--out-stream", default="tx.scored")
    ap.add_argument("--dlq-stream", default="tx.dlq")
    ap.add_argument("--group", default="scorers")
    ap.add_argument("--consumer", default="scorer-1", help="keep this fixed so a restart picks up its own pending events")
    ap.add_argument("--count", type=int, default=100, help="max events per batch")
    ap.add_argument("--block-ms", type=int, default=1000)
    ap.add_argument("--out-maxlen", type=int, default=1_000_000)
    ap.add_argument("--warm-up", metavar="CSV", default=os.environ.get("WARM_UP_CSV"),
                    help="Sparkov CSV of earlier payments to load card history from (e.g. fraudTrain.csv)")
    ap.add_argument("--metrics-key", default="metrics:scorer")
    ap.add_argument("--metrics-interval", type=float, default=1.0)
    ap.add_argument("--max-events", type=int, default=0, help="stop after N input entries (tests)")
    return ap


def main(argv=None):
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [stage3] %(levelname)s %(message)s")
    args = build_parser().parse_args(argv)
    scorer = FraudScorer()
    if args.warm_up:
        t = time.time()
        n = scorer.warm_up_from_csv(args.warm_up)
        log.info("warm-up: %s past payments, %s cards, history until %s (%.0fs)",
                 f"{n:,}", f"{len(scorer.memory.cards):,}", scorer.history_until, time.time() - t)
    r = redis.Redis.from_url(args.redis_url, decode_responses=False)
    worker = Stage3Worker(r, scorer, args)

    def _stop(signum, frame):
        log.info("signal %s: finishing the current batch, then exiting", signum)
        worker.stop = True
    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)
    log.info("reading %s (group %s, consumer %s) on %s -> %s", args.in_stream, args.group,
             args.consumer, socket.gethostname(), args.out_stream)
    worker.run()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
