"""
run.py — stage 3 as a running process (what the container starts).

Reads one event per line (JSON, event.schema.json) from stdin, writes one scored
event per line (scored_event.schema.json) to stdout. Works with any transport
that can be piped: a file, Albert's stubgen, or a queue consumer writing lines.

    ../.venv/bin/python make_events.py 1000 | ../.venv/bin/python run.py > scored.jsonl
    ../.venv/bin/python run.py --warm-up ../../Datasets/fraudTrain.csv < events.jsonl

An event that can't be scored doesn't crash the process: it is written to stderr
as one JSON line (stage, reason_code, reason_detail, raw_payload), the same fields
as Albert's dead_letter_events table.
"""

import argparse
import json
import sys

from scorer import DuplicateEvent, FraudScorer


def dead_letter(raw, code, detail):
    print(json.dumps({"stage": "scoring", "reason_code": code,
                      "reason_detail": str(detail)[:500], "raw_payload": raw}), file=sys.stderr, flush=True)


def main():
    ap = argparse.ArgumentParser(description="Stage 3: score events from stdin")
    ap.add_argument("--warm-up", metavar="CSV", help="Sparkov CSV to load card history from first")
    args = ap.parse_args()

    scorer = FraudScorer()
    if args.warm_up:
        n = scorer.warm_up_from_csv(args.warm_up)
        print(f"[stage3] warmed up with {n:,} past payments", file=sys.stderr, flush=True)

    for raw in sys.stdin:
        raw = raw.strip()
        if not raw:
            continue
        try:
            event = json.loads(raw)
        except json.JSONDecodeError as e:
            dead_letter(raw, "bad_json", e)
            continue
        try:
            scored = scorer.score(event)
        except DuplicateEvent as e:
            print(f"[stage3] skipped duplicate event_id {e}", file=sys.stderr, flush=True)
            continue
        except ValueError as e:
            dead_letter(raw, "scoring_error", e)
            continue
        sys.stdout.write(json.dumps(scored) + "\n")
        sys.stdout.flush()  # send each result on immediately; this is a stream, not a batch


if __name__ == "__main__":
    main()
