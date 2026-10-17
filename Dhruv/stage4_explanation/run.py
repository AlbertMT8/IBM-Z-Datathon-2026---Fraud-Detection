"""
run.py — stage 4 as a running process (what the container starts).

Reads one scored event per line from stdin (stage 3's output), writes the same
event with `explanation` filled to stdout, one per line.

    ../.venv/bin/python ../stage3_scoring/run.py < events.jsonl | ../.venv/bin/python run.py > explained.jsonl
    ../.venv/bin/python run.py --backend watsonx < scored.jsonl      # needs WATSONX_* env vars

A line that isn't valid JSON goes to stderr as a dead-letter record. Anything else is
always written out, even if the explanation failed (status "failed"/"timeout").
"""

import argparse
import json
import sys

from explainer import Explainer


def main():
    ap = argparse.ArgumentParser(description="Stage 4: explain scored events from stdin")
    ap.add_argument("--backend", choices=["template", "watsonx"], default="template")
    ap.add_argument("--timeout-ms", type=int, default=800)
    args = ap.parse_args()

    ex = Explainer(backend=args.backend, timeout_ms=args.timeout_ms)  # fails loudly if watsonx is misconfigured
    print(f"[stage4] backend: {ex.backend.name}", file=sys.stderr, flush=True)

    for raw in sys.stdin:
        raw = raw.strip()
        if not raw:
            continue
        try:
            scored = json.loads(raw)
        except json.JSONDecodeError as e:
            print(json.dumps({"stage": "explanation", "reason_code": "bad_json",
                              "reason_detail": str(e), "raw_payload": raw}), file=sys.stderr, flush=True)
            continue
        sys.stdout.write(json.dumps(ex.explain(scored)) + "\n")
        sys.stdout.flush()


if __name__ == "__main__":
    main()
