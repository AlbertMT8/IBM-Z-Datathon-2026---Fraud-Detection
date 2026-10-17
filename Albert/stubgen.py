#!/usr/bin/env python3
"""
stubgen.py — contract-conformant fake data for the fraud pipeline.

WHY THIS EXISTS
    Nobody waits on anybody. Part 2 needs raw events to score before the
    producer is written. Part 3 needs scored events to explain before the
    scorer is written. Part 4 needs rows in Postgres to draw a dashboard
    before any of it exists. This generates all three, guaranteed to match
    the contracts, so all four workstreams start at the same minute.

USAGE
    python3 stubgen.py --events 20                  > events.jsonl
    python3 stubgen.py --scored 20                  > scored.jsonl
    python3 stubgen.py --malformed 10               > bad.jsonl
    python3 stubgen.py --scored 500 --seed-db       # straight into Postgres
    python3 stubgen.py --validate scored.jsonl      # check anything, anytime

    DB connection comes from the standard PG* env vars, or --dsn.

DEPENDENCIES
    stdlib only for generation. `jsonschema` for --validate (optional but
    recommended). `psycopg2` or `psycopg` for --seed-db.
"""

import argparse
import hashlib
import json
import math
import os
import random
import sys
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

HERE = Path(__file__).resolve().parent
SCHEMA_VERSION = "1.0.0"

# The 14 Sparkov categories. See the note in event.schema.json: these are
# documentation, not a validation enum, until someone confirms them against
# the real CSV with df.category.unique().
CATEGORIES = [
    "entertainment", "food_dining", "gas_transport", "grocery_net",
    "grocery_pos", "health_fitness", "home", "kids_pets", "misc_net",
    "misc_pos", "personal_care", "shopping_net", "shopping_pos", "travel",
]

# Merchant names in Sparkov follow a "fraud_<Faker company>" shape. The prefix
# is on EVERY merchant, legitimate or not — it is not a label leak, but it
# will confuse a teammate who sees it for the first time at 2am. Confirm the
# exact format against the CSV before quoting it anywhere.
MERCHANTS = [
    "fraud_Kozey-Boehm", "fraud_Rippin, Kub and Mann", "fraud_Schumm PLC",
    "fraud_Kuhn LLC", "fraud_Boyer-Reichert", "fraud_Cormier LLC",
    "fraud_Baumbach, Feeney and Morar", "fraud_Doyle Ltd",
    "fraud_Heller-Langosh", "fraud_Lind-Buckridge",
]

# (city, state, zip, lat, lon, population) — zip deliberately as a STRING,
# including a leading-zero case, because that is the bug this guards against.
CITIES = [
    ("Columbia",    "SC", "29201", 34.0007,  -81.0348,  131_674),
    ("Orient",      "WA", "99160", 48.8878, -118.2105,      149),
    ("Malad City",  "ID", "83252", 42.1808, -112.2620,    4_154),
    ("Boston",      "MA", "02108", 42.3584,  -71.0598,  654_776),
    ("Grenada",     "CA", "96038", 41.6125, -122.5258,    1_139),
    ("Holyoke",     "CO", "80734", 40.5852, -102.3037,    2_326),
    ("Westport",    "CT", "06880", 41.1415,  -73.3579,   27_141),
    ("Clarksville", "TN", "37040", 36.5298,  -87.3595,  132_929),
]

JOBS = [
    "Systems analyst", "Naval architect", "Dance movement psychotherapist",
    "Librarian, academic", "Patent attorney", "Soil scientist",
    "Materials engineer", "Set designer",
]

MODEL_NAMES = ["xgboost", "random_forest", "logistic_regression"]


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------

def _iso(dt: datetime) -> str:
    """RFC3339 UTC with milliseconds — the one timestamp format in this system."""
    return dt.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _card_id(cc_num: str) -> str:
    """Stable per-card identifier. Exactly how the producer must derive it."""
    return hashlib.sha256(cc_num.encode()).hexdigest()[:16]


def _offset_latlon(lat, lon, km, rng):
    """Move a point a given distance in a random direction. Used to place the
    merchant near the cardholder for normal activity and far away for fraud,
    so the explanation layer has a real geographic signal to talk about."""
    bearing = rng.uniform(0, 2 * math.pi)
    dlat = (km / 111.0) * math.cos(bearing)
    dlon = (km / (111.0 * max(math.cos(math.radians(lat)), 0.01))) * math.sin(bearing)
    return round(lat + dlat, 6), round(lon + dlon, 6)


# ---------------------------------------------------------------------------
# generators
# ---------------------------------------------------------------------------

def make_event(rng: random.Random, *, run_id=None, fraud=None, emitted_at=None) -> dict:
    """One contract-valid RawTransactionEvent."""
    if fraud is None:
        fraud = rng.random() < 0.05          # oversampled vs the real ~0.5%,
                                             # so stub data exercises alert paths
    city, state, zipc, hlat, hlon, pop = rng.choice(CITIES)
    cc_num = str(rng.randint(4_000_000_000_000_000, 4_999_999_999_999_999))

    # Fraud sits far from home and skews large; normal activity sits close.
    dist_km = rng.uniform(300, 2500) if fraud else rng.uniform(1, 60)
    mlat, mlon = _offset_latlon(hlat, hlon, dist_km, rng)
    amount = round(rng.uniform(250, 1400), 2) if fraud else round(rng.lognormvariate(3.6, 0.9), 2)
    amount = min(amount, 28_948.90)

    occurred = datetime(2019, 1, 1, tzinfo=timezone.utc) + timedelta(
        seconds=rng.randint(0, 730 * 24 * 3600)
    )
    emitted = emitted_at or datetime.now(timezone.utc)

    event = {
        "schema_version": SCHEMA_VERSION,
        "event_id": str(uuid.uuid4()),
        "emitted_at": _iso(emitted),
        "source_trans_num": hashlib.md5(f"{cc_num}{occurred}".encode()).hexdigest(),
        "transaction": {
            "occurred_at": _iso(occurred),
            "unix_time": int(occurred.timestamp()),
            "card_id": _card_id(cc_num),
            "card_last4": cc_num[-4:],
            "amount": amount,
            "merchant": rng.choice(MERCHANTS),
            "category": rng.choice(CATEGORIES),
            "merchant_lat": mlat,
            "merchant_lon": mlon,
        },
        "cardholder": {
            "city": city,
            "state": state,
            "zip": zipc,                       # STRING. leading zeros intact.
            "lat": hlat,
            "lon": hlon,
            "city_pop": pop,
            "job": rng.choice(JOBS),
            "gender": rng.choice(["M", "F"]),
            "dob": f"{rng.randint(1950, 2000)}-{rng.randint(1,12):02d}-{rng.randint(1,28):02d}",
        },
        "label": {"is_fraud": 1 if fraud else 0},
    }
    if run_id:
        event["run_id"] = run_id
    return event


def make_scored(rng: random.Random, event: dict, *, threshold=0.5, explain=True) -> dict:
    """Wrap an event in a contract-valid ScoredTransactionEvent."""
    truth = event.get("label", {}).get("is_fraud", 0)

    # Scores correlate with truth but are deliberately imperfect, so Part 4's
    # precision/recall tiles show something other than 1.00.
    base = rng.betavariate(6, 2) if truth else rng.betavariate(1.6, 9)
    scores = {}
    for name in MODEL_NAMES:
        scores[name] = round(min(max(base + rng.gauss(0, 0.07), 0.0), 1.0), 4)
    ensemble = round(sum(scores.values()) / len(scores), 4)

    if ensemble >= 0.85:
        decision = "BLOCK"
    elif ensemble >= threshold:
        decision = "REVIEW"
    else:
        decision = "ALLOW"

    scored_at = datetime.now(timezone.utc)
    scored = {
        "schema_version": SCHEMA_VERSION,
        "event": event,
        "scoring": {
            "scored_at": _iso(scored_at),
            "model_scores": scores,
            "ensemble_score": ensemble,
            "threshold": threshold,
            "decision": decision,
            "model_version": "ens-stub-v0",
            "scoring_duration_ms": round(rng.uniform(4, 22), 2),
            "feature_count": 31,
            "reasons": _reasons(rng, event, truth),
        },
        "explanation": None,
    }

    if explain:
        if decision == "ALLOW":
            scored["explanation"] = {
                "status": "skipped",
                "generated_at": _iso(scored_at),
                "model": "watsonx-assistant",
            }
        else:
            tx, ch = event["transaction"], event["cardholder"]
            km = _haversine(ch["lat"], ch["lon"], tx["merchant_lat"], tx["merchant_lon"])
            scored["explanation"] = {
                "status": "ok",
                "text": (
                    f"${tx['amount']:,.2f} at {tx['merchant']} ({tx['category']}), "
                    f"{km:,.0f} km from the cardholder's home in "
                    f"{ch['city']}, {ch['state']} — well outside this card's usual range."
                ),
                "generated_at": _iso(scored_at + timedelta(milliseconds=rng.randint(180, 700))),
                "duration_ms": round(rng.uniform(180, 700), 1),
                "model": "watsonx-assistant",
            }
    return scored


def _reasons(rng, event, truth):
    """Stand-in for the scorer's score_with_reasons(). Object form here, but the
    contract also accepts plain strings — whatever score_with_reasons() returns
    plugs in unchanged."""
    tx, ch = event["transaction"], event["cardholder"]
    km = _haversine(ch["lat"], ch["lon"], tx["merchant_lat"], tx["merchant_lon"])
    pool = [
        {"feature": "distance_from_home_km", "value": round(km, 1),
         "contribution": round(min(km / 2500, 1.0) * (0.4 if truth else 0.05), 3)},
        {"feature": "amount", "value": tx["amount"],
         "contribution": round((0.3 if truth else 0.04) * rng.random(), 3)},
        {"feature": "hour_of_day", "value": int(tx["occurred_at"][11:13]),
         "contribution": round((0.2 if truth else 0.03) * rng.random(), 3)},
        {"feature": "category", "value": tx["category"],
         "contribution": round((0.15 if truth else 0.02) * rng.random(), 3)},
    ]
    pool.sort(key=lambda r: -r["contribution"])
    return pool[:3]


def _haversine(lat1, lon1, lat2, lon2):
    r = 6371.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


# Each mutation breaks the contract in a DIFFERENT way, so the DLQ demo shows
# a spread of reason codes rather than the same error ten times.
def make_malformed(rng: random.Random) -> tuple[str, dict | str]:
    ev = make_event(rng)
    kind = rng.choice([
        "bad_json", "missing_required", "wrong_type", "bad_enum",
        "out_of_range", "unknown_field", "bad_zip", "null_amount",
    ])
    if kind == "bad_json":
        return kind, json.dumps(ev)[:-18] + "}}"          # truncated payload
    if kind == "missing_required":
        del ev["transaction"]["amount"]
    elif kind == "wrong_type":
        ev["transaction"]["amount"] = "129.99"             # string, not number
    elif kind == "bad_enum":
        ev["cardholder"]["gender"] = "X"
    elif kind == "out_of_range":
        ev["transaction"]["merchant_lat"] = 412.7
    elif kind == "unknown_field":
        ev["transaction"]["cvv"] = "412"                   # additionalProperties
    elif kind == "bad_zip":
        ev["cardholder"]["zip"] = "2108"                   # the leading-zero bug
    elif kind == "null_amount":
        ev["transaction"]["amount"] = None
    return kind, ev


# ---------------------------------------------------------------------------
# validation
# ---------------------------------------------------------------------------

def _validators():
    """Build validators for both contracts, with the $ref between them wired up.
    Copy this function into the producer, the scorer and the writer — the
    contract is only real if every stage enforces it."""
    try:
        from jsonschema import Draft202012Validator
        from referencing import Registry, Resource
    except ImportError:
        print("jsonschema not installed:  pip install jsonschema", file=sys.stderr)
        raise SystemExit(2)

    ev = json.loads((HERE / "event.schema.json").read_text())
    sc = json.loads((HERE / "scored_event.schema.json").read_text())
    registry = Registry().with_resources([
        ("event.schema.json", Resource.from_contents(ev)),
        ("scored_event.schema.json", Resource.from_contents(sc)),
    ])
    return (Draft202012Validator(ev, registry=registry),
            Draft202012Validator(sc, registry=registry))


def validate_file(path: str) -> int:
    ev_v, sc_v = _validators()
    ok = bad = 0
    with open(path) as fh:
        for n, line in enumerate(fh, 1):
            line = line.strip()
            if not line:
                continue
            try:
                doc = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"line {n}: bad_json — {e}"); bad += 1; continue
            v = sc_v if "scoring" in doc else ev_v
            errs = sorted(v.iter_errors(doc), key=lambda e: list(e.path))
            if errs:
                bad += 1
                loc = "/".join(str(p) for p in errs[0].path) or "(root)"
                print(f"line {n}: schema_violation at {loc} — {errs[0].message}")
            else:
                ok += 1
    print(f"\n{ok} valid, {bad} invalid")
    return 1 if bad else 0


# ---------------------------------------------------------------------------
# postgres seeding
# ---------------------------------------------------------------------------

INSERT_SQL = """
INSERT INTO scored_transactions (
    event_id, run_id, source_trans_num, occurred_at, emitted_at,
    card_id, card_last4, amount, merchant, category, merchant_lat, merchant_lon,
    holder_city, holder_state, holder_zip, holder_lat, holder_lon,
    holder_city_pop, holder_job,
    model_scores, ensemble_score, threshold, decision, model_version,
    scored_at, scoring_duration_ms, scoring_reasons,
    explanation_text, explanation_status, explanation_ms, explained_at,
    is_fraud
) VALUES (
    %(event_id)s, %(run_id)s, %(source_trans_num)s, %(occurred_at)s, %(emitted_at)s,
    %(card_id)s, %(card_last4)s, %(amount)s, %(merchant)s, %(category)s,
    %(merchant_lat)s, %(merchant_lon)s,
    %(holder_city)s, %(holder_state)s, %(holder_zip)s, %(holder_lat)s, %(holder_lon)s,
    %(holder_city_pop)s, %(holder_job)s,
    %(model_scores)s, %(ensemble_score)s, %(threshold)s, %(decision)s, %(model_version)s,
    %(scored_at)s, %(scoring_duration_ms)s, %(scoring_reasons)s,
    %(explanation_text)s, %(explanation_status)s, %(explanation_ms)s, %(explained_at)s,
    %(is_fraud)s
) ON CONFLICT (event_id) DO NOTHING
"""


def to_row(scored: dict) -> dict:
    """The canonical scored-event -> table-row mapping. Stage 5 should use this
    exact function; if it diverges, the contract has quietly forked."""
    e, tx, ch = scored["event"], scored["event"]["transaction"], scored["event"]["cardholder"]
    s, x = scored["scoring"], scored.get("explanation") or {}
    return {
        "event_id": e["event_id"],
        "run_id": e.get("run_id", "default"),
        "source_trans_num": e["source_trans_num"],
        "occurred_at": tx["occurred_at"],
        "emitted_at": e["emitted_at"],
        "card_id": tx["card_id"],
        "card_last4": tx["card_last4"],
        "amount": tx["amount"],
        "merchant": tx["merchant"],
        "category": tx["category"],
        "merchant_lat": tx.get("merchant_lat"),
        "merchant_lon": tx.get("merchant_lon"),
        "holder_city": ch.get("city"),
        "holder_state": ch.get("state"),
        "holder_zip": ch.get("zip"),
        "holder_lat": ch.get("lat"),
        "holder_lon": ch.get("lon"),
        "holder_city_pop": ch.get("city_pop"),
        "holder_job": ch.get("job"),
        "model_scores": json.dumps(s["model_scores"]),
        "ensemble_score": s["ensemble_score"],
        "threshold": s["threshold"],
        "decision": s["decision"],
        "model_version": s["model_version"],
        "scored_at": s["scored_at"],
        "scoring_duration_ms": s["scoring_duration_ms"],
        "scoring_reasons": json.dumps(s["reasons"]) if s.get("reasons") else None,
        "explanation_text": x.get("text"),
        "explanation_status": x.get("status"),
        "explanation_ms": x.get("duration_ms"),
        "explained_at": x.get("generated_at"),
        "is_fraud": e.get("label", {}).get("is_fraud"),
    }


def seed_db(rows, dsn=None):
    try:
        import psycopg2 as pg
    except ImportError:
        try:
            import psycopg as pg
        except ImportError:
            print("need psycopg2-binary or psycopg:  pip install psycopg2-binary",
                  file=sys.stderr)
            raise SystemExit(2)
    conn = pg.connect(dsn) if dsn else pg.connect()
    with conn, conn.cursor() as cur:
        for r in rows:
            cur.execute(INSERT_SQL, r)
    conn.close()


# ---------------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--events", type=int, metavar="N")
    ap.add_argument("--scored", type=int, metavar="N")
    ap.add_argument("--malformed", type=int, metavar="N")
    ap.add_argument("--validate", metavar="FILE")
    ap.add_argument("--seed-db", action="store_true",
                    help="write --scored events into Postgres instead of stdout")
    ap.add_argument("--dsn", help="libpq DSN; defaults to PG* env vars")
    ap.add_argument("--run-id", default=None)
    ap.add_argument("--threshold", type=float, default=0.5)
    ap.add_argument("--seed", type=int, default=None, help="RNG seed; makes output reproducible")
    ap.add_argument("--out", metavar="FILE")
    args = ap.parse_args()

    if args.validate:
        raise SystemExit(validate_file(args.validate))

    rng = random.Random(args.seed)
    out = open(args.out, "w") if args.out else sys.stdout

    if args.events:
        for _ in range(args.events):
            print(json.dumps(make_event(rng, run_id=args.run_id)), file=out)

    elif args.scored:
        docs = [make_scored(rng, make_event(rng, run_id=args.run_id),
                            threshold=args.threshold)
                for _ in range(args.scored)]
        if args.seed_db:
            seed_db((to_row(d) for d in docs), args.dsn)
            n_alert = sum(1 for d in docs if d["scoring"]["decision"] != "ALLOW")
            print(f"seeded {len(docs)} rows ({n_alert} alerts)", file=sys.stderr)
        else:
            for d in docs:
                print(json.dumps(d), file=out)

    elif args.malformed:
        for _ in range(args.malformed):
            kind, payload = make_malformed(rng)
            print(payload if isinstance(payload, str) else json.dumps(payload), file=out)

    else:
        ap.print_help()

    if args.out:
        out.close()


if __name__ == "__main__":
    main()
