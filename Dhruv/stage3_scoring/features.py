"""
features.py — turns ONE payment into a list of numbers the models can read.

Big idea: a model can't read "grocery_pos" or "2019-01-01T00:00:18Z". It only
reads numbers. So for each payment we compute a fixed list of numbers
("features"), always in the same order.

Some features need MEMORY of the card's past ("how long since this card's last
payment?"). CardMemory is that memory. The exact same code is used:
  - in train.py, by replaying the history one payment at a time, and
  - in scorer.py, on the live stream.
That guarantees the model sees features computed the same way in both places.

Payments come from two places, so there are two small translators that turn
each into the same flat dict that CardMemory.compute() reads:
  - flatten_event(): a live event in Albert's event.schema.json format
  - rows_from_csv(): rows of the Sparkov CSV (training, warm-up)
"""

import hashlib
import math
from collections import deque
from datetime import datetime, timezone

# The 14 merchant categories in the Sparkov data. An unknown category just gets all zeros.
CATEGORIES = [
    "entertainment", "food_dining", "gas_transport", "grocery_net", "grocery_pos",
    "health_fitness", "home", "kids_pets", "misc_net", "misc_pos",
    "personal_care", "shopping_net", "shopping_pos", "travel",
]

# The fixed order of the numbers we hand to the models.
FEATURE_NAMES = [
    "amt",                 # payment amount in dollars
    "log_amt",             # same, squashed so $10 vs $100 matters as much as $100 vs $1000
    "hour",                # hour of day, 0-23
    "is_night",            # 1 if between 10 PM and 4 AM, else 0
    "age",                 # cardholder age in years
    "is_male",             # 1 if gender is M
    "log_city_pop",        # size of cardholder's city (squashed)
    "distance_km",         # cardholder home -> merchant distance
    "log_secs_since_last", # time since this card's previous payment (squashed)
    "card_n_prev",         # how many payments we've seen from this card before
    "amt_vs_card_avg",     # this amount / this card's usual amount
    "card_n_24h",          # this card's payments in the last 24 hours
    "card_amt_24h",        # total $ this card spent in the last 24 hours
] + ["cat_" + c for c in CATEGORIES]  # one yes/no column per category

DAY = 24 * 3600
MEDIAN_AGE = 44.0  # median cardholder age in fraudTrain; used when dob is missing (it's optional)


def card_id_from_cc_num(cc_num):
    """Same rule as the producer: SHA-256 of the raw card number, first 16 hex chars."""
    return hashlib.sha256(str(cc_num).encode()).hexdigest()[:16]


def _to_datetime(x):
    """'2019-01-01T00:00:18Z', '2019-01-01 00:00:18' or a datetime -> plain UTC datetime."""
    if not isinstance(x, datetime):
        x = datetime.fromisoformat(str(x).replace("Z", "+00:00"))
    if x.tzinfo is not None:
        x = x.astimezone(timezone.utc).replace(tzinfo=None)
    return x


def flatten_event(event):
    """Live event (event.schema.json) -> flat dict. Never reads event['label']."""
    t, c = event["transaction"], event["cardholder"]
    return {
        "occurred_at": t["occurred_at"], "card_id": t["card_id"], "amount": t["amount"],
        "category": t["category"], "merchant_lat": t["merchant_lat"], "merchant_lon": t["merchant_lon"],
        "lat": c["lat"], "lon": c["lon"], "city_pop": c["city_pop"],
        "gender": c.get("gender"), "dob": c.get("dob"),
    }


def rows_from_csv(df):
    """Sparkov CSV rows (a pandas DataFrame) -> list of flat dicts, same keys as flatten_event."""
    return [
        {"occurred_at": ts, "card_id": card_id_from_cc_num(cc), "amount": amt, "category": cat,
         "merchant_lat": mlat, "merchant_lon": mlon, "lat": lat, "lon": lon,
         "city_pop": pop, "gender": g, "dob": dob}
        for ts, cc, amt, cat, mlat, mlon, lat, lon, pop, g, dob in zip(
            df["trans_date_trans_time"], df["cc_num"], df["amt"], df["category"],
            df["merch_lat"], df["merch_long"], df["lat"], df["long"],
            df["city_pop"], df["gender"], df["dob"])
    ]


def _haversine_km(lat1, lon1, lat2, lon2):
    """Straight-line distance between two points on Earth, in km."""
    lat1, lon1, lat2, lon2 = map(math.radians, (lat1, lon1, lat2, lon2))
    a = (math.sin((lat2 - lat1) / 2) ** 2
         + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2)
    return 2 * 6371 * math.asin(math.sqrt(a))


class CardMemory:
    """Remembers a little about every card we've seen. One small record per card."""

    def __init__(self):
        self.cards = {}  # card_id -> {"last": datetime, "n": int, "sum": float, "recent": deque}

    def compute(self, p):
        """Flat payment dict -> (features as a list of numbers, a few raw facts for reasons).
        Reads the card's memory, then updates it with this payment."""
        t = _to_datetime(p["occurred_at"])
        amt = float(p["amount"])
        age = (t - _to_datetime(p["dob"])).days / 365.25 if p.get("dob") else MEDIAN_AGE
        card = self.cards.get(p["card_id"])

        if card is None:  # first time we've seen this card
            card = {"last": None, "n": 0, "sum": 0.0, "recent": deque()}
            self.cards[p["card_id"]] = card

        # Forget payments older than 24h, then summarise what's left.
        recent = card["recent"]
        while recent and (t - recent[0][0]).total_seconds() > DAY:
            recent.popleft()
        n_24h = len(recent)
        amt_24h = sum(a for _, a in recent)

        secs_since_last = (t - card["last"]).total_seconds() if card["last"] else 30 * DAY
        card_avg = card["sum"] / card["n"] if card["n"] else None
        amt_vs_avg = amt / card_avg if card_avg else 1.0

        cat = p["category"]
        feats = [
            amt,
            math.log1p(amt),
            t.hour,
            1 if (t.hour >= 22 or t.hour < 4) else 0,
            age,
            1 if p.get("gender") == "M" else 0,
            math.log1p(float(p["city_pop"])),
            _haversine_km(float(p["lat"]), float(p["lon"]),
                          float(p["merchant_lat"]), float(p["merchant_lon"])),
            math.log1p(max(secs_since_last, 0)),
            card["n"],
            amt_vs_avg,
            n_24h,
            amt_24h,
        ] + [1 if cat == c else 0 for c in CATEGORIES]

        # Facts the reason-writer can quote ("12x this card's usual $40").
        facts = {
            "amt": amt, "hour": t.hour, "category": cat,
            "card_avg_amt": card_avg, "secs_since_last": secs_since_last if card["last"] else None,
            "card_n_24h": n_24h, "card_amt_24h": amt_24h,
        }

        # Now remember this payment for next time.
        card["last"] = t
        card["n"] += 1
        card["sum"] += amt
        recent.append((t, amt))
        return feats, facts
