"""
features.py — turns ONE payment into a list of numbers the models can read.

Big idea: a model can't read "grocery_pos" or "2019-01-01 00:00:18". It only
reads numbers. So for each payment we compute a fixed list of numbers
("features"), always in the same order.

Some features need MEMORY of the card's past ("how long since this card's last
payment?"). CardMemory is that memory. The exact same code is used:
  - in train.py, by replaying the history one payment at a time, and
  - in scorer.py, on the live stream.
That guarantees the model sees features computed the same way in both places.
"""

import math
from collections import deque
from datetime import datetime

# The 14 merchant categories in the Sparkov data.
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


def _to_datetime(x):
    """Accept '2019-01-01 00:00:18' style strings or datetime objects."""
    return x if isinstance(x, datetime) else datetime.fromisoformat(str(x))


def _haversine_km(lat1, lon1, lat2, lon2):
    """Straight-line distance between two points on Earth, in km."""
    lat1, lon1, lat2, lon2 = map(math.radians, (lat1, lon1, lat2, lon2))
    a = (math.sin((lat2 - lat1) / 2) ** 2
         + math.cos(lat1) * math.cos(lat2) * math.sin((lon2 - lon1) / 2) ** 2)
    return 2 * 6371 * math.asin(math.sqrt(a))


class CardMemory:
    """Remembers a little about every card we've seen. One small record per card."""

    def __init__(self):
        self.cards = {}  # cc_num -> {"last": datetime, "n": int, "sum": float, "recent": deque}

    def compute(self, p):
        """Payment dict -> (features as a list of numbers, a few raw facts for reasons).
        Reads the card's memory, then updates it with this payment."""
        t = _to_datetime(p["trans_date_trans_time"])
        amt = float(p["amt"])
        dob = _to_datetime(p["dob"])
        card = self.cards.get(p["cc_num"])

        if card is None:  # first time we've seen this card
            card = {"last": None, "n": 0, "sum": 0.0, "recent": deque()}
            self.cards[p["cc_num"]] = card

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
            (t - dob).days / 365.25,
            1 if p.get("gender") == "M" else 0,
            math.log1p(float(p["city_pop"])),
            _haversine_km(float(p["lat"]), float(p["long"]),
                          float(p["merch_lat"]), float(p["merch_long"])),
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
