"""Generate a synthetic CSV in the exact Sparkov fraudTrain.csv layout
(leading unnamed index column, 22 named columns), rows deliberately unsorted."""
import csv, random, sys, datetime as dt

n = int(sys.argv[2]) if len(sys.argv) > 2 else 60000
out = sys.argv[1]
rng = random.Random(7)
cats = ["misc_net", "grocery_pos", "entertainment", "gas_transport", "misc_pos", "shopping_net", "food_dining"]
states = ["NC", "WA", "ID", "MT", "VA", "NY", "TX", "CA"]
base = 1325376018
cols = ["", "trans_date_trans_time", "cc_num", "merchant", "category", "amt", "first", "last",
        "gender", "street", "city", "state", "zip", "lat", "long", "city_pop", "job", "dob",
        "trans_num", "unix_time", "merch_lat", "merch_long", "is_fraud"]
# ZIPs from 1000: a 4-digit ZIP is a New England code whose leading zero was
# lost, as when the real CSV is read with an int dtype.
with open(out, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(cols)
    for i in range(n):
        ut = base + rng.randint(0, 3600 * 24 * 30)
        ts = dt.datetime.utcfromtimestamp(ut + 220_000_000).strftime("%Y-%m-%d %H:%M:%S")
        lat = round(rng.uniform(25, 49), 4); lon = round(rng.uniform(-124, -67), 4)
        w.writerow([i, ts, str(rng.randint(10**11, 10**16)), f"fraud_Merchant {rng.randint(1,700)}, Inc",
                    rng.choice(cats), round(rng.uniform(1, 3000), 2), "Jennifer", "Banks",
                    rng.choice("MF"), f"{rng.randint(1,9999)} Perry Cove", "Moravian Falls",
                    rng.choice(states), f"{rng.randint(1000,99999)}", lat, lon, rng.randint(20, 2_900_000),
                    "Psychologist, counselling", "1988-03-09", "%032x" % rng.getrandbits(128), ut,
                    round(lat + rng.uniform(-1, 1), 6), round(lon + rng.uniform(-1, 1), 6),
                    1 if rng.random() < 0.006 else 0])
