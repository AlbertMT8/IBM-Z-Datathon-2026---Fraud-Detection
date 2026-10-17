"""
make_events.py — test helper, a stand-in for Albert's producer. Writes the first N
fraudTest rows as events (event.schema.json), one JSON per line, in time order.

    ../.venv/bin/python make_events.py 1000 > events.jsonl
"""

import json
import os
import sys

import pandas as pd

from demo_stream import ROOT, make_event

n = int(sys.argv[1]) if len(sys.argv) > 1 else 1000
df = pd.read_csv(os.path.join(ROOT, "Datasets", "fraudTest.csv"), nrows=n,
                 dtype={"cc_num": str, "zip": str})
for r in df.sort_values("trans_date_trans_time", kind="stable").to_dict("records"):
    print(json.dumps(make_event(r)))
