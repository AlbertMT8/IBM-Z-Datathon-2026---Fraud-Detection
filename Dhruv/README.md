# Dhruv — stages 3 and 4

| Folder | Container | In → out |
| --- | --- | --- |
| [`stage3_scoring/`](stage3_scoring/README.md) | Fraud scoring | event → scored event (score, decision, reasons) |
| [`stage4_explanation/`](stage4_explanation/README.md) | Explanation | scored event → same event + one plain-English sentence |

Both read one JSON per line on stdin and write one per line on stdout, so they chain:

```bash
cd stage4_explanation
../.venv/bin/python ../stage3_scoring/make_events.py 1000 \
  | ../.venv/bin/python ../stage3_scoring/run.py --warm-up ../../Datasets/fraudTrain.csv \
  | ../.venv/bin/python run.py > explained.jsonl
```

The `.venv/` here is shared by both and not committed. Contracts live in Albert's folder.
