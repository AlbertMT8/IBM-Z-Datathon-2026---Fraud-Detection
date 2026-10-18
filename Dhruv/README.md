# Dhruv — stages 3 and 4

```
stage 1 producer ─▶ tx.raw ─▶ stage 2 validator ─▶ tx.valid ─▶ STAGE 3 scorer ─▶ tx.scored ─▶ STAGE 4 explainer ─▶ tx.explained ─▶ stage 5
   (Albert)                      (Albert)    └─▶ tx.dlq ◀─ broken events ─┘                                        (Postgres + dashboard)
```
All arrows are Redis streams. Stages 1–2 are in `Albert/`.

| Folder | Container | Reads → writes | Redis hash for the dashboard |
| --- | --- | --- | --- |
| [`stage3_scoring/`](stage3_scoring/README.md) | Fraud scoring (`fraud-scorer`) | `tx.valid` → `tx.scored` (+ `tx.dlq`) | `metrics:scorer` |
| [`stage4_explanation/`](stage4_explanation/README.md) | Explanation (`fraud-explainer`) | `tx.scored` → `tx.explained` | `metrics:explainer` |

## Running it all on the LinuxONE VM (build every image ON the VM)
```bash
docker network create fraud-net
docker run -d --name redis --network fraud-net redis:8
docker build -t fraud-producer:latest  -f Albert/stage1_producer/Dockerfile  Albert
docker build -t fraud-validator:latest -f Albert/stage2_validator/Dockerfile Albert
docker build -t fraud-scorer:latest    Dhruv/stage3_scoring
docker build -t fraud-explainer:latest Dhruv/stage4_explanation
docker volume create fraud-models
docker run --rm -v "$PWD/Datasets:/data:ro" -v fraud-models:/models fraud-scorer:latest /app/train.py   # once
docker run -d --name validator --network fraud-net fraud-validator:latest --consumer-id val-1
docker run -d --name scorer    --network fraud-net -v "$PWD/Datasets:/data:ro" -v fraud-models:/models fraud-scorer:latest
docker run -d --name explainer --network fraud-net fraud-explainer:latest
# then start Albert's producer (see Albert/stage1_producer/README.md), replaying fraudTest.csv
```

## Things the team must agree on
- **Stream names**: `tx.scored` (stage 3 → 4) and `tx.explained` (stage 4 → 5) are new. Stage 5 should read `tx.explained`.
- **Stage 5 must ignore a repeated `event_id`.** Delivery is at-least-once, so a crash can repeat a few events.
  Albert's table already has `event_id` as the primary key: insert with `ON CONFLICT (event_id) DO NOTHING`.
- **Replay `fraudTest.csv`, not `fraudTrain.csv`.** The models learned from fraudTrain; the scorer loads it as card history.
- **Exactly one scorer container.** Card history is in its memory.
- **Stage 4 timeout** for the local model: ~10 s instead of the contract's 800 ms (see stage 4 README).

## Local testing without Docker
Both stages also run over stdin/stdout (`run.py`), and the Redis workers were tested against a fake Redis server
with Albert's real producer and validator. See each README's test section.

The `.venv/` here is shared by both and not committed. Contracts live in `Albert/New stuff/`.
