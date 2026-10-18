#!/bin/bash
# Usage: ~/demo.sh RATE EVENTS    e.g.  ~/demo.sh 1000 60000
# Restarts the scorer (fresh card memory), replays fraudTest at RATE events/s with 1% broken events,
# waits for the pipeline to finish, then prints throughput, latency and accuracy.
RATE=${1:-1000}; N=${2:-60000}; RUN="demo-${RATE}eps-$(date +%H%M%S)"
echo "== machine: $(uname -m), $(nproc) cores | containers:"; docker ps --format "   {{.Names}} ({{.Image}}) {{.Status}}"
echo "== restarting scorer for fresh card memory..."; docker restart scorer >/dev/null
until docker logs --since 60s scorer 2>&1 | grep -q "reading tx.valid"; do sleep 1; done
echo "== replaying $N payments at $RATE/s (run id $RUN)"
docker run --rm --network fraud-net -v ~/fraud/Datasets:/app/data fraud-producer run --input /app/data/sorted_test.csv \
  --rate $RATE --burst $((RATE*2)) --batch-size 200 --flush-interval 0.05 --max-events $N --malformed-rate 0.01 \
  --run-id $RUN --state-file /tmp/state.json 2>&1 | grep FINAL | grep -o "\"emitted\": [0-9]*, \"malformed\": [0-9]*"
while true; do
  p=$(( $(docker exec redis redis-cli XPENDING tx.raw validators | head -1) + $(docker exec redis redis-cli XPENDING tx.valid scorers | head -1) + $(docker exec redis redis-cli XPENDING tx.scored explainers | head -1) ))
  [ "$p" = "0" ] && break; sleep 1; done; sleep 2
docker run --rm --network fraud-net -v ~/measure.py:/m.py --entrypoint python fraud-explainer /m.py --run-id $RUN
