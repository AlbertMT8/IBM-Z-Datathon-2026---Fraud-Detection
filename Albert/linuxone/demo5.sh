#!/bin/bash
# Usage: ~/fraud/Albert/linuxone/demo5.sh RATE EVENTS     e.g. demo5.sh 1000 60000
# Empties the stage 5 tables, runs the existing ~/demo.sh RATE EVENTS unchanged (it restarts the
# scorer and replays fraudTest with 1% broken events), waits until stage 5 has stored everything,
# then prints the Postgres-inclusive results with measure_db.py.
set -uo pipefail
RATE=${1:-1000}; N=${2:-60000}
FRAUD_DIR="${FRAUD_DIR:-$HOME/fraud}"
ENV_FILE="${ENV_FILE:-$FRAUD_DIR/.env}"
DEMO_SH="${DEMO_SH:-$HOME/demo.sh}"
psql_() { docker exec postgres sh -c 'psql -qtA -U "$POSTGRES_USER" -d "$POSTGRES_DB" -c "$0"' "$1"; }
pending() { docker exec redis redis-cli XPENDING "$1" "$2" 2>/dev/null | head -1; }

echo "== emptying stage 5 tables"
psql_ "TRUNCATE scored_transactions, dead_letter_events"

out=$(mktemp)
"$DEMO_SH" "$RATE" "$N" 2>&1 | tee "$out"
RUN=$(grep -o "run id demo-[^)]*" "$out" | head -1 | cut -d' ' -f3)
rm -f "$out"
[ -n "$RUN" ] || { echo "could not find the run id in demo.sh output" >&2; exit 1; }

echo "== waiting for stage 5 (run $RUN): storer pending 0 and row counts stable"
last=""; stable=0
while [ "$stable" -lt 3 ]; do
  p=$(( $(pending tx.explained storers || echo 1) + $(pending tx.dlq dlq-storers || echo 1) ))
  now="$(psql_ "SELECT count(*) FROM scored_transactions")/$(psql_ "SELECT count(*) FROM dead_letter_events")"
  if [ "$p" = "0" ] && [ "$now" = "$last" ]; then stable=$((stable + 1)); else stable=0; fi
  last=$now; sleep 1
done
echo "   stored rows/dlq rows: $last"

docker run --rm --network fraud-net --env-file "$ENV_FILE" \
  -v "$FRAUD_DIR/Albert/linuxone/measure_db.py:/m.py:ro" --entrypoint python3 fraud-storer:latest /m.py --run-id "$RUN"
