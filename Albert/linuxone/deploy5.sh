#!/bin/bash
# Stage 5 deploy: Postgres + storer + dlq-storer + dashboard on the fraud-net network.
# Run ON the LinuxONE VM from ~/fraud after `git pull`. Every image is built HERE (s390x);
# never push laptop-built images. Safe to re-run: containers are replaced, data in the
# fraud-pg volume is kept, and schema.sql (idempotent) is re-applied.
#
# Secrets come only from ~/fraud/.env (git-ignored, created by hand; see .env.example).
# Stages 1-4 (redis, validators, scorer, explainer) are started per Dhruv/README.md.
set -euo pipefail

FRAUD_DIR="${FRAUD_DIR:-$HOME/fraud}"
ENV_FILE="${ENV_FILE:-$FRAUD_DIR/.env}"
PG_IMAGE="postgres:17.11-trixie"   # official image; tag checked to publish linux/s390x
SCHEMA="$FRAUD_DIR/Albert/New stuff/schema.sql"
cd "$FRAUD_DIR"

if [ ! -f "$ENV_FILE" ]; then
  echo "missing $ENV_FILE. Create it from .env.example (cp .env.example .env), set real passwords, re-run." >&2
  exit 1
fi
for key in POSTGRES_DB POSTGRES_USER POSTGRES_PASSWORD PGUSER PGPASSWORD PGDATABASE PGHOST; do
  grep -q "^${key}=." "$ENV_FILE" || { echo "$ENV_FILE has no value for $key" >&2; exit 1; }
done

echo "== machine: $(uname -m)"
docker network inspect fraud-net >/dev/null 2>&1 || docker network create fraud-net
docker volume inspect fraud-pg >/dev/null 2>&1 || docker volume create fraud-pg

echo "== building stage 5 images on this machine"
docker pull -q "$PG_IMAGE"
docker build -q -t fraud-storer:latest    -f Albert/stage5_storage/Dockerfile Albert
docker build -q -t fraud-dashboard:latest -f Albert/dashboard/Dockerfile      Albert

echo "== postgres"
docker rm -f postgres >/dev/null 2>&1 || true
# schema.sql runs automatically the first time the fraud-pg volume is initialised.
docker run -d --name postgres --network fraud-net --restart unless-stopped --env-file "$ENV_FILE" \
  -v fraud-pg:/var/lib/postgresql/data \
  -v "$SCHEMA:/docker-entrypoint-initdb.d/schema.sql:ro" \
  "$PG_IMAGE" >/dev/null
until docker exec postgres sh -c 'pg_isready -q -U "$POSTGRES_USER" -d "$POSTGRES_DB"' 2>/dev/null; do sleep 1; done
# ... and again on every deploy, for a volume created by an older schema (idempotent).
until docker exec -i postgres sh -c 'psql -q -v ON_ERROR_STOP=1 -U "$POSTGRES_USER" -d "$POSTGRES_DB"' < "$SCHEMA" 2>/dev/null; do sleep 1; done

echo "== storer, dlq-storer, dashboard"
for c in storer dlq-storer dashboard; do docker rm -f "$c" >/dev/null 2>&1 || true; done
docker run -d --name storer     --network fraud-net --restart unless-stopped --env-file "$ENV_FILE" \
  fraud-storer:latest --mode explained --consumer storer-1 >/dev/null
docker run -d --name dlq-storer --network fraud-net --restart unless-stopped --env-file "$ENV_FILE" \
  fraud-storer:latest --mode dlq --consumer dlq-storer-1 >/dev/null
# Loopback only: reach it through an SSH tunnel, never the VM's public address.
docker run -d --name dashboard  --network fraud-net --restart unless-stopped --env-file "$ENV_FILE" \
  -p 127.0.0.1:8080:8080 fraud-dashboard:latest >/dev/null

sleep 3
docker ps --format '   {{.Names}} ({{.Image}}) {{.Status}} {{.Ports}}'
for c in storer dlq-storer; do docker logs "$c" 2>&1 | tail -1; done
echo "== dashboard: ssh -i \"\$L1CC_KEY\" -L 8080:127.0.0.1:8080 linux1@\"\$L1CC_HOST\", then open http://localhost:8080"
