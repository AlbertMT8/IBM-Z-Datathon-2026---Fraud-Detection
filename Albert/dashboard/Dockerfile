# ZSCORE dashboard. BUILD THIS ON THE LinuxONE (s390x) VM, not on a laptop.
# Build context is the Albert/ folder:
#   docker build -t fraud-dashboard:latest -f dashboard/Dockerfile Albert
# Run it published on the host's loopback ONLY (never the VM's public IP):
#   docker run -d --name dashboard --network fraud-net --env-file ~/fraud/.env \
#       -p 127.0.0.1:8080:8080 fraud-dashboard:latest
# Standard library http.server + Debian's python3-redis / python3-psycopg2
# (s390x builds, no PyPI wheels). The page is self-contained (inline SVG chart).
FROM debian:trixie-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-psycopg2 python3-redis \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY dashboard/server.py dashboard/index.html /app/

ENV REDIS_URL=redis://redis:6379/0 \
    PGHOST=postgres \
    PGPORT=5432 \
    PGDATABASE=fraud \
    PORT=8080
EXPOSE 8080
ENTRYPOINT ["python3", "/app/server.py"]
