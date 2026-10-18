# Stage 5 storer. BUILD THIS ON THE LinuxONE (s390x) VM, not on a laptop.
# Build context is the Albert/ folder:
#   docker build -t fraud-storer:latest -f stage5_storage/Dockerfile Albert
#
# Debian's own packages instead of pip: psycopg2 is compiled code and PyPI has no
# reliable s390x wheel for it; Debian trixie builds python3-psycopg2 (2.9.10) and
# python3-redis (6.1.0) for s390x. Same approach as Dhruv's stage 3 image.
FROM debian:trixie-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-psycopg2 python3-redis \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
# The contract's to_row() mapping, kept at the same relative path as in the repo
# (JSON-array COPY because the folder name has a space).
COPY ["New stuff/stubgen.py", "/app/New stuff/stubgen.py"]
COPY stage5_storage/storer.py /app/stage5_storage/storer.py

# Config only via env: REDIS_URL, PGHOST, PGPORT, PGDATABASE, PGUSER, PGPASSWORD.
ENV REDIS_URL=redis://redis:6379/0 \
    PGHOST=postgres \
    PGPORT=5432 \
    PGDATABASE=fraud

ENTRYPOINT ["python3", "/app/stage5_storage/storer.py"]
CMD ["--mode", "explained"]
