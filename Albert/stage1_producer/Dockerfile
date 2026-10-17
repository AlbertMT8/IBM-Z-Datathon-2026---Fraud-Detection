# Build on the LinuxONE (s390x) VM. Pure-Python deps only — redis and
# jsonschema both publish sdists that install cleanly on s390x. Do NOT build
# this image on an x86 laptop and push it; the resulting image will not run
# natively on s390x.
# Build context is the Albert/ folder:
#   docker build -t fraud-producer:latest -f stage1_producer/Dockerfile Albert
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY stage1_producer/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY stage1_producer/producer.py /app/producer/producer.py

ENV PYTHONPATH=/app
ENV REDIS_URL=redis://redis:6379/0

ENTRYPOINT ["python", "/app/producer/producer.py"]
CMD ["--help"]
