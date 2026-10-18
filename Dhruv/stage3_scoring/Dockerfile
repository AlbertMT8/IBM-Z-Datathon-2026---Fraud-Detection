# Stage 3 — fraud scoring. BUILD THIS ON THE LinuxONE (s390x) VM, not on a laptop.
#   docker build -t fraud-scorer:latest Dhruv/stage3_scoring
#
# Why Debian packages instead of pip: numpy / pandas / scikit-learn / xgboost need compiled
# code. PyPI has no reliable s390x builds for them, but Debian builds all of them for s390x
# (checked: trixie has python3-xgboost 3.0.0, python3-sklearn 1.4.2, python3-pandas 2.2.3,
# python3-numpy 2.2.4, python3-redis 6.1.0). So apt installs them ready-made: nothing to compile.
FROM debian:trixie-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    DEBIAN_FRONTEND=noninteractive

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      python3 python3-numpy python3-pandas python3-sklearn python3-xgboost python3-joblib python3-redis \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY features.py dedupe.py scorer.py train.py stream_worker.py run.py /app/

# Models are NOT baked in: train them inside this image on the VM (byte order + library
# versions must match). /data holds the Sparkov CSVs, /models the trained files.
ENV DATA_DIR=/data \
    MODELS_DIR=/models \
    REDIS_URL=redis://redis:6379/0

ENTRYPOINT ["python3"]
# Default: the stream worker, with card history loaded from the training file.
CMD ["/app/stream_worker.py", "--warm-up", "/data/fraudTrain.csv"]
