# Stage 4 — explanation layer. BUILD THIS ON THE LinuxONE (s390x) VM, not on a laptop.
#   docker build -t fraud-explainer:latest Dhruv/stage4_explanation
#
# Only Python's standard library + redis (pure Python), so the official python:3.12-slim
# image (which has an s390x build) is all it needs. Same base as Albert's stage 1/2 images.
FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
COPY explainer.py stream_worker.py run.py /app/

# BACKEND: template | local | watsonx.  For local, also set LLAMA_URL (e.g. http://llm:8080)
# and TIMEOUT_MS (e.g. 10000). For watsonx, pass WATSONX_APIKEY / WATSONX_PROJECT_ID at run time
# with -e. NEVER bake keys into the image.
ENV REDIS_URL=redis://redis:6379/0 \
    BACKEND=template \
    TIMEOUT_MS=800

ENTRYPOINT ["python", "/app/stream_worker.py"]
