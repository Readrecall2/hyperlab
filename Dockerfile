FROM python:3.12.13-alpine3.24@sha256:6d43704baacd1bfbe7c295d7f13079d5d8104ed33568873133f8fc69980419df AS runtime

LABEL org.opencontainers.image.title="HyperLab" \
      org.opencontainers.image.description="Read-only public market-data collector and dashboard" \
      org.opencontainers.image.version="0.2.1"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONPATH=/app/src \
    HYPERLAB_MODE=readonly \
    HYPERLAB_DATA_DIR=/data

RUN addgroup -S -g 1000 hyperlab \
    && adduser -S -D -H -u 1000 -G hyperlab -s /sbin/nologin hyperlab

WORKDIR /app
COPY requirements-runtime.lock ./
RUN python -m pip install --disable-pip-version-check --no-cache-dir \
    --only-binary=:all: --require-hashes --requirement requirements-runtime.lock

COPY src ./src
COPY config ./config

RUN mkdir -p /data/lake /data/reports /data/paper /data/backups \
    && chown -R hyperlab:hyperlab /data
USER hyperlab
EXPOSE 8000
ENTRYPOINT ["python", "-m", "hyperlab.cli"]
CMD ["serve", "--host", "0.0.0.0", "--port", "8000"]
