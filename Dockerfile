FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    HYPERLAB_MODE=readonly \
    HYPERLAB_DATA_DIR=/data

RUN addgroup --system --gid 1000 hyperlab \
    && adduser --system --uid 1000 --ingroup hyperlab --home /app hyperlab

WORKDIR /app
COPY pyproject.toml README.md ./
COPY src ./src
COPY config ./config
RUN python -m pip install --upgrade pip \
    && python -m pip install .

RUN mkdir -p /data && chown -R hyperlab:hyperlab /app /data
USER hyperlab
VOLUME ["/data"]
EXPOSE 8000
ENTRYPOINT ["hyperlab"]
CMD ["serve", "--host", "0.0.0.0", "--port", "8000"]
