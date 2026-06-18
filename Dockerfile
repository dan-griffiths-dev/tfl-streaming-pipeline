# Use a slim Python image
FROM python:3.11-slim AS builder

# Install only the C-dependencies needed for confluent-kafka (avoids using Java)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    librdkafka-dev \
    && rm -rf /var/lib/apt/lists/*

# use uv binary
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

COPY pyproject.toml uv.lock ./

# Sync dependencies excludes dev/analytics during pipeline prototyping
RUN uv sync --frozen --no-group dev --no-group analytics

ENV PATH="/app/.venv/bin:$PATH"

COPY ingestion/ ./ingestion/
COPY pipeline_config.py .