FROM python:3.14-slim

ARG GIT_SHA=""
ARG UV_VERSION="0.10.0"

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    UV_CACHE_DIR=/tmp/uv-cache \
    GIT_SHA=${GIT_SHA}

WORKDIR /app

# Install uv
RUN pip install --no-cache-dir "uv==${UV_VERSION}"

# Python 3.14 wheels are not available for every dependency yet.
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential libffi-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency files first for third-party dependency layer caching.
COPY pyproject.toml uv.lock ./

# Install production dependencies first, then install this project after source is copied.
RUN uv sync --no-dev --frozen --no-install-project

# Copy source
COPY . .

RUN uv sync --no-dev --frozen

RUN useradd --create-home --uid 10001 app \
    && mkdir -p /app/lake-data \
    && chown -R app:app /app

USER app

# Default: start a Prefect worker. Override in docker-compose for other commands.
CMD ["uv", "run", "--no-dev", "--frozen", "prefect", "worker", "start", "--pool", "default-process-pool"]
