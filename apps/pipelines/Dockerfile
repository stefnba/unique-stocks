FROM python:3.14-slim

WORKDIR /app

# Install uv
RUN pip install --no-cache-dir uv

# Copy dependency files first for layer caching
COPY pyproject.toml uv.lock ./

# Install production deps only (no dev tools in the image)
RUN uv sync --no-dev --frozen

# Copy source
COPY . .

# Default: start a Prefect worker. Override in docker-compose for other commands.
CMD ["uv", "run", "prefect", "worker", "start", "--pool", "default-process-pool"]
