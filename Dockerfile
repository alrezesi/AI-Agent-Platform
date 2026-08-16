# Use Python 3.13 slim image based on Debian 12 (bookworm) – stable and reliable
FROM python:3.13-slim-bookworm

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        gcc \
        libpq-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy pyproject.toml and install Python dependencies (including dev extras)
COPY pyproject.toml .
RUN pip install --no-cache-dir -e ".[dev]"

# Copy the rest of the application code
COPY src/ ./src/
COPY scripts/ ./scripts/
COPY tests/ ./tests/

# Copy Alembic configuration and migration files
COPY alembic.ini .
COPY migrations ./migrations

# Set Python path
ENV PYTHONPATH=/app

# Default command (overridden in docker-compose.yml)
CMD ["uvicorn", "src.agent_platform.api.main:app", "--host", "0.0.0.0", "--port", "8000"]