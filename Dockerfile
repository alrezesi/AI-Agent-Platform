FROM python:3.13-alpine AS builder

WORKDIR /app

RUN apk add --no-cache \
    musl-dev \
    gcc \
    g++ \
    postgresql-dev \
    curl

COPY pyproject.toml README.md alembic.ini ./

RUN pip install --upgrade pip \
    && pip install --extra-index-url https://download.pytorch.org/whl/cpu -e . \
    && pip install uvicorn

FROM python:3.13-alpine

WORKDIR /app

RUN apk add --no-cache \
    libpq \
    curl

ENV PIP_NO_CACHE_DIR=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    TOKENIZERS_PARALLELISM=false \
    OMP_NUM_THREADS=1 \
    MKL_NUM_THREADS=1 \
    OPENBLAS_NUM_THREADS=1 \
    NUMEXPR_NUM_THREADS=1 \
    VECLIB_MAXIMUM_THREADS=1 \
    TORCH_NUM_THREADS=1 \
    TORCH_NUM_INTEROP_THREADS=1 \
    HF_HUB_OFFLINE=1 \
    TRANSFORMERS_OFFLINE=1 \
    BGE_MODEL_PATH=/app/models/bge-m3

COPY --from=builder /usr/local/lib/python3.13/site-packages /usr/local/lib/python3.13/site-packages
COPY --from=builder /usr/local/bin /usr/local/bin

COPY src ./src
COPY migrations ./migrations
COPY scripts ./scripts

ENV PYTHONPATH=/app

CMD ["uvicorn", "src.agent_platform.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
