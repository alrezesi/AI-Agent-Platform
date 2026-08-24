FROM python:3.13-slim-bookworm

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libpq-dev \
        curl \
    || (apt-get install -y --fix-missing \
        libpq-dev \
        curl) \
    && rm -rf /var/lib/apt/lists/*

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

COPY pyproject.toml README.md alembic.ini ./

RUN pip install --upgrade pip \
    && pip install --extra-index-url https://download.pytorch.org/whl/cpu -e .

COPY src ./src
COPY migrations ./migrations
COPY scripts ./scripts

ENV PYTHONPATH=/app

CMD ["uvicorn", "src.agent_platform.api.main:app", "--host", "0.0.0.0", "--port", "8000"]
