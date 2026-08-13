# 🤖 AI Agent Platform

[![Python](https://img.shields.io/badge/python-3.11%2B-blue.svg)](https://www.python.org/)
[![FastAPI](https://img.shields.io/badge/FastAPI-0.104+-green.svg)](https://fastapi.tiangolo.com/)
[![pytest](https://img.shields.io/badge/pytest-7.4+-orange.svg)](https://docs.pytest.org/)
[![Coverage](https://img.shields.io/badge/coverage-82%25-brightgreen.svg)](https://github.com/your-repo/ai-agent-platform)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**A multi-tenant orchestration platform for AI agents.**

---

## Overview

The AI Agent Platform provides a compact but complete foundation for building agent-based systems. It includes:

- agent registration, heartbeat, and discovery
- priority-based task scheduling
- point-to-point, broadcast, and topic-based messaging
- workflow execution with dependencies and fallback steps
- agent-to-agent collaboration patterns
- tool validation and execution
- plugin discovery and hook registration
- retry, circuit breaker, checkpointing, and idempotency
- Redis-backed registry, queue, and message bus implementations
- multi-tenant management and API key handling
- monitoring endpoints for status, metrics, traces, and logs

---

## Tech stack

| Category | Technologies |
|---|---|
| Language | Python 3.11+ |
| API | FastAPI, Uvicorn |
| Data | PostgreSQL, Redis, SQLAlchemy, asyncpg |
| Validation | Pydantic |
| Testing | pytest, pytest-asyncio, pytest-cov |
| Infrastructure | Docker, Docker Compose |

---

## Browser entry points

After starting the server, open:

- `http://127.0.0.1:8000/` — landing page
- `http://127.0.0.1:8000/docs` — interactive API docs
- `http://127.0.0.1:8000/health` — health check

The monitoring endpoints are API endpoints, not a separate HTML dashboard:

- `http://127.0.0.1:8000/monitoring/status`
- `http://127.0.0.1:8000/monitoring/agents`
- `http://127.0.0.1:8000/monitoring/tasks`
- `http://127.0.0.1:8000/monitoring/metrics`
- `http://127.0.0.1:8000/monitoring/traces`
- `http://127.0.0.1:8000/monitoring/logs`

---

## Quick start

### Prerequisites

- Python 3.11 or newer
- Docker and Docker Compose
- Git

### 1. Clone the repository

```powershell
git clone https://github.com/your-repo/ai-agent-platform.git
cd ai-agent-platform
```

### 2. Create and activate a virtual environment

```powershell
python -m venv .venv
.\.venv\Scripts\activate
```

### 3. Install dependencies

```powershell
pip install -e ".[dev]"
```

### 4. Configure environment variables

Create a `.env` file:

```env
DATABASE_URL=postgresql://user:pass@localhost:5433/agent_platform
REDIS_URL=redis://localhost:6379/0
API_HOST=127.0.0.1
API_PORT=8000
LOG_LEVEL=INFO
```

### 5. Start infrastructure services

```powershell
docker-compose up -d
```

### 6. Run the API server

```powershell
uvicorn src.agent_platform.api.main:app --reload --host 127.0.0.1 --port 8000
```

---

## Testing

Run the full suite:

```powershell
pytest tests/ -v --cov=src/agent_platform --cov-report=term --cov-report=html
```

Current test coverage: 82% with 151 passing tests.

---

## Project structure

```text
ai-agent-platform/
├── src/agent_platform/
│   ├── api/
│   ├── core/
│   ├── registry/
│   ├── scheduler/
│   ├── message_bus/
│   ├── workflow/
│   ├── a2a/
│   ├── tools/
│   ├── plugins/
│   ├── recovery/
│   ├── distributed/
│   ├── multi_tenant/
│   └── monitoring/
├── tests/
│   ├── unit/
│   └── integration/
├── scripts/
├── docker-compose.yml
├── pyproject.toml
└── README.md
```

---

## Notes

- The project currently does not include a `docs/` directory.
- The root page is a lightweight landing page for the API, not a separate frontend app.
- PostgreSQL is mapped to `localhost:5433` in `docker-compose.yml`.

---

## Contributing

1. Fork the repository
2. Create a feature branch
3. Write tests and implement changes
4. Make sure the test suite passes
5. Open a pull request


