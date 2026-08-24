"""Shared fixtures and configuration for all tests."""

from __future__ import annotations

import asyncio
import os
import sys
import uuid
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pytest
from fastapi import FastAPI
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.agent_platform.scheduler.postgres_tasks import Base as TaskBase
from src.agent_platform.scheduler.postgres_tasks import TaskORM
from src.agent_platform.scheduler.redis_queue import RedisTaskQueue
from src.agent_platform.scheduler.scheduler import TaskScheduler

REPORTS_DIR = PROJECT_ROOT / "reports"
REPORTS_DIR.mkdir(exist_ok=True)

# ruff: noqa: E402
# Import runtime to monkeypatch
from src.agent_platform import runtime
from src.agent_platform.api.main import app as original_app
from src.agent_platform.api.routes.tasks import get_scheduler as original_get_scheduler
from src.agent_platform.api.routes.tenants import get_tenant_manager as original_get_tenant_manager
from src.agent_platform.scheduler.in_memory import InMemoryTaskQueue


# Shared storage for tenants (persists across the test session)
class SharedStorage:
    _tenants = {}

shared_storage = SharedStorage()

def get_test_tenant_manager():
    """Return the runtime tenant manager backed by shared in-memory storage."""
    return runtime.get_tenant_manager()


# CRITICAL: Make the runtime storage point at the same shared in-memory tenant store
# used by the tests so any runtime-created manager sees the same tenants.
runtime._tenant_storage = shared_storage
runtime.reset_runtime_cache()


# Singleton scheduler instance
_scheduler = None

def get_test_scheduler() -> TaskScheduler:
    """Return a singleton TaskScheduler for tests."""
    global _scheduler
    if _scheduler is None:
        _scheduler = TaskScheduler(InMemoryTaskQueue())
    return _scheduler


# Override the app with test dependencies
@pytest.fixture(scope="session")
def app() -> FastAPI:
    """Return a FastAPI app with test dependency overrides."""
    app = original_app
    app.dependency_overrides[original_get_tenant_manager] = get_test_tenant_manager
    app.dependency_overrides[original_get_scheduler] = get_test_scheduler
    for middleware in app.user_middleware:
        if getattr(middleware.cls, "__name__", "") == "TenantMiddleware":
            middleware.kwargs["tenant_manager"] = get_test_tenant_manager()
    app.middleware_stack = app.build_middleware_stack()
    return app


# ---------------------------------------------------------------------------
# Real Redis + PostgreSQL fixtures for concurrency / race / security tests
# ---------------------------------------------------------------------------

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
POSTGRES_URL = os.getenv(
    "POSTGRES_URL",
    "postgresql+asyncpg://agent:agent123@localhost:5433/agent_platform_test",
)


def _docker_available() -> bool:
    import subprocess

    try:
        subprocess.run(["docker", "info"], capture_output=True, text=True, timeout=10, check=True)
        return True
    except Exception:
        return False


@pytest.fixture(scope="session")
def docker_ready():
    """Skip tests that require Docker/Redis/Postgres when unavailable."""
    if not _docker_available():
        pytest.skip("Docker not available for this test session")
    import redis.asyncio

    try:
        r = redis.asyncio.Redis.from_url(REDIS_URL)
        asyncio.get_event_loop().run_until_complete(r.ping())
        asyncio.get_event_loop().run_until_complete(r.aclose())
    except Exception:
        pytest.skip("Redis not reachable at " + REDIS_URL)


@pytest.fixture
async def pg_engine():
    """Create an async PostgreSQL engine. Function-scoped to avoid
    cross-loop issues with pytest-asyncio."""
    engine = create_async_engine(POSTGRES_URL, pool_size=2, max_overflow=2)
    # Ensure schema exists
    async with engine.begin() as conn:
        await conn.run_sync(TaskBase.metadata.create_all)
    yield engine
    await engine.dispose()


@pytest.fixture
async def pg_session_factory(pg_engine):
    """Provide a session factory backed by real PostgreSQL."""
    return async_sessionmaker(pg_engine, expire_on_commit=False)


@pytest.fixture
async def clean_db(pg_session_factory):
    """Truncate task tables before and after each test that needs a clean DB."""
    from sqlalchemy import delete

    async with pg_session_factory() as session:
        await session.execute(delete(TaskORM))
        await session.commit()
    yield pg_session_factory
    async with pg_session_factory() as session:
        await session.execute(delete(TaskORM))
        await session.commit()


@pytest.fixture
async def redis_client():
    """Provide a real Redis client, flushed before and after each test."""
    import redis.asyncio

    r = redis.asyncio.Redis.from_url(REDIS_URL)
    await r.flushdb()
    yield r
    await r.flushdb()
    await r.aclose()


@pytest.fixture
async def redis_queue(redis_client, pg_session_factory):
    """Provide a RedisTaskQueue backed by real Redis and real PostgreSQL."""
    queue = RedisTaskQueue(redis_client=redis_client, ttl_seconds=3600, session_factory=pg_session_factory)
    yield queue


@pytest.fixture
def unique_id():
    """Generate a unique ID for test isolation."""
    return uuid.uuid4().hex[:8]
