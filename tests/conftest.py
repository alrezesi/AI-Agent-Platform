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
from src.agent_platform.db import get_engine, get_session_factory
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

def _resolve_database_url() -> str:
    """Resolve the async PostgreSQL URL for tests.

    CI exports ``DATABASE_URL`` (sync scheme, port 5432, db ``agent_platform``)
    at the workflow level, but the unit/integration steps do *not* also export
    ``POSTGRES_URL`` (only the concurrency/race/security/observability steps do).
    Prefer an explicit ``POSTGRES_URL``; otherwise derive an asyncpg URL from
    ``DATABASE_URL`` so the PostgreSQL-backed fixtures connect to the very same
    live cluster the service containers expose.  The previous hard-coded default
    pointed at port 5433 / db ``agent_platform_test``, neither of which exists
    in CI, which caused every PostgreSQL-backed test to fail at setup.  Fall
    back to the local docker-compose endpoint only when neither variable is set.
    """
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        if db_url.startswith("postgresql://"):
            return "postgresql+asyncpg://" + db_url[len("postgresql://") :]
        return db_url
    return "postgresql+asyncpg://agent:agent123@localhost:5433/agent_platform"


REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
POSTGRES_URL = os.getenv("POSTGRES_URL") or _resolve_database_url()


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
    # Ensure schema exists AND is up-to-date with all alembic migrations.
    # ``Base.metadata.create_all`` only creates tables that don't exist; it
    # does NOT add columns that were added by later migrations (e.g.
    # ``message_id``, ``error_category``, ``retry_history``).  Running
    # ``alembic upgrade head`` against the test database ensures every
    # migration is applied so the ORM model and the real Postgres schema
    # stay in sync — which is what the concurrency / race / security /
    # observability suites (all backed by real Postgres) require.
    async with engine.begin() as conn:
        await conn.run_sync(TaskBase.metadata.create_all)
    await engine.dispose()  # close the async engine before running migrations
    # Apply alembic migrations to add columns that ``create_all`` does not
    # handle (e.g. ``message_id``, ``error_category``, ``retry_history``).
    # Run as a subprocess so alembic's own ``env.py`` (which calls
    # ``asyncio.run``) does not conflict with the pytest-asyncio event loop.
    _run_migrations_subprocess(POSTGRES_URL)
    engine = create_async_engine(POSTGRES_URL, pool_size=2, max_overflow=2)
    yield engine
    await engine.dispose()


def _run_migrations_subprocess(db_url: str) -> None:
    """Apply alembic ``upgrade head`` via subprocess to avoid event-loop conflicts."""
    import subprocess
    import sys

    sync_url = db_url
    if sync_url.startswith("postgresql+asyncpg://"):
        sync_url = "postgresql://" + sync_url[len("postgresql+asyncpg://"):]

    env = os.environ.copy()
    env["DATABASE_URL"] = sync_url
    result = subprocess.run(
        [sys.executable, "-m", "alembic", "-c", str(PROJECT_ROOT / "alembic.ini"),
         "upgrade", "head"],
        cwd=str(PROJECT_ROOT), capture_output=True, text=True, env=env,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"alembic upgrade failed (exit {result.returncode}):\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )


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


# ---------------------------------------------------------------------------
# Loop-bound async singleton isolation for starlette 1.0.0 TestClient
# ---------------------------------------------------------------------------
#
# starlette 1.0.0's ``TestClient`` (when used without a ``with`` block) spins
# up a fresh ``anyio`` event loop for *every* ``client.get/post(...)`` call and
# tears it down when the call returns.  The application caches its async
# PostgreSQL engine, session factory, Redis client, task queue and scheduler as
# ``functools.lru_cache`` singletons (see ``src/agent_platform/runtime.py``).
# Once one of those singletons is bound to a request's event loop and that loop
# closes, reusing the cached object on a later request blows up with
# ``RuntimeError: Event loop is closed`` (surfacing as
# ``AttributeError: 'NoneType' object has no attribute 'send'`` from the ASGI
# transport).
#
# The fix is purely a test-harness lifecycle mismatch -- in production uvicorn
# runs a single long-lived loop so the singletons work as intended -- and it is
# *not* correct to weaken the engine/redis caching in production code.  We clear
# only the loop-bound async singletons before and after each test so every loop
# gets fresh clients, while leaving the shared in-memory tenant store (and its
# authentication state) untouched.  This is the single source of truth for that
# isolation; ``tests/integration/conftest.py`` mirrors it for the integration
# suite.
_LOOP_BOUND_SINGLETS = (get_engine, get_session_factory, runtime.get_redis_client, runtime.get_task_queue, runtime.get_scheduler)


@pytest.fixture(autouse=True)
def _reset_loop_bound_runtime():
    """Recreate loop-bound async singletons around every test."""
    for factory in _LOOP_BOUND_SINGLETS:
        factory.cache_clear()
    yield
    for factory in _LOOP_BOUND_SINGLETS:
        factory.cache_clear()
