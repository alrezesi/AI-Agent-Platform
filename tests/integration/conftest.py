"""Integration-test isolation.

The CI integration suite drives the application through the **synchronous**
``fastapi.testclient.TestClient``.  In the installed starlette, an un-scoped
``TestClient`` (no ``with`` block) spins up a fresh ``anyio.BlockingPortal`` --
i.e. a fresh event loop -- for *every* ``client.get/post(...)`` call and tears
the loop down again when the call returns.

The application keeps its async PostgreSQL engine and async Redis client as
``functools.lru_cache`` singletons (``runtime.get_engine``,
``runtime.get_redis_client``, ``runtime.get_task_queue`` and
``runtime.get_scheduler``).  Once such a client is bound to one request's
event loop and that loop is closed, every subsequent request that reuses the
cached client fails with ``RuntimeError: Event loop is closed`` (surfacing as
``AttributeError: 'NoneType' object has no attribute 'send'`` from the ASGI
transport).  This is exactly what made ``test_monitoring_tasks`` pass in
isolation but fail in the full suite: ``GET /monitoring/status`` was the first
request to touch the lru-cached engine (in loop A), the loop closed, and
``GET /monitoring/tasks`` then reused the stale engine.

This is purely a test-harness lifecycle mismatch -- in production uvicorn runs
a single long-lived loop so the singletons work as intended -- and it is *not*
correct to "fix" it by weakening the engine/redis caching in production code.
The codebase already ships :func:`src.agent_platform.runtime.reset_runtime_cache`
so cached, loop-bound objects can be recreated per test (see
``tests/unit/test_api_tasks_runtime.py``).  We recreate only the loop-bound
async singletons before each integration test so every request gets a client
bound to its own loop, while leaving the shared in-memory tenant store (and
its authentication state) untouched.
"""

from __future__ import annotations

import pytest

from src.agent_platform.db import get_engine, get_session_factory
from src.agent_platform.runtime import get_redis_client, get_scheduler, get_task_queue

# Loop-bound async singletons that must be recreated per test/event-loop.
_LOOP_BOUND_SINGLETS = (get_engine, get_session_factory, get_redis_client, get_task_queue, get_scheduler)


@pytest.fixture(autouse=True)
def _reset_loop_bound_runtime() -> None:
    """Recreate loop-bound async singletons before and after every integration test."""
    for factory in _LOOP_BOUND_SINGLETS:
        factory.cache_clear()
    yield
    for factory in _LOOP_BOUND_SINGLETS:
        factory.cache_clear()
