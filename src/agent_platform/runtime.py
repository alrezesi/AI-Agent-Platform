# Shared runtime factories for API and worker processes.

from __future__ import annotations

import os
from functools import lru_cache

from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from src.agent_platform.db import ensure_schema, get_session_factory
from src.agent_platform.scheduler.in_memory import InMemoryTaskQueue
from src.agent_platform.scheduler.redis_queue import RedisTaskQueue
from src.agent_platform.scheduler.scheduler import TaskScheduler


def _queue_backend() -> str:
    return os.getenv("TASK_QUEUE_BACKEND", "memory").strip().lower()


@lru_cache(maxsize=1)
def get_redis_client() -> Redis:
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    return Redis.from_url(redis_url)


@lru_cache(maxsize=1)
def get_task_queue():
    backend = _queue_backend()
    if backend == "redis":
        return RedisTaskQueue(get_redis_client(), session_factory=get_session_factory())
    return InMemoryTaskQueue()


@lru_cache(maxsize=1)
def get_scheduler() -> TaskScheduler:
    return TaskScheduler(get_task_queue())


def reset_runtime_cache() -> None:
    """Reset cached runtime objects. Useful for tests."""
    get_scheduler.cache_clear()
    get_task_queue.cache_clear()
    get_redis_client.cache_clear()
    get_session_factory.cache_clear()


async def prepare_runtime() -> None:
    """Create any required database schema before serving traffic."""
    if _queue_backend() == "redis":
        await ensure_schema()
