# Shared runtime factories for API and worker processes.

from __future__ import annotations

import os
from functools import lru_cache

from redis.asyncio import Redis

from src.agent_platform.db import ensure_schema, get_session_factory
from src.agent_platform.multi_tenant.manager import TenantManager
from src.agent_platform.scheduler.in_memory import InMemoryTaskQueue
from src.agent_platform.scheduler.redis_queue import RedisTaskQueue
from src.agent_platform.scheduler.scheduler import TaskScheduler


def _queue_backend() -> str:
    return os.getenv("TASK_QUEUE_BACKEND", "memory").strip().lower()


class _TenantStorage:
    def __init__(self) -> None:
        self._tenants: dict[str, object] = {}


_tenant_storage = _TenantStorage()


def _configure_cpu_runtime() -> None:
    """
    Keep each container/process inside a predictable CPU envelope.

    The local audit stack runs on a CPU-only machine with two workers, so we
    keep BLAS/OpenMP thread counts at one unless the operator has explicitly
    overridden them.
    """
    defaults = {
        "TOKENIZERS_PARALLELISM": "false",
        "OMP_NUM_THREADS": "1",
        "MKL_NUM_THREADS": "1",
        "OPENBLAS_NUM_THREADS": "1",
        "NUMEXPR_NUM_THREADS": "1",
        "VECLIB_MAXIMUM_THREADS": "1",
    }
    for key, value in defaults.items():
        os.environ.setdefault(key, value)
    try:
        import torch

        torch.set_num_threads(int(os.getenv("TORCH_NUM_THREADS", "1")))
        torch.set_num_interop_threads(int(os.getenv("TORCH_NUM_INTEROP_THREADS", "1")))
    except Exception:
        # Torch is optional in some unit-test paths.
        pass


_configure_cpu_runtime()


@lru_cache(maxsize=1)
def get_redis_client() -> Redis:
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    return Redis.from_url(redis_url)


@lru_cache(maxsize=1)
def get_task_queue() -> RedisTaskQueue | InMemoryTaskQueue:
    backend = _queue_backend()
    if backend == "redis":
        return RedisTaskQueue(get_redis_client(), session_factory=get_session_factory())
    return InMemoryTaskQueue()


@lru_cache(maxsize=1)
def get_scheduler() -> TaskScheduler:
    return TaskScheduler(get_task_queue())


@lru_cache(maxsize=1)
def get_tenant_manager() -> TenantManager:
    return TenantManager(_tenant_storage)


def reset_runtime_cache() -> None:
    """Reset cached runtime objects. Useful for tests."""
    get_scheduler.cache_clear()
    get_task_queue.cache_clear()
    get_redis_client.cache_clear()
    get_tenant_manager.cache_clear()
    get_session_factory.cache_clear()
    _tenant_storage._tenants.clear()


async def prepare_runtime() -> None:
    """Create any required database schema before serving traffic."""
    if _queue_backend() == "redis":
        await ensure_schema()
