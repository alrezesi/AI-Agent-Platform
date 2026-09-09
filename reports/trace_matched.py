"""Diagnose: capture the actual _matched value in dequeue."""
import asyncio
import os
import sys
import time

sys.path.insert(0, '.')

import logging
logging.basicConfig(level=logging.WARNING)

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy import delete

from src.agent_platform.scheduler.postgres_tasks import Base as TaskBase, TaskORM
from src.agent_platform.scheduler.redis_queue import RedisTaskQueue

URL = 'postgresql+asyncpg://agent:agent123@localhost:5433/agent_platform_test'

async def main():
    import redis.asyncio as aioredis
    rc = aioredis.Redis.from_url('redis://localhost:6379/0')

    engine = create_async_engine(URL, pool_size=4, max_overflow=4)
    async with engine.begin() as conn:
        await conn.run_sync(TaskBase.metadata.create_all)
    sf = async_sessionmaker(engine, expire_on_commit=False)
    async with sf() as s:
        await s.execute(delete(TaskORM))
        await s.commit()
    await rc.flushdb()

    # Monkeypatch dequeue to log _matched exactly
    from src.agent_platform.scheduler import redis_queue as rq_mod

    orig_deq = rq_mod.RedisTaskQueue.dequeue

    async def traced_dequeue(self, worker_id=None, lease_seconds=None):
        print(f"[DEQ-START t={time.time():.4f}]", flush=True)
        # patch via subclass to log _matched
        return await orig_deq(self, worker_id, lease_seconds)

    rq_mod.RedisTaskQueue.dequeue = traced_dequeue

    # Patch the inner code to print _matched
    from sqlalchemy import update as sa_update
    from sqlalchemy import update as sql_update
    from sqlalchemy.ext.asyncio import AsyncSession

    # Hook session.execute to capture RETURNING result
    orig_execute = AsyncSession.execute
    async def traced_execute(self, statement, *a, **kw):
        result = await orig_execute(self, statement, *a, **kw)
        try:
            stmt_str = str(statement)
            if "WHERE tasks.task_id = " in stmt_str and "version" in stmt_str and "RETURNING" in stmt_str:
                # Try to fetch rowcount
                row = result.scalar_one_or_none()
                print(f"[EXEC t={time.time():.4f}] Core-UPDATE returned: {row!r}", flush=True)
        except Exception as e:
            pass
        return result
    AsyncSession.execute = traced_execute

    q = RedisTaskQueue(rc, 3600, sf)

    from src.agent_platform.core.task import Task, TaskPriority
    from datetime import datetime, UTC
    t = Task(task_id='race-cancel-001', agent_id='agent-a', type='echo', payload={}, priority=TaskPriority.MEDIUM, created_at=datetime.now(UTC))
    await q.enqueue(t)

    async def cancel():
        await asyncio.sleep(0.01)
        try:
            r = await q.cancel('race-cancel-001')
            print(f"[CANCEL-T t={time.time():.4f}] result={r!r}", flush=True)
        except Exception as e:
            print(f"[CANCEL-T t={time.time():.4f}] EXC {type(e).__name__}: {e}", flush=True)
    async def deq():
        try:
            r = await q.dequeue('w-a', 30)
            print(f"[DEQUEUE-T t={time.time():.4f}] result={r!r}", flush=True)
        except Exception as e:
            print(f"[DEQUEUE-T t={time.time():.4f}] EXC {type(e).__name__}: {e}", flush=True)

    await asyncio.gather(cancel(), deq())
    await rc.aclose()
    await engine.dispose()

asyncio.run(main())