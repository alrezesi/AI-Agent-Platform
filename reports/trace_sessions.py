"""Map each BEGIN/COMMIT/ROLLBACK to its source (cancel vs dequeue) using SQLAlchemy events."""
import asyncio
import os
import sys
import logging
import time
import threading

sys.path.insert(0, '.')

logging.basicConfig(level=logging.WARNING)

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy import event, delete

from src.agent_platform.scheduler.postgres_tasks import Base as TaskBase, TaskORM

URL = 'postgresql+asyncpg://agent:agent123@localhost:5433/agent_platform_test'

_engine = create_async_engine(URL, pool_size=4, max_overflow=4)


@event.listens_for(_engine.sync_engine, "checkout")
def _on_checkout(dbapi_connection, connection_record, connection_proxy):
    import traceback
    stack = "".join(traceback.format_stack(limit=8))
    print(f"[CHK {time.time():.3f}] conn={id(dbapi_connection)} {stack[-1200:]}", flush=True)

@event.listens_for(_engine.sync_engine, "checkin")
def _on_checkin(dbapi_connection, connection_record):
    import traceback
    stack = "".join(traceback.format_stack(limit=8))
    print(f"[RELEASE {time.time():.3f}] conn={id(dbapi_connection)} {stack[-1200:]}", flush=True)


async def main():
    import redis.asyncio as aioredis
    rc = aioredis.Redis.from_url('redis://localhost:6379/0')
    await rc.flushdb()

    async with _engine.begin() as conn:
        await conn.run_sync(TaskBase.metadata.create_all)
    sf = async_sessionmaker(_engine, expire_on_commit=False)
    async with sf() as s:
        await s.execute(delete(TaskORM))
        await s.commit()
    await rc.flushdb()

    # Track which Python frame is responsible for each transaction begin
    from src.agent_platform.scheduler import redis_queue as rq_mod
    orig_session_factory_init = sf
    import sqlalchemy.ext.asyncio as _as

    # We'll wrap session_factory().begin
    class TracedSessionFactory:
        def __init__(self, inner):
            self._inner = inner
        def __call__(self):
            s = self._inner()
            orig_begin = s.begin
            def traced_begin(*a, **kw):
                # capture stack
                import traceback
                stack = "".join(traceback.format_stack(limit=12))
                # find interesting frame
                print(f"[SESS-BEGIN {time.time():.3f}] session={id(s)}\n{stack[-1500:]}", flush=True)
                return orig_begin(*a, **kw)
            s.begin = traced_begin
            return s
        def __getattr__(self, n):
            return getattr(self._inner, n)

    traced_sf = TracedSessionFactory(sf)

    from src.agent_platform.scheduler.redis_queue import RedisTaskQueue
    q = RedisTaskQueue(rc, 3600, traced_sf)

    from src.agent_platform.core.task import Task, TaskPriority
    from datetime import datetime, UTC
    t = Task(task_id='race-cancel-001', agent_id='agent-a', type='echo', payload={}, priority=TaskPriority.MEDIUM, created_at=datetime.now(UTC))
    await q.enqueue(t)

    async def cancel():
        await asyncio.sleep(0.01)
        r = await q.cancel('race-cancel-001')
        print(f"[CANCEL-T {time.time():.3f}] result={r}", flush=True)
    async def deq():
        r = await q.dequeue('w-a', 30)
        print(f"[DEQ-T {time.time():.3f}] status={r.status if r else None}", flush=True)

    await asyncio.gather(cancel(), deq())
    await rc.aclose()
    await _engine.dispose()


asyncio.run(main())