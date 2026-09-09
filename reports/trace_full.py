"""Capture exact BEGIN, COMMIT, ROLLBACK, and SQL per-connection."""
import asyncio
import os
import sys
import time
import logging

sys.path.insert(0, '.')

logging.basicConfig(level=logging.WARNING)

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy import event, delete

from src.agent_platform.scheduler.postgres_tasks import Base as TaskBase, TaskORM

URL = 'postgresql+asyncpg://agent:agent123@localhost:5433/agent_platform_test'
_engine = create_async_engine(URL, pool_size=4, max_overflow=4)

@event.listens_for(_engine.sync_engine, "begin")
def _on_begin(conn):
    print(f"  [BEGIN-EVT {time.time():.4f}] conn={id(conn)}", flush=True)

@event.listens_for(_engine.sync_engine, "commit")
def _on_commit(conn):
    print(f"  [COMMIT-EVT {time.time():.4f}] conn={id(conn)}", flush=True)

@event.listens_for(_engine.sync_engine, "rollback")
def _on_rollback(conn):
    print(f"  [ROLLBACK-EVT {time.time():.4f}] conn={id(conn)}", flush=True)

@event.listens_for(_engine.sync_engine, "before_cursor_execute")
def _on_exec(conn, cursor, statement, parameters, context, executemany):
    print(f"  [SQL {time.time():.4f} conn={id(conn)}] {statement.splitlines()[0][:130]}", flush=True)

async def main():
    import redis.asyncio as aioredis
    rc = aioredis.Redis.from_url('redis://localhost:6379/0')

    async with _engine.begin() as conn:
        await conn.run_sync(TaskBase.metadata.create_all)
    sf = async_sessionmaker(_engine, expire_on_commit=False)
    async with sf() as s:
        await s.execute(delete(TaskORM))
        await s.commit()
    await rc.flushdb()

    from src.agent_platform.scheduler.redis_queue import RedisTaskQueue
    q = RedisTaskQueue(rc, 3600, sf)

    from src.agent_platform.core.task import Task, TaskPriority
    from datetime import datetime, UTC
    t = Task(task_id='race-cancel-001', agent_id='agent-a', type='echo', payload={}, priority=TaskPriority.MEDIUM, created_at=datetime.now(UTC))
    await q.enqueue(t)

    print(f">>> GATHER START", flush=True)
    async def cancel():
        await asyncio.sleep(0.01)
        r = await q.cancel('race-cancel-001')
        print(f">>> CANCEL done result={r}", flush=True)
    async def deq():
        r = await q.dequeue('w-a', 30)
        print(f">>> DEQUEUE done status={r.status if r else None}", flush=True)

    await asyncio.gather(cancel(), deq())
    print(f">>> GATHER DONE", flush=True)
    await rc.aclose()
    await _engine.dispose()

asyncio.run(main())