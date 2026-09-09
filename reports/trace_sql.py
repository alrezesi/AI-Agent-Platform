"""Trace each SQL statement with its connection id and call site."""
import asyncio
import os
import sys
import logging
import time
import traceback

sys.path.insert(0, '.')

logging.basicConfig(level=logging.WARNING)

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy import event, delete

from src.agent_platform.scheduler.postgres_tasks import Base as TaskBase, TaskORM

URL = 'postgresql+asyncpg://agent:agent123@localhost:5433/agent_platform_test'
_engine = create_async_engine(URL, pool_size=4, max_overflow=4)

# We'll record SQL on connection events. Async engine emits "before_cursor_execute"
# on the sync engine. Each DBAPI connection has its own context, and we can fetch
# the connection id from the bind.

@event.listens_for(_engine.sync_engine, "before_cursor_execute")
def _on_exec(conn, cursor, statement, parameters, context, executemany):
    # Look for redis_queue frames in current call stack
    stack = "".join(traceback.format_stack(limit=30))
    interesting = []
    for line in stack.splitlines()[::-1]:
        line = line.strip()
        if "redis_queue.py" in line:
            interesting.append(line)
            if len(interesting) >= 1:
                break
    site = interesting[0] if interesting else ""
    # Extract first 80 chars of statement for trace
    stmt_first = statement.split('\n', 1)[0][:80]
    # SQLite/Postgres parameter placeholder
    print(f"[SQL {time.time():.4f} conn={id(conn)}] {stmt_first} | {site}", flush=True)


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

    from src.agent_platform.scheduler.redis_queue import RedisTaskQueue
    q = RedisTaskQueue(rc, 3600, sf)

    from src.agent_platform.core.task import Task, TaskPriority
    from datetime import datetime, UTC
    t = Task(task_id='race-cancel-001', agent_id='agent-a', type='echo', payload={}, priority=TaskPriority.MEDIUM, created_at=datetime.now(UTC))
    await q.enqueue(t)

    async def cancel():
        await asyncio.sleep(0.01)
        r = await q.cancel('race-cancel-001')
        print(f"[CANCEL-T {time.time():.4f}] result={r}", flush=True)
    async def deq():
        r = await q.dequeue('w-a', 30)
        print(f"[DEQ-T {time.time():.4f}] status={r.status if r else None}", flush=True)

    await asyncio.gather(cancel(), deq())
    await rc.aclose()
    await _engine.dispose()

asyncio.run(main())