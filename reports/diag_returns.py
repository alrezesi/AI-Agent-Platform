"""Diagnose: directly check what cancel() and dequeue() return + post-DB state."""
import asyncio
import sys
sys.path.insert(0, '.')

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy import delete, select

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

    q = RedisTaskQueue(rc, 3600, sf)

    from src.agent_platform.core.task import Task, TaskPriority
    from src.agent_platform.scheduler.exceptions import TaskWriteConflictError
    from datetime import datetime, UTC
    t = Task(task_id='race-cancel-001', agent_id='agent-a', type='echo', payload={}, priority=TaskPriority.MEDIUM, created_at=datetime.now(UTC))
    await q.enqueue(t)

    cancel_result = None
    dequeue_result = None
    cancel_exc = None
    dequeue_exc = None

    async def cancel_task():
        nonlocal cancel_result, cancel_exc
        await asyncio.sleep(0.01)
        try:
            cancel_result = await q.cancel('race-cancel-001')
            print(f"[CANCEL] result={cancel_result!r}", flush=True)
        except Exception as e:
            cancel_exc = e
            print(f"[CANCEL] EXC {type(e).__name__}: {e}", flush=True)

    async def dequeue_task():
        nonlocal dequeue_result, dequeue_exc
        try:
            dequeue_result = await q.dequeue('w-a', 30)
            print(f"[DEQUEUE] result={dequeue_result!r}", flush=True)
        except Exception as e:
            dequeue_exc = e
            print(f"[DEQUEUE] EXC {type(e).__name__}: {e}", flush=True)

    await asyncio.gather(cancel_task(), dequeue_task())

    print()
    print(f"cancel_result = {cancel_result!r}", flush=True)
    print(f"cancel_exc    = {cancel_exc!r}", flush=True)
    print(f"dequeue_result = {dequeue_result!r}", flush=True)
    print(f"dequeue_exc   = {dequeue_exc!r}", flush=True)

    # Now read DB directly
    async with sf() as s:
        r = (await s.execute(select(TaskORM).where(TaskORM.task_id == 'race-cancel-001'))).scalars().first()
        print(f"\nDB ROW: status={r.status} version={r.version} lease_owner={r.lease_owner} started_at={r.started_at} completed_at={r.completed_at}", flush=True)

    # Read Redis cache
    cached = await rc.get(f"tasks:data:race-cancel-001")
    print(f"REDIS CACHE: {cached}", flush=True)

    await rc.aclose()
    await engine.dispose()

asyncio.run(main())