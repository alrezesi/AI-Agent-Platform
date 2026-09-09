"""Diagnostic harness: capture full per-session timeline of the cancel/dequeue race.

Writes a structured log to reports/race_trace.log so we can pinpoint exactly
which BEGIN/SELECT/UPDATE/COMMIT belongs to which code path.
"""
import asyncio
import os
import sys
import logging
import time

sys.path.insert(0, '.')

logging.basicConfig(level=logging.INFO, format='%(asctime)s.%(msecs)03d %(name)s %(levelname)s %(message)s', datefmt='%H:%M:%S')
logging.getLogger('sqlalchemy.engine').setLevel(logging.INFO)

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy import event, delete

from src.agent_platform.scheduler.postgres_tasks import Base as TaskBase, TaskORM
from src.agent_platform.scheduler.redis_queue import RedisTaskQueue

URL = 'postgresql+asyncpg://agent:agent123@localhost:5433/agent_platform_test'

_TXN = [0]
_PHASE = ["init"]


def _tag(prefix):
    return f"[{prefix.upper()}-{int(time.time()*1000)%100000:05d}]"


async def main():
    import redis.asyncio as aioredis
    rc = aioredis.Redis.from_url('redis://localhost:6379/0')
    await rc.flushdb()

    engine = create_async_engine(URL, pool_size=4, max_overflow=4, echo=True)
    async with engine.begin() as conn:
        await conn.run_sync(TaskBase.metadata.create_all)
    sf = async_sessionmaker(engine, expire_on_commit=False)
    async with sf() as s:
        await s.execute(delete(TaskORM))
        await s.commit()
    await rc.flushdb()

    # Patch the relevant call sites to log which is which
    from src.agent_platform.scheduler import redis_queue as rq_mod

    orig_save = rq_mod.RedisTaskQueue._save_task_to_db
    orig_deq = rq_mod.RedisTaskQueue.dequeue
    orig_cancel = rq_mod.RedisTaskQueue.cancel

    async def logged_save(self, task):
        tag = _tag("SAVE")
        print(f"{tag} ENTER _save_task_to_db task_id={task.task_id} task.version={task.version} task.status={task.status}", flush=True)
        try:
            r = await orig_save(self, task)
            print(f"{tag} RETURN _save_task_to_db task_id={task.task_id} task.version={task.version}", flush=True)
            return r
        except Exception as e:
            print(f"{tag} RAISE _save_task_to_db {type(e).__name__}: {e}", flush=True)
            raise

    async def logged_deq(self, worker_id=None, lease_seconds=None):
        tag = _tag("DEQ")
        print(f"{tag} ENTER dequeue worker_id={worker_id}", flush=True)
        r = await orig_deq(self, worker_id, lease_seconds)
        print(f"{tag} RETURN dequeue status={r.status if r else None}", flush=True)
        return r

    async def logged_cancel(self, task_id, tenant_id=None):
        tag = _tag("CAN")
        print(f"{tag} ENTER cancel task_id={task_id}", flush=True)
        r = await orig_cancel(self, task_id, tenant_id)
        print(f"{tag} RETURN cancel result={r}", flush=True)
        return r

    rq_mod.RedisTaskQueue._save_task_to_db = logged_save
    rq_mod.RedisTaskQueue.dequeue = logged_deq
    rq_mod.RedisTaskQueue.cancel = logged_cancel

    q = RedisTaskQueue(rc, 3600, sf)

    from src.agent_platform.core.task import Task, TaskPriority
    from datetime import datetime, UTC
    t = Task(task_id='race-cancel-001', agent_id='agent-a', type='echo', payload={}, priority=TaskPriority.MEDIUM, created_at=datetime.now(UTC))
    print(f"{_tag('SET')} enqueue start", flush=True)
    await q.enqueue(t)
    print(f"{_tag('SET')} enqueue done", flush=True)

    async def cancel():
        await asyncio.sleep(0.01)
        try:
            r = await q.cancel('race-cancel-001')
            print(f"{_tag('CAN-T')} CANCEL_RESULT={r}", flush=True)
        except Exception as e:
            print(f"{_tag('CAN-T')} CANCEL_EXC={type(e).__name__} {e}", flush=True)

    async def deq():
        r = await q.dequeue('w-a', 30)
        print(f"{_tag('DEQ-T')} DEQUEUE_RESULT={r.status if r else None}", flush=True)

    await asyncio.gather(cancel(), deq())

    final = await q.get_task('race-cancel-001')
    print(f"{_tag('SET')} FINAL_STATUS={final.status if final else None}", flush=True)
    async with sf() as s:
        from sqlalchemy import select
        r = (await s.execute(select(TaskORM))).scalars().first()
        print(f"{_tag('SET')} DB_FINAL status={r.status} version={r.version}", flush=True)

    await rc.aclose()
    await engine.dispose()


asyncio.run(main())