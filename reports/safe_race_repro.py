"""Pure SQL-level reproduction harness for the cancel/dequeue race.

This script does NOT modify any application code. It instruments only a
dedicated test engine via SQLAlchemy's built-in `echo=True` so we can see
the actual SQL statements + their connection identity without changing
dequeue() / cancel() / _save_task_to_db() control flow.

This is the safe replacement for the previous in-application log
instrumentation, which inadvertently moved `task.version = expected_version + 1`
inside the `if _matched is None:` block (making it dead code) and
caused 10 test regressions.
"""
import asyncio
import sys
import time

sys.path.insert(0, '.')

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy import event, delete

from src.agent_platform.scheduler.postgres_tasks import Base as TaskBase, TaskORM
from src.agent_platform.scheduler.redis_queue import RedisTaskQueue
from src.agent_platform.core.task import Task, TaskPriority
from src.agent_platform.scheduler.exceptions import TaskWriteConflictError

from datetime import datetime, UTC

URL = 'postgresql+asyncpg://agent:agent123@localhost:5433/agent_platform_test'
REDIS_URL = 'redis://localhost:6379/0'

# Each connection's checkout is logged with a small per-conn tag so we
# can tell which AsyncSession / conn executed which statements.
_ENGINE = create_async_engine(URL, pool_size=4, max_overflow=4, echo=False)

# Tag DBAPI connections by id (asyncpg connections don't allow attribute
# assignment, so we keep a side table).
_CONN_TAGS: dict[int, str] = {}
_NEXT_TAG = [0]


def _tag_for(dbapi_conn) -> str:
    if dbapi_conn is None:
        return "?"
    cid = id(dbapi_conn)
    if cid not in _CONN_TAGS:
        _NEXT_TAG[0] += 1
        _CONN_TAGS[cid] = f"C{_NEXT_TAG[0]:02d}"
    return _CONN_TAGS[cid]


@event.listens_for(_ENGINE.sync_engine, "checkout")
def _on_checkout(dbapi_conn, conn_record, conn_proxy):
    tag = _tag_for(dbapi_conn)
    print(f"  [POOL  {time.time():.4f}] CHECKOUT conn={tag}", flush=True)


@event.listens_for(_ENGINE.sync_engine, "checkin")
def _on_checkin(dbapi_conn, conn_record):
    tag = _tag_for(dbapi_conn) if dbapi_conn is not None else "?"
    print(f"  [POOL  {time.time():.4f}] CHECKIN  conn={tag}", flush=True)


@event.listens_for(_ENGINE.sync_engine, "begin")
def _on_begin(conn):
    inner = getattr(conn, 'connection', None)
    dbapi = getattr(inner, 'dbapi_connection', None) if inner is not None else None
    print(f"  [TX    {time.time():.4f}] BEGIN   conn={_tag_for(dbapi)}", flush=True)


@event.listens_for(_ENGINE.sync_engine, "commit")
def _on_commit(conn):
    inner = getattr(conn, 'connection', None)
    dbapi = getattr(inner, 'dbapi_connection', None) if inner is not None else None
    print(f"  [TX    {time.time():.4f}] COMMIT  conn={_tag_for(dbapi)}", flush=True)


@event.listens_for(_ENGINE.sync_engine, "rollback")
def _on_rollback(conn):
    inner = getattr(conn, 'connection', None)
    dbapi = getattr(inner, 'dbapi_connection', None) if inner is not None else None
    print(f"  [TX    {time.time():.4f}] ROLLBACK conn={_tag_for(dbapi)}", flush=True)


@event.listens_for(_ENGINE.sync_engine, "before_cursor_execute")
def _on_exec(conn, cursor, statement, parameters, context, executemany):
    inner = getattr(conn, 'connection', None)
    dbapi = getattr(inner, 'dbapi_connection', None) if inner is not None else None
    tag = _tag_for(dbapi)
    s = " ".join(statement.split())
    print(f"  [SQL   {time.time():.4f}] {tag} {s[:160]}", flush=True)


async def _reset(sf, rc):
    async with sf() as s:
        await s.execute(delete(TaskORM))
        await s.commit()
    await rc.flushdb()


async def one_run(sf, rc, n: int):
    """One cancel+dequeue race run; prints 'OK' or 'DOUBLE-SUCCESS' at the end."""
    await _reset(sf, rc)
    q = RedisTaskQueue(rc, 3600, sf)
    t = Task(task_id='race-cancel-001', agent_id='agent-a', type='echo',
             payload={}, priority=TaskPriority.MEDIUM, created_at=datetime.now(UTC))
    await q.enqueue(t)

    cancel_result = None
    cancel_exc = None
    dequeue_result = None
    dequeue_exc = None

    async def cancel_coro():
        nonlocal cancel_result, cancel_exc
        await asyncio.sleep(0.01)
        try:
            cancel_result = await q.cancel('race-cancel-001')
        except TaskWriteConflictError as e:
            cancel_exc = e

    async def deq_coro():
        nonlocal dequeue_result, dequeue_exc
        try:
            dequeue_result = await q.dequeue('w-a', 30)
        except Exception as e:
            dequeue_exc = e

    await asyncio.gather(cancel_coro(), deq_coro())

    cancel_won = cancel_result is True
    dequeue_won = dequeue_result is not None
    if cancel_won and dequeue_won:
        # Read DB to see who really persisted
        async with sf() as s:
            from sqlalchemy import select
            row = (await s.execute(select(TaskORM).where(TaskORM.task_id == 'race-cancel-001'))).scalars().first()
            db_status = row.status if row else None
            db_version = row.version if row else None
        return f"DOUBLE-SUCCESS iter={n} db={db_status}/v{db_version}"
    if cancel_won:
        return f"OK(cancel) iter={n}"
    if dequeue_won:
        return f"OK(dequeue) iter={n}"
    return f"BOTH-LOST iter={n} cancel_exc={type(cancel_exc).__name__} dequeue_exc={type(dequeue_exc).__name__}"


async def main():
    import redis.asyncio as aioredis
    rc = aioredis.Redis.from_url(REDIS_URL)
    async with _ENGINE.begin() as conn:
        await conn.run_sync(TaskBase.metadata.create_all)
    sf = async_sessionmaker(_ENGINE, expire_on_commit=False)
    N = int(sys.argv[1]) if len(sys.argv) > 1 else 50
    print(f"=== Running {N} iterations of the cancel/dequeue race ===", flush=True)
    counts = {"OK(cancel)": 0, "OK(dequeue)": 0, "DOUBLE-SUCCESS": 0, "BOTH-LOST": 0}
    for i in range(N):
        res = await one_run(sf, rc, i)
        counts_key = res.split()[0]
        # map
        if "DOUBLE-SUCCESS" in res:
            counts["DOUBLE-SUCCESS"] += 1
        elif "OK(cancel)" in res:
            counts["OK(cancel)"] += 1
        elif "OK(dequeue)" in res:
            counts["OK(dequeue)"] += 1
        elif "BOTH-LOST" in res:
            counts["BOTH-LOST"] += 1
        if "DOUBLE-SUCCESS" in res or "BOTH-LOST" in res or i < 3:
            print(f"  iter {i}: {res}", flush=True)
    print(f"\n=== Summary over {N} runs ===", flush=True)
    for k, v in counts.items():
        print(f"  {k}: {v}", flush=True)
    await rc.aclose()
    await _ENGINE.dispose()


if __name__ == "__main__":
    asyncio.run(main())