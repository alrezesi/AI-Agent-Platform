"""Two concurrent sessions racing on autoflush UPDATE + Core UPDATE."""
import asyncio
import sys
import time
sys.path.insert(0, '.')

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy import update

from src.agent_platform.scheduler.postgres_tasks import Base as TaskBase, TaskORM

URL = 'postgresql+asyncpg://agent:agent123@localhost:5433/agent_platform_test'

async def main():
    engine = create_async_engine(URL, pool_size=4, max_overflow=4)
    async with engine.begin() as conn:
        await conn.run_sync(TaskBase.metadata.create_all)
    sf = async_sessionmaker(engine, expire_on_commit=False)

    async with sf() as s:
        from sqlalchemy import delete
        await s.execute(delete(TaskORM))
        await s.commit()

    async with sf() as s:
        orm = TaskORM(task_id='x', agent_id='a', task_type='t', payload={}, priority=2,
                      status='pending', version=0)
        s.add(orm)
        await s.commit()

    async def worker(name, new_status, sleep=0.0):
        async with sf() as s:
            orm = await s.get(TaskORM, 'x')
            print(f"[{name} t={time.time():.3f}] loaded version={orm.version} status={orm.status}", flush=True)
            await asyncio.sleep(sleep)
            orm.status = new_status
            print(f"[{name} t={time.time():.3f}] mutated to status={new_status}", flush=True)
            stmt = update(TaskORM).where(TaskORM.task_id == 'x', TaskORM.version == 0).values(status=new_status, version=1).returning(TaskORM.task_id)
            r = await s.execute(stmt)
            m = r.scalar_one_or_none()
            print(f"[{name} t={time.time():.3f}] core UPDATE matched={m}", flush=True)
            if m:
                await s.commit()
                print(f"[{name} t={time.time():.3f}] COMMITTED", flush=True)
            else:
                await s.rollback()
                print(f"[{name} t={time.time():.3f}] ROLLED BACK", flush=True)

    await asyncio.gather(worker("A", "running", sleep=0), worker("B", "cancelled", sleep=0.005))

    async with sf() as s:
        from sqlalchemy import select
        r = (await s.execute(select(TaskORM).where(TaskORM.task_id == 'x'))).scalars().first()
        print(f"\nFINAL: status={r.status} version={r.version}", flush=True)

    await engine.dispose()

asyncio.run(main())