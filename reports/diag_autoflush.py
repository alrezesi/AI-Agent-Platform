"""Minimal reproduction of autoflush UPDATE + Core UPDATE interaction."""
import asyncio
import sys
sys.path.insert(0, '.')

from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy import update

from src.agent_platform.scheduler.postgres_tasks import Base as TaskBase, TaskORM

URL = 'postgresql+asyncpg://agent:agent123@localhost:5433/agent_platform_test'

async def main():
    engine = create_async_engine(URL, pool_size=2, max_overflow=2, echo=True)
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
        print(f"Inserted row, version=0", flush=True)

    async with sf() as s:
        orm = await s.get(TaskORM, 'x')
        print(f"Loaded orm: version={orm.version} status={orm.status}", flush=True)
        # Mutate
        orm.status = 'running'
        # Core update
        stmt = update(TaskORM).where(TaskORM.task_id == 'x', TaskORM.version == 0).values(status='running', version=1).returning(TaskORM.task_id)
        r = await s.execute(stmt)
        m = r.scalar_one_or_none()
        print(f"Core UPDATE matched: {m}", flush=True)
        await s.rollback()
        # Check actual DB
        from sqlalchemy import select
        r = (await s.execute(select(TaskORM).where(TaskORM.task_id == 'x'))).scalars().first()
        print(f"After rollback, DB row: status={r.status} version={r.version}", flush=True)

    await engine.dispose()

asyncio.run(main())