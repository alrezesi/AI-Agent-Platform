# Async database helpers for PostgreSQL-backed task state.

from __future__ import annotations

import os
from functools import lru_cache

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from src.agent_platform.scheduler.postgres_tasks import Base


def _database_url() -> str:
    raw = os.getenv("DATABASE_URL", "postgresql+asyncpg://user:pass@localhost:5433/agent_platform")
    if raw.startswith("postgresql://"):
        return raw.replace("postgresql://", "postgresql+asyncpg://", 1)
    return raw


@lru_cache(maxsize=1)
def get_engine():
    return create_async_engine(_database_url(), pool_pre_ping=True)


@lru_cache(maxsize=1)
def get_session_factory() -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(get_engine(), expire_on_commit=False)


async def ensure_schema() -> None:
    async with get_engine().begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
