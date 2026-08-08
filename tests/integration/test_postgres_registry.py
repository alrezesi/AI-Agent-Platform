
# Requires PostgreSQL running with docker-compose

import pytest
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from src.agent_platform.registry.postgres_registry import PostgresAgentRegistry, Base
from src.agent_platform.core.agent import AgentRecord

@pytest.fixture
async def registry():
    engine = create_async_engine("postgresql+asyncpg://user:pass@localhost:5432/agent_platform")
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)
    session_factory = async_sessionmaker(engine)
    return PostgresAgentRegistry(session_factory)

@pytest.mark.asyncio
async def test_postgres_registry(registry):
    agent = AgentRecord(agent_id="test", name="Test")
    await registry.register(agent)
    retrieved = await registry.get_agent("test")
    assert retrieved is not None