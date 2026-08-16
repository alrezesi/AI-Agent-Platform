
# Integration tests for PostgreSQL registry (requires Docker)

import pytest
import pytest_asyncio
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

from src.agent_platform.core.agent import AgentRecord
from src.agent_platform.registry.postgres_registry import Base, PostgresAgentRegistry


@pytest_asyncio.fixture
async def registry():
    """Create a fresh PostgreSQL registry for testing."""
    pytest.importorskip("asyncpg")
    database_url = "postgresql+asyncpg://agent:agent123@localhost:5433/agent_platform"
    engine = create_async_engine(database_url)

    # Create tables
    async def _prepare():
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
            await conn.run_sync(Base.metadata.create_all)

    await _prepare()

    session_factory = async_sessionmaker(engine)
    try:
        yield PostgresAgentRegistry(session_factory)
    finally:
        await engine.dispose()


@pytest.mark.asyncio
async def test_postgres_registry(registry):
    """Test PostgreSQL registry operations."""
    agent = AgentRecord(agent_id="test", name="Test Agent")
    await registry.register(agent)

    retrieved = await registry.get_agent("test")
    assert retrieved is not None
    assert retrieved.agent_id == "test"
    assert retrieved.name == "Test Agent"

    # Test heartbeat
    result = await registry.heartbeat("test")
    assert result is True

    # Test unregister
    await registry.unregister("test")
    retrieved = await registry.get_agent("test")
    assert retrieved is None
