
# Integration tests for PostgreSQL registry (requires Docker)

import pytest
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from src.agent_platform.registry.postgres_registry import PostgresAgentRegistry, Base
from src.agent_platform.core.agent import AgentRecord


@pytest.fixture
async def registry():
    """Create a fresh PostgreSQL registry for testing."""
    # Use the same credentials as in docker-compose.yml
    DATABASE_URL = "postgresql+asyncpg://user:pass@localhost:5433/agent_platform"
    engine = create_async_engine(DATABASE_URL)

    # Create tables
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.drop_all)
        await conn.run_sync(Base.metadata.create_all)

    session_factory = async_sessionmaker(engine)
    return PostgresAgentRegistry(session_factory)


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
