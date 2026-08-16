
# PostgreSQL-backed registry using SQLAlchemy 2.0 async

import json
from datetime import UTC, datetime, timedelta

from sqlalchemy import delete, select, update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.types import DateTime, String

from src.agent_platform.core.agent import AgentCapability, AgentRecord, AgentStatus
from src.agent_platform.registry.base import BaseAgentRegistry


class Base(DeclarativeBase):
    pass


def utcnow_naive() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


class AgentORM(Base):
    __tablename__ = "agents"

    agent_id: Mapped[str] = mapped_column(String(64), primary_key=True)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[str | None] = mapped_column(String(500))
    capabilities: Mapped[str | None] = mapped_column(String(1000))  # JSON array
    status: Mapped[str] = mapped_column(String(20), default=AgentStatus.ACTIVE.value)
    endpoint: Mapped[str | None] = mapped_column(String(255))
    metadata_json: Mapped[str | None] = mapped_column(String(2000))  # JSON
    registered_at: Mapped[datetime] = mapped_column(DateTime, default=utcnow_naive)
    last_heartbeat: Mapped[datetime] = mapped_column(DateTime, default=utcnow_naive)
    tenant_id: Mapped[str | None] = mapped_column(String(64), index=True)

    def to_record(self) -> AgentRecord:
        caps = json.loads(self.capabilities) if self.capabilities else []
        meta = json.loads(self.metadata_json) if self.metadata_json else {}
        return AgentRecord(
            agent_id=self.agent_id,
            name=self.name,
            description=self.description,
            capabilities=[AgentCapability(**c) for c in caps],
            status=AgentStatus(self.status),
            endpoint=self.endpoint,
            metadata=meta,
            registered_at=self.registered_at,
            last_heartbeat=self.last_heartbeat,
            tenant_id=self.tenant_id,
        )


class PostgresAgentRegistry(BaseAgentRegistry):
    def __init__(self, session_factory: async_sessionmaker[AsyncSession]):
        self.session_factory = session_factory

    async def register(self, agent: AgentRecord) -> None:
        async with self.session_factory() as session:
            # Check if exists
            stmt = select(AgentORM).where(AgentORM.agent_id == agent.agent_id)
            result = await session.execute(stmt)
            existing = result.scalar_one_or_none()

            if existing:
                # Update
                existing.name = agent.name
                existing.description = agent.description
                existing.capabilities = json.dumps([c.model_dump() for c in agent.capabilities])
                existing.status = agent.status.value
                existing.endpoint = agent.endpoint
                existing.metadata_json = json.dumps(agent.metadata)
                existing.last_heartbeat = utcnow_naive()
                existing.tenant_id = agent.tenant_id
            else:
                # Insert
                orm = AgentORM(
                    agent_id=agent.agent_id,
                    name=agent.name,
                    description=agent.description,
                    capabilities=json.dumps([c.model_dump() for c in agent.capabilities]),
                    status=agent.status.value,
                    endpoint=agent.endpoint,
                    metadata_json=json.dumps(agent.metadata),
                    registered_at=utcnow_naive(),
                    last_heartbeat=utcnow_naive(),
                    tenant_id=agent.tenant_id,
                )
                session.add(orm)

            await session.commit()

    async def unregister(self, agent_id: str, tenant_id: str | None = None) -> bool:
        async with self.session_factory() as session:
            stmt = delete(AgentORM).where(AgentORM.agent_id == agent_id)
            if tenant_id:
                stmt = stmt.where(AgentORM.tenant_id == tenant_id)
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount > 0

    async def get_agent(self, agent_id: str, tenant_id: str | None = None) -> AgentRecord | None:
        async with self.session_factory() as session:
            stmt = select(AgentORM).where(AgentORM.agent_id == agent_id)
            if tenant_id:
                stmt = stmt.where(AgentORM.tenant_id == tenant_id)
            result = await session.execute(stmt)
            orm = result.scalar_one_or_none()
            if not orm:
                return None
            return orm.to_record()

    async def heartbeat(self, agent_id: str, tenant_id: str | None = None) -> bool:
        async with self.session_factory() as session:
            stmt = (
                update(AgentORM)
                .where(AgentORM.agent_id == agent_id)
                .values(last_heartbeat=utcnow_naive())
            )
            if tenant_id:
                stmt = stmt.where(AgentORM.tenant_id == tenant_id)
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount > 0

    async def discover(
        self,
        capability: str | None = None,
        status: AgentStatus | None = None,
        tenant_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[AgentRecord]:
        async with self.session_factory() as session:
            stmt = select(AgentORM)
            if tenant_id:
                stmt = stmt.where(AgentORM.tenant_id == tenant_id)
            if status:
                stmt = stmt.where(AgentORM.status == status.value)
            if capability:
                # Filter for capability in JSON array (PostgreSQL specific)
                # Using JSON_CONTAINS or ->> operator, but let's keep simple:
                # We'll fetch and filter in Python, or use PostgreSQL jsonb.
                # Since we stored as JSON string, we can use func.json_contains.
                # For portability, let's do a simple Python-side filter after fetch.
                # But for performance, we could use PostgreSQL JSON queries.
                # I'll implement a simpler version: fetch all and filter in Python.
                pass  # We'll filter later

            stmt = stmt.offset(offset).limit(limit)
            result = await session.execute(stmt)
            orms = result.scalars().all()

            records = [o.to_record() for o in orms]

            # Apply capability filter in Python (if provided)
            if capability:
                records = [
                    r for r in records
                    if any(cap.name == capability for cap in r.capabilities)
                ]

            return records

    async def list_all(
        self, tenant_id: str | None = None, limit: int = 100, offset: int = 0
    ) -> list[AgentRecord]:
        return await self.discover(tenant_id=tenant_id, limit=limit, offset=offset)

    async def cleanup_stale(self, ttl_seconds: int = 60) -> int:
        async with self.session_factory() as session:
            cutoff = utcnow_naive() - timedelta(seconds=ttl_seconds)
            stmt = delete(AgentORM).where(AgentORM.last_heartbeat < cutoff)
            result = await session.execute(stmt)
            await session.commit()
            return result.rowcount
