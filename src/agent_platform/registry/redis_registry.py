
# Redis-backed registry using async Redis client with TTL for heartbeat

import json
from typing import Any, List, Optional, TYPE_CHECKING
from datetime import datetime, timezone

try:
    from redis.asyncio import Redis
except ImportError:  # pragma: no cover - optional dependency
    Redis = Any

if TYPE_CHECKING:
    from redis.asyncio import Redis as RedisClient
else:
    RedisClient = Any

from src.agent_platform.core.agent import AgentRecord, AgentStatus
from src.agent_platform.registry.base import BaseAgentRegistry


class RedisAgentRegistry(BaseAgentRegistry):
    """
    Redis-backed registry.
    Each agent is stored as a JSON string under key 'agent:{agent_id}'.
    TTL is set to ttl_seconds on register and refreshed on heartbeat.
    """

    def __init__(self, redis_client: RedisClient, ttl_seconds: int = 60):
        self.redis = redis_client
        self.ttl_seconds = ttl_seconds

    def _key(self, agent_id: str) -> str:
        return f"agent:{agent_id}"

    async def register(self, agent: AgentRecord) -> None:
        agent.last_heartbeat = datetime.now(timezone.utc)
        data = agent.model_dump_json()
        key = self._key(agent.agent_id)
        await self.redis.setex(key, self.ttl_seconds, data)

    async def unregister(self, agent_id: str, tenant_id: Optional[str] = None) -> bool:
        # Optional tenant check: we need to fetch first
        if tenant_id:
            agent = await self.get_agent(agent_id, tenant_id)
            if not agent:
                return False
        key = self._key(agent_id)
        deleted = await self.redis.delete(key)
        return deleted > 0

    async def get_agent(self, agent_id: str, tenant_id: Optional[str] = None) -> Optional[AgentRecord]:
        key = self._key(agent_id)
        data = await self.redis.get(key)
        if not data:
            return None
        agent = AgentRecord.model_validate_json(data)
        if tenant_id and agent.tenant_id != tenant_id:
            return None
        return agent

    async def heartbeat(self, agent_id: str, tenant_id: Optional[str] = None) -> bool:
        key = self._key(agent_id)
        # Check existence
        exists = await self.redis.exists(key)
        if not exists:
            return False
        # Refresh TTL and update last_heartbeat in stored data
        data = await self.redis.get(key)
        if not data:
            return False
        agent = AgentRecord.model_validate_json(data)
        if tenant_id and agent.tenant_id != tenant_id:
            return False
        agent.last_heartbeat = datetime.now(timezone.utc)
        await self.redis.setex(key, self.ttl_seconds, agent.model_dump_json())
        return True

    async def discover(
        self,
        capability: Optional[str] = None,
        status: Optional[AgentStatus] = None,
        tenant_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[AgentRecord]:
        # Scan all keys (not optimal for huge datasets, but works for now)
        # In production, consider using Redisearch or maintain separate sets.
        cursor = 0
        keys = []
        while True:
            cursor, batch = await self.redis.scan(cursor, match="agent:*", count=100)
            keys.extend(batch)
            if cursor == 0:
                break

        results = []
        for key in keys:
            data = await self.redis.get(key)
            if not data:
                continue
            agent = AgentRecord.model_validate_json(data)

            # Apply filters
            if tenant_id and agent.tenant_id != tenant_id:
                continue
            if status and agent.status != status:
                continue
            if capability:
                has_cap = any(cap.name == capability for cap in agent.capabilities)
                if not has_cap:
                    continue
            results.append(agent)

        return results[offset : offset + limit]

    async def list_all(
        self, tenant_id: Optional[str] = None, limit: int = 100, offset: int = 0
    ) -> List[AgentRecord]:
        # Same as discover without filters, but we use scan
        return await self.discover(tenant_id=tenant_id, limit=limit, offset=offset)

    async def cleanup_stale(self, ttl_seconds: int = 60) -> int:
        # Redis automatically removes expired keys via TTL.
        # This method is a no-op for Redis, but we implement it to satisfy the ABC.
        # We can optionally scan and delete manually, but TTL is preferred.
        return 0
