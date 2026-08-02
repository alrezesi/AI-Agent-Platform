
# In-memory registry implementation using a thread-safe dict

import asyncio
from typing import List, Optional, Dict
from datetime import datetime, timedelta

from src.agent_platform.core.agent import AgentRecord, AgentStatus
from src.agent_platform.registry.base import BaseAgentRegistry


class InMemoryAgentRegistry(BaseAgentRegistry):
    """
    Simple in-memory registry using a dict.
    Not persistent, suitable for unit tests and single-node development.
    """

    def __init__(self):
        # agent_id -> AgentRecord
        self._agents: Dict[str, AgentRecord] = {}
        self._lock = asyncio.Lock()

    async def register(self, agent: AgentRecord) -> None:
        async with self._lock:
            agent.last_heartbeat = datetime.utcnow()
            self._agents[agent.agent_id] = agent

    async def unregister(self, agent_id: str, tenant_id: Optional[str] = None) -> bool:
        async with self._lock:
            if agent_id in self._agents:
                # Optional tenant check (if provided)
                if tenant_id and self._agents[agent_id].tenant_id != tenant_id:
                    return False
                del self._agents[agent_id]
                return True
            return False

    async def get_agent(self, agent_id: str, tenant_id: Optional[str] = None) -> Optional[AgentRecord]:
        async with self._lock:
            agent = self._agents.get(agent_id)
            if agent and tenant_id and agent.tenant_id != tenant_id:
                return None
            return agent

    async def heartbeat(self, agent_id: str, tenant_id: Optional[str] = None) -> bool:
        async with self._lock:
            agent = self._agents.get(agent_id)
            if not agent:
                return False
            if tenant_id and agent.tenant_id != tenant_id:
                return False
            agent.last_heartbeat = datetime.utcnow()
            return True

    async def discover(
        self,
        capability: Optional[str] = None,
        status: Optional[AgentStatus] = None,
        tenant_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[AgentRecord]:
        async with self._lock:
            results = []
            for agent in self._agents.values():
                # Apply filters
                if tenant_id and agent.tenant_id != tenant_id:
                    continue
                if status and agent.status != status:
                    continue
                if capability:
                    # Check if any capability matches by name
                    has_cap = any(cap.name == capability for cap in agent.capabilities)
                    if not has_cap:
                        continue
                results.append(agent)

            # Apply pagination
            return results[offset : offset + limit]

    async def list_all(
        self, tenant_id: Optional[str] = None, limit: int = 100, offset: int = 0
    ) -> List[AgentRecord]:
        async with self._lock:
            agents = list(self._agents.values())
            if tenant_id:
                agents = [a for a in agents if a.tenant_id == tenant_id]
            return agents[offset : offset + limit]

    async def cleanup_stale(self, ttl_seconds: int = 60) -> int:
        async with self._lock:
            now = datetime.utcnow()
            stale_ids = []
            for agent_id, agent in self._agents.items():
                if (now - agent.last_heartbeat).total_seconds() > ttl_seconds:
                    stale_ids.append(agent_id)

            for agent_id in stale_ids:
                del self._agents[agent_id]

            return len(stale_ids)