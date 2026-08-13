
# Distributed Agent Registry using Redis or PostgreSQL

import json
import logging
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone

from src.agent_platform.registry.base import BaseAgentRegistry
from src.agent_platform.core.agent import AgentRecord, AgentStatus
from src.agent_platform.distributed.node import NodeInfo, NodeStatus

logger = logging.getLogger(__name__)


class DistributedRegistry(BaseAgentRegistry):
    """
    Distributed registry that stores agent and node information in Redis.
    Supports multi-node discovery and node health monitoring.
    """

    def __init__(self, redis_client, ttl_seconds: int = 60):
        self.redis = redis_client
        self.ttl_seconds = ttl_seconds

    def _agent_key(self, agent_id: str) -> str:
        return f"dist:agent:{agent_id}"

    def _node_key(self, node_id: str) -> str:
        return f"dist:node:{node_id}"

    def _node_set_key(self) -> str:
        return "dist:nodes"

    async def register(self, agent: AgentRecord) -> None:
        """Register an agent in the distributed registry."""
        agent.last_heartbeat = datetime.now(timezone.utc)
        key = self._agent_key(agent.agent_id)
        await self.redis.setex(
            key,
            self.ttl_seconds,
            agent.model_dump_json()
        )

    async def unregister(self, agent_id: str, tenant_id: Optional[str] = None) -> bool:
        """Unregister an agent."""
        if tenant_id:
            agent = await self.get_agent(agent_id, tenant_id)
            if not agent:
                return False
        key = self._agent_key(agent_id)
        deleted = await self.redis.delete(key)
        return deleted > 0

    async def get_agent(self, agent_id: str, tenant_id: Optional[str] = None) -> Optional[AgentRecord]:
        """Get an agent by ID."""
        key = self._agent_key(agent_id)
        data = await self.redis.get(key)
        if not data:
            return None
        agent = AgentRecord.model_validate_json(data)
        if tenant_id and agent.tenant_id != tenant_id:
            return None
        return agent

    async def heartbeat(self, agent_id: str, tenant_id: Optional[str] = None) -> bool:
        """Update agent heartbeat."""
        key = self._agent_key(agent_id)
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
        """Discover agents matching filters."""
        cursor = 0
        keys = []
        while True:
            cursor, batch = await self.redis.scan(cursor, match="dist:agent:*", count=100)
            keys.extend(batch)
            if cursor == 0:
                break

        results = []
        for key in keys:
            data = await self.redis.get(key)
            if not data:
                continue
            agent = AgentRecord.model_validate_json(data)
            if tenant_id and agent.tenant_id != tenant_id:
                continue
            if status and agent.status != status:
                continue
            if capability:
                has_cap = any(cap.name == capability for cap in agent.capabilities)
                if not has_cap:
                    continue
            results.append(agent)

        return results[offset:offset + limit]

    async def list_all(
        self,
        tenant_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[AgentRecord]:
        """List all active agents."""
        return await self.discover(tenant_id=tenant_id, limit=limit, offset=offset)

    async def cleanup_stale(self, ttl_seconds: int = 60) -> int:
        """Clean up stale agents. Redis handles TTL automatically."""
        return 0

    async def update_task(self, task) -> None:
        """Update a task in the distributed system."""
        # Task updates are handled by the queue
        pass

    # --- Node Management Methods ---

    async def register_node(self, node_info: NodeInfo) -> None:
        """Register a node in the distributed cluster."""
        key = self._node_key(node_info.node_id)
        await self.redis.setex(
            key,
            self.ttl_seconds,
            json.dumps({
                "node_id": node_info.node_id,
                "hostname": node_info.hostname,
                "ip_address": node_info.ip_address,
                "port": node_info.port,
                "status": node_info.status.value,
                "capabilities": node_info.capabilities,
                "started_at": node_info.started_at.isoformat(),
                "last_heartbeat": node_info.last_heartbeat.isoformat(),
                "metadata": node_info.metadata,
            })
        )
        # Add to node set
        await self.redis.sadd(self._node_set_key(), node_info.node_id)

    async def update_node_status(self, node_info: NodeInfo) -> None:
        """Update a node's status and heartbeat."""
        key = self._node_key(node_info.node_id)
        # Get existing data
        data = await self.redis.get(key)
        if data:
            node_data = json.loads(data)
            node_data["status"] = node_info.status.value
            node_data["last_heartbeat"] = node_info.last_heartbeat.isoformat()
            await self.redis.setex(key, self.ttl_seconds, json.dumps(node_data))

    async def get_node(self, node_id: str) -> Optional[Dict[str, Any]]:
        """Get node information by ID."""
        key = self._node_key(node_id)
        data = await self.redis.get(key)
        if data:
            return json.loads(data)
        return None

    async def list_nodes(self) -> List[Dict[str, Any]]:
        """List all registered nodes."""
        node_ids = await self.redis.smembers(self._node_set_key())
        nodes = []
        for node_id in node_ids:
            node = await self.get_node(node_id.decode() if isinstance(node_id, bytes) else node_id)
            if node:
                nodes.append(node)
        return nodes

    async def get_active_nodes(self) -> List[Dict[str, Any]]:
        """Get all active nodes (with recent heartbeat)."""
        nodes = await self.list_nodes()
        active = []
        for node in nodes:
            last_heartbeat = datetime.fromisoformat(node.get("last_heartbeat", ""))
            age = (datetime.now(timezone.utc) - last_heartbeat).total_seconds()
            if age < self.ttl_seconds:
                active.append(node)
        return active
