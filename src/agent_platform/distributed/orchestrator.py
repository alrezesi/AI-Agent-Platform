
# Distributed orchestrator that coordinates multiple nodes

import asyncio
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime

from .node import NodeInfo, NodeStatus
from .registry import DistributedRegistry
from .queue import DistributedTaskQueue
from .lock import DistributedLock
from .worker import WorkerNode, WorkerConfig
from ..registry.in_memory import InMemoryAgentRegistry

logger = logging.getLogger(__name__)


class DistributedOrchestrator:
    """
    Orchestrates distributed execution across multiple worker nodes.
    Manages node registration, task distribution, and cluster health.
    """

    def __init__(
        self,
        registry: DistributedRegistry,
        queue: DistributedTaskQueue,
        redis_client,
    ):
        self.registry = registry
        self.queue = queue
        self.redis = redis_client
        self._nodes: Dict[str, WorkerNode] = {}
        self._running = False
        self._health_check_task: Optional[asyncio.Task] = None

    async def start(self) -> None:
        """Start the orchestrator."""
        self._running = True
        self._health_check_task = asyncio.create_task(self._health_check_loop())
        logger.info("DistributedOrchestrator started")

    async def stop(self) -> None:
        """Stop the orchestrator and all nodes."""
        self._running = False
        if self._health_check_task:
            self._health_check_task.cancel()

        # Stop all nodes
        for node_id, node in list(self._nodes.items()):
            await node.stop()
            del self._nodes[node_id]

        logger.info("DistributedOrchestrator stopped")

    async def add_node(
        self,
        node_info: NodeInfo,
        config: Optional[WorkerConfig] = None,
        agent_registry: Optional[Any] = None,
    ) -> str:
        """
        Add a worker node to the cluster.
        """
        if agent_registry is None:
            # Use a local agent registry for this node
            agent_registry = InMemoryAgentRegistry()

        node = WorkerNode(node_info, self.queue, agent_registry, config)
        self._nodes[node_info.node_id] = node
        await node.start()

        # Register node in distributed registry
        await self.registry.register_node(node_info)

        logger.info(f"Node {node_info.node_id} added to cluster")
        return node_info.node_id

    async def remove_node(self, node_id: str) -> bool:
        """
        Remove a worker node from the cluster.
        """
        if node_id not in self._nodes:
            return False

        node = self._nodes[node_id]
        await node.stop()
        del self._nodes[node_id]

        # Unregister from distributed registry
        await self.redis.delete(f"dist:node:{node_id}")
        await self.redis.srem("dist:nodes", node_id)

        logger.info(f"Node {node_id} removed from cluster")
        return True

    async def get_active_nodes(self) -> List[Dict[str, Any]]:
        """Get all active nodes."""
        return await self.registry.get_active_nodes()

    async def get_node_count(self) -> int:
        """Get the number of active nodes."""
        return len(await self.get_active_nodes())

    async def _health_check_loop(self) -> None:
        """
        Periodically check node health and clean up stale nodes.
        """
        while self._running:
            try:
                # Get all registered nodes
                nodes = await self.registry.list_nodes()
                for node_data in nodes:
                    node_id = node_data.get('node_id')
                    last_heartbeat_str = node_data.get('last_heartbeat')
                    if last_heartbeat_str:
                        last_heartbeat = datetime.fromisoformat(last_heartbeat_str)
                        age = (datetime.utcnow() - last_heartbeat).total_seconds()
                        if age > 60:  # More than 60 seconds
                            logger.warning(f"Node {node_id} has stale heartbeat, marking as offline")
                            node_data['status'] = NodeStatus.OFFLINE.value
                            await self.registry.update_node_status(
                                NodeInfo(
                                    node_id=node_id,
                                    hostname=node_data.get('hostname', ''),
                                    ip_address=node_data.get('ip_address', ''),
                                    port=node_data.get('port', 0),
                                    status=NodeStatus.OFFLINE,
                                )
                            )

                await asyncio.sleep(30)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in health check loop: {e}")
                await asyncio.sleep(10)

    async def acquire_global_lock(self, lock_name: str, ttl_seconds: int = 30) -> DistributedLock:
        """
        Acquire a global distributed lock.
        """
        return DistributedLock(self.redis, lock_name, ttl_seconds)