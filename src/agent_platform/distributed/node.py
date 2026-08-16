
# Node representation and management for distributed execution

import logging
import socket
import uuid
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

logger = logging.getLogger(__name__)


class NodeStatus(StrEnum):
    """Status of a node in the cluster."""
    INITIALIZING = "initializing"
    ACTIVE = "active"
    PAUSED = "paused"
    DEGRADED = "degraded"
    OFFLINE = "offline"
    UNHEALTHY = "unhealthy"


@dataclass
class NodeInfo:
    """
    Information about a node in the distributed system.
    """
    node_id: str
    hostname: str
    ip_address: str
    port: int
    status: NodeStatus = NodeStatus.INITIALIZING
    capabilities: dict[str, Any] = field(default_factory=dict)
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    last_heartbeat: datetime = field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] = field(default_factory=dict)

    @classmethod
    def create(cls, port: int, capabilities: dict[str, Any] | None = None) -> 'NodeInfo':
        """Create a NodeInfo for the current node."""
        hostname = socket.gethostname()
        try:
            ip_address = socket.gethostbyname(hostname)
        except socket.gaierror:
            ip_address = "127.0.0.1"
        return cls(
            node_id=f"node-{uuid.uuid4().hex[:8]}",
            hostname=hostname,
            ip_address=ip_address,
            port=port,
            capabilities=capabilities or {},
        )


class Node:
    """
    Represents a node in the distributed system.
    Manages node lifecycle, heartbeats, and health checks.
    """

    def __init__(self, info: NodeInfo):
        self.info = info
        self._running = False

    async def start(self) -> None:
        """Start the node."""
        self._running = True
        self.info.status = NodeStatus.ACTIVE
        self.info.last_heartbeat = datetime.now(UTC)
        logger.info(f"Node {self.info.node_id} started on {self.info.hostname}:{self.info.port}")

    async def stop(self) -> None:
        """Stop the node."""
        self._running = False
        self.info.status = NodeStatus.OFFLINE
        logger.info(f"Node {self.info.node_id} stopped")

    async def heartbeat(self) -> None:
        """Update the heartbeat timestamp."""
        if self._running:
            self.info.last_heartbeat = datetime.now(UTC)

    async def health_check(self) -> bool:
        """
        Perform a health check on the node.
        Returns True if healthy.
        """
        # Basic check: node is running and has recent heartbeat
        if not self._running:
            return False
        # Check if heartbeat is too old (more than 30 seconds)
        age = (datetime.now(UTC) - self.info.last_heartbeat).total_seconds()
        if age > 30:
            self.info.status = NodeStatus.UNHEALTHY
            return False
        self.info.status = NodeStatus.ACTIVE
        return True

    def to_dict(self) -> dict[str, Any]:
        """Serialize node info to dictionary."""
        return {
            "node_id": self.info.node_id,
            "hostname": self.info.hostname,
            "ip_address": self.info.ip_address,
            "port": self.info.port,
            "status": self.info.status.value,
            "capabilities": self.info.capabilities,
            "started_at": self.info.started_at.isoformat(),
            "last_heartbeat": self.info.last_heartbeat.isoformat(),
            "metadata": self.info.metadata,
        }

    @property
    def is_active(self) -> bool:
        return self._running and self.info.status == NodeStatus.ACTIVE
