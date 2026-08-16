
# Abstract base class for Agent Registry implementations

from abc import ABC, abstractmethod

from src.agent_platform.core.agent import AgentRecord, AgentStatus


class BaseAgentRegistry(ABC):
    """
    Abstract interface for agent registration and discovery.
    All registry implementations (InMemory, Redis, PostgreSQL) must inherit this.
    """

    @abstractmethod
    async def register(self, agent: AgentRecord) -> None:
        """
        Register a new agent or update an existing one.
        If agent already exists, it will be updated.
        """
        pass

    @abstractmethod
    async def unregister(self, agent_id: str, tenant_id: str | None = None) -> bool:
        """
        Remove an agent from the registry.
        Returns True if removed, False if not found.
        """
        pass

    @abstractmethod
    async def get_agent(self, agent_id: str, tenant_id: str | None = None) -> AgentRecord | None:
        """
        Retrieve a single agent by its ID.
        Returns None if not found or expired.
        """
        pass

    @abstractmethod
    async def heartbeat(self, agent_id: str, tenant_id: str | None = None) -> bool:
        """
        Update the last_heartbeat timestamp for an agent.
        Returns True if agent exists and heartbeat was updated.
        """
        pass

    @abstractmethod
    async def discover(
        self,
        capability: str | None = None,
        status: AgentStatus | None = None,
        tenant_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[AgentRecord]:
        """
        Discover agents matching the given filters.
        - capability: filter by capability name (exact match)
        - status: filter by agent status
        - tenant_id: multi-tenant isolation
        - limit, offset: pagination support
        """
        pass

    @abstractmethod
    async def list_all(
        self, tenant_id: str | None = None, limit: int = 100, offset: int = 0
    ) -> list[AgentRecord]:
        """
        List all active agents (non-expired) with pagination.
        """
        pass

    @abstractmethod
    async def cleanup_stale(self, ttl_seconds: int = 60) -> int:
        """
        Remove agents whose last_heartbeat is older than ttl_seconds.
        Returns the number of removed agents.
        """
        pass
