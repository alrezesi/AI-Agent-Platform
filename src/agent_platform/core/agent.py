# src/agent_platform/core/agent.py
# Core agent model definitions and abstract base class

from abc import ABC, abstractmethod
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class AgentStatus(StrEnum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    PAUSED = "paused"
    ERROR = "error"


class AgentCapability(BaseModel):
    name: str
    description: str | None = None
    parameters_schema: dict[str, Any] | None = None


class AgentRecord(BaseModel):
    agent_id: str = Field(..., description="Unique identifier for the agent")
    name: str = Field(..., description="Human-readable name")
    description: str | None = None
    capabilities: list[AgentCapability] = Field(default_factory=list)
    status: AgentStatus = AgentStatus.ACTIVE
    endpoint: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    registered_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    last_heartbeat: datetime = Field(default_factory=lambda: datetime.now(UTC))
    tenant_id: str | None = None


# ---- NEW: Agent State Enum for Engine ----
class AgentRuntimeState(StrEnum):
    """Runtime state of an agent within the engine."""
    IDLE = "idle"          # Loaded but not processing
    RUNNING = "running"    # Actively processing tasks
    PAUSED = "paused"      # Temporarily suspended
    STOPPED = "stopped"    # Stopped, needs re-initialization
    ERROR = "error"        # Encountered an unrecoverable error


# ---- NEW: Abstract Base Agent ----
class BaseAgent(ABC):
    """
    Abstract base class for all agents in the platform.
    Defines the lifecycle and task processing contract.
    """

    def __init__(self, agent_id: str, name: str, tenant_id: str | None = None):
        self.agent_id = agent_id
        self.name = name
        self.tenant_id = tenant_id
        self.state = AgentRuntimeState.IDLE
        self.context: Any = None  # Will be set during initialization
        self._task_queue: Any = None
        self._initialized = False

    @abstractmethod
    async def initialize(self) -> None:
        """
        Initialize the agent. Load models, connect to services, set up state.
        Called once during agent registration.
        """
        pass

    @abstractmethod
    async def run(self, task: Any) -> Any:
        """
        Execute a given task. This is the main processing method.
        'task' will be the Task object from the scheduler.
        """
        pass

    @abstractmethod
    async def shutdown(self) -> None:
        """
        Gracefully shutdown the agent. Release resources, save state.
        Called when agent is removed or engine stops.
        """
        pass

    async def pause(self) -> None:
        """Pause the agent. It will not pick up new tasks."""
        self.state = AgentRuntimeState.PAUSED

    async def resume(self) -> None:
        """Resume the agent from a paused state."""
        if self.state == AgentRuntimeState.PAUSED:
            self.state = AgentRuntimeState.RUNNING

    async def stop(self) -> None:
        """Stop the agent completely."""
        await self.shutdown()
        self.state = AgentRuntimeState.STOPPED
        self._initialized = False

    def is_ready(self) -> bool:
        """Check if the agent is ready to process tasks."""
        return self._initialized and self.state == AgentRuntimeState.RUNNING
