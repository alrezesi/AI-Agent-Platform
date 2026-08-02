# src/agent_platform/core/agent.py
# Core agent model definitions and abstract base class

from enum import Enum
from typing import List, Optional, Dict, Any
from pydantic import BaseModel, Field
from datetime import datetime
from abc import ABC, abstractmethod



class AgentStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    PAUSED = "paused"
    ERROR = "error"


class AgentCapability(BaseModel):
    name: str
    description: Optional[str] = None
    parameters_schema: Optional[Dict[str, Any]] = None


class AgentRecord(BaseModel):
    agent_id: str = Field(..., description="Unique identifier for the agent")
    name: str = Field(..., description="Human-readable name")
    description: Optional[str] = None
    capabilities: List[AgentCapability] = Field(default_factory=list)
    status: AgentStatus = AgentStatus.ACTIVE
    endpoint: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    registered_at: datetime = Field(default_factory=datetime.utcnow)
    last_heartbeat: datetime = Field(default_factory=datetime.utcnow)
    tenant_id: Optional[str] = None


# ---- NEW: Agent State Enum for Engine ----
class AgentRuntimeState(str, Enum):
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

    def __init__(self, agent_id: str, name: str, tenant_id: Optional[str] = None):
        self.agent_id = agent_id
        self.name = name
        self.tenant_id = tenant_id
        self.state = AgentRuntimeState.IDLE
        self.context = None  # Will be set during initialization
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