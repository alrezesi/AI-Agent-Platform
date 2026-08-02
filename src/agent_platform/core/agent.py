
# Core agent model definitions

from __future__ import annotations

from enum import Enum
from datetime import datetime
from typing import Any
from pydantic import BaseModel, Field


class AgentStatus(str, Enum):
    ACTIVE = "active"
    INACTIVE = "inactive"
    PAUSED = "paused"
    ERROR = "error"


class AgentCapability(BaseModel):
    name: str
    description: str | None = None  # جایگزین Optional
    parameters_schema: dict[str, Any] | None = None  # جایگزین Optional[Dict]


class AgentRecord(BaseModel):
    agent_id: str = Field(..., description="Unique identifier for the agent")
    name: str = Field(..., description="Human-readable name")
    description: str | None = None
    capabilities: list[AgentCapability] = Field(default_factory=list)  # لیست
    status: AgentStatus = AgentStatus.ACTIVE
    endpoint: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    registered_at: datetime = Field(default_factory=datetime.utcnow)
    last_heartbeat: datetime = Field(default_factory=datetime.utcnow)
    tenant_id: str | None = None

