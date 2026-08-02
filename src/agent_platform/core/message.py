
# Message models for inter-agent communication

from __future__ import annotations

from enum import Enum
from datetime import datetime
from typing import Any
from pydantic import BaseModel, Field


class MessageType(str, Enum):
    REQUEST = "request"
    RESPONSE = "response"
    EVENT = "event"
    COMMAND = "command"
    HEARTBEAT = "heartbeat"


class Message(BaseModel):
    message_id: str = Field(..., description="Unique message ID")
    from_agent: str = Field(..., description="Sender agent ID")
    to_agent: str | None = None  # None means broadcast
    type: MessageType
    content: dict[str, Any] = Field(default_factory=dict)
    correlation_id: str | None = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    ttl_seconds: int | None = None
    tenant_id: str | None = None

