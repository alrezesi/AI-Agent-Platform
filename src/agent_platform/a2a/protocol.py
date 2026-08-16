
# Handover protocol models for A2A communication

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class HandoverStatus(StrEnum):
    """Status of a handover request."""
    PENDING = "pending"
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    COMPLETED = "completed"
    FAILED = "failed"


class A2AMessageType(StrEnum):
    """Types of A2A messages."""
    HANDOVER_REQUEST = "handover_request"
    HANDOVER_RESPONSE = "handover_response"
    CONTEXT_SHARE = "context_share"
    DELEGATION_REQUEST = "delegation_request"
    DELEGATION_RESPONSE = "delegation_response"
    STATUS_UPDATE = "status_update"
    QUERY = "query"
    QUERY_RESPONSE = "query_response"


class HandoverRequest(BaseModel):
    """
    A handover request from one agent to another.
    Used to transfer a task or conversation.
    """
    request_id: str = Field(..., description="Unique request ID")
    from_agent: str = Field(..., description="Agent initiating handover")
    to_agent: str = Field(..., description="Agent receiving handover")
    task_id: str | None = Field(None, description="Task being handed over")
    session_id: str | None = Field(None, description="Session/Conversation ID")
    context: dict[str, Any] = Field(default_factory=dict)
    reason: str | None = Field(None, description="Reason for handover")
    priority: int = Field(0, description="Priority level")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    timeout_seconds: int = 30


class HandoverResponse(BaseModel):
    """
    Response to a handover request.
    """
    request_id: str = Field(..., description="Request being responded to")
    from_agent: str = Field(..., description="Agent responding")
    status: HandoverStatus = Field(..., description="Response status")
    message: str | None = Field(None, description="Optional message")
    accepted_context: dict[str, Any] | None = Field(None)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))


class A2AMessage(BaseModel):
    """
    Generic A2A message for agent-to-agent communication.
    """
    message_id: str = Field(..., description="Unique message ID")
    from_agent: str = Field(..., description="Sender agent")
    to_agent: str = Field(..., description="Target agent")
    type: A2AMessageType = Field(..., description="Message type")
    content: dict[str, Any] = Field(default_factory=dict)
    correlation_id: str | None = Field(None)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(UTC))
    tenant_id: str | None = Field(None)
