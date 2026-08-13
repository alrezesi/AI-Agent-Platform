
# Handover protocol models for A2A communication

from enum import Enum
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field
from datetime import datetime, timezone


class HandoverStatus(str, Enum):
    """Status of a handover request."""
    PENDING = "pending"
    ACCEPTED = "accepted"
    REJECTED = "rejected"
    COMPLETED = "completed"
    FAILED = "failed"


class A2AMessageType(str, Enum):
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
    task_id: Optional[str] = Field(None, description="Task being handed over")
    session_id: Optional[str] = Field(None, description="Session/Conversation ID")
    context: Dict[str, Any] = Field(default_factory=dict)
    reason: Optional[str] = Field(None, description="Reason for handover")
    priority: int = Field(0, description="Priority level")
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    timeout_seconds: int = 30


class HandoverResponse(BaseModel):
    """
    Response to a handover request.
    """
    request_id: str = Field(..., description="Request being responded to")
    from_agent: str = Field(..., description="Agent responding")
    status: HandoverStatus = Field(..., description="Response status")
    message: Optional[str] = Field(None, description="Optional message")
    accepted_context: Optional[Dict[str, Any]] = Field(None)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class A2AMessage(BaseModel):
    """
    Generic A2A message for agent-to-agent communication.
    """
    message_id: str = Field(..., description="Unique message ID")
    from_agent: str = Field(..., description="Sender agent")
    to_agent: str = Field(..., description="Target agent")
    type: A2AMessageType = Field(..., description="Message type")
    content: Dict[str, Any] = Field(default_factory=dict)
    correlation_id: Optional[str] = Field(None)
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    tenant_id: Optional[str] = Field(None)
