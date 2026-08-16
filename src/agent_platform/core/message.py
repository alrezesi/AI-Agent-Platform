
# Message models for inter-agent communication

import uuid
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class MessageType(StrEnum):
    """Type of message indicating its purpose."""
    REQUEST = "request"          # Request-response pattern
    RESPONSE = "response"        # Response to a request
    EVENT = "event"              # One-way event notification
    COMMAND = "command"          # Command to an agent
    HEARTBEAT = "heartbeat"      # Agent heartbeat signal
    ERROR = "error"              # Error message
    BROADCAST = "broadcast"      # Broadcast to all age


class MessagePriority(StrEnum):
    """Priority lor message delivery."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class MessageStatus(StrEnum):
    """Status of a message in the delivery process."""
    PENDING = "pending"          # Queued for delivery
    DELIVERED = "delivered"      # Successfully delivered
    FAILED = "failed"            # Delivery failed
    EXPIRED = "expired"          # TTL expired before delivery


class Message(BaseModel):
    """
    Core message model for all inter-agent communication.
    Supports request-response, events, commands, and broadcasts.
    """

    # --- Identifiers ---
    message_id: str = Field(
        default_factory=lambda: f"msg-{uuid.uuid4().hex[:12]}",
        description="Unique message ID"
    )
    correlation_id: str | None = Field(
        None,
        description="Correlation ID for request-response matching"
    )

    # --- Routing ---
    from_agent: str = Field(..., description="Sender agent ID")
    to_agent: str | None = Field(
        None,
        description="Target agent ID (None for broadcast)"
    )
    topic: str | None = Field(
        None,
        description="Topic for pub/sub routing"
    )

    # --- Content ---
    type: MessageType = Field(..., description="Message type")
    content: dict[str, Any] = Field(
        default_factory=dict,
        description="Payload content"
    )
    priority: MessagePriority = Field(
        default=MessagePriority.MEDIUM,
        description="Delivery priority"
    )

    # --- Metadata ---
    timestamp: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="Creation timestamp"
    )
    ttl_seconds: int | None = Field(
        300,
        description="Time-to-live in seconds (None = no expiry)"
    )
    tenant_id: str | None = Field(
        None,
        description="Tenant ID for multi-tenancy"
    )

    # --- Status (for tracking) ---
    status: MessageStatus = Field(
        default=MessageStatus.PENDING,
        description="Current delivery status"
    )
    retry_count: int = Field(
        default=0,
        description="Number of delivery retry attempts"
    )
    max_retries: int = Field(
        default=3,
        description="Maximum delivery retries"
    )

    def is_expired(self) -> bool:
        """Check if the message TTL has expired."""
        if self.ttl_seconds is None:
            return False
        age = (datetime.now(UTC) - self.timestamp).total_seconds()
        return age > self.ttl_seconds

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return self.model_dump(mode='json')

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> 'Message':
        """Create a Message from a dictionary."""
        return cls.model_validate(data)
