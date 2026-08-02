
# Message models for inter-agent communication

from enum import Enum
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field, validator
from datetime import datetime
import uuid


class MessageType(str, Enum):
    """Type of message indicating its purpose."""
    REQUEST = "request"          # Request-response pattern
    RESPONSE = "response"        # Response to a request
    EVENT = "event"              # One-way event notification
    COMMAND = "command"          # Command to an agent
    HEARTBEAT = "heartbeat"      # Agent heartbeat signal
    ERROR = "error"              # Error message
    BROADCAST = "broadcast"      # Broadcast to all age
class MessagePriority(str, Enum):
    """Priority lor message delivery."""
    LOW = "low"
    MEDIUM = "medium"
    HIGH = "high"
    CRITICAL = "critical"


class MessageStatus(str, Enum):
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
    correlation_id: Optional[str] = Field(
        None,
        description="Correlation ID for request-response matching"
    )

    # --- Routing ---
    from_agent: str = Field(..., description="Sender agent ID")
    to_agent: Optional[str] = Field(
        None,
        description="Target agent ID (None for broadcast)"
    )
    topic: Optional[str] = Field(
        None,
        description="Topic for pub/sub routing"
    )

    # --- Content ---
    type: MessageType = Field(..., description="Message type")
    content: Dict[str, Any] = Field(
        default_factory=dict,
        description="Payload content"
    )
    priority: MessagePriority = Field(
        default=MessagePriority.MEDIUM,
        description="Delivery priority"
    )

    # --- Metadata ---
    timestamp: datetime = Field(
        default_factory=datetime.utcnow,
        description="Creation timestamp"
    )
    ttl_seconds: Optional[int] = Field(
        300,
        description="Time-to-live in seconds (None = no expiry)"
    )
    tenant_id: Optional[str] = Field(
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

    # --- Schema validation ---
    @validator('to_agent')
    def check_target(cls, v, values):
        """Ensure to_agent is set for non-broadcast messages."""
        msg_type = values.get('type')
        if msg_type and msg_type != MessageType.BROADCAST and v is None:
            raise ValueError(f"to_agent is required for {msg_type} messages")
        return v

    def is_expired(self) -> bool:
        """Check if the message TTL has expired."""
        if self.ttl_seconds is None:
            return False
        age = (datetime.utcnow() - self.timestamp).total_seconds()
        return age > self.ttl_seconds

    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for serialization."""
        return self.model_dump(mode='json')

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Message':
        """Create a Message from a dictionary."""
        return cls.model_validate(data)