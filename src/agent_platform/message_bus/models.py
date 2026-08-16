
# Advanced models for subscriptions, routing, and persistence

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class SubscriptionType(StrEnum):
    """Type of subscription."""
    POINT_TO_POINT = "point_to_point"  # Direct messages to a specific agent
    TOPIC = "topic"                    # Topic-based pub/sub
    BROADCAST = "broadcast"            # Receive all broadcast messages
    ROLE = "role"                      # Based on agent role


class Subscription(BaseModel):
    """Represents a subscription of an agent to message streams."""
    subscription_id: str = Field(..., description="Unique subscription ID")
    agent_id: str = Field(..., description="Subscribing agent ID")
    type: SubscriptionType = Field(..., description="Type of subscription")
    topic: str | None = Field(None, description="Topic name (for TOPIC type)")
    role: str | None = Field(None, description="Role name (for ROLE type)")
    filter_criteria: dict[str, Any] | None = Field(
        None, description="Optional filter criteria (e.g., {'priority': 'high'})"
    )
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    is_active: bool = Field(True, description="Whether subscription is active")


class MessageDeliveryStatus(StrEnum):
    """Status of a message delivery attempt."""
    PENDING = "pending"
    DELIVERED = "delivered"
    ACKNOWLEDGED = "acknowledged"
    FAILED = "failed"
    EXPIRED = "expired"


class MessageDeliveryRecord(BaseModel):
    """Record of a message delivery attempt to an agent."""
    message_id: str
    agent_id: str
    status: MessageDeliveryStatus = MessageDeliveryStatus.PENDING
    delivered_at: datetime | None = None
    acknowledged_at: datetime | None = None
    attempt_count: int = 0
    last_error: str | None = None


class RouteRule(BaseModel):
    """
    A routing rule that determines which agents receive a message
    based on message attributes and agent roles.
    """
    rule_id: str
    name: str
    description: str | None = None
    # Conditions: e.g., {"message.type": "event", "message.priority": "high"}
    conditions: dict[str, Any] = Field(default_factory=dict)
    # Target: list of agent roles or specific agent IDs
    target_roles: list[str] = Field(default_factory=list)
    target_agents: list[str] = Field(default_factory=list)
    priority: int = 0  # Higher priority rules are evaluated first
    is_active: bool = True
