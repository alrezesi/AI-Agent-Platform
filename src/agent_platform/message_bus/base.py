
# Abstract base class for message bus implementations (enhanced)

from abc import ABC, abstractmethod
from typing import Optional, Callable, Awaitable, List, Dict, Any
from src.agent_platform.core.message import Message
from src.agent_platform.message_bus.models import Subscription, RouteRule, MessageDeliveryRecord


MessageHandler = Callable[[Message], Awaitable[None]]
"""Type alias for async message handler functions."""


class BaseMessageBus(ABC):
    """
    Abstract interface for a message bus.
    Supports point-to-point, broadcast, topic-based messaging,
    role-based routing, message persistence, and acknowledgments.
    """

    # --- Core Send Methods ---

    @abstractmethod
    async def send(self, message: Message) -> str:
        """
        Send a point-to-point message to a specific agent.
        Returns the message ID.
        """
        pass

    @abstractmethod
    async def broadcast(self, message: Message) -> List[str]:
        """
        Broadcast a message to all registered agents.
        Returns list of delivered message IDs.
        """
        pass

    @abstractmethod
    async def publish(self, topic: str, message: Message) -> str:
        """
        Publish a message to a specific topic.
        Subscribers to this topic will receive it.
        Returns the message ID.
        """
        pass

    # --- Advanced Routing ---

    @abstractmethod
    async def route_by_role(self, message: Message) -> List[str]:
        """
        Route a message to agents based on their roles.
        Returns list of agent IDs that should receive the message.
        """
        pass

    @abstractmethod
    async def add_route_rule(self, rule: RouteRule) -> None:
        """Add a routing rule."""
        pass

    @abstractmethod
    async def remove_route_rule(self, rule_id: str) -> bool:
        """Remove a routing rule."""
        pass

    # --- Subscription Management ---

    @abstractmethod
    async def subscribe(
        self,
        agent_id: str,
        handler: MessageHandler,
        topics: Optional[List[str]] = None,
        roles: Optional[List[str]] = None,
        filter_criteria: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Subscribe an agent to receive messages.
        Returns a subscription ID.
        """
        pass

    @abstractmethod
    async def unsubscribe(self, agent_id: str, subscription_id: Optional[str] = None) -> bool:
        """
        Unsubscribe an agent from all or a specific subscription.
        Returns True if unsubscribed.
        """
        pass

    @abstractmethod
    async def get_subscriptions(self, agent_id: str) -> List[Subscription]:
        """Get all subscriptions for an agent."""
        pass

    # --- Message Persistence ---

    @abstractmethod
    async def persist_message(self, message: Message) -> None:
        """Store a message in persistent storage."""
        pass

    @abstractmethod
    async def get_message_history(
        self,
        agent_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Message]:
        """Retrieve message history for an agent or all messages."""
        pass

    @abstractmethod
    async def get_message(self, message_id: str) -> Optional[Message]:
        """Retrieve a message by ID from persistent storage."""
        pass

    # --- Acknowledgment ---

    @abstractmethod
    async def acknowledge(self, message_id: str, agent_id: str) -> bool:
        """
        Acknowledge successful processing of a message.
        Returns True if acknowledged successfully.
        """
        pass

    @abstractmethod
    async def get_delivery_status(self, message_id: str) -> List[MessageDeliveryRecord]:
        """Get delivery status for all recipients of a message."""
        pass

    # --- Lifecycle ---

    @abstractmethod
    async def start(self) -> None:
        """Start the message bus."""
        pass

    @abstractmethod
    async def stop(self) -> None:
        """Gracefully stop the message bus."""
        pass