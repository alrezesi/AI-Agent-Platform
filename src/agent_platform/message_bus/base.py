
# Abstract base class for message bus implementations

from abc import ABC, abstractmethod
from typing import Optional, Callable, Awaitable, List
from src.agent_platform.core.message import Message


MessageHandler = Callable[[Message], Awaitable[None]]
"""Type alias for async message handler functions."""


class BaseMessageBus(ABC):
    """
    Abstract interface for a message bus.
    Supports point-to-point, broadcast, and topic-based messaging.
    """

    # --- Send Methods ---

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
        Returns list of delivered message IDs (if tracking is supported).
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

    # --- Subscription Methods ---

    @abstractmethod
    async def subscribe(
        self,
        agent_id: str,
        handler: MessageHandler,
        topics: Optional[List[str]] = None,
    ) -> None:
        """
        Subscribe an agent to receive messages.
        - If topics is None, agent receives all point-to-point and broadcast messages.
        - If topics is provided, agent only receives messages published to those topics.
        """
        pass

    @abstractmethod
    async def unsubscribe(self, agent_id: str) -> None:
        """Unsubscribe an agent from all message streams."""
        pass

    # --- Lifecycle ---

    @abstractmethod
    async def start(self) -> None:
        """Start the message bus (connect to brokers, etc.)."""
        pass

    @abstractmethod
    async def stop(self) -> None:
        """Gracefully stop the message bus."""
        pass

    # --- Utilities ---

    @abstractmethod
    async def get_message(self, message_id: str) -> Optional[Message]:
        """Retrieve a message by ID (if persisted)."""
        pass

    @abstractmethod
    async def acknowledge(self, message_id: str, agent_id: str) -> bool:
        """
        Acknowledge successful processing of a message.
        Returns True if acknowledged successfully.
        """
        pass