
# In-memory message bus implementation (suitable for testing and single-node)

import asyncio
import logging
from typing import Dict, List, Optional, Set

from src.agent_platform.core.message import Message, MessageStatus
from src.agent_platform.message_bus.base import BaseMessageBus, MessageHandler
from src.agent_platform.message_bus.exceptions import MessageDeliveryError

logger = logging.getLogger(__name__)


class InMemoryMessageBus(BaseMessageBus):
    """
    Simple in-memory message bus using asyncio.Queue for each subscriber.
    Supports point-to-point, broadcast, and topic-based subscriptions.
    """

    def __init__(self):
        # agent_id -> asyncio.Queue of messages
        self._queues: Dict[str, asyncio.Queue] = {}
        # agent_id -> set of topics they are subscribed to
        self._subscriptions: Dict[str, Set[str]] = {}
        # agent_id -> handler function (for processing messages)
        self._handlers: Dict[str, MessageHandler] = {}
        # message_id -> Message (for history/ack tracking)
        self._message_store: Dict[str, Message] = {}

        self._lock = asyncio.Lock()
        self._running = False
        self._worker_tasks: List[asyncio.Task] = []

    async def start(self) -> None:
        """Start the bus and workers."""
        if self._running:
            return
        self._running = True
        logger.info("InMemoryMessageBus started")

    async def stop(self) -> None:
        """Stop the bus and cancel all workers."""
        self._running = False
        for task in self._worker_tasks:
            task.cancel()
        await asyncio.gather(*self._worker_tasks, return_exceptions=True)
        self._worker_tasks.clear()
        logger.info("InMemoryMessageBus stopped")

    async def send(self, message: Message) -> str:
        """Send a point-to-point message to a specific agent."""
        if not message.to_agent:
            raise ValueError("to_agent is required for point-to-point send")

        async with self._lock:
            # Validate recipient exists
            if message.to_agent not in self._queues:
                raise MessageDeliveryError(
                    f"Recipient agent {message.to_agent} not subscribed"
                )

            # Store message
            self._message_store[message.message_id] = message

            # Enqueue to recipient
            await self._queues[message.to_agent].put(message)
            message.status = MessageStatus.DELIVERED

        logger.debug(f"Message {message.message_id} sent to {message.to_agent}")
        return message.message_id

    async def broadcast(self, message: Message) -> List[str]:
        """Broadcast a message to all subscribed agents."""
        delivered_ids = []
        async with self._lock:
            for agent_id in list(self._queues.keys()):
                # Skip sender if needed? Usually we send to everyone.
                # We'll allow sending to sender as well.
                msg_copy = message.model_copy(deep=True)
                msg_copy.to_agent = agent_id
                msg_copy.message_id = f"broadcast-{message.message_id}-{agent_id}"
                self._message_store[msg_copy.message_id] = msg_copy
                await self._queues[agent_id].put(msg_copy)
                delivered_ids.append(msg_copy.message_id)

        logger.info(f"Broadcast message delivered to {len(delivered_ids)} agents")
        return delivered_ids

    async def publish(self, topic: str, message: Message) -> str:
        """Publish a message to a topic."""
        # Find all agents subscribed to this topic
        async with self._lock:
            recipients = []
            for agent_id, topics in self._subscriptions.items():
                if topic in topics:
                    recipients.append(agent_id)

            if not recipients:
                logger.warning(f"No subscribers for topic '{topic}'")
                return message.message_id

            for agent_id in recipients:
                msg_copy = message.model_copy(deep=True)
                msg_copy.to_agent = agent_id
                msg_copy.topic = topic
                msg_copy.message_id = f"topic-{topic}-{message.message_id}-{agent_id}"
                self._message_store[msg_copy.message_id] = msg_copy
                await self._queues[agent_id].put(msg_copy)

        logger.debug(f"Topic '{topic}' message delivered to {len(recipients)} subscribers")
        return message.message_id

    async def subscribe(
        self,
        agent_id: str,
        handler: MessageHandler,
        topics: Optional[List[str]] = None,
    ) -> None:
        """Subscribe an agent with a handler."""
        async with self._lock:
            if agent_id not in self._queues:
                self._queues[agent_id] = asyncio.Queue(maxsize=1000)
            self._handlers[agent_id] = handler
            if topics:
                self._subscriptions[agent_id] = set(topics)
            else:
                self._subscriptions[agent_id] = set()  # Empty set = receive all

            # Start a worker for this agent if not already running
            if self._running:
                task = asyncio.create_task(self._deliver_messages(agent_id))
                self._worker_tasks.append(task)

        logger.info(f"Agent {agent_id} subscribed to message bus")

    async def unsubscribe(self, agent_id: str) -> None:
        """Unsubscribe an agent and clean up its queue."""
        async with self._lock:
            if agent_id in self._queues:
                del self._queues[agent_id]
            if agent_id in self._subscriptions:
                del self._subscriptions[agent_id]
            if agent_id in self._handlers:
                del self._handlers[agent_id]

        logger.info(f"Agent {agent_id} unsubscribed")

    async def _deliver_messages(self, agent_id: str) -> None:
        """
        Worker loop that delivers messages from the queue to the agent's handler.
        """
        queue = self._queues.get(agent_id)
        if not queue:
            return

        while self._running and agent_id in self._queues:
            try:
                message = await asyncio.wait_for(queue.get(), timeout=1.0)
                handler = self._handlers.get(agent_id)
                if handler:
                    await handler(message)
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error delivering message to {agent_id}: {e}")
                # Mark message as failed
                message.status = MessageStatus.FAILED
                # Optionally retry later

    async def get_message(self, message_id: str) -> Optional[Message]:
        """Retrieve a message by ID."""
        return self._message_store.get(message_id)

    async def acknowledge(self, message_id: str, agent_id: str) -> bool:
        """
        Acknowledge successful processing of a message.
        For in-memory, we just update status if message exists.
        """
        message = self._message_store.get(message_id)
        if message:
            if message.to_agent == agent_id:
                message.status = MessageStatus.DELIVERED
                return True
        return False