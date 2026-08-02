
# Redis-backed message bus using Pub/Sub and List for point-to-point

import json
import asyncio
import logging
from typing import Dict, List, Optional, Set, Callable, Awaitable
from redis.asyncio import Redis


from src.agent_platform.core.message import Message, MessageStatus
from src.agent_platform.message_bus.base import BaseMessageBus, MessageHandler
from src.agent_platform.message_bus.exceptions import MessageDeliveryError

logger = logging.getLogger(__name__)


class RedisMessageBus(BaseMessageBus):
    """
    Redis-based message bus.
    Uses Redis Pub/Sub for broadcasts and topic messages.
    Uses Redis List (LPUSH/RPOP) for point-to-point reliable queues.
    """

    def __init__(
        self,
        redis_client: Redis,
        message_ttl_seconds: int = 3600,
    ):
        self.redis = redis_client
        self.message_ttl = message_ttl_seconds
        self._running = False

        # agent_id -> asyncio.Queue for received messages (for handler dispatch)
        self._receive_queues: Dict[str, asyncio.Queue] = {}
        self._handlers: Dict[str, MessageHandler] = {}
        self._pubsub = None
        self._pubsub_task: Optional[asyncio.Task] = None
        self._worker_tasks: List[asyncio.Task] = []

        # Redis key patterns
        self._queue_prefix = "msgbus:queue:"
        self._store_prefix = "msgbus:store:"
        self._topic_prefix = "msgbus:topic:"

    async def start(self) -> None:
        """Start the bus and connect to Redis."""
        if self._running:
            return
        self._running = True

        # Initialize pubsub
        self._pubsub = self.redis.pubsub()
        await self._pubsub.connect()

        # Start the pubsub listener
        self._pubsub_task = asyncio.create_task(self._pubsub_listener())

        # Start worker for each subscribed agent
        for agent_id in list(self._handlers.keys()):
            task = asyncio.create_task(self._deliver_messages(agent_id))
            self._worker_tasks.append(task)

        logger.info("RedisMessageBus started")

    async def stop(self) -> None:
        """Stop the bus and clean up."""
        self._running = False

        # Cancel pubsub task
        if self._pubsub_task:
            self._pubsub_task.cancel()
            try:
                await self._pubsub_task
            except asyncio.CancelledError:
                pass

        # Cancel workers
        for task in self._worker_tasks:
            task.cancel()
        await asyncio.gather(*self._worker_tasks, return_exceptions=True)

        await self._pubsub.close()
        logger.info("RedisMessageBus stopped")

    async def send(self, message: Message) -> str:
        """Send point-to-point using Redis List."""
        if not message.to_agent:
            raise ValueError("to_agent required for point-to-point")

        queue_key = f"{self._queue_prefix}{message.to_agent}"
        store_key = f"{self._store_prefix}{message.message_id}"

        # Store message with TTL
        await self.redis.setex(
            store_key,
            self.message_ttl,
            message.model_dump_json()
        )

        # Push to recipient's queue
        await self.redis.lpush(queue_key, message.message_id)
        message.status = MessageStatus.DELIVERED

        logger.debug(f"Message {message.message_id} sent to {message.to_agent}")
        return message.message_id

    async def broadcast(self, message: Message) -> List[str]:
        """Broadcast using Redis Pub/Sub on a well-known channel."""
        channel = "msgbus:broadcast"
        # Publish to all subscribers
        payload = message.model_dump_json()
        await self.redis.publish(channel, payload)
        # We can't track individual deliveries in pubsub
        # Just return the message ID
        return [message.message_id]

    async def publish(self, topic: str, message: Message) -> str:
        """Publish to a topic channel."""
        channel = f"{self._topic_prefix}{topic}"
        payload = message.model_dump_json()
        await self.redis.publish(channel, payload)
        return message.message_id

    async def subscribe(
        self,
        agent_id: str,
        handler: MessageHandler,
        topics: Optional[List[str]] = None,
    ) -> None:
        """Subscribe an agent with a handler."""
        # Store handler
        self._handlers[agent_id] = handler

        # Create a receive queue for this agent
        if agent_id not in self._receive_queues:
            self._receive_queues[agent_id] = asyncio.Queue()

        # Subscribe to pubsub channels for topics if any
        if topics:
            channels = [f"{self._topic_prefix}{t}" for t in topics]
            await self._pubsub.subscribe(*channels)
        else:
            # Subscribe to broadcast channel
            await self._pubsub.subscribe("msgbus:broadcast")

        # Start worker if running
        if self._running:
            task = asyncio.create_task(self._deliver_messages(agent_id))
            self._worker_tasks.append(task)

        logger.info(f"Agent {agent_id} subscribed to RedisMessageBus")

    async def unsubscribe(self, agent_id: str) -> None:
        """Unsubscribe an agent."""
        # Remove handler and queue
        if agent_id in self._handlers:
            del self._handlers[agent_id]
        if agent_id in self._receive_queues:
            del self._receive_queues[agent_id]

        # Unsubscribe from all pubsub channels (simplified)
        # In practice, we'd track per-agent subscriptions
        await self._pubsub.unsubscribe()

    async def _pubsub_listener(self) -> None:
        """
        Listens to Redis pubsub and pushes messages to agent queues.
        """
        while self._running:
            try:
                message = await self._pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=1.0
                )
                if message is None:
                    continue

                # Parse the payload
                data = message.get('data')
                if data:
                    try:
                        msg_dict = json.loads(data)
                        msg = Message.model_validate(msg_dict)
                        # Determine which agents should receive this
                        # For broadcast, send to all with handlers
                        # For topics, we need to filter by subscription
                        # But since we have per-channel subscriptions,
                        # we can check the channel.
                        channel = message.get('channel').decode()
                        if channel == "msgbus:broadcast":
                            # Send to all agents
                            for agent_id in self._receive_queues.keys():
                                await self._receive_queues[agent_id].put(msg)
                        elif channel.startswith(self._topic_prefix):
                            topic = channel[len(self._topic_prefix):]
                            # Send to agents subscribed to this topic
                            # We need to track per-agent subscriptions
                            # For simplicity, we'll send to all for now.
                            # In a production implementation, we'd maintain a mapping.
                            for agent_id in self._receive_queues.keys():
                                await self._receive_queues[agent_id].put(msg)
                    except Exception as e:
                        logger.error(f"Failed to parse pubsub message: {e}")
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Pubsub listener error: {e}")
                await asyncio.sleep(1)

    async def _deliver_messages(self, agent_id: str) -> None:
        """
        Worker that takes messages from agent's queue and calls handler.
        Also polls Redis queue for point-to-point messages.
        """
        queue = self._receive_queues.get(agent_id)
        if not queue:
            return

        while self._running and agent_id in self._handlers:
            try:
                # Check Redis queue for point-to-point messages
                queue_key = f"{self._queue_prefix}{agent_id}"
                msg_id = await self.redis.rpop(queue_key)
                if msg_id:
                    # Fetch message from store
                    store_key = f"{self._store_prefix}{msg_id}"
                    data = await self.redis.get(store_key)
                    if data:
                        msg = Message.model_validate_json(data)
                        # Add to local queue
                        await queue.put(msg)

                # Process local queue
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=0.5)
                    handler = self._handlers.get(agent_id)
                    if handler:
                        await handler(msg)
                except asyncio.TimeoutError:
                    pass

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Delivery worker error for {agent_id}: {e}")
                await asyncio.sleep(0.5)

    async def get_message(self, message_id: str) -> Optional[Message]:
        """Retrieve a message by ID from Redis store."""
        key = f"{self._store_prefix}{message_id}"
        data = await self.redis.get(key)
        if data:
            return Message.model_validate_json(data)
        return None

    async def acknowledge(self, message_id: str, agent_id: str) -> bool:
        """
        Acknowledge a message. For Redis, we just remove from store if needed.
        """
        key = f"{self._store_prefix}{message_id}"
        # Check if it belongs to this agent
        # We'll just delete it; in a production system, we'd add more tracking.
        deleted = await self.redis.delete(key)
        return bool(deleted)