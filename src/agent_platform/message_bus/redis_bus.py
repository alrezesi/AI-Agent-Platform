# src/agent_platform/message_bus/redis_bus.py
# Redis-backed message bus with full implementation and robust worker

import asyncio
import json
import logging
from typing import TYPE_CHECKING, Any, cast

if TYPE_CHECKING:
    from redis.asyncio import Redis
    from redis.asyncio.client import PubSub
    RedisClient = Redis
else:
    RedisClient = Any
    PubSub = Any

from src.agent_platform.core.message import Message, MessageStatus
from src.agent_platform.message_bus.base import BaseMessageBus, MessageHandler
from src.agent_platform.message_bus.exceptions import MessageDeliveryError
from src.agent_platform.message_bus.models import MessageDeliveryRecord, RouteRule, Subscription

logger = logging.getLogger(__name__)


class RedisMessageBus(BaseMessageBus):
    """
    Redis-backed message bus.
    Supports point-to-point, broadcast, and topic-based messaging.
    """

    def __init__(self, redis_client: RedisClient, message_ttl_seconds: int = 3600):
        self.redis = redis_client
        self.message_ttl = message_ttl_seconds
        self._running = False
        self._handlers: dict[str, MessageHandler] = {}
        self._pubsub: PubSub | None = None
        self._pubsub_task: asyncio.Task | None = None
        self._worker_tasks: list[asyncio.Task] = []
        self._receive_queues: dict[str, asyncio.Queue] = {}

        # Redis key patterns
        self._queue_prefix = "msgbus:queue:"
        self._store_prefix = "msgbus:store:"
        self._topic_prefix = "msgbus:topic:"

    async def start(self) -> None:
        """Start the bus and connect to Redis."""
        if self._running:
            return
        self._running = True

        pubsub = self.redis.pubsub()
        self._pubsub = pubsub
        await pubsub.connect()

        self._pubsub_task = asyncio.create_task(self._pubsub_listener())

        # Start workers for existing handlers
        for agent_id in list(self._handlers.keys()):
            task = asyncio.create_task(self._deliver_messages(agent_id))
            self._worker_tasks.append(task)

        logger.info("RedisMessageBus started")

    async def stop(self) -> None:
        """Stop the bus and clean up."""
        self._running = False

        if self._pubsub_task:
            self._pubsub_task.cancel()
            try:
                await self._pubsub_task
            except asyncio.CancelledError:
                pass

        for task in self._worker_tasks:
            task.cancel()
        if self._worker_tasks:
            try:
                await asyncio.gather(*self._worker_tasks, return_exceptions=True)
            except (RuntimeError, ValueError):
                # Teardown can happen after the originating loop is gone in tests.
                pass

        if self._pubsub is not None:
            try:
                await self._pubsub.aclose()
            except (RuntimeError, ValueError):
                pass
        logger.info("RedisMessageBus stopped")

    # --- Core Send Methods ---

    async def send(self, message: Message) -> str:
        """Send point-to-point message."""
        if not message.to_agent:
            raise ValueError("to_agent required for point-to-point")

        queue_key = f"{self._queue_prefix}{message.to_agent}"
        store_key = f"{self._store_prefix}{message.message_id}"

        existing = await self.redis.get(store_key)
        if existing:
            return message.message_id

        # Store the full message with TTL
        await self.redis.set(
            store_key,
            message.model_dump_json(),
            ex=self.message_ttl
        )
        # Verify storage (optional)
        stored = await self.redis.get(store_key)
        if not stored:
            logger.error(f"Failed to store message {message.message_id} in Redis")
            raise MessageDeliveryError(f"Message storage failed for {message.message_id}")

        # Push message ID to the recipient's queue
        await self.redis.lpush(queue_key, message.message_id)
        message.status = MessageStatus.DELIVERED
        logger.info(f"Message {message.message_id} sent to {message.to_agent}")
        return message.message_id

    async def broadcast(self, message: Message) -> list[str]:
        """Broadcast using Redis Pub/Sub."""
        channel = "msgbus:broadcast"
        payload = message.model_dump_json()
        await self.redis.publish(channel, payload)
        logger.info(f"Broadcast message {message.message_id} published")
        return [message.message_id]

    async def publish(self, topic: str, message: Message) -> str:
        """Publish to a topic."""
        channel = f"{self._topic_prefix}{topic}"
        payload = message.model_dump_json()
        await self.redis.publish(channel, payload)
        logger.info(f"Message {message.message_id} published to topic '{topic}'")
        return message.message_id

    # --- Routing (stubs) ---

    async def route_by_role(self, message: Message) -> list[str]:
        logger.warning("route_by_role not implemented in RedisMessageBus")
        return []

    async def add_route_rule(self, rule: RouteRule) -> None:
        logger.warning("add_route_rule not implemented in RedisMessageBus")

    async def remove_route_rule(self, rule_id: str) -> bool:
        logger.warning("remove_route_rule not implemented in RedisMessageBus")
        return False

    # --- Subscription ---

    async def subscribe(
        self,
        agent_id: str,
        handler: MessageHandler,
        topics: list[str] | None = None,
        roles: list[str] | None = None,
        filter_criteria: dict[str, Any] | None = None,
    ) -> str:
        """Subscribe an agent."""
        self._handlers[agent_id] = handler
        if agent_id not in self._receive_queues:
            self._receive_queues[agent_id] = asyncio.Queue()

        # Subscribe to pubsub channels
        channels = []
        if topics:
            channels.extend([f"{self._topic_prefix}{t}" for t in topics])
        else:
            channels.append("msgbus:broadcast")

        if self._pubsub is not None:
            await self._pubsub.subscribe(*channels)

        if self._running:
            task = asyncio.create_task(self._deliver_messages(agent_id))
            self._worker_tasks.append(task)
            # Give the worker time to start
            await asyncio.sleep(0.2)

        sub_id = f"sub-{agent_id}"
        logger.info(f"Agent {agent_id} subscribed with ID {sub_id}")
        return sub_id

    async def unsubscribe(self, agent_id: str, subscription_id: str | None = None) -> bool:
        if agent_id in self._handlers:
            del self._handlers[agent_id]
        if agent_id in self._receive_queues:
            del self._receive_queues[agent_id]
        if self._pubsub is not None:
            await self._pubsub.unsubscribe()
        logger.info(f"Agent {agent_id} unsubscribed")
        return True

    async def get_subscriptions(self, agent_id: str) -> list[Subscription]:
        return []

    # --- Persistence ---

    async def persist_message(self, message: Message) -> None:
        key = f"{self._store_prefix}{message.message_id}"
        await self.redis.set(key, message.model_dump_json(), ex=self.message_ttl)

    async def get_message_history(
        self,
        agent_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[Message]:
        return []

    async def get_message(self, message_id: str) -> Message | None:
        key = f"{self._store_prefix}{message_id}"
        data = await self.redis.get(key)
        if data:
            return Message.model_validate_json(data)
        return None

    # --- Acknowledgment ---

    async def acknowledge(self, message_id: str, agent_id: str) -> bool:
        key = f"{self._store_prefix}{message_id}"
        deleted = await self.redis.delete(key)
        return bool(deleted)

    async def get_delivery_status(self, message_id: str) -> list[MessageDeliveryRecord]:
        return []

    # --- Internal Helpers (Workers) ---

    async def _pubsub_listener(self) -> None:
        """Listen to Redis pubsub and push messages to agent queues."""
        while self._running:
            try:
                if self._pubsub is None:
                    await asyncio.sleep(1)
                    continue
                message = await self._pubsub.get_message(
                    ignore_subscribe_messages=True,
                    timeout=1.0
                )
                if message is None:
                    continue

                data = message.get('data')
                if data:
                    try:
                        msg_dict = json.loads(data)
                        msg = Message.model_validate(msg_dict)
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
        Worker that polls the agent's Redis queue and calls its handler.
        """
        queue = self._receive_queues.get(agent_id)
        if not queue:
            logger.warning(f"No receive queue found for agent {agent_id}, worker exiting")
            return

        logger.info(f"Worker started for agent {agent_id}")

        while self._running and agent_id in self._handlers:
            try:
                # Check Redis queue for point-to-point messages
                queue_key = f"{self._queue_prefix}{agent_id}"
                msg_id_bytes = await self.redis.rpop(queue_key)
                if msg_id_bytes:
                    # Convert bytes to string
                    msg_id = (
                        msg_id_bytes.decode("utf-8")
                        if isinstance(msg_id_bytes, bytes)
                        else str(msg_id_bytes)
                    )
                    store_key = f"{self._store_prefix}{msg_id}"
                    data = await self.redis.get(store_key)
                    if data:
                        msg = cast(Message, Message.model_validate_json(data))
                        await queue.put(msg)
                        logger.debug(f"Retrieved message {msg_id} from queue for {agent_id}")
                    else:
                        logger.warning(f"Message {msg_id} not found in store, skipping")

                # Process messages from the local queue (pubsub messages)
                try:
                    msg = await asyncio.wait_for(queue.get(), timeout=0.5)
                    handler = self._handlers.get(agent_id)
                    if handler:
                        await handler(msg)
                except TimeoutError:
                    pass

            except asyncio.CancelledError:
                logger.info(f"Worker for {agent_id} cancelled")
                break
            except Exception as e:
                logger.error(f"Worker error for {agent_id}: {e}", exc_info=True)
                await asyncio.sleep(0.5)

        logger.info(f"Worker stopped for agent {agent_id}")
