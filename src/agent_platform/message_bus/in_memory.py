
# Enhanced in-memory message bus with topics, roles, persistence, and acks

import asyncio
import logging
from typing import Dict, List, Optional, Set, Any
from collections import defaultdict
from datetime import datetime, timezone
import uuid

from src.agent_platform.core.message import Message, MessageStatus
from src.agent_platform.message_bus.base import BaseMessageBus, MessageHandler
from src.agent_platform.message_bus.models import (
    Subscription, SubscriptionType, RouteRule,
    MessageDeliveryRecord, MessageDeliveryStatus,
)
from src.agent_platform.message_bus.exceptions import MessageDeliveryError

logger = logging.getLogger(__name__)


class InMemoryMessageBus(BaseMessageBus):
    """
    Enhanced in-memory message bus with full feature set.
    Supports topics, role-based routing, persistence, and acknowledgments.
    """

    def __init__(self):
        # agent_id -> asyncio.Queue of messages
        self._queues: Dict[str, asyncio.Queue] = {}
        # agent_id -> Subscription objects
        self._subscriptions: Dict[str, List[Subscription]] = defaultdict(list)
        # agent_id -> handler function
        self._handlers: Dict[str, MessageHandler] = {}
        # topic -> set of subscription_ids
        self._topic_subscribers: Dict[str, Set[str]] = defaultdict(set)
        # role -> set of agent_ids
        self._role_members: Dict[str, Set[str]] = defaultdict(set)
        # routing rules
        self._route_rules: Dict[str, RouteRule] = {}
        # message_id -> Message (persistent store)
        self._message_store: Dict[str, Message] = {}
        # message_id -> List[MessageDeliveryRecord]
        self._delivery_records: Dict[str, List[MessageDeliveryRecord]] = defaultdict(list)
        # subscription_id -> Subscription
        self._sub_by_id: Dict[str, Subscription] = {}

        self._lock = asyncio.Lock()
        self._running = False
        self._worker_tasks: List[asyncio.Task] = []

    async def start(self) -> None:
        if self._running:
            return
        self._running = True
        # Start workers for all subscribed agents
        for agent_id in self._handlers.keys():
            task = asyncio.create_task(self._deliver_messages(agent_id))
            self._worker_tasks.append(task)
        logger.info("InMemoryMessageBus (enhanced) started")

    async def stop(self) -> None:
        self._running = False
        for task in self._worker_tasks:
            task.cancel()
        if self._worker_tasks:
            try:
                await asyncio.gather(*self._worker_tasks, return_exceptions=True)
            except (RuntimeError, ValueError):
                # The bus may be stopped from a different event loop in tests or teardown.
                pass
        self._worker_tasks.clear()
        logger.info("InMemoryMessageBus stopped")

    # --- Core Send Methods ---

    async def send(self, message: Message) -> str:
        if not message.to_agent:
            raise ValueError("to_agent is required for point-to-point")
        async with self._lock:
            if message.message_id in self._message_store:
                return message.message_id
            if message.to_agent not in self._queues:
                raise MessageDeliveryError(f"Agent {message.to_agent} not subscribed")
            self._message_store[message.message_id] = message
            await self._record_delivery(message.message_id, message.to_agent)
            await self._queues[message.to_agent].put(message)
        return message.message_id

    async def broadcast(self, message: Message) -> List[str]:
        delivered = []
        async with self._lock:
            for agent_id in list(self._queues.keys()):
                msg_copy = message.model_copy(deep=True)
                msg_copy.to_agent = agent_id
                msg_copy.message_id = f"broadcast-{message.message_id}-{agent_id}"
                self._message_store[msg_copy.message_id] = msg_copy
                await self._record_delivery(msg_copy.message_id, agent_id)
                await self._queues[agent_id].put(msg_copy)
                delivered.append(msg_copy.message_id)
        return delivered

    async def publish(self, topic: str, message: Message) -> str:
        async with self._lock:
            sub_ids = self._topic_subscribers.get(topic, set())
            if not sub_ids:
                logger.warning(f"No subscribers for topic '{topic}'")
                return message.message_id

            for sub_id in sub_ids:
                sub = self._sub_by_id.get(sub_id)
                if not sub or not sub.is_active:
                    continue
                agent_id = sub.agent_id
                if agent_id not in self._queues:
                    continue
                msg_copy = message.model_copy(deep=True)
                msg_copy.to_agent = agent_id
                msg_copy.topic = topic
                msg_copy.message_id = f"topic-{topic}-{message.message_id}-{agent_id}"
                self._message_store[msg_copy.message_id] = msg_copy
                await self._record_delivery(msg_copy.message_id, agent_id)
                await self._queues[agent_id].put(msg_copy)

        return message.message_id

    # --- Routing ---

    async def route_by_role(self, message: Message) -> List[str]:
        """Route message to agents based on their roles."""
        recipients = []
        async with self._lock:
            # Evaluate route rules in priority order
            sorted_rules = sorted(
                self._route_rules.values(),
                key=lambda r: r.priority,
                reverse=True
            )
            for rule in sorted_rules:
                if not rule.is_active:
                    continue
                # Check if conditions match
                if self._match_conditions(rule.conditions, message):
                    # Add target roles
                    for role in rule.target_roles:
                        recipients.extend(self._role_members.get(role, set()))
                    # Add target agents
                    recipients.extend(rule.target_agents)
                    # If we have matches, we can stop (first matching rule wins)
                    if recipients:
                        break
        return list(set(recipients))  # deduplicate

    def _match_conditions(self, conditions: Dict[str, Any], message: Message) -> bool:
        """Check if message matches the condition dict."""
        for key, expected in conditions.items():
            # Support dot notation: "message.type" -> getattr(message, "type")
            parts = key.split(".")
            value = message
            for part in parts:
                if hasattr(value, part):
                    value = getattr(value, part)
                else:
                    return False
            if value != expected:
                return False
        return True

    async def add_route_rule(self, rule: RouteRule) -> None:
        async with self._lock:
            self._route_rules[rule.rule_id] = rule

    async def remove_route_rule(self, rule_id: str) -> bool:
        async with self._lock:
            if rule_id in self._route_rules:
                del self._route_rules[rule_id]
                return True
            return False

    # --- Subscription ---

    async def subscribe(
        self,
        agent_id: str,
        handler: MessageHandler,
        topics: Optional[List[str]] = None,
        roles: Optional[List[str]] = None,
        filter_criteria: Optional[Dict[str, Any]] = None,
    ) -> str:
        async with self._lock:
            # Create queue if not exists
            if agent_id not in self._queues:
                self._queues[agent_id] = asyncio.Queue(maxsize=1000)

            # Store handler
            self._handlers[agent_id] = handler

            # Create subscription
            sub_id = f"sub-{uuid.uuid4().hex[:8]}"
            sub_type = SubscriptionType.TOPIC if topics else (
                SubscriptionType.ROLE if roles else SubscriptionType.BROADCAST
            )
            subscription = Subscription(
                subscription_id=sub_id,
                agent_id=agent_id,
                type=sub_type,
                topic=topics[0] if topics and len(topics) == 1 else None,
                role=roles[0] if roles and len(roles) == 1 else None,
                filter_criteria=filter_criteria,
            )
            self._sub_by_id[sub_id] = subscription
            self._subscriptions[agent_id].append(subscription)

            # Register topic subscriptions
            if topics:
                for topic in topics:
                    self._topic_subscribers[topic].add(sub_id)

            # Register role membership
            if roles:
                for role in roles:
                    self._role_members[role].add(agent_id)

            # Start worker if running
            if self._running:
                task = asyncio.create_task(self._deliver_messages(agent_id))
                self._worker_tasks.append(task)

            logger.info(f"Agent {agent_id} subscribed with ID {sub_id}")
            return sub_id

    async def unsubscribe(self, agent_id: str, subscription_id: Optional[str] = None) -> bool:
        async with self._lock:
            if subscription_id:
                # Remove specific subscription
                sub = self._sub_by_id.get(subscription_id)
                if not sub or sub.agent_id != agent_id:
                    return False
                # Remove from topic subscribers
                if sub.topic:
                    self._topic_subscribers[sub.topic].discard(subscription_id)
                # Remove from role members
                if sub.role:
                    self._role_members[sub.role].discard(agent_id)
                del self._sub_by_id[subscription_id]
                self._subscriptions[agent_id] = [
                    s for s in self._subscriptions.get(agent_id, [])
                    if s.subscription_id != subscription_id
                ]
                logger.info(f"Subscription {subscription_id} removed")
                return True
            else:
                # Remove all subscriptions for agent
                for sub in self._subscriptions.get(agent_id, []):
                    if sub.topic:
                        self._topic_subscribers[sub.topic].discard(sub.subscription_id)
                    if sub.role:
                        self._role_members[sub.role].discard(agent_id)
                    if sub.subscription_id in self._sub_by_id:
                        del self._sub_by_id[sub.subscription_id]
                self._subscriptions[agent_id] = []
                if agent_id in self._handlers:
                    del self._handlers[agent_id]
                logger.info(f"All subscriptions removed for agent {agent_id}")
                return True

    async def get_subscriptions(self, agent_id: str) -> List[Subscription]:
        async with self._lock:
            return self._subscriptions.get(agent_id, [])

    # --- Message Delivery ---

    async def _deliver_messages(self, agent_id: str) -> None:
        queue = self._queues.get(agent_id)
        if not queue:
            return
        while self._running and agent_id in self._queues:
            try:
                message = await asyncio.wait_for(queue.get(), timeout=1.0)
                handler = self._handlers.get(agent_id)
                if handler:
                    await handler(message)
                    # Update delivery status to DELIVERED (will be ACKNOWLEDGED later)
                    await self._update_delivery_status(
                        message.message_id, agent_id, MessageDeliveryStatus.DELIVERED
                    )
            except asyncio.TimeoutError:
                continue
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error delivering message to {agent_id}: {e}")
                await self._update_delivery_status(
                    message.message_id, agent_id, MessageDeliveryStatus.FAILED,
                    last_error=str(e)
                )

    # --- Persistence ---

    async def persist_message(self, message: Message) -> None:
        async with self._lock:
            self._message_store[message.message_id] = message

    async def get_message_history(
        self,
        agent_id: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[Message]:
        async with self._lock:
            if agent_id:
                # Get messages where this agent is the recipient
                # For simplicity, we'll filter the store
                msgs = [
                    m for m in self._message_store.values()
                    if m.to_agent == agent_id
                ]
            else:
                msgs = list(self._message_store.values())
            msgs.sort(key=lambda m: m.timestamp, reverse=True)
            return msgs[offset:offset + limit]

    async def get_message(self, message_id: str) -> Optional[Message]:
        return self._message_store.get(message_id)

    # --- Acknowledgment ---

    async def acknowledge(self, message_id: str, agent_id: str) -> bool:
        """Acknowledge successful processing of a message."""
        return await self._update_delivery_status(
            message_id, agent_id, MessageDeliveryStatus.ACKNOWLEDGED
        )

    async def has_processed(self, message_id: str) -> bool:
        return message_id in self._message_store

    async def get_delivery_status(self, message_id: str) -> List[MessageDeliveryRecord]:
        return self._delivery_records.get(message_id, [])

    # --- Internal Helpers ---

    async def _record_delivery(self, message_id: str, agent_id: str) -> None:
        record = MessageDeliveryRecord(
            message_id=message_id,
            agent_id=agent_id,
            status=MessageDeliveryStatus.PENDING,
        )
        self._delivery_records[message_id].append(record)

    async def _update_delivery_status(
        self,
        message_id: str,
        agent_id: str,
        status: MessageDeliveryStatus,
        last_error: Optional[str] = None,
    ) -> bool:
        async with self._lock:
            records = self._delivery_records.get(message_id, [])
            for record in records:
                if record.agent_id == agent_id:
                    record.status = status
                    if status == MessageDeliveryStatus.ACKNOWLEDGED:
                        record.acknowledged_at = datetime.now(timezone.utc)
                    if status == MessageDeliveryStatus.DELIVERED:
                        record.delivered_at = datetime.now(timezone.utc)
                    if last_error:
                        record.last_error = last_error
                    record.attempt_count += 1
                    return True
            return False
