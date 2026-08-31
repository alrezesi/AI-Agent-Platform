import asyncio
from collections import defaultdict, deque

import pytest

from src.agent_platform.core.agent import AgentCapability, AgentRecord, AgentStatus
from src.agent_platform.core.message import Message, MessageType
from src.agent_platform.core.task import Task, TaskStatus
from src.agent_platform.distributed.lock import DistributedLock
from src.agent_platform.distributed.queue import DistributedTaskQueue
from src.agent_platform.message_bus.redis_bus import RedisMessageBus
from src.agent_platform.registry.redis_registry import RedisAgentRegistry
from src.agent_platform.scheduler.models import TaskFilterOptions


class FakePubSub:
    def __init__(self):
        self.channels: list[str] = []
        self.closed = False

    async def connect(self):
        return None

    async def subscribe(self, *channels):
        self.channels.extend(channels)

    async def unsubscribe(self):
        self.channels.clear()

    async def get_message(self, ignore_subscribe_messages=True, timeout=1.0):
        # Simulate real Redis pubsub blocking for 'timeout' seconds when no
        # message is available.  Capping at a small value keeps tests fast while
        # still yielding control to the event loop so other async tasks (worker
        # tasks) get a fair chance to run.
        await asyncio.sleep(min(timeout, 0.01))
        return None

    async def aclose(self):
        self.closed = True


class FakeRedis:
    def __init__(self):
        self.store: dict[str, str] = {}
        self.zsets: dict[str, dict[str, float]] = defaultdict(dict)
        self.lists: dict[str, deque[str]] = defaultdict(deque)
        self.published: list[tuple[str, str]] = []
        self.pubsub_obj = FakePubSub()
        self.eval_result = 1
        self.set_results: deque[bool] = deque()

    def pubsub(self):
        return self.pubsub_obj

    async def setex(self, key, ttl, value):
        self.store[str(key)] = value
        return True

    async def set(self, key, value, ex=None, nx=False):
        key = str(key)
        if self.set_results:
            return self.set_results.popleft()
        if nx and key in self.store:
            return False
        self.store[key] = value
        return True

    async def get(self, key):
        return self.store.get(str(key))

    async def delete(self, key):
        return int(self.store.pop(str(key), None) is not None)

    async def exists(self, key):
        return int(str(key) in self.store)

    async def scan(self, cursor, match=None, count=100):
        prefix = match[:-1] if match and match.endswith("*") else match
        keys = [key for key in self.store if prefix is None or key.startswith(prefix)]
        return 0, keys

    async def zadd(self, key, mapping):
        self.zsets[str(key)].update({str(member): float(score) for member, score in mapping.items()})
        return len(mapping)

    async def zpopmin(self, key, count=1):
        zset = self.zsets[str(key)]
        if not zset:
            return []
        member = min(zset, key=zset.get)
        score = zset.pop(member)
        return [(member, score)]

    async def zrange(self, key, start, end, withscores=False):
        items = sorted(self.zsets[str(key)].items(), key=lambda item: item[1])
        selected = items[start:] if end == -1 else items[start : end + 1]
        return selected if withscores else [member for member, _ in selected]

    async def zrem(self, key, member):
        return int(self.zsets[str(key)].pop(str(member), None) is not None)

    async def zcard(self, key):
        return len(self.zsets[str(key)])

    async def lpush(self, key, value):
        self.lists[str(key)].appendleft(value)
        return len(self.lists[str(key)])

    async def rpop(self, key):
        queue = self.lists[str(key)]
        return queue.pop() if queue else None

    async def publish(self, channel, payload):
        self.published.append((channel, payload))
        return 1

    async def eval(self, script, numkeys, key, lock_id, *args):
        if self.eval_result:
            self.store.pop(str(key), None)
        return self.eval_result


@pytest.mark.asyncio
async def test_redis_registry_filters_tenant_capability_status_and_unregisters():
    redis = FakeRedis()
    registry = RedisAgentRegistry(redis, ttl_seconds=30)
    agent_a = AgentRecord(
        agent_id="a",
        name="Agent A",
        tenant_id="tenant-a",
        status=AgentStatus.PAUSED,
        capabilities=[AgentCapability(name="search")],
    )
    agent_b = AgentRecord(
        agent_id="b",
        name="Agent B",
        tenant_id="tenant-b",
        status=AgentStatus.ACTIVE,
        capabilities=[AgentCapability(name="code")],
    )

    await registry.register(agent_a)
    await registry.register(agent_b)

    assert (await registry.get_agent("a", tenant_id="tenant-a")).agent_id == "a"
    assert await registry.get_agent("a", tenant_id="tenant-b") is None
    assert [a.agent_id for a in await registry.discover(capability="search")] == ["a"]
    assert [a.agent_id for a in await registry.discover(status=AgentStatus.ACTIVE)] == ["b"]
    assert [a.agent_id for a in await registry.list_all(tenant_id="tenant-b")] == ["b"]
    assert await registry.heartbeat("missing") is False
    assert await registry.unregister("a", tenant_id="tenant-b") is False
    assert await registry.unregister("a", tenant_id="tenant-a") is True
    assert await registry.cleanup_stale() == 0


@pytest.mark.asyncio
async def test_redis_message_bus_persists_routes_acknowledges_and_stops():
    redis = FakeRedis()
    bus = RedisMessageBus(redis, message_ttl_seconds=60)
    received: list[Message] = []

    async def handler(message: Message):
        received.append(message)

    await bus.start()
    sub_id = await bus.subscribe("receiver", handler, topics=["alerts"])
    message = Message(
        from_agent="sender",
        to_agent="receiver",
        type=MessageType.REQUEST,
        content={"command": "ping"},
        correlation_id="corr",
    )

    assert sub_id == "sub-receiver"
    assert await bus.send(message) == message.message_id
    # The worker polls Redis then blocks on a local queue.get() with a 0.5 s
    # timeout, so give it enough time to pick up the message after it lands
    # in the Redis list.
    await asyncio.sleep(0.7)
    assert received[0].content == {"command": "ping"}
    assert (await bus.get_message(message.message_id)).message_id == message.message_id

    event = Message(from_agent="sender", type=MessageType.EVENT, content={"ok": True})
    assert await bus.publish("alerts", event) == event.message_id
    assert await bus.broadcast(event) == [event.message_id]
    assert redis.published[-2][0] == "msgbus:topic:alerts"
    assert redis.published[-1][0] == "msgbus:broadcast"
    assert await bus.acknowledge(message.message_id, "receiver") is True
    assert await bus.unsubscribe("receiver", sub_id) is True
    assert await bus.route_by_role(event) == []
    assert await bus.remove_route_rule("missing") is False
    await bus.add_route_rule(object())
    assert await bus.get_subscriptions("receiver") == []
    assert await bus.get_message_history() == []
    assert await bus.get_delivery_status("missing") == []
    await bus.stop()
    assert redis.pubsub_obj.closed is True


@pytest.mark.asyncio
async def test_distributed_queue_reclaims_filters_cancels_and_sizes():
    redis = FakeRedis()
    queue = DistributedTaskQueue(redis, ttl_seconds=60)
    task = Task(task_id="task-1", agent_id="agent", type="work", tenant_id="tenant-a")

    await queue.enqueue(task)
    assert await queue.size() == 1
    assert (await queue.peek()).task_id == "task-1"

    dequeued = await queue.dequeue(worker_id="worker-a", lease_seconds=0.01)
    assert dequeued is not None
    assert dequeued.status == TaskStatus.RUNNING

    # Wait for the short lease to expire so reclaim can find it.
    await asyncio.sleep(0.05)
    reclaimed = await queue.reclaim_expired_tasks()
    assert reclaimed == ["task-1"]
    assert (await queue.get_task("task-1")).retry_count == 1

    tasks = await queue.list_tasks(TaskFilterOptions(tenant_id="tenant-a"))
    assert [t.task_id for t in tasks] == ["task-1"]
    assert await queue.cancel("task-1", tenant_id="tenant-b") is False
    assert await queue.cancel("task-1", tenant_id="tenant-a") is True
    assert (await queue.get_stats("tenant-a")).cancelled == 1

    redis.set_results.append(False)
    await queue.enqueue(Task(task_id="task-1", agent_id="agent", type="work"))
    assert await queue.size() == 0


@pytest.mark.asyncio
async def test_distributed_lock_waits_refreshes_and_handles_failed_release():
    redis = FakeRedis()
    lock = DistributedLock(redis, "critical", ttl_seconds=5)

    redis.set_results.extend([False, True])
    assert await lock.acquire(wait_timeout=1.0) is True
    assert lock.is_locked is True
    assert await lock.refresh() is True

    redis.eval_result = 0
    assert await lock.release() is False
    redis.eval_result = 1
    assert await lock.release() is True
    assert lock.is_locked is False
