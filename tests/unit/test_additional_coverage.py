# Additional coverage tests for low-hit branches

import asyncio
from collections import defaultdict

import pytest

from src.agent_platform.core.message import Message, MessageType
from src.agent_platform.core.task import Task, TaskPriority, TaskStatus
from src.agent_platform.message_bus.models import RouteRule
from src.agent_platform.message_bus.exceptions import MessageValidationError
from src.agent_platform.message_bus.redis_bus import RedisMessageBus
from src.agent_platform.message_bus.validator import MessageValidator
from src.agent_platform.scheduler.models import TaskFilterOptions
from src.agent_platform.scheduler.redis_queue import RedisTaskQueue
from src.agent_platform.workflow.executor import WorkflowExecutor
from src.agent_platform.workflow.exceptions import WorkflowExecutionError
from src.agent_platform.workflow.models import (
    StepDependency,
    StepStatus,
    Workflow,
    WorkflowStatus,
    WorkflowStep,
)
from src.agent_platform.workflow.state import WorkflowStateManager


class FakePubSub:
    def __init__(self):
        self.channels = set()
        self.closed = False
        self._queue = asyncio.Queue()

    async def connect(self):
        return None

    async def subscribe(self, *channels):
        self.channels.update(channels)

    async def unsubscribe(self, *channels):
        if channels:
            self.channels.difference_update(channels)
        else:
            self.channels.clear()

    async def get_message(self, ignore_subscribe_messages=True, timeout=1.0):
        try:
            return await asyncio.wait_for(self._queue.get(), timeout=timeout)
        except asyncio.TimeoutError:
            return None

    async def aclose(self):
        self.closed = True


class FakeRedisClient:
    def __init__(self):
        self.kv = {}
        self.zsets = defaultdict(dict)
        self.lists = defaultdict(list)
        self.published = []
        self.pubsub_instance = FakePubSub()

    def pubsub(self):
        return self.pubsub_instance

    async def flushall(self):
        self.kv.clear()
        self.zsets.clear()
        self.lists.clear()
        self.published.clear()

    async def aclose(self):
        return None

    async def set(self, key, value, nx=False, ex=None):
        if nx and key in self.kv:
            return False
        self.kv[key] = value.encode("utf-8") if isinstance(value, str) else value
        return True

    async def setex(self, key, ttl, value):
        return await self.set(key, value, ex=ttl)

    async def get(self, key):
        return self.kv.get(key)

    async def exists(self, key):
        return 1 if key in self.kv else 0

    async def delete(self, *keys):
        removed = 0
        for key in keys:
            if key in self.kv:
                del self.kv[key]
                removed += 1
        return removed

    async def lpush(self, key, *values):
        bucket = self.lists[key]
        for value in values:
            bucket.insert(0, value)
        return len(bucket)

    async def rpop(self, key):
        bucket = self.lists.get(key, [])
        if not bucket:
            return None
        value = bucket.pop()
        return value.encode("utf-8") if isinstance(value, str) else value

    async def publish(self, channel, message):
        self.published.append((channel, message))
        if channel in self.pubsub_instance.channels:
            await self.pubsub_instance._queue.put(
                {"type": "message", "channel": channel, "data": message}
            )
        return 1

    async def zadd(self, key, mapping):
        self.zsets[key].update(mapping)
        return len(mapping)

    async def zpopmin(self, key, count=1):
        bucket = self.zsets.get(key, {})
        items = sorted(bucket.items(), key=lambda item: item[1])[:count]
        for member, _ in items:
            bucket.pop(member, None)
        return [(member.encode("utf-8"), score) for member, score in items]

    async def zrange(self, key, start, end, withscores=False):
        bucket = self.zsets.get(key, {})
        items = sorted(bucket.items(), key=lambda item: item[1])
        sliced = items[start : None if end == -1 else end + 1]
        if withscores:
            return [(member.encode("utf-8"), score) for member, score in sliced]
        return [member.encode("utf-8") for member, _ in sliced]

    async def zrem(self, key, *members):
        bucket = self.zsets.get(key, {})
        removed = 0
        for member in members:
            if member in bucket:
                del bucket[member]
                removed += 1
        return removed

    async def zcard(self, key):
        return len(self.zsets.get(key, {}))

    async def scan(self, cursor=0, match=None, count=100):
        keys = list(self.kv.keys())
        if match:
            import fnmatch

            keys = [key for key in keys if fnmatch.fnmatch(key, match)]
        return 0, keys


def test_message_validator_branch_coverage():
    valid_event = Message(
        from_agent="a1",
        type=MessageType.EVENT,
        content={"event": "ok"},
    )
    valid_command = Message(
        from_agent="a1",
        to_agent="a2",
        type=MessageType.COMMAND,
        content={"cmd": "run"},
    )
    assert MessageValidator.is_valid(valid_event) is True
    assert MessageValidator.is_valid(valid_command) is True

    invalid_cases = [
        (Message.model_construct(from_agent="", type=MessageType.EVENT, content={}), "from_agent is required"),
        (Message.model_construct(from_agent="a1", type=None, content={}), "message type is required"),
        (
            Message(
                from_agent="a1",
                to_agent="a2",
                type=MessageType.REQUEST,
                content={},
                correlation_id="c1",
            ),
            "Request messages should have content",
        ),
        (
            Message(
                from_agent="a1",
                to_agent="a2",
                type=MessageType.REQUEST,
                content={"x": 1},
            ),
            "correlation_id is required for requests",
        ),
        (
            Message(
                from_agent="a1",
                to_agent="a2",
                type=MessageType.RESPONSE,
                content={"x": 1},
            ),
            "correlation_id is required for responses",
        ),
        (
            Message(
                from_agent="a1",
                to_agent="a2",
                type=MessageType.BROADCAST,
                content={"x": 1},
            ),
            "Broadcast messages should not have a specific target",
        ),
        (
            Message(
                from_agent="a1",
                type=MessageType.EVENT,
                content={},
            ),
            "Event messages should have content",
        ),
        (
            Message.model_construct(
                from_agent="a1",
                to_agent="a2",
                type=MessageType.COMMAND,
                content={},
            ),
            "Command messages should have content",
        ),
        (
            Message(
                from_agent="a1",
                type=MessageType.BROADCAST,
                content={"x": 1},
                ttl_seconds=0,
            ),
            "TTL must be positive",
        ),
    ]

    for message, expected in invalid_cases:
        with pytest.raises(MessageValidationError) as excinfo:
            MessageValidator.validate(message)
        assert expected in str(excinfo.value)
        assert MessageValidator.is_valid(message) is False


@pytest.mark.asyncio
async def test_workflow_state_round_trip_and_dependencies():
    workflow = Workflow(
        workflow_id="wf",
        name="State Test",
        steps=[
            WorkflowStep(step_id="s1", name="Step 1", agent_id="a1", task_type="test", payload={}),
            WorkflowStep(
                step_id="s2",
                name="Step 2",
                agent_id="a2",
                task_type="test",
                payload={},
                dependencies=[StepDependency(depends_on="s1", condition="result != None")],
            ),
        ],
    )
    state = WorkflowStateManager(workflow)
    state.start()
    state.set_step_status("s1", StepStatus.COMPLETED)

    assert state.get_ready_steps() == []
    state.set_step_result("s1", "done")
    assert state.get_ready_steps() == ["s2"]

    snapshot = state.to_dict()
    restored = WorkflowStateManager.from_dict(snapshot, workflow)

    assert restored.workflow_status == WorkflowStatus.RUNNING
    assert restored.get_step_status("s1") == StepStatus.COMPLETED
    assert restored.to_dict()["workflow_id"] == "wf"


@pytest.mark.asyncio
async def test_workflow_executor_rejects_completed_workflow():
    workflow = Workflow(
        workflow_id="wf-complete",
        name="Completed",
        steps=[WorkflowStep(step_id="s1", name="Step 1", agent_id="a1", task_type="test", payload={})],
    )
    state = WorkflowStateManager(workflow)
    state.workflow_status = WorkflowStatus.COMPLETED
    executor = WorkflowExecutor(object(), state)

    with pytest.raises(WorkflowExecutionError):
        await executor.execute()


@pytest.mark.asyncio
async def test_redis_task_queue_branch_coverage():
    redis_client = FakeRedisClient()
    queue = RedisTaskQueue(redis_client)

    assert await queue.dequeue() is None
    assert await queue.peek() is None
    assert await queue.get_task("missing") is None
    assert await queue.cancel("missing") is False

    low = Task(task_id="low", agent_id="a1", type="test", priority=TaskPriority.LOW)
    high = Task(task_id="high", agent_id="a2", type="test", priority=TaskPriority.HIGH, tenant_id="tenant")
    await queue.enqueue(low)
    await queue.enqueue(high)

    peeked = await queue.peek()
    assert peeked is not None
    assert peeked.task_id == "high"

    first = await queue.dequeue()
    assert first is not None
    assert first.task_id == "high"
    assert first.status == TaskStatus.RUNNING

    second = await queue.dequeue()
    assert second is not None
    assert second.task_id == "low"

    completed = Task(
        task_id="done",
        agent_id="a1",
        type="test",
        status=TaskStatus.COMPLETED,
        result="ok",
    )
    await queue.update_task(completed)
    assert await queue.cancel("done") is False

    tenant_task = Task(
        task_id="tenant-task",
        agent_id="a2",
        type="test",
        status=TaskStatus.PENDING,
        tenant_id="tenant",
    )
    await queue.update_task(tenant_task)
    assert await queue.cancel("tenant-task", tenant_id="wrong") is False

    filtered = await queue.list_tasks(
        TaskFilterOptions(agent_id="a2", tenant_id="tenant", status=TaskStatus.PENDING)
    )
    assert [task.task_id for task in filtered] == ["tenant-task"]

    stats = await queue.get_stats(tenant_id="tenant")
    assert stats.total == 2
    assert stats.pending == 1
    assert stats.running == 1

    stored = await queue.get_task("tenant-task", tenant_id="tenant")
    assert stored is not None
    assert stored.task_id == "tenant-task"
    assert await queue.get_task("tenant-task", tenant_id="other") is None


@pytest.mark.asyncio
async def test_redis_message_bus_branch_coverage():
    redis_client = FakeRedisClient()
    bus = RedisMessageBus(redis_client)

    await bus.start()

    received = []

    async def handler(msg):
        received.append(msg)

    sub_id = await bus.subscribe("agent-1", handler, topics=["alerts"])
    assert sub_id == "sub-agent-1"
    assert "agent-1" in bus._handlers

    msg = Message(
        from_agent="sender",
        to_agent="agent-1",
        type=MessageType.REQUEST,
        content={"ping": "pong"},
        correlation_id="corr-1",
    )
    msg_id = await bus.send(msg)
    assert msg_id == msg.message_id
    assert await bus.get_message(msg_id) is not None
    assert await bus.acknowledge(msg_id, "agent-1") is True

    broadcast = Message(
        from_agent="sender",
        type=MessageType.BROADCAST,
        content={"info": "hello"},
    )
    assert await bus.broadcast(broadcast) == [broadcast.message_id]
    assert await bus.publish("alerts", broadcast) == broadcast.message_id
    assert redis_client.published[0][0] == "msgbus:broadcast"
    assert redis_client.published[1][0] == "msgbus:topic:alerts"

    rule = RouteRule(
        rule_id="r1",
        name="Rule",
        conditions={"type": "event"},
        target_agents=["agent-1"],
        priority=1,
    )
    assert await bus.route_by_role(broadcast) == []
    await bus.add_route_rule(rule)
    assert await bus.remove_route_rule("r1") is False
    assert await bus.get_subscriptions("agent-1") == []

    assert await bus.unsubscribe("agent-1") is True
    await bus.stop()
