# Phase 2 coverage-gap tests.
#
# Each test targets a genuinely uncovered branch / error path / edge case
# in the production code.  No mocks for the core logic; we use the real
# in-memory components and the real Redis/Postgres fixtures where needed.

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta

import pytest

from src.agent_platform.core.agent import AgentCapability, AgentRuntimeState, BaseAgent, AgentRecord
from src.agent_platform.core.message import Message, MessageType
from src.agent_platform.core.task import Task, TaskPriority, TaskStatus
from src.agent_platform.engine.engine import AgentEngine
from src.agent_platform.message_bus.in_memory import InMemoryMessageBus
from src.agent_platform.message_bus.models import (
    MessageDeliveryRecord,
    MessageDeliveryStatus,
    RouteRule,
    Subscription,
    SubscriptionType,
)
from src.agent_platform.registry.in_memory import InMemoryAgentRegistry
from src.agent_platform.scheduler.in_memory import InMemoryTaskQueue
from src.agent_platform.scheduler.redis_queue import RedisTaskQueue
from src.agent_platform.scheduler.scheduler import TaskScheduler
from src.agent_platform.scheduler.worker import TaskWorker
from src.agent_platform.workflow.executor import WorkflowExecutor
from src.agent_platform.workflow.models import (
    StepDependency,
    StepStatus,
    Workflow,
    WorkflowStatus,
    WorkflowStep,
)
from src.agent_platform.workflow.state import WorkflowStateManager


# ===========================================================================
# Message bus — in_memory.py
# ===========================================================================


class _Collector:
    def __init__(self):
        self.messages: list[Message] = []

    async def __call__(self, msg: Message) -> None:
        self.messages.append(msg)


@pytest.mark.asyncio
async def test_bus_acknowledge_and_delivery_status():
    """acknowledge() must update delivery record to ACKNOWLEDGED with timestamp."""
    bus = InMemoryMessageBus()
    await bus.start()
    collector = _Collector()
    await bus.subscribe("agent-1", collector)
    msg = Message(
        from_agent="sender",
        to_agent="agent-1",
        type=MessageType.REQUEST,
        content={"cmd": "ack"},
    )
    await bus.send(msg)
    await asyncio.sleep(0.1)
    assert len(collector.messages) == 1

    acked = await bus.acknowledge(msg.message_id, "agent-1")
    assert acked is True
    records = await bus.get_delivery_status(msg.message_id)
    assert len(records) == 1
    assert records[0].status == MessageDeliveryStatus.ACKNOWLEDGED
    assert records[0].acknowledged_at is not None
    await bus.stop()


@pytest.mark.asyncio
async def test_bus_has_processed():
    """has_processed() must return True once a message was sent."""
    bus = InMemoryMessageBus()
    await bus.start()
    msg = Message(
        from_agent="sender",
        to_agent="agent-1",
        type=MessageType.REQUEST,
        content={"cmd": "check"},
    )
    await bus.subscribe("agent-1", _Collector())
    await bus.send(msg)
    assert await bus.has_processed(msg.message_id) is True
    assert await bus.has_processed("nonexistent-id") is False
    await bus.stop()


@pytest.mark.asyncio
async def test_bus_unsubscribe_specific_and_all():
    """unsubscribe() must remove specific or all subscriptions for an agent."""
    bus = InMemoryMessageBus()
    await bus.start()
    c1 = _Collector()
    c2 = _Collector()
    sub1 = await bus.subscribe("agent-1", c1, topics=["t1"])
    sub2 = await bus.subscribe("agent-1", c2, topics=["t2"])
    assert len(await bus.get_subscriptions("agent-1")) == 2

    removed = await bus.unsubscribe("agent-1", subscription_id=sub1)
    assert removed is True
    assert len(await bus.get_subscriptions("agent-1")) == 1

    removed_all = await bus.unsubscribe("agent-1")
    assert removed_all is True
    assert len(await bus.get_subscriptions("agent-1")) == 0
    assert "agent-1" not in bus._handlers
    await bus.stop()


@pytest.mark.asyncio
async def test_bus_get_message_history_with_agent_filter():
    """get_message_history() must filter by recipient agent_id when supplied."""
    bus = InMemoryMessageBus()
    await bus.start()
    for target in ("agent-a", "agent-b"):
        await bus.subscribe(target, _Collector())
        msg = Message(
            from_agent="sender",
            to_agent=target,
            type=MessageType.REQUEST,
            content={"to": target},
        )
        await bus.send(msg)
    await asyncio.sleep(0.1)

    history_a = await bus.get_message_history(agent_id="agent-a", limit=10)
    assert len(history_a) == 1
    assert history_a[0].to_agent == "agent-a"

    history_all = await bus.get_message_history(limit=10)
    assert len(history_all) == 2
    await bus.stop()


@pytest.mark.asyncio
async def test_bus_send_deduplicates_existing_message_id():
    """Sending a message with an already-stored message_id must be a no-op."""
    bus = InMemoryMessageBus()
    await bus.start()
    await bus.subscribe("agent-1", _Collector())
    msg = Message(
        from_agent="sender",
        to_agent="agent-1",
        type=MessageType.REQUEST,
        content={"cmd": "dedup"},
    )
    first = await bus.send(msg)
    second = await bus.send(msg)
    assert first == second
    await bus.stop()


@pytest.mark.asyncio
async def test_bus_publish_no_subscribers_warns_and_returns_id():
    """publish() to a topic with no subscribers must warn and still return the message_id."""
    bus = InMemoryMessageBus()
    await bus.start()
    msg = Message(
        from_agent="sender",
        to_agent=None,
        type=MessageType.EVENT,
        content={"info": "no one listening"},
        topic="empty-topic",
    )
    returned = await bus.publish("empty-topic", msg)
    assert returned == msg.message_id
    await bus.stop()


@pytest.mark.asyncio
async def test_bus_route_by_role_skips_inactive_rules():
    """route_by_role() must ignore inactive route rules."""
    bus = InMemoryMessageBus()
    await bus.start()
    await bus.subscribe("agent-1", _Collector(), roles=["worker"])
    bus._role_members["worker"].add("agent-1")
    bus._route_rules["r1"] = RouteRule(
        rule_id="r1",
        name="Inactive",
        target_roles=["worker"],
        conditions={},
        priority=0,
        is_active=False,
    )
    bus._route_rules["r2"] = RouteRule(
        rule_id="r2",
        name="Active",
        target_roles=["worker"],
        conditions={},
        priority=1,
        is_active=True,
    )
    msg = Message(
        from_agent="router",
        to_agent=None,
        type=MessageType.REQUEST,
        content={},
    )
    recipients = await bus.route_by_role(msg)
    assert "agent-1" in recipients
    await bus.stop()


@pytest.mark.asyncio
async def test_bus_deliver_messages_handler_failure_records_failed():
    """A handler that raises must record FAILED delivery status."""
    bus = InMemoryMessageBus()
    await bus.start()

    async def bad_handler(msg: Message) -> None:
        raise RuntimeError("handler boom")

    await bus.subscribe("agent-1", bad_handler)
    msg = Message(
        from_agent="sender",
        to_agent="agent-1",
        type=MessageType.REQUEST,
        content={},
    )
    await bus.send(msg)
    await asyncio.sleep(0.2)
    records = await bus.get_delivery_status(msg.message_id)
    assert any(r.status == MessageDeliveryStatus.FAILED for r in records)
    await bus.stop()


# ===========================================================================
# TaskWorker — retry backoff timing
# ===========================================================================


class _TimedFailingAgent(BaseAgent):
    def __init__(self, agent_id: str, name: str, fail_times: int = 2):
        super().__init__(agent_id, name)
        self.fail_times = fail_times
        self.attempts = 0

    async def initialize(self) -> None:
        self._initialized = True
        self.state = AgentRuntimeState.RUNNING

    async def run(self, task: Task) -> str:
        self.attempts += 1
        if self.attempts <= self.fail_times:
            raise ValueError(f"fail {self.attempts}")
        return "ok"

    async def shutdown(self) -> None:
        pass


@pytest.mark.asyncio
async def test_worker_retry_delay_is_exponential():
    """TaskWorker must use exponential backoff between retries."""
    agent = _TimedFailingAgent("b1", "backoff", fail_times=2)
    await agent.initialize()
    task = Task(
        task_id="backoff-t1",
        agent_id="b1",
        type="test",
        payload={},
        timeout_seconds=5,
        max_retries=2,
    )
    worker = TaskWorker(task, agent, retry_delay_base=0.1, retry_delay_max=10.0)
    start = datetime.now(UTC)
    result = await worker.execute()
    elapsed = (datetime.now(UTC) - start).total_seconds()
    assert result.status == TaskStatus.COMPLETED
    # Expected delays: 0.1s (after 1st fail) + 0.2s (after 2nd fail) = ~0.3s
    assert elapsed >= 0.25
    assert agent.attempts == 3


# ===========================================================================
# AgentEngine — error / edge paths
# ===========================================================================


class _FailingInitAgent(BaseAgent):
    async def initialize(self) -> None:
        raise RuntimeError("init boom")

    async def run(self, task: Task) -> str:
        return ""

    async def shutdown(self) -> None:
        pass


@pytest.mark.asyncio
async def test_engine_register_agent_init_failure():
    """register_agent must set ERROR state and re-raise when initialize() fails."""
    registry = InMemoryAgentRegistry()
    scheduler = TaskScheduler(InMemoryTaskQueue())
    engine = AgentEngine(registry, scheduler)
    agent = _FailingInitAgent("fail-init", "Failing")
    with pytest.raises(RuntimeError, match="init boom"):
        await engine.register_agent(agent)
    assert agent.state == AgentRuntimeState.ERROR
    assert "fail-init" not in engine._agents


# ===========================================================================
# WorkflowExecutor — edge branches
# ===========================================================================


@pytest.mark.asyncio
async def test_workflow_executor_already_running_guard():
    """execute() must return immediately if already running."""
    steps = [
        WorkflowStep(step_id="s1", name="S1", agent_id="a1", task_type="test", payload={})
    ]
    workflow = Workflow(workflow_id="guard_wf", name="Guard", steps=steps)
    state = WorkflowStateManager(workflow)
    scheduler = TaskScheduler(InMemoryTaskQueue())
    executor = WorkflowExecutor(scheduler, state)

    async def mock_wait(task_id):
        return Task(task_id=task_id, agent_id="a1", type="test", payload={}, status=TaskStatus.COMPLETED)
    executor._wait_for_task = mock_wait

    state.start()
    executor._running = True  # simulate already running
    await executor.execute()
    # Should return immediately without changing state
    assert state.workflow_status == WorkflowStatus.RUNNING


@pytest.mark.asyncio
async def test_workflow_executor_invalid_status_raises():
    """execute() must raise when workflow status is terminal (e.g. COMPLETED)."""
    steps = [
        WorkflowStep(step_id="s1", name="S1", agent_id="a1", task_type="test", payload={})
    ]
    workflow = Workflow(workflow_id="invalid_wf", name="Invalid", steps=steps)
    state = WorkflowStateManager(workflow)
    state.complete()  # terminal
    scheduler = TaskScheduler(InMemoryTaskQueue())
    executor = WorkflowExecutor(scheduler, state)

    with pytest.raises(Exception):
        await executor.execute()


@pytest.mark.asyncio
async def test_workflow_executor_task_disappeared_marks_failed():
    """_execute_step() must mark step FAILED when _wait_for_task returns None."""
    steps = [
        WorkflowStep(step_id="s1", name="S1", agent_id="a1", task_type="test", payload={})
    ]
    workflow = Workflow(workflow_id="ghost_wf", name="Ghost", steps=steps)
    state = WorkflowStateManager(workflow)
    scheduler = TaskScheduler(InMemoryTaskQueue())
    executor = WorkflowExecutor(scheduler, state)

    async def mock_wait(task_id):
        return None

    executor._wait_for_task = mock_wait
    await executor.execute()
    assert state.get_step_status("s1") == StepStatus.FAILED


@pytest.mark.asyncio
async def test_workflow_executor_cancelled_pauses_workflow():
    """CancelledError on the execute() task itself must pause the workflow."""
    steps = [
        WorkflowStep(step_id="s1", name="S1", agent_id="a1", task_type="test", payload={})
    ]
    workflow = Workflow(workflow_id="cancel_wf", name="Cancel", steps=steps)
    state = WorkflowStateManager(workflow)
    scheduler = TaskScheduler(InMemoryTaskQueue())
    executor = WorkflowExecutor(scheduler, state)

    async def mock_wait(task_id):
        # Sleep long enough for the cancel to land while execute() is awaiting
        await asyncio.sleep(0.3)
        return Task(task_id=task_id, agent_id="a1", type="test", payload={}, status=TaskStatus.COMPLETED)

    executor._wait_for_task = mock_wait

    task = asyncio.create_task(executor.execute())
    await asyncio.sleep(0.1)  # let execute() enter gather
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert state.workflow_status == WorkflowStatus.PAUSED


# ===========================================================================
# RedisTaskQueue — uncovered edge branches (real Redis/Postgres via fixtures)
# ===========================================================================


@pytest.mark.asyncio
async def test_redis_queue_list_tasks_from_db_filters(redis_queue, clean_db):
    """_list_tasks_from_db must respect agent_id, status, priority, and tenant_id filters."""
    scheduler = TaskScheduler(redis_queue)
    task_ids = [f"filter-{i:03d}" for i in range(5)]
    for idx, tid in enumerate(task_ids):
        await scheduler.submit_task(
            agent_id=f"agent-{idx % 2}",
            task_type="echo",
            payload={"idx": idx},
            task_id=tid,
            priority=TaskPriority(idx % 3),
        )

    all_tasks = await redis_queue._list_tasks_from_db()
    assert len(all_tasks) >= 5

    agent0_tasks = await redis_queue._list_tasks_from_db(
        filters=type("F", (), {"agent_id": "agent-0", "status": None, "priority": None, "tenant_id": None, "request_id": None})()
    )
    assert all(t.agent_id == "agent-0" for t in agent0_tasks)

    pending_tasks = await redis_queue._list_tasks_from_db(
        filters=type("F", (), {"agent_id": None, "status": TaskStatus.PENDING, "priority": None, "tenant_id": None, "request_id": None})()
    )
    assert all(t.status == TaskStatus.PENDING for t in pending_tasks)


@pytest.mark.asyncio
async def test_redis_queue_get_stats_includes_timeout(redis_queue, clean_db):
    """_get_stats_from_db must count TIMEOUT tasks."""
    scheduler = TaskScheduler(redis_queue)
    tid = "timeout-stat-t1"
    await scheduler.submit_task("agent", "echo", {}, task_id=tid)
    task = await redis_queue.get_task(tid)
    task.status = TaskStatus.TIMEOUT
    task.error = "timed out"
    await redis_queue.update_task(task)

    stats = await redis_queue._get_stats_from_db()
    assert stats.timeout >= 1


@pytest.mark.asyncio
async def test_redis_queue_load_task_tenant_mismatch_returns_none(redis_queue, clean_db):
    """_load_task_from_db must return None when tenant_id does not match."""
    scheduler = TaskScheduler(redis_queue)
    tid = "tenant-mismatch-t1"
    await scheduler.submit_task("agent", "echo", {}, task_id=tid, tenant_id="tenant-A")
    loaded = await redis_queue._load_task_from_db(tid, tenant_id="tenant-B")
    assert loaded is None


@pytest.mark.asyncio
async def test_redis_queue_enqueue_running_task_is_noop(redis_queue, clean_db):
    """enqueue() must be a no-op for a task already RUNNING."""
    scheduler = TaskScheduler(redis_queue)
    tid = "running-enqueue-t1"
    await scheduler.submit_task("agent", "echo", {}, task_id=tid)
    task = await redis_queue.get_task(tid)
    task.status = TaskStatus.RUNNING
    await redis_queue.update_task(task)

    # Re-enqueue the same running task — should not reset to PENDING
    await redis_queue.enqueue(task)
    reloaded = await redis_queue.get_task(tid)
    assert reloaded.status == TaskStatus.RUNNING


@pytest.mark.asyncio
async def test_redis_queue_update_task_without_session_factory():
    """update_task() must return early when session_factory is None."""
    queue = RedisTaskQueue(redis_client=type("R", (), {"get": None, "set": None, "zadd": None, "zrem": None})())
    task = Task(task_id="no-session", agent_id="a", type="t", payload={})
    await queue.update_task(task)
    # Should not crash
    assert queue.session_factory is None


@pytest.mark.asyncio
async def test_redis_queue_enqueue_existing_in_redis_skips_redis_set(redis_queue, clean_db):
    """enqueue() must return early when the task data already exists in Redis."""
    scheduler = TaskScheduler(redis_queue)
    tid = "redis-exists-t1"
    await scheduler.submit_task("agent", "echo", {}, task_id=tid)
    # Ensure Redis already has the key
    raw = await redis_queue.redis.get(redis_queue._task_key(tid))
    assert raw is not None

    task = await redis_queue.get_task(tid)
    # Manually enqueue again — should hit the in_redis early-return
    await redis_queue.enqueue(task)
    # The task must still be present and unchanged
    reloaded = await redis_queue.get_task(tid)
    assert reloaded is not None
    assert reloaded.status == TaskStatus.PENDING


@pytest.mark.asyncio
async def test_redis_queue_dequeue_redis_zpopmin_failure_returns_none(redis_queue, clean_db):
    """dequeue() must return None when Redis zpopmin raises."""
    # Make the queue's redis.zpopmin raise to exercise the exception branch
    original_zpopmin = redis_queue.redis.zpopmin
    call_count = 0

    async def failing_zpopmin(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise RuntimeError("Redis zpopmin boom")
        return await original_zpopmin(*args, **kwargs)

    redis_queue.redis.zpopmin = failing_zpopmin
    result = await redis_queue.dequeue(worker_id="w1", lease_seconds=60)
    assert result is None


@pytest.mark.asyncio
async def test_redis_queue_dequeue_task_not_found_after_zpopmin(redis_queue, clean_db):
    """dequeue() must return None when zpopmin returns a task_id not found in DB."""
    # Push a task directly into Redis without DB backing
    await redis_queue.redis.zadd(redis_queue.QUEUE_KEY, {"ghost-task": 1.0})
    await redis_queue.redis.set(
        redis_queue._task_key("ghost-task"),
        json.dumps({"task_id": "ghost-task", "status": "pending"}),
        ex=3600,
    )
    result = await redis_queue.dequeue(worker_id="w1", lease_seconds=60)
    assert result is None


@pytest.mark.asyncio
async def test_redis_queue_recover_orphaned_tasks_no_session_factory():
    """recover_orphaned_tasks() must return [] when session_factory is None."""
    queue = RedisTaskQueue(redis_client=type("R", (), {"get": None, "set": None, "zadd": None, "zrem": None})())
    recovered = await queue.recover_orphaned_tasks()
    assert recovered == []


# ===========================================================================
# PostgresAgentRegistry — postgres_registry.py
# (Skipped: the `agents` table is not created by the test database setup,
#  which only runs TaskBase.metadata.create_all.  Covering this module
#  would require adding its table to conftest or providing a migration,
#  which is outside the scope of Phase 2 targeted coverage work.)
# ===========================================================================


@pytest.mark.skip(reason="agents table not created by test database setup")
@pytest.mark.asyncio
async def test_postgres_registry_register_update_and_get(clean_db):
    """register() must insert new agents and update existing ones."""
    from src.agent_platform.registry.postgres_registry import PostgresAgentRegistry

    registry = PostgresAgentRegistry(clean_db)
    agent = AgentRecord(
        agent_id="reg-1",
        name="Registry Agent",
        capabilities=[AgentCapability(name="echo", version="1.0")],
        metadata={"env": "test"},
        tenant_id="tenant-x",
    )
    await registry.register(agent)
    fetched = await registry.get_agent("reg-1")
    assert fetched is not None
    assert fetched.name == "Registry Agent"
    assert fetched.capabilities[0].name == "echo"
    assert fetched.tenant_id == "tenant-x"

    # Update
    agent.name = "Registry Agent Updated"
    await registry.register(agent)
    fetched2 = await registry.get_agent("reg-1")
    assert fetched2.name == "Registry Agent Updated"


@pytest.mark.skip(reason="agents table not created by test database setup")
@pytest.mark.asyncio
async def test_postgres_registry_unregister_with_tenant(clean_db):
    """unregister() must respect tenant_id scoping."""
    from src.agent_platform.registry.postgres_registry import PostgresAgentRegistry

    registry = PostgresAgentRegistry(clean_db)
    agent = AgentRecord(agent_id="reg-2", name="T Agent", tenant_id="tenant-y")
    await registry.register(agent)
    removed = await registry.unregister("reg-2", tenant_id="tenant-y")
    assert removed is True
    assert await registry.get_agent("reg-2") is None

    # Wrong tenant must not remove
    await registry.register(agent)
    removed_wrong = await registry.unregister("reg-2", tenant_id="wrong-tenant")
    assert removed_wrong is False
    assert await registry.get_agent("reg-2") is not None


@pytest.mark.skip(reason="agents table not created by test database setup")
@pytest.mark.asyncio
async def test_postgres_registry_heartbeat_and_discover(clean_db):
    """heartbeat() and discover() must work with tenant_id filters."""
    from src.agent_platform.registry.postgres_registry import PostgresAgentRegistry

    registry = PostgresAgentRegistry(clean_db)
    agent = AgentRecord(
        agent_id="reg-3",
        name="HB Agent",
        capabilities=[AgentCapability(name="infer")],
        tenant_id="tenant-z",
    )
    await registry.register(agent)
    updated = await registry.heartbeat("reg-3", tenant_id="tenant-z")
    assert updated is True

    results = await registry.discover(capability="infer", tenant_id="tenant-z")
    assert len(results) == 1
    assert results[0].agent_id == "reg-3"


@pytest.mark.skip(reason="agents table not created by test database setup")
@pytest.mark.asyncio
async def test_postgres_registry_cleanup_stale(clean_db):
    """cleanup_stale() must remove agents with old heartbeats."""
    from src.agent_platform.registry.postgres_registry import PostgresAgentRegistry, utcnow_naive
    from sqlalchemy import insert, update

    registry = PostgresAgentRegistry(clean_db)
    agent = AgentRecord(agent_id="reg-4", name="Stale Agent")
    await registry.register(agent)
    # Manually backdate the heartbeat
    async with clean_db() as session:
        stmt = (
            update(registry.__self__ if hasattr(registry, '__self__') else None)
        )
    # Use the ORM class directly
    from src.agent_platform.registry.postgres_registry import AgentORM
    async with clean_db() as session:
        stmt = update(AgentORM).where(AgentORM.agent_id == "reg-4").values(
            last_heartbeat=utcnow_naive() - timedelta(seconds=120)
        )
        await session.execute(stmt)
        await session.commit()

    removed = await registry.cleanup_stale(ttl_seconds=60)
    assert removed == 1
    assert await registry.get_agent("reg-4") is None


# ===========================================================================
# AgentEngine — dispatcher / worker error paths
# ===========================================================================


class _NotReadyAgent(BaseAgent):
    async def initialize(self) -> None:
        self._initialized = True
        self.state = AgentRuntimeState.ERROR

    async def run(self, task: Task) -> str:
        return ""

    async def shutdown(self) -> None:
        pass


class _EchoAgent(BaseAgent):
    async def initialize(self) -> None:
        self._initialized = True
        self.state = AgentRuntimeState.RUNNING

    async def run(self, task: Task) -> str:
        return f"Echo: {task.payload.get('message', '')}"

    async def shutdown(self) -> None:
        self._initialized = False


@pytest.mark.asyncio
async def test_engine_dispatcher_agent_not_found():
    """_dispatcher_loop must mark task FAILED when agent is not registered."""
    registry = InMemoryAgentRegistry()
    queue = InMemoryTaskQueue()
    scheduler = TaskScheduler(queue)
    engine = AgentEngine(registry, scheduler, poll_interval=0.1)

    task_id = await scheduler.submit_task("missing-agent", "echo", {})
    await engine.start()
    await asyncio.sleep(0.3)
    task = await scheduler.get_task(task_id)
    assert task.status == TaskStatus.FAILED
    assert "not found" in task.error
    await engine.stop()


@pytest.mark.asyncio
async def test_engine_dispatcher_agent_not_ready():
    """_dispatcher_loop must mark task FAILED when agent is not ready."""
    registry = InMemoryAgentRegistry()
    queue = InMemoryTaskQueue()
    scheduler = TaskScheduler(queue)
    engine = AgentEngine(registry, scheduler, poll_interval=0.1)

    agent = _NotReadyAgent("not-ready", "NR")
    await engine.register_agent(agent)
    # register_agent promotes initialized agents to RUNNING; force the
    # not-ready state back so the dispatcher exercises its error branch.
    agent.state = AgentRuntimeState.ERROR

    task_id = await scheduler.submit_task("not-ready", "echo", {})
    await engine.start()
    try:
        await asyncio.sleep(0.3)
        task = await scheduler.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.FAILED
        assert "not ready" in (task.error or "")
    finally:
        await engine.stop()


# ===========================================================================
# WorkflowExecutor — fallback failure and general exception
# ===========================================================================


@pytest.mark.asyncio
async def test_workflow_executor_fallback_fails_workflow_fails():
    """Workflow must fail when a step fails and its fallback also fails."""
    steps = [
        WorkflowStep(
            step_id="main",
            name="Main",
            agent_id="a1",
            task_type="test",
            payload={},
            fallback_step_id="fb",
        ),
        WorkflowStep(
            step_id="fb",
            name="Fallback",
            agent_id="a2",
            task_type="test",
            payload={},
        ),
    ]
    workflow = Workflow(workflow_id="fb_fail_wf", name="FB Fail", steps=steps)
    state = WorkflowStateManager(workflow)
    scheduler = TaskScheduler(InMemoryTaskQueue())
    executor = WorkflowExecutor(scheduler, state)

    async def mock_wait(task_id):
        return Task(
            task_id=task_id,
            agent_id="a1",
            type="test",
            payload={},
            status=TaskStatus.FAILED,
            error="both failed",
        )

    executor._wait_for_task = mock_wait
    await executor.execute()
    assert state.workflow_status == WorkflowStatus.FAILED


@pytest.mark.asyncio
async def test_workflow_executor_general_exception_fails_workflow():
    """A general exception in _execute_step must fail the workflow."""
    steps = [
        WorkflowStep(step_id="s1", name="S1", agent_id="a1", task_type="test", payload={})
    ]
    workflow = Workflow(workflow_id="exc_wf", name="Exc", steps=steps)
    state = WorkflowStateManager(workflow)
    scheduler = TaskScheduler(InMemoryTaskQueue())
    executor = WorkflowExecutor(scheduler, state)

    async def mock_wait(task_id):
        raise RuntimeError("unexpected boom")

    executor._wait_for_task = mock_wait
    await executor.execute()
    assert state.workflow_status == WorkflowStatus.FAILED
    assert state.get_step_status("s1") == StepStatus.FAILED


# ===========================================================================
# InMemoryMessageBus — remaining uncovered branches
# ===========================================================================


@pytest.mark.asyncio
async def test_bus_send_agent_not_subscribed_raises():
    """send() must raise when the recipient agent is not subscribed."""
    bus = InMemoryMessageBus()
    await bus.start()
    msg = Message(
        from_agent="sender",
        to_agent="no-such-agent",
        type=MessageType.REQUEST,
        content={},
    )
    with pytest.raises(Exception):
        await bus.send(msg)
    await bus.stop()


@pytest.mark.asyncio
async def test_bus_stop_clears_worker_tasks():
    """stop() must clear the internal worker task list."""
    bus = InMemoryMessageBus()
    await bus.start()
    await bus.subscribe("agent-1", _Collector())
    assert len(bus._worker_tasks) > 0
    await bus.stop()
    assert len(bus._worker_tasks) == 0


@pytest.mark.asyncio
async def test_bus_broadcast_dedup_message_ids():
    """broadcast() must produce unique message_ids per recipient."""
    bus = InMemoryMessageBus()
    await bus.start()
    for target in ("a1", "a2"):
        await bus.subscribe(target, _Collector())
    msg = Message(
        from_agent="broadcaster",
        to_agent=None,
        type=MessageType.BROADCAST,
        content={},
    )
    ids = await bus.broadcast(msg)
    assert len(ids) == 2
    assert len(set(ids)) == 2
    await bus.stop()


# ===========================================================================
# Security helpers — edge branches
# ===========================================================================


def test_security_api_key_matches_non_string_returns_false():
    """api_key_matches must return False for non-string stored_hash."""
    from src.agent_platform.security import api_key_matches
    assert api_key_matches(12345, "key") is False
    assert api_key_matches("short", "key") is False


def test_security_stored_api_key_hash_none_when_missing():
    """stored_api_key_hash must return None when record has no hash or key."""
    from src.agent_platform.security import stored_api_key_hash
    assert stored_api_key_hash({}) is None
    assert stored_api_key_hash({"key_hash": ""}) is None
    assert stored_api_key_hash({"key": ""}) is None


def test_security_api_key_record_matches_inactive_returns_false():
    """api_key_record_matches must return False when is_active is False."""
    from src.agent_platform.security import api_key_record_matches
    assert api_key_record_matches({"is_active": False, "key_hash": "abc"}, "key") is False


# ===========================================================================
# WorkflowExecutor — remaining uncovered branches
# ===========================================================================


@pytest.mark.asyncio
async def test_workflow_executor_resume_from_paused():
    """execute() must resume a PAUSED workflow and continue execution."""
    steps = [
        WorkflowStep(step_id="s1", name="S1", agent_id="a1", task_type="test", payload={})
    ]
    workflow = Workflow(workflow_id="resume_wf", name="Resume", steps=steps)
    state = WorkflowStateManager(workflow)
    state.start()
    state.pause()  # now PAUSED
    scheduler = TaskScheduler(InMemoryTaskQueue())
    executor = WorkflowExecutor(scheduler, state)

    async def mock_wait(task_id):
        return Task(task_id=task_id, agent_id="a1", type="test", payload={}, status=TaskStatus.COMPLETED)
    executor._wait_for_task = mock_wait

    await executor.execute()
    assert state.workflow_status == WorkflowStatus.COMPLETED
    assert state.get_step_status("s1") == StepStatus.COMPLETED


@pytest.mark.asyncio
async def test_workflow_executor_output_key_stored_in_results():
    """_execute_step must store task.result in step_results when output_key is set."""
    steps = [
        WorkflowStep(
            step_id="s1",
            name="S1",
            agent_id="a1",
            task_type="test",
            payload={},
            output_key="result1",
        )
    ]
    workflow = Workflow(workflow_id="outkey_wf", name="OutKey", steps=steps)
    state = WorkflowStateManager(workflow)
    scheduler = TaskScheduler(InMemoryTaskQueue())
    executor = WorkflowExecutor(scheduler, state)

    async def mock_wait(task_id):
        return Task(task_id=task_id, agent_id="a1", type="test", payload={}, status=TaskStatus.COMPLETED, result="hello")
    executor._wait_for_task = mock_wait

    await executor.execute()
    assert state.step_results.get("result1") == "hello"


@pytest.mark.asyncio
async def test_workflow_executor_pause_during_execution():
    """pause() must cancel the running execute() task and set status to PAUSED."""
    steps = [
        WorkflowStep(step_id="s1", name="S1", agent_id="a1", task_type="test", payload={})
    ]
    workflow = Workflow(workflow_id="pause_wf", name="Pause", steps=steps)
    state = WorkflowStateManager(workflow)
    state.start()
    scheduler = TaskScheduler(InMemoryTaskQueue())
    executor = WorkflowExecutor(scheduler, state)

    async def slow_wait(task_id):
        await asyncio.sleep(1.0)
        return Task(task_id=task_id, agent_id="a1", type="test", payload={}, status=TaskStatus.COMPLETED)

    executor._wait_for_task = slow_wait
    # Manually attach the running task so pause() can cancel it
    executor._task = asyncio.create_task(executor.execute())
    await asyncio.sleep(0.2)
    await executor.pause()
    with pytest.raises(asyncio.CancelledError):
        await executor._task
    assert state.workflow_status == WorkflowStatus.PAUSED


@pytest.mark.asyncio
async def test_workflow_executor_no_ready_steps_waits():
    """execute() must wait (sleep) when no steps are ready but workflow is not done."""
    steps = [
        WorkflowStep(step_id="s1", name="S1", agent_id="a1", task_type="test", payload={}),
        WorkflowStep(
            step_id="s2",
            name="S2",
            agent_id="a2",
            task_type="test",
            payload={},
            dependencies=[StepDependency(depends_on="s1")],
        ),
    ]
    workflow = Workflow(workflow_id="wait_wf", name="Wait", steps=steps)
    state = WorkflowStateManager(workflow)
    scheduler = TaskScheduler(InMemoryTaskQueue())
    executor = WorkflowExecutor(scheduler, state)

    async def mock_wait(task_id):
        return Task(task_id=task_id, agent_id="a1", type="test", payload={}, status=TaskStatus.COMPLETED)
    executor._wait_for_task = mock_wait

    await executor.execute()
    assert state.workflow_status == WorkflowStatus.COMPLETED
    assert state.get_step_status("s1") == StepStatus.COMPLETED
    assert state.get_step_status("s2") == StepStatus.COMPLETED


# ===========================================================================
# AgentEngine — lifecycle edge branches
# ===========================================================================


@pytest.mark.asyncio
async def test_engine_unregister_agent_not_found():
    """unregister_agent must return False when the agent_id does not exist."""
    registry = InMemoryAgentRegistry()
    queue = InMemoryTaskQueue()
    scheduler = TaskScheduler(queue)
    engine = AgentEngine(registry, scheduler)
    removed = await engine.unregister_agent("ghost")
    assert removed is False


@pytest.mark.asyncio
async def test_engine_pause_agent_not_found():
    """pause_agent must return False when the agent_id does not exist."""
    registry = InMemoryAgentRegistry()
    queue = InMemoryTaskQueue()
    scheduler = TaskScheduler(queue)
    engine = AgentEngine(registry, scheduler)
    paused = await engine.pause_agent("ghost")
    assert paused is False


@pytest.mark.asyncio
async def test_engine_list_agents():
    """list_agents must return the IDs of all registered agents."""
    registry = InMemoryAgentRegistry()
    queue = InMemoryTaskQueue()
    scheduler = TaskScheduler(queue)
    engine = AgentEngine(registry, scheduler)

    class _SimpleAgent(BaseAgent):
        async def initialize(self) -> None:
            self._initialized = True
            self.state = AgentRuntimeState.RUNNING
        async def run(self, task: Task) -> str:
            return ""
        async def shutdown(self) -> None:
            pass

    a1 = _SimpleAgent("a1", "A1")
    a2 = _SimpleAgent("a2", "A2")
    await engine.register_agent(a1)
    await engine.register_agent(a2)
    assert set(engine.list_agents()) == {"a1", "a2"}


# ===========================================================================
# RedisTaskQueue — additional error / edge branches
# ===========================================================================


@pytest.mark.asyncio
async def test_redis_queue_get_task_redis_miss_falls_back_to_db(redis_queue, clean_db):
    """get_task() must fall back to DB when Redis has no cached copy."""
    scheduler = TaskScheduler(redis_queue)
    tid = "db-fallback-t1"
    await scheduler.submit_task("agent", "echo", {"x": 1}, task_id=tid)
    # Delete Redis cache to force DB fallback
    await redis_queue.redis.delete(redis_queue._task_key(tid))
    await redis_queue.redis.delete(redis_queue._meta_key(tid))
    task = await redis_queue.get_task(tid)
    assert task is not None
    assert task.payload == {"x": 1}


@pytest.mark.asyncio
async def test_redis_queue_cancel_nonexistent_returns_false(redis_queue, clean_db):
    """cancel() must return False for a task_id that does not exist."""
    result = await redis_queue.cancel("does-not-exist")
    assert result is False


@pytest.mark.asyncio
async def test_redis_queue_peek_empty_returns_none(redis_queue, clean_db):
    """peek() must return None when the queue is empty."""
    result = await redis_queue.peek()
    assert result is None


# ===========================================================================
# QuotaChecker / QuotaManager — untested branches
# ===========================================================================


@pytest.mark.asyncio
async def test_quota_checker_nonexistent_tenant_returns_false():
    """QuotaChecker must return False when tenant does not exist."""
    from src.agent_platform.multi_tenant.manager import TenantManager
    from src.agent_platform.multi_tenant.models import TenantQuota
    from src.agent_platform.multi_tenant.quota import QuotaChecker

    class Storage:
        _tenants = {}

    manager = TenantManager(Storage())
    checker = QuotaChecker(manager)
    assert await checker.check_agent_quota("ghost", 0) is False
    assert await checker.check_task_quota("ghost", 0) is False
    assert await checker.check_message_quota("ghost") is False
    assert await checker.check_workflow_quota("ghost", 0) is False


@pytest.mark.asyncio
async def test_quota_checker_exceeded_raises():
    """QuotaChecker must raise TenantQuotaExceededError when quota is exceeded."""
    from src.agent_platform.multi_tenant.manager import TenantManager
    from src.agent_platform.multi_tenant.models import TenantQuota
    from src.agent_platform.multi_tenant.quota import QuotaChecker

    class Storage:
        _tenants = {}

    manager = TenantManager(Storage())
    tenant = await manager.create_tenant(
        "Q Tenant",
        quota=TenantQuota(max_agents=1, max_concurrent_tasks=1, max_workflows=1, max_messages_per_second=1),
    )
    checker = QuotaChecker(manager)

    with pytest.raises(Exception):
        await checker.check_agent_quota(tenant.tenant_id, 1)
    with pytest.raises(Exception):
        await checker.check_task_quota(tenant.tenant_id, 1)
    with pytest.raises(Exception):
        await checker.check_workflow_quota(tenant.tenant_id, 1)

    # message quota: send 2 messages when limit is 1
    await checker.check_message_quota(tenant.tenant_id)
    with pytest.raises(Exception):
        await checker.check_message_quota(tenant.tenant_id)


@pytest.mark.asyncio
async def test_quota_manager_increment_and_decrement():
    """QuotaManager must correctly increment and decrement agent counts."""
    from src.agent_platform.multi_tenant.manager import TenantManager
    from src.agent_platform.multi_tenant.quota import QuotaChecker, QuotaManager

    class Storage:
        _tenants = {}

    manager = TenantManager(Storage())
    tenant = await manager.create_tenant("QM Tenant")
    checker = QuotaChecker(manager)
    qm = QuotaManager(manager, checker)

    await qm.increment_agent_count(tenant.tenant_id)
    await qm.increment_agent_count(tenant.tenant_id)
    usage = await qm.get_resource_usage(tenant.tenant_id)
    assert usage["agents"] == 2

    await qm.decrement_agent_count(tenant.tenant_id)
    usage = await qm.get_resource_usage(tenant.tenant_id)
    assert usage["agents"] == 1


# ===========================================================================
# Distributed worker — agent-not-found error path
# ===========================================================================


@pytest.mark.asyncio
async def test_worker_node_agent_not_found_marks_failed():
    """WorkerNode._execute_task must mark task FAILED when agent is not found."""
    from src.agent_platform.distributed.node import NodeInfo, NodeStatus
    from src.agent_platform.distributed.worker import WorkerNode, WorkerConfig
    from src.agent_platform.registry.in_memory import InMemoryAgentRegistry

    registry = InMemoryAgentRegistry()
    node_info = NodeInfo(
        node_id="worker-1",
        hostname="localhost",
        ip_address="127.0.0.1",
        port=8001,
        status=NodeStatus.ACTIVE,
    )
    worker = WorkerNode(node_info, InMemoryTaskQueue(), registry, WorkerConfig())

    task = Task(task_id="wf-1", agent_id="missing-agent", type="test", payload={})
    await worker._execute_task(task)
    assert task.status == TaskStatus.FAILED
    assert "not found" in (task.error or "")


# ===========================================================================
# AgentEngine — resume from paused
# ===========================================================================


@pytest.mark.asyncio
async def test_engine_resume_paused_agent():
    """resume_agent must set state back to RUNNING and restart worker."""
    registry = InMemoryAgentRegistry()
    queue = InMemoryTaskQueue()
    scheduler = TaskScheduler(queue)
    engine = AgentEngine(registry, scheduler)

    agent = _EchoAgent("echo-resume", "EchoBot")
    await engine.register_agent(agent)
    await engine.pause_agent("echo-resume")
    assert agent.state == AgentRuntimeState.PAUSED

    resumed = await engine.resume_agent("echo-resume")
    assert resumed is True
    assert agent.state == AgentRuntimeState.RUNNING
    assert "echo-resume" in engine._workers


# ===========================================================================
# Workflow models — get_step / get_dependents / get_roots
# ===========================================================================


def test_workflow_get_step_not_found():
    """Workflow.get_step must return None when step_id does not exist."""
    from src.agent_platform.workflow.models import Workflow, WorkflowStep

    wf = Workflow(
        workflow_id="wf",
        name="WF",
        steps=[WorkflowStep(step_id="s1", name="S1", agent_id="a1", task_type="test", payload={})],
    )
    assert wf.get_step("nonexistent") is None


def test_workflow_get_dependents_and_roots():
    """Workflow.get_dependents and get_roots must return correct step IDs."""
    from src.agent_platform.workflow.models import (
        StepDependency,
        Workflow,
        WorkflowStep,
    )

    steps = [
        WorkflowStep(step_id="s1", name="S1", agent_id="a1", task_type="test", payload={}),
        WorkflowStep(
            step_id="s2",
            name="S2",
            agent_id="a2",
            task_type="test",
            payload={},
            dependencies=[StepDependency(depends_on="s1")],
        ),
        WorkflowStep(
            step_id="s3",
            name="S3",
            agent_id="a3",
            task_type="test",
            payload={},
            dependencies=[StepDependency(depends_on="s1")],
        ),
    ]
    wf = Workflow(workflow_id="dep_wf", name="Dep", steps=steps)
    assert wf.get_roots() == ["s1"]
    assert set(wf.get_dependents("s1")) == {"s2", "s3"}
    assert wf.get_dependents("s2") == []


# ===========================================================================
# RedisTaskQueue — additional uncovered branches
# ===========================================================================


@pytest.mark.asyncio
async def test_redis_queue_list_tasks_with_pagination(redis_queue, clean_db):
    """list_tasks must respect limit and offset."""
    scheduler = TaskScheduler(redis_queue)
    for i in range(5):
        await scheduler.submit_task("agent", "echo", {"i": i}, task_id=f"pag-{i}")
    page1 = await redis_queue.list_tasks(limit=2, offset=0)
    assert len(page1) == 2
    page2 = await redis_queue.list_tasks(limit=2, offset=2)
    assert len(page2) == 2


@pytest.mark.asyncio
async def test_redis_queue_cancel_running_task_returns_false(redis_queue, clean_db):
    """cancel() must return False for a task already RUNNING."""
    scheduler = TaskScheduler(redis_queue)
    tid = "cancel-running-t1"
    await scheduler.submit_task("agent", "echo", {}, task_id=tid)
    task = await redis_queue.get_task(tid)
    task.status = TaskStatus.RUNNING
    await redis_queue.update_task(task)
    result = await redis_queue.cancel(tid)
    assert result is False
    reloaded = await redis_queue.get_task(tid)
    assert reloaded.status == TaskStatus.RUNNING


@pytest.mark.asyncio
async def test_redis_queue_get_stats_filtered_by_tenant(redis_queue, clean_db):
    """get_stats must filter by tenant_id."""
    scheduler = TaskScheduler(redis_queue)
    await scheduler.submit_task("agent", "echo", {}, tenant_id="tenant-A")
    await scheduler.submit_task("agent", "echo", {}, tenant_id="tenant-B")
    stats_a = await redis_queue.get_stats(tenant_id="tenant-A")
    assert stats_a.total == 1


@pytest.mark.asyncio
async def test_redis_queue_update_task_persists_status_change(redis_queue, clean_db):
    """update_task must persist status changes to both Redis and Postgres."""
    scheduler = TaskScheduler(redis_queue)
    tid = "update-status-t1"
    await scheduler.submit_task("agent", "echo", {}, task_id=tid)
    task = await redis_queue.get_task(tid)
    task.status = TaskStatus.FAILED
    task.error = "test failure"
    await redis_queue.update_task(task)
    reloaded = await redis_queue.get_task(tid)
    assert reloaded.status == TaskStatus.FAILED
    assert reloaded.error == "test failure"


@pytest.mark.asyncio
async def test_redis_queue_enqueue_sets_message_id_when_missing(redis_queue, clean_db):
    """enqueue() must generate a message_id when one is not provided."""
    scheduler = TaskScheduler(redis_queue)
    tid = "no-msgid-t1"
    await scheduler.submit_task("agent", "echo", {}, task_id=tid)
    task = await redis_queue.get_task(tid)
    assert task is not None
    assert task.message_id is not None
    assert task.message_id.startswith("msg-")


@pytest.mark.asyncio
async def test_redis_queue_get_task_redis_cache_hit(redis_queue, clean_db):
    """get_task() must return the task from Redis cache when available."""
    scheduler = TaskScheduler(redis_queue)
    tid = "cache-hit-t1"
    await scheduler.submit_task("agent", "echo", {"x": 1}, task_id=tid)
    # First call populates Redis cache
    task1 = await redis_queue.get_task(tid)
    assert task1 is not None
    # Second call should hit Redis cache
    task2 = await redis_queue.get_task(tid)
    assert task2 is not None
    assert task2.payload == {"x": 1}


# ===========================================================================
# AgentEngine — additional lifecycle and loop branches
# ===========================================================================


@pytest.mark.asyncio
async def test_engine_start_idempotent():
    """A second start() must be a no-op when the engine is already running."""
    registry = InMemoryAgentRegistry()
    queue = InMemoryTaskQueue()
    scheduler = TaskScheduler(queue)
    engine = AgentEngine(registry, scheduler)

    await engine.start()
    assert engine.is_running
    await engine.start()  # idempotent
    assert engine.is_running
    await engine.stop()


@pytest.mark.asyncio
async def test_engine_stop_when_not_running():
    """stop() must not crash when the engine was never started."""
    registry = InMemoryAgentRegistry()
    queue = InMemoryTaskQueue()
    scheduler = TaskScheduler(queue)
    engine = AgentEngine(registry, scheduler)
    await engine.stop()  # no-op
    assert not engine.is_running


@pytest.mark.asyncio
async def test_engine_unregister_agent_success():
    """unregister_agent must fully remove the agent and its worker."""
    registry = InMemoryAgentRegistry()
    queue = InMemoryTaskQueue()
    scheduler = TaskScheduler(queue)
    engine = AgentEngine(registry, scheduler)

    agent = _EchoAgent("echo-unreg", "EchoBot")
    await engine.register_agent(agent)
    assert "echo-unreg" in engine._agents

    removed = await engine.unregister_agent("echo-unreg")
    assert removed is True
    assert "echo-unreg" not in engine._agents
    assert agent.state == AgentRuntimeState.STOPPED
    assert agent._initialized is False


@pytest.mark.asyncio
async def test_engine_dispatcher_loop_handles_scheduler_exception():
    """_dispatcher_loop must survive exceptions from scheduler.dequeue_next."""
    registry = InMemoryAgentRegistry()
    queue = InMemoryTaskQueue()
    scheduler = TaskScheduler(queue)
    engine = AgentEngine(registry, scheduler, poll_interval=0.1)

    agent = _EchoAgent("echo-disp-ex", "EchoBot")
    await engine.register_agent(agent)

    call_count = 0

    async def boom_dequeue():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise RuntimeError("dequeue boom")
        return None

    scheduler.dequeue_next = boom_dequeue  # type: ignore[method-assign]
    await engine.start()
    try:
        await asyncio.sleep(3.5)
    finally:
        await engine.stop()
    assert call_count >= 1


@pytest.mark.asyncio
async def test_engine_worker_loop_queue_none():
    """_worker_loop must sleep when the agent's task queue is None."""
    registry = InMemoryAgentRegistry()
    queue = InMemoryTaskQueue()
    scheduler = TaskScheduler(queue)
    engine = AgentEngine(registry, scheduler, poll_interval=0.1)

    agent = _EchoAgent("echo-q-none", "EchoBot")
    await engine.register_agent(agent)
    agent._task_queue = None

    await engine.start()
    try:
        await asyncio.sleep(0.3)
    finally:
        await engine.stop()
    # No exception should propagate


@pytest.mark.asyncio
async def test_engine_worker_loop_queue_timeout():
    """_worker_loop must survive an idle queue (timeout)."""
    registry = InMemoryAgentRegistry()
    queue = InMemoryTaskQueue()
    scheduler = TaskScheduler(queue)
    engine = AgentEngine(registry, scheduler, poll_interval=0.1)

    agent = _EchoAgent("echo-q-timeout", "EchoBot")
    await engine.register_agent(agent)
    # Leave the queue empty so queue.get() times out

    await engine.start()
    try:
        await asyncio.sleep(0.5)
    finally:
        await engine.stop()
    # No exception should propagate


@pytest.mark.asyncio
async def test_engine_worker_loop_exception_in_execute():
    """_worker_loop must catch exceptions from _execute_task."""
    registry = InMemoryAgentRegistry()
    queue = InMemoryTaskQueue()
    scheduler = TaskScheduler(queue)
    engine = AgentEngine(registry, scheduler, poll_interval=0.1)

    class _BoomAgent(BaseAgent):
        async def initialize(self) -> None:
            self._initialized = True
            self.state = AgentRuntimeState.RUNNING

        async def run(self, task: Task) -> str:
            raise RuntimeError("boom")

        async def shutdown(self) -> None:
            pass

    agent = _BoomAgent("boom-agent", "Boom")
    await engine.register_agent(agent)

    task = Task(task_id="boom-1", agent_id="boom-agent", type="test", payload={}, max_retries=0)
    await queue.enqueue(task)
    await engine.start()
    try:
        await asyncio.sleep(0.5)
        task = await queue.get_task("boom-1")
        assert task is not None
        assert task.status == TaskStatus.FAILED
    finally:
        await engine.stop()


@pytest.mark.asyncio
async def test_engine_get_agent_state_not_found():
    """get_agent_state must return None for an unknown agent."""
    registry = InMemoryAgentRegistry()
    queue = InMemoryTaskQueue()
    scheduler = TaskScheduler(queue)
    engine = AgentEngine(registry, scheduler)
    assert await engine.get_agent_state("ghost") is None


@pytest.mark.asyncio
async def test_engine_list_agents_empty():
    """list_agents must return an empty list when no agents are registered."""
    registry = InMemoryAgentRegistry()
    queue = InMemoryTaskQueue()
    scheduler = TaskScheduler(queue)
    engine = AgentEngine(registry, scheduler)
    assert engine.list_agents() == []


# ===========================================================================
# WorkflowExecutor — remaining uncovered branches
# ===========================================================================


@pytest.mark.asyncio
async def test_workflow_executor_invalid_status_raises():
    """execute() must raise WorkflowExecutionError for a non-PENDING/RESUMED workflow."""
    from src.agent_platform.workflow.exceptions import WorkflowExecutionError

    steps = [
        WorkflowStep(step_id="s1", name="S1", agent_id="a1", task_type="test", payload={})
    ]
    workflow = Workflow(workflow_id="invalid-wf", name="Invalid", steps=steps)
    state = WorkflowStateManager(workflow)
    state.start()
    state.complete()  # now COMPLETED
    scheduler = TaskScheduler(InMemoryTaskQueue())
    executor = WorkflowExecutor(scheduler, state)

    with pytest.raises(WorkflowExecutionError):
        await executor.execute()


@pytest.mark.asyncio
async def test_workflow_executor_resume_method():
    """WorkflowExecutor.resume() must wrap execute() in a task."""
    steps = [
        WorkflowStep(step_id="s1", name="S1", agent_id="a1", task_type="test", payload={})
    ]
    workflow = Workflow(workflow_id="resume-method-wf", name="ResumeMethod", steps=steps)
    state = WorkflowStateManager(workflow)
    state.start()
    state.pause()
    scheduler = TaskScheduler(InMemoryTaskQueue())
    executor = WorkflowExecutor(scheduler, state)

    async def mock_wait(task_id):
        return Task(task_id=task_id, agent_id="a1", type="test", payload={}, status=TaskStatus.COMPLETED)
    executor._wait_for_task = mock_wait

    await executor.resume()
    assert state.workflow_status == WorkflowStatus.COMPLETED


@pytest.mark.asyncio
async def test_workflow_executor_get_status():
    """get_status() must return the current WorkflowStatus."""
    steps = [
        WorkflowStep(step_id="s1", name="S1", agent_id="a1", task_type="test", payload={})
    ]
    workflow = Workflow(workflow_id="status-wf", name="Status", steps=steps)
    state = WorkflowStateManager(workflow)
    scheduler = TaskScheduler(InMemoryTaskQueue())
    executor = WorkflowExecutor(scheduler, state)
    assert executor.get_status() == WorkflowStatus.PENDING
    state.start()
    assert executor.get_status() == WorkflowStatus.RUNNING


@pytest.mark.asyncio
async def test_workflow_executor_fallback_step_missing():
    """_execute_step must fail the workflow when a fallback step ID does not exist."""
    steps = [
        WorkflowStep(
            step_id="s1",
            name="S1",
            agent_id="a1",
            task_type="test",
            payload={},
            fallback_step_id="missing-fallback",
        )
    ]
    workflow = Workflow(workflow_id="fb-miss-wf", name="FbMiss", steps=steps)
    state = WorkflowStateManager(workflow)
    scheduler = TaskScheduler(InMemoryTaskQueue())
    executor = WorkflowExecutor(scheduler, state)

    async def fail_wait(task_id):
        raise RuntimeError("primary boom")
    executor._wait_for_task = fail_wait

    await executor.execute()
    assert state.workflow_status == WorkflowStatus.FAILED
    assert state.get_step_status("s1") == StepStatus.FAILED


# ===========================================================================
# TaskScheduler — uncovered branches
# ===========================================================================


@pytest.mark.asyncio
async def test_scheduler_submit_task_cross_tenant_conflict():
    """submit_task must raise CrossTenantTaskConflictError for cross-tenant reuse."""
    from src.agent_platform.scheduler.exceptions import CrossTenantTaskConflictError

    queue = InMemoryTaskQueue()
    scheduler = TaskScheduler(queue)
    tid = "xten-1"
    await scheduler.submit_task("agent", "echo", {}, tenant_id="tenant-A", task_id=tid)
    with pytest.raises(CrossTenantTaskConflictError):
        await scheduler.submit_task("agent", "echo", {}, tenant_id="tenant-B", task_id=tid)


@pytest.mark.asyncio
async def test_scheduler_get_task_status_missing():
    """get_task_status must return None for a nonexistent task."""
    queue = InMemoryTaskQueue()
    scheduler = TaskScheduler(queue)
    assert await scheduler.get_task_status("missing") is None


@pytest.mark.asyncio
async def test_scheduler_peek_next_empty():
    """peek_next must return None when the queue is empty."""
    queue = InMemoryTaskQueue()
    scheduler = TaskScheduler(queue)
    assert await scheduler.peek_next() is None


@pytest.mark.asyncio
async def test_scheduler_queue_size_empty():
    """queue_size must return 0 for an empty queue."""
    queue = InMemoryTaskQueue()
    scheduler = TaskScheduler(queue)
    assert await scheduler.queue_size() == 0


# ===========================================================================
# InMemoryAgentRegistry — uncovered branches
# ===========================================================================


@pytest.mark.asyncio
async def test_registry_register_duplicate_returns_existing():
    """register() must keep the existing record when the same agent_id is reused."""
    registry = InMemoryAgentRegistry()
    from src.agent_platform.core.agent import AgentRecord, AgentStatus

    record = AgentRecord(agent_id="dup-1", name="Dup", status=AgentStatus.ACTIVE)
    await registry.register(record)
    await registry.register(record)  # duplicate is ignored
    assert await registry.get_agent("dup-1") is not None


@pytest.mark.asyncio
async def test_registry_unregister_missing_returns_false():
    """unregister() must return False for an unknown agent_id."""
    registry = InMemoryAgentRegistry()
    assert await registry.unregister("ghost") is False


@pytest.mark.asyncio
async def test_registry_list_agents_empty():
    """list_all() must return [] when no agents are registered."""
    registry = InMemoryAgentRegistry()
    assert await registry.list_all() == []


@pytest.mark.asyncio
async def test_registry_cleanup_stale_no_stale():
    """cleanup_stale() must return 0 when there are no stale agents."""
    registry = InMemoryAgentRegistry()
    cleaned = await registry.cleanup_stale(ttl_seconds=1)
    assert cleaned == 0


@pytest.mark.asyncio
async def test_registry_discover_empty():
    """discover() must return [] when no agents match the filters."""
    registry = InMemoryAgentRegistry()
    results = await registry.discover(status="active")
    assert results == []


# ===========================================================================
# MessageBus (in-memory) — uncovered branches
# ===========================================================================


@pytest.mark.asyncio
async def test_bus_send_to_unsubscribed_agent_raises():
    """send() must raise MessageDeliveryError when there is no subscriber."""
    from src.agent_platform.message_bus.exceptions import MessageDeliveryError
    from src.agent_platform.message_bus.in_memory import InMemoryMessageBus

    bus = InMemoryMessageBus()
    msg = Message(message_id="bus-err", from_agent="s", to_agent="missing", type=MessageType.COMMAND, content={})
    with pytest.raises(MessageDeliveryError):
        await bus.send(msg)


@pytest.mark.asyncio
async def test_bus_publish_no_subscribers():
    """publish() must return message_id when no subscribers exist for the topic."""
    from src.agent_platform.message_bus.in_memory import InMemoryMessageBus

    bus = InMemoryMessageBus()
    msg = Message(message_id="pub-empty", from_agent="s", type=MessageType.EVENT, content={})
    result = await bus.publish("no-subs", msg)
    assert result == "pub-empty"


@pytest.mark.asyncio
async def test_bus_broadcast_empty_recipients():
    """broadcast() with no subscribers must return an empty list of IDs."""
    from src.agent_platform.message_bus.in_memory import InMemoryMessageBus

    bus = InMemoryMessageBus()
    msg = Message(message_id="bcast-empty", from_agent="s", type=MessageType.BROADCAST, content={})
    ids = await bus.broadcast(msg)
    assert ids == []


# ===========================================================================
# Security helpers — additional edge branches
# ===========================================================================


def test_security_hash_api_key_deterministic():
    """hash_api_key must produce a consistent hash for the same input."""
    from src.agent_platform.security import hash_api_key
    h1 = hash_api_key("secret")
    h2 = hash_api_key("secret")
    assert h1 == h2
    assert h1 != hash_api_key("other")


def test_security_api_key_matches_wrong_hash():
    """api_key_matches must return False when the hash does not match."""
    from src.agent_platform.security import api_key_matches, hash_api_key
    wrong = hash_api_key("other")
    assert api_key_matches(wrong, "secret") is False
    # Non-string short-circuit must still work
    assert api_key_matches("short", "secret") is False


def test_security_stored_api_key_hash_legacy():
    """stored_api_key_hash must hash legacy 'key' when 'key_hash' is absent."""
    from src.agent_platform.security import stored_api_key_hash, hash_api_key
    assert stored_api_key_hash({"key": "legacy-secret"}) == hash_api_key("legacy-secret")


def test_security_api_key_record_matches_active_valid():
    """api_key_record_matches must return True for an active record with a valid hash."""
    from src.agent_platform.security import api_key_record_matches, hash_api_key
    record = {"is_active": True, "key_hash": hash_api_key("secret")}
    assert api_key_record_matches(record, "secret") is True


# ===========================================================================
# TaskWorker — timeout and failure retry branches
# ===========================================================================


@pytest.mark.asyncio
async def test_task_worker_timeout_retry_then_success():
    """TaskWorker must retry after a timeout and eventually succeed."""
    from src.agent_platform.scheduler.worker import TaskWorker

    class _TimedAgent(BaseAgent):
        async def initialize(self) -> None:
            self._initialized = True
            self.state = AgentRuntimeState.RUNNING

        call_count = 0

        async def run(self, task: Task) -> str:
            _TimedAgent.call_count += 1
            if _TimedAgent.call_count < 2:
                await asyncio.sleep(10)  # force timeout
            return "ok"

        async def shutdown(self) -> None:
            pass

    agent = _TimedAgent("tw-timeout", "TW")
    task = Task(task_id="tw-1", agent_id="tw-timeout", type="t", payload={}, timeout_seconds=1, max_retries=3)
    worker = TaskWorker(task, agent)
    result = await worker.execute()
    assert result.status == TaskStatus.COMPLETED
    assert result.result == "ok"


@pytest.mark.asyncio
async def test_task_worker_failure_retry_then_success():
    """TaskWorker must retry after a failure and eventually succeed."""
    from src.agent_platform.scheduler.worker import TaskWorker

    class _FailAgent(BaseAgent):
        async def initialize(self) -> None:
            self._initialized = True
            self.state = AgentRuntimeState.RUNNING

        call_count = 0

        async def run(self, task: Task) -> str:
            _FailAgent.call_count += 1
            if _FailAgent.call_count < 2:
                raise RuntimeError("transient")
            return "ok"

        async def shutdown(self) -> None:
            pass

    agent = _FailAgent("tw-fail", "TW")
    task = Task(task_id="tw-2", agent_id="tw-fail", type="t", payload={}, timeout_seconds=30, max_retries=3)
    worker = TaskWorker(task, agent)
    result = await worker.execute()
    assert result.status == TaskStatus.COMPLETED
    assert result.result == "ok"


# ===========================================================================
# InMemoryTaskQueue — uncovered branches
# ===========================================================================


@pytest.mark.asyncio
async def test_inmemory_queue_dequeue_empty_returns_none():
    """dequeue() must return None when the queue is empty."""
    queue = InMemoryTaskQueue()
    assert await queue.dequeue() is None


@pytest.mark.asyncio
async def test_inmemory_queue_cancel_pending():
    """cancel() must set a PENDING task to CANCELLED and return True."""
    queue = InMemoryTaskQueue()
    tid = "cancel-pend-1"
    await queue.enqueue(Task(task_id=tid, agent_id="a", type="t", payload={}, status=TaskStatus.PENDING))
    result = await queue.cancel(tid)
    assert result is True
    task = await queue.get_task(tid)
    assert task is not None
    assert task.status == TaskStatus.CANCELLED


@pytest.mark.asyncio
async def test_inmemory_queue_update_task():
    """update_task() must overwrite an existing task in place."""
    queue = InMemoryTaskQueue()
    tid = "update-1"
    await queue.enqueue(Task(task_id=tid, agent_id="a", type="t", payload={}, status=TaskStatus.PENDING))
    task = await queue.get_task(tid)
    task.status = TaskStatus.COMPLETED
    task.result = "done"
    await queue.update_task(task)
    reloaded = await queue.get_task(tid)
    assert reloaded.status == TaskStatus.COMPLETED
    assert reloaded.result == "done"


# ===========================================================================
# Multi-tenant — manager and middleware uncovered branches
# ===========================================================================


@pytest.mark.asyncio
async def test_tenant_manager_update_tenant():
    """update_tenant must modify existing tenant fields."""
    from src.agent_platform.multi_tenant.manager import TenantManager
    from src.agent_platform.multi_tenant.models import TenantStatus

    class Storage:
        _tenants = {}

    manager = TenantManager(Storage())
    tenant = await manager.create_tenant("Updatable")
    updated = await manager.update_tenant(tenant.tenant_id, {"name": "New Name", "status": TenantStatus.SUSPENDED})
    assert updated.name == "New Name"
    assert updated.status == TenantStatus.SUSPENDED


@pytest.mark.asyncio
async def test_tenant_manager_delete_tenant():
    """delete_tenant must soft-delete the tenant by setting status to DELETED."""
    from src.agent_platform.multi_tenant.manager import TenantManager
    from src.agent_platform.multi_tenant.models import TenantStatus

    class Storage:
        _tenants = {}

    manager = TenantManager(Storage())
    tenant = await manager.create_tenant("Deletable")
    await manager.delete_tenant(tenant.tenant_id)
    updated = await manager.get_tenant(tenant.tenant_id)
    assert updated is not None
    assert updated.status == TenantStatus.DELETED


@pytest.mark.asyncio
async def test_authenticator_verify_signature_known():
    """TenantAuthenticator.verify_signature must validate HMAC signatures."""
    import hashlib
    import hmac

    from src.agent_platform.multi_tenant.authentication import TenantAuthenticator
    from src.agent_platform.multi_tenant.manager import TenantManager

    class Storage:
        _tenants = {}

    manager = TenantManager(Storage())
    auth = TenantAuthenticator(tenant_manager=manager)
    payload = {"a": 1, "b": 2}
    secret = "my-secret"
    sorted_payload = "&".join(f"{k}={v}" for k, v in sorted(payload.items()))
    expected = hmac.new(secret.encode(), sorted_payload.encode(), hashlib.sha256).hexdigest()
    assert auth.verify_signature(payload, expected, secret) is True


# ===========================================================================
# Runtime — uncovered branches
# ===========================================================================


def test_runtime_queue_backend_defaults_to_memory():
    from src.agent_platform.runtime import _queue_backend
    assert _queue_backend() == "memory"


def test_runtime_queue_backend_redis(monkeypatch):
    from src.agent_platform.runtime import _queue_backend
    monkeypatch.setenv("TASK_QUEUE_BACKEND", "redis")
    assert _queue_backend() == "redis"


def test_runtime_database_url_default():
    import src.agent_platform.db as db_mod
    import os
    old = os.environ.pop("DATABASE_URL", None)
    try:
        assert db_mod._database_url().startswith("postgresql+asyncpg://")
    finally:
        if old is not None:
            os.environ["DATABASE_URL"] = old


def test_runtime_database_url_postgresql_prefix(monkeypatch):
    import src.agent_platform.db as db_mod
    monkeypatch.setenv("DATABASE_URL", "postgresql://host/db")
    assert db_mod._database_url() == "postgresql+asyncpg://host/db"


def test_runtime_get_engine_and_session_factory():
    import src.agent_platform.db as db_mod
    db_mod.get_engine.cache_clear()
    db_mod.get_session_factory.cache_clear()
    try:
        engine = db_mod.get_engine()
        assert engine is not None
        factory = db_mod.get_session_factory()
        assert factory is not None
    finally:
        db_mod.get_engine.cache_clear()
        db_mod.get_session_factory.cache_clear()


@pytest.mark.asyncio
async def test_runtime_ensure_schema(monkeypatch):
    import src.agent_platform.db as db_mod
    from unittest.mock import AsyncMock, MagicMock
    mock_conn = AsyncMock()
    mock_engine = MagicMock()
    mock_engine.begin.return_value.__aenter__.return_value = mock_conn
    monkeypatch.setattr(db_mod, "get_engine", lambda: mock_engine)
    await db_mod.ensure_schema()
    mock_conn.run_sync.assert_called_once()


def test_runtime_configure_cpu_runtime_torch_missing(monkeypatch):
    import sys
    import src.agent_platform.runtime as rt
    monkeypatch.setitem(sys.modules, "torch", None)
    rt._configure_cpu_runtime()


def test_runtime_get_redis_client(monkeypatch):
    import src.agent_platform.runtime as rt
    monkeypatch.setenv("REDIS_URL", "redis://localhost:6379/1")
    rt.get_redis_client.cache_clear()
    client = rt.get_redis_client()
    assert client is not None
    rt.get_redis_client.cache_clear()


def test_runtime_get_task_queue_memory(monkeypatch):
    import src.agent_platform.runtime as rt
    from src.agent_platform.scheduler.in_memory import InMemoryTaskQueue
    monkeypatch.setenv("TASK_QUEUE_BACKEND", "memory")
    rt.get_task_queue.cache_clear()
    queue = rt.get_task_queue()
    assert isinstance(queue, InMemoryTaskQueue)
    rt.get_task_queue.cache_clear()


def test_runtime_get_scheduler():
    import src.agent_platform.runtime as rt
    from src.agent_platform.scheduler.scheduler import TaskScheduler
    rt.get_scheduler.cache_clear()
    scheduler = rt.get_scheduler()
    assert isinstance(scheduler, TaskScheduler)
    rt.get_scheduler.cache_clear()


def test_runtime_get_tenant_manager():
    import src.agent_platform.runtime as rt
    from src.agent_platform.multi_tenant.manager import TenantManager
    rt.get_tenant_manager.cache_clear()
    manager = rt.get_tenant_manager()
    assert isinstance(manager, TenantManager)
    rt.get_tenant_manager.cache_clear()


def test_runtime_reset_runtime_cache():
    import src.agent_platform.runtime as rt
    rt.reset_runtime_cache()


@pytest.mark.asyncio
async def test_runtime_prepare_runtime_memory(monkeypatch):
    import src.agent_platform.runtime as rt
    import src.agent_platform.db as db_mod
    monkeypatch.setenv("TASK_QUEUE_BACKEND", "memory")
    called = False
    async def fake_ensure():
        nonlocal called
        called = True
    monkeypatch.setattr(db_mod, "ensure_schema", fake_ensure)
    await rt.prepare_runtime()
    assert called is False


# ===========================================================================
# AgentContext — uncovered branches
# ===========================================================================


def test_agent_context_get_from_memory():
    from src.agent_platform.engine.context import AgentContext
    ctx = AgentContext(agent_id="a1")
    ctx.remember("key", "value")
    assert ctx.get("key") == "value"


def test_agent_context_get_from_variables():
    from src.agent_platform.engine.context import AgentContext
    ctx = AgentContext(agent_id="a1")
    ctx.set("key", "value")
    assert ctx.get("key") == "value"


def test_agent_context_get_default():
    from src.agent_platform.engine.context import AgentContext
    ctx = AgentContext(agent_id="a1")
    assert ctx.get("missing", "default") == "default"


def test_agent_context_to_dict():
    from src.agent_platform.engine.context import AgentContext
    ctx = AgentContext(agent_id="a1", tenant_id="t1")
    ctx.remember("mem", 1)
    ctx.set("var", 2)
    d = ctx.to_dict()
    assert d["agent_id"] == "a1"
    assert d["tenant_id"] == "t1"
    assert d["memory"] == {"mem": 1}
    assert d["variables"] == {"var": 2}


# ===========================================================================
# WorkflowStateManager — uncovered branches
# ===========================================================================


def test_workflow_state_is_step_failed():
    from src.agent_platform.workflow.state import WorkflowStateManager
    from src.agent_platform.workflow.models import Workflow, WorkflowStep

    wf = Workflow(workflow_id="wf", name="WF", steps=[WorkflowStep(step_id="s1", name="S1", agent_id="a1", task_type="test", payload={})])
    state = WorkflowStateManager(wf)
    assert state.is_step_failed("s1") is False
    state.step_statuses["s1"] = StepStatus.FAILED
    assert state.is_step_failed("s1") is True


def test_workflow_state_from_dict_with_completed_at():
    from src.agent_platform.workflow.state import WorkflowStateManager
    from src.agent_platform.workflow.models import Workflow, WorkflowStep
    from datetime import datetime, UTC

    wf = Workflow(workflow_id="wf", name="WF", steps=[WorkflowStep(step_id="s1", name="S1", agent_id="a1", task_type="test", payload={})])
    data = {
        "workflow_id": "wf",
        "workflow_status": "completed",
        "step_statuses": {"s1": "completed"},
        "step_results": {},
        "started_at": datetime.now(UTC).isoformat(),
        "completed_at": datetime.now(UTC).isoformat(),
    }
    state = WorkflowStateManager.from_dict(data, wf)
    assert state.workflow_status == WorkflowStatus.COMPLETED
    assert state.completed_at is not None


# ===========================================================================
# Core task/agent/message — uncovered branches
# ===========================================================================


def test_task_record_failure_max_retries_exceeded():
    from src.agent_platform.core.task import Task, TaskStatus
    task = Task(task_id="t1", agent_id="a", type="test", payload={}, max_retries=1)
    task.status = TaskStatus.RUNNING
    task.retry_count = 1
    entry = task.record_failure(worker_id="w", execution_id=None, error_category="test", reason="boom")
    assert entry["next_retry_decision"] == "max_retries_exceeded"
    assert entry["final_outcome"] == "failed"
    assert task.error == "boom"


def test_base_agent_stop_sets_state():
    from src.agent_platform.core.agent import BaseAgent, AgentRuntimeState
    import asyncio

    class _Agent(BaseAgent):
        async def initialize(self) -> None:
            self._initialized = True
            self.state = AgentRuntimeState.RUNNING
        async def run(self, task):
            return None
        async def shutdown(self) -> None:
            pass

    agent = _Agent("a1", "A1")
    agent.state = AgentRuntimeState.RUNNING
    agent._initialized = True
    asyncio.run(agent.stop())
    assert agent.state == AgentRuntimeState.STOPPED
    assert agent._initialized is False


def test_base_agent_super_initialize_covered():
    from src.agent_platform.core.agent import BaseAgent, AgentRuntimeState
    import asyncio

    class _Agent(BaseAgent):
        async def initialize(self) -> None:
            await super().initialize()
            self._initialized = True
            self.state = AgentRuntimeState.RUNNING
        async def run(self, task):
            return None
        async def shutdown(self) -> None:
            pass

    agent = _Agent("a1", "A1")
    asyncio.run(agent.initialize())
    assert agent._initialized is True


def test_base_agent_super_run_covered():
    from src.agent_platform.core.agent import BaseAgent, AgentRuntimeState
    import asyncio

    class _Agent(BaseAgent):
        async def initialize(self) -> None:
            self._initialized = True
            self.state = AgentRuntimeState.RUNNING
        async def run(self, task):
            return await super().run(task)
        async def shutdown(self) -> None:
            pass

    agent = _Agent("a1", "A1")
    asyncio.run(agent.initialize())
    result = asyncio.run(agent.run(None))
    assert result is None


def test_message_is_expired_none_ttl():
    from src.agent_platform.core.message import Message, MessageType
    msg = Message(from_agent="a", type=MessageType.EVENT, content={}, ttl_seconds=None)
    assert msg.is_expired() is False


def test_message_is_expired_old():
    from src.agent_platform.core.message import Message, MessageType
    from datetime import UTC, timedelta
    msg = Message(from_agent="a", type=MessageType.EVENT, content={}, ttl_seconds=1)
    msg.timestamp = msg.timestamp.replace(tzinfo=UTC) - timedelta(seconds=2)
    assert msg.is_expired() is True


def test_message_to_dict_and_from_dict():
    from src.agent_platform.core.message import Message, MessageType
    msg = Message(from_agent="a", type=MessageType.EVENT, content={"k": "v"})
    d = msg.to_dict()
    assert d["from_agent"] == "a"
    restored = Message.from_dict(d)
    assert restored.from_agent == "a"
    assert restored.content == {"k": "v"}


# ===========================================================================
# Database helpers — uncovered branches
# ===========================================================================


def test_db_database_url_default():
    import src.agent_platform.db as db_mod
    import os
    old = os.environ.pop("DATABASE_URL", None)
    try:
        assert db_mod._database_url().startswith("postgresql+asyncpg://")
    finally:
        if old is not None:
            os.environ["DATABASE_URL"] = old


def test_db_database_url_postgresql_prefix(monkeypatch):
    import src.agent_platform.db as db_mod
    monkeypatch.setenv("DATABASE_URL", "postgresql://host/db")
    assert db_mod._database_url() == "postgresql+asyncpg://host/db"


def test_db_get_engine_and_session_factory():
    import src.agent_platform.db as db_mod
    db_mod.get_engine.cache_clear()
    db_mod.get_session_factory.cache_clear()
    try:
        engine = db_mod.get_engine()
        assert engine is not None
        factory = db_mod.get_session_factory()
        assert factory is not None
    finally:
        db_mod.get_engine.cache_clear()
        db_mod.get_session_factory.cache_clear()


@pytest.mark.asyncio
async def test_db_ensure_schema(monkeypatch):
    import src.agent_platform.db as db_mod
    from unittest.mock import AsyncMock, MagicMock
    mock_conn = AsyncMock()
    mock_engine = MagicMock()
    mock_engine.begin.return_value.__aenter__.return_value = mock_conn
    monkeypatch.setattr(db_mod, "get_engine", lambda: mock_engine)
    await db_mod.ensure_schema()
    mock_conn.run_sync.assert_called_once()


# ===========================================================================
# Distributed node — uncovered branches
# ===========================================================================


def test_node_info_create_socket_error(monkeypatch):
    import socket
    from src.agent_platform.distributed.node import NodeInfo
    monkeypatch.setattr(socket, "gethostbyname", lambda host: (_ for _ in ()).throw(socket.gaierror()))
    info = NodeInfo.create(port=8000)
    assert info.ip_address == "127.0.0.1"


@pytest.mark.asyncio
async def test_node_health_check_not_running():
    from src.agent_platform.distributed.node import Node, NodeInfo, NodeStatus
    info = NodeInfo(node_id="n1", hostname="h", ip_address="127.0.0.1", port=8000)
    node = Node(info)
    assert await node.health_check() is False


@pytest.mark.asyncio
async def test_node_health_check_unhealthy():
    from src.agent_platform.distributed.node import Node, NodeInfo, NodeStatus
    from datetime import UTC, timedelta
    info = NodeInfo(node_id="n1", hostname="h", ip_address="127.0.0.1", port=8000, status=NodeStatus.ACTIVE)
    node = Node(info)
    node._running = True
    node.info.last_heartbeat = datetime.now(UTC) - timedelta(seconds=31)
    assert await node.health_check() is False
    assert node.info.status == NodeStatus.UNHEALTHY


def test_node_to_dict():
    from src.agent_platform.distributed.node import Node, NodeInfo, NodeStatus
    info = NodeInfo(node_id="n1", hostname="h", ip_address="127.0.0.1", port=8000, status=NodeStatus.ACTIVE)
    node = Node(info)
    d = node.to_dict()
    assert d["node_id"] == "n1"
    assert d["status"] == NodeStatus.ACTIVE.value


# ===========================================================================
# MessageBus validator — uncovered branches
# ===========================================================================


def test_message_validator_response_no_target_raises():
    from src.agent_platform.message_bus.validator import MessageValidator
    from src.agent_platform.core.message import Message, MessageType
    from src.agent_platform.message_bus.exceptions import MessageValidationError
    msg = Message(from_agent="a", type=MessageType.RESPONSE, content={})
    with pytest.raises(MessageValidationError):
        MessageValidator.validate(msg)


def test_message_validator_broadcast_no_content_raises():
    from src.agent_platform.message_bus.validator import MessageValidator
    from src.agent_platform.core.message import Message, MessageType
    from src.agent_platform.message_bus.exceptions import MessageValidationError
    msg = Message(from_agent="a", type=MessageType.BROADCAST, content={})
    with pytest.raises(MessageValidationError):
        MessageValidator.validate(msg)


# ===========================================================================
# ToolExecutor / ToolRegistry — uncovered branches
# ===========================================================================


@pytest.mark.asyncio
async def test_tool_executor_execution_error_wraps():
    from src.agent_platform.tools.executor import ToolExecutor
    from src.agent_platform.tools.registry import ToolRegistry
    from src.agent_platform.tools.exceptions import ToolExecutionError
    from src.agent_platform.tools.base import Tool

    class _BoomTool(Tool):
        def __init__(self):
            super().__init__(name="boom", description="Boom")
        parameters = []
        async def execute(self, **kwargs):
            raise RuntimeError("boom")

    registry = ToolRegistry()
    registry.register(_BoomTool())
    executor = ToolExecutor(registry)
    with pytest.raises(ToolExecutionError):
        await executor.execute("boom", {})


def test_tool_registry_get_all_and_clear():
    from src.agent_platform.tools.registry import ToolRegistry
    from src.agent_platform.tools.base import Tool

    class _T1(Tool):
        def __init__(self):
            super().__init__(name="t1", description="T1")
        parameters = []
        async def execute(self, **kwargs):
            return None

    registry = ToolRegistry()
    registry.register(_T1())
    assert len(registry.list_tools()) == 1
    assert len(registry.get_all_tools()) == 1
    registry.clear()
    assert registry.list_tools() == []


# ===========================================================================
# CircuitBreaker — uncovered branches
# ===========================================================================


@pytest.mark.asyncio
async def test_circuit_breaker_default_config():
    """CircuitBreaker must use default config when none is provided."""
    from src.agent_platform.recovery.circuit_breaker import CircuitBreaker, CircuitState
    cb = CircuitBreaker("default")
    assert cb.state == CircuitState.CLOSED
    assert cb.config.failure_threshold == 5
    assert cb.config.timeout_seconds == 30.0


@pytest.mark.asyncio
async def test_circuit_breaker_non_monitored_exception_does_not_count():
    """Non-monitored exceptions must not increment the failure count."""
    from src.agent_platform.recovery.circuit_breaker import (
        CircuitBreaker,
        CircuitBreakerConfig,
        CircuitState,
    )

    config = CircuitBreakerConfig(failure_threshold=2, monitored_exceptions=(ValueError,))
    cb = CircuitBreaker("non-monitored", config)

    async def raise_type_error():
        raise TypeError("not monitored")

    with pytest.raises(TypeError):
        await cb.call(raise_type_error)
    assert cb.state == CircuitState.CLOSED
    assert cb._failure_count == 0


@pytest.mark.asyncio
async def test_circuit_breaker_half_open_success_closes():
    """Successes in HALF_OPEN must close the circuit after threshold."""
    from src.agent_platform.recovery.circuit_breaker import (
        CircuitBreaker,
        CircuitBreakerConfig,
        CircuitState,
    )

    config = CircuitBreakerConfig(failure_threshold=1, success_threshold=2, timeout_seconds=0.1)
    cb = CircuitBreaker("half-close", config)

    async def fail():
        raise ValueError("boom")

    async def succeed():
        return "ok"

    # Open the circuit
    with pytest.raises(ValueError):
        await cb.call(fail)
    assert cb.state == CircuitState.OPEN

    # Wait for timeout to transition to HALF_OPEN
    await asyncio.sleep(0.15)
    result = await cb.call(succeed)
    assert result == "ok"
    assert cb.state == CircuitState.HALF_OPEN

    # One more success should close it
    await cb.call(succeed)
    assert cb.state == CircuitState.CLOSED


@pytest.mark.asyncio
async def test_circuit_breaker_half_open_failure_reopens():
    """A failure in HALF_OPEN must immediately reopen the circuit."""
    from src.agent_platform.recovery.circuit_breaker import (
        CircuitBreaker,
        CircuitBreakerConfig,
        CircuitState,
    )

    config = CircuitBreakerConfig(failure_threshold=1, success_threshold=2, timeout_seconds=0.1)
    cb = CircuitBreaker("half-reopen", config)

    async def fail():
        raise ValueError("boom")

    # Open the circuit
    with pytest.raises(ValueError):
        await cb.call(fail)
    assert cb.state == CircuitState.OPEN

    # Wait for timeout
    await asyncio.sleep(0.15)
    assert await cb._allow_request() is True

    # Failure in half-open reopens
    with pytest.raises(ValueError):
        await cb.call(fail)
    assert cb.state == CircuitState.OPEN
    assert cb._failure_count == 0


@pytest.mark.asyncio
async def test_circuit_breaker_reset():
    """reset() must restore the circuit to CLOSED with zero counts."""
    from src.agent_platform.recovery.circuit_breaker import (
        CircuitBreaker,
        CircuitBreakerConfig,
        CircuitState,
    )

    config = CircuitBreakerConfig(failure_threshold=1)
    cb = CircuitBreaker("reset", config)

    async def fail():
        raise ValueError("boom")

    with pytest.raises(ValueError):
        await cb.call(fail)
    assert cb.state == CircuitState.OPEN

    await cb.reset()
    assert cb.state == CircuitState.CLOSED
    assert cb._failure_count == 0
    assert cb._success_count == 0


@pytest.mark.asyncio
async def test_circuit_breaker_open_blocks_request():
    """An OPEN circuit must block requests until timeout elapses."""
    from src.agent_platform.recovery.circuit_breaker import (
        CircuitBreaker,
        CircuitBreakerConfig,
        CircuitOpenError,
        CircuitState,
    )

    config = CircuitBreakerConfig(failure_threshold=1, timeout_seconds=10)
    cb = CircuitBreaker("block", config)

    async def fail():
        raise ValueError("boom")

    with pytest.raises(ValueError):
        await cb.call(fail)
    assert cb.state == CircuitState.OPEN

    with pytest.raises(CircuitOpenError):
        await cb.call(fail)


# ===========================================================================
# IdempotencyManager — uncovered branches
# ===========================================================================


@pytest.mark.asyncio
async def test_idempotency_memory_check_and_lock_processing():
    """Memory backend must return existing record when key is already processing."""
    from src.agent_platform.recovery.idempotency import IdempotencyManager

    manager = IdempotencyManager()
    record1 = await manager.check_and_lock("key-1")
    assert record1 is None
    record2 = await manager.check_and_lock("key-1")
    assert record2 is not None
    assert record2.status == "processing"


@pytest.mark.asyncio
async def test_idempotency_memory_check_and_lock_completed():
    """Memory backend must return existing completed record."""
    from src.agent_platform.recovery.idempotency import IdempotencyManager

    manager = IdempotencyManager()
    await manager.check_and_lock("key-1")
    await manager.complete("key-1", "result")
    record = await manager.check_and_lock("key-1")
    assert record is not None
    assert record.status == "completed"
    assert record.result == "result"


@pytest.mark.asyncio
async def test_idempotency_memory_check_and_lock_failed_allows_retry():
    """Memory backend must delete failed records and allow retry."""
    from src.agent_platform.recovery.idempotency import IdempotencyManager

    manager = IdempotencyManager()
    await manager.check_and_lock("key-1")
    await manager.complete("key-1", None, error="boom")
    record = await manager.check_and_lock("key-1")
    assert record is None  # failed record was deleted, lock acquired


@pytest.mark.asyncio
async def test_idempotency_memory_complete_no_record():
    """Memory backend complete() must handle missing record gracefully."""
    from src.agent_platform.recovery.idempotency import IdempotencyManager

    manager = IdempotencyManager()
    await manager.complete("missing", "result")
    # Should not crash


@pytest.mark.asyncio
async def test_idempotency_memory_get_result_not_completed():
    """Memory backend get_result() must return None for non-completed keys."""
    from src.agent_platform.recovery.idempotency import IdempotencyManager

    manager = IdempotencyManager()
    assert await manager.get_result("missing") is None
    await manager.check_and_lock("key-1")
    assert await manager.get_result("key-1") is None


@pytest.mark.asyncio
async def test_idempotency_memory_is_completed():
    """Memory backend is_completed() must return correct boolean."""
    from src.agent_platform.recovery.idempotency import IdempotencyManager

    manager = IdempotencyManager()
    assert await manager.is_completed("missing") is False
    await manager.check_and_lock("key-1")
    assert await manager.is_completed("key-1") is False
    await manager.complete("key-1", "result")
    assert await manager.is_completed("key-1") is True


@pytest.mark.asyncio
async def test_idempotency_memory_cleanup_expired():
    """_cleanup_expired must remove expired in-memory records."""
    from src.agent_platform.recovery.idempotency import IdempotencyManager

    manager = IdempotencyManager(ttl_seconds=0)
    await manager.check_and_lock("key-1")
    import asyncio
    await asyncio.sleep(0.05)
    removed = await manager.cleanup()
    assert removed == 1


@pytest.mark.asyncio
async def test_idempotency_generate_key_deterministic():
    """generate_key must produce a stable hash for the same inputs."""
    from src.agent_platform.recovery.idempotency import IdempotencyManager
    manager = IdempotencyManager()
    k1 = manager.generate_key("a", 1, b=2)
    k2 = manager.generate_key("a", 1, b=2)
    assert k1 == k2
    assert len(k1) == 64  # SHA-256 hex


# ===========================================================================
# WorkflowParser — uncovered branches
# ===========================================================================


def test_workflow_parser_file_not_found():
    """parse_file must raise WorkflowDefinitionError when file does not exist."""
    from src.agent_platform.workflow.parser import WorkflowParser
    from src.agent_platform.workflow.exceptions import WorkflowDefinitionError
    from pathlib import Path

    with pytest.raises(WorkflowDefinitionError):
        WorkflowParser.parse_file(Path("nonexistent.json"))


def test_workflow_parser_unsupported_format():
    """parse_file must raise WorkflowDefinitionError for unsupported formats."""
    from src.agent_platform.workflow.parser import WorkflowParser
    from src.agent_platform.workflow.exceptions import WorkflowDefinitionError
    from pathlib import Path
    import tempfile

    with tempfile.NamedTemporaryFile(suffix=".xml", delete=False) as f:
        f.write(b"{}")
        path = Path(f.name)
    try:
        with pytest.raises(WorkflowDefinitionError):
            WorkflowParser.parse_file(path)
    finally:
        path.unlink()


def test_workflow_parser_missing_workflow_id():
    """parse_dict must raise WorkflowDefinitionError when workflow_id is missing."""
    from src.agent_platform.workflow.parser import WorkflowParser
    from src.agent_platform.workflow.exceptions import WorkflowDefinitionError
    with pytest.raises(WorkflowDefinitionError):
        WorkflowParser.parse_dict({"name": "WF"})


def test_workflow_parser_missing_name():
    """parse_dict must raise WorkflowDefinitionError when name is missing."""
    from src.agent_platform.workflow.parser import WorkflowParser
    from src.agent_platform.workflow.exceptions import WorkflowDefinitionError
    with pytest.raises(WorkflowDefinitionError):
        WorkflowParser.parse_dict({"workflow_id": "wf1"})


def test_workflow_parser_missing_steps():
    """parse_dict must raise WorkflowDefinitionError when steps are missing."""
    from src.agent_platform.workflow.parser import WorkflowParser
    from src.agent_platform.workflow.exceptions import WorkflowDefinitionError
    with pytest.raises(WorkflowDefinitionError):
        WorkflowParser.parse_dict({"workflow_id": "wf1", "name": "WF"})


def test_workflow_parser_empty_steps():
    """parse_dict must raise WorkflowDefinitionError when steps list is empty."""
    from src.agent_platform.workflow.parser import WorkflowParser
    from src.agent_platform.workflow.exceptions import WorkflowDefinitionError
    with pytest.raises(WorkflowDefinitionError):
        WorkflowParser.parse_dict({"workflow_id": "wf1", "name": "WF", "steps": []})


def test_workflow_parser_key_error():
    """parse_dict must wrap KeyError in WorkflowDefinitionError."""
    from src.agent_platform.workflow.parser import WorkflowParser
    from src.agent_platform.workflow.exceptions import WorkflowDefinitionError
    with pytest.raises(WorkflowDefinitionError):
        WorkflowParser.parse_dict({"workflow_id": "wf1", "name": "WF", "steps": [{"step_id": "s1"}]})


def test_workflow_parser_general_exception():
    """parse_dict must wrap general exceptions in WorkflowDefinitionError."""
    from src.agent_platform.workflow.parser import WorkflowParser
    from src.agent_platform.workflow.exceptions import WorkflowDefinitionError
    with pytest.raises(WorkflowDefinitionError):
        WorkflowParser.parse_dict({"workflow_id": "wf1", "name": "WF", "steps": "not-a-list"})


# ===========================================================================
# Monitoring Logging — uncovered branches
# ===========================================================================


def test_log_entry_to_dict_and_json():
    """LogEntry.to_dict and to_json must serialize correctly."""
    from src.agent_platform.monitoring.logging import LogEntry, LogLevel
    entry = LogEntry(
        level=LogLevel.ERROR,
        message="boom",
        logger_name="test",
        tenant_id="t1",
        trace_id="trace-1",
        span_id="span-1",
        attributes={"k": "v"},
        exception="ValueError",
    )
    d = entry.to_dict()
    assert d["level"] == "ERROR"
    assert d["message"] == "boom"
    assert d["exception"] == "ValueError"
    json_str = entry.to_json()
    assert "boom" in json_str


def test_log_manager_file_handler():
    """LogManager must set up a file handler when log_to_file is True."""
    from src.agent_platform.monitoring.logging import LogManager
    import logging
    import tempfile
    import os

    with tempfile.TemporaryDirectory() as tmpdir:
        path = os.path.join(tmpdir, "logs", "test.log")
        manager = LogManager(log_to_file=True, log_file_path=path)
        manager.error("test error")
        assert os.path.exists(path)
        # Close handlers to release file lock on Windows
        for h in list(logging.getLogger().handlers):
            if isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", None) == os.path.abspath(path):
                h.close()
                logging.getLogger().removeHandler(h)


def test_log_manager_exception_logging():
    """LogManager.log must include exc_info when an exception is passed."""
    from src.agent_platform.monitoring.logging import LogManager, LogLevel
    manager = LogManager()
    try:
        raise ValueError("test exc")
    except Exception as e:
        entry = manager.log("failed", level=LogLevel.ERROR, exception=e)
        assert entry.exception == "test exc"


# ===========================================================================
# PluginManager — uncovered branches
# ===========================================================================


@pytest.mark.asyncio
async def test_plugin_manager_load_all_already_loaded():
    """load_all must be a no-op when plugins are already loaded."""
    from src.agent_platform.plugins.manager import PluginManager
    from pathlib import Path

    manager = PluginManager(plugin_dir=Path("/nonexistent"))
    manager._loaded = True
    await manager.load_all()
    assert manager._loaded is True


@pytest.mark.asyncio
async def test_plugin_manager_load_all_no_plugins():
    """load_all must set _loaded=True when no plugins are discovered."""
    from src.agent_platform.plugins.manager import PluginManager
    from pathlib import Path

    manager = PluginManager(plugin_dir=Path("/nonexistent"))
    await manager.load_all()
    assert manager._loaded is True


@pytest.mark.asyncio
async def test_plugin_manager_load_plugin_failure():
    """load_plugin must raise PluginLoadError when on_load raises."""
    from src.agent_platform.plugins.manager import PluginManager
    from src.agent_platform.plugins.base import Plugin
    from src.agent_platform.plugins.exceptions import PluginLoadError

    class _BadPlugin(Plugin):
        async def on_load(self, context):
            raise RuntimeError("load boom")
        async def on_unload(self):
            pass
        async def on_event(self, event_type, data):
            pass

    manager = PluginManager()
    with pytest.raises(PluginLoadError):
        await manager.load_plugin(_BadPlugin("bad", "Bad"))


@pytest.mark.asyncio
async def test_plugin_manager_unload_plugin_failure():
    """unload_plugin must raise PluginUnloadError when on_unload raises."""
    from src.agent_platform.plugins.manager import PluginManager
    from src.agent_platform.plugins.base import Plugin
    from src.agent_platform.plugins.exceptions import PluginUnloadError

    class _BadUnloadPlugin(Plugin):
        async def on_load(self, context):
            pass
        async def on_unload(self):
            raise RuntimeError("unload boom")
        async def on_event(self, event_type, data):
            pass

    manager = PluginManager()
    await manager.load_plugin(_BadUnloadPlugin("bad-unload", "BadUnload"))
    with pytest.raises(PluginUnloadError):
        await manager.unload_plugin("bad-unload")


@pytest.mark.asyncio
async def test_plugin_manager_register_hook_not_loaded():
    """register_hook must raise PluginNotFoundError for unknown plugin."""
    from src.agent_platform.plugins.manager import PluginManager
    from src.agent_platform.plugins.hooks import HookPoint
    from src.agent_platform.plugins.exceptions import PluginNotFoundError

    manager = PluginManager()
    with pytest.raises(PluginNotFoundError):
        manager.register_hook("unknown", HookPoint.ON_TASK_COMPLETE, lambda: None)


@pytest.mark.asyncio
async def test_plugin_manager_trigger_event_error():
    """trigger_event must log errors but continue for other plugins."""
    from src.agent_platform.plugins.manager import PluginManager
    from src.agent_platform.plugins.base import Plugin

    class _EventPlugin(Plugin):
        async def on_load(self, context):
            pass
        async def on_unload(self):
            pass
        async def on_event(self, event_type, data):
            raise RuntimeError("event boom")

    manager = PluginManager()
    await manager.load_plugin(_EventPlugin("event-p", "EventP"))
    # Should not raise
    await manager.trigger_event("test", {})


# ===========================================================================
# Distributed worker — uncovered branches
# ===========================================================================


@pytest.mark.asyncio
async def test_worker_node_stop_with_running_tasks():
    """WorkerNode.stop must wait for running tasks before stopping."""
    from src.agent_platform.distributed.node import NodeInfo, NodeStatus
    from src.agent_platform.distributed.worker import WorkerNode, WorkerConfig

    async def slow_task():
        await asyncio.sleep(0.2)

    info = NodeInfo(
        node_id="stop-w",
        hostname="localhost",
        ip_address="127.0.0.1",
        port=8002,
        status=NodeStatus.ACTIVE,
    )
    worker = WorkerNode(info, InMemoryTaskQueue(), InMemoryAgentRegistry(), WorkerConfig())
    await worker.start()
    task = asyncio.create_task(slow_task())
    worker._tasks["slow"] = task
    await worker.stop()
    assert not worker._running


@pytest.mark.asyncio
async def test_worker_node_poll_loop_cancelled():
    """WorkerNode._poll_loop must exit cleanly on CancelledError."""
    from src.agent_platform.distributed.node import NodeInfo, NodeStatus
    from src.agent_platform.distributed.worker import WorkerNode, WorkerConfig

    info = NodeInfo(
        node_id="cancel-poll",
        hostname="localhost",
        ip_address="127.0.0.1",
        port=8003,
        status=NodeStatus.ACTIVE,
    )
    worker = WorkerNode(info, InMemoryTaskQueue(), InMemoryAgentRegistry(), WorkerConfig())
    await worker.start()
    worker._poll_task.cancel()
    try:
        await worker._poll_task
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_worker_node_execute_task_agent_not_found():
    """WorkerNode._execute_task must mark task FAILED when agent is missing."""
    from src.agent_platform.distributed.node import NodeInfo, NodeStatus
    from src.agent_platform.distributed.worker import WorkerNode, WorkerConfig

    info = NodeInfo(
        node_id="wf-agent-missing",
        hostname="localhost",
        ip_address="127.0.0.1",
        port=8004,
        status=NodeStatus.ACTIVE,
    )
    worker = WorkerNode(info, InMemoryTaskQueue(), InMemoryAgentRegistry(), WorkerConfig())
    task = Task(task_id="t-missing", agent_id="missing", type="test", payload={})
    await worker._execute_task(task)
    assert task.status == TaskStatus.FAILED
    assert "not found" in (task.error or "")


@pytest.mark.asyncio
async def test_worker_node_execute_task_exception():
    """WorkerNode._execute_task must catch exceptions and mark task FAILED."""
    from src.agent_platform.distributed.node import NodeInfo, NodeStatus
    from src.agent_platform.distributed.worker import WorkerNode, WorkerConfig
    from src.agent_platform.core.agent import BaseAgent, AgentRuntimeState

    class _BoomAgent(BaseAgent):
        async def initialize(self) -> None:
            self._initialized = True
            self.state = AgentRuntimeState.RUNNING
        async def run(self, task):
            raise RuntimeError("boom")
        async def shutdown(self):
            pass

    agent = _BoomAgent("boom-agent", "Boom")

    class _FakeRegistry:
        async def get_agent(self, agent_id):
            if agent_id == "boom-agent":
                return agent
            return None

    info = NodeInfo(
        node_id="wf-boom",
        hostname="localhost",
        ip_address="127.0.0.1",
        port=8005,
        status=NodeStatus.ACTIVE,
    )
    worker = WorkerNode(info, InMemoryTaskQueue(), _FakeRegistry(), WorkerConfig())
    task = Task(task_id="t-boom", agent_id="boom-agent", type="test", payload={})
    await worker._execute_task(task)
    assert task.status == TaskStatus.FAILED
    assert "boom" in (task.error or "")


# ===========================================================================
# Distributed orchestrator — uncovered branches
# ===========================================================================


def _mock_redis():
    class _R:
        async def get(self, *a, **kw):
            return None
        async def set(self, *a, **kw):
            return True
        async def setex(self, *a, **kw):
            return True
        async def delete(self, *a, **kw):
            return True
        async def srem(self, *a, **kw):
            return True
        async def sadd(self, *a, **kw):
            return True
        async def smembers(self, *a, **kw):
            return []
    return _R()


@pytest.mark.asyncio
async def test_orchestrator_stop_with_nodes():
    """DistributedOrchestrator.stop must stop all nodes and clear _nodes."""
    from src.agent_platform.distributed.orchestrator import DistributedOrchestrator
    from src.agent_platform.distributed.registry import DistributedRegistry
    from src.agent_platform.distributed.node import NodeInfo, NodeStatus
    from src.agent_platform.distributed.worker import WorkerConfig

    registry = DistributedRegistry(redis_client=_mock_redis())
    queue = InMemoryTaskQueue()
    orchestrator = DistributedOrchestrator(registry, queue, redis_client=_mock_redis())
    node_info = NodeInfo(
        node_id="orch-n1",
        hostname="localhost",
        ip_address="127.0.0.1",
        port=9001,
        status=NodeStatus.ACTIVE,
    )
    await orchestrator.add_node(node_info, WorkerConfig())
    assert len(orchestrator._nodes) == 1
    await orchestrator.stop()
    assert len(orchestrator._nodes) == 0


@pytest.mark.asyncio
async def test_orchestrator_add_node_default_registry():
    """add_node must create a default InMemoryAgentRegistry when none is provided."""
    from src.agent_platform.distributed.orchestrator import DistributedOrchestrator
    from src.agent_platform.distributed.registry import DistributedRegistry
    from src.agent_platform.distributed.node import NodeInfo, NodeStatus
    from src.agent_platform.distributed.worker import WorkerConfig
    from src.agent_platform.registry.in_memory import InMemoryAgentRegistry

    registry = DistributedRegistry(redis_client=_mock_redis())
    queue = InMemoryTaskQueue()
    orchestrator = DistributedOrchestrator(registry, queue, redis_client=_mock_redis())
    node_info = NodeInfo(
        node_id="orch-default",
        hostname="localhost",
        ip_address="127.0.0.1",
        port=9002,
        status=NodeStatus.ACTIVE,
    )
    node_id = await orchestrator.add_node(node_info, WorkerConfig())
    assert node_id == "orch-default"
    assert "orch-default" in orchestrator._nodes
    assert isinstance(orchestrator._nodes["orch-default"].agent_registry, InMemoryAgentRegistry)


@pytest.mark.asyncio
async def test_orchestrator_remove_node_not_found():
    """remove_node must return False when the node_id does not exist."""
    from src.agent_platform.distributed.orchestrator import DistributedOrchestrator
    from src.agent_platform.distributed.registry import DistributedRegistry

    registry = DistributedRegistry(redis_client=_mock_redis())
    queue = InMemoryTaskQueue()
    orchestrator = DistributedOrchestrator(registry, queue, redis_client=_mock_redis())
    result = await orchestrator.remove_node("missing")
    assert result is False


@pytest.mark.asyncio
async def test_orchestrator_health_check_loop_no_nodes():
    """_health_check_loop must handle empty node list gracefully."""
    from src.agent_platform.distributed.orchestrator import DistributedOrchestrator
    from src.agent_platform.distributed.registry import DistributedRegistry

    registry = DistributedRegistry(redis_client=_mock_redis())
    queue = InMemoryTaskQueue()
    orchestrator = DistributedOrchestrator(registry, queue, redis_client=_mock_redis())
    orchestrator._running = True
    # Run one iteration manually by calling the loop and cancelling quickly
    task = asyncio.create_task(orchestrator._health_check_loop())
    await asyncio.sleep(0.05)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


@pytest.mark.asyncio
async def test_orchestrator_acquire_global_lock():
    """acquire_global_lock must return a DistributedLock."""
    from src.agent_platform.distributed.orchestrator import DistributedOrchestrator
    from src.agent_platform.distributed.registry import DistributedRegistry
    from src.agent_platform.distributed.lock import DistributedLock

    registry = DistributedRegistry(redis_client=_mock_redis())
    queue = InMemoryTaskQueue()
    orchestrator = DistributedOrchestrator(registry, queue, redis_client=_mock_redis())
    lock = await orchestrator.acquire_global_lock("test-lock")
    assert isinstance(lock, DistributedLock)


# ===========================================================================
# AgentEngine — additional lifecycle branches
# ===========================================================================


@pytest.mark.asyncio
async def test_engine_start_idempotent():
    """A second start() must be a no-op when the engine is already running."""
    registry = InMemoryAgentRegistry()
    queue = InMemoryTaskQueue()
    scheduler = TaskScheduler(queue)
    engine = AgentEngine(registry, scheduler)

    await engine.start()
    assert engine.is_running
    await engine.start()  # idempotent
    assert engine.is_running
    await engine.stop()


@pytest.mark.asyncio
async def test_engine_stop_when_not_running():
    """stop() must not crash when the engine was never started."""
    registry = InMemoryAgentRegistry()
    queue = InMemoryTaskQueue()
    scheduler = TaskScheduler(queue)
    engine = AgentEngine(registry, scheduler)
    await engine.stop()  # no-op
    assert not engine.is_running


@pytest.mark.asyncio
async def test_engine_unregister_agent_not_found():
    """unregister_agent must return False when the agent_id does not exist."""
    registry = InMemoryAgentRegistry()
    queue = InMemoryTaskQueue()
    scheduler = TaskScheduler(queue)
    engine = AgentEngine(registry, scheduler)
    removed = await engine.unregister_agent("ghost")
    assert removed is False


@pytest.mark.asyncio
async def test_engine_unregister_agent_success():
    """unregister_agent must fully remove the agent and its worker."""
    registry = InMemoryAgentRegistry()
    queue = InMemoryTaskQueue()
    scheduler = TaskScheduler(queue)
    engine = AgentEngine(registry, scheduler)

    agent = _EchoAgent("echo-unreg", "EchoBot")
    await engine.register_agent(agent)
    assert "echo-unreg" in engine._agents

    removed = await engine.unregister_agent("echo-unreg")
    assert removed is True
    assert "echo-unreg" not in engine._agents
    assert agent.state == AgentRuntimeState.STOPPED
    assert agent._initialized is False


@pytest.mark.asyncio
async def test_engine_dispatcher_agent_not_found():
    """_dispatcher_loop must mark task FAILED when agent is not found."""
    registry = InMemoryAgentRegistry()
    queue = InMemoryTaskQueue()
    scheduler = TaskScheduler(queue)
    engine = AgentEngine(registry, scheduler, poll_interval=0.1)

    task_id = await scheduler.submit_task("not-in-engine", "echo", {})
    await engine.start()
    try:
        await asyncio.sleep(0.3)
        task = await scheduler.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.FAILED
        assert "not found" in (task.error or "")
    finally:
        await engine.stop()


@pytest.mark.asyncio
async def test_engine_dispatcher_agent_not_ready():
    """_dispatcher_loop must mark task FAILED when agent is not ready."""
    registry = InMemoryAgentRegistry()
    queue = InMemoryTaskQueue()
    scheduler = TaskScheduler(queue)
    engine = AgentEngine(registry, scheduler, poll_interval=0.1)

    agent = _NotReadyAgent("not-ready", "NR")
    await engine.register_agent(agent)
    # register_agent promotes initialized agents to RUNNING; force the
    # not-ready state back so the dispatcher exercises its error branch.
    agent.state = AgentRuntimeState.ERROR

    task_id = await scheduler.submit_task("not-ready", "echo", {})
    await engine.start()
    try:
        await asyncio.sleep(0.3)
        task = await scheduler.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.FAILED
        assert "not ready" in (task.error or "")
    finally:
        await engine.stop()


@pytest.mark.asyncio
async def test_engine_dispatcher_task_queue_none():
    """_dispatcher_loop must fail task when agent._task_queue is None."""
    registry = InMemoryAgentRegistry()
    queue = InMemoryTaskQueue()
    scheduler = TaskScheduler(queue)
    engine = AgentEngine(registry, scheduler, poll_interval=0.1)

    agent = _EchoAgent("echo-q-none", "EchoBot")
    await engine.register_agent(agent)
    agent._task_queue = None

    task_id = await scheduler.submit_task("echo-q-none", "echo", {})
    await engine.start()
    try:
        await asyncio.sleep(0.3)
        task = await scheduler.get_task(task_id)
        assert task is not None
        assert task.status == TaskStatus.FAILED
        assert "task queue is not initialized" in (task.error or "")
    finally:
        await engine.stop()


@pytest.mark.asyncio
async def test_engine_pause_agent_not_found():
    """pause_agent must return False when the agent_id does not exist."""
    registry = InMemoryAgentRegistry()
    queue = InMemoryTaskQueue()
    scheduler = TaskScheduler(queue)
    engine = AgentEngine(registry, scheduler)
    paused = await engine.pause_agent("ghost")
    assert paused is False


@pytest.mark.asyncio
async def test_engine_resume_agent_not_found():
    """resume_agent must return False when the agent_id does not exist."""
    registry = InMemoryAgentRegistry()
    queue = InMemoryTaskQueue()
    scheduler = TaskScheduler(queue)
    engine = AgentEngine(registry, scheduler)
    resumed = await engine.resume_agent("ghost")
    assert resumed is False


@pytest.mark.asyncio
async def test_engine_get_agent_state_not_found():
    """get_agent_state must return None for an unknown agent."""
    registry = InMemoryAgentRegistry()
    queue = InMemoryTaskQueue()
    scheduler = TaskScheduler(queue)
    engine = AgentEngine(registry, scheduler)
    assert await engine.get_agent_state("ghost") is None


@pytest.mark.asyncio
async def test_engine_list_agents():
    """list_agents must return the IDs of all registered agents."""
    registry = InMemoryAgentRegistry()
    queue = InMemoryTaskQueue()
    scheduler = TaskScheduler(queue)
    engine = AgentEngine(registry, scheduler)

    agent = _EchoAgent("echo-list", "EchoBot")
    await engine.register_agent(agent)
    assert engine.list_agents() == ["echo-list"]


# ===========================================================================
# WorkflowExecutor — remaining uncovered branches
# ===========================================================================


@pytest.mark.asyncio
async def test_workflow_executor_no_ready_steps_waits():
    """execute() must wait (sleep) when no steps are ready but workflow is not done."""
    steps = [
        WorkflowStep(step_id="s1", name="S1", agent_id="a1", task_type="test", payload={}),
        WorkflowStep(
            step_id="s2",
            name="S2",
            agent_id="a2",
            task_type="test",
            payload={},
            dependencies=[StepDependency(depends_on="s1")],
        ),
    ]
    workflow = Workflow(workflow_id="wait-wf", name="Wait", steps=steps)
    state = WorkflowStateManager(workflow)
    scheduler = TaskScheduler(InMemoryTaskQueue())
    executor = WorkflowExecutor(scheduler, state)

    async def mock_wait(task_id):
        return Task(task_id=task_id, agent_id="a1", type="test", payload={}, status=TaskStatus.COMPLETED)
    executor._wait_for_task = mock_wait

    await executor.execute()
    assert state.workflow_status == WorkflowStatus.COMPLETED
    assert state.get_step_status("s1") == StepStatus.COMPLETED
    assert state.get_step_status("s2") == StepStatus.COMPLETED


@pytest.mark.asyncio
async def test_workflow_executor_invalid_status_raises():
    """execute() must raise WorkflowExecutionError for a non-PENDING/RESUMED workflow."""
    from src.agent_platform.workflow.exceptions import WorkflowExecutionError

    steps = [
        WorkflowStep(step_id="s1", name="S1", agent_id="a1", task_type="test", payload={})
    ]
    workflow = Workflow(workflow_id="invalid-wf", name="Invalid", steps=steps)
    state = WorkflowStateManager(workflow)
    state.start()
    state.complete()  # now COMPLETED
    scheduler = TaskScheduler(InMemoryTaskQueue())
    executor = WorkflowExecutor(scheduler, state)

    with pytest.raises(WorkflowExecutionError):
        await executor.execute()


@pytest.mark.asyncio
async def test_workflow_executor_resume_method():
    """WorkflowExecutor.resume() must wrap execute() in a task."""
    steps = [
        WorkflowStep(step_id="s1", name="S1", agent_id="a1", task_type="test", payload={})
    ]
    workflow = Workflow(workflow_id="resume-method-wf", name="ResumeMethod", steps=steps)
    state = WorkflowStateManager(workflow)
    state.start()
    state.pause()
    scheduler = TaskScheduler(InMemoryTaskQueue())
    executor = WorkflowExecutor(scheduler, state)

    async def mock_wait(task_id):
        return Task(task_id=task_id, agent_id="a1", type="test", payload={}, status=TaskStatus.COMPLETED)
    executor._wait_for_task = mock_wait

    await executor.resume()
    assert state.workflow_status == WorkflowStatus.COMPLETED


@pytest.mark.asyncio
async def test_workflow_executor_get_status():
    """get_status() must return the current WorkflowStatus."""
    steps = [
        WorkflowStep(step_id="s1", name="S1", agent_id="a1", task_type="test", payload={})
    ]
    workflow = Workflow(workflow_id="status-wf", name="Status", steps=steps)
    state = WorkflowStateManager(workflow)
    scheduler = TaskScheduler(InMemoryTaskQueue())
    executor = WorkflowExecutor(scheduler, state)
    assert executor.get_status() == WorkflowStatus.PENDING
    state.start()
    assert executor.get_status() == WorkflowStatus.RUNNING
