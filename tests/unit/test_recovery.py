from __future__ import annotations

import pytest

from src.agent_platform.recovery.checkpoint import CheckpointManager, InMemoryCheckpointStore
from src.agent_platform.recovery.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitOpenError,
    CircuitState,
)
from src.agent_platform.recovery.dead_letter import DeadLetterQueue, DeadLetterReason
from src.agent_platform.recovery.idempotency import IdempotencyManager
from src.agent_platform.recovery.retry import (
    FixedDelayRetry,
    RetryExecutor,
    RetryExhaustedError,
)


@pytest.mark.asyncio
async def test_fixed_retry_success():
    policy = FixedDelayRetry(delay=0.01, max_retries=3)
    executor = RetryExecutor(policy)
    call_count = 0

    async def func():
        nonlocal call_count
        call_count += 1
        if call_count < 2:
            raise ValueError("temporary")
        return "success"

    result = await executor.execute(func)
    assert result == "success"
    assert call_count == 2


@pytest.mark.asyncio
async def test_fixed_retry_fail_fail_success():
    policy = FixedDelayRetry(delay=0.01, max_retries=3)
    executor = RetryExecutor(policy)
    call_count = 0

    async def func():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise ValueError("temporary")
        return "success"

    result = await executor.execute(func)
    assert result == "success"
    assert call_count == 3


@pytest.mark.asyncio
async def test_retry_exhausted():
    policy = FixedDelayRetry(delay=0.01, max_retries=2)
    executor = RetryExecutor(policy)

    async def func():
        raise ValueError("always fail")

    with pytest.raises(RetryExhaustedError):
        await executor.execute(func)


@pytest.mark.asyncio
async def test_retry_exhausted_after_four_failures():
    policy = FixedDelayRetry(delay=0.01, max_retries=3)
    executor = RetryExecutor(policy)
    call_count = 0

    async def func():
        nonlocal call_count
        call_count += 1
        raise ValueError("still failing")

    with pytest.raises(RetryExhaustedError):
        await executor.execute(func)
    assert call_count == 4


@pytest.mark.asyncio
async def test_circuit_breaker():
    config = CircuitBreakerConfig(failure_threshold=2, timeout_seconds=1)
    cb = CircuitBreaker("test", config)
    call_count = 0

    async def failing_func():
        nonlocal call_count
        call_count += 1
        raise ValueError("failure")

    for _ in range(2):
        with pytest.raises(ValueError):
            await cb.call(failing_func)

    assert cb.state == CircuitState.OPEN
    assert call_count == 2

    with pytest.raises(CircuitOpenError):
        await cb.call(failing_func)

    assert call_count == 2


@pytest.mark.asyncio
async def test_dead_letter():
    dlq = DeadLetterQueue(max_size=10)
    entry_id = await dlq.add(
        source="test",
        data={"msg": "hello"},
        reason=DeadLetterReason.MAX_RETRIES_EXCEEDED,
        error_message="Retries exhausted",
    )
    entries = await dlq.list_entries()
    assert len(entries) == 1
    assert entries[0].id == entry_id

    async def handler(data):
        return True

    success = await dlq.replay(entry_id, handler)
    assert success is True
    entries_after = await dlq.list_entries()
    assert len(entries_after) == 0


@pytest.mark.asyncio
async def test_checkpoint():
    store = InMemoryCheckpointStore()
    manager = CheckpointManager(store)

    chk_id = await manager.create_checkpoint("wf1", {"step": "done"}, step_id="s1")
    assert chk_id is not None

    chk = await manager.restore_checkpoint(chk_id)
    assert chk is not None
    assert chk.workflow_id == "wf1"
    assert chk.state == {"step": "done"}

    latest = await manager.get_latest_checkpoint("wf1")
    assert latest is not None
    assert latest.checkpoint_id == chk_id


@pytest.mark.asyncio
async def test_idempotency():
    manager = IdempotencyManager()
    key = "test-key"

    record = await manager.check_and_lock(key)
    assert record is None

    record2 = await manager.check_and_lock(key)
    assert record2 is not None
    assert record2.status == "processing"

    await manager.complete(key, "result")

    record3 = await manager.check_and_lock(key)
    assert record3 is not None
    assert record3.status == "completed"
    assert record3.result == "result"

    result = await manager.get_result(key)
    assert result == "result"
    assert await manager.is_completed(key) is True


@pytest.mark.asyncio
async def test_dead_letter_replay_failure():
    dlq = DeadLetterQueue()
    entry_id = await dlq.add(
        source="test",
        data={"msg": "hello"},
        reason=DeadLetterReason.MAX_RETRIES_EXCEEDED,
    )

    async def failing_handler(data):
        raise ValueError("Replay failed")

    success = await dlq.replay(entry_id, failing_handler)
    assert success is False

    entry = await dlq.get_entry(entry_id)
    assert entry is not None
    assert entry.retry_count == 1


@pytest.mark.asyncio
async def test_checkpoint_cleanup():
    store = InMemoryCheckpointStore()
    manager = CheckpointManager(store)
    for i in range(15):
        await manager.create_checkpoint("wf1", {"step": i})

    removed = await manager.cleanup_old_checkpoints("wf1", keep=10)
    assert removed == 5

    checkpoints = await store.list_checkpoints("wf1")
    assert len(checkpoints) == 10
