# tests/unit/test_recovery.py
# Unit tests for recovery components

import pytest
import asyncio
import time

from src.agent_platform.recovery.retry import (
    FixedDelayRetry,
    ExponentialBackoffRetry,
    RetryExecutor,
    RetryExhaustedError,
)
from src.agent_platform.recovery.circuit_breaker import (
    CircuitBreaker,
    CircuitBreakerConfig,
    CircuitState,
    CircuitOpenError,
)
from src.agent_platform.recovery.dead_letter import (
    DeadLetterQueue,
    DeadLetterReason,
)
from src.agent_platform.recovery.checkpoint import (
    CheckpointManager,
    InMemoryCheckpointStore,
)
from src.agent_platform.recovery.idempotency import IdempotencyManager


# --- Retry Tests ---

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
async def test_retry_exhausted():
    policy = FixedDelayRetry(delay=0.01, max_retries=2)
    executor = RetryExecutor(policy)

    async def func():
        raise ValueError("always fail")

    with pytest.raises(RetryExhaustedError):
        await executor.execute(func)


# --- Circuit Breaker Tests ---

@pytest.mark.asyncio
async def test_circuit_breaker():
    config = CircuitBreakerConfig(failure_threshold=2, timeout_seconds=1)
    cb = CircuitBreaker("test", config)
    call_count = 0

    async def failing_func():
        nonlocal call_count
        call_count += 1
        raise ValueError("failure")

    # First two calls should fail, circuit opens
    for _ in range(2):
        with pytest.raises(ValueError):
            await cb.call(failing_func)

    assert cb.state == CircuitState.OPEN
    assert call_count == 2

    # Next call should be blocked
    with pytest.raises(CircuitOpenError):
        await cb.call(failing_func)

    assert call_count == 2  # not executed


# --- Dead Letter Queue Tests ---

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

    # Replay with successful handler
    async def handler(data):
        return True

    success = await dlq.replay(entry_id, handler)
    assert success is True
    entries_after = await dlq.list_entries()
    assert len(entries_after) == 0


# --- Checkpoint Tests ---

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


# --- Idempotency Tests ---

@pytest.mark.asyncio
async def test_idempotency():
    manager = IdempotencyManager()

    key = "test-key"

    # First call: lock acquired (returns None)
    record = await manager.check_and_lock(key)
    assert record is None

    # Second call before completion: should return processing record
    record2 = await manager.check_and_lock(key)
    assert record2 is not None
    assert record2.status == "processing"

    # Complete the first execution
    await manager.complete(key, "result")

    # Third call should return completed record
    record3 = await manager.check_and_lock(key)
    assert record3 is not None
    assert record3.status == "completed"
    assert record3.result == "result"

    # Get result
    result = await manager.get_result(key)
    assert result == "result"
    assert await manager.is_completed(key) is True