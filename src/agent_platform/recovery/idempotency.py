# src/agent_platform/recovery/idempotency.py
# Idempotency manager to prevent duplicate execution / duplicate enqueue.
#
# Two backends are supported:
#
#  * In-memory (``redis_client=None``) — process-local fallback that preserves
#    the original behaviour.  Useful for unit tests and single-process
#    contexts.  NOT safe across worker processes.
#
#  * Redis-backed (``redis_client`` supplied) — the record store lives in
#    Redis and lock acquisition uses ``SET NX``, so the manager works
#    correctly across the separate API/worker processes.  This is the backend
#    used when the manager is wired into the real enqueue path.

import asyncio
import json
import logging
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class ExecutionRecord:
    """Record of an executed idempotent operation."""

    key: str
    result: Any
    status: str  # "completed", "failed", "processing"
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    completed_at: datetime | None = None
    error: str | None = None

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        # datetimes are not JSON-native; serialize to ISO strings.
        data["started_at"] = self.started_at.isoformat()
        data["completed_at"] = self.completed_at.isoformat() if self.completed_at else None
        return data

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ExecutionRecord":
        started = data.get("started_at")
        if isinstance(started, str):
            data["started_at"] = datetime.fromisoformat(started)
        completed = data.get("completed_at")
        if isinstance(completed, str):
            data["completed_at"] = datetime.fromisoformat(completed)
        return cls(**data)


class IdempotencyManager:
    """
    Manages idempotent execution to prevent duplicate processing.

    Uses a key (e.g., task_id) to deduplicate.  When ``redis_client`` is
    supplied, records are stored in Redis and lock acquisition is performed
    with ``SET NX``, making the manager safe across separate processes
    (the in-memory ``asyncio.Lock`` + ``dict`` implementation is process-local
    only and would not work across the Docker workers).
    """

    def __init__(self, redis_client: Any = None, ttl_seconds: int = 86400) -> None:
        self._redis = redis_client
        self._ttl_seconds = ttl_seconds
        # Process-local store — only used when no Redis client is provided.
        self._records: dict[str, ExecutionRecord] = {}
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Key helpers
    # ------------------------------------------------------------------
    def _redis_key(self, key: str) -> str:
        return f"idempotency:{key}"

    # ------------------------------------------------------------------
    # Redis-backed operations
    # ------------------------------------------------------------------
    async def _check_and_lock_redis(self, key: str) -> ExecutionRecord | None:
        redis_key = self._redis_key(key)
        data = await self._redis.get(redis_key)
        if data:
            record = ExecutionRecord.from_dict(json.loads(data))
            if record.status in ("processing", "completed"):
                # Already handled / in progress elsewhere; do not proceed.
                return record
            # Previously failed: allow retry by clearing the stale record.
            await self._redis.delete(redis_key)

        # Attempt to acquire the lock atomically across processes.
        record = ExecutionRecord(key=key, result=None, status="processing")
        acquired = await self._redis.set(
            redis_key,
            json.dumps(record.to_dict()),
            nx=True,
            ex=self._ttl_seconds,
        )
        if acquired:
            return None  # lock acquired; caller may proceed

        # Lost the race: another process acquired it first.  Return its record
        # so the caller knows the key is already in flight.
        data = await self._redis.get(redis_key)
        if data:
            return ExecutionRecord.from_dict(json.loads(data))
        return None

    async def _complete_redis(self, key: str, result: Any, error: str | None) -> None:
        redis_key = self._redis_key(key)
        record = ExecutionRecord(
            key=key,
            result=result if not error else None,
            status="failed" if error else "completed",
            error=error,
            completed_at=datetime.now(UTC),
        )
        await self._redis.set(
            redis_key,
            json.dumps(record.to_dict()),
            ex=self._ttl_seconds,
        )

    async def _get_redis(self, key: str) -> ExecutionRecord | None:
        data = await self._redis.get(self._redis_key(key))
        if not data:
            return None
        return ExecutionRecord.from_dict(json.loads(data))

    # ------------------------------------------------------------------
    # In-memory operations (original behaviour, process-local)
    # ------------------------------------------------------------------
    async def _check_and_lock_memory(self, key: str) -> ExecutionRecord | None:
        async with self._lock:
            self._cleanup_expired()

            existing = self._records.get(key)
            if existing:
                if existing.status == "processing":
                    logger.warning(f"Idempotency key {key} is already in progress")
                    return existing
                elif existing.status == "completed":
                    return existing
                elif existing.status == "failed":
                    logger.info(f"Idempotency key {key} previously failed, allowing retry")
                    del self._records[key]

            record = ExecutionRecord(key=key, result=None, status="processing")
            self._records[key] = record
            return None  # lock acquired

    async def _complete_memory(self, key: str, result: Any, error: str | None) -> None:
        async with self._lock:
            record = self._records.get(key)
            if not record:
                logger.warning(f"No record found for key {key} to complete")
                return
            record.status = "failed" if error else "completed"
            record.result = result if not error else None
            record.error = error
            record.completed_at = datetime.now(UTC)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def generate_key(self, *args: Any, **kwargs: Any) -> str:
        """Generate a unique key from arguments (deterministic)."""
        import hashlib

        data = json.dumps({"args": args, "kwargs": kwargs}, sort_keys=True)
        return hashlib.sha256(data.encode()).hexdigest()

    async def check_and_lock(self, key: str) -> ExecutionRecord | None:
        """
        Check if a key is already being processed or completed.
        If not, lock it by creating a record with status "processing".

        Returns the existing record if found, or ``None`` if the lock was
        acquired (caller may proceed).
        """
        if self._redis is not None:
            return await self._check_and_lock_redis(key)
        return await self._check_and_lock_memory(key)

    async def complete(self, key: str, result: Any, error: str | None = None) -> None:
        """Mark an idempotent operation as completed (success or failure)."""
        if self._redis is not None:
            return await self._complete_redis(key, result, error)
        return await self._complete_memory(key, result, error)

    async def get_result(self, key: str) -> Any | None:
        """Get the result of a completed idempotent operation."""
        if self._redis is not None:
            record = await self._get_redis(key)
            if record and record.status == "completed":
                return record.result
            return None
        async with self._lock:
            record = self._records.get(key)
            if record and record.status == "completed":
                return record.result
            return None

    async def is_completed(self, key: str) -> bool:
        """Check if an operation with the given key has been completed."""
        if self._redis is not None:
            record = await self._get_redis(key)
            return record is not None and record.status == "completed"
        async with self._lock:
            record = self._records.get(key)
            return record is not None and record.status == "completed"

    def _cleanup_expired(self) -> None:
        """Remove in-memory records older than TTL (no-op for Redis; TTL is server-side)."""
        now = datetime.now(UTC)
        expired_keys = [
            key for key, rec in self._records.items()
            if (now - rec.started_at).total_seconds() > self._ttl_seconds
        ]
        for key in expired_keys:
            del self._records[key]
            logger.debug(f"Removed expired idempotency record for key {key}")

    async def cleanup(self) -> int:
        """Manually trigger cleanup of expired records. Returns number removed."""
        if self._redis is not None:
            # Redis expires keys server-side; nothing to reap here.
            return 0
        async with self._lock:
            before = len(self._records)
            self._cleanup_expired()
            after = len(self._records)
            return before - after
