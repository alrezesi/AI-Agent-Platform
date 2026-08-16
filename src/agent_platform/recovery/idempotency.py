# src/agent_platform/recovery/idempotency.py
# Idempotency manager to prevent duplicate execution

import asyncio
import hashlib
import json
import logging
from dataclasses import dataclass, field
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


class IdempotencyManager:
    """
    Manages idempotent execution to prevent duplicate processing.
    Uses a key (e.g., task_id + action) to deduplicate.
    """

    def __init__(self, ttl_seconds: int = 86400):  # 24 hours default
        self._records: dict[str, ExecutionRecord] = {}
        self._ttl_seconds = ttl_seconds
        self._lock = asyncio.Lock()

    def generate_key(self, *args, **kwargs) -> str:
        """
        Generate a unique key from arguments.
        Override this method for custom key generation.
        """
        # Combine args and kwargs into a deterministic string
        data = json.dumps({"args": args, "kwargs": kwargs}, sort_keys=True)
        return hashlib.sha256(data.encode()).hexdigest()

    async def check_and_lock(self, key: str) -> ExecutionRecord | None:
        """
        Check if a key is already being processed or completed.
        If not, lock it by creating a record with status "processing".
        Returns the existing record if found, or None if lock acquired.
        """
        async with self._lock:
            # Cleanup expired records
            self._cleanup_expired()

            existing = self._records.get(key)
            if existing:
                if existing.status == "processing":
                    # Another instance is processing; we should not proceed
                    logger.warning(f"Idempotency key {key} is already in progress")
                    return existing
                elif existing.status == "completed":
                    # Already completed; return result
                    return existing
                elif existing.status == "failed":
                    # Could optionally allow retry after failure
                    # For now, treat as failed and allow retry
                    logger.info(f"Idempotency key {key} previously failed, allowing retry")
                    # Remove old record and create new processing record
                    del self._records[key]
                # fall through to create new record

            # Create new processing record
            record = ExecutionRecord(
                key=key,
                result=None,
                status="processing",
            )
            self._records[key] = record
            return None  # Indicates lock acquired

    async def complete(self, key: str, result: Any, error: str | None = None) -> None:
        """
        Mark an idempotent operation as completed (success or failure).
        """
        async with self._lock:
            record = self._records.get(key)
            if not record:
                logger.warning(f"No record found for key {key} to complete")
                return
            record.status = "failed" if error else "completed"
            record.result = result if not error else None
            record.error = error
            record.completed_at = datetime.now(UTC)

    async def get_result(self, key: str) -> Any | None:
        """
        Get the result of a completed idempotent operation.
        """
        async with self._lock:
            record = self._records.get(key)
            if record and record.status == "completed":
                return record.result
            return None

    async def is_completed(self, key: str) -> bool:
        """
        Check if an operation with the given key has been completed.
        """
        async with self._lock:
            record = self._records.get(key)
            return record is not None and record.status == "completed"

    def _cleanup_expired(self) -> None:
        """Remove records older than TTL."""
        now = datetime.now(UTC)
        expired_keys = [
            key for key, rec in self._records.items()
            if (now - rec.started_at).total_seconds() > self._ttl_seconds
        ]
        for key in expired_keys:
            del self._records[key]
            logger.debug(f"Removed expired idempotency record for key {key}")

    async def cleanup(self) -> int:
        """
        Manually trigger cleanup of expired records.
        Returns number of records removed.
        """
        async with self._lock:
            before = len(self._records)
            self._cleanup_expired()
            after = len(self._records)
            return before - after
