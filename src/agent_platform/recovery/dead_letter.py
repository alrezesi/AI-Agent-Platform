
# Dead Letter Queue for failed messages/tasks

import json
import asyncio
import logging
from typing import List, Optional, Dict, Any
from datetime import datetime
from enum import Enum
from dataclasses import dataclass, field

from .exceptions import DeadLetterError

logger = logging.getLogger(__name__)


class DeadLetterReason(Enum):
    """Reason for moving an item to dead letter queue."""
    MAX_RETRIES_EXCEEDED = "max_retries_exceeded"
    CIRCUIT_OPEN = "circuit_open"
    NON_RETRYABLE_ERROR = "non_retryable_error"
    TIMEOUT = "timeout"
    VALIDATION_ERROR = "validation_error"
    UNKNOWN = "unknown"


@dataclass
class DeadLetterEntry:
    """An entry in the dead letter queue."""
    id: str
    source: str  # e.g., "scheduler", "message_bus", "workflow"
    original_data: Dict[str, Any]
    reason: DeadLetterReason
    error_message: Optional[str] = None
    created_at: datetime = field(default_factory=datetime.utcnow)
    retry_count: int = 0
    metadata: Dict[str, Any] = field(default_factory=dict)


class DeadLetterQueue:
    """
    Stores failed items (tasks, messages) for later inspection and replay.
    Supports in-memory and can be extended to persistent storage.
    """

    def __init__(self, max_size: int = 1000):
        self._entries: List[DeadLetterEntry] = []
        self._max_size = max_size
        self._lock = asyncio.Lock()

    async def add(
        self,
        source: str,
        data: Dict[str, Any],
        reason: DeadLetterReason,
        error_message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Add an item to the dead letter queue.
        Returns the entry ID.
        """
        async with self._lock:
            if len(self._entries) >= self._max_size:
                logger.warning(f"Dead letter queue at max size ({self._max_size}), oldest entry will be evicted")
                self._entries.pop(0)

            entry_id = f"dlq-{datetime.utcnow().timestamp()}-{len(self._entries)}"
            entry = DeadLetterEntry(
                id=entry_id,
                source=source,
                original_data=data,
                reason=reason,
                error_message=error_message,
                metadata=metadata or {},
            )
            self._entries.append(entry)
            logger.info(f"Added dead letter entry {entry_id} from {source}")
            return entry_id

    async def list_entries(
        self,
        source: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> List[DeadLetterEntry]:
        """List dead letter entries with optional filtering."""
        async with self._lock:
            entries = self._entries
            if source:
                entries = [e for e in entries if e.source == source]
            # Sort by created_at descending (newest first)
            entries = sorted(entries, key=lambda e: e.created_at, reverse=True)
            return entries[offset:offset + limit]

    async def get_entry(self, entry_id: str) -> Optional[DeadLetterEntry]:
        """Get a specific entry by ID."""
        async with self._lock:
            for entry in self._entries:
                if entry.id == entry_id:
                    return entry
            return None

    async def remove_entry(self, entry_id: str) -> bool:
        """Remove an entry from the queue."""
        async with self._lock:
            for i, entry in enumerate(self._entries):
                if entry.id == entry_id:
                    del self._entries[i]
                    return True
            return False

    async def replay(self, entry_id: str, handler: callable) -> bool:
        """
        Replay a dead letter entry by passing it to the given handler.
        If successful, removes the entry; otherwise keeps it.
        """
        entry = await self.get_entry(entry_id)
        if not entry:
            return False
        try:
            # Handler should accept the original data and return success bool or raise
            success = await handler(entry.original_data)
            if success:
                await self.remove_entry(entry_id)
                logger.info(f"Replayed and removed dead letter entry {entry_id}")
                return True
            else:
                entry.retry_count += 1
                logger.warning(f"Replay failed for entry {entry_id}, retry count {entry.retry_count}")
                return False
        except Exception as e:
            entry.retry_count += 1
            entry.metadata["last_replay_error"] = str(e)
            logger.error(f"Replay error for entry {entry_id}: {e}")
            return False

    async def clear(self) -> None:
        """Clear all entries."""
        async with self._lock:
            self._entries.clear()