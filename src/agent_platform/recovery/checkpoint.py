
# Checkpoint manager for saving and restoring state

import logging
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any

logger = logging.getLogger(__name__)


@dataclass
class Checkpoint:
    """A checkpoint representing saved state."""
    checkpoint_id: str
    workflow_id: str
    step_id: str | None = None
    state: dict[str, Any] = field(default_factory=dict)
    created_at: datetime = field(default_factory=lambda: datetime.now(UTC))
    metadata: dict[str, Any] = field(default_factory=dict)


class CheckpointStore(ABC):
    """
    Abstract interface for storing checkpoints.
    """

    @abstractmethod
    async def save(self, checkpoint: Checkpoint) -> None:
        pass

    @abstractmethod
    async def load(self, checkpoint_id: str) -> Checkpoint | None:
        pass

    @abstractmethod
    async def list_checkpoints(self, workflow_id: str) -> list[Checkpoint]:
        pass

    @abstractmethod
    async def delete(self, checkpoint_id: str) -> bool:
        pass


class InMemoryCheckpointStore(CheckpointStore):
    """In-memory implementation of checkpoint store."""

    def __init__(self):
        self._checkpoints: dict[str, Checkpoint] = {}
        self._workflow_map: dict[str, list[str]] = {}  # workflow_id -> list of checkpoint_ids

    async def save(self, checkpoint: Checkpoint) -> None:
        self._checkpoints[checkpoint.checkpoint_id] = checkpoint
        if checkpoint.workflow_id not in self._workflow_map:
            self._workflow_map[checkpoint.workflow_id] = []
        if checkpoint.checkpoint_id not in self._workflow_map[checkpoint.workflow_id]:
            self._workflow_map[checkpoint.workflow_id].append(checkpoint.checkpoint_id)

    async def load(self, checkpoint_id: str) -> Checkpoint | None:
        return self._checkpoints.get(checkpoint_id)

    async def list_checkpoints(self, workflow_id: str) -> list[Checkpoint]:
        checkpoint_ids = self._workflow_map.get(workflow_id, [])
        return [self._checkpoints[cid] for cid in checkpoint_ids if cid in self._checkpoints]

    async def delete(self, checkpoint_id: str) -> bool:
        if checkpoint_id in self._checkpoints:
            checkpoint = self._checkpoints[checkpoint_id]
            del self._checkpoints[checkpoint_id]
            if checkpoint.workflow_id in self._workflow_map:
                self._workflow_map[checkpoint.workflow_id] = [
                    cid for cid in self._workflow_map[checkpoint.workflow_id]
                    if cid != checkpoint_id
                ]
            return True
        return False


class CheckpointManager:
    """
    Manages checkpoints for workflows and tasks to enable recovery.
    """

    def __init__(self, store: CheckpointStore):
        self.store = store

    async def create_checkpoint(
        self,
        workflow_id: str,
        state: dict[str, Any],
        step_id: str | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> str:
        """
        Create a new checkpoint.
        """
        checkpoint_id = f"chk-{workflow_id}-{datetime.now(UTC).timestamp()}-{uuid.uuid4().hex[:8]}"
        checkpoint = Checkpoint(
            checkpoint_id=checkpoint_id,
            workflow_id=workflow_id,
            step_id=step_id,
            state=state,
            metadata=metadata or {},
        )
        await self.store.save(checkpoint)
        logger.info(f"Checkpoint {checkpoint_id} saved for workflow {workflow_id}")
        return checkpoint_id

    async def restore_checkpoint(self, checkpoint_id: str) -> Checkpoint | None:
        """
        Restore a checkpoint.
        """
        checkpoint = await self.store.load(checkpoint_id)
        if checkpoint:
            logger.info(f"Checkpoint {checkpoint_id} restored for workflow {checkpoint.workflow_id}")
        return checkpoint

    async def get_latest_checkpoint(self, workflow_id: str) -> Checkpoint | None:
        """
        Get the most recent checkpoint for a workflow.
        """
        checkpoints = await self.store.list_checkpoints(workflow_id)
        if not checkpoints:
            return None
        # Sort by created_at descending
        checkpoints.sort(key=lambda c: c.created_at, reverse=True)
        return checkpoints[0]

    async def delete_checkpoint(self, checkpoint_id: str) -> bool:
        """
        Delete a checkpoint.
        """
        return await self.store.delete(checkpoint_id)

    async def cleanup_old_checkpoints(self, workflow_id: str, keep: int = 10) -> int:
        """
        Keep only the most recent N checkpoints for a workflow.
        Returns number of deleted checkpoints.
        """
        checkpoints = await self.store.list_checkpoints(workflow_id)
        if len(checkpoints) <= keep:
            return 0
        checkpoints.sort(key=lambda c: c.created_at, reverse=True)
        to_delete = checkpoints[keep:]
        count = 0
        for c in to_delete:
            if await self.store.delete(c.checkpoint_id):
                count += 1
        return count
