# Worker node for executing tasks in a distributed environment

import asyncio
import logging
from dataclasses import dataclass
from typing import Any

from ..core.task import Task, TaskStatus
from ..scheduler.base import BaseTaskQueue
from ..scheduler.worker import TaskWorker
from .node import Node, NodeInfo

logger = logging.getLogger(__name__)


@dataclass
class WorkerConfig:
    """Configuration for a worker node."""
    max_concurrent_tasks: int = 5
    poll_interval: float = 0.5
    node_heartbeat_interval: float = 10.0
    task_timeout_seconds: int = 60


class WorkerNode(Node):
    """
    A worker node that executes tasks from a distributed task queue.
    """

    def __init__(
        self,
        info: NodeInfo,
        queue: BaseTaskQueue,
        agent_registry: Any,  # AgentRegistry for retrieving agents
        config: WorkerConfig | None = None,
    ):
        super().__init__(info)
        self.queue = queue
        self.agent_registry = agent_registry
        self.config = config or WorkerConfig()
        self._tasks: dict = {}  # task_id -> asyncio.Task
        self._semaphore = asyncio.Semaphore(self.config.max_concurrent_tasks)
        self._heartbeat_task: asyncio.Task | None = None
        self._poll_task: asyncio.Task | None = None
        self._recovery_task: asyncio.Task | None = None

    async def start(self) -> None:
        """Start the worker node."""
        await super().start()
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())
        self._poll_task = asyncio.create_task(self._poll_loop())
        self._recovery_task = asyncio.create_task(self._recovery_loop())
        logger.info(f"Worker node {self.info.node_id} started with {self.config.max_concurrent_tasks} concurrent tasks")

    async def stop(self) -> None:
        """Stop the worker node and cancel all running tasks."""
        self._running = False

        # Cancel heartbeat and poll tasks
        if self._heartbeat_task:
            self._heartbeat_task.cancel()
        if self._poll_task:
            self._poll_task.cancel()
        if self._recovery_task:
            self._recovery_task.cancel()

        # Wait for all running tasks to complete
        if self._tasks:
            logger.info(f"Waiting for {len(self._tasks)} tasks to complete...")
            await asyncio.gather(*self._tasks.values(), return_exceptions=True)
            self._tasks.clear()

        await super().stop()
        logger.info(f"Worker node {self.info.node_id} stopped")

    async def _heartbeat_loop(self) -> None:
        """Periodically send heartbeats."""
        while self._running:
            try:
                await self.heartbeat()
                # Also update node status in registry
                await self.agent_registry.update_node_status(self.info)
                await asyncio.sleep(self.config.node_heartbeat_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in heartbeat loop: {e}")
                await asyncio.sleep(5)

    async def _poll_loop(self) -> None:
        """
        Poll the distributed task queue for new tasks.
        """
        while self._running:
            try:
                if hasattr(self.queue, "reclaim_expired_tasks"):
                    await self.queue.reclaim_expired_tasks()
                # Get a task from the queue
                task = await self.queue.dequeue(
                    worker_id=self.info.node_id,
                    lease_seconds=self.config.task_timeout_seconds,
                )
                if task:
                    # Execute the task with concurrency limit
                    task_runner = asyncio.create_task(self._execute_task_with_semaphore(task))
                    self._tasks[task.task_id] = task_runner
                    task_runner.add_done_callback(self._make_task_cleanup_callback(task.task_id))
                else:
                    await asyncio.sleep(self.config.poll_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in poll loop: {e}")
                await asyncio.sleep(1)

    async def _recovery_loop(self) -> None:
        """Periodically recover tasks after Redis outages or worker loss."""
        while self._running:
            try:
                if hasattr(self.queue, "recover_orphaned_tasks"):
                    await self.queue.recover_orphaned_tasks()
                await asyncio.sleep(max(self.config.poll_interval, 1.0))
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Error in recovery loop: {e}")
                await asyncio.sleep(1)

    async def _execute_task_with_semaphore(self, task: Task) -> None:
        """
        Execute a task with semaphore to limit concurrency.
        """
        async with self._semaphore:
            await self._execute_task(task)

    async def _execute_task(self, task: Task) -> None:
        """
        Execute a single task using the TaskWorker.
        """
        task_id = task.task_id
        logger.info(f"Worker {self.info.node_id} executing task {task_id}")

        try:
            # Get the agent for this task
            agent = await self.agent_registry.get_agent(task.agent_id)
            if not agent:
                logger.error(f"Agent {task.agent_id} not found for task {task_id}")
                task.status = TaskStatus.FAILED
                task.error = "Agent not found"
                await self.queue.update_task(task)
                return

            # Execute using TaskWorker
            worker = TaskWorker(task, agent)
            result = await worker.execute()

            # Store result back in the queue
            await self.queue.update_task(result)

            logger.info(f"Worker {self.info.node_id} completed task {task_id} with status {result.status}")

        except Exception as e:
            logger.error(f"Worker {self.info.node_id} failed to execute task {task_id}: {e}")
            task.status = TaskStatus.FAILED
            task.error = str(e)
            await self.queue.update_task(task)

    def _make_task_cleanup_callback(self, task_id: str):
        def _cleanup(_: asyncio.Task[Any]) -> None:
            self._tasks.pop(task_id, None)

        return _cleanup
