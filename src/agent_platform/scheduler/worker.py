
# TaskWorker: Executes a single task with timeout and retry logic

import asyncio
import logging
from typing import Optional, Callable, Awaitable, Any
from datetime import datetime

from src.agent_platform.core.task import Task, TaskStatus
from src.agent_platform.core.agent import BaseAgent

logger = logging.getLogger(__name__)


class TaskWorker:
    """
    Executes a given task on an agent with:
    - Timeout control
    - Retry with exponential backoff
    - Status and result handling
    """

    def __init__(
        self,
        task: Task,
        agent: BaseAgent,
        retry_delay_base: float = 1.0,   # seconds
        retry_delay_max: float = 60.0,   # seconds
    ):
        self.task = task
        self.agent = agent
        self.retry_delay_base = retry_delay_base
        self.retry_delay_max = retry_delay_max

    async def execute(self) -> Task:
        """
        Execute the task with retry logic.
        Returns the updated task with result or error.
        """
        attempt = 0
        while attempt <= self.task.max_retries:
            try:
                # Execute with timeout
                result = await asyncio.wait_for(
                    self.agent.run(self.task),
                    timeout=self.task.timeout_seconds
                )
                # Success
                self.task.status = TaskStatus.COMPLETED
                self.task.result = result
                self.task.completed_at = datetime.utcnow()
                logger.info(f"Task {self.task.task_id} completed successfully on attempt {attempt+1}")
                break

            except asyncio.TimeoutError:
                self.task.retry_count = attempt + 1
                if attempt >= self.task.max_retries:
                    self.task.status = TaskStatus.TIMEOUT
                    self.task.error = f"Task timed out after {self.task.timeout_seconds}s"
                    logger.error(f"Task {self.task.task_id} timed out")
                    break
                # Retry with backoff
                delay = min(
                    self.retry_delay_base * (2 ** attempt),
                    self.retry_delay_max
                )
                logger.warning(f"Task {self.task.task_id} timed out, retrying in {delay:.2f}s (attempt {attempt+1}/{self.task.max_retries})")
                await asyncio.sleep(delay)

            except Exception as e:
                self.task.retry_count = attempt + 1
                if attempt >= self.task.max_retries:
                    self.task.status = TaskStatus.FAILED
                    self.task.error = str(e)
                    logger.error(f"Task {self.task.task_id} failed: {e}")
                    break
                # Retry with backoff
                delay = min(
                    self.retry_delay_base * (2 ** attempt),
                    self.retry_delay_max
                )
                logger.warning(f"Task {self.task.task_id} failed, retrying in {delay:.2f}s (attempt {attempt+1}/{self.task.max_retries})")
                await asyncio.sleep(delay)

            attempt += 1

        # Update completed_at if not set (e.g., if it failed without setting)
        if self.task.completed_at is None and self.task.status in (TaskStatus.FAILED, TaskStatus.TIMEOUT, TaskStatus.CANCELLED):
            self.task.completed_at = datetime.utcnow()

        return self.task