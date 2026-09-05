from __future__ import annotations

import asyncio
import os
from typing import Any

from redis.asyncio import Redis

from src.agent_platform.core.agent import AgentRuntimeState, BaseAgent
from src.agent_platform.core.task import Task


class SimpleTaskAgent(BaseAgent):
    """A lightweight real agent used for deterministic integration and Docker E2E tests."""

    async def initialize(self) -> None:
        self._initialized = True
        self.state = AgentRuntimeState.RUNNING

    async def run(self, task: Task) -> Any:
        redis_url = os.getenv("EXECUTION_COUNTER_REDIS_URL")
        execution_count = None
        if redis_url:
            redis = Redis.from_url(redis_url, decode_responses=True, max_connections=2000)
            try:
                execution_count = await redis.incr(f"executions:{task.task_id}")
            finally:
                await redis.aclose()
        delay = float(task.payload.get("delay_seconds", 0) or 0)
        if delay > 0:
            await asyncio.sleep(delay)
        return {
            "task_id": task.task_id,
            "agent_id": self.agent_id,
            "worker": task.payload.get("worker"),
            "echo": task.payload.get("message"),
            "payload": task.payload,
            "execution_count": execution_count,
        }

    async def shutdown(self) -> None:
        self._initialized = False
        self.state = AgentRuntimeState.STOPPED
