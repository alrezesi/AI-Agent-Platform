"""Redis-backed worker process for the chaos/hardening deployment."""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
from collections import defaultdict
from typing import Any, Dict

from src.agent_platform.core.agent import BaseAgent, AgentRuntimeState
from src.agent_platform.core.task import Task
from src.agent_platform.runtime import get_task_queue
from src.agent_platform.scheduler.worker import TaskWorker

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
logger = logging.getLogger("agent_platform.worker")


class ChaosWorkerAgent(BaseAgent):
    """Simple worker-side agent used by the integration harness."""

    def __init__(self, agent_id: str, name: str, tenant_id: str | None = None):
        super().__init__(agent_id, name, tenant_id)
        self._attempts: Dict[str, int] = defaultdict(int)

    async def initialize(self) -> None:
        self.state = AgentRuntimeState.RUNNING
        self._initialized = True

    async def run(self, task: Task) -> Any:
        payload = task.payload or {}
        delay_seconds = float(payload.get("delay_seconds", 0))
        if delay_seconds > 0:
            await asyncio.sleep(delay_seconds)

        fail_times = int(payload.get("fail_times", 0))
        if fail_times > 0:
            self._attempts[task.task_id] += 1
            if self._attempts[task.task_id] <= fail_times:
                raise ValueError(f"Injected failure {self._attempts[task.task_id]}/{fail_times}")

        return {
            "task_id": task.task_id,
            "worker_agent_id": self.agent_id,
            "worker_name": self.name,
            "payload": payload,
        }

    async def shutdown(self) -> None:
        self.state = AgentRuntimeState.STOPPED
        self._initialized = False


async def run_worker(worker_id: str, lease_seconds: float) -> None:
    queue = get_task_queue()
    agent = ChaosWorkerAgent(agent_id="default-agent", name=worker_id)
    await agent.initialize()

    if hasattr(queue, "recover_orphaned_tasks"):
        await queue.recover_orphaned_tasks()

    stop_event = asyncio.Event()

    def _signal_handler() -> None:
        stop_event.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            pass

    logger.info("Worker %s started", worker_id)
    try:
        while not stop_event.is_set():
            if hasattr(queue, "reclaim_orphaned_tasks"):
                await queue.reclaim_orphaned_tasks()
            if hasattr(queue, "reclaim_expired_tasks"):
                await queue.reclaim_expired_tasks()

            task = await queue.dequeue(worker_id=worker_id, lease_seconds=lease_seconds)
            if task is None:
                await asyncio.sleep(0.25)
                continue

            worker = TaskWorker(task, agent, retry_delay_base=0.1, retry_delay_max=2.0)
            result = await worker.execute()
            await queue.update_task(result)
    finally:
        await agent.shutdown()
        logger.info("Worker %s stopped", worker_id)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--worker-id", required=True)
    parser.add_argument("--lease-seconds", type=float, default=30.0)
    args = parser.parse_args()
    asyncio.run(run_worker(args.worker_id, args.lease_seconds))


if __name__ == "__main__":
    main()
