"""Synthetic load test harness for the hardened task pipeline."""

from __future__ import annotations

import argparse
import asyncio
import json
import time
from dataclasses import dataclass, asdict
from typing import Any, Dict, List
import logging

from src.agent_platform.core.agent import BaseAgent, AgentRuntimeState
from src.agent_platform.core.task import Task
from src.agent_platform.scheduler.in_memory import InMemoryTaskQueue
from src.agent_platform.scheduler.scheduler import TaskScheduler
from src.agent_platform.scheduler.worker import TaskWorker

logging.basicConfig(level=logging.ERROR, format="%(message)s")
logging.getLogger("src.agent_platform.scheduler.worker").setLevel(logging.ERROR)


class FastLoadAgent(BaseAgent):
    def __init__(self, agent_id: str = "default-agent", name: str = "load-agent"):
        super().__init__(agent_id, name)
        self._attempts: Dict[str, int] = {}

    async def initialize(self) -> None:
        self.state = AgentRuntimeState.RUNNING
        self._initialized = True

    async def run(self, task: Task) -> Any:
        payload = task.payload or {}
        await asyncio.sleep(float(payload.get("sleep_seconds", 0.001)))
        fail_times = int(payload.get("fail_times", 0))
        if fail_times > 0:
            count = self._attempts.get(task.task_id, 0) + 1
            self._attempts[task.task_id] = count
            if count <= fail_times:
                raise ValueError("injected load-test failure")
        return {"task_id": task.task_id, "ok": True}

    async def shutdown(self) -> None:
        self.state = AgentRuntimeState.STOPPED
        self._initialized = False


@dataclass
class LoadMetrics:
    total_tasks: int
    workers: int
    concurrent_requests: int
    throughput: float
    latency_p50: float
    latency_p95: float
    latency_p99: float
    error_rate: float
    retry_rate: float
    max_queue_depth: int
    elapsed_seconds: float


def _percentile(values: List[float], percentile: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    index = int(round((len(values) - 1) * percentile))
    return values[index]


async def run_load_test(total_tasks: int = 1000, concurrent_requests: int = 100, workers: int = 5) -> LoadMetrics:
    queue = InMemoryTaskQueue()
    scheduler = TaskScheduler(queue)
    agent = FastLoadAgent()
    await agent.initialize()

    created_task_ids: List[str] = []
    queue_depth_samples: List[int] = []
    stop_sampling = asyncio.Event()

    async def sample_queue_depth() -> None:
        while not stop_sampling.is_set():
            queue_depth_samples.append(await scheduler.queue_size())
            await asyncio.sleep(0.01)

    completed = 0
    completed_lock = asyncio.Lock()

    async def worker_loop() -> None:
        nonlocal completed
        while True:
            async with completed_lock:
                if completed >= total_tasks:
                    return
            task = await scheduler.dequeue_next()
            if not task:
                await asyncio.sleep(0.001)
                continue
            worker = TaskWorker(task, agent, retry_delay_base=0.001, retry_delay_max=0.01)
            result = await worker.execute()
            await scheduler.on_task_completed(result)
            async with completed_lock:
                completed += 1

    async def submit_batch(batch_index: int) -> None:
        for i in range(total_tasks // concurrent_requests):
            sequence = batch_index * (total_tasks // concurrent_requests) + i
            payload = {"sleep_seconds": 0.001}
            if sequence % 10 == 0:
                payload["fail_times"] = 1
            task_id = await scheduler.submit_task(
                agent_id="default-agent",
                task_type="load",
                payload=payload,
                task_id=f"load-{sequence}",
                max_retries=1,
            )
            created_task_ids.append(task_id)

    started = time.perf_counter()
    sampler = asyncio.create_task(sample_queue_depth())
    workers_tasks = [asyncio.create_task(worker_loop()) for _ in range(workers)]
    submitters = [asyncio.create_task(submit_batch(i)) for i in range(concurrent_requests)]
    await asyncio.gather(*submitters)
    await asyncio.gather(*workers_tasks)
    stop_sampling.set()
    await sampler
    elapsed = time.perf_counter() - started

    tasks = [await scheduler.get_task(task_id) for task_id in created_task_ids]
    latencies = [
        (task.completed_at - task.created_at).total_seconds()
        for task in tasks
        if task and task.completed_at
    ]
    failed_tasks = [task for task in tasks if task and task.status.value == "failed"]
    retry_count = sum(task.retry_count for task in tasks if task)

    return LoadMetrics(
        total_tasks=total_tasks,
        workers=workers,
        concurrent_requests=concurrent_requests,
        throughput=round(total_tasks / elapsed, 2),
        latency_p50=round(_percentile(latencies, 0.50), 4),
        latency_p95=round(_percentile(latencies, 0.95), 4),
        latency_p99=round(_percentile(latencies, 0.99), 4),
        error_rate=round(len(failed_tasks) / total_tasks, 4),
        retry_rate=round(retry_count / total_tasks, 4),
        max_queue_depth=max(queue_depth_samples) if queue_depth_samples else 0,
        elapsed_seconds=round(elapsed, 3),
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tasks", type=int, default=1000)
    parser.add_argument("--concurrency", type=int, default=100)
    parser.add_argument("--workers", type=int, default=5)
    args = parser.parse_args()

    metrics = asyncio.run(
        run_load_test(
            total_tasks=args.tasks,
            concurrent_requests=args.concurrency,
            workers=args.workers,
        )
    )
    print(json.dumps(asdict(metrics), indent=2))


if __name__ == "__main__":
    main()
