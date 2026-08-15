# scripts/chaos_load_test.py
# Real infrastructure load test using Redis + PostgreSQL + Docker Workers

import asyncio
import time
import argparse
import statistics
import json
import httpx
from datetime import datetime
from typing import List, Dict, Any
from collections import defaultdict

# Import real queue and scheduler
from src.agent_platform.scheduler.redis_queue import RedisTaskQueue
from src.agent_platform.scheduler.scheduler import TaskScheduler
from src.agent_platform.core.task import Task, TaskPriority

# Redis connection (using the same URL as docker-compose)
REDIS_URL = "redis://localhost:6379/0"
API_URL = "http://localhost:8000"

# Test parameters
DEFAULT_TASKS = 10000
DEFAULT_CONCURRENCY = 500
DEFAULT_WORKERS = 5

class LoadTestResult:
    """Container for load test results."""
    def __init__(self):
        self.task_ids: List[str] = []
        self.submit_times: List[float] = []
        self.completion_times: List[float] = []
        self.errors: int = 0
        self.retries: int = 0
        self.queue_depth: int = 0
        self.start_time: float = 0.0
        self.end_time: float = 0.0

    def throughput(self) -> float:
        """Tasks per second."""
        if self.end_time == self.start_time:
            return 0.0
        return len(self.task_ids) / (self.end_time - self.start_time)

    def latency(self, percentile: float) -> float:
        """Latency at given percentile (0-1)."""
        if not self.completion_times:
            return 0.0
        sorted_times = sorted(self.completion_times)
        idx = int(len(sorted_times) * percentile)
        return sorted_times[idx]

    def p50(self) -> float:
        return self.latency(0.50)

    def p95(self) -> float:
        return self.latency(0.95)

    def p99(self) -> float:
        return self.latency(0.99)

    def error_rate(self) -> float:
        total = len(self.task_ids) + self.errors
        if total == 0:
            return 0.0
        return self.errors / total

    def retry_rate(self) -> float:
        total = len(self.task_ids)
        if total == 0:
            return 0.0
        return self.retries / total

    def to_dict(self) -> Dict[str, Any]:
        return {
            "total_tasks": len(self.task_ids),
            "errors": self.errors,
            "retries": self.retries,
            "throughput": self.throughput(),
            "p50_latency": self.p50(),
            "p95_latency": self.p95(),
            "p99_latency": self.p99(),
            "error_rate": self.error_rate(),
            "retry_rate": self.retry_rate(),
            "max_queue_depth": self.queue_depth,
            "elapsed": self.end_time - self.start_time,
        }


async def submit_task(client: httpx.AsyncClient, agent_id: str, payload: dict, task_id: str = None) -> str:
    """
    Submit a single task via the API.
    Returns the task_id.
    """
    request_body = {
        "agent_id": agent_id,
        "task_type": "echo",  # We'll use echo-agent for simplicity; can be extended
        "payload": payload,
        "priority": 1,  # HIGH
        "timeout_seconds": 30,
        "max_retries": 1,
    }
    if task_id:
        request_body["task_id"] = task_id  # For idempotency test

    start = time.time()
    resp = await client.post(f"{API_URL}/tasks/", json=request_body)
    elapsed = time.time() - start
    if resp.status_code != 200:
        raise Exception(f"Submit failed: {resp.text}")
    data = resp.json()
    return data["task_id"], elapsed


async def poll_task(client: httpx.AsyncClient, task_id: str, timeout: float = 30.0) -> Dict:
    """
    Poll a task until completion or timeout.
    Returns the final task data.
    """
    start = time.time()
    while time.time() - start < timeout:
        resp = await client.get(f"{API_URL}/tasks/{task_id}")
        if resp.status_code == 200:
            data = resp.json()
            status = data.get("status")
            if status in ("completed", "failed", "cancelled", "timeout"):
                return data
        await asyncio.sleep(0.2)
    raise TimeoutError(f"Task {task_id} did not complete within {timeout}s")


async def single_task_flow(client: httpx.AsyncClient, task_id: str, payload: dict) -> tuple:
    """
    Submit and wait for a single task.
    Returns (success, duration, retry_count, error_msg).
    """
    try:
        submit_start = time.time()
        tid, _ = await submit_task(client, "echo-agent", payload, task_id)
        result = await poll_task(client, tid)
        duration = time.time() - submit_start
        if result.get("status") == "completed":
            return True, duration, result.get("retry_count", 0), None
        else:
            return False, duration, result.get("retry_count", 0), result.get("error", "Unknown error")
    except Exception as e:
        return False, 0.0, 0, str(e)


async def run_load_test(total_tasks: int, concurrency: int, workers: int) -> LoadTestResult:
    """
    Run the load test with the given parameters.
    """
    print(f"🚀 Starting load test: {total_tasks} tasks, {concurrency} concurrent, {workers} workers")
    result = LoadTestResult()
    result.start_time = time.time()

    semaphore = asyncio.Semaphore(concurrency)
    client = httpx.AsyncClient(timeout=30.0)

    # We'll use a simple echo payload for all tasks
    base_payload = {"message": "Load test task"}

    tasks = []
    for i in range(total_tasks):
        # Generate a unique task_id (for idempotency we could reuse, but here we want unique)
        task_id = f"loadtest-{i:05d}"
        payload = base_payload.copy()
        payload["task_id"] = task_id  # Not used by echo, but for tracking

        async def bounded_submit(tid=task_id, pl=payload):
            async with semaphore:
                success, duration, retry, error = await single_task_flow(client, tid, pl)
                if success:
                    result.task_ids.append(tid)
                    result.completion_times.append(duration)
                else:
                    result.errors += 1
                result.retries += retry

        tasks.append(bounded_submit)

    # Monitor queue depth (approximate) from Redis
    # We'll run a separate coroutine to sample queue depth
    queue_depth_samples = []
    async def monitor_queue():
        from redis.asyncio import Redis
        redis = Redis.from_url(REDIS_URL)
        while True:
            try:
                depth = await redis.zcard("tasks:queue")
                queue_depth_samples.append(depth)
            except Exception:
                pass
            await asyncio.sleep(0.1)

    monitor_task = asyncio.create_task(monitor_queue())

    # Execute all tasks concurrently
    await asyncio.gather(*tasks)

    # Stop monitoring
    monitor_task.cancel()
    try:
        await monitor_task
    except asyncio.CancelledError:
        pass

    result.end_time = time.time()
    if queue_depth_samples:
        result.queue_depth = max(queue_depth_samples)

    await client.aclose()
    return result


async def main():
    parser = argparse.ArgumentParser(description="Real load test with Redis+PostgreSQL")
    parser.add_argument("--tasks", type=int, default=DEFAULT_TASKS, help="Number of tasks")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="Concurrent requests")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Number of workers (for info)")
    args = parser.parse_args()

    # Ensure the API is available
    try:
        async with httpx.AsyncClient() as client:
            resp = await client.get(f"{API_URL}/health")
            if resp.status_code != 200:
                print("❌ API not healthy. Make sure docker-compose is up.")
                return
    except Exception:
        print("❌ Cannot connect to API. Make sure docker-compose is up.")
        return

    print(f"✅ API is healthy. Starting load test...")

    result = await run_load_test(args.tasks, args.concurrency, args.workers)

    # Output results
    print("\n📊 Load Test Results:")
    print(f"  Total tasks:         {len(result.task_ids)}")
    print(f"  Errors:              {result.errors}")
    print(f"  Retries:             {result.retries}")
    print(f"  Throughput:          {result.throughput():.2f} tasks/sec")
    print(f"  Latency p50:         {result.p50():.4f} s")
    print(f"  Latency p95:         {result.p95():.4f} s")
    print(f"  Latency p99:         {result.p99():.4f} s")
    print(f"  Error rate:          {result.error_rate():.2%}")
    print(f"  Retry rate:          {result.retry_rate():.2%}")
    print(f"  Max queue depth:     {result.queue_depth}")
    print(f"  Elapsed:             {result.end_time - result.start_time:.2f} s")

    # Save results to JSON file for reporting
    output = {
        "timestamp": datetime.utcnow().isoformat(),
        "parameters": {
            "tasks": args.tasks,
            "concurrency": args.concurrency,
            "workers": args.workers,
        },
        "results": result.to_dict()
    }
    with open("load_test_results.json", "w") as f:
        json.dump(output, f, indent=2)
    print(f"\n📁 Results saved to load_test_results.json")


if __name__ == "__main__":
    asyncio.run(main())