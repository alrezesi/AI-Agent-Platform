from __future__ import annotations

import argparse
import asyncio
import json
import statistics
import subprocess
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import httpx
from redis.asyncio import Redis

API_URL = "http://localhost:8000"
REDIS_URL = "redis://localhost:6379/1"
DEFAULT_TASKS = 10_000
DEFAULT_CONCURRENCY = 500
WORKER_CONTAINERS = [
    "agent_platform_worker_1",
    "agent_platform_worker_2",
]


@dataclass
class LoadMetrics:
    throughput: float
    p50: float
    p95: float
    p99: float
    error_rate: float
    retry_rate: float
    queue_depth: int
    cpu: dict
    memory: dict
    redis_latency_ms: float
    postgres_latency_ms: float


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    index = min(int(round((len(values) - 1) * pct)), len(values) - 1)
    return values[index]


async def _ping_redis(redis: Redis, samples: int = 20) -> float:
    timings = []
    for _ in range(samples):
        start = time.perf_counter()
        await redis.ping()
        timings.append((time.perf_counter() - start) * 1000.0)
        await asyncio.sleep(0.05)
    return statistics.mean(timings)


async def _ping_postgres(client: httpx.AsyncClient, samples: int = 20) -> float:
    timings = []
    for _ in range(samples):
        start = time.perf_counter()
        resp = await client.get(f"{API_URL}/health")
        resp.raise_for_status()
        timings.append((time.perf_counter() - start) * 1000.0)
        await asyncio.sleep(0.05)
    return statistics.mean(timings)


def _docker_stats() -> tuple[dict, dict]:
    cmd = [
        "docker",
        "stats",
        "--no-stream",
        "--format",
        "{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}",
        *WORKER_CONTAINERS,
    ]
    result = subprocess.run(cmd, check=True, capture_output=True, text=True)
    cpu = {}
    memory = {}
    for line in result.stdout.splitlines():
        name, cpu_perc, mem_usage = line.split("|", 2)
        cpu[name] = cpu_perc.strip()
        memory[name] = mem_usage.strip()
    return cpu, memory


async def _submit_and_wait(client: httpx.AsyncClient, task_id: str, headers: dict[str, str]) -> tuple[float, int]:
    started = time.perf_counter()
    response = await client.post(
        "/tasks/",
        json={
            "task_id": task_id,
            "agent_id": "bge-m3",
            "task_type": "load",
            "payload": {"text": "load-test"},
            "timeout_seconds": 30,
            "max_retries": 1,
        },
        headers=headers,
    )
    response.raise_for_status()

    while True:
        task = await client.get(f"/tasks/{task_id}")
        task.raise_for_status()
        body = task.json()
        if body["status"] in {"completed", "failed", "timeout", "cancelled"}:
            break
        await asyncio.sleep(0.05)

    elapsed = time.perf_counter() - started
    return elapsed, int(body.get("retry_count", 0))


async def _get_auth_headers(client: httpx.AsyncClient) -> dict[str, str]:
    tenant_resp = await client.post(
        "/tenants/",
        json={"name": "Load Test Tenant", "description": "Synthetic load test tenant"},
    )
    tenant_resp.raise_for_status()
    tenant_id = tenant_resp.json()["tenant_id"]
    return {"X-Tenant-ID": tenant_id}


async def run_load(total_tasks: int, concurrency: int) -> LoadMetrics:
    sem = asyncio.Semaphore(concurrency)
    durations: list[float] = []
    retries = 0
    errors = 0

    async with httpx.AsyncClient(base_url=API_URL, timeout=60.0, trust_env=False) as client:
        try:
            health = await client.get("/health")
            health.raise_for_status()
        except Exception as exc:
            raise RuntimeError(
                "API is not reachable at http://localhost:8000. "
                "Start the Docker stack first with: docker compose up -d"
            ) from exc

        headers = await _get_auth_headers(client)
        redis = Redis.from_url(REDIS_URL, decode_responses=True)

        async def worker(index: int) -> None:
            nonlocal retries, errors
            async with sem:
                try:
                    duration, retry_count = await _submit_and_wait(client, f"load-{index:05d}", headers)
                    durations.append(duration)
                    retries += retry_count
                except Exception:
                    errors += 1

        start = time.perf_counter()
        await asyncio.gather(*[worker(i) for i in range(total_tasks)])
        elapsed = time.perf_counter() - start

        cpu, memory = _docker_stats()
        queue_depth = int(await redis.zcard("tasks:queue"))
        redis_latency = await _ping_redis(redis)
        postgres_latency = await _ping_postgres(client)
        await redis.aclose()

    return LoadMetrics(
        throughput=total_tasks / elapsed if elapsed else 0.0,
        p50=_percentile(durations, 0.50),
        p95=_percentile(durations, 0.95),
        p99=_percentile(durations, 0.99),
        error_rate=(errors / total_tasks) if total_tasks else 0.0,
        retry_rate=(retries / total_tasks) if total_tasks else 0.0,
        queue_depth=queue_depth,
        cpu=cpu,
        memory=memory,
        redis_latency_ms=redis_latency,
        postgres_latency_ms=postgres_latency,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tasks", type=int, default=DEFAULT_TASKS)
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    parser.add_argument("--output", type=Path, default=Path("load_test_results.json"))
    args = parser.parse_args()

    metrics = asyncio.run(run_load(args.tasks, args.concurrency))
    payload = {
        "timestamp": datetime.now(UTC).isoformat(),
        "tasks": args.tasks,
        "concurrency": args.concurrency,
        "metrics": asdict(metrics),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
