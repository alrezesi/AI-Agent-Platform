from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import statistics
import subprocess
import time
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path

import httpx
from redis.asyncio import Redis

API_URL = "http://localhost:8000"
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/1")
DEFAULT_TASKS = 10_000
DEFAULT_CONCURRENCY = 500
WORKER_CONTAINERS = [
    "agent_platform_worker_1",
    "agent_platform_worker_2",
]

logger = logging.getLogger(__name__)


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
    """
    Sample CPU/Memory for running worker containers.

    Container names are DISCOVERED rather than hardcoded: the load test must
    not crash merely because the compose project name differs from the
    previous run.  We match any running container whose name contains
    ``worker``.  If no worker containers are running (e.g. the audit is run
    against a lightweight stack without dedicated worker containers), we
    return empty dicts rather than aborting the whole load test — CPU/Memory
    are informational, not a pass/fail signal.
    """
    cpu: dict[str, str] = {}
    memory: dict[str, str] = {}

    # Discover candidate worker containers currently running.
    try:
        list_cmd = [
            "docker", "ps",
            "--format", "{{.Names}}",
            "--filter", "name=worker",
        ]
        listed = subprocess.run(list_cmd, capture_output=True, text=True, timeout=15)
    except Exception as exc:
        logger.debug("docker ps failed: %s", exc)
        return cpu, memory

    names = [n.strip() for n in listed.stdout.splitlines() if n.strip()]
    if not names:
        return cpu, memory

    stats_cmd = [
        "docker", "stats", "--no-stream", "--format", "{{.Name}}|{{.CPUPerc}}|{{.MemUsage}}",
        *names,
    ]
    try:
        result = subprocess.run(stats_cmd, capture_output=True, text=True, timeout=30)
    except Exception as exc:
        logger.debug("docker stats failed: %s", exc)
        return cpu, memory

    for line in result.stdout.splitlines():
        parts = line.split("|", 2)
        if len(parts) != 3:
            continue
        name, cpu_perc, mem_usage = parts
        cpu[name.strip()] = cpu_perc.strip()
        memory[name.strip()] = mem_usage.strip()
    return cpu, memory


async def _submit_and_wait(
    client: httpx.AsyncClient,
    task_id: str,
    headers: dict[str, str],
    poll_timeout: float = 120.0,
) -> tuple[float, int]:
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

    deadline = asyncio.get_running_loop().time() + poll_timeout
    while True:
        task = await client.get(f"/tasks/{task_id}")
        task.raise_for_status()
        body = task.json()
        if body["status"] in {"completed", "failed", "timeout", "cancelled"}:
            break
        if asyncio.get_running_loop().time() >= deadline:
            raise TimeoutError(
                f"task {task_id} did not reach a terminal state within {poll_timeout}s "
                f"(last status: {body['status']})"
            )
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
    key_resp = await client.post(f"/tenants/{tenant_id}/api-keys")
    key_resp.raise_for_status()
    api_key = key_resp.json()["api_key"]
    return {"X-API-Key": api_key, "X-Tenant-ID": tenant_id}


async def _wait_for_api_healthy(
    client: httpx.AsyncClient,
    timeout: float = 60.0,
    interval: float = 1.0,
) -> None:
    """Poll ``GET /health`` until the API returns 200 or ``timeout`` elapses.

    Mirrors the retry-with-timeout pattern used by the e2e/chaos pytest
    suites (``tests/chaos/test_production_verification.py::_wait_for_api``).
    Resolves almost immediately on a healthy stack; only raises after the
    full window is exhausted, so a single transient blip no longer
    aborts the entire load test.  The exception message preserves the
    original "start the Docker stack first" guidance, which is still
    correct advice if the API is genuinely never up.
    """
    deadline = asyncio.get_running_loop().time() + timeout
    last_error: str | None = None
    while asyncio.get_running_loop().time() < deadline:
        try:
            resp = await client.get("/health")
            if resp.status_code == 200:
                return
            last_error = f"status={resp.status_code} body={resp.text[:200]!r}"
        except Exception as exc:  # pragma: no cover
            last_error = f"{type(exc).__name__}: {exc}"
        await asyncio.sleep(interval)
    raise RuntimeError(
        "API is not reachable at http://localhost:8000. "
        "Start the Docker stack first with: docker compose up -d"
        + (f" (last health-check error: {last_error})" if last_error else "")
    )


async def run_load(total_tasks: int, concurrency: int) -> LoadMetrics:
    sem = asyncio.Semaphore(concurrency)
    durations: list[float] = []
    retries = 0
    errors = 0

    async with httpx.AsyncClient(base_url=API_URL, timeout=60.0, trust_env=False) as client:
        await _wait_for_api_healthy(client)

        headers = await _get_auth_headers(client)
        redis = Redis.from_url(REDIS_URL, decode_responses=True, max_connections=2000)

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

    # GUARD against the defect that produced the all-zero report: if the run
    # completed but measured nothing (no successful tasks, zero elapsed
    # time, or no latency signal), it is not a valid load test — it means
    # the stack was not actually processing tasks.  Raising here prevents
    # a degenerate all-zero JSON from being written and silently reported
    # as "Throughput: 0.0 tasks/sec".
    if not durations:
        raise RuntimeError(
            f"Load test completed with 0 successful tasks out of {total_tasks} "
            f"(errors={errors}). The stack is not processing tasks — refusing "
            f"to write a degenerate all-zero result."
        )
    if not elapsed:
        raise RuntimeError(
            "Load test recorded zero elapsed time — refusing to report a "
            "degenerate throughput figure."
        )

    return LoadMetrics(
        throughput=total_tasks / elapsed,
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
