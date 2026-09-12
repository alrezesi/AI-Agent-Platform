from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import statistics
import subprocess
import time
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import asyncpg
import httpx
from redis.asyncio import Redis

API_URL = "http://localhost:8000"
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/1")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql+asyncpg://test:test@localhost:5432/agent_platform_test")
DEFAULT_TASKS = 10_000
DEFAULT_CONCURRENCY = 500
# Wait this long AFTER all submissions have been polled before measuring
# queue_remaining.  This drain window lets workers finish processing tasks
# that were still RUNNING when the last poll completed.
DEFAULT_DRAIN_WAIT = 10.0


def _normalize_db_url(url: str) -> str:
    """Convert SQLAlchemy-style postgresql+asyncpg:// to asyncpg-compatible postgresql://."""
    if url.startswith("postgresql+asyncpg://"):
        return url.replace("postgresql+asyncpg://", "postgresql://", 1)
    if url.startswith("postgresql://"):
        return url
    return url


logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Benchmark metadata — determines agent_id and payload for each benchmark type.
# The two benchmarks are kept in **separate** output files; they are never
# averaged or mixed.
# ---------------------------------------------------------------------------

BENCHMARK_CONFIG: dict[str, dict[str, str]] = {
    "bge-m3": {
        "agent_id": "bge-m3",
        "task_type": "embedding",
        "label": "workload-bge-m3",
    },
    "noop": {
        "agent_id": "noop",
        "task_type": "noop",
        "label": "pipeline-noop",
    },
}


@dataclass
class LoadMetrics:
    # --- throughput / latency ---
    throughput: float
    p50: float
    p95: float
    p99: float
    # --- full outcome counts ---
    submitted: int
    completed: int          # any terminal state reached
    successful: int         # status == "completed"
    failed: int             # status == "failed"
    timeout: int            # task-level TIMEOUT (not poll timeout)
    pending: int            # never reached a terminal state within poll_timeout
    running: int            # last seen RUNNING at poll-timeout
    # --- drain / queue ---
    queue_remaining: int    # zcard("tasks:queue") measured AFTER drain-wait
    drain_time: float       # seconds spent in the explicit drain wait
    # --- derived rates ---
    success_rate: float     # successful / submitted
    failure_rate: float     # (failed + timeout) / submitted
    # --- legacy / additional ---
    retry_rate: float
    cpu: dict = field(default_factory=dict)
    memory: dict = field(default_factory=dict)
    redis_latency_ms: float = 0.0
    postgres_latency_ms: float = 0.0
    # --- overall verdict ---
    outcome: str = "PASS"   # "PASS" or "INCOMPLETE"


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
    return statistics.mean(timings) if timings else 0.0


async def _ping_postgres(samples: int = 20) -> float:
    """Measure real PostgreSQL round-trip latency using SELECT 1.

    Uses a shared asyncpg connection pool so the timing reflects actual
    query execution over a pooled connection, not TCP/TLS handshake
    overhead from creating a brand-new connection on every sample.
    """
    timings = []
    db_url = _normalize_db_url(DATABASE_URL)
    pool = await asyncpg.create_pool(db_url, min_size=1, max_size=5)
    try:
        for _ in range(samples):
            start = time.perf_counter()
            try:
                async with pool.acquire() as conn:
                    await conn.fetchrow("SELECT 1")
                timings.append((time.perf_counter() - start) * 1000.0)
            except Exception as exc:
                logger.debug("PostgreSQL ping failed: %s", exc)
                continue
        await asyncio.sleep(0.5)
    finally:
        await pool.close()
    return statistics.mean(timings) if timings else 0.0


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
    agent_id: str,
    task_type: str,
    payload: dict[str, str | int],
    poll_timeout: float = 600.0,
) -> tuple[float, int, str | None, str]:
    """Submit a task and poll until it reaches a terminal state.

    Returns ``(elapsed_seconds, retry_count, terminal_status, last_status)``.
    ``terminal_status`` is ``None`` when the task did not reach a terminal
    state within ``poll_timeout`` (the caller counts it as pending / in-
    flight).  ``last_status`` is always the most-recent status observed.
    """
    started = time.perf_counter()
    response = await client.post(
        "/tasks/",
        json={
            "task_id": task_id,
            "agent_id": agent_id,
            "task_type": task_type,
            "payload": payload,
            "timeout_seconds": 30,
            "max_retries": 1,
        },
        headers=headers,
    )
    if response.status_code != 200:
        raise RuntimeError(f"Task submission failed: {response.status_code} {response.text}")
    response.raise_for_status()

    deadline = asyncio.get_running_loop().time() + poll_timeout
    terminal_status: str | None = None
    last_status = "pending"
    while True:
        task = await client.get(f"/tasks/{task_id}", headers=headers)
        task.raise_for_status()
        body = task.json()
        last_status = body["status"]
        if body["status"] in {"completed", "failed", "timeout", "cancelled"}:
            terminal_status = body["status"]
            break
        if asyncio.get_running_loop().time() >= deadline:
            # Did not reach a terminal state within the poll window.
            break
        await asyncio.sleep(0.05)

    elapsed = time.perf_counter() - started
    retry_count = int(body.get("retry_count", 0))
    return elapsed, retry_count, terminal_status, last_status


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


async def run_load(
    total_tasks: int,
    concurrency: int,
    benchmark: str = "bge-m3",
    poll_timeout: float = 600.0,
    drain_wait: float = DEFAULT_DRAIN_WAIT,
) -> LoadMetrics:
    """Run a load test and return detailed metrics.

    The ``benchmark`` parameter selects the agent_id / payload:
    ``"bge-m3"`` measures end-to-end latency including BGE-M3 model
    inference; ``"noop"`` measures raw pipeline capacity (API ? Queue ?
    Scheduler ? Worker ? DB) with no model cost.  The two benchmarks are
    never averaged or mixed — they produce separate output files.
    """
    if benchmark not in BENCHMARK_CONFIG:
        raise ValueError(f"Unknown benchmark '{benchmark}'. Choose from: {list(BENCHMARK_CONFIG)}")

    cfg = BENCHMARK_CONFIG[benchmark]
    agent_id = cfg["agent_id"]
    task_type = cfg["task_type"]

    sem = asyncio.Semaphore(concurrency)
    durations: list[float] = []
    retries = 0
    errors = 0
    run_id = int(time.time() * 1000)  # unique per run to avoid task-id conflicts

    # Outcome counters — populated as tasks reach terminal states.
    submitted = 0
    successful = 0
    failed = 0
    timeout = 0
    pending = 0       # never reached a terminal state
    running = 0       # last seen RUNNING when poll timed out
    completed = 0     # any terminal state

    async with httpx.AsyncClient(base_url=API_URL, timeout=60.0, trust_env=False) as client:
        await _wait_for_api_healthy(client)

        headers = await _get_auth_headers(client)
        redis = Redis.from_url(REDIS_URL, decode_responses=True, max_connections=2000)

        async def worker(index: int) -> None:
            nonlocal retries, errors, submitted, successful, failed, timeout, pending, running, completed
            async with sem:
                submitted += 1
                try:
                    task_id = f"load-{benchmark.replace('-', '')}-{run_id}-{index:05d}"
                    if benchmark == "bge-m3":
                        payload: dict[str, str | int] = {"text": "load-test"}
                    else:
                        payload = {"message": "load-test"}
                    duration, retry_count, terminal_status, last_status = await _submit_and_wait(
                        client, task_id, headers, agent_id, task_type, payload,
                        poll_timeout=poll_timeout,
                    )
                    durations.append(duration)
                    retries += retry_count

                    if terminal_status == "completed":
                        successful += 1
                        completed += 1
                    elif terminal_status == "failed":
                        failed += 1
                        completed += 1
                    elif terminal_status == "timeout":
                        timeout += 1
                        completed += 1
                    elif terminal_status == "cancelled":
                        # Treat cancelled as a failure for failure_rate purposes.
                        failed += 1
                        completed += 1
                    else:
                        # terminal_status is None — poll timeout expired.
                        # Distinguish RUNNING (in-flight) from PENDING.
                        if last_status == "running":
                            running += 1
                        else:
                            pending += 1
                except Exception as exc:
                    errors += 1
                    logger.debug("Worker %d failed: %s", index, exc, exc_info=True)

        start = time.perf_counter()
        await asyncio.gather(*[worker(i) for i in range(total_tasks)])
        elapsed = time.perf_counter() - start

        # --- Explicit drain-wait period ---
        # After all submissions have been polled, wait for the drain window
        # so that tasks still in the queue / running get a chance to complete.
        # queue_remaining is measured AFTER this wait, not at exit.
        drain_start = time.perf_counter()
        if drain_wait > 0:
            logger.info("Draining for %.1fs before measuring queue_remaining...", drain_wait)
            await asyncio.sleep(drain_wait)
        drain_time = time.perf_counter() - drain_start

        cpu, memory = _docker_stats()
        queue_remaining = int(await redis.zcard("tasks:queue"))
        redis_latency = await _ping_redis(redis)
        postgres_latency = await _ping_postgres()
        await redis.aclose()

    # Determine overall outcome: INCOMPLETE if any task is still in the queue
    # or still pending/running after the drain window.
    incomplete = queue_remaining > 0 or pending > 0 or running > 0
    outcome = "INCOMPLETE" if incomplete else "PASS"

    # GUARD against the defect that produced the all-zero report: if the run
    # completed but measured nothing (no successful tasks, zero elapsed
    # time, or no latency signal), it is not a valid load test — it means
    # the stack was not actually processing tasks.  Raising here prevents
    # a degenerate all-zero JSON from being written and silently reported
    # as "Throughput: 0.0 tasks/sec".
    if not durations:
        raise RuntimeError(
            f"Load test completed with 0 successful tasks out of {total_tasks} "
            f"(errors={errors}, submitted={submitted}). The stack is not "
            f"processing tasks — refusing to write a degenerate all-zero result."
        )
    if not elapsed:
        raise RuntimeError(
            "Load test recorded zero elapsed time — refusing to report a "
            "degenerate throughput figure."
        )

    total_terminal = completed  # successful + failed + timeout
    success_rate = (successful / submitted * 100.0) if submitted else 0.0
    failure_rate = ((failed + timeout) / submitted * 100.0) if submitted else 0.0

    return LoadMetrics(
        throughput=total_terminal / elapsed if elapsed else 0.0,
        p50=_percentile(durations, 0.50),
        p95=_percentile(durations, 0.95),
        p99=_percentile(durations, 0.99),
        submitted=submitted,
        completed=completed,
        successful=successful,
        failed=failed,
        timeout=timeout,
        pending=pending,
        running=running,
        queue_remaining=queue_remaining,
        drain_time=drain_time,
        success_rate=success_rate,
        failure_rate=failure_rate,
        retry_rate=(retries / total_tasks) if total_tasks else 0.0,
        cpu=cpu,
        memory=memory,
        redis_latency_ms=redis_latency,
        postgres_latency_ms=postgres_latency,
        outcome=outcome,
    )


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--tasks", type=int, default=DEFAULT_TASKS)
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY)
    parser.add_argument(
        "--benchmark",
        choices=list(BENCHMARK_CONFIG),
        default="bge-m3",
        help="Which agent benchmark to run: 'bge-m3' (embedding inference) or "
             "'noop' (raw pipeline, no model).",
    )
    parser.add_argument("--poll-timeout", type=float, default=600.0,
                        help="Seconds to poll each task for a terminal state.")
    parser.add_argument("--drain-wait", type=float, default=DEFAULT_DRAIN_WAIT,
                        help="Seconds to wait after polling completes before "
                             "measuring queue_remaining.")
    parser.add_argument("--output", type=Path, default=None,
                        help="Output JSON path. If omitted, defaults to "
                             "reports/loadtest/<benchmark>-run<N>.json.")
    parser.add_argument("--run-number", type=int, default=1,
                        help="Run number appended to the default output filename.")
    args = parser.parse_args()

    cfg = BENCHMARK_CONFIG[args.benchmark]
    if args.output is None:
        label = cfg["label"]
        args.output = Path(f"reports/loadtest/{label}-run{args.run_number}.json")

    metrics = asyncio.run(
        run_load(args.tasks, args.concurrency, benchmark=args.benchmark,
                 poll_timeout=args.poll_timeout, drain_wait=args.drain_wait)
    )
    payload = {
        "timestamp": datetime.now(UTC).isoformat(),
        "benchmark": args.benchmark,
        "tasks": args.tasks,
        "concurrency": args.concurrency,
        "metrics": asdict(metrics),
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    print(json.dumps(payload, indent=2))

    # Exit non-zero when the load test was incomplete (tasks left in queue).
    # This makes the load test a real gate, not just a report.
    return 0 if metrics.outcome == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main())
