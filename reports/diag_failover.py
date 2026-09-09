"""Diagnostic harness for the worker failover timeout.

Reproduces the failover scenario step-by-step and captures evidence:
  - task state in Postgres at t=0, t=5s, t=10s, t=20s
  - task state in Redis at the same points
  - worker-2 logs around the kill event
"""
from __future__ import annotations

import asyncio
import json
import os
import subprocess
import time
from pathlib import Path

import httpx
import redis.asyncio as aioredis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

PROJECT_ROOT = Path(__file__).resolve().parents[1]
POSTGRES_URL = os.getenv("POSTGRES_URL", "postgresql+asyncpg://agent:agent123@localhost:5433/agent_platform")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/1")
API_URL = "http://127.0.0.1:8000"
TASK_ID = f"failover-diag-{int(time.time() * 1000)}"


def _container_name(service: str) -> str:
    return {
        "postgres": "agent_platform_postgres",
        "redis": "agent_platform_redis",
        "api": "agent_platform_api",
        "worker-1": "agent_platform_worker_1",
        "worker-2": "agent_platform_worker_2",
    }[service]


async def _wait_for_api(client: httpx.AsyncClient, timeout: float = 60.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        try:
            resp = await client.get("/health")
            if resp.status_code == 200:
                return
        except Exception:
            pass
        await asyncio.sleep(1)
    raise RuntimeError("API not ready")


async def _get_auth_headers(client: httpx.AsyncClient) -> dict[str, str]:
    tenant = await client.post("/tenants/", json={"name": "Failover Diag Tenant", "description": "diag"})
    tenant.raise_for_status()
    tenant_id = tenant.json()["tenant_id"]
    key = await client.post(f"/tenants/{tenant_id}/api-keys")
    key.raise_for_status()
    return {"X-API-Key": key.json()["api_key"], "X-Tenant-ID": tenant_id}


async def _query_postgres(task_id: str) -> dict | None:
    engine = create_async_engine(POSTGRES_URL)
    async with engine.connect() as conn:
        result = await conn.execute(
            text("SELECT task_id, status, version, retry_count, lease_owner, lease_expires_at, started_at, completed_at, error FROM tasks WHERE task_id = :tid"),
            {"tid": task_id},
        )
        row = result.fetchone()
        if row is None:
            return None
        return {
            "task_id": row[0],
            "status": row[1],
            "version": row[2],
            "retry_count": row[3],
            "lease_owner": row[4],
            "lease_expires_at": str(row[5]) if row[5] else None,
            "started_at": str(row[6]) if row[6] else None,
            "completed_at": str(row[7]) if row[7] else None,
            "error": row[8],
        }
    await engine.dispose()


async def _query_redis(task_id: str) -> dict:
    r = aioredis.Redis.from_url(REDIS_URL, decode_responses=True)
    data = await r.get(f"tasks:data:{task_id}")
    meta = await r.get(f"tasks:meta:{task_id}")
    queue_score = await r.zscore("tasks:queue", task_id)
    processing_score = await r.zscore("tasks:processing", task_id)
    await r.aclose()
    return {
        "data": json.loads(data) if data else None,
        "meta": json.loads(meta) if meta else None,
        "in_queue": queue_score is not None,
        "in_processing": processing_score is not None,
    }


def _capture_worker_logs(label: str) -> None:
    for worker in ("worker-1", "worker-2"):
        name = _container_name(worker)
        try:
            out = subprocess.run(
                ["docker", "logs", "--no-color", "--tail=80", name],
                capture_output=True, text=True, timeout=10,
            )
            print(f"\n=== {label}: {worker} logs ===")
            print(out.stdout[-2000:] if out.stdout else "(no stdout)")
            if out.stderr:
                print("STDERR:", out.stderr[-500:])
        except Exception as exc:
            print(f"\n=== {label}: {worker} logs FAILED: {exc} ===")


async def main() -> None:
    print(f"Task ID: {TASK_ID}")
    print("Waiting for API...")
    async with httpx.AsyncClient(base_url=API_URL, timeout=30.0, trust_env=False) as client:
        await _wait_for_api(client)
        headers = await _get_auth_headers(client)

        # Submit failover task
        print("Submitting failover task...")
        submit = await client.post(
            "/tasks/",
            json={
                "task_id": TASK_ID,
                "agent_id": "bge-m3",
                "task_type": "failover",
                "payload": {"text": "kill worker-1 mid-task"},
                "timeout_seconds": 30,
                "max_retries": 0,
            },
            headers=headers,
        )
        submit.raise_for_status()
        print(f"Submitted: {submit.status_code} {submit.text}")

        # Snapshot at t=0
        print("\n--- SNAPSHOT t=0 (just submitted) ---")
        pg = await _query_postgres(TASK_ID)
        redis = await _query_redis(TASK_ID)
        print("Postgres:", json.dumps(pg, indent=2, default=str))
        print("Redis:", json.dumps(redis, indent=2, default=str))
        _capture_worker_logs("t=0")

        # Wait 1.5s then kill worker-1
        print("\nSleeping 1.5s...")
        await asyncio.sleep(1.5)

        print("Killing worker-1...")
        subprocess.run(
            ["docker", "kill", _container_name("worker-1")],
            check=True, capture_output=True, text=True,
        )
        print("worker-1 killed")

        # Snapshots at t=5, t=10, t=20
        for label, delay in [("t=5s", 3.5), ("t=10s", 5.0), ("t=20s", 10.0)]:
            print(f"\n--- SNAPSHOT {label} ---")
            await asyncio.sleep(delay)
            pg = await _query_postgres(TASK_ID)
            redis = await _query_redis(TASK_ID)
            print("Postgres:", json.dumps(pg, indent=2, default=str))
            print("Redis:", json.dumps(redis, indent=2, default=str))
            _capture_worker_logs(label)

        # Final wait for task completion (up to 150s)
        print("\nWaiting for task terminal state (up to 150s)...")
        deadline = asyncio.get_running_loop().time() + 150
        final_status = None
        while asyncio.get_running_loop().time() < deadline:
            resp = await client.get(f"/tasks/{TASK_ID}", headers=headers)
            body = resp.json()
            if body["status"] in {"completed", "failed", "timeout", "cancelled"}:
                final_status = body["status"]
                print(f"Final status: {final_status}")
                print("Result:", json.dumps(body.get("result"), default=str)[:500])
                break
            await asyncio.sleep(1)
        else:
            print("TIMEOUT: task did not reach terminal state within 150s")

        print("\n--- FINAL SNAPSHOT ---")
        pg = await _query_postgres(TASK_ID)
        redis = await _query_redis(TASK_ID)
        print("Postgres:", json.dumps(pg, indent=2, default=str))
        print("Redis:", json.dumps(redis, indent=2, default=str))
        _capture_worker_logs("final")


if __name__ == "__main__":
    asyncio.run(main())
