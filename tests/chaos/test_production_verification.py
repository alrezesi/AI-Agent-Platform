from __future__ import annotations

import asyncio
import os
import subprocess
import time
from pathlib import Path

import httpx
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = PROJECT_ROOT / "docker-compose.yml"
API_URL = os.getenv("PRODUCTION_VERIFY_API_URL", "http://127.0.0.1:8000")
DOCKER = ["docker", "compose", "-f", str(COMPOSE_FILE)]
TEST_TENANT_NAME = "Production Verification Tenant"


def _run(*args: str) -> None:
    subprocess.run([*DOCKER, *args], cwd=PROJECT_ROOT, check=True, capture_output=True)


def _up() -> None:
    _run("up", "-d", "--build", "postgres", "redis", "api", "worker-1", "worker-2")


def _down() -> None:
    _run("down", "-v")


def _restart(service: str) -> None:
    _run("up", "-d", "--no-deps", "--build", service)


async def _wait_for_api(client: httpx.AsyncClient, timeout: float = 120.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    last_error = None
    while asyncio.get_running_loop().time() < deadline:
        try:
            resp = await client.get("/health")
            if resp.status_code == 200:
                return
            last_error = resp.text
        except Exception as exc:
            last_error = str(exc)
        await asyncio.sleep(2)
    raise RuntimeError(f"API not ready: {last_error}")


async def _wait_for_task(
    client: httpx.AsyncClient,
    task_id: str,
    headers: dict[str, str],
    timeout: float = 120.0,
) -> dict:
    deadline = asyncio.get_running_loop().time() + timeout

    while asyncio.get_running_loop().time() < deadline:
        resp = await client.get(
            f"/tasks/{task_id}",
            headers=headers,
        )

        if resp.status_code == 200:
            task = resp.json()

            if task["status"] in {
                "completed",
                "failed",
                "cancelled",
                "timeout",
            }:
                return task

        await asyncio.sleep(1)

    raise TimeoutError(task_id)


async def _get_auth_headers(client: httpx.AsyncClient) -> dict[str, str]:
    tenant_resp = await client.post(
        "/tenants/",
        json={"name": TEST_TENANT_NAME, "description": "Chaos verification tenant"},
    )
    tenant_resp.raise_for_status()
    tenant_id = tenant_resp.json()["tenant_id"]
    return {"X-Tenant-ID": tenant_id}


@pytest.fixture(scope="session", autouse=True)
def production_stack():
    _up()
    yield
    _down()


@pytest.mark.asyncio
async def test_worker_failover_to_second_worker():
    async with httpx.AsyncClient(base_url=API_URL, timeout=30.0, trust_env=False) as client:
        await _wait_for_api(client)
        headers = await _get_auth_headers(client)
        task_id = f"failover-{int(time.time() * 1000)}"
        submit = await client.post(
            "/tasks/",
            json={
                "task_id": task_id,
                "agent_id": "default-agent",
                "task_type": "failover",
                "payload": {"message": "worker1 should die", "delay_seconds": 8, "worker": "w1"},
                "timeout_seconds": 20,
                "max_retries": 0,
            },
            headers=headers,
        )
        assert submit.status_code == 200, submit.text
        await asyncio.sleep(1.5)
        subprocess.run(["docker", "kill", "agent_platform_worker_1"], check=True, capture_output=True)
        task = await _wait_for_task(
            client,
            task_id,
            headers,
            timeout=90,
        )
        assert task["status"] == "completed"
        assert task["result"]["task_id"] == task_id


@pytest.mark.asyncio
async def test_redis_outage_recovers_pending_task():
    async with httpx.AsyncClient(base_url=API_URL, timeout=30.0, trust_env=False) as client:
        await _wait_for_api(client)
        headers = await _get_auth_headers(client)
        subprocess.run(["docker", "stop", "agent_platform_redis"], check=True, capture_output=True)
        task_id = f"redis-outage-{int(time.time() * 1000)}"
        submit = await client.post(
            "/tasks/",
            json={
                "task_id": task_id,
                "agent_id": "default-agent",
                "task_type": "redis-outage",
                "payload": {"message": "redis down", "delay_seconds": 0.1},
                "timeout_seconds": 20,
                "max_retries": 0,
            },
            headers=headers,
        )
        assert submit.status_code == 200, submit.text
        subprocess.run(["docker", "start", "agent_platform_redis"], check=True, capture_output=True)
        _restart("worker-2")
        task = await _wait_for_task(client, task_id, timeout=120)
        assert task["status"] == "completed"


@pytest.mark.asyncio
async def test_duplicate_task_id_executes_once():
    async with httpx.AsyncClient(base_url=API_URL, timeout=30.0, trust_env=False) as client:
        await _wait_for_api(client)
        headers = await _get_auth_headers(client)
        task_id = f"dedupe-{int(time.time() * 1000)}"
        body = {
            "task_id": task_id,
            "agent_id": "default-agent",
            "task_type": "dedupe",
            "payload": {"message": "same id", "delay_seconds": 0.1},
            "timeout_seconds": 20,
            "max_retries": 0,
        }
        responses = await asyncio.gather(*[client.post("/tasks/", json=body, headers=headers) for _ in range(100)])
        assert all(resp.status_code == 200 for resp in responses)
        assert len({resp.json()["task_id"] for resp in responses}) == 1
        task = await _wait_for_task(client, task_id, timeout=60)
        assert task["status"] == "completed"
        assert task["result"]["task_id"] == task_id
        assert task["result"]["execution_count"] == 1


@pytest.mark.asyncio
async def test_duplicate_message_enqueued_multiple_times_executes_once():
    async with httpx.AsyncClient(base_url=API_URL, timeout=30.0, trust_env=False) as client:
        await _wait_for_api(client)
        headers = await _get_auth_headers(client)
        task_id = f"dup-msg-{int(time.time() * 1000)}"
        body = {
            "task_id": task_id,
            "agent_id": "default-agent",
            "task_type": "duplicate-message",
            "payload": {"message": "same enqueue", "delay_seconds": 0.1},
            "timeout_seconds": 20,
            "max_retries": 0,
        }
        for _ in range(10):
            resp = await client.post("/tasks/", json=body, headers=headers)
            assert resp.status_code == 200, resp.text
        task = await _wait_for_task(client, task_id, timeout=60)
        assert task["status"] == "completed"
        assert task["result"]["task_id"] == task_id
        assert task["result"]["execution_count"] == 1
