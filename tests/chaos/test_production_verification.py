from __future__ import annotations

import asyncio
import os
import subprocess
import time
from pathlib import Path

import httpx
import pytest

# skipped because of lack of memory
pytestmark = pytest.mark.chaos

PROJECT_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = PROJECT_ROOT / "docker-compose.yml"
API_URL = os.getenv("PRODUCTION_VERIFY_API_URL", "http://127.0.0.1:8000")
DOCKER = ["docker", "compose", "-f", str(COMPOSE_FILE)]
STACK_SERVICES = ["postgres", "redis", "api", "worker-1", "worker-2"]


def _docker_available() -> bool:
    try:
        subprocess.run(
            ["docker", "info"],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
            text=True,
            timeout=10,
        )
        return True
    except Exception:
        return False


def _run(*args: str) -> None:
    subprocess.run([*DOCKER, *args], cwd=PROJECT_ROOT, check=True, capture_output=True, text=True)


def _up() -> None:
    _run("up", "-d", *STACK_SERVICES)


def _down() -> None:
    # Only stop API and worker services; preserve Redis/PostgreSQL for other tests.
    _run("stop", "worker-1", "worker-2", "api")


async def _wait_for_api(client: httpx.AsyncClient, timeout: float = 180.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    last_error = None
    while asyncio.get_running_loop().time() < deadline:
        try:
            resp = await client.get("/health")
            if resp.status_code == 200:
                return
            last_error = resp.text
        except Exception as exc:  # pragma: no cover
            last_error = str(exc)
        await asyncio.sleep(2)
    raise RuntimeError(f"API not ready: {last_error}")


async def _wait_for_task(client: httpx.AsyncClient, task_id: str, headers: dict[str, str], timeout: float = 180.0) -> dict:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        resp = await client.get(f"/tasks/{task_id}", headers=headers)
        if resp.status_code == 200:
            body = resp.json()
            if body["status"] in {"completed", "failed", "cancelled", "timeout"}:
                return body
        await asyncio.sleep(1)
    raise TimeoutError(task_id)


async def _get_auth_headers(client: httpx.AsyncClient) -> dict[str, str]:
    tenant = await client.post("/tenants/", json={"name": "Production Verification Tenant"})
    tenant.raise_for_status()
    tenant_id = tenant.json()["tenant_id"]
    key_resp = await client.post(f"/tenants/{tenant_id}/api-keys")
    key_resp.raise_for_status()
    api_key = key_resp.json()["api_key"]
    return {"X-API-Key": api_key, "X-Tenant-ID": tenant_id}


def _container_name(service: str) -> str:
    return {
        "postgres": "agent_platform_postgres",
        "redis": "agent_platform_redis",
        "api": "agent_platform_api",
        "worker-1": "agent_platform_worker_1",
        "worker-2": "agent_platform_worker_2",
    }[service]


@pytest.fixture(scope="session")
def production_stack():
    if not _docker_available():
        pytest.skip("Docker chaos tests require a running Docker daemon")
    _up()
    yield
    # Only stop API and workers; preserve Redis/PostgreSQL for other test suites.
    try:
        _run("stop", "api", "worker-1", "worker-2")
    except Exception:
        pass


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
                "agent_id": "bge-m3",
                "task_type": "failover",
                "payload": {"text": "kill worker-1 mid-task"},
                "timeout_seconds": 30,
                "max_retries": 0,
            },
            headers=headers,
        )
        submit.raise_for_status()
        await asyncio.sleep(1.5)
        subprocess.run(["docker", "kill", _container_name("worker-1")], check=True, capture_output=True, text=True)
        task = await _wait_for_task(client, task_id, headers, timeout=150)
        assert task["status"] == "completed"
        assert isinstance(task["result"]["embedding"], list)
        assert len(task["result"]["embedding"]) > 0


@pytest.mark.asyncio
async def test_duplicate_task_id_executes_once():
    async with httpx.AsyncClient(base_url=API_URL, timeout=30.0, trust_env=False) as client:
        await _wait_for_api(client)
        headers = await _get_auth_headers(client)
        task_id = f"dedupe-{int(time.time() * 1000)}"
        body = {
            "task_id": task_id,
            "agent_id": "bge-m3",
            "task_type": "dedupe",
            "payload": {"text": "same id"},
            "timeout_seconds": 30,
            "max_retries": 0,
        }
        responses = await asyncio.gather(*[client.post("/tasks/", json=body, headers=headers) for _ in range(10)])
        assert all(resp.status_code == 200 for resp in responses), [resp.text for resp in responses if resp.status_code != 200]
        assert len({resp.json()["task_id"] for resp in responses}) == 1
        task = await _wait_for_task(client, task_id, headers, timeout=120)
        assert task["status"] == "completed"
        assert isinstance(task["result"]["embedding"], list)


@pytest.mark.asyncio
async def test_duplicate_message_enqueued_multiple_times_executes_once():
    async with httpx.AsyncClient(base_url=API_URL, timeout=30.0, trust_env=False) as client:
        await _wait_for_api(client)
        headers = await _get_auth_headers(client)
        task_id = f"dup-msg-{int(time.time() * 1000)}"
        body = {
            "task_id": task_id,
            "agent_id": "bge-m3",
            "task_type": "duplicate-message",
            "payload": {"text": "same enqueue"},
            "timeout_seconds": 30,
            "max_retries": 0,
        }
        for _ in range(10):
            resp = await client.post("/tasks/", json=body, headers=headers)
            assert resp.status_code == 200, resp.text
        task = await _wait_for_task(client, task_id, headers, timeout=120)
        assert task["status"] == "completed"
        assert isinstance(task["result"]["embedding"], list)
