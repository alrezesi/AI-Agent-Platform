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
STACK_SERVICES = ["postgres", "redis", "api", "worker-1", "worker-2", "worker-3", "worker-4", "worker-5"]


def _enabled() -> bool:
    return os.getenv("RUN_DOCKER_E2E", "").lower() in {"1", "true", "yes"}


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
    _run("up", "-d", "--build", *STACK_SERVICES)


def _down() -> None:
    _run("down", "-v")


async def _wait_for_api(client: httpx.AsyncClient, timeout: float = 180.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        try:
            resp = await client.get("/health")
            if resp.status_code == 200:
                return
        except Exception:
            pass
        await asyncio.sleep(2)
    raise RuntimeError("API did not become healthy")


async def _wait_for_task(client: httpx.AsyncClient, task_id: str, headers: dict[str, str], timeout: float = 180.0) -> dict:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        resp = await client.get(f"/tasks/{task_id}", headers=headers)
        resp.raise_for_status()
        body = resp.json()
        if body["status"] in {"completed", "failed", "timeout", "cancelled"}:
            return body
        await asyncio.sleep(1)
    raise TimeoutError(task_id)


async def _get_auth_headers(client: httpx.AsyncClient) -> dict[str, str]:
    tenant = await client.post("/tenants/", json={"name": "Docker E2E Tenant", "description": "Docker e2e test tenant"})
    tenant.raise_for_status()
    return {"X-Tenant-ID": tenant.json()["tenant_id"]}


@pytest.fixture(scope="session")
def docker_stack():
    if not _enabled() or not _docker_available():
        pytest.skip("Docker E2E is only run in CI or explicitly enabled")
    _up()
    yield
    _down()


@pytest.mark.asyncio
async def test_docker_e2e_with_real_stack(docker_stack):
    async with httpx.AsyncClient(base_url=API_URL, timeout=30.0, trust_env=False) as client:
        await _wait_for_api(client)
        headers = await _get_auth_headers(client)
        task_id = f"docker-e2e-{int(time.time() * 1000)}"
        resp = await client.post(
            "/tasks/",
            json={
                "task_id": task_id,
                "agent_id": "default-agent",
                "task_type": "round-trip",
                "payload": {"message": "client-api-postgres-redis-worker-agent-postgres-client", "delay_seconds": 0.1},
                "timeout_seconds": 30,
                "max_retries": 0,
            },
            headers=headers,
        )
        resp.raise_for_status()
        task = await _wait_for_task(client, task_id, headers)
        assert task["status"] == "completed"
        assert task["task_id"] == task_id
        assert task["result"]["task_id"] == task_id
        assert task["result"]["execution_count"] == 1
