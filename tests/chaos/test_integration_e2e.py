from __future__ import annotations

import asyncio
import os
import subprocess
import time
from pathlib import Path

import httpx
import pytest

pytestmark = pytest.mark.chaos

PROJECT_ROOT = Path(__file__).resolve().parents[2]
COMPOSE_FILE = PROJECT_ROOT / "docker-compose.yml"
API_URL = os.getenv("PRODUCTION_VERIFY_API_URL", "http://127.0.0.1:8000")
DOCKER = ["docker", "compose", "-f", str(COMPOSE_FILE)]
STACK_SERVICES = ["postgres", "redis", "api", "worker-1", "worker-2"]


def _docker_available() -> bool:
    try:
        subprocess.run(  # noqa: F821
            ["docker", "info"],
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
    # Only stop API and worker services; preserve Redis/PostgreSQL.
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
        except Exception as exc:
            last_error = str(exc)
        await asyncio.sleep(2)
    raise RuntimeError(f"API not ready: {last_error}")


async def _wait_for_task(client: httpx.AsyncClient, task_id: str, headers: dict[str, str], timeout: float = 120.0) -> dict:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        fetched = await client.get(f"/tasks/{task_id}", headers=headers)
        fetched.raise_for_status()
        body = fetched.json()
        if body["status"] in {"completed", "failed", "timeout", "cancelled"}:
            return body
        await asyncio.sleep(0.5)
    raise TimeoutError(task_id)


async def _get_auth_headers(client: httpx.AsyncClient) -> dict[str, str]:
    tenant = await client.post("/tenants/", json={"name": "chaos-e2e"})
    tenant.raise_for_status()
    tenant_id = tenant.json()["tenant_id"]
    key_resp = await client.post(f"/tenants/{tenant_id}/api-keys")
    key_resp.raise_for_status()
    api_key = key_resp.json()["api_key"]
    return {"X-API-Key": api_key, "X-Tenant-ID": tenant_id}


pytestmark = pytest.mark.chaos


@pytest.fixture(scope="session")
def docker_stack():
    if not _docker_available():
        pytest.skip("Chaos integration tests require a running Docker daemon")
    _up()
    yield
    _down()


@pytest.mark.asyncio
async def test_end_to_end_task_round_trip(docker_stack) -> None:
    async with httpx.AsyncClient(base_url=API_URL, timeout=30.0, trust_env=False) as client:
        await _wait_for_api(client)
        headers = await _get_auth_headers(client)
        submitted = await client.post(
            "/tasks/",
            json={
                "task_id": f"chaos-e2e-{int(time.time() * 1000)}",
                "agent_id": "bge-m3",
                "task_type": "echo",
                "payload": {"text": "hello"},
                "timeout_seconds": 30,
                "max_retries": 0,
            },
            headers=headers,
        )
        submitted.raise_for_status()
        task_id = submitted.json()["task_id"]
        body = await _wait_for_task(client, task_id, headers)
        assert body["task_id"] == task_id
        assert body["status"] == "completed"
        assert isinstance(body["result"]["embedding"], list)
