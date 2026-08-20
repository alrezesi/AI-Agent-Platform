from __future__ import annotations

import asyncio
import os
import time

import httpx
import pytest

# skipped because of lack of memory
pytestmark = pytest.mark.chaos

API_URL = os.getenv("PRODUCTION_VERIFY_API_URL", "http://127.0.0.1:8000")


def _enabled() -> bool:
    return os.getenv("RUN_DOCKER_CHAOS", "").lower() in {"1", "true", "yes"}


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


pytestmark = pytest.mark.chaos


@pytest.mark.skipif(
    not _enabled() or not _docker_available(),
    reason="Chaos integration tests are only run in CI or explicitly enabled",
)
@pytest.mark.asyncio
async def test_end_to_end_task_round_trip() -> None:
    async with httpx.AsyncClient(base_url=API_URL, timeout=30.0, trust_env=False) as client:
        tenant = await client.post("/tenants/", json={"name": "chaos-e2e"})
        tenant.raise_for_status()
        headers = {"X-Tenant-ID": tenant.json()["tenant_id"]}

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
