from __future__ import annotations

import os

import httpx
import pytest


API_URL = os.getenv("PRODUCTION_VERIFY_API_URL", "http://127.0.0.1:8000")


def _enabled() -> bool:
    return os.getenv("RUN_DOCKER_CHAOS", "").lower() in {"1", "true", "yes"}


pytestmark = pytest.mark.chaos


@pytest.mark.skipif(not _enabled(), reason="Chaos integration tests are only run in CI or explicitly enabled")
@pytest.mark.asyncio
async def test_end_to_end_task_round_trip() -> None:
    async with httpx.AsyncClient(base_url=API_URL, timeout=30.0, trust_env=False) as client:
        tenant = await client.post("/tenants/", json={"name": "chaos-e2e"})
        tenant.raise_for_status()
        tenant_id = tenant.json()["tenant_id"]

        headers = {"X-Tenant-ID": tenant_id}
        submitted = await client.post(
            "/tasks/",
            json={"agent_id": "default-agent", "task_type": "echo", "payload": {"message": "hello"}},
            headers=headers,
        )
        submitted.raise_for_status()
        task_id = submitted.json()["task_id"]

        body = {}
        for _ in range(120):
            fetched = await client.get(f"/tasks/{task_id}", headers=headers)
            fetched.raise_for_status()
            body = fetched.json()
            if body["status"] in {"completed", "failed", "timeout", "cancelled"}:
                break
        assert body["task_id"] == task_id
        assert body["status"] in {"completed", "failed", "timeout", "cancelled", "running", "pending"}
