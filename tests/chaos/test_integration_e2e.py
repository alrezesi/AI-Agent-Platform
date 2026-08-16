import asyncio
import os
import subprocess
import time

import httpx
import pytest

API_BASE_URL = os.getenv("CHAOS_API_URL", "http://127.0.0.1:8000")


async def _wait_for_api(client: httpx.AsyncClient, timeout: float = 90.0) -> None:
    deadline = asyncio.get_event_loop().time() + timeout
    last_error = None
    while asyncio.get_event_loop().time() < deadline:
        try:
            health = await client.get("/health")
            if health.status_code == 200:
                return
            last_error = f"health={health.status_code}: {health.text}"
        except Exception as exc:  # pragma: no cover - network/bootstrap path
            last_error = str(exc)
        await asyncio.sleep(1.0)
    raise RuntimeError(f"API did not become ready within {timeout}s: {last_error}")


def _ensure_live_stack() -> None:
    subprocess.run(
        ["docker", "compose", "up", "-d", "--build", "postgres", "redis", "api", "worker-1", "worker-2"],
        check=True,
    )


@pytest.mark.asyncio
async def test_live_task_round_trip():
    async with httpx.AsyncClient(base_url=API_BASE_URL, timeout=30.0, trust_env=False) as client:
        health = await client.get("/health")
        if health.status_code != 200:
            _ensure_live_stack()
            await client.aclose()
            client = httpx.AsyncClient(base_url=API_BASE_URL, timeout=30.0, trust_env=False)
            await _wait_for_api(client)

        task_id = f"chaos-e2e-{int(time.time() * 1000)}"
        submit = await client.post(
            "/tasks/",
            json={
                "task_id": task_id,
                "agent_id": "default-agent",
                "task_type": "chaos-e2e",
                "payload": {"message": "hello", "delay_seconds": 0.1},
                "timeout_seconds": 30,
                "max_retries": 0,
            },
        )
        assert submit.status_code == 200, submit.text
        assert submit.json()["task_id"] == task_id

        deadline = asyncio.get_event_loop().time() + 30.0
        task = None
        while asyncio.get_event_loop().time() < deadline:
            response = await client.get(f"/tasks/{task_id}")
            assert response.status_code == 200, response.text
            task = response.json()
            if task["status"] in ("completed", "failed", "timeout"):
                break
            await asyncio.sleep(0.5)

        assert task is not None
        assert task["status"] == "completed", task
        assert task["result"]["worker_agent_id"] == "default-agent"

        await client.aclose()
