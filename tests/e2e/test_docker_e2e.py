# End-to-end test on real Docker Compose stack (no mocks)

import asyncio
import os
import socket
import subprocess
import time
from pathlib import Path

import httpx
import pytest

PROJECT_ROOT = Path(__file__).parent.parent.parent
COMPOSE_FILE = PROJECT_ROOT / "docker-compose.chaos.yml"


def docker_compose_up() -> None:
    subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE_FILE), "up", "-d"],
        cwd=str(PROJECT_ROOT),
        check=True,
        capture_output=True,
    )


def docker_compose_down() -> None:
    try:
        subprocess.run(
            ["docker", "compose", "-f", str(COMPOSE_FILE), "down", "-v"],
            cwd=str(PROJECT_ROOT),
            check=True,
            capture_output=True,
        )
    except subprocess.CalledProcessError:
        return


def is_api_ready(timeout: int = 2) -> bool:
    try:
        with socket.create_connection(("localhost", 8000), timeout=timeout):
            return True
    except Exception:
        return False


def wait_for_api(timeout: int = 60) -> bool:
    start = time.time()
    while time.time() - start < timeout:
        try:
            response = httpx.get("http://localhost:8000/health", timeout=2, trust_env=False)
            if response.status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(2)
    return False


async def get_auth_headers(client: httpx.AsyncClient) -> dict[str, str]:
    tenant_resp = await client.post(
        "http://localhost:8000/tenants/",
        json={"name": "Docker E2E Tenant", "description": "Docker e2e test tenant"},
        timeout=30,
    )
    tenant_resp.raise_for_status()
    tenant_id = tenant_resp.json()["tenant_id"]
    return {"X-Tenant-ID": tenant_id}


async def poll_task(client: httpx.AsyncClient, task_id: str, timeout: int = 30):
    for _ in range(timeout):
        await asyncio.sleep(1)
        resp = await client.get(f"http://localhost:8000/tasks/{task_id}")
        if resp.status_code != 200:
            continue
        data = resp.json()
        status = data.get("status")
        if status == "completed":
            return data.get("result")
        if status in ("failed", "cancelled", "timeout"):
            raise RuntimeError(f"Task {task_id} failed with status {status}: {data}")
    raise TimeoutError(f"Task {task_id} did not complete within {timeout} seconds")


@pytest.fixture(scope="session")
def docker_stack():
    if not os.path.exists(str(COMPOSE_FILE)):
        pytest.skip("Docker Compose stack definition is not available")
    if is_api_ready():
        print("\nDocker Compose stack is already running. Waiting for health...")
        if not wait_for_api():
            docker_compose_down()
            docker_compose_up()
            if not wait_for_api():
                raise RuntimeError("Existing Docker stack is not healthy")
        yield
        return

    print("\nStarting Docker Compose stack...")
    docker_compose_up()
    print("Waiting for services to be ready...")
    if not wait_for_api():
        docker_compose_down()
        raise RuntimeError("API did not become healthy in time")
    print("Stack is ready.")
    yield
    print("\nTearing down Docker Compose stack...")
    docker_compose_down()
    print("Stack removed.")


@pytest.mark.asyncio
async def test_docker_e2e_with_real_agents(docker_stack):
    async with httpx.AsyncClient(trust_env=False, timeout=60.0) as client:
        headers = await get_auth_headers(client)

        print("\nTesting BGE-M3 embedding agent...")
        resp = await client.post(
            "http://localhost:8000/tasks/",
            json={
                "agent_id": "bge-m3",
                "task_type": "embed",
                "payload": {"text": "Hello world!"},
                "priority": 1,
                "timeout_seconds": 30,
            },
            headers=headers,
        )
        assert resp.status_code == 200, f"Submission failed: {resp.text}"
        task_id = resp.json()["task_id"]
        result = await poll_task(client, task_id, timeout=30)
        assert isinstance(result, list), f"Expected list, got {type(result)}"
        assert len(result) > 0, "Embedding list is empty"
        print(f"BGE-M3 returned embedding of length {len(result)}")

        print("\nTesting Gemma text generation agent...")
        resp = await client.post(
            "http://localhost:8000/tasks/",
            json={
                "agent_id": "gemma-2b",
                "task_type": "generate",
                "payload": {
                    "prompt": "Explain AI in one sentence:",
                    "max_tokens": 50,
                    "temperature": 0.7,
                },
                "priority": 1,
                "timeout_seconds": 60,
            },
            headers=headers,
        )
        assert resp.status_code == 200, f"Submission failed: {resp.text}"
        task_id = resp.json()["task_id"]
        result = await poll_task(client, task_id, timeout=60)
        assert isinstance(result, str), f"Expected string, got {type(result)}"
        assert len(result) > 0, "Generated text is empty"
        print(f"Gemma generated: {result[:100]}...")

        print("\nTesting default-agent (sanity check)...")
        resp = await client.post(
            "http://localhost:8000/tasks/",
            json={
                "agent_id": "default-agent",
                "task_type": "echo",
                "payload": {"message": "Hello E2E!", "worker": "docker-e2e"},
                "priority": 1,
            },
            headers=headers,
        )
        assert resp.status_code == 200
        task_id = resp.json()["task_id"]
        result = await poll_task(client, task_id, timeout=10)
        assert isinstance(result, dict), f"Expected dict, got {type(result)}"
        assert result["worker"] == "docker-e2e"
        assert result["echo"] == "Hello E2E!"
        print("default-agent passed.")

        print("\nAll real agents tested successfully!")
