# tests/e2e/test_docker_e2e.py
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


def docker_compose_up():
    """Start Docker Compose services using 'docker compose'."""
    subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE_FILE), "up", "-d"],
        cwd=str(PROJECT_ROOT),
        check=True,
        capture_output=True,
    )


def docker_compose_down():
    """Stop and remove Docker Compose services."""
    subprocess.run(
        ["docker", "compose", "-f", str(COMPOSE_FILE), "down", "-v"],
        cwd=str(PROJECT_ROOT),
        check=True,
        capture_output=True,
    )


def is_api_ready(timeout: int = 2) -> bool:
    """Quick check if API is responding."""
    try:
        with socket.create_connection(("localhost", 8000), timeout=timeout):
            return True
    except Exception:
        return False


def wait_for_api(timeout: int = 60) -> bool:
    """Wait for API to become healthy."""
    start = time.time()
    while time.time() - start < timeout:
        try:
            response = httpx.get("http://localhost:8000/health", timeout=2)
            if response.status_code == 200:
                return True
        except Exception:
            pass
        time.sleep(2)
    return False


async def wait_for_agent(agent_id: str, timeout: int = 60) -> bool:
    """Wait until the given agent is listed in the registry."""
    async with httpx.AsyncClient() as client:
        start = time.time()
        while time.time() - start < timeout:
            try:
                resp = await client.get("http://localhost:8000/monitoring/agents", timeout=2)
                if resp.status_code == 200:
                    agents = resp.json()
                    # Print agents for debugging
                    print(f"🔍 Agents found: {[a.get('agent_id') for a in agents]}")
                    for agent in agents:
                        if agent.get("agent_id") == agent_id:
                            return True
            except Exception as e:
                print(f"⚠️  Error checking agents: {e}")
            await asyncio.sleep(2)
        return False


async def get_auth_headers(client: httpx.AsyncClient) -> dict[str, str]:
    """Create a tenant and return headers that satisfy the API auth middleware."""
    tenant_resp = await client.post(
        "http://localhost:8000/tenants/",
        json={"name": "Docker E2E Tenant", "description": "Docker e2e test tenant"},
        timeout=10,
    )
    tenant_resp.raise_for_status()
    tenant_id = tenant_resp.json()["tenant_id"]
    return {"X-Tenant-ID": tenant_id}


async def poll_task(client, task_id, timeout=30):
    """
    Poll the task status until it completes, fails, or times out.
    Returns the result if successful, raises an exception otherwise.
    """
    for _ in range(timeout):
        await asyncio.sleep(1)
        resp = await client.get(f"http://localhost:8000/tasks/{task_id}")
        if resp.status_code != 200:
            continue
        data = resp.json()
        status = data.get("status")
        if status == "completed":
            return data.get("result")
        elif status in ("failed", "cancelled", "timeout"):
            raise RuntimeError(f"Task {task_id} failed with status {status}: {data}")
    raise TimeoutError(f"Task {task_id} did not complete within {timeout} seconds")


@pytest.fixture(scope="session")
def docker_stack():
    """
    Set up Docker Compose stack if not already running.
    If already up, just use the existing stack.
    """
    if not os.path.exists(str(COMPOSE_FILE)):
        pytest.skip("Docker Compose stack definition is not available")
    if is_api_ready():
        print("\n✅ Docker Compose stack is already running. Using existing stack.")
        yield
        return

    print("\n🐳 Starting Docker Compose stack...")
    docker_compose_up()
    print("⏳ Waiting for services to be ready...")
    if not wait_for_api():
        docker_compose_down()
        raise RuntimeError("API did not become healthy in time")
    print("✅ Stack is ready.")
    yield
    print("\n🧹 Tearing down Docker Compose stack...")
    docker_compose_down()
    print("✅ Stack removed.")


@pytest.mark.asyncio
async def test_docker_e2e_with_real_agents(docker_stack):
    """
    Send tasks to real agents (BGE-M3, Gemma) and verify they work.
    """
    # Wait for all agents to be registered
    agents_to_wait = ["bge-m3", "gemma-2b", "echo-agent"]
    for agent_id in agents_to_wait:
        print(f"⏳ Waiting for {agent_id} to be registered...")
        found = await wait_for_agent(agent_id, timeout=60)
        if not found:
            pytest.skip(f"{agent_id} is not registered in the current Docker stack")

    async with httpx.AsyncClient() as client:
        headers = await get_auth_headers(client)
        # 1. Test BGE-M3 embedding
        print("\n📤 Testing BGE-M3 embedding agent...")
        payload = {
            "agent_id": "bge-m3",
            "task_type": "embed",
            "payload": {"text": "Hello world!"},
            "priority": 1,
            "timeout_seconds": 30,
        }
        resp = await client.post("http://localhost:8000/tasks/", json=payload, headers=headers)
        assert resp.status_code == 200, f"Submission failed: {resp.text}"
        task_id = resp.json()["task_id"]
        result = await poll_task(client, task_id, timeout=30)
        assert result is not None
        # embedding should be a list of floats
        assert isinstance(result, list), f"Expected list, got {type(result)}"
        assert len(result) > 0, "Embedding list is empty"
        print(f"✅ BGE-M3 returned embedding of length {len(result)}")

        # 2. Test Gemma text generation
        print("\n📤 Testing Gemma text generation agent...")
        payload = {
            "agent_id": "gemma-2b",
            "task_type": "generate",
            "payload": {
                "prompt": "Explain AI in one sentence:",
                "max_tokens": 50,
                "temperature": 0.7,
            },
            "priority": 1,
            "timeout_seconds": 60,
        }
        resp = await client.post("http://localhost:8000/tasks/", json=payload, headers=headers)
        assert resp.status_code == 200, f"Submission failed: {resp.text}"
        task_id = resp.json()["task_id"]
        result = await poll_task(client, task_id, timeout=60)
        assert result is not None
        assert isinstance(result, str), f"Expected string, got {type(result)}"
        assert len(result) > 0, "Generated text is empty"
        print(f"✅ Gemma generated: {result[:100]}...")

        # 3. (Optional) Test EchoAgent for sanity
        print("\n📤 Testing EchoAgent (sanity check)...")
        payload = {
            "agent_id": "echo-agent",
            "task_type": "echo",
            "payload": {"message": "Hello E2E!"},
            "priority": 1,
        }
        resp = await client.post("http://localhost:8000/tasks/", json=payload, headers=headers)
        assert resp.status_code == 200
        task_id = resp.json()["task_id"]
        result = await poll_task(client, task_id, timeout=10)
        assert result == "Echo: Hello E2E!", f"Unexpected result: {result}"
        print("✅ EchoAgent passed.")

        print("\n🎉 All real agents tested successfully!")
