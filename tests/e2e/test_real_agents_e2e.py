import subprocess
import time
from typing import Any

import pytest
import requests

API_BASE_URL = "http://localhost:8000"
HEALTH_ENDPOINT = f"{API_BASE_URL}/health"
TASKS_ENDPOINT = f"{API_BASE_URL}/tasks"
DOCKER_COMPOSE_FILE = "docker-compose.yml"

STARTUP_WAIT = 15
POLL_INTERVAL = 2
TASK_TIMEOUT = 180


@pytest.fixture(scope="session")
def docker_stack():
    print("\n[E2E] Starting Docker stack for real agent testing...")
    subprocess.run(["docker", "compose", "-f", DOCKER_COMPOSE_FILE, "down"], capture_output=True)
    subprocess.run(["docker", "compose", "-f", DOCKER_COMPOSE_FILE, "up", "-d"], check=True)
    time.sleep(STARTUP_WAIT)
    yield
    print("\n[E2E] Tearing down Docker stack...")
    subprocess.run(["docker", "compose", "-f", DOCKER_COMPOSE_FILE, "down"], check=True)


def wait_for_api_ready(timeout: int = 60) -> bool:
    start_time = time.time()
    while time.time() - start_time < timeout:
        try:
            response = requests.get(HEALTH_ENDPOINT, timeout=5)
            if response.status_code == 200:
                return True
        except requests.ConnectionError:
            pass
        time.sleep(1)
    return False


@pytest.fixture(scope="module")
def tenant_auth(docker_stack) -> dict[str, str]:
    response = requests.post(
        f"{API_BASE_URL}/tenants/",
        json={"name": "E2E Tenant", "description": "E2E test tenant"},
        timeout=10,
    )
    response.raise_for_status()
    tenant_id = response.json()["tenant_id"]
    return {"X-Tenant-ID": tenant_id}


def submit_task_and_wait(agent_name: str, payload: dict[str, Any], headers: dict[str, str]) -> dict[str, Any]:
    task_data = {
        "agent_id": agent_name,
        "task_type": agent_name,
        "payload": payload,
    }
    response = requests.post(TASKS_ENDPOINT, json=task_data, headers=headers, timeout=10)
    assert response.status_code == 200, f"Failed to submit task: {response.text}"
    task_id = response.json().get("task_id")
    assert task_id is not None

    print(f"[E2E] Task submitted for {agent_name} with ID: {task_id}")

    start_time = time.time()
    while time.time() - start_time < TASK_TIMEOUT:
        status_response = requests.get(f"{TASKS_ENDPOINT}/{task_id}", headers=headers, timeout=10)
        assert status_response.status_code == 200
        result = status_response.json()
        status = result.get("status")
        if status == "completed":
            return result
        if status == "failed":
            raise AssertionError(f"Task {task_id} failed: {result.get('error', 'Unknown error')}")
        time.sleep(POLL_INTERVAL)

    raise TimeoutError(f"Task {task_id} did not complete within {TASK_TIMEOUT} seconds.")


def test_real_bge_m3_agent(docker_stack):
    assert wait_for_api_ready(), "API service did not become ready in time."
    response = requests.post(
        f"{API_BASE_URL}/tenants/",
        json={"name": "E2E Tenant", "description": "E2E test tenant"},
        timeout=10,
    )
    response.raise_for_status()
    tenant_id = response.json()["tenant_id"]
    headers = {"X-Tenant-ID": tenant_id}

    result = submit_task_and_wait(
        agent_name="bge-m3",
        payload={"text": "This is a test sentence for embedding generation."},
        headers=headers,
    )

    output = result.get("result")
    assert output is not None, "No result received from BGE-M3 agent."
    embedding_vector = output.get("embedding") if isinstance(output, dict) else output
    assert isinstance(embedding_vector, list), "Embedding result is not a list."
    assert len(embedding_vector) == 1024, f"Unexpected embedding dimension: {len(embedding_vector)}"
    assert len(embedding_vector) > 0, "Embedding vector is empty."
    assert all(isinstance(x, float) for x in embedding_vector), "Embedding contains non-float values."


def test_real_gemma_agent(docker_stack, tenant_auth):
    assert wait_for_api_ready(), "API service did not become ready in time."
    result = submit_task_and_wait(
        agent_name="gemma-2b",
        payload={"prompt": "What is the capital of France? Answer in one word.", "max_tokens": 10},
        headers=tenant_auth,
    )

    output = result.get("result")
    assert output is not None, "No result received from Gemma agent."
    generated_text = output.get("text") if isinstance(output, dict) else output
    assert isinstance(generated_text, str), "Generated text is not a string."
    assert len(generated_text.strip()) > 0, "Generated text is empty."
    assert "paris" in generated_text.lower() or len(generated_text) > 1, f"Unexpected output from Gemma: {generated_text}"
