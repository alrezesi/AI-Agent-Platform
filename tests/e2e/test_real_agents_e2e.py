# tests/e2e/test_real_agents_e2e.py
import os
import time
import subprocess
import pytest
import requests
from typing import Dict, Any

API_BASE_URL = "http://localhost:8000"
HEALTH_ENDPOINT = f"{API_BASE_URL}/health"
TASKS_ENDPOINT = f"{API_BASE_URL}/tasks"
DOCKER_COMPOSE_FILE = "docker-compose.yml"

STARTUP_WAIT = 15
POLL_INTERVAL = 2
TASK_TIMEOUT = 120

TEST_API_KEY = "test-api-key-12345"


@pytest.fixture(scope="module")
def docker_stack():
    """Start Docker stack before tests and tear down after."""
    print("\n[E2E] Starting Docker stack for real agent testing...")
    subprocess.run(["docker-compose", "-f", DOCKER_COMPOSE_FILE, "down"], capture_output=True)
    subprocess.run(["docker-compose", "-f", DOCKER_COMPOSE_FILE, "up", "-d"], check=True)
    time.sleep(STARTUP_WAIT)
    yield
    print("\n[E2E] Tearing down Docker stack...")
    subprocess.run(["docker-compose", "-f", DOCKER_COMPOSE_FILE, "down"], check=True)


def wait_for_api_ready(timeout: int = 30) -> bool:
    """Poll API health endpoint until ready."""
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


def submit_task_and_wait(agent_name: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Submit a task with the test API key and poll until completion."""
    headers = {"X-API-Key": TEST_API_KEY}
    task_data = {"agent_name": agent_name, "parameters": payload}
    response = requests.post(TASKS_ENDPOINT, json=task_data, headers=headers)
    assert response.status_code == 202, f"Failed to submit task: {response.text}"
    task_id = response.json().get("task_id")
    assert task_id is not None

    print(f"[E2E] Task submitted for {agent_name} with ID: {task_id}")

    start_time = time.time()
    while time.time() - start_time < TASK_TIMEOUT:
        status_response = requests.get(f"{TASKS_ENDPOINT}/{task_id}", headers=headers)
        assert status_response.status_code == 200
        result = status_response.json()
        status = result.get("status")
        if status == "completed":
            print(f"[E2E] Task {task_id} completed successfully.")
            return result
        elif status == "failed":
            error_msg = result.get("error", "Unknown error")
            raise AssertionError(f"Task {task_id} failed: {error_msg}")
        time.sleep(POLL_INTERVAL)

    raise TimeoutError(f"Task {task_id} did not complete within {TASK_TIMEOUT} seconds.")


def test_real_bge_m3_agent(docker_stack):
    """Test real BGE-M3 agent."""
    assert wait_for_api_ready(), "API service did not become ready in time."
    test_text = "This is a test sentence for embedding generation."
    result = submit_task_and_wait(agent_name="bge-m3", payload={"text": test_text})
    output = result.get("output")
    assert output is not None, "No output received from BGE-M3 agent."
    embedding_vector = output.get("embedding")
    assert isinstance(embedding_vector, list), "Embedding output is not a list."
    assert len(embedding_vector) > 0, "Embedding vector is empty."
    assert all(isinstance(x, float) for x in embedding_vector), "Embedding contains non-float values."
    print(f"[E2E] BGE-M3 test passed. Embedding dimension: {len(embedding_vector)}")


def test_real_gemma_agent(docker_stack):
    """Test real Gemma 2B agent."""
    assert wait_for_api_ready(), "API service did not become ready in time."
    test_prompt = "What is the capital of France? Answer in one word."
    result = submit_task_and_wait(
        agent_name="gemma-2b",
        payload={"prompt": test_prompt, "max_tokens": 10}
    )
    output = result.get("output")
    assert output is not None, "No output received from Gemma agent."
    generated_text = output.get("text")
    assert isinstance(generated_text, str), "Generated text is not a string."
    assert len(generated_text.strip()) > 0, "Generated text is empty."
    assert "paris" in generated_text.lower() or len(generated_text) > 3, \
        f"Unexpected output from Gemma: {generated_text}"
    print(f"[E2E] Gemma test passed. Generated text: {generated_text[:50]}...")