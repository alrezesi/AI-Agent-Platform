
# Integration tests for API endpoints

import pytest
from fastapi.testclient import TestClient

from src.agent_platform.api.main import app


@pytest.fixture
def client():
    return TestClient(app)


def test_health_check(client):
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_root(client):
    response = client.get("/")
    assert response.status_code == 200
    assert "AI Agent Platform" in response.json()["message"]


def test_monitoring_status(client):
    response = client.get("/monitoring/status")
    assert response.status_code == 200
    assert "status" in response.json()


def test_monitoring_agents(client):
    response = client.get("/monitoring/agents")
    assert response.status_code == 200


def test_monitoring_tasks(client):
    response = client.get("/monitoring/tasks")
    assert response.status_code == 200


def test_monitoring_metrics(client):
    response = client.get("/monitoring/metrics")
    assert response.status_code == 200


def test_tenants_list(client):
    response = client.get("/tenants")
    assert response.status_code == 200


def test_create_tenant(client):
    response = client.post(
        "/tenants",
        params={"name": "Test Tenant", "description": "Integration test"}
    )
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "Test Tenant"
    tenant_id = data["tenant_id"]

    # Get tenant
    response = client.get(f"/tenants/{tenant_id}")
    assert response.status_code == 200


def test_tasks_api(client):
    # Submit task
    response = client.post(
        "/tasks",
        params={
            "agent_id": "test-agent",
            "task_type": "echo",
            "payload": {"msg": "hello"}
        }
    )
    assert response.status_code == 200
    task_id = response.json()["task_id"]

    # Get task
    response = client.get(f"/tasks/{task_id}")
    assert response.status_code == 200