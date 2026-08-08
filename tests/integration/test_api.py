# tests/integration/test_api.py
# Integration tests for API endpoints

import pytest
from fastapi.testclient import TestClient

from src.agent_platform.api.main import app
from src.agent_platform.api.routes.tenants import get_tenant_manager as original_get_tenant_manager
from src.agent_platform.api.routes.tasks import get_scheduler as original_get_scheduler
from src.agent_platform.multi_tenant.manager import TenantManager
from src.agent_platform.scheduler.scheduler import TaskScheduler
from src.agent_platform.scheduler.in_memory import InMemoryTaskQueue

# Shared storage for tenants
class SharedStorage:
    _tenants = {}

shared_storage = SharedStorage()

def test_get_tenant_manager():
    """Override tenant manager for testing."""
    return TenantManager(shared_storage)

# Shared scheduler instance
shared_scheduler = TaskScheduler(InMemoryTaskQueue())

def test_get_scheduler():
    """Override scheduler for testing."""
    return shared_scheduler

# Override dependencies
app.dependency_overrides[original_get_tenant_manager] = test_get_tenant_manager
app.dependency_overrides[original_get_scheduler] = test_get_scheduler


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
        "/tenants/",
        json={"name": "Test Tenant", "description": "Integration test"}
    )
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "Test Tenant"
    tenant_id = data["tenant_id"]

    response = client.get(f"/tenants/{tenant_id}")
    assert response.status_code == 200


def test_tasks_api(client):
    response = client.post(
        "/tasks/",
        json={
            "agent_id": "test-agent",
            "task_type": "echo",
            "payload": {"msg": "hello"}
        }
    )
    assert response.status_code == 200
    task_id = response.json()["task_id"]

    response = client.get(f"/tasks/{task_id}")
    assert response.status_code == 200


def test_get_tenant_not_found(client):
    response = client.get("/tenants/nonexistent")
    assert response.status_code == 404


def test_delete_tenant(client):
    resp = client.post("/tenants/", json={"name": "ToDelete"})
    assert resp.status_code == 200
    tenant_id = resp.json()["tenant_id"]

    resp2 = client.delete(f"/tenants/{tenant_id}")
    assert resp2.status_code == 200

    resp3 = client.get(f"/tenants/{tenant_id}")
    assert resp3.status_code == 200
    assert resp3.json()["status"] == "deleted"


def test_tasks_api_cancel(client):
    resp = client.post("/tasks/", json={"agent_id": "a1", "task_type": "echo", "payload": {}})
    assert resp.status_code == 200
    task_id = resp.json()["task_id"]

    resp2 = client.delete(f"/tasks/{task_id}")
    assert resp2.status_code == 200


def test_update_tenant(client):
    resp = client.post("/tenants/", json={"name": "ToUpdate", "description": "old"})
    assert resp.status_code == 200
    tenant_id = resp.json()["tenant_id"]

    resp = client.put(f"/tenants/{tenant_id}", json={"description": "updated"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["description"] == "updated"


def test_generate_api_key(client):
    resp = client.post("/tenants/", json={"name": "APIKeyTest"})
    assert resp.status_code == 200
    tenant_id = resp.json()["tenant_id"]

    resp = client.post(f"/tenants/{tenant_id}/api-keys")
    assert resp.status_code == 200
    data = resp.json()
    assert "api_key" in data
    assert data["api_key"].startswith("tk-")


def test_revoke_api_key(client):
    # Create tenant
    resp = client.post("/tenants/", json={"name": "RevokeTest"})
    assert resp.status_code == 200
    tenant_id = resp.json()["tenant_id"]

    # Generate API key
    resp = client.post(f"/tenants/{tenant_id}/api-keys")
    assert resp.status_code == 200
    api_key = resp.json()["api_key"]

    # Revoke API key (now using query parameter, which matches our endpoint)
    resp = client.delete(f"/tenants/{tenant_id}/api-keys", params={"api_key": api_key})
    assert resp.status_code == 200