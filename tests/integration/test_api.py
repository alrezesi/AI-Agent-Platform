
# Integration tests for API endpoints

import asyncio

import pytest
from fastapi.testclient import TestClient

from tests.conftest import get_test_tenant_manager


@pytest.fixture
def client(app):
    """Create a test client for the FastAPI application."""
    return TestClient(app)


def _tenant_headers():
    """
    Create a tenant and generate an API key using the shared TenantManager.
    Returns headers with X-API-Key and X-Tenant-ID.
    """
    manager = get_test_tenant_manager()
    tenant = asyncio.run(manager.create_tenant("TaskTenant"))
    api_key = asyncio.run(manager.generate_api_key(tenant.tenant_id))
    return {
        "X-API-Key": api_key,
        "X-Tenant-ID": tenant.tenant_id,
    }


def test_health_check(client):
    """Test the health check endpoint."""
    response = client.get("/health")
    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_root(client):
    """Test the root endpoint (returns HTML)."""
    response = client.get("/")
    assert response.status_code == 200
    assert "AI Agent Platform" in response.text or "FastAPI" in response.text


def test_monitoring_status(client):
    """Test the monitoring status endpoint (now requires authentication).

    The audit removed the ``/monitoring/*`` auth exemption; unauthenticated
    callers must now be rejected with 401.  See
    ``tests/security/test_authorization.py`` for the security tests that
    assert the tenant-isolation contract.
    """
    # Unauthenticated callers must be rejected.
    response = client.get("/monitoring/status")
    assert response.status_code == 401

    # Authenticated callers get the same body shape as before.
    headers = _tenant_headers()
    response = client.get("/monitoring/status", headers=headers)
    assert response.status_code == 200
    assert "status" in response.json()


def test_monitoring_agents(client):
    """Test the monitoring agents endpoint (now requires authentication)."""
    response = client.get("/monitoring/agents")
    assert response.status_code == 401

    headers = _tenant_headers()
    response = client.get("/monitoring/agents", headers=headers)
    assert response.status_code == 200


def test_monitoring_tasks(client):
    """Test the monitoring tasks endpoint (now requires authentication
    and is tenant-scoped)."""
    response = client.get("/monitoring/tasks")
    assert response.status_code == 401

    headers = _tenant_headers()
    response = client.get("/monitoring/tasks", headers=headers)
    assert response.status_code == 200


def test_monitoring_metrics(client):
    """Test the monitoring metrics endpoint (now requires authentication)."""
    response = client.get("/monitoring/metrics")
    assert response.status_code == 401

    headers = _tenant_headers()
    response = client.get("/monitoring/metrics", headers=headers)
    assert response.status_code == 200


def test_tenants_list(client):
    """Test listing tenants with authentication."""
    headers = _tenant_headers()
    response = client.get("/tenants", headers=headers)
    assert response.status_code == 200


def test_create_tenant(client):
    """
    Test creating a tenant (public endpoint).
    Then verify the tenant can be retrieved with authentication.
    """
    # Create tenant (public endpoint - no auth required)
    response = client.post(
        "/tenants/",
        json={"name": "Test Tenant", "description": "Integration test"}
    )
    assert response.status_code == 200
    data = response.json()
    assert data["name"] == "Test Tenant"
    tenant_id = data["tenant_id"]

    # Use the shared tenant manager to generate an API key
    manager = get_test_tenant_manager()
    api_key = asyncio.run(manager.generate_api_key(tenant_id))

    # Retrieve the tenant with authentication
    headers = {"X-API-Key": api_key, "X-Tenant-ID": tenant_id}
    response = client.get(f"/tenants/{tenant_id}", headers=headers)
    assert response.status_code == 200


def test_tasks_api(client):
    """Test submitting and retrieving a task with authentication."""
    headers = _tenant_headers()
    response = client.post(
        "/tasks/",
        json={
            "agent_id": "test-agent",
            "task_type": "echo",
            "payload": {"msg": "hello"}
        },
        headers=headers,
    )
    assert response.status_code == 200
    task_id = response.json()["task_id"]

    response = client.get(f"/tasks/{task_id}", headers=headers)
    assert response.status_code == 200


def test_get_tenant_not_found(client):
    """Test retrieving a non-existent tenant returns 404."""
    headers = _tenant_headers()
    response = client.get("/tenants/nonexistent", headers=headers)
    assert response.status_code == 404


def test_delete_tenant(client):
    """Test soft-deleting a tenant."""
    headers = _tenant_headers()
    tenant_id = headers["X-Tenant-ID"]

    # Delete the tenant (soft delete)
    resp = client.delete(f"/tenants/{tenant_id}", headers=headers)
    assert resp.status_code == 200

    # After soft-delete, the tenant is inactive, so authentication should fail.
    # The API key is no longer valid, so GET should return 401.
    resp2 = client.get(f"/tenants/{tenant_id}", headers=headers)
    assert resp2.status_code == 401


def test_tasks_api_cancel(client):
    """Test cancelling a task."""
    headers = _tenant_headers()
    resp = client.post(
        "/tasks/",
        json={"agent_id": "a1", "task_type": "echo", "payload": {}},
        headers=headers,
    )
    assert resp.status_code == 200
    task_id = resp.json()["task_id"]

    resp2 = client.delete(f"/tasks/{task_id}", headers=headers)
    assert resp2.status_code == 200


def test_update_tenant(client):
    """Test updating a tenant."""
    headers = _tenant_headers()
    tenant_id = headers["X-Tenant-ID"]

    resp = client.put(
        f"/tenants/{tenant_id}",
        json={"description": "updated"},
        headers=headers,
    )
    assert resp.status_code == 200
    assert resp.json()["description"] == "updated"


def test_generate_api_key(client):
    """Test generating a new API key for a tenant."""
    headers = _tenant_headers()
    tenant_id = headers["X-Tenant-ID"]

    resp = client.post(f"/tenants/{tenant_id}/api-keys", headers=headers)
    assert resp.status_code == 200
    assert "api_key" in resp.json()


def test_revoke_api_key(client):
    """Test revoking an API key for a tenant."""
    headers = _tenant_headers()
    tenant_id = headers["X-Tenant-ID"]

    # Generate a new API key
    resp = client.post(f"/tenants/{tenant_id}/api-keys", headers=headers)
    assert resp.status_code == 200
    api_key = resp.json()["api_key"]

    # Revoke the API key
    resp2 = client.delete(
        f"/tenants/{tenant_id}/api-keys",
        params={"api_key": api_key},
        headers=headers,
    )
    assert resp2.status_code == 200
