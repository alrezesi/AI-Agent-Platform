
# Shared fixtures and configuration for all tests

import pytest
from fastapi import FastAPI

# Import runtime to monkeypatch
from src.agent_platform import runtime
from src.agent_platform.api.main import app as original_app
from src.agent_platform.api.routes.tenants import get_tenant_manager as original_get_tenant_manager
from src.agent_platform.api.routes.tasks import get_scheduler as original_get_scheduler
from src.agent_platform.multi_tenant.manager import TenantManager
from src.agent_platform.scheduler.scheduler import TaskScheduler
from src.agent_platform.scheduler.in_memory import InMemoryTaskQueue


# Shared storage for tenants (persists across the test session)
class SharedStorage:
    _tenants = {}

shared_storage = SharedStorage()

# Singleton tenant manager instance
_tenant_manager = None

def get_test_tenant_manager() -> TenantManager:
    """Return a singleton TenantManager for tests."""
    global _tenant_manager
    if _tenant_manager is None:
        _tenant_manager = TenantManager(shared_storage)
        # Rebuild index to ensure it's fresh
        _tenant_manager._rebuild_api_key_index()
    return _tenant_manager


# CRITICAL: Override runtime.get_tenant_manager to use the test singleton
# This ensures the middleware uses the same manager instance as the tests.
runtime.get_tenant_manager = get_test_tenant_manager


# Singleton scheduler instance
_scheduler = None

def get_test_scheduler() -> TaskScheduler:
    """Return a singleton TaskScheduler for tests."""
    global _scheduler
    if _scheduler is None:
        _scheduler = TaskScheduler(InMemoryTaskQueue())
    return _scheduler


# Override the app with test dependencies
@pytest.fixture(scope="session")
def app() -> FastAPI:
    """Return a FastAPI app with test dependency overrides."""
    app = original_app
    app.dependency_overrides[original_get_tenant_manager] = get_test_tenant_manager
    app.dependency_overrides[original_get_scheduler] = get_test_scheduler
    return app