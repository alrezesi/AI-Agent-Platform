"""Shared fixtures and configuration for all tests."""

from __future__ import annotations

import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import pytest
from fastapi import FastAPI

REPORTS_DIR = PROJECT_ROOT / "reports"
REPORTS_DIR.mkdir(exist_ok=True)

# ruff: noqa: E402
# Import runtime to monkeypatch
from src.agent_platform import runtime
from src.agent_platform.api.main import app as original_app
from src.agent_platform.api.routes.tasks import get_scheduler as original_get_scheduler
from src.agent_platform.api.routes.tenants import get_tenant_manager as original_get_tenant_manager
from src.agent_platform.multi_tenant.manager import TenantManager
from src.agent_platform.scheduler.in_memory import InMemoryTaskQueue
from src.agent_platform.scheduler.scheduler import TaskScheduler


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


# CRITICAL: Make the runtime storage point at the same shared in-memory tenant store
# used by the tests so any runtime-created manager sees the same tenants.
runtime._tenant_storage = shared_storage
runtime.reset_runtime_cache()


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
    for middleware in app.user_middleware:
        if getattr(middleware.cls, "__name__", "") == "TenantMiddleware":
            middleware.kwargs["tenant_manager"] = get_test_tenant_manager()
    app.middleware_stack = app.build_middleware_stack()
    return app
