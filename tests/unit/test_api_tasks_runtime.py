from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import HTTPException
from fastapi import Request

from src.agent_platform.api.routes.tasks import (
    cancel_task,
    get_current_tenant_id,
    get_stats,
    get_task,
    list_tasks,
    submit_task,
)
from src.agent_platform.runtime import get_scheduler, get_task_queue, get_tenant_manager, reset_runtime_cache


def _request_with_tenant(tenant_id: str | None) -> Request:
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/",
        "headers": [],
        "query_string": b"",
        "client": ("testclient", 1234),
        "server": ("testserver", 80),
        "scheme": "http",
        "root_path": "",
        "app": SimpleNamespace(state=SimpleNamespace()),
    }
    request = Request(scope)
    if tenant_id is not None:
        request.state.tenant_id = tenant_id
    return request


def test_get_current_tenant_id_requires_authentication() -> None:
    request = _request_with_tenant(None)

    with pytest.raises(HTTPException, match="Tenant authentication required"):
        get_current_tenant_id(request)


def test_get_current_tenant_id_returns_string() -> None:
    request = _request_with_tenant(123)

    assert get_current_tenant_id(request) == "123"


@pytest.mark.asyncio
async def test_submit_task_routes_to_scheduler() -> None:
    scheduler = AsyncMock()
    scheduler.submit_task.return_value = "task-1"

    result = await submit_task(
        agent_id="agent-1",
        task_type="echo",
        payload={"message": "hello"},
        task_id=None,
        priority=None,
        timeout_seconds=30,
        max_retries=3,
        tenant_id="tenant-1",
        scheduler=scheduler,
    )

    assert result == {"task_id": "task-1", "status": "submitted"}
    scheduler.submit_task.assert_awaited_once()


@pytest.mark.asyncio
async def test_list_tasks_builds_filters() -> None:
    scheduler = AsyncMock()
    scheduler.list_tasks.return_value = [{"task_id": "task-1"}]

    result = await list_tasks(
        agent_id="agent-1",
        status=None,
        priority=None,
        limit=25,
        offset=5,
        tenant_id="tenant-1",
        scheduler=scheduler,
    )

    assert result["count"] == 1
    assert result["tasks"] == [{"task_id": "task-1"}]
    scheduler.list_tasks.assert_awaited_once()
    filters = scheduler.list_tasks.await_args.args[0]
    assert filters.agent_id == "agent-1"
    assert filters.tenant_id == "tenant-1"


@pytest.mark.asyncio
async def test_task_routes_handle_missing_and_cancellation_paths() -> None:
    scheduler = AsyncMock()
    scheduler.get_stats.return_value = {"queued": 1}
    scheduler.get_task.return_value = None
    scheduler.cancel_task.return_value = False

    assert await get_stats(tenant_id="tenant-1", scheduler=scheduler) == {"queued": 1}

    with pytest.raises(HTTPException, match="Task not found"):
        await get_task("missing", tenant_id="tenant-1", scheduler=scheduler)

    with pytest.raises(HTTPException, match="cannot be cancelled"):
        await cancel_task("missing", tenant_id="tenant-1", scheduler=scheduler)


def test_runtime_factories_cache_and_reset() -> None:
    reset_runtime_cache()

    scheduler_1 = get_scheduler()
    scheduler_2 = get_scheduler()
    queue_1 = get_task_queue()
    queue_2 = get_task_queue()
    tenant_manager_1 = get_tenant_manager()
    tenant_manager_2 = get_tenant_manager()

    assert scheduler_1 is scheduler_2
    assert queue_1 is queue_2
    assert tenant_manager_1 is tenant_manager_2

    reset_runtime_cache()

    assert get_scheduler() is not scheduler_1
    assert get_task_queue() is not queue_1
    assert get_tenant_manager() is not tenant_manager_1
