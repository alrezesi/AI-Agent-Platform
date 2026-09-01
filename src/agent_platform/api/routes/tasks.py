# src/agent_platform/api/routes/tasks.py
# REST API endpoints for task management


from fastapi import APIRouter, Body, Depends, HTTPException, Query, Request

from src.agent_platform.core.task import TaskPriority, TaskStatus
from src.agent_platform.monitoring.request_id import get_request_id
from src.agent_platform.scheduler.exceptions import CrossTenantTaskConflictError
from src.agent_platform.scheduler.models import TaskFilterOptions
from src.agent_platform.scheduler.scheduler import TaskScheduler

router = APIRouter(prefix="/tasks", tags=["tasks"])


# Dependency: get scheduler instance (will be injected in main app)
def get_scheduler() -> TaskScheduler:
    from src.agent_platform.runtime import get_scheduler as get_runtime_scheduler

    return get_runtime_scheduler()


def get_current_tenant_id(request: Request) -> str:
    tenant_id = getattr(request.state, "tenant_id", None)
    if not tenant_id:
        raise HTTPException(status_code=401, detail="Tenant authentication required")
    return str(tenant_id)


@router.post("/")
async def submit_task(
    agent_id: str = Body(...),
    task_type: str = Body(...),
    payload: dict = Body(...),
    task_id: str | None = Body(None),
    priority: TaskPriority = Body(TaskPriority.MEDIUM),
    timeout_seconds: int = Body(30),
    max_retries: int = Body(3),
    tenant_id: str = Depends(get_current_tenant_id),
    scheduler: TaskScheduler = Depends(get_scheduler),
):
    """Submit a new task."""
    try:
        task_id = await scheduler.submit_task(
            agent_id=agent_id,
            task_type=task_type,
            payload=payload,
            task_id=task_id,
            priority=priority,
            timeout_seconds=timeout_seconds,
            max_retries=max_retries,
            tenant_id=tenant_id,
            request_id=get_request_id(),
        )
    except CrossTenantTaskConflictError as exc:
        raise HTTPException(status_code=409, detail=f"Task id conflict: {exc}") from exc
    return {"task_id": task_id, "status": "submitted"}


@router.get("/")
async def list_tasks(
    agent_id: str | None = Query(None),
    status: TaskStatus | None = Query(None),
    priority: TaskPriority | None = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    tenant_id: str = Depends(get_current_tenant_id),
    scheduler: TaskScheduler = Depends(get_scheduler),
):
    """List tasks with filtering and pagination."""
    filters = TaskFilterOptions(
        agent_id=agent_id,
        status=status,
        priority=priority,
        tenant_id=tenant_id,
        request_id=None,
        from_date=None,
        to_date=None,
    )
    tasks = await scheduler.list_tasks(filters, limit, offset)
    return {"tasks": tasks, "count": len(tasks), "limit": limit, "offset": offset}


@router.get("/stats")
async def get_stats(
    tenant_id: str = Depends(get_current_tenant_id),
    scheduler: TaskScheduler = Depends(get_scheduler),
):
    """Get task statistics."""
    stats = await scheduler.get_stats(tenant_id)
    return stats


@router.get("/{task_id}")
async def get_task(
    task_id: str,
    tenant_id: str = Depends(get_current_tenant_id),
    scheduler: TaskScheduler = Depends(get_scheduler),
):
    """Get task details by ID."""
    task = await scheduler.get_task(task_id, tenant_id)
    if not task:
        raise HTTPException(status_code=404, detail="Task not found")
    return task


@router.delete("/{task_id}")
async def cancel_task(
    task_id: str,
    tenant_id: str = Depends(get_current_tenant_id),
    scheduler: TaskScheduler = Depends(get_scheduler),
):
    """Cancel a pending task."""
    cancelled = await scheduler.cancel_task(task_id, tenant_id)
    if not cancelled:
        raise HTTPException(status_code=404, detail="Task not found or cannot be cancelled")
    return {"status": "cancelled"}
