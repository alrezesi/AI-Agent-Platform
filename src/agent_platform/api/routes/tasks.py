# src/agent_platform/api/routes/tasks.py
# REST API endpoints for task management

from typing import Optional
from fastapi import APIRouter, HTTPException, Query, Depends, Body

from src.agent_platform.core.task import TaskPriority, TaskStatus
from src.agent_platform.scheduler.scheduler import TaskScheduler
from src.agent_platform.scheduler.models import TaskFilterOptions

router = APIRouter(prefix="/tasks", tags=["tasks"])


# Dependency: get scheduler instance (will be injected in main app)
def get_scheduler() -> TaskScheduler:
    from src.agent_platform.scheduler.in_memory import InMemoryTaskQueue
    return TaskScheduler(InMemoryTaskQueue())


@router.post("/")
async def submit_task(
    agent_id: str = Body(...),
    task_type: str = Body(...),
    payload: dict = Body(...),
    priority: TaskPriority = Body(TaskPriority.MEDIUM),
    timeout_seconds: int = Body(30),
    max_retries: int = Body(3),
    tenant_id: Optional[str] = Body(None),
    scheduler: TaskScheduler = Depends(get_scheduler),
):
    """Submit a new task."""
    task_id = await scheduler.submit_task(
        agent_id=agent_id,
        task_type=task_type,
        payload=payload,
        priority=priority,
        timeout_seconds=timeout_seconds,
        max_retries=max_retries,
        tenant_id=tenant_id,
    )
    return {"task_id": task_id, "status": "submitted"}


@router.get("/{task_id}")
async def get_task(
    task_id: str,
    tenant_id: Optional[str] = None,
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
    tenant_id: Optional[str] = None,
    scheduler: TaskScheduler = Depends(get_scheduler),
):
    """Cancel a pending task."""
    cancelled = await scheduler.cancel_task(task_id, tenant_id)
    if not cancelled:
        raise HTTPException(status_code=404, detail="Task not found or cannot be cancelled")
    return {"status": "cancelled"}


@router.get("/")
async def list_tasks(
    agent_id: Optional[str] = Query(None),
    status: Optional[TaskStatus] = Query(None),
    priority: Optional[TaskPriority] = Query(None),
    tenant_id: Optional[str] = Query(None),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    scheduler: TaskScheduler = Depends(get_scheduler),
):
    """List tasks with filtering and pagination."""
    filters = TaskFilterOptions(
        agent_id=agent_id,
        status=status,
        priority=priority,
        tenant_id=tenant_id,
    )
    tasks = await scheduler.list_tasks(filters, limit, offset)
    return {"tasks": tasks, "count": len(tasks), "limit": limit, "offset": offset}


@router.get("/stats")
async def get_stats(
    tenant_id: Optional[str] = None,
    scheduler: TaskScheduler = Depends(get_scheduler),
):
    """Get task statistics."""
    stats = await scheduler.get_stats(tenant_id)
    return stats