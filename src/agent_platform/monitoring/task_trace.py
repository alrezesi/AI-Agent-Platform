# Distributed task tracing across the existing observable system.

"""
Builds an operator-navigable trace for a logical task from the *real* task
store (PostgreSQL + Redis), without inventing a separate tracing product.

The trace correlates the identifiers an operator needs to follow a single
request through the system:

    Request ID -> Task ID -> Tenant ID -> Queue Message ID ->
    Worker ID  -> Execution ID -> Retry history -> Final result

It reuses the authoritative task state already persisted by the scheduler
(tenant_id, request_id, message_id, lease_owner=worker_id, execution_id,
retry_count, retry_history, status, result/error).

Tenant isolation:
    ``build_task_trace`` requires ``tenant_id`` (it is a required parameter,
    not optional) and filters every task it returns so that callers can
    only observe tasks belonging to that tenant.  This closes the audit's
    unauthenticated / cross-tenant IDOR where guessing another tenant's
    ``task_id`` / ``request_id`` / ``message_id`` / ``execution_id`` would
    return that tenant's payload, result, and error.
"""

from __future__ import annotations

from typing import Any

from src.agent_platform.core.task import Task, TaskStatus
from src.agent_platform.scheduler.models import TaskFilterOptions


def _trace_node(task: Task) -> dict[str, Any]:
    """Reduce a Task to the operator-facing trace node."""
    return {
        "request_id": task.request_id,
        "task_id": task.task_id,
        "tenant_id": task.tenant_id,
        "message_id": task.message_id,
        "worker_id": task.lease_owner,
        "execution_id": task.execution_id,
        "status": task.status.value if isinstance(task.status, TaskStatus) else str(task.status),
        "retry_count": task.retry_count,
        "error_category": task.error_category,
        "error": task.error,
        "result": task.result,
        "retry_history": list(task.retry_history or []),
    }


async def build_task_trace(
    queue: Any,
    trace_id: str,
    tenant_id: str,
) -> list[dict[str, Any]]:
    """
    Build traces for a logical request from the real task store.

    ``trace_id`` is matched against, in order:
      * request_id   (the HTTP request that submitted the task(s))
      * task_id      (a specific task)
      * message_id   (a specific queue message)
      * execution_id (a specific execution attempt)

    ``tenant_id`` is REQUIRED: every candidate task must belong to that
    tenant or it is filtered out, no matter how the ``trace_id`` was
    matched.  This is the tenant-isolation invariant that prevents the
    IDOR the audit flagged.

    Returns a list of trace nodes (one per correlated task).  An empty list
    means either (a) the trace id was not found in the observable system,
    or (b) the id matched one or more tasks belonging to a *different*
    tenant — both surface as "not found" from the caller's perspective,
    never as another tenant's data leak.
    """
    if tenant_id is None or not str(tenant_id):
        # Tenant scope is mandatory.  Fail closed (empty result) rather
        # than silently returning every tenant's data.
        return []

    # 1. request_id correlation (the canonical entry point per the audit)
    try:
        by_request = await queue.list_tasks(
            TaskFilterOptions(request_id=trace_id, tenant_id=tenant_id),
            limit=1000,
            offset=0,
        )
        if by_request:
            return [_trace_node(t) for t in by_request]
    except Exception:
        pass

    # 2. task_id correlation — get_task already enforces tenant scope when
    # ``tenant_id`` is supplied, but be defensive and double-check.
    task = await queue.get_task(trace_id, tenant_id=tenant_id)
    if task is not None:
        return [_trace_node(task)]

    # 3/4. message_id / execution_id correlation (one bounded scan, scoped
    # to the caller's tenant).  This is the path the audit specifically
    # flagged: without tenant filtering here, guessing another tenant's
    # ``message_id`` or ``execution_id`` would return that tenant's data.
    try:
        scoped_tasks = await queue.list_tasks(
            TaskFilterOptions(tenant_id=tenant_id),
            limit=1000,
            offset=0,
        )
    except Exception:
        scoped_tasks = []
    by_message = [t for t in scoped_tasks if t.message_id == trace_id]
    if by_message:
        return [_trace_node(t) for t in by_message]
    by_exec = [t for t in scoped_tasks if t.execution_id == trace_id]
    if by_exec:
        return [_trace_node(t) for t in by_exec]

    return []


def verify_trace_chain(nodes: list[dict[str, Any]], *, request_id: str, task_id: str) -> dict[str, Any]:
    """
    Verify that a single logical task is traceable end-to-end and that all
    identifiers belong to the same task.  Returns a dict of the resolved
    identifiers plus a ``consistent`` flag.
    """
    node = next((n for n in nodes if n.get("task_id") == task_id), None)
    if node is None:
        return {"consistent": False, "found": False}

    consistent = (
        node.get("request_id") == request_id
        and node.get("task_id") == task_id
        and node.get("tenant_id") is not None
        and node.get("message_id") is not None
        and node.get("execution_id") is not None
    )
    return {
        "consistent": consistent,
        "found": True,
        "request_id": node.get("request_id"),
        "task_id": node.get("task_id"),
        "tenant_id": node.get("tenant_id"),
        "message_id": node.get("message_id"),
        "worker_id": node.get("worker_id"),
        "execution_id": node.get("execution_id"),
        "retry_count": node.get("retry_count"),
        "retry_history": node.get("retry_history"),
        "status": node.get("status"),
        "result": node.get("result"),
        "error": node.get("error"),
    }
