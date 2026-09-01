# REST API endpoints for monitoring and dashboard.
#
# Tenant isolation:
#   * ``/monitoring/*`` is NOT auth-exempt (that exemption was removed from
#     ``TenantMiddleware`` after the audit flagged an unauthenticated cross-
#     tenant data leak / IDOR via ``/monitoring/traces?trace_id=...``).
#   * Every endpoint here requires a valid API key.  The authenticated
#     caller's ``tenant_id`` is taken from ``request.state`` (set by
#     ``TenantMiddleware``) — never from a client header — and every
#     task-derived view is scoped to that ``tenant_id``.
#   * There is no admin/operator role in this codebase (no cross-tenant
#     observability is offered).  Monitoring is therefore strictly
#     tenant-scoped, exactly like every other endpoint.

from fastapi import APIRouter, Depends, HTTPException, Query, Request

from src.agent_platform.monitoring.dashboard import DashboardAPI
from src.agent_platform.monitoring.task_trace import build_task_trace

router = APIRouter(prefix="/monitoring", tags=["monitoring"])


def _current_tenant_id(request: Request) -> str:
    """Return the authenticated caller's ``tenant_id``.

    ``TenantMiddleware`` sets ``request.state.tenant_id`` from the
    authenticated tenant record; if the dependency is invoked in a context
    where the middleware did not run (e.g. a test app that forgot to wire
    it), fail closed rather than fabricating a tenant scope from nothing.
    """
    tenant_id = getattr(request.state, "tenant_id", None)
    if not tenant_id:
        raise HTTPException(
            status_code=401, detail="Tenant authentication required"
        )
    return str(tenant_id)


# Dependency: get dashboard API instance
def get_dashboard_api() -> DashboardAPI:
    # In production, this would be injected via DI
    from src.agent_platform.monitoring.logging import LogManager
    from src.agent_platform.monitoring.metrics import MetricRegistry, MetricsCollector
    from src.agent_platform.monitoring.tracing import Tracer
    from src.agent_platform.runtime import get_scheduler

    registry = MetricRegistry()
    metrics = MetricsCollector(registry)
    tracer = Tracer()
    logs = LogManager()
    scheduler = None
    try:
        scheduler = get_scheduler()
    except Exception:
        scheduler = None
    return DashboardAPI(metrics, tracer, logs, None, scheduler)


@router.get("/status")
async def get_system_status(
    tenant_id: str = Depends(_current_tenant_id),
    dashboard: DashboardAPI = Depends(get_dashboard_api),
):
    """Get overall system status (tenant-scoped)."""
    return await dashboard.get_system_status(tenant_id=tenant_id)


@router.get("/agents")
async def get_agents_status(
    tenant_id: str = Depends(_current_tenant_id),
    dashboard: DashboardAPI = Depends(get_dashboard_api),
):
    """Get status of all agents (tenant-scoped)."""
    return await dashboard.get_agents_status(tenant_id=tenant_id)


@router.get("/tasks")
async def get_task_stats(
    tenant_id: str = Depends(_current_tenant_id),
    dashboard: DashboardAPI = Depends(get_dashboard_api),
):
    """Get task statistics (strictly tenant-scoped)."""
    return await dashboard.get_task_stats(tenant_id=tenant_id)


@router.get("/metrics")
async def get_metrics(
    tenant_id: str = Depends(_current_tenant_id),
    dashboard: DashboardAPI = Depends(get_dashboard_api),
):
    """Get metrics data (tenant-scoped; counters are per-tenant)."""
    return await dashboard.get_metrics_data(tenant_id=tenant_id)


@router.get("/traces")
async def get_traces(
    trace_id: str | None = Query(None, description="Filter by request ID, task ID, message ID or execution ID"),
    tenant_id: str = Depends(_current_tenant_id),
    dashboard: DashboardAPI = Depends(get_dashboard_api),
):
    """Get traces.

    When ``trace_id`` is provided, the trace is built from the real task
    store (PostgreSQL + Redis) and is strictly scoped to the caller's
    ``tenant_id`` — a caller can never retrieve a trace node owned by
    another tenant (e.g. by guessing or learning another tenant's
    ``task_id``/``request_id``/``message_id``/``execution_id``).
    """
    if trace_id and dashboard.scheduler is not None:
        nodes = await build_task_trace(
            dashboard.scheduler.queue, trace_id, tenant_id=tenant_id
        )
        if nodes:
            return {"traces": nodes, "count": len(nodes), "source": "task_store"}
    return await dashboard.get_traces(trace_id, tenant_id=tenant_id)


@router.get("/logs")
async def get_logs(
    level: str | None = Query(None, description="Filter by log level"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    tenant_id: str = Depends(_current_tenant_id),
    dashboard: DashboardAPI = Depends(get_dashboard_api),
):
    """Get recent logs (tenant-scoped; never returns other tenants' logs)."""
    return await dashboard.get_logs(level, limit, offset, tenant_id=tenant_id)


@router.get("/health")
async def health_check(
    tenant_id: str = Depends(_current_tenant_id),
    dashboard: DashboardAPI = Depends(get_dashboard_api),
):
    """Simple health check endpoint (still tenant-scoped for consistency)."""
    status = await dashboard.get_system_status(tenant_id=tenant_id)
    return {
        "status": "ok",
        "timestamp": status.get("timestamp"),
        "uptime_seconds": status.get("uptime_seconds", 0),
    }
