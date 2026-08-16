
# REST API endpoints for monitoring and dashboard


from fastapi import APIRouter, Depends, Query

from src.agent_platform.monitoring.dashboard import DashboardAPI

router = APIRouter(prefix="/monitoring", tags=["monitoring"])


# Dependency: get dashboard API instance
def get_dashboard_api() -> DashboardAPI:
    # In production, this would be injected via DI
    from src.agent_platform.monitoring.logging import LogManager
    from src.agent_platform.monitoring.metrics import MetricRegistry, MetricsCollector
    from src.agent_platform.monitoring.tracing import Tracer

    registry = MetricRegistry()
    metrics = MetricsCollector(registry)
    tracer = Tracer()
    logs = LogManager()
    return DashboardAPI(metrics, tracer, logs, None, None)


@router.get("/status")
async def get_system_status(
    dashboard: DashboardAPI = Depends(get_dashboard_api),
):
    """Get overall system status."""
    return await dashboard.get_system_status()


@router.get("/agents")
async def get_agents_status(
    dashboard: DashboardAPI = Depends(get_dashboard_api),
):
    """Get status of all agents."""
    return await dashboard.get_agents_status()


@router.get("/tasks")
async def get_task_stats(
    dashboard: DashboardAPI = Depends(get_dashboard_api),
):
    """Get task statistics."""
    return await dashboard.get_task_stats()


@router.get("/metrics")
async def get_metrics(
    dashboard: DashboardAPI = Depends(get_dashboard_api),
):
    """Get all metrics data."""
    return await dashboard.get_metrics_data()


@router.get("/traces")
async def get_traces(
    trace_id: str | None = Query(None, description="Filter by trace ID"),
    dashboard: DashboardAPI = Depends(get_dashboard_api),
):
    """Get traces."""
    return await dashboard.get_traces(trace_id)


@router.get("/logs")
async def get_logs(
    level: str | None = Query(None, description="Filter by log level"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    dashboard: DashboardAPI = Depends(get_dashboard_api),
):
    """Get recent logs."""
    return await dashboard.get_logs(level, limit, offset)


@router.get("/health")
async def health_check(
    dashboard: DashboardAPI = Depends(get_dashboard_api),
):
    """Simple health check endpoint."""
    status = await dashboard.get_system_status()
    return {
        "status": "ok",
        "timestamp": status.get("timestamp"),
        "uptime_seconds": status.get("uptime_seconds", 0),
    }
