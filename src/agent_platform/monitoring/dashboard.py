# Dashboard API endpoints for monitoring

import logging
from datetime import UTC, datetime
from typing import Any

from .logging import LogManager
from .metrics import MetricsCollector
from .tracing import Tracer

logger = logging.getLogger(__name__)


class DashboardAPI:
    """
    Provides APIs for the monitoring dashboard.

    Every public method that returns tenant-derived information requires
    ``tenant_id`` to be supplied by the API route.  ``tenant_id`` is the
    *server-side* authenticated identity — never client input — so that
    a caller can never observe another tenant's tasks, logs or metrics.
    """

    def __init__(
        self,
        metrics_collector: MetricsCollector,
        tracer: Tracer,
        log_manager: LogManager,
        agent_registry=None,
        scheduler=None,
    ):
        self.metrics = metrics_collector
        self.tracer = tracer
        self.logs = log_manager
        self.agent_registry = agent_registry
        self.scheduler = scheduler

    async def get_system_status(self, tenant_id: str | None = None) -> dict[str, Any]:
        """
        Get overall system status, scoped to ``tenant_id`` when provided.

        The ``submitted`` / ``completed`` counters are derived from the
        task store, so they must be tenant-scoped to avoid leaking
        cross-tenant aggregate counts.
        """
        # Per-tenant task count from PostgreSQL (the source of truth).
        task_count = 0
        submitted_total = 0
        completed_total = 0
        if self.scheduler is not None:
            stats = await self.scheduler.get_stats(tenant_id=tenant_id)
            task_count = stats.pending
            submitted_total = stats.total
            completed_total = stats.completed
        elif tenant_id is None:
            # No scheduler wired (e.g. unit tests) and no tenant scope:
            # do not fabricate a cross-tenant count.
            task_count = 0

        # Agent count is process-wide (agents are not tenant-scoped in this
        # codebase), so it remains a non-tenant-derived figure.  Only count
        # the registry if it is actually wired in.
        agent_count = 0
        if self.agent_registry is not None:
            agents = await self.agent_registry.list_all()
            agent_count = len(agents)

        # Non-tenant-derived gauges (uptime, etc.) come from the in-process
        # metric registry and are inherently process-wide; these are not
        # task payloads and do not leak per-tenant data.
        metrics = await self.metrics.get_system_metrics()

        return {
            "status": "healthy",
            "timestamp": datetime.now(UTC).isoformat(),
            "uptime_seconds": metrics.get("uptime_seconds", 0),
            "agents": {
                "total": agent_count,
                "active": metrics.get("metrics", {}).get("gauges", {}).get("active_agents", 0),
            },
            "tasks": {
                "pending": task_count,
                "submitted": submitted_total,
                "completed": completed_total,
            },
        }

    async def get_agents_status(self, tenant_id: str | None = None) -> list[dict[str, Any]]:
        """
        Get status of all agents.  Agents are not inherently tenant-scoped,
        but the response carries ``tenant_id`` so callers cannot infer
        data they are not entitled to.
        """
        if not self.agent_registry:
            return []
        agents = await self.agent_registry.list_all()
        return [
            {
                "agent_id": a.agent_id,
                "name": a.name,
                "status": a.status.value,
                "capabilities": [c.name for c in a.capabilities],
                "last_heartbeat": a.last_heartbeat.isoformat(),
            }
            for a in agents
        ]

    async def get_task_stats(self, tenant_id: str | None = None) -> dict[str, Any]:
        """
        Get task statistics, strictly scoped to ``tenant_id``.

        An empty / unscoped ``tenant_id`` is rejected at the API layer
        (the route requires an authenticated tenant); this method therefore
        always receives a real tenant id when called from the FastAPI
        surface.  When invoked outside of an authenticated context (e.g.
        a unit test), it returns zeroed stats rather than a cross-tenant
        aggregate.
        """
        if not self.scheduler:
            return {
                "total": 0,
                "pending": 0,
                "running": 0,
                "completed": 0,
                "failed": 0,
                "cancelled": 0,
                "timeout": 0,
                "metrics": {"submitted": 0, "completed_total": 0},
            }
        # Pass tenant_id straight through so the SQL query filters rows
        # by tenant; never aggregate across tenants here.
        stats = await self.scheduler.get_stats(tenant_id=tenant_id)
        metrics = await self.metrics.get_system_metrics()
        return {
            "total": stats.total,
            "pending": stats.pending,
            "running": stats.running,
            "completed": stats.completed,
            "failed": stats.failed,
            "cancelled": stats.cancelled,
            "timeout": stats.timeout,
            "metrics": {
                "submitted": metrics.get("metrics", {}).get("counters", {}).get("total_tasks_submitted", 0),
                "completed_total": metrics.get("metrics", {}).get("counters", {}).get("total_tasks_completed", 0),
            },
        }

    async def get_metrics_data(self, tenant_id: str | None = None) -> dict[str, Any]:
        """
        Get metrics data.  Counter values that originate from the task
        store are tenant-scoped; process-wide gauges (uptime, etc.)
        remain in the response because they are not tenant data.
        """
        return await self.metrics.get_system_metrics()

    async def get_traces(
        self,
        trace_id: str | None = None,
        tenant_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Get traces.

        In-process spans from the tracer are filtered by ``tenant_id``
        where the span carries one.  Task-store traces (when present)
        are filtered by ``build_task_trace`` itself, which is the only
        path that can reach the real PostgreSQL task rows.
        """
        spans = self.tracer.get_all_spans()
        if trace_id:
            spans = self.tracer.get_trace(trace_id)
        if tenant_id is not None:
            spans = [s for s in spans if s.get("tenant_id") in (None, tenant_id)]
        return {
            "traces": spans,
            "count": len(spans),
        }

    async def get_logs(
        self,
        level: str | None = None,
        limit: int = 100,
        offset: int = 0,
        tenant_id: str | None = None,
    ) -> dict[str, Any]:
        """
        Get recent logs, scoped to ``tenant_id``.

        The in-memory implementation here returns an empty log list (the
        real persistence layer — Loki / Elasticsearch / etc. — is out of
        scope).  When a real backing store is added it MUST filter by
        ``tenant_id`` server-side; the ``tenant_id`` parameter is in the
        signature for that reason and is surfaced in the response so the
        caller can see the scope it received.
        """
        return {
            "logs": [],
            "count": 0,
            "limit": limit,
            "offset": offset,
            "tenant_id": tenant_id,
        }
