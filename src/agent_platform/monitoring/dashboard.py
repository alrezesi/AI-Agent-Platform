
# Dashboard API endpoints for monitoring

import asyncio
import logging
from typing import Optional, Dict, Any, List
from datetime import datetime

from .metrics import MetricsCollector, MetricRegistry
from .tracing import Tracer, TraceSpan
from .logging import LogManager

logger = logging.getLogger(__name__)


class DashboardAPI:
    """
    Provides APIs for the monitoring dashboard.
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

    async def get_system_status(self) -> Dict[str, Any]:
        """
        Get overall system status.
        """
        # Get agent count
        agent_count = 0
        task_count = 0
        if self.agent_registry:
            agents = await self.agent_registry.list_all()
            agent_count = len(agents)
        if self.scheduler:
            task_count = await self.scheduler.queue_size()

        metrics = await self.metrics.get_system_metrics()

        return {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "uptime_seconds": metrics.get("uptime_seconds", 0),
            "agents": {
                "total": agent_count,
                "active": metrics.get("metrics", {}).get("gauges", {}).get("active_agents", 0),
            },
            "tasks": {
                "pending": task_count,
                "submitted": metrics.get("metrics", {}).get("counters", {}).get("total_tasks_submitted", 0),
                "completed": metrics.get("metrics", {}).get("counters", {}).get("total_tasks_completed", 0),
            },
        }

    async def get_agents_status(self) -> List[Dict[str, Any]]:
        """
        Get status of all agents.
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

    async def get_task_stats(self) -> Dict[str, Any]:
        """
        Get task statistics.
        """
        if not self.scheduler:
            return {}
        stats = await self.scheduler.get_stats()
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

    async def get_metrics_data(self) -> Dict[str, Any]:
        """
        Get all metrics data.
        """
        return await self.metrics.get_system_metrics()

    async def get_traces(self, trace_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Get traces.
        """
        spans = self.tracer.get_all_spans()
        if trace_id:
            spans = self.tracer.get_trace(trace_id)
        return {
            "traces": spans,
            "count": len(spans),
        }

    async def get_logs(
        self,
        level: Optional[str] = None,
        limit: int = 100,
        offset: int = 0,
    ) -> Dict[str, Any]:
        """
        Get recent logs.
        """
        # For this implementation, we'll return sample logs since we don't have a persistent log store.
        # In production, logs would be stored in Elasticsearch, Loki, or similar.
        # For now, we'll simulate with recent logger entries.
        return {
            "logs": [],
            "count": 0,
            "limit": limit,
            "offset": offset,
        }