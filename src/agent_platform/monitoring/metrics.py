# src/agent_platform/monitoring/metrics.py
# Metrics collection with Prometheus-style counters, gauges, and histograms

import time
import asyncio
import logging
from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from datetime import datetime
from collections import defaultdict

logger = logging.getLogger(__name__)


@dataclass
class Metric:
    """Base metric class."""
    name: str
    value: Any
    labels: Dict[str, str] = field(default_factory=dict)
    timestamp: float = field(default_factory=time.time)


@dataclass
class Counter(Metric):
    """Monotonically increasing counter."""
    value: int = 0


@dataclass
class Gauge(Metric):
    """Current value gauge (can go up and down)."""
    value: float = 0.0


@dataclass
class Histogram(Metric):
    """Distribution of values over time."""
    buckets: List[float] = field(default_factory=list)
    counts: Dict[str, int] = field(default_factory=dict)
    sum: float = 0.0


class MetricRegistry:
    """
    Registry for all metrics.
    Supports counters, gauges, and histograms.
    """

    def __init__(self):
        self._counters: Dict[str, Counter] = {}
        self._gauges: Dict[str, Gauge] = {}
        self._histograms: Dict[str, Histogram] = {}
        self._lock = asyncio.Lock()

    async def counter(self, name: str, labels: Optional[Dict[str, str]] = None) -> Counter:
        """Get or create a counter."""
        key = self._key(name, labels)
        async with self._lock:
            if key not in self._counters:
                self._counters[key] = Counter(name=name, value=0, labels=labels or {})
            return self._counters[key]

    async def gauge(self, name: str, labels: Optional[Dict[str, str]] = None) -> Gauge:
        """Get or create a gauge."""
        key = self._key(name, labels)
        async with self._lock:
            if key not in self._gauges:
                self._gauges[key] = Gauge(name=name, value=0.0, labels=labels or {})
            return self._gauges[key]

    async def histogram(self, name: str, labels: Optional[Dict[str, str]] = None) -> Histogram:
        """Get or create a histogram."""
        key = self._key(name, labels)
        async with self._lock:
            if key not in self._histograms:
                self._histograms[key] = Histogram(
                    name=name,
                    value=0.0,
                    labels=labels or {},
                    buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1.0, 2.5, 5.0, 7.5, 10.0]
                )
            return self._histograms[key]

    async def increment_counter(self, name: str, value: int = 1, labels: Optional[Dict[str, str]] = None) -> None:
        """Increment a counter."""
        counter = await self.counter(name, labels)
        counter.value += value

    async def set_gauge(self, name: str, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        """Set a gauge value."""
        gauge = await self.gauge(name, labels)
        gauge.value = value

    async def record_histogram(self, name: str, value: float, labels: Optional[Dict[str, str]] = None) -> None:
        """Record a value in a histogram."""
        hist = await self.histogram(name, labels)
        hist.sum += value
        hist.value = hist.sum
        # Determine bucket
        for bucket in hist.buckets:
            bucket_key = f"le_{bucket}"
            if value <= bucket:
                hist.counts[bucket_key] = hist.counts.get(bucket_key, 0) + 1
        # Infinity bucket
        hist.counts["le_inf"] = hist.counts.get("le_inf", 0) + 1

    def _key(self, name: str, labels: Optional[Dict[str, str]]) -> str:
        """Generate a unique key from name and labels."""
        if not labels:
            return name
        label_str = ",".join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}:{label_str}"

    def get_all_metrics(self) -> Dict[str, Any]:
        """Get all metrics as a dictionary."""
        return {
            "counters": {k: v.value for k, v in self._counters.items()},
            "gauges": {k: v.value for k, v in self._gauges.items()},
            "histograms": {
                k: {"sum": v.sum, "counts": v.counts} for k, v in self._histograms.items()
            },
        }


class MetricsCollector:
    """
    Collects and aggregates system metrics.
    """

    def __init__(self, registry: MetricRegistry):
        self.registry = registry
        self._start_time = time.time()

    async def record_agent_registration(self, tenant_id: str = "default") -> None:
        """Record an agent registration."""
        await self.registry.increment_counter(
            "agents_registered",
            labels={"tenant": tenant_id}
        )
        await self.registry.increment_counter(
            "total_agents_registered"
        )

    async def record_task_submission(self, tenant_id: str = "default") -> None:
        """Record a task submission."""
        await self.registry.increment_counter(
            "tasks_submitted",
            labels={"tenant": tenant_id}
        )
        await self.registry.increment_counter(
            "total_tasks_submitted"
        )

    async def record_task_completion(
        self,
        duration_ms: float,
        status: str,
        tenant_id: str = "default"
    ) -> None:
        """Record a task completion with duration."""
        await self.registry.increment_counter(
            "tasks_completed",
            labels={"tenant": tenant_id, "status": status}
        )
        await self.registry.increment_counter(
            "total_tasks_completed"
        )
        await self.registry.record_histogram(
            "task_duration_ms",
            duration_ms,
            labels={"tenant": tenant_id}
        )

    async def record_message_sent(self, tenant_id: str = "default") -> None:
        """Record a message sent."""
        await self.registry.increment_counter(
            "messages_sent",
            labels={"tenant": tenant_id}
        )
        await self.registry.increment_counter(
            "total_messages_sent"
        )

    async def set_active_agents(self, count: int, tenant_id: str = "default") -> None:
        """Set the number of active agents."""
        await self.registry.set_gauge(
            "active_agents",
            float(count),
            labels={"tenant": tenant_id}
        )

    async def set_pending_tasks(self, count: int, tenant_id: str = "default") -> None:
        """Set the number of pending tasks."""
        await self.registry.set_gauge(
            "pending_tasks",
            float(count),
            labels={"tenant": tenant_id}
        )

    async def get_system_metrics(self) -> Dict[str, Any]:
        """Get system metrics including uptime."""
        return {
            "uptime_seconds": time.time() - self._start_time,
            "metrics": self.registry.get_all_metrics(),
            "timestamp": datetime.utcnow().isoformat(),
        }