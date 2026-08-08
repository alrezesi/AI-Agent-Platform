# tests/unit/test_monitoring.py
# Unit tests for monitoring components

import pytest
import asyncio
import time

from src.agent_platform.monitoring.metrics import MetricRegistry, MetricsCollector
from src.agent_platform.monitoring.tracing import Tracer, TraceSpan
from src.agent_platform.monitoring.logging import LogManager, LogLevel
from src.agent_platform.monitoring.dashboard import DashboardAPI


@pytest.mark.asyncio
async def test_metric_registry():
    registry = MetricRegistry()

    # Counter
    await registry.increment_counter("test_counter")
    await registry.increment_counter("test_counter", value=5)
    counter = registry.counter("test_counter")
    assert counter.value == 6

    # Gauge
    await registry.set_gauge("test_gauge", 42.0)
    gauge = registry.gauge("test_gauge")
    assert gauge.value == 42.0

    # Histogram
    await registry.record_histogram("test_histogram", 0.5)
    hist = registry.histogram("test_histogram")
    assert hist.sum == 0.5


@pytest.mark.asyncio
async def test_metrics_collector():
    registry = MetricRegistry()
    collector = MetricsCollector(registry)

    await collector.record_agent_registration()
    await collector.record_task_submission()
    await collector.record_task_completion(100.0, "success")

    metrics = await collector.get_system_metrics()
    assert metrics["uptime_seconds"] > 0
    assert "total_agents_registered" in metrics["metrics"]["counters"]


@pytest.mark.asyncio
async def test_tracer():
    tracer = Tracer()

    span = tracer.start_span("test_operation")
    span.set_attribute("key", "value")
    span.add_event("test_event")
    span.end()

    spans = tracer.get_all_spans()
    assert len(spans) == 1
    assert spans[0]["name"] == "test_operation"
    assert spans[0]["duration_ms"] > 0


@pytest.mark.asyncio
async def test_log_manager():
    log_manager = LogManager()
    entry = log_manager.info("Test message", attributes={"key": "value"})
    assert entry.message == "Test message"
    assert entry.level == LogLevel.INFO


@pytest.mark.asyncio
async def test_dashboard_api():
    registry = MetricRegistry()
    metrics = MetricsCollector(registry)
    tracer = Tracer()
    logs = LogManager()

    dashboard = DashboardAPI(metrics, tracer, logs)

    status = await dashboard.get_system_status()
    assert status["status"] == "healthy"
    assert "uptime_seconds" in status

    metrics_data = await dashboard.get_metrics_data()
    assert "metrics" in metrics_data