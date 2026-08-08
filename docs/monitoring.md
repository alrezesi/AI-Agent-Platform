# Monitoring Dashboard

The platform includes a comprehensive monitoring system with:

- **Metrics**: Counters, gauges, and histograms for system performance
- **Tracing**: Distributed tracing for request flow analysis
- **Logging**: Structured logging with tenant isolation
- **Dashboard**: Real-time visualization of system status

## Components

### Metrics

| Metric Type | Examples |
|-------------|----------|
| Counter | Total tasks submitted, messages sent |
| Gauge | Active agents, pending tasks |
| Histogram | Task duration, response time |

### Tracing

Tracing uses OpenTelemetry-style spans with:
- Trace ID, Span ID, Parent Span
- Attributes and events
- Duration tracking

### Logging

Structured logs with:
- Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
- Tenant ID, Trace ID, Span ID
- Custom attributes
- Exception tracking

## API Endpoints

| Endpoint | Description |
|----------|-------------|
| `/monitoring/status` | Overall system status |
| `/monitoring/agents` | List of agents with status |
| `/monitoring/tasks` | Task statistics |
| `/monitoring/metrics` | All metrics data |
| `/monitoring/traces` | Distributed traces |
| `/monitoring/logs` | Recent logs |
| `/monitoring/health` | Health check |

## Dashboard UI

Access the dashboard at: `http://localhost:8000/monitoring/dashboard`

The dashboard shows:
- System status and uptime
- Agent count and activity
- Task statistics
- Agent details table
- Task distribution

## Integration

### With Scheduler

```python
# Record task metrics
await metrics.record_task_submission(tenant_id)
await metrics.record_task_completion(duration_ms, status, tenant_id)
With Agent Registry
python
# Record agent registration
await metrics.record_agent_registration(tenant_id)
await metrics.set_active_agents(count, tenant_id)
With Message Bus
python
# Record message metrics
await metrics.record_message_sent(tenant_id)
Prometheus Integration
Metrics are exposed in Prometheus format at /metrics endpoint:

bash
curl http://localhost:8000/monitoring/metrics/prometheus