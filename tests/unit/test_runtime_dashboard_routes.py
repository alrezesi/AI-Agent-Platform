from __future__ import annotations

from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest

from src.agent_platform.api.routes import tenants as tenant_routes
from src.agent_platform.core.agent import AgentCapability, AgentStatus
from src.agent_platform.core.task import Task, TaskPriority
from src.agent_platform.monitoring.dashboard import DashboardAPI
from src.agent_platform.multi_tenant.models import Tenant, TenantQuota, TenantStatus
from src.agent_platform.runtime import prepare_runtime
from src.agent_platform.scheduler.redis_queue import RedisTaskQueue


class DummyRedis:
    def __init__(self):
        self.store = {}
        self.queue = []
        self.processing = {}
        self.set = AsyncMock(side_effect=self._set)
        self.setex = AsyncMock(side_effect=self._set)
        self.get = AsyncMock(side_effect=self._get)
        self.exists = AsyncMock(side_effect=self._exists)
        self.zadd = AsyncMock(side_effect=self._zadd)
        self.zpopmin = AsyncMock(side_effect=self._zpopmin)
        self.zrange = AsyncMock(side_effect=self._zrange)
        self.zrem = AsyncMock(side_effect=self._zrem)
        self.zcard = AsyncMock(side_effect=lambda key: len(self.queue))
        self.scan = AsyncMock(side_effect=self._scan)
        self.ping = AsyncMock(return_value=True)
        self.aclose = AsyncMock(return_value=None)

    async def _set(self, key, *args, **kwargs):
        value = args[-1] if args else kwargs.get("value")
        self.store[key] = value
        return True

    async def _get(self, key):
        return self.store.get(key)

    async def _exists(self, key):
        return key in self.store

    async def _zadd(self, key, mapping):
        for member, score in mapping.items():
            self.queue.append((member, score))
        self.queue.sort(key=lambda item: item[1])
        return 1

    async def _zpopmin(self, key, count=1):
        if not self.queue:
            return []
        member, score = self.queue.pop(0)
        return [(member, score)]

    async def _zrange(self, key, start, stop, withscores=False):
        if not self.queue:
            return []
        member, score = self.queue[0]
        return [(member, score)] if withscores else [member]

    async def _zrem(self, key, member):
        self.queue = [item for item in self.queue if item[0] != member]
        return 1

    async def _scan(self, cursor, match=None, count=100):
        return 0, [key for key in self.store if match is None or key.startswith(match[:-1])]


class FakeTenantManager:
    def __init__(self):
        self._tenants: dict[str, Tenant] = {}

    async def create_tenant(self, name, description=None, quota=None, config=None):
        tenant = Tenant(
            tenant_id=f"tenant-{len(self._tenants) + 1}",
            name=name,
            description=description,
            quota=quota or TenantQuota(),
            config=config or {},
        )
        self._tenants[tenant.tenant_id] = tenant
        return tenant

    async def get_tenant(self, tenant_id):
        return self._tenants.get(tenant_id)

    async def update_tenant(self, tenant_id, updates):
        tenant = self._tenants[tenant_id]
        for key, value in updates.items():
            setattr(tenant, key, value)
        return tenant

    async def delete_tenant(self, tenant_id):
        self._tenants[tenant_id].status = TenantStatus.DELETED
        return True

    async def generate_api_key(self, tenant_id):
        tenant = self._tenants[tenant_id]
        tenant.api_keys.append({"key_hash": "hash", "is_active": True})
        return "tk-test"

    async def revoke_api_key(self, tenant_id, api_key):
        return True

    async def list_tenants(self, status=None, limit=100, offset=0):
        tenants = list(self._tenants.values())
        return tenants[offset : offset + limit]


@pytest.mark.asyncio
async def test_runtime_cache_and_prepare(monkeypatch):
    calls = []

    async def fake_ensure_schema():
        calls.append(True)

    monkeypatch.setattr("src.agent_platform.runtime.ensure_schema", fake_ensure_schema)
    monkeypatch.setenv("TASK_QUEUE_BACKEND", "redis")
    await prepare_runtime()
    assert calls == [True]


@pytest.mark.asyncio
async def test_dashboard_api_paths():
    metrics = AsyncMock()
    metrics.get_system_metrics.return_value = {
        "uptime_seconds": 12,
        "metrics": {"gauges": {"active_agents": 3}, "counters": {"total_tasks_submitted": 9, "total_tasks_completed": 7}},
    }
    tracer = SimpleNamespace(get_all_spans=lambda: [{"id": 1}], get_trace=lambda trace_id: [{"id": trace_id}])
    logs = SimpleNamespace()
    agent_registry = SimpleNamespace(
        list_all=AsyncMock(
            return_value=[
                SimpleNamespace(
                    agent_id="a1",
                    name="A1",
                    status=AgentStatus.ACTIVE,
                    capabilities=[AgentCapability(name="echo")],
                    last_heartbeat=datetime.now(UTC),
                )
            ]
        )
    )
    scheduler = SimpleNamespace(queue_size=AsyncMock(return_value=4), get_stats=AsyncMock(return_value=SimpleNamespace(total=10, pending=4, running=1, completed=3, failed=1, cancelled=1, timeout=0)))
    dashboard = DashboardAPI(metrics, tracer, logs, agent_registry, scheduler)

    status = await dashboard.get_system_status()
    assert status["agents"]["total"] == 1
    assert status["tasks"]["pending"] == 4

    agents = await dashboard.get_agents_status()
    assert agents[0]["agent_id"] == "a1"

    task_stats = await dashboard.get_task_stats()
    assert task_stats["total"] == 10

    traces = await dashboard.get_traces("trace-1")
    assert traces["traces"] == [{"id": "trace-1"}]

    logs_data = await dashboard.get_logs(limit=10, offset=1)
    assert logs_data["count"] == 0


@pytest.mark.asyncio
async def test_tenant_routes_and_scheduler_with_real_queue_logic():
    manager = FakeTenantManager()
    tenant = await manager.create_tenant("Acme")

    created = await tenant_routes.create_tenant(name="Acme 2", description=None, quota=None, config=None, manager=manager)
    assert created.name == "Acme 2"
    listed = await tenant_routes.list_tenants(manager=manager)
    assert listed["count"] == 2

    updated = await tenant_routes.update_tenant(tenant.tenant_id, {"description": "updated"}, manager=manager)
    assert updated.description == "updated"

    key = await tenant_routes.generate_api_key(tenant.tenant_id, manager=manager)
    assert key["api_key"] == "tk-test"

    revoked = await tenant_routes.revoke_api_key(tenant.tenant_id, "tk-test", manager=manager)
    assert revoked["status"] == "revoked"

    deleted = await tenant_routes.delete_tenant(tenant.tenant_id, manager=manager)
    assert deleted["status"] == "deleted"

    with pytest.raises(Exception):
        await tenant_routes.get_tenant("missing", manager=manager)


@pytest.mark.asyncio
async def test_redis_queue_full_lifecycle():
    redis = DummyRedis()
    queue = RedisTaskQueue(redis, ttl_seconds=60)
    task = Task(task_id="task-1", agent_id="agent", type="echo", payload={"msg": "hello"}, priority=TaskPriority.HIGH)

    await queue.enqueue(task)
    assert await queue.size() == 1
    assert queue._task_key("task-1") in redis.store
    assert redis.zadd.await_count >= 1

    fresh = Task(task_id="task-2", agent_id="agent", type="echo", payload={}, priority=TaskPriority.MEDIUM)
    await queue.enqueue(fresh)
    assert await queue.size() == 2
    stats = await queue.get_stats()
    assert stats.total >= 1
