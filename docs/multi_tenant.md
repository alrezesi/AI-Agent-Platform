# Multi-Tenant Architecture

The platform supports multi-tenancy, allowing multiple organizations or users to use the same system with complete data isolation.

## Concepts

### Tenant
A tenant represents an organization or user. Each tenant has:
- Unique ID and name
- Status (active, suspended, inactive, deleted)
- Resource quotas (max agents, tasks, messages, etc.)
- API keys for authentication
- Custom configuration

### Isolation
All data is isolated at the tenant level:
- **Agent Registry**: Agents belong to a specific tenant.
- **Task Scheduler**: Tasks are associated with a tenant.
- **Message Bus**: Messages are tagged with tenant ID.
- **Workflows**: Workflows are scoped to a tenant.
- **Storage**: All persistent data includes tenant_id.

### Quotas
Resource limits per tenant:
- `max_agents`: Maximum number of agents
- `max_concurrent_tasks`: Maximum concurrent tasks
- `max_messages_per_second`: Message rate limit
- `max_storage_mb`: Storage limit
- `max_workflows`: Maximum number of workflows

## Usage

### Creating a Tenant

```python
from agent_platform.multi_tenant import TenantManager, TenantQuota

manager = TenantManager(storage)
tenant = await manager.create_tenant(
    name="Acme Corp",
    description="Enterprise customer",
    quota=TenantQuota(max_agents=50, max_concurrent_tasks=500)
)
API Key Authentication
python
# Generate API key
api_key = await manager.generate_api_key(tenant.tenant_id)

# Use in requests
headers = {
    "X-Tenant-ID": tenant.tenant_id,
    "X-API-Key": api_key
}
Tenant-Aware Operations
All operations automatically include tenant isolation:

python
# Register agent with tenant
agent = AgentRecord(
    agent_id="agent-1",
    tenant_id=tenant.tenant_id,
    # ...
)

# Submit task (tenant_id is derived from request context)
await scheduler.submit_task(
    agent_id="agent-1",
    task_type="process",
    payload={},
    tenant_id=tenant.tenant_id,
)
Checking Quotas
python
from agent_platform.multi_tenant import QuotaChecker

checker = QuotaChecker(tenant_manager)
await checker.check_agent_quota(tenant.tenant_id, current_count)
await checker.check_message_quota(tenant.tenant_id)
API Endpoints
Method	Endpoint	Description
POST	/tenants	Create tenant
GET	/tenants/{id}	Get tenant
PUT	/tenants/{id}	Update tenant
DELETE	/tenants/{id}	Delete tenant
POST	/tenants/{id}/api-keys	Generate API key
DELETE	/tenants/{id}/api-keys	Revoke API key
Middleware
The TenantMiddleware automatically extracts tenant information from request headers:

python
from agent_platform.multi_tenant import TenantMiddleware

app.add_middleware(TenantMiddleware, tenant_manager=manager)