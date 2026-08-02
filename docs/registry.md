# Agent Registry

The registry manages the lifecycle of agents, providing registration, discovery, and heartbeat monitoring.

## Interfaces

All registries implement `BaseAgentRegistry`:

- `register(agent)`: Register or update an agent.
- `unregister(agent_id)`: Remove an agent.
- `get_agent(agent_id)`: Retrieve a single agent.
- `heartbeat(agent_id)`: Refresh the agent's last seen timestamp.
- `discover(capability, status, tenant_id, limit, offset)`: Find agents matching criteria.
- `cleanup_stale(ttl_seconds)`: Remove agents that haven't sent a heartbeat.

## Implementations

| Implementation | Use Case |
|----------------|----------|
| `InMemoryAgentRegistry` | Unit tests, local development |
| `RedisAgentRegistry` | Caching, ephemeral state, auto-TTL cleanup |
| `PostgresAgentRegistry` | Persistent storage, production multi-node |

## Configuration

### Redis
```python
from redis.asyncio import Redis
from src.agent_platform.registry import RedisAgentRegistry

redis_client = Redis.from_url("redis://localhost:6379")
registry = RedisAgentRegistry(redis_client, ttl_seconds=60)