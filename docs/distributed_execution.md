# Distributed Execution

The platform supports distributed execution across multiple nodes for scalability and fault tolerance.

# Distributed Execution

The platform supports distributed execution across multiple nodes for scalability and fault tolerance.

## Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     Client/API                          │
└─────────────────────────┬───────────────────────────────┘
                          │
┌─────────────────────────▼───────────────────────────────┐
│              Distributed Orchestrator                   │
│  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐      │
│  │  Registry   │  │    Queue    │  │    Lock     │      │
│  └─────────────┘  └─────────────┘  └─────────────┘      │
└─────────────────────────────────────────────────────────┘
                          │
        ┌─────────────────┼─────────────────┐
        ▼                 ▼                 ▼
   ┌────────┐        ┌────────┐        ┌────────┐
   │ Node 1 │        │ Node 2 │        │ Node 3 │
   │ Worker │        │ Worker │        │ Worker │
   └────────┘        └────────┘        └────────┘
```

text

## Components

### Node
Represents a physical/virtual node in the cluster with:
- Unique ID, hostname, IP, port
- Status (active, paused, offline)
- Heartbeat monitoring

### Worker Node
Executes tasks from the distributed queue:
- Polls for tasks
- Executes with concurrency control
- Reports results back

### Distributed Registry
Stores agent and node information in Redis:
- Agent registration and discovery
- Node registration and health tracking
- Automatic TTL for stale entries

### Distributed Task Queue
Priority-based task queue using Redis:
- Atomic ZPOPMIN for distributed safety
- Persistent task storage
- Status tracking across nodes

### Distributed Lock
Provides distributed locking using Redis:
- TTL-based auto-release
- Safe unlocking with ownership validation
- Context manager support

## Configuration

```python
from agent_platform.distributed import (
    DistributedOrchestrator,
    NodeInfo,
    WorkerConfig,
)

# Create orchestrator
orchestrator = DistributedOrchestrator(registry, queue, redis_client)

# Add a worker node
node_info = NodeInfo.create(port=8080)
config = WorkerConfig(max_concurrent_tasks=10)
await orchestrator.add_node(node_info, config)

# Acquire global lock
async with await orchestrator.acquire_global_lock("resource_lock"):
    # Critical section
    pass
Scaling
To add more nodes, simply start additional workers pointing to the same Redis instance.

Fault Tolerance
Node failure: Tasks are reassigned to other nodes

Task failure: Retry policies handle failures

Lock expiration: Auto-release prevents deadlocks




