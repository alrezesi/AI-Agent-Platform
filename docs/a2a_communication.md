# Agent-to-Agent Communication (A2A)

A2A enables agents to collaborate, delegate tasks, and share context.

## Core Concepts

Transfer a task or conversation from one agent to another.

```python
from agent_platform.a2a import HandoverRequest, HandoverResponse

request = HandoverRequest(
    from_agent="agent1",
    to_agent="agent2",
    task_id="task123",
    context={"state": "processing"}
)