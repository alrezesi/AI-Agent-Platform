# Message Bus 

The message bus enables communication between agents with advanced features:

## Features

1. **Point-to-Point**: Direct messages to a specific agent.
2. **Broadcast**: Send to all subscribed agents.
3. **Topic-based Pub/Sub**: Publish to topics; subscribers receive messages.
4. **Role-based Routing**: Route messages to agents based on their roles.
5. **Message Persistence**: Store messages for history and replay.
6. **Acknowledgment**: Agents can acknowledge message processing.
7. **Delivery Status**: Track delivery status per recipient.

## Subscription Types

| Type | Description |
|------|-------------|
| `POINT_TO_POINT` | Direct messages addressed to the agent. |
| `TOPIC` | Messages published to specific topics. |
| `BROADCAST` | All broadcast messages. |
| `ROLE` | Messages routed by agent roles. |

## Routing Rules

Rules are evaluated in priority order. The first matching rule determines the recipients.

Example:
```python
rule = RouteRule(
    rule_id="high_priority",
    name="High Priority Events",
    conditions={"message.type": "EVENT", "message.priority": "HIGH"},
    target_roles=["critical_processor"],
    priority=10
)