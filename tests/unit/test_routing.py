# tests/unit/test_routing.py
# Unit tests for message routing

import pytest
from src.agent_platform.message_bus.routing import RoleRouter
from src.agent_platform.message_bus.models import RouteRule
from src.agent_platform.core.message import Message, MessageType


@pytest.mark.asyncio
async def test_role_router_add_rule():
    router = RoleRouter()
    rule = RouteRule(
        rule_id="rule1",
        name="Test Rule",
        conditions={"type": "command"},  # <-- اصلاح: کوچک
        target_roles=["processor"],
        priority=1,
    )
    router.add_rule(rule)
    assert len(router._rules) == 1


@pytest.mark.asyncio
async def test_role_router_remove_rule():
    router = RoleRouter()
    rule = RouteRule(
        rule_id="rule1",
        name="Test Rule",
        conditions={"type": "command"},
        target_roles=["processor"],
        priority=1,
    )
    router.add_rule(rule)
    removed = router.remove_rule("rule1")
    assert removed is True
    assert len(router._rules) == 0


@pytest.mark.asyncio
async def test_role_router_evaluate():
    router = RoleRouter()
    rule = RouteRule(
        rule_id="rule1",
        name="Test Rule",
        conditions={"type": "command"},
        target_roles=["processor"],
        priority=1,
    )
    router.add_rule(rule)
    msg = Message(
        from_agent="a1",
        to_agent="a2",
        type=MessageType.COMMAND,
        content={"cmd": "do"},
    )
    targets = router.evaluate(msg)
    assert targets == ["processor"]