
# Unit tests for A2A communication

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock

from src.agent_platform.a2a.protocol import HandoverRequest, HandoverResponse, HandoverStatus
from src.agent_platform.a2a.context import ConversationContext, ContextSharingManager
from src.agent_platform.a2a.delegation import DelegationManager, DelegationRequest, DelegationResult
from src.agent_platform.a2a.router import RoutingAgent, RoutingStrategy
from src.agent_platform.a2a.collaboration import ChainCollaboration, ParallelCollaboration


@pytest.mark.asyncio
async def test_conversation_context():
    context = ConversationContext(session_id="session1")
    context.set("key1", "value1")
    assert context.get("key1") == "value1"
    context.push_history({"event": "test"})
    history = context.get_history()
    assert len(history) == 1

    # Serialization
    data = context.to_dict()
    context2 = ConversationContext.from_dict(data)
    assert context2.session_id == "session1"
    assert context2.get("key1") == "value1"


@pytest.mark.asyncio
async def test_context_sharing():
    manager = ContextSharingManager()
    context = manager.create_context("session1", {"data": "test"})
    shared = manager.share_context("session1", "agent2")
    assert shared["data"] == {"data": "test"}

    manager.receive_context({"session_id": "session2", "data": {"foo": "bar"}})
    assert manager.get_context("session2").get("foo") == "bar"


@pytest.mark.asyncio
async def test_routing_agent():
    # Mock registry
    from src.agent_platform.registry.in_memory import InMemoryAgentRegistry
    from src.agent_platform.core.agent import AgentRecord, AgentCapability

    registry = InMemoryAgentRegistry()
    agent1 = AgentRecord(
        agent_id="agent1",
        name="Agent1",
        capabilities=[AgentCapability(name="process"), AgentCapability(name="analyze")],
    )
    agent2 = AgentRecord(
        agent_id="agent2",
        name="Agent2",
        capabilities=[AgentCapability(name="process")],
    )
    await registry.register(agent1)
    await registry.register(agent2)

    router = RoutingAgent(registry, strategy=RoutingStrategy.CAPABILITY_MATCH)

    # Route by capability
    decision = await router.route("test", required_capabilities=["process"])
    assert decision.target_agent_id in ["agent1", "agent2"]

    decision = await router.route("test", required_capabilities=["analyze"])
    assert decision.target_agent_id == "agent1"


@pytest.mark.asyncio
async def test_delegation():
    context_manager = ContextSharingManager()
    delegation_manager = DelegationManager(context_manager)

    # Mock send function
    async def send_message(msg):
        pass

    request = DelegationRequest(
        from_agent="agent1",
        to_agent="agent2",
        task_type="test",
        task_payload={"x": 1},
    )

    del_id = await delegation_manager.delegate(request, send_message)
    assert del_id in delegation_manager._pending_delegations

    # Simulate response
    result = DelegationResult(
        delegation_id=del_id,
        status="completed",
        result="success",
        from_agent="agent2",
        to_agent="agent1",
    )
    delegation_manager._completed_delegations[del_id] = result
    del delegation_manager._pending_delegations[del_id]

    retrieved = delegation_manager.get_delegation_result(del_id)
    assert retrieved is not None
    assert retrieved.status == "completed"



@pytest.mark.asyncio
async def test_chain_collaboration():
    from src.agent_platform.a2a.collaboration import ChainCollaboration
    from src.agent_platform.a2a.context import ConversationContext

    context = ConversationContext(session_id="session1")
    router = AsyncMock()
    delegation_manager = AsyncMock()

    chain = ChainCollaboration(router, delegation_manager, agent_chain=["agent1", "agent2"])

    # Mock delegation
    delegation_manager.delegate = AsyncMock(return_value="del-123")
    delegation_manager._wait_for_delegation_result = AsyncMock(return_value={"result": "processed"})

    result = await chain.execute(
        {"initial_payload": "data", "required_capabilities": ["process"]}, context
    )
    assert "final_result" in result


@pytest.mark.asyncio
async def test_parallel_collaboration():
    from src.agent_platform.a2a.collaboration import ParallelCollaboration

    router = AsyncMock()
    delegation_manager = AsyncMock()

    parallel = ParallelCollaboration(router, delegation_manager)

    request = {
        "subtasks": [
            {"type": "task1", "capabilities": ["cap1"], "payload": {}},
            {"type": "task2", "capabilities": ["cap2"], "payload": {}},
        ]
    }
    context = ConversationContext(session_id="session1")

    # Mock router to return target agents
    router.route = AsyncMock(return_value=MagicMock(target_agent_id="agent1"))
    delegation_manager.delegate = AsyncMock(return_value="del-123")
    delegation_manager._wait_for_delegation_result = AsyncMock(return_value={"result": "ok"})

    result = await parallel.execute(request, context)
    assert "results" in result