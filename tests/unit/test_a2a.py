
# Unit tests for A2A communication

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.agent_platform.a2a.collaboration import ChainCollaboration, ParallelCollaboration
from src.agent_platform.a2a.context import ContextSharingManager, ConversationContext
from src.agent_platform.a2a.delegation import DelegationManager, DelegationRequest, DelegationResult
from src.agent_platform.a2a.router import RoutingAgent, RoutingStrategy
from src.agent_platform.core.agent import AgentCapability, AgentRecord
from src.agent_platform.registry.in_memory import InMemoryAgentRegistry


@pytest.mark.asyncio
async def test_conversation_context():
    context = ConversationContext(session_id="session1")
    context.set("key1", "value1")
    assert context.get("key1") == "value1"
    context.push_history({"event": "test"})
    history = context.get_history()
    assert len(history) == 1

    data = context.to_dict()
    context2 = ConversationContext.from_dict(data)
    assert context2.session_id == "session1"
    assert context2.get("key1") == "value1"


@pytest.mark.asyncio
async def test_context_sharing():
    manager = ContextSharingManager()
    _context = manager.create_context("session1", {"data": "test"})
    shared = manager.share_context("session1", "agent2")
    assert shared["data"] == {"data": "test"}

    manager.receive_context({"session_id": "session2", "data": {"foo": "bar"}})
    assert manager.get_context("session2").get("foo") == "bar"


@pytest.mark.asyncio
async def test_routing_agent():
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

    decision = await router.route("test", required_capabilities=["process"])
    assert decision.target_agent_id in ["agent1", "agent2"]

    decision = await router.route("test", required_capabilities=["analyze"])
    assert decision.target_agent_id == "agent1"


@pytest.mark.asyncio
async def test_delegation():
    context_manager = ContextSharingManager()
    delegation_manager = DelegationManager(context_manager)

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
    context = ConversationContext(session_id="session1")
    router = AsyncMock()
    delegation_manager = AsyncMock()

    chain = ChainCollaboration(router, delegation_manager, agent_chain=["agent1", "agent2"])

    delegation_manager.delegate = AsyncMock(return_value="del-123")
    delegation_manager._wait_for_delegation_result = AsyncMock(return_value={"result": "processed"})

    result = await chain.execute(
        {"initial_payload": "data", "required_capabilities": ["process"]},
        context
    )
    assert "final_result" in result


@pytest.mark.asyncio
async def test_parallel_collaboration():
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

    router.route = AsyncMock(return_value=MagicMock(target_agent_id="agent1"))
    delegation_manager.delegate = AsyncMock(return_value="del-123")
    delegation_manager._wait_for_delegation_result = AsyncMock(return_value={"result": "ok"})

    result = await parallel.execute(request, context)
    assert "results" in result



@pytest.mark.asyncio
async def test_delegation_manager_handle_delegation_request():
    context_manager = ContextSharingManager()
    manager = DelegationManager(context_manager)

    from src.agent_platform.a2a.protocol import A2AMessage, A2AMessageType

    msg = A2AMessage(
        message_id="m1",
        from_agent="a1",
        to_agent="a2",
        type=A2AMessageType.DELEGATION_REQUEST,
        content={
            "delegation_id": "del-123",
            "task_type": "test",
            "task_payload": {"x": 1},
            "session_id": "session1",
            "context_snapshot": {"data": {"foo": "bar"}},
        },
    )

    agent = AsyncMock()
    agent.handle_delegated_task = AsyncMock(return_value="result")

    result = await manager.handle_delegation_request(msg, agent)
    assert result.status == "completed"
    assert result.result == "result"

    # Check context was stored
    context = context_manager.get_context("session1")
    assert context is not None
    assert context.get("foo") == "bar"


@pytest.mark.asyncio
async def test_delegation_manager_handle_delegation_response():
    context_manager = ContextSharingManager()
    manager = DelegationManager(context_manager)

    # Simulate a pending delegation
    request = DelegationRequest(
        from_agent="a1",
        to_agent="a2",
        task_type="test",
        task_payload={},
    )
    manager._pending_delegations[request.delegation_id] = request

    # Simulate response
    from src.agent_platform.a2a.protocol import A2AMessage, A2AMessageType

    response_msg = A2AMessage(
        message_id="m2",
        from_agent="a2",
        to_agent="a1",
        type=A2AMessageType.DELEGATION_RESPONSE,
        content={
            "delegation_id": request.delegation_id,
            "status": "completed",
            "result": "success",
        },
    )

    callback_called = False

    def callback(result):
        nonlocal callback_called
        callback_called = True

    manager.register_callback(request.delegation_id, callback)

    result = manager.handle_delegation_response(response_msg)
    assert result is not None
    assert result.status == "completed"
    assert callback_called is True
    assert request.delegation_id not in manager._pending_delegations


# اضافه به tests/unit/test_a2a.py


@pytest.mark.asyncio
async def test_hierarchical_collaboration():
    """Test hierarchical collaboration pattern."""
    from src.agent_platform.a2a.collaboration import HierarchicalCollaboration

    router = AsyncMock()
    delegation_manager = AsyncMock()

    # Mock router to return workers
    router.route = AsyncMock(return_value=MagicMock(target_agent_id="worker1"))
    delegation_manager.delegate = AsyncMock(return_value="del-123")
    delegation_manager._wait_for_delegation_result = AsyncMock(return_value={"result": "ok"})

    collaboration = HierarchicalCollaboration(
        master_agent_id="master", router=router, delegation_manager=delegation_manager
    )

    context = ConversationContext(session_id="session1")
    request = {
        "type": "process",
        "capability": "worker",
        "data": {"items": [1, 2, 3]},
        "required_capabilities": ["process", "analyze"],
    }

    result = await collaboration.execute(request, context)
    assert "final_result" in result
    assert "workers" in result
    assert "worker_results" in result


@pytest.mark.asyncio
async def test_collaboration_orchestrator():
    """Test collaboration orchestrator with different patterns."""
    from src.agent_platform.a2a.collaboration import CollaborationOrchestrator

    router = AsyncMock()
    delegation_manager = AsyncMock()
    router.route = AsyncMock(return_value=MagicMock(target_agent_id="agent1"))
    delegation_manager.delegate = AsyncMock(return_value="del-123")
    delegation_manager._wait_for_delegation_result = AsyncMock(return_value={"result": "ok"})

    orchestrator = CollaborationOrchestrator(router, delegation_manager)
    context = ConversationContext(session_id="session1")

    # Test chain pattern
    result = await orchestrator.execute(
        pattern_type="chain",
        request={"initial_payload": "data", "required_capabilities": ["process"]},
        context=context,
        agent_chain=["agent1", "agent2"],
    )
    assert "final_result" in result

    # Test parallel pattern
    result = await orchestrator.execute(
        pattern_type="parallel",
        request={
            "subtasks": [
                {"type": "task1", "capabilities": ["cap1"], "payload": {}},
                {"type": "task2", "capabilities": ["cap2"], "payload": {}},
            ]
        },
        context=context,
    )
    assert "results" in result


@pytest.mark.asyncio
async def test_routing_agent_round_robin():
    """Test round-robin routing strategy."""
    registry = InMemoryAgentRegistry()
    for i in range(3):
        agent = AgentRecord(
            agent_id=f"agent{i + 1}",
            name=f"Agent{i + 1}",
            capabilities=[AgentCapability(name="process")],
        )
        await registry.register(agent)

    router = RoutingAgent(registry, strategy=RoutingStrategy.ROUND_ROBIN)

    # First request should go to agent1
    decision = await router.route("test", required_capabilities=["process"])
    assert decision.target_agent_id == "agent1"

    # Second request should go to agent2
    decision = await router.route("test", required_capabilities=["process"])
    assert decision.target_agent_id == "agent2"

    # Third request should go to agent3
    decision = await router.route("test", required_capabilities=["process"])
    assert decision.target_agent_id == "agent3"


@pytest.mark.asyncio
async def test_routing_agent_least_loaded():
    """Test least-loaded routing strategy."""
    registry = InMemoryAgentRegistry()
    for i in range(2):
        agent = AgentRecord(
            agent_id=f"agent{i + 1}",
            name=f"Agent{i + 1}",
            capabilities=[AgentCapability(name="process")],
        )
        await registry.register(agent)

    router = RoutingAgent(registry, strategy=RoutingStrategy.LEAST_LOADED)

    # First request goes to agent1 (both have load 0, first wins)
    decision = await router.route("test", required_capabilities=["process"])
    assert decision.target_agent_id == "agent1"

    # Agent1 has load 1, agent2 has load 0, so next goes to agent2
    decision = await router.route("test", required_capabilities=["process"])
    assert decision.target_agent_id == "agent2"

    # Report completion for agent1
    router.report_completion("agent1")

    # Agent1 and agent2 both have load 0, but agent1 should be selected first again
    decision = await router.route("test", required_capabilities=["process"])
    assert decision.target_agent_id == "agent1"
