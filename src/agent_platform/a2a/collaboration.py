
# Collaboration patterns for multi-agent systems

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any

from .context import ConversationContext
from .delegation import DelegationManager, DelegationRequest
from .exceptions import CollaborationError
from .router import RoutingAgent

logger = logging.getLogger(__name__)


class CollaborationPattern(ABC):
    """
    Abstract base class for collaboration patterns.
    """

    @abstractmethod
    async def execute(
        self,
        request: dict[str, Any],
        context: ConversationContext,
    ) -> dict[str, Any]:
        """
        Execute the collaboration pattern.
        """
        pass


class ChainCollaboration(CollaborationPattern):
    """
    Chain pattern: agents process sequentially, each passing result to the next.
    Agent1 -> Agent2 -> Agent3 -> ... -> Final
    """

    def __init__(
        self,
        router: RoutingAgent,
        delegation_manager: DelegationManager,
        agent_chain: list[str] | None = None,
    ):
        self.router = router
        self.delegation_manager = delegation_manager
        self.agent_chain = agent_chain or []

    async def execute(
        self,
        request: dict[str, Any],
        context: ConversationContext,
    ) -> dict[str, Any]:
        """
        Execute the chain: each agent processes and passes to the next.
        """
        result = request.get('initial_payload', {})
        chain = self.agent_chain

        # If no chain specified, use routing to determine chain dynamically
        if not chain:
            chain = await self._build_chain(request)

        for i, agent_id in enumerate(chain):
            logger.info(f"Chain step {i+1}/{len(chain)}: agent {agent_id}")
            try:
                # Delegate to agent
                delegation_request = DelegationRequest(
                    from_agent="chain_orchestrator",
                    to_agent=agent_id,
                    task_type="chain_step",
                    task_payload={
                        "step_index": i,
                        "input": result,
                        "request": request,
                        "context": context.data,
                    },
                    session_id=context.session_id,
                )
                del_id = await self.delegation_manager.delegate(
                    delegation_request,
                    send_message_fn=self._send_delegation_message,
                )

                # Wait for result (simplified: poll)
                # In real implementation, we'd use async callbacks
                # For now, we'll simulate with a timeout
                # The delegation manager would have a method to wait
                # For this example, we'll just simulate a result
                # In a full implementation, you'd have a proper async wait mechanism
                result = await self._wait_for_delegation_result(del_id)

            except Exception as e:
                logger.error(f"Chain step {i} failed: {e}")
                raise CollaborationError(f"Chain collaboration failed at step {i}: {e}") from e

        return {"final_result": result, "chain": chain}

    async def _build_chain(self, request: dict[str, Any]) -> list[str]:
        """Build chain dynamically using routing."""
        required_capabilities = request.get('required_capabilities', [])
        chain = []
        for cap in required_capabilities:
            decision = await self.router.route(
                request_type="chain_step",
                required_capabilities=[cap],
                context={"request": request},
            )
            if decision.target_agent_id:
                chain.append(decision.target_agent_id)
        return chain

    async def _send_delegation_message(self, message):
        """Stub: send message via message bus."""
        pass

    async def _wait_for_delegation_result(self, delegation_id: str) -> Any:
        """Wait for delegation result (simplified)."""
        # In a real implementation, use asyncio.Event
        # For now, we'll just return a dummy result
        return {"status": "completed", "result": "processed"}


class ParallelCollaboration(CollaborationPattern):
    """
    Parallel pattern: agents process independently, results are aggregated.
    All agents run simultaneously.
    """

    def __init__(
        self,
        router: RoutingAgent,
        delegation_manager: DelegationManager,
    ):
        self.router = router
        self.delegation_manager = delegation_manager

    async def execute(
        self,
        request: dict[str, Any],
        context: ConversationContext,
    ) -> dict[str, Any]:
        """
        Execute tasks in parallel across multiple agents.
        """
        subtasks = request.get('subtasks', [])
        if not subtasks:
            raise CollaborationError("No subtasks provided for parallel execution")

        tasks = []
        for subtask in subtasks:
            task = self._execute_single_subtask(subtask, context)
            tasks.append(task)

        results = await asyncio.gather(*tasks, return_exceptions=True)

        # Aggregate results
        aggregated = {
            "results": [],
            "errors": [],
            "success_count": 0,
            "failure_count": 0,
        }

        for i, result in enumerate(results):
            if isinstance(result, Exception):
                aggregated["errors"].append({"subtask_index": i, "error": str(result)})
                aggregated["failure_count"] += 1
            else:
                aggregated["results"].append(result)
                aggregated["success_count"] += 1

        return aggregated

    async def _execute_single_subtask(
        self,
        subtask: dict[str, Any],
        context: ConversationContext,
    ) -> Any:
        """Execute a single subtask."""
        # Route to appropriate agent
        decision = await self.router.route(
            request_type=subtask.get('type', 'default'),
            required_capabilities=subtask.get('capabilities', []),
            context={"subtask": subtask},
            tenant_id=context.metadata.get('tenant_id'),
        )

        if not decision.target_agent_id:
            raise CollaborationError(f"No agent found for subtask: {subtask}")

        # Delegate and wait
        delegation_request = DelegationRequest(
            from_agent="parallel_orchestrator",
            to_agent=decision.target_agent_id,
            task_type=subtask.get('type', 'unknown'),
            task_payload=subtask.get('payload', {}),
            session_id=context.session_id,
        )
        del_id = await self.delegation_manager.delegate(
            delegation_request,
            send_message_fn=self._send_delegation_message,
        )
        result = await self._wait_for_delegation_result(del_id)
        return result

    async def _send_delegation_message(self, message):
        pass

    async def _wait_for_delegation_result(self, delegation_id: str) -> Any:
        return {"status": "completed"}


class HierarchicalCollaboration(CollaborationPattern):
    """
    Hierarchical pattern: a master agent coordinates worker agents.
    Master splits task, workers process, master aggregates.
    """

    def __init__(
        self,
        master_agent_id: str,
        router: RoutingAgent,
        delegation_manager: DelegationManager,
    ):
        self.master_agent_id = master_agent_id
        self.router = router
        self.delegation_manager = delegation_manager

    async def execute(
        self,
        request: dict[str, Any],
        context: ConversationContext,
    ) -> dict[str, Any]:
        """
        Execute hierarchical collaboration.
        The master agent orchestrates the work.
        """
        # The master agent would typically handle splitting and aggregation
        # For this implementation, we simulate the master's role
        # The master delegates to workers and collects results

        workers = await self._select_workers(request, context)
        if not workers:
            raise CollaborationError("No workers available for hierarchical collaboration")

        # Master splits the task (simplified)
        subtasks = await self._split_task(request, workers, context)

        # Execute subtasks in parallel
        parallel = ParallelCollaboration(self.router, self.delegation_manager)
        results = await parallel.execute(
            {"subtasks": subtasks},
            context,
        )

        # Master aggregates
        final_result = await self._aggregate_results(results, context)

        return {
            "final_result": final_result,
            "workers": workers,
            "worker_results": results,
        }

    async def _select_workers(
        self,
        request: dict[str, Any],
        context: ConversationContext,
    ) -> list[str]:
        """Select worker agents based on capabilities."""
        required_caps = request.get('required_capabilities', [])
        workers = []
        for cap in required_caps:
            decision = await self.router.route(
                request_type="worker",
                required_capabilities=[cap],
                context=context.data,
            )
            if decision.target_agent_id:
                workers.append(decision.target_agent_id)
        return workers

    async def _split_task(
        self,
        request: dict[str, Any],
        workers: list[str],
        context: ConversationContext,
    ) -> list[dict[str, Any]]:
        """Split the main task into subtasks for workers."""
        # Simplified: assign one subtask per worker
        subtasks = []
        for i, worker in enumerate(workers):
            subtasks.append({
                "type": request.get('type', 'process'),
                "capabilities": [request.get('capability', 'general')],
                "payload": {
                    "worker_id": worker,
                    "subtask_id": f"subtask_{i}",
                    "data": request.get('data', {}),
                },
            })
        return subtasks

    async def _aggregate_results(self, results: dict[str, Any], context: ConversationContext) -> dict[str, Any]:
        """Aggregate results from workers."""
        # Simplified aggregation
        return {
            "aggregated": results,
            "total_success": results.get('success_count', 0),
            "total_failure": results.get('failure_count', 0),
        }


class CollaborationOrchestrator:
    """
    Orchestrates collaboration patterns based on request type.
    """

    def __init__(
        self,
        router: RoutingAgent,
        delegation_manager: DelegationManager,
    ):
        self.router = router
        self.delegation_manager = delegation_manager

    async def execute(
        self,
        pattern_type: str,
        request: dict[str, Any],
        context: ConversationContext,
        **kwargs,
    ) -> dict[str, Any]:
        """
        Execute a collaboration pattern.
        pattern_type: "chain", "parallel", "hierarchical"
        """
        if pattern_type == "chain":
            pattern = ChainCollaboration(self.router, self.delegation_manager, kwargs.get('agent_chain'))
        elif pattern_type == "parallel":
            pattern = ParallelCollaboration(self.router, self.delegation_manager)
        elif pattern_type == "hierarchical":
            master_agent = kwargs.get('master_agent_id')
            if not master_agent:
                raise CollaborationError("Master agent ID required for hierarchical collaboration")
            pattern = HierarchicalCollaboration(master_agent, self.router, self.delegation_manager)
        else:
            raise CollaborationError(f"Unknown pattern type: {pattern_type}")

        return await pattern.execute(request, context)
