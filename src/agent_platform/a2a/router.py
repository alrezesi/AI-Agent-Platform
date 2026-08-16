
# Routing Agent for intelligent delegation of requests

import logging
from dataclasses import dataclass, field
from enum import StrEnum
from typing import Any

from src.agent_platform.core.agent import AgentRecord
from src.agent_platform.registry.base import BaseAgentRegistry

logger = logging.getLogger(__name__)


class RoutingStrategy(StrEnum):
    """Routing strategy for selecting an agent."""
    ROUND_ROBIN = "round_robin"
    LEAST_LOADED = "least_loaded"
    CAPABILITY_MATCH = "capability_match"
    PRIORITY = "priority"
    RANDOM = "random"


@dataclass
class RouteDecision:
    """
    Decision made by the router.
    """
    target_agent_id: str | None
    confidence: float = 1.0
    strategy: RoutingStrategy = RoutingStrategy.CAPABILITY_MATCH
    reason: str | None = None
    metadata: dict[str, Any] = field(default_factory=dict)


class RoutingAgent:
    """
    A specialized agent that routes requests to the most suitable agent
    based on capabilities, load, or other criteria.
    """

    def __init__(self, registry: BaseAgentRegistry, strategy: RoutingStrategy = RoutingStrategy.CAPABILITY_MATCH):
        self.registry = registry
        self.strategy = strategy
        self._round_robin_index: dict[str, int] = {}  # capability -> last index
        self._load_counts: dict[str, int] = {}  # agent_id -> current load

    async def route(
        self,
        request_type: str,
        required_capabilities: list[str] | None = None,
        context: dict[str, Any] | None = None,
        tenant_id: str | None = None,
    ) -> RouteDecision:
        """
        Route a request to the most appropriate agent.
        """
        if required_capabilities:
            # Find agents with matching capabilities
            agents = await self._find_agents_by_capability(required_capabilities, tenant_id)
            if not agents:
                logger.warning(f"No agents found with capabilities: {required_capabilities}")
                return RouteDecision(
                    target_agent_id=None,
                    confidence=0,
                    reason="No agents with matching capabilities",
                )
        else:
            # Get all agents
            agents = await self.registry.list_all(tenant_id=tenant_id)
            if not agents:
                return RouteDecision(
                    target_agent_id=None,
                    confidence=0,
                    reason="No agents available",
                )

        # Apply routing strategy
        if self.strategy == RoutingStrategy.CAPABILITY_MATCH:
            # Already filtered by capability, just pick the first one
            target = agents[0]
            confidence = 1.0
            reason = f"Capability match: {required_capabilities}"
        elif self.strategy == RoutingStrategy.ROUND_ROBIN:
            target = self._round_robin_select(agents, required_capabilities)
            confidence = 0.9
            reason = "Round-robin distribution"
        elif self.strategy == RoutingStrategy.LEAST_LOADED:
            target = self._least_loaded_select(agents)
            confidence = 0.9
            reason = "Least loaded agent"
        elif self.strategy == RoutingStrategy.RANDOM:
            import random
            target = random.choice(agents)
            confidence = 0.8
            reason = "Random selection"
        else:
            target = agents[0]
            confidence = 1.0
            reason = f"Default strategy: {self.strategy}"

        return RouteDecision(
            target_agent_id=target.agent_id,
            confidence=confidence,
            strategy=self.strategy,
            reason=reason,
        )

    async def _find_agents_by_capability(
        self,
        capabilities: list[str],
        tenant_id: str | None = None,
    ) -> list[AgentRecord]:
        """Find agents that have all required capabilities."""
        all_agents = await self.registry.list_all(tenant_id=tenant_id)
        matched = []
        for agent in all_agents:
            agent_caps = [cap.name for cap in agent.capabilities]
            # Check if all required capabilities are present
            if all(cap in agent_caps for cap in capabilities):
                matched.append(agent)
        return matched

    def _round_robin_select(self, agents: list[AgentRecord], required_capabilities: list[str] | None) -> AgentRecord:
        """Round-robin selection among agents."""
        cap_key = ",".join(sorted(required_capabilities or []))
        if cap_key not in self._round_robin_index:
            self._round_robin_index[cap_key] = 0
        idx = self._round_robin_index[cap_key]
        selected = agents[idx % len(agents)]
        self._round_robin_index[cap_key] = (idx + 1) % len(agents)
        return selected

    def _least_loaded_select(self, agents: list[AgentRecord]) -> AgentRecord:
        """Select the agent with the least load."""
        # Get load counts for each agent
        loads = {agent.agent_id: self._load_counts.get(agent.agent_id, 0) for agent in agents}
        # Select agent with minimum load
        min_agent = min(agents, key=lambda a: loads.get(a.agent_id, 0))
        # Update load (increment)
        self._load_counts[min_agent.agent_id] = self._load_counts.get(min_agent.agent_id, 0) + 1
        return min_agent

    def report_completion(self, agent_id: str) -> None:
        """Decrement load count when an agent completes a task."""
        if agent_id in self._load_counts:
            self._load_counts[agent_id] = max(0, self._load_counts[agent_id] - 1)
