
# Execution context for an agent instance

from typing import Dict, Any, Optional
from dataclasses import dataclass, field


@dataclass
class AgentContext:
    """
    Holds the execution context for a specific agent instance.
    Includes configuration, session info, and short-term memory.
    """

    agent_id: str
    tenant_id: Optional[str] = None

    # Configuration dictionary (e.g., model settings, API keys)
    config: Dict[str, Any] = field(default_factory=dict)

    # Short-term memory for the current session or conversation
    memory: Dict[str, Any] = field(default_factory=dict)

    # Session ID for tracking multi-turn interactions
    session_id: Optional[str] = None

    # Arbitrary variables that can be set during execution
    variables: Dict[str, Any] = field(default_factory=dict)

    def get(self, key: str, default: Any = None) -> Any:
        """Get a value from memory or variables."""
        if key in self.memory:
            return self.memory[key]
        return self.variables.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """Set a value in variables (temporary)."""
        self.variables[key] = value

    def remember(self, key: str, value: Any) -> None:
        """Store a value in memory (persistent for the session)."""
        self.memory[key] = value

    def to_dict(self) -> Dict[str, Any]:
        """Serialize context for logging or transfer."""
        return {
            "agent_id": self.agent_id,
            "tenant_id": self.tenant_id,
            "session_id": self.session_id,
            "config": self.config,
            "memory": self.memory,
            "variables": self.variables,
        }