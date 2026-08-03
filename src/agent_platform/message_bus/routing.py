
# Role-based routing utilities

from typing import Dict, List, Any, Optional
from src.agent_platform.message_bus.models import RouteRule


class RoleRouter:
    """
    Manages role-based routing rules and evaluates them.
    """

    def __init__(self):
        self._rules: Dict[str, RouteRule] = {}

    def add_rule(self, rule: RouteRule) -> None:
        """Add a routing rule."""
        self._rules[rule.rule_id] = rule

    def remove_rule(self, rule_id: str) -> bool:
        """Remove a routing rule."""
        if rule_id in self._rules:
            del self._rules[rule_id]
            return True
        return False

    def evaluate(self, message: Any, context: Optional[Dict[str, Any]] = None) -> List[str]:
        """
        Evaluate all rules and return target agent IDs or roles.
        """
        targets = []
        # Sort by priority (higher first)
        sorted_rules = sorted(
            self._rules.values(),
            key=lambda r: r.priority,
            reverse=True
        )
        for rule in sorted_rules:
            if not rule.is_active:
                continue
            if self._matches(rule, message, context):
                targets.extend(rule.target_agents)
                # We could also expand roles to agent IDs here
                break  # First matching rule wins
        return list(set(targets))

    def _matches(self, rule: RouteRule, message: Any, context: Optional[Dict] = None) -> bool:
        """Check if message matches rule conditions."""
        for key, expected in rule.conditions.items():
            # Support dot notation
            value = self._get_nested(message, key)
            if value != expected:
                return False
        return True

    def _get_nested(self, obj: Any, path: str) -> Any:
        """Get nested attribute using dot notation."""
        parts = path.split(".")
        current = obj
        for part in parts:
            if hasattr(current, part):
                current = getattr(current, part)
            elif isinstance(current, dict) and part in current:
                current = current[part]
            else:
                return None
        return current