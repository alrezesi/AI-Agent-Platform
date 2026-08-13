
# Conversation context sharing between agents

from typing import Dict, Any, Optional, List
from dataclasses import dataclass, field
from datetime import datetime, timezone
import json


@dataclass
class ConversationContext:
    """
    Shared context for a conversation or session.
    Agents can read from and write to this context.
    """
    session_id: str
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    updated_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    data: Dict[str, Any] = field(default_factory=dict)
    history: List[Dict[str, Any]] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def get(self, key: str, default: Any = None) -> Any:
        """Get a value from context data."""
        return self.data.get(key, default)

    def set(self, key: str, value: Any) -> None:
        """Set a value in context data."""
        self.data[key] = value
        self.updated_at = datetime.now(timezone.utc)

    def push_history(self, entry: Dict[str, Any]) -> None:
        """Add an entry to conversation history."""
        entry['timestamp'] = datetime.now(timezone.utc).isoformat()
        self.history.append(entry)
        self.updated_at = datetime.now(timezone.utc)

    def get_history(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent history entries."""
        return self.history[-limit:]

    def merge(self, other: Dict[str, Any]) -> None:
        """Merge data from another context."""
        self.data.update(other)
        self.updated_at = datetime.now(timezone.utc)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize to dictionary."""
        return {
            "session_id": self.session_id,
            "created_at": self.created_at.isoformat(),
            "updated_at": self.updated_at.isoformat(),
            "data": self.data,
            "history": self.history,
            "metadata": self.metadata,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ConversationContext':
        """Deserialize from dictionary."""
        context = cls(
            session_id=data['session_id'],
            created_at=datetime.fromisoformat(data['created_at']) if 'created_at' in data else datetime.now(timezone.utc),
            data=data.get('data', {}),
            history=data.get('history', []),
            metadata=data.get('metadata', {}),
        )
        context.updated_at = datetime.fromisoformat(data['updated_at']) if 'updated_at' in data else datetime.now(timezone.utc)
        return context


class ContextSharingManager:
    """
    Manages sharing of conversation context between agents.
    Supports passing context during handover and delegation.
    """

    def __init__(self):
        self._contexts: Dict[str, ConversationContext] = {}

    def create_context(self, session_id: str, initial_data: Optional[Dict[str, Any]] = None) -> ConversationContext:
        """Create a new conversation context."""
        context = ConversationContext(
            session_id=session_id,
            data=initial_data or {}
        )
        self._contexts[session_id] = context
        return context

    def get_context(self, session_id: str) -> Optional[ConversationContext]:
        """Retrieve a context by session ID."""
        return self._contexts.get(session_id)

    def update_context(self, session_id: str, updates: Dict[str, Any]) -> Optional[ConversationContext]:
        """Update a context with new data."""
        context = self._contexts.get(session_id)
        if context:
            context.merge(updates)
            return context
        return None

    def share_context(self, session_id: str, target_agent_id: str) -> Dict[str, Any]:
        """
        Prepare context for sharing with another agent.
        Returns the context data without sensitive metadata if needed.
        """
        context = self._contexts.get(session_id)
        if not context:
            return {}
        # Filter out sensitive data if needed (e.g., private keys)
        return {
            "session_id": context.session_id,
            "data": context.data,
            "history": context.get_history(5),  # Share last 5 entries
        }

    def receive_context(self, shared_data: Dict[str, Any]) -> str:
        """
        Receive shared context from another agent.
        Returns the session_id.
        """
        session_id = shared_data.get('session_id')
        if not session_id:
            return None
        # Create or update context with shared data
        if session_id in self._contexts:
            context = self._contexts[session_id]
            context.merge(shared_data.get('data', {}))
            for entry in shared_data.get('history', []):
                context.push_history(entry)
        else:
            context = ConversationContext(
                session_id=session_id,
                data=shared_data.get('data', {}),
                history=shared_data.get('history', []),
            )
            self._contexts[session_id] = context
        return session_id

    def delete_context(self, session_id: str) -> bool:
        """Delete a context."""
        if session_id in self._contexts:
            del self._contexts[session_id]
            return True
        return False
