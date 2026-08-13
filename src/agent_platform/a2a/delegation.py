
# Delegation: agent delegates a task to another agent

from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field
from datetime import datetime, timezone
import uuid
import logging

from .exceptions import DelegationError
from .protocol import A2AMessage, A2AMessageType
from .context import ConversationContext, ContextSharingManager

logger = logging.getLogger(__name__)


class DelegationRequest(BaseModel):
    """
    A request from one agent to delegate a task to another.
    """
    delegation_id: str = Field(default_factory=lambda: f"del-{uuid.uuid4().hex[:8]}")
    from_agent: str
    to_agent: str
    task_type: str
    task_payload: Dict[str, Any] = Field(default_factory=dict)
    session_id: Optional[str] = None
    context_snapshot: Optional[Dict[str, Any]] = None
    priority: int = 0
    deadline: Optional[datetime] = None
    callback_url: Optional[str] = None


class DelegationResult(BaseModel):
    """
    Result of a delegation.
    """
    delegation_id: str
    status: str  # "completed", "failed", "timeout"
    result: Optional[Any] = None
    error: Optional[str] = None
    from_agent: str
    to_agent: str
    completed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


class DelegationManager:
    """
    Manages delegation of tasks between agents.
    Tracks active delegations and handles callbacks.
    """

    def __init__(self, context_manager: ContextSharingManager):
        self.context_manager = context_manager
        self._pending_delegations: Dict[str, DelegationRequest] = {}
        self._completed_delegations: Dict[str, DelegationResult] = {}
        self._callbacks: Dict[str, list] = {}

    async def delegate(
        self,
        request: DelegationRequest,
        send_message_fn,  # Function to send A2A message
    ) -> str:
        """
        Delegate a task to another agent.
        Returns the delegation ID.
        """
        delegation_id = request.delegation_id
        self._pending_delegations[delegation_id] = request

        # Prepare A2A message
        message = A2AMessage(
            message_id=f"a2a-{uuid.uuid4().hex[:8]}",
            from_agent=request.from_agent,
            to_agent=request.to_agent,
            type=A2AMessageType.DELEGATION_REQUEST,
            content={
                "delegation_id": delegation_id,
                "task_type": request.task_type,
                "task_payload": request.task_payload,
                "session_id": request.session_id,
                "context_snapshot": request.context_snapshot,
            },
        )

        # Send delegation request
        try:
            await send_message_fn(message)
            logger.info(f"Delegation {delegation_id} sent from {request.from_agent} to {request.to_agent}")
        except Exception as e:
            raise DelegationError(f"Failed to send delegation request: {e}") from e

        return delegation_id

    async def handle_delegation_request(
        self,
        message: A2AMessage,
        agent_instance,  # The receiving agent instance
    ) -> DelegationResult:
        """
        Handle an incoming delegation request.
        The receiving agent executes the task and returns a result.
        """
        content = message.content
        delegation_id = content.get('delegation_id')
        task_type = content.get('task_type')
        task_payload = content.get('task_payload', {})
        session_id = content.get('session_id')
        context_snapshot = content.get('context_snapshot')

        # Store context if provided
        if session_id and context_snapshot:
            # Receive shared context
            self.context_manager.receive_context({
                "session_id": session_id,
                "data": context_snapshot.get('data', {}),
                "history": context_snapshot.get('history', []),
            })

        try:
            # Execute the delegated task using the receiving agent
            # The agent should implement a method to handle delegated tasks
            if hasattr(agent_instance, 'handle_delegated_task'):
                result = await agent_instance.handle_delegated_task(
                    task_type=task_type,
                    payload=task_payload,
                    session_id=session_id,
                )
                status = "completed"
                error = None
            else:
                # Fallback: use the agent's run method
                result = await agent_instance.run(task_payload)
                status = "completed"
                error = None
        except Exception as e:
            result = None
            status = "failed"
            error = str(e)

        # Create result
        delegation_result = DelegationResult(
            delegation_id=delegation_id,
            status=status,
            result=result,
            error=error,
            from_agent=message.from_agent,
            to_agent=message.to_agent,
        )

        # Store completed delegation
        self._completed_delegations[delegation_id] = delegation_result

        # Send response back
        response = A2AMessage(
            message_id=f"a2a-{uuid.uuid4().hex[:8]}",
            from_agent=message.to_agent,
            to_agent=message.from_agent,
            type=A2AMessageType.DELEGATION_RESPONSE,
            content={
                "delegation_id": delegation_id,
                "status": status,
                "result": result,
                "error": error,
            },
        )
        # The caller will handle sending this response via the message bus

        return delegation_result

    def handle_delegation_response(self, message: A2AMessage) -> Optional[DelegationResult]:
        """
        Handle a delegation response (result from delegated task).
        """
        content = message.content
        delegation_id = content.get('delegation_id')
        status = content.get('status')
        result = content.get('result')
        error = content.get('error')

        if delegation_id not in self._pending_delegations:
            logger.warning(f"Received response for unknown delegation: {delegation_id}")
            return None

        # Create delegation result
        delegation_result = DelegationResult(
            delegation_id=delegation_id,
            status=status,
            result=result,
            error=error,
            from_agent=message.from_agent,
            to_agent=message.to_agent,
        )

        self._completed_delegations[delegation_id] = delegation_result
        del self._pending_delegations[delegation_id]

        # Trigger callbacks
        if delegation_id in self._callbacks:
            for callback in self._callbacks[delegation_id]:
                callback(delegation_result)
            del self._callbacks[delegation_id]

        return delegation_result

    def get_delegation_result(self, delegation_id: str) -> Optional[DelegationResult]:
        """Get the result of a completed delegation."""
        return self._completed_delegations.get(delegation_id)

    def register_callback(self, delegation_id: str, callback):
        """Register a callback for delegation completion."""
        if delegation_id not in self._callbacks:
            self._callbacks[delegation_id] = []
        self._callbacks[delegation_id].append(callback)
