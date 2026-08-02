
# Message validation utilities

from src.agent_platform.core.message import Message, MessageType
from src.agent_platform.message_bus.exceptions import MessageValidationError


class MessageValidator:
    """
    Validates messages for schema compliance, required fields, and business rules.
    """

    @staticmethod
    def validate(message: Message) -> None:
        """
        Perform comprehensive validation on a message.
        Raises MessageValidationError if validation fails.
        """
        # 1. Ensure required fields are present
        if not message.from_agent:
            raise MessageValidationError("from_agent is required")
        if not message.type:
            raise MessageValidationError("message type is required")

        # 2. Validate based on message type
        if message.type == MessageType.REQUEST:
            MessageValidator._validate_request(message)
        elif message.type == MessageType.RESPONSE:
            MessageValidator._validate_response(message)
        elif message.type == MessageType.BROADCAST:
            MessageValidator._validate_broadcast(message)
        elif message.type == MessageType.EVENT:
            MessageValidator._validate_event(message)
        elif message.type == MessageType.COMMAND:
            MessageValidator._validate_command(message)

        # 3. Validate TTL
        if message.ttl_seconds is not None and message.ttl_seconds <= 0:
            raise MessageValidationError("TTL must be positive")

        # 4. Validate tenant isolation (if tenant_id is provided, ensure consistency)
        # This can be extended

    @staticmethod
    def _validate_request(message: Message) -> None:
        """Validate a request message."""
        if not message.to_agent:
            raise MessageValidationError("Request messages must have a target agent")
        if not message.content:
            raise MessageValidationError("Request messages should have content")
        if not message.correlation_id:
            # We can generate one, but let's enforce it for traceability
            raise MessageValidationError("correlation_id is required for requests")

    @staticmethod
    def _validate_response(message: Message) -> None:
        """Validate a response message."""
        if not message.to_agent:
            raise MessageValidationError("Response messages must have a target agent")
        if not message.correlation_id:
            raise MessageValidationError("correlation_id is required for responses")

    @staticmethod
    def _validate_broadcast(message: Message) -> None:
        """Validate a broadcast message."""
        if message.to_agent:
            raise MessageValidationError("Broadcast messages should not have a specific target")
        if not message.content:
            raise MessageValidationError("Broadcast messages should have content")

    @staticmethod
    def _validate_event(message: Message) -> None:
        """Validate an event message."""
        if not message.content:
            raise MessageValidationError("Event messages should have content")

    @staticmethod
    def _validate_command(message: Message) -> None:
        """Validate a command message."""
        if not message.to_agent:
            raise MessageValidationError("Command messages must have a target agent")
        if not message.content:
            raise MessageValidationError("Command messages should have content")

    @staticmethod
    def is_valid(message: Message) -> bool:
        """Check if a message is valid without raising exceptions."""
        try:
            MessageValidator.validate(message)
            return True
        except MessageValidationError:
            return False