# src/agent_platform/monitoring/logging.py
# Centralized structured logging

import json
import logging
import sys
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any


class LogLevel(StrEnum):
    """Log levels."""
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@dataclass
class LogEntry:
    """
    A structured log entry.
    """
    timestamp: datetime = field(default_factory=lambda: datetime.now(UTC))
    level: LogLevel = LogLevel.INFO
    message: str = ""
    logger_name: str = ""
    tenant_id: str | None = None
    trace_id: str | None = None
    span_id: str | None = None
    attributes: dict[str, Any] = field(default_factory=dict)
    exception: str | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "timestamp": self.timestamp.isoformat(),
            "level": self.level.value,
            "message": self.message,
            "logger": self.logger_name,
            "tenant_id": self.tenant_id,
            "trace_id": self.trace_id,
            "span_id": self.span_id,
            "attributes": self.attributes,
            "exception": self.exception,
        }

    def to_json(self) -> str:
        """Convert to JSON string."""
        return json.dumps(self.to_dict())


class LogManager:
    """
    Manages structured logging with support for multiple outputs.
    """

    def __init__(self, log_to_file: bool = False, log_file_path: str = "logs/app.log"):
        self._handlers: list[logging.Handler] = []
        self.log_to_file = log_to_file
        self.log_file_path = log_file_path

        # Configure standard logging
        self._setup_logging()

    def _setup_logging(self) -> None:
        """Set up Python logging.

        This is **idempotent**: ``LogManager`` is instantiated in many
        places (per-request dependencies, the worker entrypoint, and
        several test modules).  Attaching a handler on every construction
        multiplies every log line by the number of instantiations — e.g.
        the race test's ``dequeue: version conflict for ...`` line was
        emitted 6× to stdout while pytest's own caplog saw it once, because
        six ``StreamHandler``s had piled up on the root logger.  We now
        attach each handler at most once.
        """
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.DEBUG)

        # Console handler — attach only if root does not already have one
        # streaming to stdout.
        has_console = any(
            isinstance(h, logging.StreamHandler) and getattr(h, "stream", None) is sys.stdout
            for h in root_logger.handlers
        )
        if not has_console:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(logging.INFO)
            console_formatter = logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )
            console_handler.setFormatter(console_formatter)
            root_logger.addHandler(console_handler)

        # File handler (if enabled) — also attach at most once.
        if self.log_to_file:
            import os

            log_file_abs = os.path.abspath(self.log_file_path)
            os.makedirs(os.path.dirname(log_file_abs), exist_ok=True)
            has_file = any(
                isinstance(h, logging.FileHandler) and getattr(h, "baseFilename", None) == log_file_abs
                for h in root_logger.handlers
            )
            if not has_file:
                file_handler = logging.FileHandler(log_file_abs)
                file_handler.setLevel(logging.DEBUG)
                file_formatter = logging.Formatter(
                    '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
                )
                file_handler.setFormatter(file_formatter)
                root_logger.addHandler(file_handler)

    def log(
        self,
        message: str,
        level: LogLevel = LogLevel.INFO,
        logger_name: str | None = None,
        tenant_id: str | None = None,
        trace_id: str | None = None,
        span_id: str | None = None,
        attributes: dict[str, Any] | None = None,
        exception: Exception | None = None,
    ) -> LogEntry:
        """
        Create a structured log entry.
        """
        entry = LogEntry(
            level=level,
            message=message,
            logger_name=logger_name or __name__,
            tenant_id=tenant_id,
            trace_id=trace_id,
            span_id=span_id,
            attributes=attributes or {},
            exception=str(exception) if exception else None,
        )

        # Also log to standard logging
        log_level = self._to_logging_level(level)
        logger = logging.getLogger(entry.logger_name)
        log_method = getattr(logger, log_level)

        # Format the log message with context
        context = {
            "tenant": entry.tenant_id,
            "trace": entry.trace_id,
            "span": entry.span_id,
        }
        context_str = " ".join(f"{k}={v}" for k, v in context.items() if v)
        full_message = f"[{context_str}] {message}" if context_str else message

        if exception:
            log_method(full_message, exc_info=exception)
        else:
            log_method(full_message)

        return entry

    def debug(self, message: str, **kwargs) -> LogEntry:
        """Log at DEBUG level."""
        return self.log(message, LogLevel.DEBUG, **kwargs)

    def info(self, message: str, **kwargs) -> LogEntry:
        """Log at INFO level."""
        return self.log(message, LogLevel.INFO, **kwargs)

    def warning(self, message: str, **kwargs) -> LogEntry:
        """Log at WARNING level."""
        return self.log(message, LogLevel.WARNING, **kwargs)

    def error(self, message: str, **kwargs) -> LogEntry:
        """Log at ERROR level."""
        return self.log(message, LogLevel.ERROR, **kwargs)

    def critical(self, message: str, **kwargs) -> LogEntry:
        """Log at CRITICAL level."""
        return self.log(message, LogLevel.CRITICAL, **kwargs)

    def _to_logging_level(self, level: LogLevel) -> str:
        """Convert LogLevel to Python logging level."""
        mapping = {
            LogLevel.DEBUG: "debug",
            LogLevel.INFO: "info",
            LogLevel.WARNING: "warning",
            LogLevel.ERROR: "error",
            LogLevel.CRITICAL: "critical",
        }
        return mapping.get(level, "info")
