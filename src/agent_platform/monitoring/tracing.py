
# Distributed tracing with OpenTelemetry-style spans

import logging
import time
import uuid
from contextvars import ContextVar
from dataclasses import dataclass, field
from typing import Any, Optional

logger = logging.getLogger(__name__)

# Context variable for current trace
_current_span: ContextVar[Optional['TraceSpan']] = ContextVar('current_span', default=None)


@dataclass
class TraceSpan:
    """
    A single span in a distributed trace.
    Represents a unit of work.
    """
    span_id: str
    trace_id: str
    name: str
    parent_span_id: str | None = None
    start_time: float = field(default_factory=time.perf_counter)
    end_time: float | None = None
    attributes: dict[str, Any] = field(default_factory=dict)
    events: list[dict[str, Any]] = field(default_factory=list)
    status: str = "ok"  # ok, error, unknown

    def end(self) -> None:
        """End the span."""
        self.end_time = time.perf_counter()

    def add_event(self, name: str, attributes: dict[str, Any] | None = None) -> None:
        """Add an event to the span."""
        self.events.append({
            "name": name,
            "timestamp": time.perf_counter(),
            "attributes": attributes or {},
        })

    def set_attribute(self, key: str, value: Any) -> None:
        """Set an attribute on the span."""
        self.attributes[key] = value

    def set_status(self, status: str) -> None:
        """Set the status of the span."""
        self.status = status

    @property
    def duration_ms(self) -> float:
        """Get the duration of the span in milliseconds."""
        if self.end_time:
            return (self.end_time - self.start_time) * 1000
        return (time.perf_counter() - self.start_time) * 1000

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for serialization."""
        return {
            "span_id": self.span_id,
            "trace_id": self.trace_id,
            "name": self.name,
            "parent_span_id": self.parent_span_id,
            "start_time": self.start_time,
            "end_time": self.end_time,
            "duration_ms": self.duration_ms,
            "attributes": self.attributes,
            "events": self.events,
            "status": self.status,
        }


@dataclass
class TracingConfig:
    """Configuration for tracing."""
    service_name: str = "ai-agent-platform"
    sample_rate: float = 1.0
    max_spans: int = 1000


class Tracer:
    """
    Distributed tracer that creates and manages spans.
    """

    def __init__(self, config: TracingConfig | None = None):
        self.config = config or TracingConfig()
        self._spans: list[TraceSpan] = []
        self._max_spans = self.config.max_spans

    def start_span(
        self,
        name: str,
        parent_span: TraceSpan | None = None,
        attributes: dict[str, Any] | None = None,
    ) -> TraceSpan:
        """
        Start a new span.
        """
        trace_id = parent_span.trace_id if parent_span else uuid.uuid4().hex[:16]
        parent_span_id = parent_span.span_id if parent_span else None

        span = TraceSpan(
            span_id=uuid.uuid4().hex[:8],
            trace_id=trace_id,
            name=name,
            parent_span_id=parent_span_id,
            attributes=attributes or {},
        )

        # Store span
        self._spans.append(span)
        if len(self._spans) > self._max_spans:
            self._spans.pop(0)

        # Set as current span in context
        _current_span.set(span)

        return span

    def get_current_span(self) -> TraceSpan | None:
        """Get the current span from context."""
        return _current_span.get()

    def end_span(self, span: TraceSpan) -> None:
        """End a span."""
        span.end()

    def get_all_spans(self) -> list[dict[str, Any]]:
        """Get all spans as dictionaries."""
        return [s.to_dict() for s in self._spans]

    def clear_spans(self) -> None:
        """Clear all spans."""
        self._spans.clear()

    def get_trace(self, trace_id: str) -> list[dict[str, Any]]:
        """Get all spans for a specific trace."""
        return [s.to_dict() for s in self._spans if s.trace_id == trace_id]
