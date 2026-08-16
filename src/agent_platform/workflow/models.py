
# Data models for workflows and steps

from datetime import UTC, datetime
from enum import StrEnum
from typing import Any

from pydantic import BaseModel, Field


class StepStatus(StrEnum):
    """Status of a single workflow step."""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"
    WAITING = "waiting"  # waiting for dependencies


class WorkflowStatus(StrEnum):
    """Status of the entire workflow."""
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"


class StepDependency(BaseModel):
    """Defines a dependency between steps."""
    depends_on: str = Field(..., description="ID of the step this depends on")
    condition: str | None = Field(
        None,
        description="Optional condition expression (e.g., 'result.status == success')"
    )


class WorkflowStep(BaseModel):
    """A single step in a workflow."""
    step_id: str = Field(..., description="Unique step identifier")
    name: str = Field(..., description="Human-readable name")
    description: str | None = None

    # What to execute: agent_id + task_type, or a tool call
    agent_id: str = Field(..., description="Agent that will execute this step")
    task_type: str = Field(..., description="Type of task to submit")
    payload: dict[str, Any] = Field(default_factory=dict)
    timeout_seconds: int = 60
    retry_count: int = 0

    # Dependencies
    dependencies: list[StepDependency] = Field(default_factory=list)

    # Output mapping: how to pass result to next steps
    output_key: str | None = Field(
        None,
        description="Key under which this step's result will be stored"
    )

    # Optional fallback step on failure
    fallback_step_id: str | None = None


class Workflow(BaseModel):
    """Complete workflow definition."""
    workflow_id: str = Field(..., description="Unique workflow ID")
    name: str = Field(..., description="Workflow name")
    description: str | None = None
    version: str = "1.0.0"

    # Steps in the workflow
    steps: list[WorkflowStep] = Field(..., description="List of steps")

    # Metadata
    created_at: datetime = Field(default_factory=lambda: datetime.now(UTC))
    updated_at: datetime | None = None
    tenant_id: str | None = None

    def get_step(self, step_id: str) -> WorkflowStep | None:
        """Get a step by ID."""
        for step in self.steps:
            if step.step_id == step_id:
                return step
        return None

    def get_dependents(self, step_id: str) -> list[str]:
        """Get all steps that depend on the given step."""
        dependents = []
        for step in self.steps:
            for dep in step.dependencies:
                if dep.depends_on == step_id:
                    dependents.append(step.step_id)
        return dependents

    def get_roots(self) -> list[str]:
        """Get step IDs that have no dependencies."""
        all_deps = set()
        for step in self.steps:
            for dep in step.dependencies:
                all_deps.add(dep.depends_on)
        return [s.step_id for s in self.steps if s.step_id not in all_deps]
