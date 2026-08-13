
# Workflow state management for pause/resume

from typing import Dict, Any, Optional
from datetime import datetime, timezone

from .models import Workflow, WorkflowStatus, StepStatus


class WorkflowStateManager:
    """
    Manages the state of a workflow execution.
    Stores step results, statuses, and allows persistence.
    """

    def __init__(self, workflow: Workflow):
        self.workflow = workflow
        self.workflow_status = WorkflowStatus.PENDING
        self.step_statuses: Dict[str, StepStatus] = {}
        self.step_results: Dict[str, Any] = {}
        self.step_errors: Dict[str, str] = {}
        self.current_step: Optional[str] = None
        self.started_at: Optional[datetime] = None
        self.completed_at: Optional[datetime] = None

        # Initialize step statuses
        for step in workflow.steps:
            self.step_statuses[step.step_id] = StepStatus.PENDING

    def start(self) -> None:
        """Mark workflow as running."""
        self.workflow_status = WorkflowStatus.RUNNING
        self.started_at = datetime.now(timezone.utc)

    def pause(self) -> None:
        """Pause workflow execution."""
        self.workflow_status = WorkflowStatus.PAUSED

    def resume(self) -> None:
        """Resume workflow execution."""
        self.workflow_status = WorkflowStatus.RUNNING

    def complete(self) -> None:
        """Mark workflow as completed."""
        self.workflow_status = WorkflowStatus.COMPLETED
        self.completed_at = datetime.now(timezone.utc)

    def fail(self) -> None:
        """Mark workflow as failed."""
        self.workflow_status = WorkflowStatus.FAILED
        self.completed_at = datetime.now(timezone.utc)

    def set_step_status(self, step_id: str, status: StepStatus) -> None:
        """Update status of a step."""
        if step_id in self.step_statuses:
            self.step_statuses[step_id] = status

    def set_step_result(self, step_id: str, result: Any) -> None:
        """Store result of a step."""
        self.step_results[step_id] = result

    def set_step_error(self, step_id: str, error: str) -> None:
        """Store error of a step."""
        self.step_errors[step_id] = error

    def get_step_status(self, step_id: str) -> Optional[StepStatus]:
        """Get status of a step."""
        return self.step_statuses.get(step_id)

    def get_step_result(self, step_id: str) -> Optional[Any]:
        """Get result of a step."""
        return self.step_results.get(step_id)

    def is_step_completed(self, step_id: str) -> bool:
        """Check if a step is completed successfully."""
        return self.step_statuses.get(step_id) == StepStatus.COMPLETED

    def is_step_failed(self, step_id: str) -> bool:
        """Check if a step has failed."""
        return self.step_statuses.get(step_id) == StepStatus.FAILED

    def get_ready_steps(self) -> list:
        """
        Get step IDs that are ready to run:
        - Status is PENDING or WAITING
        - All dependencies are completed
        """
        ready = []
        for step in self.workflow.steps:
            if self.step_statuses[step.step_id] in (StepStatus.PENDING, StepStatus.WAITING):
                # Check all dependencies
                deps_met = True
                for dep in step.dependencies:
                    dep_status = self.step_statuses.get(dep.depends_on)
                    if dep_status != StepStatus.COMPLETED:
                        # If dependency failed and no condition, we might not run
                        # For simplicity, we only run if dependency completed
                        deps_met = False
                        break
                    # Check condition if any
                    if dep.condition:
                        # Very simple condition evaluation (could use expression engine)
                        # For now, assume condition is 'result == something'
                        # We'll just check if result exists
                        result = self.step_results.get(dep.depends_on)
                        if result is None:
                            deps_met = False
                            break
                if deps_met:
                    ready.append(step.step_id)
        return ready

    def to_dict(self) -> Dict[str, Any]:
        """Serialize state to dict for persistence."""
        return {
            "workflow_id": self.workflow.workflow_id,
            "workflow_status": self.workflow_status.value,
            "step_statuses": {k: v.value for k, v in self.step_statuses.items()},
            "step_results": self.step_results,
            "step_errors": self.step_errors,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any], workflow: Workflow) -> 'WorkflowStateManager':
        """Restore state from dict."""
        manager = cls(workflow)
        manager.workflow_status = WorkflowStatus(data['workflow_status'])
        manager.step_statuses = {
            k: StepStatus(v) for k, v in data['step_statuses'].items()
        }
        manager.step_results = data.get('step_results', {})
        manager.step_errors = data.get('step_errors', {})
        if data.get('started_at'):
            manager.started_at = datetime.fromisoformat(data['started_at'])
        if data.get('completed_at'):
            manager.completed_at = datetime.fromisoformat(data['completed_at'])
        return manager
