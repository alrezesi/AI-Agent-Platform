# src/agent_platform/workflow/executor.py
# Workflow executor: runs steps with dependencies, parallel execution, pause/resume

import asyncio
import logging
from typing import Dict, Optional, List, Any
from datetime import datetime

from .models import Workflow, WorkflowStep, StepStatus, WorkflowStatus
from .state import WorkflowStateManager
from .exceptions import WorkflowExecutionError, WorkflowStepError
from src.agent_platform.scheduler.scheduler import TaskScheduler
from src.agent_platform.core.task import TaskStatus

logger = logging.getLogger(__name__)


class WorkflowExecutor:
    """
    Executes a workflow step by step, respecting dependencies.
    Supports parallel execution, pause/resume, and state persistence.
    """

    def __init__(self, scheduler: TaskScheduler, state_manager: WorkflowStateManager):
        self.scheduler = scheduler
        self.state = state_manager
        self._running = False
        self._task = None

    async def execute(self) -> None:
        """
        Execute the workflow from its current state.
        If state is PAUSED, it will resume.
        """
        if self._running:
            logger.warning("Workflow already running")
            return

        self._running = True
        try:
            # If workflow is PENDING, start it
            if self.state.workflow_status == WorkflowStatus.PENDING:
                self.state.start()
            elif self.state.workflow_status == WorkflowStatus.PAUSED:
                self.state.resume()
            elif self.state.workflow_status == WorkflowStatus.RUNNING:
                # Already running, continue
                pass
            else:
                raise WorkflowExecutionError(
                    f"Cannot execute workflow in status {self.state.workflow_status}"
                )

            # Main execution loop
            while self.state.workflow_status == WorkflowStatus.RUNNING:
                ready_steps = self.state.get_ready_steps()
                if not ready_steps:
                    # No steps ready: check if all steps are done
                    all_done = all(
                        self.state.get_step_status(s.step_id) in
                        (StepStatus.COMPLETED, StepStatus.FAILED, StepStatus.SKIPPED)
                        for s in self.state.workflow.steps
                    )
                    if all_done:
                        self.state.complete()
                        logger.info(f"Workflow {self.state.workflow.workflow_id} completed")
                        break
                    else:
                        # Some steps are still pending but not ready: wait
                        await asyncio.sleep(0.5)
                        continue

                # Execute ready steps in parallel
                tasks = []
                for step_id in ready_steps:
                    step = self.state.workflow.get_step(step_id)
                    if step:
                        tasks.append(self._execute_step(step))

                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)

                # After executing steps, check for failures with fallback handling
                workflow_failed = False
                failed_step_id = None
                for step in self.state.workflow.steps:
                    step_status = self.state.get_step_status(step.step_id)
                    if step_status == StepStatus.FAILED:
                        # Check if this step has a fallback that succeeded
                        fallback_step_id = step.fallback_step_id
                        if fallback_step_id:
                            fallback_status = self.state.get_step_status(fallback_step_id)
                            if fallback_status == StepStatus.COMPLETED:
                                # Fallback succeeded, treat this step as completed
                                self.state.set_step_status(step.step_id, StepStatus.COMPLETED)
                                logger.info(f"Step {step.step_id} marked as completed because fallback succeeded")
                                continue
                            else:
                                # Fallback failed or not run, workflow fails
                                workflow_failed = True
                                failed_step_id = step.step_id
                                break
                        else:
                            workflow_failed = True
                            failed_step_id = step.step_id
                            break

                if workflow_failed:
                    self.state.fail()
                    logger.error(f"Workflow failed due to step {failed_step_id}")
                    break

        except asyncio.CancelledError:
            # If cancelled, pause the workflow
            self.state.pause()
            logger.info(f"Workflow {self.state.workflow.workflow_id} paused")
            raise
        except Exception as e:
            self.state.fail()
            logger.error(f"Workflow execution error: {e}")
            raise WorkflowExecutionError(f"Workflow execution failed: {e}") from e
        finally:
            self._running = False

    async def _execute_step(self, step: WorkflowStep) -> None:
        """
        Execute a single workflow step by submitting a task to the scheduler.
        """
        step_id = step.step_id
        self.state.set_step_status(step_id, StepStatus.RUNNING)
        logger.info(f"Executing step {step_id} ({step.name})")

        try:
            # Submit task to scheduler
            task_id = await self.scheduler.submit_task(
                agent_id=step.agent_id,
                task_type=step.task_type,
                payload=step.payload,
                timeout_seconds=step.timeout_seconds,
                max_retries=step.retry_count,
                tenant_id=self.state.workflow.tenant_id,
            )

            # Wait for task completion (poll)
            task = await self._wait_for_task(task_id)

            if task.status == TaskStatus.COMPLETED:
                # Success
                self.state.set_step_status(step_id, StepStatus.COMPLETED)
                self.state.set_step_result(step_id, task.result)
                if step.output_key:
                    # Store output in a shared context (could be used by later steps)
                    # For simplicity, we store in step_results
                    self.state.step_results[step.output_key] = task.result
                logger.info(f"Step {step_id} completed successfully")
            else:
                # Failure
                error_msg = task.error or "Task failed"
                self.state.set_step_status(step_id, StepStatus.FAILED)
                self.state.set_step_error(step_id, error_msg)
                logger.error(f"Step {step_id} failed: {error_msg}")
                # Optionally trigger fallback
                if step.fallback_step_id:
                    await self._execute_fallback(step.fallback_step_id, error_msg)

        except Exception as e:
            self.state.set_step_status(step_id, StepStatus.FAILED)
            self.state.set_step_error(step_id, str(e))
            logger.error(f"Step {step_id} execution error: {e}")
            if step.fallback_step_id:
                await self._execute_fallback(step.fallback_step_id, str(e))

    async def _wait_for_task(self, task_id: str, poll_interval: float = 0.5) -> Any:
        """
        Poll the scheduler until the task is completed or failed.
        Returns the task object.
        """
        while True:
            task = await self.scheduler.get_task(task_id)
            if task.status in (TaskStatus.COMPLETED, TaskStatus.FAILED, TaskStatus.TIMEOUT, TaskStatus.CANCELLED):
                return task
            await asyncio.sleep(poll_interval)

    async def _execute_fallback(self, fallback_step_id: str, error_msg: str) -> None:
        """
        Execute a fallback step when a step fails.
        """
        fallback_step = self.state.workflow.get_step(fallback_step_id)
        if not fallback_step:
            logger.warning(f"Fallback step {fallback_step_id} not found")
            return
        logger.info(f"Executing fallback step {fallback_step_id}")
        # Mark as waiting to avoid duplicate execution
        self.state.set_step_status(fallback_step_id, StepStatus.RUNNING)
        await self._execute_step(fallback_step)

    async def pause(self) -> None:
        """Pause the workflow execution."""
        if self._running and self.state.workflow_status == WorkflowStatus.RUNNING:
            # Cancel the main task to pause
            if self._task:
                self._task.cancel()
                try:
                    await self._task
                except asyncio.CancelledError:
                    pass
            self.state.pause()
            logger.info(f"Workflow {self.state.workflow.workflow_id} paused")

    async def resume(self) -> None:
        """Resume a paused workflow."""
        if self.state.workflow_status == WorkflowStatus.PAUSED:
            # Start execution again
            self._task = asyncio.create_task(self.execute())
            await self._task

    def get_status(self) -> WorkflowStatus:
        """Get current workflow status."""
        return self.state.workflow_status