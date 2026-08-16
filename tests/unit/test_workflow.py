
# Unit tests for Workflow Engine components: state management, parsing, and execution

import json
from pathlib import Path
from tempfile import NamedTemporaryFile

import pytest

from src.agent_platform.core.task import Task, TaskStatus
from src.agent_platform.scheduler.in_memory import InMemoryTaskQueue
from src.agent_platform.scheduler.scheduler import TaskScheduler
from src.agent_platform.workflow.executor import WorkflowExecutor
from src.agent_platform.workflow.models import (
    StepDependency,
    StepStatus,
    Workflow,
    WorkflowStatus,
    WorkflowStep,
)
from src.agent_platform.workflow.parser import WorkflowParser
from src.agent_platform.workflow.state import WorkflowStateManager

# ---- Fixtures ----

@pytest.fixture
def sample_workflow():
    """
    Provides a sample workflow with three steps where step2 and step3 depend on step1.
    """
    steps = [
        WorkflowStep(
            step_id="step1",
            name="First Step",
            agent_id="agent1",
            task_type="test",
            payload={"x": 1}
        ),
        WorkflowStep(
            step_id="step2",
            name="Second Step",
            agent_id="agent2",
            task_type="test",
            payload={"y": 2},
            dependencies=[StepDependency(depends_on="step1")]
        ),
        WorkflowStep(
            step_id="step3",
            name="Third Step",
            agent_id="agent3",
            task_type="test",
            payload={"z": 3},
            dependencies=[StepDependency(depends_on="step1")]
        )
    ]
    return Workflow(workflow_id="test_wf", name="Test Workflow", steps=steps)


# ---- Tests for WorkflowStateManager ----

@pytest.mark.asyncio
async def test_workflow_state_manager(sample_workflow):
    """
    Test basic state management: initial status, starting, step status updates,
    result storage, and detection of ready steps.
    """
    manager = WorkflowStateManager(sample_workflow)
    assert manager.workflow_status == WorkflowStatus.PENDING
    assert manager.get_step_status("step1") == StepStatus.PENDING

    # Start the workflow
    manager.start()
    assert manager.workflow_status == WorkflowStatus.RUNNING

    # Complete step1
    manager.set_step_status("step1", StepStatus.COMPLETED)
    manager.set_step_result("step1", "result1")
    assert manager.is_step_completed("step1") is True

    # Check which steps are ready (step2 and step3 depend on step1)
    ready = manager.get_ready_steps()
    assert "step2" in ready
    assert "step3" in ready


# ---- Tests for WorkflowParser ----

@pytest.mark.asyncio
async def test_workflow_parser():
    """
    Test parsing workflow definitions from JSON files.
    """
    # Create a temporary JSON file with a simple workflow
    data = {
        "workflow_id": "test",
        "name": "Test",
        "steps": [
            {
                "step_id": "s1",
                "name": "Step 1",
                "agent_id": "a1",
                "task_type": "echo",
                "payload": {"msg": "hello"}
            }
        ]
    }
    with NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        json.dump(data, f)
        f.flush()
        workflow = WorkflowParser.parse_file(Path(f.name))
        assert workflow.workflow_id == "test"
        assert len(workflow.steps) == 1


# ---- Tests for WorkflowExecutor ----

@pytest.mark.asyncio
async def test_workflow_executor(sample_workflow):
    """
    Test successful execution of a workflow where all steps complete successfully.
    """
    queue = InMemoryTaskQueue()
    scheduler = TaskScheduler(queue)

    state = WorkflowStateManager(sample_workflow)
    executor = WorkflowExecutor(scheduler, state)

    # Mock the task waiting function to return a completed task
    async def mock_wait(task_id):
        return Task(
            task_id=task_id,
            agent_id="agent1",
            type="test",
            payload={},
            status=TaskStatus.COMPLETED,
            result="mock result"
        )
    executor._wait_for_task = mock_wait

    await executor.execute()
    assert state.workflow_status == WorkflowStatus.COMPLETED
    for step in sample_workflow.steps:
        assert state.get_step_status(step.step_id) == StepStatus.COMPLETED


@pytest.mark.asyncio
async def test_workflow_executor_parallel():
    """
    Test parallel execution of independent steps (no dependencies).
    Both steps should run concurrently and complete successfully.
    """
    steps = [
        WorkflowStep(
            step_id="step1",
            name="Step 1",
            agent_id="agent1",
            task_type="test",
            payload={"x": 1}
        ),
        WorkflowStep(
            step_id="step2",
            name="Step 2",
            agent_id="agent2",
            task_type="test",
            payload={"y": 2}
        )
    ]
    workflow = Workflow(workflow_id="parallel_wf", name="Parallel", steps=steps)

    state = WorkflowStateManager(workflow)
    scheduler = TaskScheduler(InMemoryTaskQueue())
    executor = WorkflowExecutor(scheduler, state)

    async def mock_wait(task_id):
        return Task(
            task_id=task_id,
            agent_id="agent1",
            type="test",
            payload={},
            status=TaskStatus.COMPLETED,
            result="success"
        )
    executor._wait_for_task = mock_wait

    await executor.execute()
    assert state.workflow_status == WorkflowStatus.COMPLETED


@pytest.mark.asyncio
async def test_workflow_executor_with_fallback():
    """
    Test fallback mechanism: if a step fails and has a fallback step,
    the fallback is executed and the workflow can still complete.
    """
    steps = [
        WorkflowStep(
            step_id="step1",
            name="Main Step",
            agent_id="agent1",
            task_type="test",
            payload={},
            fallback_step_id="fallback"
        ),
        WorkflowStep(
            step_id="fallback",
            name="Fallback Step",
            agent_id="agent2",
            task_type="test",
            payload={"fallback": True}
        )
    ]
    workflow = Workflow(workflow_id="fallback_wf", name="Fallback", steps=steps)

    state = WorkflowStateManager(workflow)
    scheduler = TaskScheduler(InMemoryTaskQueue())
    executor = WorkflowExecutor(scheduler, state)

    # Simulate failure for step1 and success for fallback
    async def mock_wait(task_id):
        # Use a counter to distinguish calls
        if not hasattr(mock_wait, 'call_count'):
            mock_wait.call_count = 0
        mock_wait.call_count += 1

        if mock_wait.call_count == 1:
            # First call is for step1 (Main Step)
            return Task(
                task_id=task_id,
                agent_id="agent1",
                type="test",
                payload={},
                status=TaskStatus.FAILED,
                error="Simulated failure",
            )
        else:
            # Second call is for fallback step
            return Task(
                task_id=task_id,
                agent_id="agent2",
                type="test",
                payload={"fallback": True},
                status=TaskStatus.COMPLETED,
                result="Fallback succeeded",
            )

    executor._wait_for_task = mock_wait

    await executor.execute()
    assert state.workflow_status == WorkflowStatus.COMPLETED
    assert state.get_step_status("fallback") == StepStatus.COMPLETED


@pytest.mark.asyncio
async def test_workflow_executor_get_status():
    """
    Test that get_status returns the current workflow status.
    """
    steps = [WorkflowStep(step_id="s1", name="Step 1", agent_id="a1", task_type="test", payload={})]
    workflow = Workflow(workflow_id="wf_status", name="Status Test", steps=steps)
    state = WorkflowStateManager(workflow)
    scheduler = TaskScheduler(InMemoryTaskQueue())
    executor = WorkflowExecutor(scheduler, state)

    assert executor.get_status() == WorkflowStatus.PENDING
    state.start()
    assert executor.get_status() == WorkflowStatus.RUNNING
    state.complete()
    assert executor.get_status() == WorkflowStatus.COMPLETED


@pytest.mark.asyncio
async def test_workflow_executor_failure_handling():
    """Test workflow handling when a step fails without fallback."""
    steps = [
        WorkflowStep(
            step_id="s1", name="Failing Step", agent_id="a1", task_type="test", payload={}
        ),
        WorkflowStep(
            step_id="s2",
            name="Dependent Step",
            agent_id="a2",
            task_type="test",
            payload={},
            dependencies=[StepDependency(depends_on="s1")],
        ),
    ]
    workflow = Workflow(workflow_id="fail_wf", name="Failure Test", steps=steps)
    state = WorkflowStateManager(workflow)
    scheduler = TaskScheduler(InMemoryTaskQueue())
    executor = WorkflowExecutor(scheduler, state)

    # Mock failure for s1
    async def mock_wait(task_id):
        return Task(
            task_id=task_id,
            agent_id="a1",
            type="test",
            payload={},
            status=TaskStatus.FAILED,
            error="Failed",
        )

    executor._wait_for_task = mock_wait

    await executor.execute()
    assert state.workflow_status == WorkflowStatus.FAILED
    assert state.get_step_status("s1") == StepStatus.FAILED


@pytest.mark.asyncio
async def test_workflow_executor_timeout_handling():
    """Test workflow handling when a step times out."""
    steps = [
        WorkflowStep(
            step_id="s1",
            name="Timeout Step",
            agent_id="a1",
            task_type="test",
            payload={},
            timeout_seconds=1,
        )
    ]
    workflow = Workflow(workflow_id="timeout_wf", name="Timeout Test", steps=steps)
    state = WorkflowStateManager(workflow)
    scheduler = TaskScheduler(InMemoryTaskQueue())
    executor = WorkflowExecutor(scheduler, state)

    # Mock timeout
    async def mock_wait(task_id):
        return Task(
            task_id=task_id, agent_id="a1", type="test", payload={}, status=TaskStatus.TIMEOUT
        )

    executor._wait_for_task = mock_wait

    await executor.execute()
    assert state.workflow_status == WorkflowStatus.FAILED


@pytest.mark.asyncio
async def test_workflow_executor_step_status_transitions():
    """Test step status transitions during execution."""
    steps = [WorkflowStep(step_id="s1", name="Step 1", agent_id="a1", task_type="test", payload={})]
    workflow = Workflow(workflow_id="status_wf", name="Status Test", steps=steps)
    state = WorkflowStateManager(workflow)
    scheduler = TaskScheduler(InMemoryTaskQueue())
    executor = WorkflowExecutor(scheduler, state)

    async def mock_wait(task_id):
        return Task(
            task_id=task_id,
            agent_id="a1",
            type="test",
            payload={},
            status=TaskStatus.COMPLETED,
            result="done",
        )

    executor._wait_for_task = mock_wait

    # Initially PENDING
    assert state.get_step_status("s1") == StepStatus.PENDING

    await executor.execute()

    # Should be COMPLETED
    assert state.get_step_status("s1") == StepStatus.COMPLETED
    assert state.get_step_result("s1") == "done"
