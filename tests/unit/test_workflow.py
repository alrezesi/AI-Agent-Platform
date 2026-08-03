
# Unit tests for Workflow Engine

import pytest
import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

from src.agent_platform.workflow.models import Workflow, WorkflowStep, StepDependency, WorkflowStatus, StepStatus
from src.agent_platform.workflow.state import WorkflowStateManager
from src.agent_platform.workflow.parser import WorkflowParser
from src.agent_platform.workflow.executor import WorkflowExecutor
from src.agent_platform.scheduler.in_memory import InMemoryTaskQueue
from src.agent_platform.scheduler.scheduler import TaskScheduler


@pytest.fixture
def sample_workflow():
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


@pytest.mark.asyncio
async def test_workflow_state_manager(sample_workflow):
    manager = WorkflowStateManager(sample_workflow)
    assert manager.workflow_status == WorkflowStatus.PENDING
    assert manager.get_step_status("step1") == StepStatus.PENDING

    manager.start()
    assert manager.workflow_status == WorkflowStatus.RUNNING

    manager.set_step_status("step1", StepStatus.COMPLETED)
    manager.set_step_result("step1", "result1")
    assert manager.is_step_completed("step1") is True

    ready = manager.get_ready_steps()
    # step2 and step3 depend on step1, so they should be ready now
    assert "step2" in ready
    assert "step3" in ready


@pytest.mark.asyncio
async def test_workflow_parser():
    # Create a temporary JSON file
    import json
    from tempfile import NamedTemporaryFile

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


@pytest.mark.asyncio
async def test_workflow_executor(sample_workflow):
    # Setup scheduler with a mock
    queue = InMemoryTaskQueue()
    scheduler = TaskScheduler(queue)

    # We need to mock agent execution, but for this test we'll just simulate
    # by modifying the scheduler's behavior.
    # In a real test, we would mock the task execution.

    state = WorkflowStateManager(sample_workflow)
    executor = WorkflowExecutor(scheduler, state)

    # Override _wait_for_task to simulate completion
    async def mock_wait(task_id):
        from src.agent_platform.core.task import Task, TaskStatus
        # Create a fake task
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