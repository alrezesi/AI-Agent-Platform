# Workflow Engine

The Workflow Engine allows defining and executing multi-step workflows with dependencies, parallel execution, pause/resume, and state persistence.

## Key Concepts

- **Workflow**: A collection of steps with a unique ID, name, and version.
- **Step**: A single unit of work, executed by an agent via the scheduler.
- **Dependency**: A step can depend on one or more previous steps.
- **State**: The execution state of a workflow is persisted for pause/resume.

## Workflow Definition

Workflows are defined as JSON or YAML files with the following structure:

```json
{
  "workflow_id": "unique_id",
  "name": "My Workflow",
  "steps": [
    {
      "step_id": "step1",
      "name": "First Step",
      "agent_id": "agent_id",
      "task_type": "task_type",
      "payload": {},
      "timeout_seconds": 60,
      "retry_count": 0,
      "dependencies": [
        {"depends_on": "previous_step"}
      ],
      "output_key": "optional_key"
    }
  ]
}