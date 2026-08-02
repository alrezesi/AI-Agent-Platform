# Task Execution

Tasks are executed by the `AgentEngine` using a `TaskWorker` that provides:

- **Timeout**: Each task has a `timeout_seconds` field. If execution exceeds this, the task is marked as `TIMEOUT`.
- **Retry**: Tasks can be retried up to `max_retries` times with exponential backoff (base delay 1s, max 60s).
- **Result Handling**: Successful tasks store the result in `task.result`; failures store error message in `task.error`.

## Workflow

1. Scheduler submits a task to the queue.
2. Engine's dispatcher picks the task and assigns it to the appropriate agent.
3. The agent's worker loop invokes `TaskWorker.execute()`.
4. The worker calls `agent.run()` with timeout and catches exceptions.
5. On success/failure, the task status is updated and the scheduler is notified.

## Configuration

- `timeout_seconds`: per-task timeout.
- `max_retries`: number of retry attempts.
- Retry backoff: `delay = min(base * 2^attempt, max)`.

## Task Status Transitions

- `PENDING` → `RUNNING` → `COMPLETED`
- `PENDING` → `RUNNING` → `FAILED` (if error)
- `PENDING` → `RUNNING` → `TIMEOUT` (if timeout)
- `PENDING` → `CANCELLED` (if manually cancelled)

## Example

```python
# Submit a task with custom timeout and retry
task_id = await scheduler.submit_task(
    agent_id="agent-1",
    task_type="process",
    payload={"data": "..."},
    timeout_seconds=10,
    max_retries=3
)