# Failure Recovery

The platform includes robust failure recovery mechanisms to ensure reliability.

## Components


Supports fixed delay and exponential backoff with jitter.

```python
from agent_platform.recovery import ExponentialBackoffRetry, RetryExecutor

policy = ExponentialBackoffRetry(base_delay=1.0, max_retries=5)
executor = RetryExecutor(policy)
result = await executor.execute(my_async_func)