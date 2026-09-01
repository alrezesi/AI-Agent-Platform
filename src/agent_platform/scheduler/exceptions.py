# Scheduler-level exceptions.


class TaskSchedulingError(Exception):
    """Base class for scheduler errors."""


class CrossTenantTaskConflictError(TaskSchedulingError):
    """
    Raised when a caller supplies a client-generated task_id that already
    exists under a *different* tenant.  Accepting it would let the caller
    overwrite (hijack) another tenant's task, so the submission is rejected.
    """

    def __init__(self, task_id: str, expected_tenant: str | None, actual_tenant: str | None) -> None:
        self.task_id = task_id
        self.expected_tenant = expected_tenant
        self.actual_tenant = actual_tenant
        super().__init__(
            f"Task id {task_id!r} already belongs to tenant {actual_tenant!r}; "
            f"cannot create/overwrite for tenant {expected_tenant!r}"
        )


class TaskWriteConflictError(TaskSchedulingError):
    """
    Raised when a task write is rejected because the row was modified by
    another writer since the caller last read it (optimistic-locking
    ``version`` check).  The caller must re-read the task and retry.
    """

    def __init__(self, task_id: str, expected_version: int, actual_version: int | None) -> None:
        self.task_id = task_id
        self.expected_version = expected_version
        self.actual_version = actual_version
        super().__init__(
            f"Task {task_id!r} was modified concurrently: expected "
            f"version {expected_version} but found {actual_version}"
        )
