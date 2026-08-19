from __future__ import annotations

import os

import pytest


def _enabled() -> bool:
    return os.getenv("RUN_DOCKER_CHAOS", "").lower() in {"1", "true", "yes"}


pytestmark = pytest.mark.chaos


@pytest.mark.skipif(not _enabled(), reason="Docker chaos tests are only run in CI or explicitly enabled")
def test_placeholder_for_real_worker_and_redis_failures() -> None:
    # This file exists so CI can own the real failure-injection tests against the Docker stack.
    # The actual execution is gated behind RUN_DOCKER_CHAOS.
    assert True

