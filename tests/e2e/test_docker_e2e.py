from __future__ import annotations

import os

import httpx
import pytest


API_URL = os.getenv("PRODUCTION_VERIFY_API_URL", "http://127.0.0.1:8000")


def _enabled() -> bool:
    return os.getenv("RUN_DOCKER_E2E", "").lower() in {"1", "true", "yes"}


pytestmark = pytest.mark.e2e


@pytest.mark.skipif(not _enabled(), reason="Docker E2E is only run in CI or explicitly enabled")
@pytest.mark.asyncio
async def test_api_health_is_reachable() -> None:
    async with httpx.AsyncClient(base_url=API_URL, timeout=30.0, trust_env=False) as client:
        response = await client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"

