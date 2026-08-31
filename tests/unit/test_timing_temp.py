import asyncio
import time
from datetime import UTC, datetime

import pytest


@pytest.mark.asyncio
async def test_timing_comparison():
    for i in range(5):
        t1 = time.perf_counter()
        dt1 = datetime.now(UTC)
        await asyncio.sleep(0.1)
        t2 = time.perf_counter()
        dt2 = datetime.now(UTC)
        print(f"perf: {(t2-t1)*1000:.1f}ms, datetime: {(dt2-dt1).total_seconds()*1000:.1f}ms", flush=True)
