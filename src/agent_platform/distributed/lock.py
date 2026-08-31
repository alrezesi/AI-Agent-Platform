
# Distributed lock using Redis with TTL and auto-release

import asyncio
import logging
import uuid
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from redis.asyncio import Redis
    RedisClient = Redis
else:
    RedisClient = Any

from .exceptions import LockError

logger = logging.getLogger(__name__)


class DistributedLock:
    """
    Distributed lock using Redis.
    Supports automatic release via TTL and safe unlocking.
    """

    def __init__(self, redis_client: RedisClient, key: str, ttl_seconds: int = 30):
        self.redis = redis_client
        self.key = f"dist:lock:{key}"
        self.ttl_seconds = ttl_seconds
        self._lock_id: str | None = None
        self._locked = False

    async def acquire(self, wait_timeout: float | None = None) -> bool:
        """
        Acquire the lock.
        If wait_timeout is provided, retry until timeout.
        Returns True if acquired, False otherwise.
        """
        self._lock_id = f"{uuid.uuid4().hex}:{id(self)}"
        start_time = asyncio.get_event_loop().time()

        while True:
            # Try to set key with NX (only if not exists) and TTL
            acquired = await self.redis.set(
                self.key,
                self._lock_id,
                nx=True,
                ex=self.ttl_seconds,
            )
            if acquired:
                self._locked = True
                logger.debug(f"Lock {self.key} acquired with ID {self._lock_id}")
                return True

            # Check if we should wait
            if wait_timeout is None:
                return False

            elapsed = asyncio.get_event_loop().time() - start_time
            if elapsed >= wait_timeout:
                return False

            # Wait before retrying
            await asyncio.sleep(0.1)

    async def release(self) -> bool:
        """
        Release the lock.
        Only releases if the lock is held by this instance.
        """
        if not self._locked or not self._lock_id:
            return False

        # Use Lua script for atomic check-and-delete
        lua_script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("del", KEYS[1])
        else
            return 0
        end
        """
        result = await self.redis.eval(lua_script, 1, self.key, self._lock_id)
        if result == 1:
            self._locked = False
            self._lock_id = None
            logger.debug(f"Lock {self.key} released")
            return True
        else:
            logger.warning(f"Lock {self.key} release failed: not owned by this instance")
            return False

    async def refresh(self) -> bool:
        """
        Refresh the lock TTL.
        Only works if the lock is still held by this instance.
        """
        if not self._locked or not self._lock_id:
            return False

        # Use Lua script to check ownership and refresh TTL
        lua_script = """
        if redis.call("get", KEYS[1]) == ARGV[1] then
            return redis.call("expire", KEYS[1], ARGV[2])
        else
            return 0
        end
        """
        result = await self.redis.eval(lua_script, 1, self.key, self._lock_id, self.ttl_seconds)
        if result == 1:
            logger.debug(f"Lock {self.key} refreshed")
            return True
        else:
            logger.warning(f"Lock {self.key} refresh failed: not owned by this instance")
            return False

    @property
    def is_locked(self) -> bool:
        return self._locked

    async def __aenter__(self):
        """Context manager entry."""
        acquired = await self.acquire()
        if not acquired:
            raise LockError(f"Could not acquire lock {self.key}")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        await self.release()
