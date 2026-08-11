import asyncio
import fnmatch
import sys
import types
from collections import defaultdict

import pytest


def _install_fake_redis() -> None:
    try:
        import redis.asyncio  # noqa: F401
        return
    except ImportError:
        pass

    class _RedisState:
        def __init__(self) -> None:
            self.kv = {}
            self.zsets = defaultdict(dict)
            self.sets = defaultdict(set)
            self.lists = defaultdict(list)
            self.pubsubs = []

        def clear(self) -> None:
            self.kv.clear()
            self.zsets.clear()
            self.sets.clear()
            self.lists.clear()

    class _FakePubSub:
        def __init__(self, client):
            self._client = client
            self._channels = set()
            self._queue = asyncio.Queue()
            self._closed = False
            client._state.pubsubs.append(self)

        async def connect(self):
            return None

        async def subscribe(self, *channels):
            self._channels.update(channels)

        async def unsubscribe(self, *channels):
            if channels:
                self._channels.difference_update(channels)
            else:
                self._channels.clear()

        async def get_message(self, ignore_subscribe_messages=True, timeout=1.0):
            if self._closed:
                return None
            try:
                return await asyncio.wait_for(self._queue.get(), timeout=timeout)
            except asyncio.TimeoutError:
                return None

        async def aclose(self):
            self._closed = True
            if self in self._client._state.pubsubs:
                self._client._state.pubsubs.remove(self)

    class Redis:
        _states = {}

        def __init__(self, url="redis://localhost:6379/0"):
            self.url = url
            self._state = self._states.setdefault(url, _RedisState())

        @classmethod
        def from_url(cls, url, *args, **kwargs):
            return cls(url)

        def pubsub(self):
            return _FakePubSub(self)

        async def flushall(self):
            self._state.clear()
            return True

        async def aclose(self):
            return None

        async def set(self, key, value, nx=False, ex=None):
            if nx and key in self._state.kv:
                return False
            self._state.kv[key] = value.encode("utf-8") if isinstance(value, str) else value
            return True

        async def setex(self, key, ttl, value):
            return await self.set(key, value, ex=ttl)

        async def get(self, key):
            return self._state.kv.get(key)

        async def exists(self, key):
            return 1 if key in self._state.kv else 0

        async def delete(self, *keys):
            removed = 0
            for key in keys:
                if key in self._state.kv:
                    del self._state.kv[key]
                    removed += 1
            return removed

        async def zadd(self, key, mapping):
            bucket = self._state.zsets[key]
            bucket.update(mapping)
            return len(mapping)

        async def zpopmin(self, key, count=1):
            bucket = self._state.zsets.get(key, {})
            items = sorted(bucket.items(), key=lambda item: item[1])[:count]
            for member, _ in items:
                bucket.pop(member, None)
            return items

        async def zrange(self, key, start, end, withscores=False):
            bucket = self._state.zsets.get(key, {})
            items = sorted(bucket.items(), key=lambda item: item[1])
            sliced = items[start : None if end == -1 else end + 1]
            if withscores:
                return sliced
            return [member for member, _ in sliced]

        async def zrem(self, key, *members):
            bucket = self._state.zsets.get(key, {})
            removed = 0
            for member in members:
                if member in bucket:
                    del bucket[member]
                    removed += 1
            return removed

        async def zcard(self, key):
            return len(self._state.zsets.get(key, {}))

        async def scan(self, cursor=0, match=None, count=100):
            keys = list(self._state.kv.keys())
            if match:
                keys = [key for key in keys if fnmatch.fnmatch(key, match)]
            return 0, keys

        async def sadd(self, key, *members):
            bucket = self._state.sets[key]
            before = len(bucket)
            bucket.update(members)
            return len(bucket) - before

        async def smembers(self, key):
            return set(self._state.sets.get(key, set()))

        async def srem(self, key, *members):
            bucket = self._state.sets.get(key, set())
            removed = 0
            for member in members:
                if member in bucket:
                    bucket.remove(member)
                    removed += 1
            return removed

        async def lpush(self, key, *values):
            bucket = self._state.lists[key]
            for value in values:
                bucket.insert(0, value)
            return len(bucket)

        async def rpop(self, key):
            bucket = self._state.lists.get(key, [])
            if not bucket:
                return None
            value = bucket.pop()
            return value.encode("utf-8") if isinstance(value, str) else value

        async def publish(self, channel, message):
            for pubsub in list(self._state.pubsubs):
                if channel in pubsub._channels:
                    pubsub._queue.put_nowait({"type": "message", "channel": channel, "data": message})
            return len(self._state.pubsubs)

        async def eval(self, script, numkeys, *args):
            key = args[0]
            owner = args[1]
            current = self._state.kv.get(key)
            if current is None:
                return 0
            current_value = current.decode("utf-8") if isinstance(current, (bytes, bytearray)) else current
            if current_value != owner:
                return 0
            if "del" in script:
                self._state.kv.pop(key, None)
                return 1
            if "expire" in script:
                return 1
            return 0

    redis_module = types.ModuleType("redis")
    asyncio_module = types.ModuleType("redis.asyncio")
    asyncio_module.Redis = Redis
    redis_module.asyncio = asyncio_module
    sys.modules["redis"] = redis_module
    sys.modules["redis.asyncio"] = asyncio_module


_install_fake_redis()


@pytest.hookimpl(tryfirst=True)
def pytest_configure(config):
    config.addinivalue_line("markers", "asyncio: mark a test as running in asyncio")
