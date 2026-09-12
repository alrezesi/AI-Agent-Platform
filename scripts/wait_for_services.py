import os
import time
import sys


def _normalize_db_url(url: str) -> str:
    """Convert SQLAlchemy-style postgresql+asyncpg:// to asyncpg-compatible postgresql://."""
    if url.startswith("postgresql+asyncpg://"):
        return "postgresql://" + url[len("postgresql+asyncpg://"):]
    return url


def wait_for_postgres():
    try:
        import asyncpg
        import asyncio

        # Use DATABASE_URL (the real app DB) or POSTGRES_URL (the test DB)
        # from the environment.  Falls back to a local-dev default that
        # matches tests/conftest.py::_resolve_database_url().
        db_url = os.getenv("DATABASE_URL") or "postgresql://agent:agent123@localhost:5432/agent_platform"
        db_url = _normalize_db_url(db_url)

        async def check():
            conn = await asyncpg.connect(db_url)
            await conn.close()
            return True

        for i in range(30):
            try:
                if asyncio.run(check()):
                    print(f"PostgreSQL is ready ({db_url})")
                    return True
            except Exception as e:
                print(f"Waiting for PostgreSQL... ({i+1}/30): {e}")
                time.sleep(1)
        print("PostgreSQL did not become ready in time")
        return False
    except ImportError:
        print("asyncpg not installed, skipping PostgreSQL wait")
        return True


def wait_for_redis():
    try:
        import redis as redis_pkg

        host = "localhost"
        port = int(os.getenv("REDIS_HOST_PORT", "6379"))
        db = int(os.getenv("REDIS_DB", "0"))

        for i in range(30):
            try:
                r = redis_pkg.Redis(host=host, port=port, db=db)
                r.ping()
                print(f"Redis is ready ({host}:{port}/{db})")
                return True
            except Exception as e:
                print(f"Waiting for Redis... ({i+1}/30): {e}")
                time.sleep(1)
        print("Redis did not become ready in time")
        return False
    except ImportError:
        print("redis package not installed, skipping Redis wait")
        return True

if __name__ == "__main__":
    pg_ok = wait_for_postgres()
    redis_ok = wait_for_redis()
    if pg_ok and redis_ok:
        sys.exit(0)
    else:
        sys.exit(1)
