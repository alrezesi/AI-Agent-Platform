import time
import sys

def wait_for_postgres():
    try:
        import asyncpg
        import asyncio
        
        async def check():
            conn = await asyncpg.connect('postgresql://test:test@localhost:5432/agent_platform_test')
            await conn.close()
            return True
        
        for i in range(30):
            try:
                if asyncio.run(check()):
                    print('PostgreSQL is ready')
                    return True
            except Exception as e:
                print(f'Waiting for PostgreSQL... ({i+1}/30): {e}')
                time.sleep(1)
        print('PostgreSQL did not become ready in time')
        return False
    except ImportError:
        print("asyncpg not installed, skipping PostgreSQL wait")
        return True

def wait_for_redis():
    try:
        import redis
        
        for i in range(30):
            try:
                r = redis.Redis(host='localhost', port=6379, db=0)
                r.ping()
                print('Redis is ready')
                return True
            except Exception as e:
                print(f'Waiting for Redis... ({i+1}/30): {e}')
                time.sleep(1)
        print('Redis did not become ready in time')
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
