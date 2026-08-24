import asyncio
import asyncpg

async def main():
    conn = await asyncpg.connect('postgresql://agent:agent123@localhost:5433/agent_platform')
    rows = await conn.fetch("SELECT pid, query, state FROM pg_stat_activity WHERE state != 'idle'")
    for row in rows:
        print(f"pid={row['pid']}, state={row['state']}, query={row['query']}")
    await conn.close()

asyncio.run(main())
