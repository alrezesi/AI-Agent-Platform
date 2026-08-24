import asyncio
import asyncpg

async def main():
    conn = await asyncpg.connect('postgresql://agent:agent123@localhost:5433/agent_platform')
    rows = await conn.fetch("SELECT pid, query, state, wait_event_type, wait_event FROM pg_stat_activity WHERE datname = 'agent_platform'")
    for row in rows:
        print(f"pid={row['pid']}, state={row['state']}, wait={row['wait_event_type']}:{row['wait_event']}, query={row['query'][:150]}")
    await conn.close()

asyncio.run(main())
