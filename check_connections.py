import asyncio
import asyncpg

async def main():
    conn = await asyncpg.connect('postgresql://agent:agent123@localhost:5433/agent_platform')
    
    rows = await conn.fetch("""
        SELECT pid, usename, application_name, client_addr, state, query_start, query
        FROM pg_stat_activity
        WHERE datname = 'agent_platform'
    """)
    print('ACTIVE CONNECTIONS:')
    for row in rows:
        print(f'  pid={row["pid"]}, user={row["usename"]}, app={row["application_name"]}, state={row["state"]}, query={row["query"][:100]}')
    
    await conn.close()

asyncio.run(main())
