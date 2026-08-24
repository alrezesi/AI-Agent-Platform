import asyncio
import asyncpg

async def main():
    conn = await asyncpg.connect('postgresql://agent:agent123@localhost:5433/agent_platform')
    
    # Check for background workers
    rows = await conn.fetch("""
        SELECT pid, datname, usename, application_name, state, backend_start, query_start, wait_event_type, query
        FROM pg_stat_activity
        WHERE datname = 'agent_platform'
        ORDER BY backend_start
    """)
    print('ALL CONNECTIONS:')
    for row in rows:
        print(f'  pid={row["pid"]}, state={row["state"]}, wait={row["wait_event_type"]}:{row["wait_event"]}, query={row["query"][:200]}')
    
    # Check locks
    rows = await conn.fetch("""
        SELECT locktype, relation::regclass, mode, pid, granted
        FROM pg_locks
        WHERE relation = 'tasks'::regclass
    """)
    print('LOCKS on tasks:')
    for row in rows:
        print(f'  {dict(row)}')
    
    await conn.close()

asyncio.run(main())
