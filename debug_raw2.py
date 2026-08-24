import asyncio
import asyncpg

async def main():
    conn = await asyncpg.connect('postgresql://agent:agent123@localhost:5433/agent_platform')
    
    # Check raw values
    rows = await conn.fetch("SELECT task_id, status, lease_expires_at FROM tasks WHERE task_id = 'real-req-000'")
    for row in rows:
        print(f'task={row["task_id"]}, status={row["status"]}, lease={row["lease_expires_at"]}, type={type(row["lease_expires_at"])}')
    
    now = await conn.fetchval("SELECT now()")
    now_naive = now.replace(tzinfo=None)
    print(f'now={now}, now_naive={now_naive}')
    
    # Raw SQL query
    rows = await conn.fetch("""
        SELECT task_id FROM tasks 
        WHERE status = 'running' 
        AND lease_expires_at IS NOT NULL 
        AND lease_expires_at <= $1
    """, now_naive)
    print(f'raw sql rows: {len(rows)}')
    for row in rows:
        print(f'  matched: {row["task_id"]}')
    
    # Also try with aware now
    rows2 = await conn.fetch("""
        SELECT task_id FROM tasks 
        WHERE status = 'running' 
        AND lease_expires_at IS NOT NULL 
        AND lease_expires_at <= $1
    """, now)
    print(f'raw sql with aware rows: {len(rows2)}')
    
    await conn.close()

asyncio.run(main())
