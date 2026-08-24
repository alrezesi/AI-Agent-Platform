import asyncio
import asyncpg

async def main():
    conn = await asyncpg.connect('postgresql://agent:agent123@localhost:5433/agent_platform')
    
    # Check exact raw values
    rows = await conn.fetch("SELECT task_id, status, lease_expires_at, EXTRACT(EPOCH FROM lease_expires_at) as lease_epoch FROM tasks WHERE task_id = 'real-req-000'")
    for row in rows:
        print(f'task={row["task_id"]}, status={row["status"]}, lease={row["lease_expires_at"]}, epoch={row["lease_epoch"]}')
    
    now_dt = await conn.fetchval("SELECT now()")
    now_epoch = await conn.fetchval("SELECT EXTRACT(EPOCH FROM now())")
    print(f'now={now_dt}, epoch={now_epoch}')
    
    now_naive = now_dt.replace(tzinfo=None)
    print(f'now_naive={now_naive}')
    
    # Try direct comparison with literal
    rows = await conn.fetch(f"""
        SELECT task_id, lease_expires_at, lease_expires_at <= '{now_naive}'::timestamp as is_expired
        FROM tasks 
        WHERE task_id = 'real-req-000'
    """)
    for row in rows:
        print(f'task={row["task_id"]}, lease={row["lease_expires_at"]}, is_expired={row["is_expired"]}')
    
    # Try full reclaim query with literal
    rows = await conn.fetch(f"""
        SELECT task_id FROM tasks 
        WHERE status = 'running' 
        AND lease_expires_at IS NOT NULL 
        AND lease_expires_at <= '{now_naive}'::timestamp
    """)
    print(f'full reclaim rows: {len(rows)}')
    for row in rows:
        print(f'  matched: {row["task_id"]}')
    
    await conn.close()

asyncio.run(main())
