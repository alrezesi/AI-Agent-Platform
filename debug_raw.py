import asyncio
import asyncpg

async def main():
    conn = await asyncpg.connect('postgresql://agent:agent123@localhost:5433/agent_platform')
    
    # Check current state
    rows = await conn.fetch("SELECT task_id, status, lease_expires_at FROM tasks WHERE task_id = 'test-1'")
    for row in rows:
        print(f'before: {dict(row)}')
    
    # Run the UPDATE ... RETURNING
    now = await conn.fetchval("SELECT now()")
    print(f'now={now}')
    
    rows = await conn.fetch("""
        UPDATE tasks 
        SET status = 'pending', started_at = NULL, lease_owner = NULL, lease_expires_at = NULL, retry_count = retry_count + 1
        WHERE status = 'running' AND lease_expires_at IS NOT NULL AND lease_expires_at <= $1
        RETURNING task_id
    """, now.replace(tzinfo=None) if now.tzinfo else now)
    
    print(f'returned rows: {len(rows)}')
    for row in rows:
        print(f'returned: {dict(row)}')
    
    # Check after
    rows = await conn.fetch("SELECT task_id, status, lease_expires_at FROM tasks WHERE task_id = 'test-1'")
    for row in rows:
        print(f'after: {dict(row)}')
    
    await conn.close()

asyncio.run(main())
