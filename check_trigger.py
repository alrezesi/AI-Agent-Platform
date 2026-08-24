import asyncio
import asyncpg

async def main():
    conn = await asyncpg.connect('postgresql://agent:agent123@localhost:5433/agent_platform')
    
    # Check for triggers
    rows = await conn.fetch("""
        SELECT trigger_name, event_manipulation, action_timing, action_statement
        FROM information_schema.triggers
        WHERE event_object_table = 'tasks'
    """)
    print('TRIGGERS:')
    for row in rows:
        print(f'  {dict(row)}')
    
    # Check the actual task data
    rows = await conn.fetch("SELECT task_id, status, lease_expires_at, started_at, completed_at, error FROM tasks WHERE task_id = 'real-req-000'")
    for row in rows:
        print(f'task={row["task_id"]}, status={row["status"]}, lease={row["lease_expires_at"]}, started={row["started_at"]}, completed={row["completed_at"]}, error={row["error"]}')
    
    await conn.close()

asyncio.run(main())
