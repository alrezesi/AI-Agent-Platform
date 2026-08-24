import asyncio
import asyncpg

async def main():
    conn = await asyncpg.connect('postgresql://agent:agent123@localhost:5433/agent_platform')
    
    # Check constraints
    rows = await conn.fetch("""
        SELECT conname, contype, pg_get_constraintdef(oid)
        FROM pg_constraint
        WHERE conrelid = 'tasks'::regclass
    """)
    print('CONSTRAINTS:')
    for row in rows:
        print(f'  {row["conname"]}: type={row["contype"]}, def={row["pg_get_constraintdef"]}')
    
    # Check indexes
    rows = await conn.fetch("""
        SELECT indexname, indexdef
        FROM pg_indexes
        WHERE tablename = 'tasks'
    """)
    print('INDEXES:')
    for row in rows:
        print(f'  {row["indexname"]}: {row["indexdef"]}')
    
    await conn.close()

asyncio.run(main())
