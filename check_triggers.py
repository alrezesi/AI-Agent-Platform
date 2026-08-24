import asyncio
import asyncpg

async def main():
    conn = await asyncpg.connect('postgresql://agent:agent123@localhost:5433/agent_platform')
    
    # Check all triggers
    rows = await conn.fetch("""
        SELECT * FROM information_schema.triggers
        WHERE event_object_schema = 'public'
    """)
    print('ALL TRIGGERS:')
    for row in rows:
        print(f'  {dict(row)}')
    
    # Check rules
    rows = await conn.fetch("""
        SELECT * FROM pg_rules
        WHERE schemaname = 'public'
    """)
    print('RULES:')
    for row in rows:
        print(f'  {dict(row)}')
    
    # Check events
    rows = await conn.fetch("""
        SELECT * FROM pg_event_trigger
    """)
    print('EVENT TRIGGERS:')
    for row in rows:
        print(f'  {dict(row)}')
    
    await conn.close()

asyncio.run(main())
