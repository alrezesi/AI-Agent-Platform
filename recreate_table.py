import asyncio
import asyncpg

async def main():
    conn = await asyncpg.connect('postgresql://agent:agent123@localhost:5433/agent_platform')
    
    # Drop and recreate the tasks table
    await conn.execute("DROP TABLE IF EXISTS tasks CASCADE")
    
    # Create the table manually with the same schema
    await conn.execute("""
        CREATE TABLE tasks (
            task_id VARCHAR(64) PRIMARY KEY,
            agent_id VARCHAR(64) NOT NULL,
            task_type VARCHAR(64) NOT NULL,
            payload JSONB NOT NULL,
            priority INTEGER NOT NULL,
            status VARCHAR(32) NOT NULL,
            created_at TIMESTAMP NOT NULL,
            started_at TIMESTAMP,
            completed_at TIMESTAMP,
            result JSONB,
            error VARCHAR(255),
            retry_count INTEGER NOT NULL DEFAULT 0,
            max_retries INTEGER NOT NULL DEFAULT 3,
            timeout_seconds INTEGER NOT NULL DEFAULT 30,
            tenant_id VARCHAR(64),
            lease_owner VARCHAR(64),
            lease_expires_at TIMESTAMP,
            request_id VARCHAR(64),
            execution_id VARCHAR(64)
        )
    """)
    
    print("Table recreated successfully")
    await conn.close()

asyncio.run(main())
