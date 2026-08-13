CREATE TABLE IF NOT EXISTS tasks (
    task_id VARCHAR(64) PRIMARY KEY,
    agent_id VARCHAR(64) NOT NULL,
    task_type VARCHAR(64) NOT NULL,
    payload JSONB NOT NULL,
    priority INT NOT NULL,
    status VARCHAR(32) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    result JSONB,
    error TEXT,
    retry_count INT DEFAULT 0,
    max_retries INT DEFAULT 3,
    timeout_seconds INT DEFAULT 30,
    tenant_id VARCHAR(64),
    lease_owner VARCHAR(64),
    lease_expires_at TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_tasks_status_created ON tasks (status, created_at);
CREATE INDEX IF NOT EXISTS idx_tasks_agent_status ON tasks (agent_id, status);
