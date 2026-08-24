"""add task trace correlation columns

Revision ID: add_trace_columns
Revises: 443727b49ab2
Create Date: 2026-08-22

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'add_trace_columns'
down_revision: Union[str, Sequence[str], None] = '443727b49ab2'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    # Check column existence to make migration idempotent
    columns = {row[3] for row in conn.exec_driver_sql(
        "SELECT 1,1,1,column_name FROM information_schema.columns WHERE table_name='tasks'"
    ).fetchall()} if conn.exec_driver_sql(
        "SELECT 1 FROM information_schema.tables WHERE table_name='tasks'"
    ).first() else set()

    if 'request_id' not in columns:
        op.add_column('tasks', sa.Column('request_id', sa.String(length=64), nullable=True))
        op.create_index('idx_tasks_request_id', 'tasks', ['request_id'], unique=False)
    if 'execution_id' not in columns:
        op.add_column('tasks', sa.Column('execution_id', sa.String(length=64), nullable=True))
        op.create_index('idx_tasks_execution_id', 'tasks', ['execution_id'], unique=False)


def downgrade() -> None:
    op.drop_index('idx_tasks_execution_id', table_name='tasks', if_exists=True)
    op.drop_index('idx_tasks_request_id', table_name='tasks', if_exists=True)
    op.drop_column('tasks', 'execution_id', if_exists=True)
    op.drop_column('tasks', 'request_id', if_exists=True)
