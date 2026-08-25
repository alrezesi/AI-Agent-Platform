"""add queue message id, error category and retry history columns

Revision ID: add_retry_trace_columns
Revises: add_version_column
Create Date: 2026-08-25

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'add_retry_trace_columns'
down_revision: Union[str, Sequence[str], None] = 'add_version_column'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def _existing_columns(conn) -> set[str]:
    if conn.exec_driver_sql(
        "SELECT 1 FROM information_schema.tables WHERE table_name='tasks'"
    ).first():
        return {
            row[3]
            for row in conn.exec_driver_sql(
                "SELECT 1,1,1,column_name FROM information_schema.columns WHERE table_name='tasks'"
            ).fetchall()
        }
    return set()


def upgrade() -> None:
    conn = op.get_bind()
    columns = _existing_columns(conn)

    if 'message_id' not in columns:
        op.add_column('tasks', sa.Column('message_id', sa.String(length=64), nullable=True))
        op.create_index('idx_tasks_message_id', 'tasks', ['message_id'], unique=False)
    if 'error_category' not in columns:
        op.add_column('tasks', sa.Column('error_category', sa.String(length=64), nullable=True))
    if 'retry_history' not in columns:
        op.add_column('tasks', sa.Column('retry_history', sa.JSON(), nullable=True))


def downgrade() -> None:
    conn = op.get_bind()
    columns = _existing_columns(conn)

    if 'retry_history' in columns:
        op.drop_column('tasks', 'retry_history', if_exists=True)
    if 'error_category' in columns:
        op.drop_column('tasks', 'error_category', if_exists=True)
    if 'message_id' in columns:
        op.drop_index('idx_tasks_message_id', table_name='tasks', if_exists=True)
        op.drop_column('tasks', 'message_id', if_exists=True)
