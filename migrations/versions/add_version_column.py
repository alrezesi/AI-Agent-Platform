"""add version column for optimistic locking

Revision ID: add_version_column
Revises: add_trace_columns
Create Date: 2026-08-24

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'add_version_column'
down_revision: Union[str, Sequence[str], None] = 'add_trace_columns'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    conn = op.get_bind()
    columns = {row[0] for row in conn.exec_driver_sql(
        "SELECT column_name FROM information_schema.columns WHERE table_name='tasks'"
    ).fetchall()} if conn.exec_driver_sql(
        "SELECT 1 FROM information_schema.tables WHERE table_name='tasks'"
    ).first() else set()

    if 'version' not in columns:
        op.add_column('tasks', sa.Column('version', sa.Integer(), nullable=False, server_default=sa.text('0')))
        # Backfill existing rows to ensure version is 0 for all
        conn.exec_driver_sql("UPDATE tasks SET version = 0 WHERE version IS NULL")


def downgrade() -> None:
    op.drop_column('tasks', 'version', if_exists=True)
