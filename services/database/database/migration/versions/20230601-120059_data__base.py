# pylint: disable=no-member,missing-function-docstring,invalid-name
"""data__base

Revision ID: d313ba744d62
Revises: b47ccf851657
Create Date: 2023-06-01 12:00:59.473292

"""
from alembic import op
from database.utils.migration_file import MigrationFile




# revision identifiers, used by Alembic.
revision = 'd313ba744d62'
down_revision = 'b47ccf851657'
branch_labels = None
depends_on = None

migration_file = MigrationFile(revision)

def upgrade() -> None:
    op.execute(migration_file.upgrade())


def downgrade() -> None:
    op.execute(migration_file.downgrade())
