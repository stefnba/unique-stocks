# pylint: disable=no-member,missing-function-docstring,invalid-name
"""data__fundamental

Revision ID: 2f82c0d4f5d2
Revises: b8f8edc702ba
Create Date: 2023-08-14 21:17:37.579870

"""
from alembic import op
from database.utils.migration_file import MigrationFile




# revision identifiers, used by Alembic.
revision = '2f82c0d4f5d2'
down_revision = 'b8f8edc702ba'
branch_labels = None
depends_on = None

migration_file = MigrationFile(revision)

def upgrade() -> None:
    op.execute(migration_file.upgrade())


def downgrade() -> None:
    op.execute(migration_file.downgrade())
