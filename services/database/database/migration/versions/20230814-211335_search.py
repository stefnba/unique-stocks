# pylint: disable=no-member,missing-function-docstring,invalid-name
"""search

Revision ID: b8f8edc702ba
Revises: d313ba744d62
Create Date: 2023-08-14 21:13:35.889878

"""
from alembic import op

from database.utils.migration_file import MigrationFile

# revision identifiers, used by Alembic.
revision = "b8f8edc702ba"
down_revision = "d313ba744d62"
branch_labels = None
depends_on = None

migration_file = MigrationFile(revision)


def upgrade() -> None:
    op.execute(migration_file.upgrade())


def downgrade() -> None:
    op.execute(migration_file.downgrade())
