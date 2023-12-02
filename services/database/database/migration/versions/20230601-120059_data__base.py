# pylint: disable=no-member,missing-function-docstring,invalid-name
"""data__base

Revision ID: d313ba744d62
Revises: b47ccf851657
Create Date: 2023-06-01 12:00:59.473292

"""
from alembic import op

from database.utils.migration_file import MigrationFile

SCHEMA_NAME = "data"

TABLE_NAME_SECURITY_TYPE = "security_type"
COLUMNS_SECURITY_TYPE = ["id", "parent_id", "name", "is_leaf", "name_figi", "name_figi2", "market_sector_figi"]


TABLE_NAME_SECURITY_QUOTE_INTERVAL = "security_quote_interval"
COLUMNS_SECURITY_QUOTE_INTERVAL = ["id", "name", "min"]


# revision identifiers, used by Alembic.
revision = "d313ba744d62"
down_revision = None
branch_labels = None
depends_on = None

migration_file = MigrationFile(revision)


def upgrade() -> None:
    op.execute(migration_file.upgrade(wrap_in_trx=True))


def downgrade() -> None:
    op.execute(migration_file.downgrade())
