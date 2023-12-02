# pylint: disable=no-member,missing-function-docstring,invalid-name
"""data__base

Revision ID: d313ba744d62
Revises: b47ccf851657
Create Date: 2023-06-01 12:00:59.473292

"""
from alembic import op

from database.utils import seed
from database.utils.migration_file import MigrationFile

SCHEMA_NAME = "data"

TABLE_NAME_SECURITY_TYPE = "security_type"
COLUMNS_SECURITY_TYPE = ["id", "parent_id", "name", "is_leaf", "name_figi", "name_figi2", "market_sector_figi"]


TABLE_NAME_SECURITY_QUOTE_INTERVAL = "security_quote_interval"
COLUMNS_SECURITY_QUOTE_INTERVAL = ["id", "name", "min"]


# revision identifiers, used by Alembic.
revision = "d313ba744d62"
down_revision = "b47ccf851657"
branch_labels = None
depends_on = None

migration_file = MigrationFile(revision)


def upgrade() -> None:
    op.execute(migration_file.upgrade(wrap_in_trx=True))

    seed.load_from_csv(
        table=TABLE_NAME_SECURITY_TYPE,
        schema=SCHEMA_NAME,
        columns=COLUMNS_SECURITY_TYPE,
    )
    seed.load_from_csv(
        table=TABLE_NAME_SECURITY_QUOTE_INTERVAL,
        schema=SCHEMA_NAME,
        columns=COLUMNS_SECURITY_QUOTE_INTERVAL,
    )


def downgrade() -> None:
    seed.export_to_csv(
        table=TABLE_NAME_SECURITY_TYPE,
        schema=SCHEMA_NAME,
        columns=COLUMNS_SECURITY_TYPE,
    )
    seed.export_to_csv(
        table=TABLE_NAME_SECURITY_QUOTE_INTERVAL,
        schema=SCHEMA_NAME,
        columns=COLUMNS_SECURITY_QUOTE_INTERVAL,
    )

    op.execute(migration_file.downgrade())
