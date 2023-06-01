# pylint: disable=no-member,missing-function-docstring,invalid-name
"""mapping__figi

Revision ID: b47ccf851657
Revises: b47e4ad53988
Create Date: 2023-06-01 11:55:54.168575

"""
from alembic import op
from database.utils.migration_file import MigrationFile
from database.utils import seed

TABLE_NAME = "figi"
SCHEMA_NAME = "mapping"
COLUMNS = [
    "isin",
    "wkn",
    "ticker",
    "ticker_figi",
    "name_figi",
    "exchange_mic",
    "currency",
    "country",
    "figi",
    "share_class_figi",
    "composite_figi",
    "security_type_id",
    "valid_from",
    "valid_until",
    "is_active",
]


# revision identifiers, used by Alembic.
revision = "b47ccf851657"
down_revision = "b47e4ad53988"
branch_labels = None
depends_on = None

migration_file = MigrationFile(revision)


def upgrade() -> None:
    op.execute(migration_file.upgrade(wrap_in_trx=True))
    seed.load_from_csv(
        table=TABLE_NAME,
        schema=SCHEMA_NAME,
        columns=COLUMNS,
    )


def downgrade() -> None:
    seed.export_to_csv(
        table=TABLE_NAME,
        schema=SCHEMA_NAME,
        columns=COLUMNS,
    )
    op.execute(migration_file.downgrade())
