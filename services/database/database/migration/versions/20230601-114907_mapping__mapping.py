# pylint: disable=no-member,missing-function-docstring,invalid-name
"""mapping_mapping

Revision ID: b47e4ad53988
Revises: cccf9790cfb3
Create Date: 2023-06-01 11:49:07.222278

"""
from alembic import op
from database.utils.migration_file import MigrationFile
from database.utils import seed

TABLE_NAME = "mapping"
SCHEMA_NAME = "mapping"
COLUMNS = [
    "source",
    "product",
    "field",
    "source_value",
    "source_description",
    "uid",
    "uid_description",
    "is_seed",
    "active_from",
    "active_until",
    "is_active",
]


# revision identifiers, used by Alembic.
revision = "b47e4ad53988"
down_revision = "cccf9790cfb3"
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
