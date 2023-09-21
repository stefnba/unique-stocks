# pylint: disable=no-member,missing-function-docstring,invalid-name
"""surrogate_keys

Revision ID: cccf9790cfb3
Revises: 
Create Date: 2023-03-25 18:11:33.846218

"""
from alembic import op


from database.utils.migration_file import MigrationFile
from database.utils import seed

# revision identifiers, used by Alembic.
revision = "cccf9790cfb3"
down_revision = None
branch_labels = None
depends_on = None

TABLE_NAME = "surrogate_key"
SCHEMA_NAME = "mapping"
COLUMNS = ["surrogate_key", "product", "uid", "is_active", "active_from", "active_until"]


migration_file = MigrationFile(revision)


def upgrade() -> None:
    op.execute(migration_file.upgrade(wrap_in_trx=True))

    seed.load_from_csv(
        table=TABLE_NAME,
        schema=SCHEMA_NAME,
        columns=COLUMNS,
    )

    op.execute(
        """
        --sql
        SELECT setval('mapping.surrogate_key_surrogate_key_seq', max(surrogate_key)) FROM "mapping"."surrogate_key"; 
        ;
        """
    )


def downgrade() -> None:
    seed.export_to_csv(
        table=TABLE_NAME,
        schema=SCHEMA_NAME,
        columns=COLUMNS,
    )
    op.execute(migration_file.downgrade())
