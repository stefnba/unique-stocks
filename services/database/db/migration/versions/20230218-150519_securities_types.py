# pylint: disable=no-member,missing-function-docstring,invalid-name
"""securities_types

Revision ID: d96a65f24c8a
Revises: 4bc5d460d52c
Create Date: 2023-02-18 15:05:19.593273

"""
from alembic import op
from sqlalchemy import Integer, String
from sqlalchemy.sql import column

from db.utils.seed import seed_table_from_csv

# revision identifiers, used by Alembic.
revision = "d96a65f24c8a"
down_revision = "4bc5d460d52c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
            --sql
			CREATE TABLE IF NOT EXISTS securities_types (
                id SERIAL4 PRIMARY KEY,
                type VARCHAR NOT NULL UNIQUE,
                created_at timestamp without time zone default (now() at time zone 'utc'),
                updated_at timestamp without time zone,
                is_active BOOLEAN DEFAULT TRUE
            );
		"""
    )
    seed_table_from_csv(
        table_name="securities_types",
        file_path="./db/seeds/securities_types.csv",
        columns=[column("type", String), column("id", Integer)],
    )


def downgrade() -> None:
    op.execute(
        """
            --sql
			DROP TABLE IF EXISTS securities_types CASCADE;
		"""
    )
