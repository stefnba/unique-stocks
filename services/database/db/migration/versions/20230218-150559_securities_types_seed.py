# pylint: disable=no-member,missing-function-docstring,invalid-name
"""securities_types_seed

Revision ID: 8f5fd45e2ee2
Revises: d96a65f24c8a
Create Date: 2023-02-18 15:05:59.055269

"""
from alembic import op
from sqlalchemy.sql import column
from sqlalchemy import String, Integer

from db.utils.seed import seed_table_from_csv


# revision identifiers, used by Alembic.
revision = "8f5fd45e2ee2"
down_revision = "d96a65f24c8a"
branch_labels = None
depends_on = None


def upgrade() -> None:
    seed_table_from_csv(
        table_name="securities_types",
        file_path="./db/seeds/securities_types.csv",
        columns=[column("type", String), column("id", Integer)],
    )


def downgrade() -> None:
    op.execute(
        """
            --sql
			TRUNCATE securities_types;
		"""
    )
