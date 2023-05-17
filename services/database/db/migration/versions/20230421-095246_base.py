# pylint: disable=no-member,missing-function-docstring,invalid-name
"""base

Revision ID: 0f131bd4f92b
Revises: 7ec2035245e0
Create Date: 2023-04-21 09:52:46.577236

"""
from alembic import op
from sqlalchemy import String
from sqlalchemy.sql import column

from db.utils import execute_ddl_file, export_to_csv, seed_table_from_csv

# revision identifiers, used by Alembic.
revision = "0f131bd4f92b"
down_revision = "7ec2035245e0"
branch_labels = None
depends_on = None


def upgrade() -> None:
    execute_ddl_file("./sql/20230421-095246_base.sql")

    seed_table_from_csv(
        table_name="security_type",
        file_path="./db/seeds/security_type.csv",
        columns=[
            column("id", String),
            column("parent_id", String),
            column("name", String),
            column("is_leaf", String),
            column("name_figi", String),
            column("name_figi2", String),
            column("market_sector_figi", String),
        ],
    )


def downgrade() -> None:
    export_to_csv(
        destination_path="./db/seeds/security_type.csv",
        table="security_type",
        columns=["id", "parent_id", "name", "is_leaf", "name_figi", "name_figi2", "market_sector_figi"],
    )

    op.execute(
        """
		--sql
        DROP TABLE IF EXISTS country CASCADE;
		--sql
        DROP TABLE IF EXISTS exchange CASCADE;
		--sql
        DROP TABLE IF EXISTS security_type CASCADE;
		--sql
        DROP TABLE IF EXISTS security CASCADE;
		--sql
        DROP TABLE IF EXISTS index CASCADE;
		--sql
        DROP TABLE IF EXISTS security_listing CASCADE;
		--sql
        DROP TABLE IF EXISTS security_ticker CASCADE;
		"""
    )
