# pylint: disable=no-member,missing-function-docstring,invalid-name
"""surrogate_keys

Revision ID: cccf9790cfb3
Revises: 
Create Date: 2023-03-25 18:11:33.846218

"""
from alembic import op
from sqlalchemy import Date, Integer, String
from sqlalchemy.sql import column

from db.utils import execute_ddl_file, export_to_csv, seed_table_from_csv

# revision identifiers, used by Alembic.
revision = "cccf9790cfb3"
down_revision = None
branch_labels = None
depends_on = None

TABLE_NAME = "surrogate_keys"
CSV_FILE_PATH = "./db/seeds/surrogate_keys.csv"


def upgrade() -> None:
    execute_ddl_file("./sql/20230325-181133_surrogate_key.sql")

    seed_table_from_csv(
        table_name=TABLE_NAME,
        file_path=CSV_FILE_PATH,
        columns=[
            column("surrogate_key", Integer),
            column("product", String),
            column(
                "uid",
                String,
            ),
            column(
                "is_active",
                String,
            ),
            column(
                "valid_from",
                Date,
            ),
            column(
                "valid_until",
                Date,
            ),
        ],
    )


def downgrade() -> None:
    export_to_csv(
        table=TABLE_NAME,
        destination_path=CSV_FILE_PATH,
        columns=[
            "surrogate_key",
            "product",
            "uid",
            "is_active",
            "valid_from",
            "valid_until",
        ],
    )
    op.execute(
        """
        --sql
        DROP TABLE IF EXISTS surrogate_keys CASCADE;
		"""
    )
