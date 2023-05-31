# pylint: disable=no-member,missing-function-docstring,invalid-name
"""mappings

Revision ID: b9f9efc9f0d1
Revises: cccf9790cfb3
Create Date: 2023-03-25 18:12:17.611997

"""
from alembic import op
from sqlalchemy import Date, Integer, String
from sqlalchemy.sql import column

from db.utils import execute_ddl_file, export_to_csv, seed_table_from_csv

# revision identifiers, used by Alembic.
revision = "b9f9efc9f0d1"
down_revision = "cccf9790cfb3"
branch_labels = None
depends_on = None


TABLE_NAME = "mappings"
CSV_FILE_PATH = "./db/seeds/mappings.csv"


def upgrade() -> None:
    execute_ddl_file("./sql/20230325-181217_mapping.sql")

    seed_table_from_csv(
        table_name=TABLE_NAME,
        file_path=CSV_FILE_PATH,
        columns=[
            column("id", Integer),
            column("source", String),
            column("product", String),
            column("field", String),
            column("uid_description", String),
            column("source_description", String),
            column("source_value", Integer),
            column("uid", Integer),
            column(
                "is_seed",
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
        ],
    )

    op.execute(
        """
    --sql
     SELECT setval('mappings_id_seq', max("id")) FROM "mappings";
    ;
    """
    )


def downgrade() -> None:
    export_to_csv(
        table=TABLE_NAME,
        destination_path=CSV_FILE_PATH,
        columns=[
            "source",
            "product",
            "field",
            "source_value",
            "source_description",
            "uid",
            "uid_description",
            "is_seed",
            "valid_from",
            "valid_until",
            "is_active",
        ],
    )

    op.execute(
        """
        --sql
        DROP TABLE IF EXISTS mappings CASCADE;
		"""
    )
