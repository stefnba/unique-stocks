# pylint: disable=no-member,missing-function-docstring,invalid-name
"""figi_mapping

Revision ID: 7ec2035245e0
Revises: b9f9efc9f0d1
Create Date: 2023-04-19 20:19:21.799577

"""
from alembic import op
from sqlalchemy import Date, Integer, String
from sqlalchemy.sql import column

from db.utils import execute_ddl_file, export_to_csv, seed_table_from_csv

# revision identifiers, used by Alembic.
revision = "7ec2035245e0"
down_revision = "b9f9efc9f0d1"
branch_labels = None
depends_on = None

TABLE_NAME = "mapping_figi"
CSV_FILE_PATH = "./db/seeds/mapping_figi.csv"


def upgrade() -> None:
    execute_ddl_file("./sql/20230419-201921_mapping_figi.sql")

    seed_table_from_csv(
        table_name=TABLE_NAME,
        file_path=CSV_FILE_PATH,
        columns=[
            column("isin", String),
            column("wkn", String),
            column("ticker", String),
            column("ticker_figi", String),
            column("name_figi", String),
            column("exchange_mic", String),
            column("currency", String),
            column("country", String),
            column("figi", String),
            column("share_class_figi", String),
            column("composite_figi", String),
            column("security_type_id", Integer),
            column(
                "valid_until",
                Date,
            ),
            column(
                "valid_from",
                Date,
            ),
            column(
                "is_active",
                String,
            ),
        ],
    )


def downgrade() -> None:
    export_to_csv(
        table=TABLE_NAME,
        destination_path=CSV_FILE_PATH,
        columns=[
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
        ],
    )
    op.execute(
        """
        --sql
        DROP TABLE IF EXISTS mapping_figi CASCADE;
		"""
    )
