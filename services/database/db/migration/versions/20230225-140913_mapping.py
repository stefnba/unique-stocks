# pylint: disable=no-member,missing-function-docstring,invalid-name
"""mapping

Revision ID: 7acf219a25a8
Revises: 912f2d1e65fc
Create Date: 2023-02-25 14:09:13.333169

"""
from alembic import op
from sqlalchemy import Date, Integer, String
from sqlalchemy.sql import column

from db.utils.seed import seed_table_from_csv

# revision identifiers, used by Alembic.
revision = "7acf219a25a8"
down_revision = "912f2d1e65fc"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        --sql
        CREATE TABLE IF NOT EXISTS mapping (
            id SERIAL4 PRIMARY KEY,
            source VARCHAR NOT NULL,
            product VARCHAR NOT NULL,
            field VARCHAR NOT NULL,
            source_value VARCHAR NOT NULL,
            app_value VARCHAR NOT NULL,
            is_seed BOOLEAN,
            created_at timestamp without time zone default (now() at time zone 'utc') NOT NULL,
            updated_at timestamp without time zone,
            valid_from timestamp without time zone,
            valid_until timestamp without time zone,
            is_current BOOLEAN DEFAULT FALSE
            );
        --sql
        CREATE INDEX IF NOT EXISTS source_idx ON mapping(source);
        --sql
        CREATE INDEX IF NOT EXISTS field_idx ON mapping(field);
        --sql
        CREATE INDEX IF NOT EXISTS product_idx ON mapping(product);
        --sql
        CREATE INDEX IF NOT EXISTS source_value_idx ON mapping(source_value);
        --sql
        CREATE INDEX IF NOT EXISTS app_value_idx ON mapping(app_value);
        """
    )
    seed_table_from_csv(
        table_name="mapping",
        file_path="./db/seeds/mapping.csv",
        columns=[
            column("id", Integer),
            column("source", String),
            column("product", String),
            column("field", String),
            column("source_value", Integer),
            column("app_value", Integer),
            column(
                "is_seed",
                String,
            ),
            column(
                "is_current",
                String,
            ),
            column(
                "valid_from",
                Date,
            ),
        ],
    )


def downgrade() -> None:
    op.execute(
        """
        --sql
        DROP TABLE IF EXISTS mapping CASCADE;
        """
    )
