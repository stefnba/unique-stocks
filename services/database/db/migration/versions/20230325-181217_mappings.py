# pylint: disable=no-member,missing-function-docstring,invalid-name
"""mappings

Revision ID: b9f9efc9f0d1
Revises: cccf9790cfb3
Create Date: 2023-03-25 18:12:17.611997

"""
from alembic import op
from sqlalchemy import Date, Integer, String
from sqlalchemy.sql import column

from db.utils.seed import seed_table_from_csv

# revision identifiers, used by Alembic.
revision = "b9f9efc9f0d1"
down_revision = "cccf9790cfb3"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        --sql
        CREATE TABLE IF NOT EXISTS mappings (
            id SERIAL4 PRIMARY KEY,
            source VARCHAR NOT NULL,
            product VARCHAR NOT NULL,
            field VARCHAR,
            source_value VARCHAR NOT NULL,
            uid VARCHAR NOT NULL,
            is_seed BOOLEAN DEFAULT FALSE,
            created_at timestamp without time zone default (now() at time zone 'utc') NOT NULL,
            updated_at timestamp without time zone,
            valid_from timestamp without time zone,
            valid_until timestamp without time zone,
            is_active BOOLEAN DEFAULT TRUE,
            UNIQUE (product, field, uid)
            );
        --sql
        CREATE INDEX IF NOT EXISTS source_idx ON mappings(source);
        --sql
        CREATE INDEX IF NOT EXISTS field_idx ON mappings(field);
        --sql
        CREATE INDEX IF NOT EXISTS product_idx ON mappings(product);
        --sql
        CREATE INDEX IF NOT EXISTS source_value_idx ON mappings(source_value);
        --sql
        CREATE INDEX IF NOT EXISTS app_value_idx ON mappings(uid);
		"""
    )
    seed_table_from_csv(
        table_name="mappings",
        file_path="./db/seeds/mappings.csv",
        columns=[
            column("id", Integer),
            column("source", String),
            column("product", String),
            column("field", String),
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


def downgrade() -> None:
    op.execute(
        """
        --sql
        DROP TABLE IF EXISTS mappings CASCADE;
		"""
    )
