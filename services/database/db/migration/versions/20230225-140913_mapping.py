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
            asset VARCHAR NOT NULL,
            original_value VARCHAR NOT NULL,
            translation_value VARCHAR NOT NULL,
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
        CREATE INDEX IF NOT EXISTS asset_idx ON mapping(asset);
        --sql
        CREATE INDEX IF NOT EXISTS original_value_idx ON mapping(original_value);
        --sql
        CREATE INDEX IF NOT EXISTS translation_value_idx ON mapping(translation_value);
        """
    )
    seed_table_from_csv(
        table_name="mapping",
        file_path="./db/seeds/mapping.csv",
        columns=[
            column("id", Integer),
            column("source", String),
            column("asset", Integer),
            column("original_value", Integer),
            column("translation_value", Integer),
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
