# pylint: disable=no-member,missing-function-docstring,invalid-name
"""securities_parents

Revision ID: aa2f8311a67c
Revises: d96a65f24c8a
Create Date: 2023-02-18 15:06:35.806058

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "aa2f8311a67c"
down_revision = "d96a65f24c8a"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        --sql
        CREATE TABLE IF NOT EXISTS securities_parents (
            id SERIAL4 PRIMARY KEY,
            -- type
            name VARCHAR NOT NULL,
            acronym VARCHAR,
            country CHAR(2) REFERENCES countries(id) NOT NULL,
            created_at timestamp without time zone default (now() at time zone 'utc'),
            updated_at timestamp without time zone,
            is_active BOOLEAN DEFAULT TRUE
        );
        """
    )


def downgrade() -> None:
    op.execute(
        """
        --sql
        DROP TABLE IF EXISTS securities_parents CASCADE;
        """
    )
