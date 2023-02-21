# pylint: disable=no-member,missing-function-docstring,invalid-name
"""securities_parents

Revision ID: aa2f8311a67c
Revises: 8f5fd45e2ee2
Create Date: 2023-02-18 15:06:35.806058

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = "aa2f8311a67c"
down_revision = "8f5fd45e2ee2"
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
