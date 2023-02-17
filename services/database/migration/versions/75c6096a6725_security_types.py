# pylint: disable=no-member,missing-function-docstring,invalid-name
"""security_types

Revision ID: 75c6096a6725
Revises: 2e4e448d5499
Create Date: 2023-02-17 16:12:29.617690

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = "75c6096a6725"
down_revision = "b4006cf654d5"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
			CREATE TABLE IF NOT EXISTS security_types (
                id SERIAL4 PRIMARY KEY,
                type VARCHAR(255) NOT NULL UNIQUE,
                created_at timestamp without time zone default (now() at time zone 'utc'),
                updated_at timestamp without time zone
            );
		"""
    )


def downgrade() -> None:
    op.execute(
        """
			DROP TABLE IF EXISTS security_types;
		"""
    )
