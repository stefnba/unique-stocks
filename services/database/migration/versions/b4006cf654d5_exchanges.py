# pylint: disable=no-member,missing-function-docstring,invalid-name
"""exchanges

Revision ID: b4006cf654d5
Revises: e875e6dc0755
Create Date: 2023-02-17 15:52:20.168161

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = "b4006cf654d5"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
			CREATE TABLE IF NOT EXISTS exchanges (
                id SERIAL4 PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                code VARCHAR(255) NOT NULL,
                country CHAR(2) NOT NULL,
                currency CHAR(3) NOT NULL,
                timezone VARCHAR(255) NOT NULL,
                created_at timestamp without time zone default (now() at time zone 'utc'),
                updated_at timestamp without time zone
            );
            CREATE UNIQUE INDEX IF NOT EXISTS code_unique_idx ON exchanges(code);
		"""
    )


def downgrade() -> None:
    op.execute(
        """
			DROP TABLE IF EXISTS exchanges;
		"""
    )
