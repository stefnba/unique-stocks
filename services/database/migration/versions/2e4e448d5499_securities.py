# pylint: disable=no-member,missing-function-docstring,invalid-name
"""securities

Revision ID: 2e4e448d5499
Revises: b4006cf654d5
Create Date: 2023-02-17 16:05:24.657128

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = "2e4e448d5499"
down_revision = "1a132c258f78"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """ 
            CREATE TABLE IF NOT EXISTS securities (
                id SERIAL4 PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                type_id INT4 REFERENCES security_types(id),
                exchange_id INT4 REFERENCES exchanges(id),
                ticker VARCHAR(255) NOT NULL,
                isin VARCHAR(255) NOT NULL,
                country CHAR(2) NOT NULL,
                currency CHAR(3) NOT NULL,
                created_at timestamp without time zone default (now() at time zone 'utc'),
                updated_at timestamp without time zone
            );
            CREATE UNIQUE INDEX IF NOT EXISTS ticker_unique_idx ON securities(ticker);
		"""
    )


def downgrade() -> None:
    op.execute(
        """
			DROP TABLE IF EXISTS securities;
		"""
    )
