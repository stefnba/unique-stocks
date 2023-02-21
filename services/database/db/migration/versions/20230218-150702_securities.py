# pylint: disable=no-member,missing-function-docstring,invalid-name
"""securities

Revision ID: 912f2d1e65fc
Revises: aa2f8311a67c
Create Date: 2023-02-18 15:07:02.312809

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = "912f2d1e65fc"
down_revision = "aa2f8311a67c"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
            --sql
			CREATE TABLE IF NOT EXISTS securities (
                id SERIAL4 PRIMARY KEY,
                name VARCHAR NOT NULL,
                acronym VARCHAR,
                type_id INT4 REFERENCES securities_types(id),
                exchange_id INT4 REFERENCES exchanges(id),
                ticker VARCHAR NOT NULL,
                isin VARCHAR NOT NULL,
                is_watched BOOLEAN DEFAULT TRUE,
                parent_id INT REFERENCES securities_parents(id), 
                figi VARCHAR,
                country CHAR(2) NOT NULL,
                currency CHAR(3) NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                created_at timestamp without time zone default (now() at time zone 'utc'),
                updated_at timestamp without time zone
            );
            --sql
            CREATE UNIQUE INDEX IF NOT EXISTS ticker_unique_idx ON securities(ticker);
		"""
    )


def downgrade() -> None:
    op.execute(
        """
            --sql
			DROP TABLE IF EXISTS securities CASCADE;
		"""
    )
