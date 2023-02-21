# pylint: disable=no-member,missing-function-docstring,invalid-name
"""exchanges

Revision ID: 4bc5d460d52c
Revises: e17780b5b867
Create Date: 2023-02-18 15:04:00.313749

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = "4bc5d460d52c"
down_revision = 'd919d2929391'
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
            --sql
			CREATE TABLE IF NOT EXISTS exchanges (
                id SERIAL4 PRIMARY KEY,
                name VARCHAR NOT NULL,
                acronym VARCHAR,
                code VARCHAR NOT NULL,
                mic VARCHAR NOT NULL,
                website VARCHAR,
                city VARCHAR NOT NULL,
                country CHAR(2) REFERENCES countries(id) NOT NULL,
                currency CHAR(3) REFERENCES currencies(id) NOT NULL,
                timezone INT REFERENCES timezones(id) NOT NULL,
                is_active BOOLEAN DEFAULT TRUE,
                created_at timestamp without time zone default (now() at time zone 'utc'),
                updated_at timestamp without time zone,
                enriched_at timestamp without time zone,
                enrichment_source VARCHAR
            );
            --sql
            CREATE UNIQUE INDEX IF NOT EXISTS code_unique_idx ON exchanges(code);
            --sql
            CREATE UNIQUE INDEX IF NOT EXISTS mic_unique_idx ON exchanges(mic);
		"""
    )


def downgrade() -> None:
    op.execute(
        """
            --sql
			DROP TABLE IF EXISTS exchanges CASCADE;
		"""
    )
