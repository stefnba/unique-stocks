# pylint: disable=no-member,missing-function-docstring,invalid-name
"""exchanges

Revision ID: 4bc5d460d52c
Revises: e17780b5b867
Create Date: 2023-02-18 15:04:00.313749

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = "4bc5d460d52c"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
			CREATE TABLE IF NOT EXISTS exchanges (
                id SERIAL4 PRIMARY KEY,
                name VARCHAR(255) NOT NULL,
                acronym VARCHAR(255),
                code VARCHAR(255) NOT NULL,
                mic VARCHAR(255) NOT NULL,
                website VARCHAR(255),
                city VARCHAR(255) NOT NULL,
                country CHAR(2) NOT NULL,
                currency CHAR(3) NOT NULL,
                timezone VARCHAR(255),
                created_at timestamp without time zone default (now() at time zone 'utc'),
                updated_at timestamp without time zone,
                enriched_at timestamp without time zone,
                enrichment_source VARCHAR(255)
            );
            CREATE UNIQUE INDEX IF NOT EXISTS code_unique_idx ON exchanges(code);
            CREATE UNIQUE INDEX IF NOT EXISTS mic_unique_idx ON exchanges(mic);
		"""
    )


def downgrade() -> None:
    op.execute(
        """
			DROP TABLE IF EXISTS exchanges;
		"""
    )
