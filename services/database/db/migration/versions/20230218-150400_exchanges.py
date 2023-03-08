# pylint: disable=no-member,missing-function-docstring,invalid-name
"""exchanges

Revision ID: 4bc5d460d52c
Revises: e17780b5b867
Create Date: 2023-02-18 15:04:00.313749

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "4bc5d460d52c"
down_revision = "d919d2929391"
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
                app_id VARCHAR NOT NULL,
                source_code VARCHAR,
                mic VARCHAR,
                website VARCHAR,
                city VARCHAR,
                country CHAR(2) REFERENCES countries(id),
                currency CHAR(3) REFERENCES currencies(id),
                timezone INT REFERENCES timezones(id),
                valid_from timestamp without time zone,
                valid_until timestamp without time zone,
                is_current BOOLEAN DEFAULT FALSE,
                created_at timestamp without time zone default (now() at time zone 'utc'),
                updated_at timestamp without time zone,
                --enriched_at timestamp without time zone,
                --enrichment_source VARCHAR,
                is_virtual BOOLEAN DEFAULT FALSE,
                source VARCHAR NOT NULL
            );
            --sql
            CREATE UNIQUE INDEX IF NOT EXISTS source_code_unique_idx ON exchanges(source_code);
            --sql
            CREATE UNIQUE INDEX IF NOT EXISTS app_id_unique_idx ON exchanges(app_id);
            --sql
            CREATE UNIQUE INDEX IF NOT EXISTS mic_unique_idx ON exchanges(mic);
            --sql
            COMMENT ON COLUMN exchanges.mic is 'Official MIC of exchange';
            --sql
            COMMENT ON COLUMN exchanges.app_id is 'Unique identifiert used by unique-stocks to identify exchanges';
            --sql
            COMMENT ON COLUMN exchanges.source_code is 'Identifiert used by an API that different from official MIC';
            --sql
            COMMENT ON COLUMN exchanges.is_virtual is 'Some API provide additional "virtual" exchanges as a bucket for funds, cryptos, etc.';
		"""
    )


def downgrade() -> None:
    op.execute(
        """
            --sql
			DROP TABLE IF EXISTS exchanges CASCADE;
		"""
    )
