# pylint: disable=no-member,missing-function-docstring,invalid-name
"""setup

Revision ID: d919d2929391
Revises: 912f2d1e65fc
Create Date: 2023-02-17 17:58:05.060735

"""
from alembic import op



# revision identifiers, used by Alembic.
revision = 'd919d2929391'
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
		"""
            --sql
			CREATE TABLE IF NOT EXISTS timezones (
                id SERIAL4 PRIMARY KEY,
                timezone VARCHAR NOT NULL,
                created_at timestamp without time zone default (now() at time zone 'utc'),
                updated_at timestamp without time zone
            );
            --sql
			CREATE TABLE IF NOT EXISTS currencies (
                id CHAR(3) PRIMARY KEY, -- iso code
                currency_name VARCHAR NOT NULL,
                created_at timestamp without time zone default (now() at time zone 'utc'),
                updated_at timestamp without time zone
            );
            --sql
			CREATE TABLE IF NOT EXISTS countries (
                id CHAR(2) PRIMARY KEY, -- iso code
                country VARCHAR NOT NULL,
                region VARCHAR NOT NULL,
                currency CHAR(2) REFERENCES currencies(id) NOT NULL,
                timezone INT REFERENCES timezones(id) NOT NULL,
                created_at timestamp without time zone default (now() at time zone 'utc'),
                updated_at timestamp without time zone
            );
		"""
	)


def downgrade() -> None:
    op.execute(
		"""
            --sql
			DROP TABLE IF EXISTS timezones CASCADE;
            --sql
			DROP TABLE IF EXISTS currencies CASCADE;
            --sql
			DROP TABLE IF EXISTS countries CASCADE;
		"""
	)
