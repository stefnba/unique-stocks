# pylint: disable=no-member,missing-function-docstring,invalid-name
"""surrogate_keys

Revision ID: cccf9790cfb3
Revises: 
Create Date: 2023-03-25 18:11:33.846218

"""
from alembic import op

# revision identifiers, used by Alembic.
revision = "cccf9790cfb3"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
        --sql
        CREATE TABLE IF NOT EXISTS surrogate_keys (
            surrogate_key SERIAL4 PRIMARY KEY,
            product VARCHAR NOT NULL,
            uid VARCHAR NOT NULL,
            created_at timestamp without time zone default (now() at time zone 'utc'),
            updated_at timestamp without time zone,
            valid_until timestamp without time zone,
            valid_from timestamp without time zone,
            is_active BOOLEAN DEFAULT TRUE
        );
        --sql
        CREATE INDEX IF NOT EXISTS source_idx ON surrogate_keys(product);
        --sql
        CREATE INDEX IF NOT EXISTS source_idx ON surrogate_keys(uid);
        --sql
        CREATE INDEX IF NOT EXISTS source_idx ON surrogate_keys(is_active);
		"""
    )


def downgrade() -> None:
    op.execute(
        """
        --sql
        DROP TABLE IF EXISTS surrogate_keys CASCADE;
		"""
    )
