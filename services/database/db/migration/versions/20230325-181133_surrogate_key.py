# pylint: disable=no-member,missing-function-docstring,invalid-name
"""surrogate_keys

Revision ID: cccf9790cfb3
Revises: 
Create Date: 2023-03-25 18:11:33.846218

"""
from alembic import op

from db.utils import execute_ddl_file

# revision identifiers, used by Alembic.
revision = "cccf9790cfb3"
down_revision = None
branch_labels = None
depends_on = None


def upgrade() -> None:
    execute_ddl_file("./sql/20230325-181133_surrogate_key.sql")


def downgrade() -> None:
    op.execute(
        """
        --sql
        DROP TABLE IF EXISTS surrogate_keys CASCADE;
		"""
    )
