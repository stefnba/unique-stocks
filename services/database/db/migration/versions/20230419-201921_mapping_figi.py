# pylint: disable=no-member,missing-function-docstring,invalid-name
"""figi_mapping

Revision ID: 7ec2035245e0
Revises: b9f9efc9f0d1
Create Date: 2023-04-19 20:19:21.799577

"""
from alembic import op

from db.utils.sql import execute_ddl_file

# revision identifiers, used by Alembic.
revision = "7ec2035245e0"
down_revision = "b9f9efc9f0d1"
branch_labels = None
depends_on = None


def upgrade() -> None:
    execute_ddl_file("./sql/20230419-201921_mapping_figi.sql")


def downgrade() -> None:
    op.execute(
        """
        --sql
        DROP TABLE IF EXISTS mapping_figi CASCADE;
		"""
    )
