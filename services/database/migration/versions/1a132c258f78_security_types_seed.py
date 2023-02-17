# pylint: disable=no-member,missing-function-docstring,invalid-name
"""security_types_seed

Revision ID: 1a132c258f78
Revises: 2e4e448d5499
Create Date: 2023-02-17 16:15:31.127751

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = "1a132c258f78"
down_revision = "75c6096a6725"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
			INSERT INTO security_types (type) VALUES 
                ('stock'),
                ('etf'),
                ('crypto'),
                ('index'),
                ('commodity'),
                ('option');
		"""
    )


def downgrade() -> None:
    op.execute(
        """
			TRUNCATE security_types;
		"""
    )
