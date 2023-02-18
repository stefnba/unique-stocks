# pylint: disable=no-member,missing-function-docstring,invalid-name
"""securities_types_seed

Revision ID: 8f5fd45e2ee2
Revises: d96a65f24c8a
Create Date: 2023-02-18 15:05:59.055269

"""
from alembic import op


# revision identifiers, used by Alembic.
revision = "8f5fd45e2ee2"
down_revision = "d96a65f24c8a"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.execute(
        """
			INSERT INTO securities_types (type) VALUES 
                ('equity'),
                ('etf'),
                ('etc'),
                ('bond'),
                ('mutualfund'),
                ('crypto'),
                ('index'),
                ('commodity'),
                ('option');
		"""
    )


def downgrade() -> None:
    op.execute(
        """
			TRUNCATE securities_types;
		"""
    )
