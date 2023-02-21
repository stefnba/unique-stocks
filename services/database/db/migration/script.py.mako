# pylint: disable=no-member,missing-function-docstring,invalid-name
"""${message}

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
Create Date: ${create_date}

"""
from alembic import op
${"import sqlalchemy as sa" if upgrades else ""}
${imports if imports else ""}

# revision identifiers, used by Alembic.
revision = ${repr(up_revision)}
down_revision = ${repr(down_revision)}
branch_labels = ${repr(branch_labels)}
depends_on = ${repr(depends_on)}


def upgrade() -> None:
    ${upgrades if upgrades else 'op.execute(\n\t\t"""\n\t\t\t\n\t\t"""\n\t)'}


def downgrade() -> None:
    ${downgrades if downgrades else 'op.execute(\n\t\t"""\n\t\t\t\n\t\t"""\n\t)'}
