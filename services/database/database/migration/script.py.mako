# pylint: disable=no-member,missing-function-docstring,invalid-name
"""${message}

Revision ID: ${up_revision}
Revises: ${down_revision | comma,n}
Create Date: ${create_date}

"""
from alembic import op
from database.utils.migration_file import MigrationFile

${"import sqlalchemy as sa" if upgrades else ""}
${imports if imports else ""}

# revision identifiers, used by Alembic.
revision = ${repr(up_revision)}
down_revision = ${repr(down_revision)}
branch_labels = ${repr(branch_labels)}
depends_on = ${repr(depends_on)}

migration_file = MigrationFile(revision)

def upgrade() -> None:
    ${upgrades if upgrades else 'op.execute(migration_file.upgrade())'}


def downgrade() -> None:
    ${downgrades if downgrades else 'op.execute(migration_file.downgrade())'}
