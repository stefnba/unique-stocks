# pylint: disable=E1101
import csv

from alembic import op
from sqlalchemy.sql import table
from sqlalchemy.sql.elements import ColumnClause


def seed_table_from_csv(file_path: str, table_name: str, columns: list[ColumnClause]):
    """
    Populates a table from a .csv file. It's a workaround since PostgreSQL does not
    allow COPY command from a non-superuser.

    Args:
        file_path (str): path the .csv file
        table_name (str): name of table to be populated
        *columns (ColumnClause): ColumnClause as provided by sqlalchemy
    """
    db_rows = []
    with open(file_path, "r", encoding="utf-8") as file:
        csv_reader = csv.DictReader(file)

        for file_rows in csv_reader:
            db_rows.append(file_rows)

        op.bulk_insert(
            table(table_name, *columns),
            db_rows,
        )
