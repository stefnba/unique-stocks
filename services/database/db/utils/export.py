import csv
from typing import Optional

from alembic import op
from sqlalchemy import text


def export_to_csv(destination_path: str, table: str, columns: Optional[list[str]] = None):
    """
    Export table records to .csv file.

    Args:
        destination_path (str): File path of .csv file.
        table (str): table name.
        columns (Optional[list[str]], optional): Specify columns to be exported to .csv file.
    """

    conn = op.get_bind()

    select_statement = "*"

    if columns:
        select_statement = ", ".join(f'"{col}"' for col in columns)

    result = conn.execute(text(f"SELECT {select_statement} FROM {table}"))

    with open(file=destination_path, mode='w', encoding="utf-8") as f:
        outcsv = csv.writer(f)
        outcsv.writerow(result.keys())
        outcsv.writerows(result)