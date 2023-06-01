from typing import Optional, Literal

from psycopg import connect, sql
from database.utils.connection import connection_model

from database.utils.shared import get_seed_file_path

Method = Literal["SEED", "EXPORT"]


def _build_query(method: Method, table: str, schema: Optional[str] = None, columns: Optional[list[str]] = None):
    table_identifier = sql.Identifier(table)
    if schema:
        table_identifier = sql.Identifier(schema, table)

    return sql.Composed(
        [
            sql.SQL("COPY {table}").format(table=table_identifier),
            sql.Composed(
                [
                    sql.SQL(" ("),
                    sql.SQL(", ").join(sql.Identifier(n) for n in columns),
                    sql.SQL(")"),
                ]
            )
            if columns
            else sql.SQL(""),
            sql.SQL(" FROM STDIN ") if method == "SEED" else sql.SQL(" TO STDOUT "),
            sql.SQL("(FORMAT csv, HEADER true, DELIMITER ',')"),
        ]
    )


def export_to_csv(table: str, schema: Optional[str] = None, *, columns: Optional[list[str]] = None):
    """
    Exports records from a table to a .csv file using the COPY command.

    Args:
        table (str): name of table to be populated.
        schema (str): name of schema for the table.
        *columns (list[str]): columns to populate.
    """

    query = _build_query("EXPORT", table, schema, columns)
    path = get_seed_file_path(table, schema)

    with connect(**connection_model()) as db:
        with db.cursor() as cursor:
            with open(path, "wb") as f:
                with cursor.copy(query) as copy:
                    while data := copy.read():
                        f.write(data)

    print(f'\tTable records exported to "{path}"')


def load_from_csv(table: str, schema: Optional[str] = None, *, columns: Optional[list[str]] = None):
    """
    Populates a table from a .csv file using the COPY command.

    Args:
        table (str): name of table to be populated.
        schema (str): name of schema for the table.
        *columns (list[str]): columns to populate.
    """
    query = _build_query("SEED", table, schema, columns)
    path = get_seed_file_path(table, schema)

    with connect(**connection_model()) as db:
        with db.cursor() as cursor:
            with open(path, "r") as f:
                with cursor.copy(query) as copy:
                    while data := f.read():
                        copy.write(data)

    print(f'\tTable seeded from "{path}"')
