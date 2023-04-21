import sys
from pathlib import Path

from alembic import op


def execute_ddl_file(path_to_sql_file: str) -> str:
    """
    Execute a DDL SQL file with alembic.
    """
    if not path_to_sql_file.startswith("/"):
        namespace = sys._getframe(1).f_globals  # caller's globals
        base_path = Path(str(namespace.get("__file__"))).parent
        path_to_sql_file = Path(base_path, path_to_sql_file).resolve().as_posix()


    if not path_to_sql_file.endswith('.sql'):
         raise Exception("Path must reference a file with .sql extension.")


    with open(path_to_sql_file, "r") as file:
            sql = file.read()
            sql = sql.strip()

    op.execute(sql)

    return sql
            