from typing import cast

from psycopg.sql import SQL
from pydantic import create_model_from_typeddict
from shared.utils.path.builder import FilePathBuilder
from shared.utils.sql.types import QueryFilePath
from typing_extensions import LiteralString


class QueryFile:
    """
    SQL query from an external .sql file.
    """

    _sql: SQL
    _path: str

    def __init__(self, path: str | QueryFilePath) -> None:
        self.set_file_path(path)
        self.extract_sql()

    def extract_sql(self) -> None:
        """
        Read SQL query from file.
        """
        path = self._path

        with open(path, "r") as file:
            sql = file.read()

            if isinstance(sql, str):
                # print(len(sql))
                sql = sql.strip()
                self._sql = SQL(cast(LiteralString, sql))
                return

            raise ValueError("QueryFile content must be of type str.")

    def set_file_path(self, path: str | QueryFilePath) -> None:
        """
        Constructs and sets absolute file path of QueryFile.

        Args:
            path (str | QueryFilePath): Path the .sql file.

        """
        if isinstance(path, str):
            self._path = path
            return

        _path = create_model_from_typeddict(QueryFilePath)(**path).dict()  # type: ignore
        self._path = FilePathBuilder.build_relative_file_path(**_path)

    @property
    def sql(self):
        return self._sql
