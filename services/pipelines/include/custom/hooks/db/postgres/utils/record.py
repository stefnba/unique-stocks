from typing import Optional, Type, overload, List
from psycopg.cursor import Cursor
from custom.hooks.db.postgres.types import PostgresModelRecord, PostgresDictRecord
from psycopg.rows import class_row, dict_row


class PostgresRecord:
    """
    Takes a Psycopg cursor, returns records of the desired type and closes cursor.
    """

    cursor: Cursor

    def __init__(self, cursor: Cursor) -> None:
        self.cursor = cursor

    def get_none(self) -> None:
        """
        Method to close curose. Returns None
        """
        cursor = self.cursor
        cursor.close()

    @overload
    def get_one(self, return_model: Type[PostgresModelRecord]) -> PostgresModelRecord:
        ...

    @overload
    def get_one(self) -> PostgresDictRecord:
        ...

    def get_one(
        self, return_model: Optional[Type[PostgresModelRecord]] = None
    ) -> PostgresModelRecord | PostgresDictRecord | None:
        cursor = self.cursor

        if return_model:
            cursor.row_factory = class_row(return_model)

        result = cursor.fetchone()
        cursor.close()
        return result

    @overload
    def get_all(self) -> List[PostgresDictRecord]:
        ...

    @overload
    def get_all(self, return_model: Type[PostgresModelRecord]) -> List[PostgresModelRecord]:
        ...

    def get_all(
        self, return_model: Optional[Type[PostgresModelRecord]] = None
    ) -> List[PostgresModelRecord] | List[PostgresDictRecord]:
        cursor = self.cursor

        if return_model:
            cursor.row_factory = class_row(return_model)

        result = cursor.fetchall()

        cursor.close()
        return result
