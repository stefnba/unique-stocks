from typing import List, Optional, Type, overload

from psycopg.cursor import Cursor
from psycopg.rows import class_row, dict_row

from services.hooks.postgres.types import DbDictRecord, DbModelRecord


class PgRecord:
    """
    Takes a Psycopg cursor, returns records and closes cursor.
    """

    cursor: Cursor

    def __init__(self, cursor: Cursor) -> None:
        cursor.row_factory = dict_row
        self.cursor = cursor

    def get_none(self) -> None:
        """
        Method to close curose. Returns None
        """
        cursor = self.cursor
        cursor.close()

    @overload
    def get_one(self, return_model: Type[DbModelRecord]) -> DbModelRecord:
        ...

    @overload
    def get_one(self) -> DbDictRecord:
        ...

    def get_one(
        self, return_model: Optional[Type[DbModelRecord]] = None
    ) -> DbModelRecord | DbDictRecord | None:
        cursor = self.cursor

        if return_model:
            cursor.row_factory = class_row(return_model)

        result = cursor.fetchone()
        cursor.close()
        return result

    @overload
    def get_all(self) -> List[DbDictRecord]:
        ...

    @overload
    def get_all(self, return_model: Type[DbModelRecord]) -> List[DbModelRecord]:
        ...

    def get_all(
        self, return_model: Optional[Type[DbModelRecord]] = None
    ) -> List[DbDictRecord] | List[DbModelRecord]:
        cursor = self.cursor

        if return_model:
            cursor.row_factory = class_row(return_model)

        result = cursor.fetchall()
        cursor.close()
        return result

    def get_pandas_df(self):
        try:
            import pandas as pd
        except ImportError:
            raise Exception("pandas library not installed.")

        results = self.get_all()

        return pd.DataFrame(results)

    def get_polars_df(self):
        try:
            import polars as pl
        except ImportError:
            raise Exception("polars library not installed.")

        results = self.get_all()

        return pl.DataFrame(results)
