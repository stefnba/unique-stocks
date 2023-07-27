from typing import List, Optional, Type, overload, Sequence, Mapping

import polars as pl
from polars.type_aliases import SchemaDefinition
from psycopg._column import Column
from psycopg.cursor import Cursor
from psycopg.rows import class_row, dict_row
from shared.hooks.postgres.types import DbDictRecord, DbModelRecord
from pydantic import BaseModel
from shared.utils.conversion.converter import model_to_polars_schema
from shared.loggers import logger, events as log_events


def _classify_schema_type(schema: Optional[SchemaDefinition | Type[BaseModel] | BaseModel]):
    """
    Helper function to classifiy schema type.
    """

    if schema is None:
        return []

    if isinstance(schema, Sequence) or isinstance(schema, Mapping):
        return schema

    if isinstance(schema, BaseModel):
        return model_to_polars_schema(schema)

    if issubclass(schema, BaseModel):
        return model_to_polars_schema(schema)

    return []


class PgRecord:
    """
    Takes a Psycopg cursor, returns records and closes cursor.
    """

    cursor: Cursor
    query: str
    columns: Optional[list[Column]]
    table: Optional[str | tuple[str, str]] = None

    def __init__(self, cursor: Cursor, query: str, table: Optional[str | tuple[str, str]] = None) -> None:
        cursor.row_factory = dict_row
        self.cursor = cursor
        self.columns = cursor.description

        self.query = query
        self.table = table

        logger.db.info(event=log_events.database.QueryResult(length=cursor.rowcount, query=query, table=table))

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

    def get_one(self, return_model: Optional[Type[DbModelRecord]] = None) -> DbModelRecord | DbDictRecord | None:
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

    def get_all(self, return_model: Optional[Type[DbModelRecord]] = None) -> List[DbDictRecord] | List[DbModelRecord]:
        cursor = self.cursor

        if return_model:
            cursor.row_factory = class_row(return_model)

        try:
            result = cursor.fetchall()

        except Exception as e:
            print(e, self.query)
            result = []

        cursor.close()
        return result

    def get_pandas_df(self):
        try:
            import pandas as pd
        except ImportError:
            raise ImportError("pandas library not installed.")

        results = self.get_all()

        return pd.DataFrame(results)

    def get_polars_df(self, schema: Optional[SchemaDefinition | Type[BaseModel] | BaseModel] = None) -> pl.DataFrame:
        try:
            import polars as pl
        except ImportError:
            raise ImportError("polars library not installed.")

        results = self.get_all()

        if len(results) == 0:
            return pl.DataFrame(schema=_classify_schema_type(schema) or [col[0] for col in self.columns or []])

        try:
            return pl.DataFrame(results, schema=_classify_schema_type(schema))
        except:
            print("ddd", _classify_schema_type(schema), results)
            raise

    def get_polars_lf(self, schema: Optional[SchemaDefinition | Type[BaseModel] | BaseModel] = None) -> pl.LazyFrame:
        try:
            import polars as pl
        except ImportError:
            raise ImportError("polars library not installed.")

        results = self.get_all()

        if len(results) == 0:
            return pl.LazyFrame(schema=_classify_schema_type(schema) or [col[0] for col in self.columns or []])

        return pl.LazyFrame(results, schema=_classify_schema_type(schema))
