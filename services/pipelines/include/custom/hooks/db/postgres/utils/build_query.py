from psycopg.sql import SQL, Composable, Composed, Identifier, Literal
from custom.hooks.db.postgres.types import (
    PostgresTable,
    PostgresColumns,
    PostgresData,
    PostgresQuery,
    ColumnRecord,
    PostgresReturning,
)
import psycopg
from pydantic import BaseModel
from typing_extensions import LiteralString
from typing import Sequence, Any, Optional

import polars as pl


def table_name(table: PostgresTable) -> Identifier:
    """Helper to deconstruct value into schema and table name."""
    if isinstance(table, tuple):
        return Identifier(*table)

    if "." in table:
        return Identifier(*table.split("."))

    return Identifier(table)


def column_names(columns: PostgresColumns) -> list[str]:
    """Helper to deconstruct columns."""

    if isinstance(columns, list):
        return columns

    if issubclass(columns, BaseModel):
        return list(columns.model_dump().keys())


def transform_data(data: PostgresData, columns: PostgresColumns):
    """Helper to transform data provided."""

    # convert polars df to list of dict
    if isinstance(data, pl.DataFrame):
        data = data.to_dicts()

    # insert many, columns required
    if isinstance(data, Sequence):
        if columns is None:
            raise ValueError("Columns must be specified for multiple data records.")

        # check data is not empty
        if len(data) == 0:
            return None

    # if data is only one record, we still need to make it to a Sequence
    else:
        data = [data]

    # proceed with
    # if issubclass(columns, BaseModel):
    #     columns()
    # return [columns(**record) for record in data]

    return [
        [ColumnRecord(column=column, value=record.get(column)) for column in columns if column in record]
        for record in data
    ]


def transform_query_to_composable(query: PostgresQuery) -> Composable:
    """Helper that transforms various query input formats to psycopg.sql.Composable."""

    if isinstance(query, str):
        return SQL(query)

    return query


def add_query(data: PostgresData, table: PostgresTable, columns: PostgresColumns):
    """Builds INSERT query."""
    transformed_data = transform_data(data=data, columns=columns)

    if not transformed_data:
        raise Exception("Provided data is empty.")

    query = SQL("INSERT INTO {table} ({columns}) VALUES {values}").format(
        table=table_name(table),
        columns=SQL(", ").join(map(Identifier, column_names(columns))),
        values=SQL(", ").join(
            map(
                lambda data_record: SQL("(")
                + SQL(", ").join(map(lambda record_item: SQL("{value}").format(value=record_item.value), data_record))
                + SQL(")"),
                transformed_data,
            )
        ),
    )
    return query


def update_query(data: PostgresData, table: PostgresTable, columns: PostgresColumns):
    """Builds UPDATE query."""
    transformed_data = transform_data(data=data, columns=columns)

    if not transformed_data:
        raise Exception("Provided data is empty.")

    if len(transformed_data) > 1:
        raise Exception("Bulk update not yet implmented.")

    query = SQL("UPDATE {table} SET {values}").format(
        table=table_name(table),
        values=SQL(", ").join(
            map(
                lambda x: SQL("{column}={value}").format(column=Identifier(x.column), value=Literal(x.value)),
                transformed_data[0],
            )
        ),
    )

    return query


def filter_query(filter_settings):
    if not filter_settings:
        return None
    return SQL("WEHER").format(asf="d")


def conflict_query(conflict):
    if not conflict:
        return None

    return SQL("CONFLICT ON *")


def returning_query(returning: Optional[PostgresReturning] = None):
    if not returning:
        return None

    query = SQL("RETURNING ")

    if isinstance(returning, Sequence):
        return query + SQL(", ").join(map(Identifier, column_names(returning)))

    if isinstance(returning, bool) and returning:
        return query + SQL("*")


def pagination_query():
    pass


class BaseQueryBuilder:
    parts: list[PostgresQuery | None]
    query: Composed

    def build(self):
        """Build query."""

        return SQL(" ").join([transform_query_to_composable(p) for p in self.parts if p is not None])


class FindQueryBuilder(BaseQueryBuilder):
    def __init__(self, query: PostgresQuery, filter) -> None:
        self.parts = [query, filter_query(filter), pagination_query()]


class AddQueryBuilder(BaseQueryBuilder):
    def __init__(
        self,
        data: PostgresData,
        table: PostgresTable,
        columns: PostgresColumns,
        returning: Optional[PostgresReturning] = None,
        conflict: Optional[PostgresReturning] = None,
    ) -> None:
        self.parts = [
            add_query(data=data, table=table, columns=columns),
            conflict_query(conflict=conflict),
            returning_query(returning=returning),
        ]


class UpdateQueryBuilder(BaseQueryBuilder):
    def __init__(
        self,
        data: PostgresData,
        table: PostgresTable,
        columns: PostgresColumns,
        filter=None,
        returning: Optional[PostgresReturning] = None,
    ) -> None:
        self.parts = [
            update_query(data=data, table=table, columns=columns),
            filter_query(filter_settings=filter),
            returning_query(returning=returning),
        ]


query = UpdateQueryBuilder(
    data={"asdf": 12323, "name": None},
    columns=["name", "fsdf", "asdf"],
    table="data.exhange",
    returning=True,
).build()
# query = AddQueryBuilder(data={"asdf": 12323}, columns=["asdf", "fsdf"], table="data").build()


with psycopg.connect("postgresql://admin:password@localhost:5871/stocks") as conn:
    print(query.as_string(conn))
