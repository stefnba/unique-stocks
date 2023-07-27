from typing import Optional, Sequence, Type, overload

import polars as pl
from psycopg.sql import SQL, Composed, Identifier, Literal
from pydantic import BaseModel, ValidationError
from shared.hooks.postgres.query.base import QueryBase
from shared.hooks.postgres.query.record import PgRecord
from shared.hooks.postgres.types import ConflictParams, QueryColumnModel, QueryData, ReturningParams
from shared.loggers import logger, events as log_events
from shared.hooks.postgres.query.utils import build_conflict_query, build_returning_query, build_table_name


class AddQuery(QueryBase):
    @overload
    def add(
        self,
        data: list[QueryData],
        table: str | tuple[str, str],
        column_model: Type[QueryColumnModel],
        returning: Optional[ReturningParams] = None,
        conflict: Optional[ConflictParams] = None,
    ) -> PgRecord:
        ...

    @overload
    def add(
        self,
        data: QueryData,
        table: str | tuple[str, str],
        column_model: Optional[Type[QueryColumnModel]] = None,
        returning: Optional[ReturningParams] = None,
        conflict: Optional[ConflictParams] = None,
    ) -> PgRecord:
        ...

    def add(
        self,
        data: list[QueryData] | QueryData,
        table: str | tuple[str, str],
        column_model: Optional[Type[QueryColumnModel]] = None,
        returning: Optional[ReturningParams] = None,
        conflict: Optional[ConflictParams] = None,
    ) -> PgRecord:
        data_length = len(data) if isinstance(data, (pl.DataFrame, Sequence)) else 1

        logger.db.info(
            msg=f"Adding {data_length} records to table '{table}'",
            event=log_events.database.AddInit(table=table, length=data_length),
        )

        # convert polars df to list of dict
        if isinstance(data, pl.DataFrame):
            data = data.to_dicts()

        # check data is not empty
        if isinstance(data, Sequence):
            if len(data) == 0:
                return self._execute(query=SQL(""))

        print(111, data)

        query = Composed(
            [
                SQL("INSERT INTO {table} ").format(table=build_table_name(table)),
                self.__build_create_add_logic(data=data, column_model=column_model),
            ]
        )

        if conflict:
            query += build_conflict_query(conflict)

        if returning:
            query += build_returning_query(returning)

        return self._execute(query=query, table=table)

    def __build_create_add_logic(
        self, data: QueryData | Sequence[QueryData], column_model: Optional[Type[QueryColumnModel]] = None
    ) -> Composed:
        """
        Builds the columns and values part of the INSERT INTO statement,
        i.e. `(column1, column2) VALUES (value1, value2)`.

        If data provided is a list of objects, construction is done via
        `__build_create_logic_many()` method.

        Args:
            data (QueryData): _description_

        Returns:
            Composed: _description_
        """
        # insert many, column_model required
        if isinstance(data, Sequence):
            if column_model is None:
                raise ValueError("Column model must be specified.")

            return self.__build_create_logic_with_column_model(data=data, column_model=column_model)

        # insert single but with column_model
        if column_model is not None:
            return self.__build_create_logic_with_column_model(data=[data], column_model=column_model)

        # insert single, no column_model
        columns: list[str] = []
        record: dict = {}
        if isinstance(data, dict):
            record = data
            columns = list(data.keys())
        elif isinstance(data, BaseModel):
            record = data.dict(exclude_unset=True)
            columns = list(data.dict(exclude_unset=True).keys())

        # add values in order as column are specified
        values = [record.get(column, None) for column in columns]

        return Composed(
            [
                # columns
                SQL("({columns})").format(columns=SQL(", ").join(Identifier(column) for column in columns)),
                SQL(" VALUES "),
                # values
                SQL("({values})").format(values=SQL(", ").join(Literal(value) for value in values)),
            ]
        )

    def __build_create_logic_with_column_model(
        self, data: Sequence[QueryData], column_model: Type[QueryColumnModel]
    ) -> Composed:
        """
        Builds the columns and values part of the INSERT INTO statement,
        i.e. `(column1, column2) VALUES (value1, value2)`, when a column_model is specified.
        """

        ColumnModel = column_model

        columns = ColumnModel.__fields__
        values_list: list[Composed] = []

        for data_item in data:
            record = {}
            # dict
            if isinstance(data_item, dict):
                try:
                    record = ColumnModel(**data_item).dict(exclude_unset=True)
                except ValidationError as error:
                    logger.db.error(str(error), extra={"data": data_item})
                    raise

            # pydantic Model
            elif isinstance(data_item, BaseModel):
                record = {}
                raise Exception("STILL PENDING")

            values_list.append(
                SQL("({columns})").format(
                    columns=SQL(", ").join(Literal(value) for value in [record.get(column, None) for column in columns])
                )
            )

        return Composed(
            [
                # columns
                SQL("({columns})").format(columns=SQL(", ").join(Identifier(column) for column in columns)),
                SQL(" VALUES "),
                # values
                SQL(", ").join(values_list),
            ]
        )
