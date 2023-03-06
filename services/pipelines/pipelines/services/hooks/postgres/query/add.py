from psycopg.sql import SQL, Composed, Identifier, Literal
from pydantic import BaseModel

from services.hooks.postgres.query.base import QueryBase, UpdateAddBase
from services.hooks.postgres.types import ConflictParams, QueryData, ReturningParams


class AddQuery(QueryBase, UpdateAddBase):
    def add(
        self,
        data: QueryData | list[QueryData],
        table: str,
        returning: ReturningParams,
        conflict: ConflictParams,
    ):
        query = Composed(
            [
                SQL("INSERT INTO {table} ").format(table=Identifier(table)),
                self.__build_create_add_logic(data),
            ]
        )

        if returning:
            query += self._concatenate_returning_query(returning)

        if conflict:
            query += self.___concatenate_conflict_query(conflict)

        return self._execute(query=query)

    def __build_create_add_logic(self, data: QueryData | list[QueryData]) -> Composed:
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
        # insert many
        if isinstance(data, list):
            return self.__build_create_logic_many(data)
        # insert single
        if isinstance(data, dict):
            _data = data
            columns = list(data.keys())
        elif isinstance(data, BaseModel):
            _data = data.dict(exclude_unset=True)
            columns = list(data.dict(exclude_unset=True).keys())

        # add values in order as column are specified
        values = [_data.get(column, None) for column in columns]

        return Composed(
            [
                # columns
                SQL("({columns})").format(
                    columns=SQL(", ").join(Identifier(column) for column in columns)
                ),
                SQL(" VALUES "),
                # values
                SQL("({columns})").format(
                    columns=SQL(", ").join(Literal(value) for value in values)
                ),
            ]
        )

    def __build_create_logic_many(self, data_list: list[QueryData]) -> Composed:
        # infer columns from first data item in list
        first_data = data_list[0]
        columns = []
        if isinstance(first_data, dict):
            columns = list(first_data.keys())
        elif isinstance(first_data, BaseModel):
            columns = list(first_data.dict(exclude_unset=True).keys())

        values_list: list[Composed] = []

        for data in data_list:
            _data = {}
            if isinstance(data, dict):
                _data = data
            elif isinstance(data, BaseModel):
                _data = data.dict(exclude_unset=True)

            values = [_data.get(column, None) for column in columns]

            values_list.append(
                SQL("({columns})").format(
                    columns=SQL(", ").join(Literal(value) for value in values)
                )
            )

        return Composed(
            [
                # columns
                SQL("({columns})").format(
                    columns=SQL(", ").join(Identifier(column) for column in columns)
                ),
                SQL(" VALUES "),
                # values
                SQL(", ").join(values_list),
            ]
        )

    def ___concatenate_conflict_query(self, conflict: ConflictParams) -> Composed:
        return Composed("")
