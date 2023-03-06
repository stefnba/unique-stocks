from typing import Optional

from psycopg.sql import SQL, Composed, Identifier, Literal
from pydantic import BaseModel

from services.hooks.postgres.query.base import QueryBase, UpdateAddBase
from services.hooks.postgres.query.filter import Filter
from services.hooks.postgres.types import FilterParams, QueryData, ReturningParams


class UpdateQuery(QueryBase, Filter, UpdateAddBase):
    def update(
        self,
        data: QueryData,
        table: str,
        filters: Optional[FilterParams] = None,
        returning: Optional[ReturningParams] = None,
    ):
        """

        Args:
            query (Query): _description_
            params (QueryParams): _description_
            filter (): _description_

        UPDATE
            "currencies" AS t
        SET
            "val" = v. "val",
            "msg" = v. "msg"
        FROM (
            VALUES(1, 123, 'hello'),
                (2,
                    456,
                    'world!')) AS v ("id",
                "val",
                "msg")
        WHERE
            v.id = t.id

        Returns:
            _type_: _description_
        """

        query = Composed(
            [
                SQL("UPDATE {table} SET ").format(table=Identifier(table)),
                self.__build_update_items(data),
            ]
        )

        if filters:
            query += self._concatenate_where_query(filters)
        if returning:
            query += self._concatenate_returning_query(returning)

        return self._execute(query=query)

    def __build_update_items(self, data: QueryData) -> Composed:
        """
        Builds the SET column = value part of an SQL update clause.

        Args:
            data (QueryData): `dict` or `BaseModel`

        Returns:
            Composed: _description_
        """

        if isinstance(data, dict):
            items = data.items()
        elif isinstance(data, BaseModel):
            items = data.dict(exclude_unset=True).items()

        items_as_query = [
            SQL("{column} = {value}").format(
                column=Identifier(column), value=Literal(value)
            )
            for column, value in items
        ]

        return Composed(items_as_query).join(", ")
