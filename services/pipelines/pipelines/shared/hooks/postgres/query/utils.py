from psycopg.sql import SQL, Composed, Identifier, Literal, Placeholder
from shared.hooks.postgres.types import (
    ConflictActionExcludedDict,
    ConflictActionValueDict,
    ConflictParams,
    ReturningParams,
)
from typing import cast


def build_table_name(table: str | tuple[str, str]) -> Identifier:
    """
    Create a table SQL identifiert as "table" or "schema"."table" if schema is specified.
    """
    if isinstance(table, str):
        return Identifier(table)
    return Identifier(*table)


def build_returning_query(returning: ReturningParams) -> Composed:
    return_clase = SQL(" RETURNING ")
    if isinstance(returning, str):
        if returning == "ALL_COLUMNS":
            return Composed([return_clase, SQL("*")])

        return Composed([return_clase, SQL("{}").format(returning)])

    if isinstance(returning, list):
        return Composed([return_clase, SQL(", ").join(map(Identifier, returning))])

    return Composed("")


def build_conflict_query(conflict: ConflictParams) -> Composed:
    if isinstance(conflict, str):
        action = SQL("")
        if conflict == "DO_NOTHING":
            action = SQL("DO NOTHING")

        return Composed([SQL(" ON CONFLICT "), action])

    if isinstance(conflict, dict):
        target_list = conflict["target"]
        # action_list = cast(list[ConflictActionDict], conflict["action"])
        action_list = conflict["action"]

        def map_action_list(element: ConflictActionExcludedDict | ConflictActionValueDict):
            value = element.get("value")
            excluded = element.get("excluded")
            if value:
                return SQL("{column} = {value}").format(column=Identifier(element["column"]), value=Literal(value))
            if excluded and isinstance(excluded, str):
                return Composed(
                    [SQL("{column} = EXCLUDED.").format(column=Identifier(element["column"])), SQL(excluded)]
                )

            raise Exception("Conflict must either include a 'value' or 'excluded' key")

        return Composed(
            [
                SQL(" ON CONFLICT ("),
                SQL(", ").join(Identifier(column) for column in target_list),
                SQL(")"),
                SQL(" DO UPDATE SET "),
                SQL(", ").join(map(map_action_list, action_list)),
            ]
        )

    return Composed([SQL("")])
