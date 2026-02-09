from datetime import date, datetime
from typing import get_type_hints

import polars as pl
from pydantic import BaseModel

PYDANTIC_TO_POLARS_TYPE_MAPPING: dict[object, type[pl.DataType]] = {
    str: pl.Utf8,
    int: pl.Int64,
    float: pl.Float64,
    bool: pl.Boolean,
    datetime: pl.Datetime,
    date: pl.Date,
}


def pydantic_to_polars_schema(model: type[BaseModel]) -> list[tuple[str, type[pl.DataType]]]:
    """
    Convert a Pydantic model to a Polars schema.

    Returns:
        A list of tuples where each tuple contains the field name and the Polars data type.
    """

    # Extract field types from the Pydantic model
    type_hints = get_type_hints(model)

    # if issubclass(model, RootModel) and len(type_hints.keys()) == 1 and "root" in type_hints.keys():

    #     # type_hints = get_type_hints(type_hints["root"])
    #     print(111, get_type_hints(list(type_hints.values())[0]))

    print(type_hints)

    # Build Polars schema
    schema: list[tuple[str, type[pl.DataType]]] = []

    for field_name, field_type in type_hints.items():
        polars_type = PYDANTIC_TO_POLARS_TYPE_MAPPING.get(field_type, None)
        if polars_type is None:
            raise ValueError(f"Unsupported field type: {field_type} for field {field_name}")

        schema.append((field_name, polars_type))

    return schema
