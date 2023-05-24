import datetime
import json
from typing import Dict, Type

import polars as pl
from pydantic import BaseModel

PolarsSchema = Dict[str, Type[pl.Utf8] | Type[pl.Int64] | Type[pl.Boolean] | Type[pl.Datetime]]


def json_to_bytes(json_input: dict | str, encoding="utf-8") -> bytes:
    """
    Converts JSON object to bytes that can be used for file uploads.

    Args:
        input (dict | str): JSON

    Returns:
        _type_: JSON object as bytes
    """

    return json.dumps(json_input).encode(encoding)


def model_to_polars_schema(model: Type[BaseModel]) -> PolarsSchema:
    """
    Convert a Pydantic Model to a Polars Schema.

    Args:
        model (Type[BaseModel]): Pydantic Model to be converted.

    Returns:
        PolarsSchema: Polars Schema.
    """
    schema: PolarsSchema = {}
    for field, model_type in model.__fields__.items():
        # type = model_type._type_display()

        field_type = model_type.type_
        print(field_type)

        if issubclass(model_type.type_, str):
            schema[field] = pl.Utf8
        elif issubclass(model_type.type_, int):
            schema[field] = pl.Int64
        elif issubclass(model_type.type_, bool):
            schema[field] = pl.Boolean
        elif issubclass(model_type.type_, datetime.datetime):
            schema[field] = pl.Datetime
        else:
            raise Exception(f"Schema conversion not possible for {field} {field_type}")

    return schema
