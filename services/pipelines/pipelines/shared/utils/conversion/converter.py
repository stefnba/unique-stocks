from datetime import datetime
import json
from typing import Dict, Type, TypeAlias, Optional, cast
from functools import reduce
import polars as pl


from pydantic import BaseModel, Field


PolarsSchema: TypeAlias = Dict[
    str, Type[pl.Utf8] | Type[pl.Int64] | Type[pl.Boolean] | Type[pl.Datetime] | Type[pl.Float64]
]


def json_to_bytes(json_input: dict | str, encoding="utf-8") -> bytes:
    """
    Converts JSON object to bytes that can be used for file uploads.

    Args:
        input (dict | str): JSON

    Returns:
        _type_: JSON object as bytes
    """

    return json.dumps(json_input).encode(encoding)


class FigiResult(BaseModel):
    figi: str
    name_figi: int = Field(..., alias="name")
    asdf: float = Field(..., alias="name")
    date: datetime
    is_true: bool


def _classify_schema(type: str, field: str, format: Optional[str] = None):
    if type == "string" and format == "date-time":
        return pl.Datetime
    if type == "string":
        return pl.Utf8
    if type == "integer":
        return pl.Int64
    if type == "number":
        return pl.Float64
    if type == "boolean":
        return pl.Boolean
    raise Exception(f'Schema conversion not possible for field "{field}" with type "{type}".')


def _reduce_schema(
    current: dict,
    item: tuple,
) -> PolarsSchema:
    field, value = item
    current[field] = _classify_schema(type=value.get("type", None), field=field, format=value.get("format", None))
    return current


def model_to_polars_schema(model: Type[BaseModel]):
    """
    Convert a Pydantic Model to a Polars Schema.

    Args:
        model (Type[BaseModel]): Pydantic Model to be converted.

    Returns:
        PolarsSchema: Polars Schema.
    """

    print(model.schema())

    properties = model.schema(by_alias=False).get("properties", {})

    return reduce(_reduce_schema, properties.items(), cast(PolarsSchema, {}))


model_to_polars_schema(FigiResult)
