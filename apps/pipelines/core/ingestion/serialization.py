"""Serialization helpers shared by landing and Bronze ingestion surfaces."""

import json
from collections.abc import Mapping, Sequence
from datetime import date, datetime
from decimal import Decimal
from typing import Any

from pydantic import BaseModel


def jsonable(value: Any) -> Any:
    """Return a JSON-serializable value while preserving provider aliases.

    Args:
        value: Arbitrary provider/domain value.

    Returns:
        Value normalized for JSON storage.
    """
    if isinstance(value, BaseModel):
        return value.model_dump(mode="json", by_alias=True)
    if isinstance(value, Mapping):
        return {str(key): jsonable(item) for key, item in value.items()}
    if isinstance(value, Sequence) and not isinstance(value, str | bytes | bytearray | memoryview):
        return [jsonable(item) for item in value]
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime | date):
        return value.isoformat()
    return value


def canonical_json(value: Any) -> str:
    """Return deterministic compact JSON for provider or Bronze payloads.

    Args:
        value: Value to normalize and serialize.

    Returns:
        Compact JSON string with stable key ordering.
    """
    return json.dumps(jsonable(value), sort_keys=True, separators=(",", ":"))


def sql_value(value: date | str | int) -> str | int:
    """Return a stable scalar value for SQL query parameters.

    Args:
        value: Date, string, or integer query parameter.

    Returns:
        ISO date string for dates; otherwise the original scalar.
    """
    if isinstance(value, date):
        return value.isoformat()
    return value
