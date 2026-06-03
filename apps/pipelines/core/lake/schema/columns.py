"""Column metadata and Python-to-SQL type inference for lake table models.

The schema layer uses Pydantic row models as the source for domain columns.
This module converts those Pydantic fields into simple SQL column specs and
allows explicit SQL overrides with ``typing.Annotated`` metadata.
"""

import typing
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, datetime
from decimal import Decimal
from types import NoneType, UnionType
from typing import Annotated, Any, Literal, get_args, get_origin
from uuid import UUID as UUIDValue

from pydantic.fields import FieldInfo

type SqlTypeName = Literal[
    "BIGINT",
    "BOOLEAN",
    "DATE",
    "DECIMAL",
    "DOUBLE",
    "INTEGER",
    "JSON",
    "TEXT",
    "TIMESTAMPTZ",
    "UUID",
    "VARCHAR",
]

BIGINT: SqlTypeName = "BIGINT"
BOOLEAN: SqlTypeName = "BOOLEAN"
DATE: SqlTypeName = "DATE"
DECIMAL: SqlTypeName = "DECIMAL"
DOUBLE: SqlTypeName = "DOUBLE"
INTEGER: SqlTypeName = "INTEGER"
JSON: SqlTypeName = "JSON"
TEXT: SqlTypeName = "TEXT"
TIMESTAMPTZ: SqlTypeName = "TIMESTAMPTZ"
UUID: SqlTypeName = "UUID"
VARCHAR: SqlTypeName = "VARCHAR"


@dataclass(frozen=True, slots=True)
class SqlColumn:
    """Optional SQL metadata for a Pydantic field.

    Use with ``typing.Annotated`` when the Python type is not specific enough:

    ``amount: Annotated[Decimal, SqlColumn("DECIMAL(18, 6)")].``
    """

    #: SQL type to render in DDL, for example ``"VARCHAR"`` or ``"DECIMAL(18, 6)"``.
    sql_type: str
    #: Override inferred nullability. ``None`` means infer from the Python annotation.
    nullable: bool | None = None
    #: SQL default expression rendered after ``DEFAULT``.
    default: str | None = None


@dataclass(frozen=True, slots=True)
class ColumnSpec:
    """Rendered SQL column definition.

    ``ColumnSpec`` is the normalized form used by table DDL rendering. It is
    intentionally independent from Pydantic so storage-specific concerns stay
    outside the row validation model.
    """

    #: Physical SQL column name.
    name: str
    #: SQL type string rendered exactly as provided.
    sql_type: str
    #: Whether the column may contain ``NULL`` values.
    nullable: bool = False
    #: Optional SQL default expression.
    default: str | None = None

    def to_ddl(self) -> str:
        """Return this column as a SQL DDL fragment.

        Returns:
            Column definition suitable for use inside a ``CREATE TABLE``
            statement.

        Example:
            ``ColumnSpec("name", "VARCHAR").to_ddl()`` returns
            ``"name VARCHAR NOT NULL"``.
        """
        parts = [self.name, self.sql_type]
        if not self.nullable:
            parts.append("NOT NULL")
        if self.default is not None:
            parts.append(f"DEFAULT {self.default}")
        return " ".join(parts)


def column_from_field(name: str, field: FieldInfo) -> ColumnSpec:
    """Build a SQL column spec from a Pydantic field.

    SQL type is inferred from the field annotation unless the annotation or
    field metadata contains ``SqlColumn``. Optional annotations such as
    ``str | None`` become nullable columns by default.

    Args:
        name: Physical column name to render.
        field: Pydantic field metadata from the row model.

    Returns:
        Normalized SQL column specification.
    """
    annotation, metadata = _unwrap_annotated(field.annotation)
    metadata.extend(field.metadata)
    annotation, is_optional = _unwrap_optional(annotation)
    override = next((item for item in metadata if isinstance(item, SqlColumn)), None)

    sql_type = override.sql_type if override else _infer_sql_type(annotation)
    nullable = override.nullable if override and override.nullable is not None else is_optional
    default = override.default if override else None
    return ColumnSpec(name=name, sql_type=sql_type, nullable=nullable, default=default)


def _unwrap_annotated(annotation: Any) -> tuple[Any, list[Any]]:
    """Return the base annotation and metadata from ``Annotated`` values.

    Args:
        annotation: Python annotation to inspect.

    Returns:
        A pair of the unwrapped annotation and any ``Annotated`` metadata.
    """
    if get_origin(annotation) is Annotated:
        args = get_args(annotation)
        return args[0], list(args[1:])
    return annotation, []


def _unwrap_optional(annotation: Any) -> tuple[Any, bool]:
    """Return the non-None annotation and whether the original was optional.

    Args:
        annotation: Python annotation to inspect.

    Returns:
        A pair of the effective annotation and whether ``None`` was allowed.
    """
    origin = get_origin(annotation)
    if origin in {typing.Union, UnionType}:
        args = tuple(arg for arg in get_args(annotation) if arg is not NoneType)
        if len(args) != len(get_args(annotation)):
            return (args[0] if len(args) == 1 else annotation), True
    return annotation, False


def _infer_sql_type(annotation: Any) -> str:
    """Infer a DuckDB-compatible SQL type for a Python annotation.

    Args:
        annotation: Python annotation to map to SQL.

    Returns:
        DuckDB-compatible SQL type string.

    Raises:
        TypeError: If the annotation cannot be mapped without an explicit override.
    """
    origin = get_origin(annotation)
    if annotation is str:
        return VARCHAR
    if annotation is int:
        return BIGINT
    if annotation is float:
        return DOUBLE
    if annotation is bool:
        return BOOLEAN
    if annotation is Decimal:
        return DECIMAL
    if annotation is date:
        return DATE
    if annotation is datetime:
        return TIMESTAMPTZ
    if annotation is UUIDValue:
        return UUID
    if origin in {dict, Mapping, list, tuple, Sequence}:
        return JSON
    raise TypeError(f"Cannot infer SQL type for annotation {annotation!r}; use Annotated[..., SqlColumn(...)]")
