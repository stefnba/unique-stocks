"""Table metadata models with DDL rendering.

The table model layer is separate from Pydantic row models. Row models validate
data produced by parsers; table models describe physical lake storage and can
render DDL from those row models plus layer-specific metadata.
"""

from typing import ClassVar, Literal

from pydantic import BaseModel

from core.lake.schema.columns import (
    JSON,
    TIMESTAMPTZ,
    UUID,
    VARCHAR,
    ColumnSpec,
    column_from_field,
)

type SchemaName = Literal["bronze", "silver", "gold", "pipeline", "lake"]


class TableModel:
    """Base class for lake table models.

    Table models are intentionally not Pydantic models. They describe physical
    lake schema and reference a separate Pydantic ``row_model`` that validates
    rows produced by parsers.
    """

    #: Lake schema name, such as ``"bronze"``.
    schema_name: ClassVar[SchemaName]
    #: Physical table name inside ``schema_name``.
    table_name: ClassVar[str]
    #: Pydantic model that validates domain row data for this table.
    row_model: ClassVar[type[BaseModel]]
    #: Physical columns that make a row unique.
    unique_columns: ClassVar[tuple[str, ...]]
    #: Physical columns used to decide whether an ingestion partition is already complete.
    idempotency_columns: ClassVar[tuple[str, ...]]

    def __init_subclass__(cls, **kwargs: object) -> None:
        """Validate concrete table definitions as soon as they are declared.

        Base classes may omit table metadata. Any subclass that declares table
        attributes is treated as concrete enough to validate.

        Args:
            **kwargs: Keyword arguments forwarded by Python subclass creation.
        """
        super().__init_subclass__(**kwargs)
        table_attrs = {"table_name", "row_model", "unique_columns", "idempotency_columns"}
        if not table_attrs.intersection(cls.__dict__):
            return
        cls._validate_table_definition()

    @classmethod
    def qualified_name(cls) -> str:
        """Return the fully qualified table name.

        Returns:
            Table name in ``schema.table`` form.
        """
        return f"{cls.schema_name}.{cls.table_name}"

    @classmethod
    def columns(cls) -> tuple[ColumnSpec, ...]:
        """Return SQL column specs inferred from the Pydantic row model.

        Returns:
            Physical SQL columns for the table.
        """
        return tuple(column_from_field(name, field) for name, field in cls.row_model.model_fields.items())

    @classmethod
    def column_names(cls) -> tuple[str, ...]:
        """Return all physical column names for this table.

        Returns:
            Physical column names in render order.
        """
        return tuple(column.name for column in cls.columns())

    @classmethod
    def unique_column_names(cls) -> tuple[str, ...]:
        """Return columns that make one table row unique.

        Returns:
            Unique constraint column names.
        """
        return cls.unique_columns

    @classmethod
    def idempotency_column_names(cls) -> tuple[str, ...]:
        """Return idempotency column names.

        Returns:
            Columns used to detect completed ingestion partitions.
        """
        return cls.idempotency_columns

    @classmethod
    def to_ddl(cls) -> str:
        """Return ``CREATE TABLE IF NOT EXISTS`` DDL for this table model.

        Returns:
            Idempotent table creation SQL.
        """
        columns = cls.columns()
        cls._validate_column_refs(columns)

        lines = [f"CREATE TABLE IF NOT EXISTS {cls.qualified_name()} ("]
        definitions = [f"    {column.to_ddl()}" for column in columns]
        unique_columns = cls.unique_column_names()
        if unique_columns:
            definitions.append(f"    UNIQUE ({', '.join(unique_columns)})")
        lines.append(",\n".join(definitions))
        lines.append(");")
        return "\n".join(lines)

    @classmethod
    def _validate_table_definition(cls) -> None:
        """Validate required table metadata and column references.

        Raises:
            TypeError: If required table metadata is missing or malformed.
            ValueError: If unique or idempotency metadata references an unknown column.
        """
        required_attrs = ("table_name", "row_model", "unique_columns", "idempotency_columns")
        missing = tuple(name for name in required_attrs if name not in cls.__dict__)
        missing += tuple(name for name in ("schema_name",) if not hasattr(cls, name))
        if missing:
            raise TypeError(f"{cls.__name__} is missing required table metadata: {missing}")
        if not isinstance(cls.table_name, str) or not cls.table_name:
            raise TypeError(f"{cls.__name__}.table_name must be a non-empty string")
        if not isinstance(cls.row_model, type) or not issubclass(cls.row_model, BaseModel):
            raise TypeError(f"{cls.__name__}.row_model must be a Pydantic BaseModel subclass")
        for name in ("unique_columns", "idempotency_columns"):
            if not isinstance(getattr(cls, name), tuple):
                raise TypeError(f"{cls.__name__}.{name} must be a tuple of column names")
            if not all(isinstance(column, str) for column in getattr(cls, name)):
                raise TypeError(f"{cls.__name__}.{name} must contain only strings")
        cls._validate_column_refs(cls.columns())

    @classmethod
    def _validate_column_refs(cls, columns: tuple[ColumnSpec, ...]) -> None:
        """Validate that metadata column names exist in the physical table.

        Args:
            columns: Physical columns available on the table.

        Raises:
            ValueError: If metadata references an unknown column.
        """
        known = {column.name for column in columns}
        for label, refs in {
            "unique_columns": cls.unique_columns,
            "idempotency_columns": cls.idempotency_columns,
        }.items():
            missing = tuple(ref for ref in refs if ref not in known)
            if missing:
                raise ValueError(f"{cls.__name__}.{label} references unknown columns: {missing}")


class BronzeTableModel(TableModel):
    """Base class for Bronze tables.

    Bronze row fields remain parser-owned domain data. The standard ingestion
    envelope is added only when rendering DDL, so parsers do not need to provide
    ``data_provider``, ``raw_json``, ``row_hash``, ``source_uri``, or ``ingested_at``.
    """

    schema_name: ClassVar[SchemaName] = "bronze"

    @classmethod
    def columns(cls) -> tuple[ColumnSpec, ...]:
        """Return domain columns plus the standard Bronze ingestion envelope.

        The envelope is physical storage metadata and is therefore excluded
        from parser-owned Pydantic row models.

        Returns:
            Physical SQL columns including Bronze envelope metadata.
        """
        return (
            ColumnSpec("ingestion_id", UUID, nullable=True, default="GEN_RANDOM_UUID()"),
            *super().columns(),
            ColumnSpec("data_provider", VARCHAR),
            ColumnSpec("raw_json", JSON),
            ColumnSpec("row_hash", VARCHAR),
            ColumnSpec("source_uri", VARCHAR, nullable=True),
            ColumnSpec("ingested_at", TIMESTAMPTZ, nullable=True, default="NOW()"),
        )
