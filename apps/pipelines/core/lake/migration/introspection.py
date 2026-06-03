"""Schema introspection for lake migrations."""

from collections import defaultdict
from collections.abc import Collection, Sequence
from dataclasses import dataclass

import duckdb

from core.lake.schema.columns import ColumnSpec
from core.lake.schema.table import TableModel

type TableKey = tuple[str, str]


@dataclass(frozen=True, slots=True)
class DesiredTable:
    """Desired table shape from Python table specs."""

    schema: str
    name: str
    columns: tuple[ColumnSpec, ...]
    unique_columns: tuple[str, ...]
    ddl: str

    @property
    def key(self) -> TableKey:
        """Return the table lookup key."""
        return (self.schema, self.name)

    @property
    def qualified_name(self) -> str:
        """Return the unquoted qualified table name."""
        return f"{self.schema}.{self.name}"


@dataclass(frozen=True, slots=True)
class DesiredLakeSchema:
    """Desired lake schema built from Python table specs."""

    schemas: frozenset[str]
    tables: dict[TableKey, DesiredTable]


@dataclass(frozen=True, slots=True)
class ActualColumn:
    """Existing lake column as reported by information_schema."""

    name: str
    sql_type: str
    nullable: bool
    default: str | None
    ordinal_position: int


@dataclass(frozen=True, slots=True)
class ActualTable:
    """Existing lake table as reported by information_schema."""

    schema: str
    name: str
    columns: dict[str, ActualColumn]
    unique_constraints: tuple[tuple[str, ...], ...]

    @property
    def key(self) -> TableKey:
        """Return the table lookup key."""
        return (self.schema, self.name)

    @property
    def qualified_name(self) -> str:
        """Return the unquoted qualified table name."""
        return f"{self.schema}.{self.name}"


@dataclass(frozen=True, slots=True)
class ActualLakeSchema:
    """Current lake schema discovered from information_schema."""

    schemas: frozenset[str]
    tables: dict[TableKey, ActualTable]


def desired_lake_schema_from_tables(
    tables: Sequence[type[TableModel]],
    *,
    default_schemas: Sequence[str] = (),
) -> DesiredLakeSchema:
    """Build the desired schema from registered table specs."""
    desired_tables = {
        (table.schema_name, table.table_name): DesiredTable(
            schema=table.schema_name,
            name=table.table_name,
            columns=table.columns(),
            unique_columns=table.unique_column_names(),
            ddl=table.to_ddl(),
        )
        for table in tables
    }
    schemas = frozenset((*default_schemas, *(schema for schema, _ in desired_tables)))
    return DesiredLakeSchema(schemas=schemas, tables=desired_tables)


def inspect_lake_schema(
    connection: duckdb.DuckDBPyConnection,
    *,
    schemas: Collection[str] | None = None,
) -> ActualLakeSchema:
    """Read the current lake schema from DuckDB/MotherDuck information_schema."""
    schema_filter = set(schemas) if schemas is not None else None
    existing_schemas = _fetch_schemas(connection, schema_filter)
    table_rows = _fetch_tables(connection, schema_filter)
    columns_by_table = _fetch_columns(connection, schema_filter)
    uniques_by_table = _fetch_unique_constraints(connection, schema_filter)

    actual_tables = {
        (schema, table): ActualTable(
            schema=schema,
            name=table,
            columns=columns_by_table.get((schema, table), {}),
            unique_constraints=uniques_by_table.get((schema, table), ()),
        )
        for schema, table in table_rows
    }
    return ActualLakeSchema(schemas=existing_schemas, tables=actual_tables)


def _fetch_schemas(
    connection: duckdb.DuckDBPyConnection,
    schema_filter: set[str] | None,
) -> frozenset[str]:
    rows = connection.execute("SELECT schema_name FROM information_schema.schemata").fetchall()
    schemas = {str(row[0]) for row in rows}
    if schema_filter is not None:
        schemas &= schema_filter
    return frozenset(schemas)


def _fetch_tables(
    connection: duckdb.DuckDBPyConnection,
    schema_filter: set[str] | None,
) -> tuple[TableKey, ...]:
    rows = connection.execute(
        """
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
        ORDER BY table_schema, table_name
        """
    ).fetchall()
    tables = tuple((str(row[0]), str(row[1])) for row in rows)
    if schema_filter is None:
        return tables
    return tuple(table for table in tables if table[0] in schema_filter)


def _fetch_columns(
    connection: duckdb.DuckDBPyConnection,
    schema_filter: set[str] | None,
) -> dict[TableKey, dict[str, ActualColumn]]:
    rows = connection.execute(
        """
        SELECT table_schema, table_name, column_name, data_type, is_nullable, column_default, ordinal_position
        FROM information_schema.columns
        ORDER BY table_schema, table_name, ordinal_position
        """
    ).fetchall()
    columns_by_table: dict[TableKey, dict[str, ActualColumn]] = defaultdict(dict)
    for schema, table, column, sql_type, is_nullable, default, ordinal_position in rows:
        key = (str(schema), str(table))
        if schema_filter is not None and key[0] not in schema_filter:
            continue
        name = str(column)
        columns_by_table[key][name] = ActualColumn(
            name=name,
            sql_type=str(sql_type),
            nullable=str(is_nullable).upper() == "YES",
            default=str(default) if default is not None else None,
            ordinal_position=int(ordinal_position),
        )
    return dict(columns_by_table)


def _fetch_unique_constraints(
    connection: duckdb.DuckDBPyConnection,
    schema_filter: set[str] | None,
) -> dict[TableKey, tuple[tuple[str, ...], ...]]:
    rows = connection.execute(
        """
        SELECT tc.table_schema, tc.table_name, kcu.constraint_name, kcu.column_name, kcu.ordinal_position
        FROM information_schema.table_constraints AS tc
        JOIN information_schema.key_column_usage AS kcu
          ON tc.constraint_catalog = kcu.constraint_catalog
         AND tc.constraint_schema = kcu.constraint_schema
         AND tc.constraint_name = kcu.constraint_name
         AND tc.table_schema = kcu.table_schema
         AND tc.table_name = kcu.table_name
        WHERE tc.constraint_type = 'UNIQUE'
        ORDER BY tc.table_schema, tc.table_name, kcu.constraint_name, kcu.ordinal_position
        """
    ).fetchall()
    columns_by_constraint: dict[tuple[str, str, str], list[tuple[int, str]]] = defaultdict(list)
    for schema, table, constraint, column, ordinal_position in rows:
        schema_name = str(schema)
        if schema_filter is not None and schema_name not in schema_filter:
            continue
        columns_by_constraint[(schema_name, str(table), str(constraint))].append((int(ordinal_position), str(column)))

    uniques_by_table: dict[TableKey, list[tuple[str, ...]]] = defaultdict(list)
    for schema, table, _constraint in sorted(columns_by_constraint):
        constraint_columns = columns_by_constraint[(schema, table, _constraint)]
        ordered_columns = tuple(column for _, column in sorted(constraint_columns, key=lambda item: item[0]))
        uniques_by_table[(schema, table)].append(ordered_columns)
    return {key: tuple(value) for key, value in uniques_by_table.items()}
