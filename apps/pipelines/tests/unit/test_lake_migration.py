"""Tests for lake schema migration helpers."""

from datetime import UTC, datetime
from pathlib import Path
from typing import Annotated, ClassVar

import duckdb
import pytest
from pydantic import BaseModel

import core.lake.migration.generate as generate_module
import core.lake.migration.migrate as migrate_module
from core.lake.migration.diff import LakeSchemaDiff, diff_lake_schema
from core.lake.migration.files import (
    MigrationFile,
    empty_migration_sql,
    list_migration_files,
    migration_filename,
    slugify_migration_name,
    timestamp_version,
    write_migration_file,
)
from core.lake.migration.generate import load_table_specs
from core.lake.migration.introspection import (
    ActualColumn,
    ActualLakeSchema,
    ActualTable,
    desired_lake_schema_from_tables,
    inspect_lake_schema,
)
from core.lake.migration.runner import (
    MigrationChecksumError,
    apply_pending_migrations,
    ensure_migration_table,
    plan_migrations,
)
from core.lake.schema import VARCHAR, SchemaName, SqlColumn, TableModel


class DemoRow(BaseModel):
    """Small row model for migration tests."""

    id: int
    note: str | None = None


class DemoTable(TableModel):
    """Small table model for migration tests."""

    schema_name: ClassVar[SchemaName] = "pipeline"
    table_name = "demo"
    row_model = DemoRow
    unique_columns = ("id",)
    idempotency_columns = ()


class DefaultedRow(BaseModel):
    """Row model with a defaulted non-null column."""

    id: int
    status: Annotated[str, SqlColumn(VARCHAR, default="'new'")]


class DefaultedTable(TableModel):
    """Table model with a missing defaulted non-null column."""

    schema_name: ClassVar[SchemaName] = "pipeline"
    table_name = "defaulted"
    row_model = DefaultedRow
    unique_columns = ("id",)
    idempotency_columns = ()


CLI_TABLES = (DemoTable,)


def test_desired_schema_uses_registered_tables_and_default_schemas() -> None:
    """Desired schema comes from table specs plus configured default schemas."""
    desired = desired_lake_schema_from_tables((DemoTable,), default_schemas=("pipeline", "lake"))

    assert desired.schemas == frozenset({"pipeline", "lake"})
    table = desired.tables[("pipeline", "demo")]
    assert table.qualified_name == "pipeline.demo"
    assert tuple(column.name for column in table.columns) == ("id", "note")
    assert table.unique_columns == ("id",)


def test_load_table_specs_imports_module_attribute() -> None:
    """CLI table refs load a module-level tuple of table specs."""
    assert load_table_specs(f"{__name__}:CLI_TABLES") == (DemoTable,)


def test_generate_skips_warning_only_diff_by_default(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Warning-only diffs should not create no-op migration files by default."""

    class FakeLakeClient:
        connection: object = object()

        def __init__(self, *, read_only: bool = False) -> None:
            self.read_only = read_only

        def close(self) -> None:
            return None

    monkeypatch.setattr(generate_module, "DataLakeClient", FakeLakeClient)
    monkeypatch.setattr(generate_module, "inspect_lake_schema", lambda _conn, *, schemas: object())
    monkeypatch.setattr(
        generate_module,
        "diff_lake_schema",
        lambda _actual, _desired: LakeSchemaDiff((), ("manual change required",)),
    )

    generate_module.main(
        [
            "--tables",
            f"{__name__}:CLI_TABLES",
            "--migrations-dir",
            str(tmp_path),
        ]
    )

    assert list(tmp_path.iterdir()) == []
    assert "WARNING: manual change required" in capsys.readouterr().out


def test_inspect_lake_schema_reads_columns_and_unique_constraints() -> None:
    """Information-schema introspection captures existing columns and uniques."""
    conn = duckdb.connect(":memory:")
    conn.execute("CREATE SCHEMA pipeline")
    conn.execute("CREATE TABLE pipeline.demo (id BIGINT NOT NULL, code VARCHAR, UNIQUE (id, code))")

    actual = inspect_lake_schema(conn, schemas={"pipeline"})

    table = actual.tables[("pipeline", "demo")]
    assert actual.schemas == frozenset({"pipeline"})
    assert table.columns["id"].sql_type == "BIGINT"
    assert table.columns["id"].nullable is False
    assert table.columns["code"].nullable is True
    assert table.unique_constraints == (("id", "code"),)


def test_diff_generates_missing_schema_and_table_sql() -> None:
    """Missing schemas and tables produce executable SQL."""
    desired = desired_lake_schema_from_tables((DemoTable,), default_schemas=("pipeline", "lake"))
    diff = diff_lake_schema(ActualLakeSchema(schemas=frozenset(), tables={}), desired)

    assert "CREATE SCHEMA IF NOT EXISTS lake" in diff.statements
    assert "CREATE TABLE IF NOT EXISTS pipeline.demo" in "\n".join(diff.statements)
    assert diff.warnings == ()


def test_diff_generates_missing_nullable_column_sql() -> None:
    """Missing nullable columns are safe to add automatically."""
    actual = ActualLakeSchema(
        schemas=frozenset({"pipeline"}),
        tables={
            ("pipeline", "demo"): ActualTable(
                schema="pipeline",
                name="demo",
                columns={
                    "id": ActualColumn("id", "BIGINT", False, None, 1),
                },
                unique_constraints=(("id",),),
            )
        },
    )
    desired = desired_lake_schema_from_tables((DemoTable,), default_schemas=("pipeline",))

    diff = diff_lake_schema(actual, desired)

    assert diff.statements == ("ALTER TABLE pipeline.demo ADD COLUMN IF NOT EXISTS note VARCHAR",)
    assert diff.warnings == ()


def test_diff_generates_defaulted_non_null_column_sql() -> None:
    """Missing non-null columns with defaults are added then tightened."""
    actual = ActualLakeSchema(
        schemas=frozenset({"pipeline"}),
        tables={
            ("pipeline", "defaulted"): ActualTable(
                schema="pipeline",
                name="defaulted",
                columns={
                    "id": ActualColumn("id", "BIGINT", False, None, 1),
                },
                unique_constraints=(("id",),),
            )
        },
    )
    desired = desired_lake_schema_from_tables((DefaultedTable,), default_schemas=("pipeline",))

    diff = diff_lake_schema(actual, desired)

    assert diff.statements == (
        "ALTER TABLE pipeline.defaulted ADD COLUMN IF NOT EXISTS status VARCHAR DEFAULT 'new'",
        "ALTER TABLE pipeline.defaulted ALTER COLUMN status SET NOT NULL",
    )
    assert diff.warnings == ()


def test_diff_comments_on_risky_changes() -> None:
    """Type, nullability, extra-column, and unique drift are warnings."""
    actual = ActualLakeSchema(
        schemas=frozenset({"pipeline"}),
        tables={
            ("pipeline", "demo"): ActualTable(
                schema="pipeline",
                name="demo",
                columns={
                    "id": ActualColumn("id", "INTEGER", True, None, 1),
                    "old_name": ActualColumn("old_name", "VARCHAR", True, None, 2),
                },
                unique_constraints=(),
            )
        },
    )
    desired = desired_lake_schema_from_tables((DemoTable,), default_schemas=("pipeline",))

    diff = diff_lake_schema(actual, desired)
    sql = diff.to_sql()

    assert "type differs" in sql
    assert "nullability differs" in sql
    assert "old_name" in sql
    assert "unique constraint" in sql


def test_diff_allows_extra_unique_when_desired_unique_exists() -> None:
    """Historical extra unique constraints should not warn when the desired unique is present."""
    actual = ActualLakeSchema(
        schemas=frozenset({"pipeline"}),
        tables={
            ("pipeline", "demo"): ActualTable(
                schema="pipeline",
                name="demo",
                columns={
                    "id": ActualColumn("id", "BIGINT", False, None, 1),
                    "note": ActualColumn("note", "VARCHAR", True, None, 2),
                },
                unique_constraints=(("id",), ("id", "note")),
            )
        },
    )
    desired = desired_lake_schema_from_tables((DemoTable,), default_schemas=("pipeline",))

    diff = diff_lake_schema(actual, desired)

    assert diff.warnings == ()


def test_migration_file_naming_and_listing(tmp_path: Path) -> None:
    """Migration files use timestamped names and stable checksums."""
    version = timestamp_version(datetime(2026, 6, 3, 16, 42, tzinfo=UTC))

    assert version == "20260603164200"
    assert slugify_migration_name("Add DBT node timing") == "add_dbt_node_timing"
    assert migration_filename(version=version, name="Add DBT node timing") == "20260603164200_add_dbt_node_timing.sql"

    path = write_migration_file(
        tmp_path,
        sql="CREATE SCHEMA IF NOT EXISTS lake;\n",
        name="Add DBT node timing",
        version=version,
    )
    files = list_migration_files(tmp_path)

    assert path.name == "20260603164200_add_dbt_node_timing.sql"
    assert len(files) == 1
    assert files[0].version == version
    assert files[0].name == "add_dbt_node_timing"
    assert files[0].checksum
    assert "Manual lake schema migration" in empty_migration_sql("manual rebuild")


def test_runner_applies_skips_and_detects_checksum_drift(tmp_path: Path) -> None:
    """Runner applies pending files, skips matching applied files, and rejects drift."""
    migration = tmp_path / "20260603164200_create_demo.sql"
    migration.write_text(
        "CREATE SCHEMA IF NOT EXISTS pipeline;\nCREATE TABLE IF NOT EXISTS pipeline.demo (id INTEGER);\n"
    )
    conn = duckdb.connect(":memory:")

    first = apply_pending_migrations(conn, migrations_dir=tmp_path)
    second = apply_pending_migrations(conn, migrations_dir=tmp_path)

    assert [item.version for item in first.applied] == ["20260603164200"]
    assert second.applied == ()
    assert [item.version for item in second.skipped] == ["20260603164200"]
    assert conn.execute("SELECT COUNT(*) FROM lake.schema_migration").fetchone() == (1,)

    migration.write_text("CREATE SCHEMA IF NOT EXISTS changed;\n")
    with pytest.raises(MigrationChecksumError):
        apply_pending_migrations(conn, migrations_dir=tmp_path)


def test_plan_migrations_is_non_mutating_when_tracking_table_is_missing(tmp_path: Path) -> None:
    """Dry-run planning should report pending migrations without creating tracking tables."""
    migration = tmp_path / "20260603164200_create_demo.sql"
    migration.write_text("CREATE SCHEMA IF NOT EXISTS pipeline;\n")
    conn = duckdb.connect(":memory:")

    plan = plan_migrations(conn, migrations_dir=tmp_path)

    assert [item.version for item in plan.pending] == ["20260603164200"]
    assert plan.skipped == ()
    row = conn.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_schema = 'lake' AND table_name = 'schema_migration'"
    ).fetchone()
    assert row is None


def test_print_plan_lists_migration_files(capsys: pytest.CaptureFixture[str], tmp_path: Path) -> None:
    """Dry-run status output should list the migration files behind the counts."""
    pending = MigrationFile(
        version="20260603164200",
        name="add_foo",
        path=tmp_path / "20260603164200_add_foo.sql",
        checksum="abc",
    )
    skipped = MigrationFile(
        version="20260604120000",
        name="add_bar",
        path=tmp_path / "20260604120000_add_bar.sql",
        checksum="def",
    )

    migrate_module._print_plan(pending=(pending,), skipped=(skipped,))

    output = capsys.readouterr().out
    assert "Pending lake migration(s): 1" in output
    assert "20260603164200 add_foo (20260603164200_add_foo.sql)" in output
    assert "Already applied lake migration(s): 1" in output
    assert "20260604120000 add_bar (20260604120000_add_bar.sql)" in output


def test_runner_records_only_successful_migrations(tmp_path: Path) -> None:
    """A failed migration is rolled back and not recorded."""
    migration = tmp_path / "20260603164200_bad.sql"
    migration.write_text("ALTER TABLE missing_table ADD COLUMN id INTEGER;\n")
    conn = duckdb.connect(":memory:")
    ensure_migration_table(conn)

    with pytest.raises(duckdb.Error):
        apply_pending_migrations(conn, migrations_dir=tmp_path)

    assert conn.execute("SELECT COUNT(*) FROM lake.schema_migration").fetchone() == (0,)
