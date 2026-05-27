"""Tests for the app-level lake schema registry."""

from pathlib import Path

from core.ingestion import LandingDomain
from core.schema.ddl import render_init_lake_sql
from lake.schema import ALL_TABLES, BRONZE_TABLES


def test_lake_schema_tables_have_unique_names() -> None:
    """Registered table specs should not define duplicate physical tables."""
    qualified_names = [table.qualified_name() for table in ALL_TABLES]
    assert len(qualified_names) == len(set(qualified_names))


def test_lake_schema_registers_current_bronze_tables() -> None:
    """Current Bronze table specs are all registered for generated DDL."""
    assert tuple(table.qualified_name() for table in ALL_TABLES) == (
        "bronze.eod_price",
        "bronze.exchange",
        "bronze.exchange_schedule",
        "bronze.exchange_holiday",
        "bronze.instrument",
        "pipeline.runs",
        "pipeline.run_units",
        "pipeline.landing_objects",
        "pipeline.rejections",
        "pipeline.dbt_invocations",
        "pipeline.dbt_node_results",
    )


def test_lake_schema_uses_singular_domain_names() -> None:
    """Pipeline-owned lake and package names should use singular domain terms."""
    assert tuple(table.table_name for table in BRONZE_TABLES) == (
        "eod_price",
        "exchange",
        "exchange_schedule",
        "exchange_holiday",
        "instrument",
    )
    assert tuple(domain.value for domain in LandingDomain) == (
        "exchange",
        "exchange_schedule",
        "eod_price",
        "fundamental",
        "instrument",
    )

    domain_root = Path(__file__).parents[2] / "domains"
    old_plural_packages = (
        "eod_price" + "s",
        "exchange" + "s",
        "exchange_schedule" + "s",
        "instrument" + "s",
        "fundamental" + "s",
    )
    assert [name for name in old_plural_packages if (domain_root / name).exists()] == []


def test_generated_init_lake_sql_is_current() -> None:
    """The checked-in init SQL should match the Python table specs."""
    root = Path(__file__).parents[2]
    assert (root / "scripts/init_lake.sql").read_text() == render_init_lake_sql(ALL_TABLES)
