"""Tests for the app-level lake schema registry."""

from pathlib import Path

from core.ingestion import LandingDomain
from core.lake.schema.ddl import render_greenfield_schema_sql
from lake.schema import ALL_TABLES, BRONZE_TABLES


def test_lake_schema_tables_have_unique_names() -> None:
    """Registered table specs should not define duplicate physical tables."""
    qualified_names = [table.qualified_name() for table in ALL_TABLES]
    assert len(qualified_names) == len(set(qualified_names))


def test_lake_schema_registers_current_bronze_tables() -> None:
    """Current Bronze table specs are all registered for generated DDL."""
    assert tuple(table.qualified_name() for table in ALL_TABLES) == (
        "bronze.eod_price",
        "bronze.exchange_catalog",
        "bronze.exchange_mic_registry",
        "bronze.exchange_schedule",
        "bronze.exchange_holiday",
        "bronze.instrument",
        "bronze.fundamental_document",
        "bronze.fundamental_stock_identity",
        "bronze.fundamental_statement_fact",
        "bronze.fundamental_stock_earnings_fact",
        "bronze.fundamental_stock_shares_stats",
        "bronze.fundamental_stock_outstanding_shares",
        "bronze.fundamental_stock_holder",
        "bronze.fundamental_stock_insider_transaction",
        "bronze.fundamental_stock_splits_dividends",
        "bronze.fundamental_stock_dividend_count",
        "bronze.fundamental_stock_metric_fact",
        "bronze.fundamental_stock_esg_activity",
        "bronze.fundamental_etf_identity",
        "bronze.fundamental_mutual_fund_identity",
        "bronze.fundamental_index_identity",
        "bronze.fundamental_etf_holding",
        "bronze.fundamental_mutual_fund_holding",
        "bronze.fundamental_fund_metric_fact",
        "bronze.fundamental_index_component",
        "bronze.fundamental_index_historical_component",
        "pipeline.runs",
        "pipeline.run_units",
        "pipeline.ingestion_coverage",
        "pipeline.landing_objects",
        "pipeline.rejections",
        "pipeline.dbt_invocations",
        "pipeline.dbt_node_results",
    )


def test_lake_schema_uses_singular_domain_names() -> None:
    """Pipeline-owned lake and package names should use singular domain terms."""
    assert tuple(table.table_name for table in BRONZE_TABLES) == (
        "eod_price",
        "exchange_catalog",
        "exchange_mic_registry",
        "exchange_schedule",
        "exchange_holiday",
        "instrument",
        "fundamental_document",
        "fundamental_stock_identity",
        "fundamental_statement_fact",
        "fundamental_stock_earnings_fact",
        "fundamental_stock_shares_stats",
        "fundamental_stock_outstanding_shares",
        "fundamental_stock_holder",
        "fundamental_stock_insider_transaction",
        "fundamental_stock_splits_dividends",
        "fundamental_stock_dividend_count",
        "fundamental_stock_metric_fact",
        "fundamental_stock_esg_activity",
        "fundamental_etf_identity",
        "fundamental_mutual_fund_identity",
        "fundamental_index_identity",
        "fundamental_etf_holding",
        "fundamental_mutual_fund_holding",
        "fundamental_fund_metric_fact",
        "fundamental_index_component",
        "fundamental_index_historical_component",
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


def test_initial_schema_migration_matches_current_table_specs() -> None:
    """The first migration should match the current greenfield table specs."""
    root = Path(__file__).parents[2]
    migration_sql = (root / "lake/migrations/20260603000000_initial_schema.sql").read_text()
    assert _without_header(migration_sql) == _without_header(render_greenfield_schema_sql(ALL_TABLES))


def _without_header(sql: str) -> str:
    return "\n\n".join(sql.split("\n\n")[1:])
