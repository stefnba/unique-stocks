"""Runtime contracts for dbt-built ingestion-control views."""

from typing import Protocol

SILVER_SCHEMA = "silver"
INSTRUMENT_UNIVERSE_TABLE = "int_latest_instrument_universe"
FUNDAMENTAL_INGESTION_UNIVERSE_TABLE = "int_fundamental_ingestion_universe"
FUNDAMENTAL_DOCUMENT_COMPLETION_TABLE = "int_fundamental_document_completion"
EOD_PRICE_INSTRUMENT_DAY_COVERAGE_TABLE = "int_eod_price_instrument_day_coverage"
EOD_PRICE_EXCHANGE_DAY_STATUS_TABLE = "int_eod_price_exchange_day_status"
EOD_PRICE_EXCHANGE_TRADING_DAY_TABLE = "int_exchange_trading_day"
EOD_PRICE_BACKFILL_NO_DATA_COVERAGE_TABLE = "int_eod_price_backfill_no_data_coverage"
EOD_PRICE_BACKFILL_TERMINAL_COVERAGE_TABLE = "int_eod_price_backfill_terminal_coverage"


class SilverIngestionContractError(RuntimeError):
    """Raised when a required dbt-built ingestion-control contract is unavailable."""


class InstrumentUniverseContractError(SilverIngestionContractError):
    """Raised when the dbt-built instrument universe contract is unavailable."""


class InstrumentUniverseLake(Protocol):
    """Minimal lake interface needed to resolve the instrument universe."""

    def table_exists(self, schema: str, table: str) -> bool:
        """Return True when a table or view exists."""
        ...

    def qualified_name(self, schema: str, table: str) -> str:
        """Return a fully-qualified table name."""
        ...


def require_silver_ingestion_model(
    lake: InstrumentUniverseLake,
    table_name: str,
    *,
    build_hint: str,
) -> str:
    """Return a required Silver ingestion-control relation or raise a contract error."""
    if not lake.table_exists(SILVER_SCHEMA, table_name):
        msg = f"Missing required dbt model {SILVER_SCHEMA}.{table_name}; run {build_hint} before ingestion selection."
        raise SilverIngestionContractError(msg)
    return lake.qualified_name(SILVER_SCHEMA, table_name)


def require_instrument_universe(lake: InstrumentUniverseLake) -> str:
    """Return the qualified Silver instrument universe or raise a contract error."""
    if not lake.table_exists(SILVER_SCHEMA, INSTRUMENT_UNIVERSE_TABLE):
        msg = (
            f"Missing required dbt model {SILVER_SCHEMA}.{INSTRUMENT_UNIVERSE_TABLE}; "
            "run instrument-build before auto-selecting instruments for ingestion."
        )
        raise InstrumentUniverseContractError(msg)
    return lake.qualified_name(SILVER_SCHEMA, INSTRUMENT_UNIVERSE_TABLE)


__all__ = [
    "EOD_PRICE_BACKFILL_NO_DATA_COVERAGE_TABLE",
    "EOD_PRICE_BACKFILL_TERMINAL_COVERAGE_TABLE",
    "EOD_PRICE_EXCHANGE_DAY_STATUS_TABLE",
    "EOD_PRICE_EXCHANGE_TRADING_DAY_TABLE",
    "EOD_PRICE_INSTRUMENT_DAY_COVERAGE_TABLE",
    "FUNDAMENTAL_DOCUMENT_COMPLETION_TABLE",
    "FUNDAMENTAL_INGESTION_UNIVERSE_TABLE",
    "INSTRUMENT_UNIVERSE_TABLE",
    "InstrumentUniverseContractError",
    "SILVER_SCHEMA",
    "SilverIngestionContractError",
    "require_instrument_universe",
    "require_silver_ingestion_model",
]
