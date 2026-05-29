"""Queries for dbt-built provider-code ingestion contracts.

The exchange dbt models publish the operational list of EODHD request codes in
``silver.int_exchange_provider_ingestion_universe``. Despite the historical
``provider_exchange_code`` name, these values are provider endpoint/symbol
namespace codes, not always real exchanges. Examples include ``US`` and
``XETRA`` for exchange-like namespaces and ``INDX`` for the curated global
index namespace.
"""

from collections.abc import Sequence
from typing import Any, Literal

import structlog

from core.clients.lake import get_lake_client

log = structlog.get_logger(__name__)

INGESTION_UNIVERSE_SCHEMA = "silver"
INGESTION_UNIVERSE_TABLE = "int_exchange_provider_ingestion_universe"

type ProviderCodePurpose = Literal["ingestion", "instrument", "eod_price"]

_PURPOSE_FILTER_COLUMNS: dict[ProviderCodePurpose, str] = {
    "ingestion": "is_enabled_for_ingestion",
    "instrument": "is_enabled_for_instrument",
    "eod_price": "is_enabled_for_eod_price",
}


def load_provider_exchange_codes(
    data_provider: str,
    fallback: Sequence[str] = ("US", "XETRA"),
    *,
    purpose: ProviderCodePurpose = "ingestion",
) -> list[str]:
    """Load provider endpoint/symbol namespace codes approved for ingestion.

    This is the runtime bridge from dbt policy to Python ingestion flows. It
    reads ``silver.int_exchange_provider_ingestion_universe`` and returns the
    provider request codes that a flow should pass into provider endpoints such
    as EODHD ``/exchange-symbol-list/{code}`` or
    ``/eod-bulk-last-day/{code}``.

    The ``purpose`` argument chooses which Silver eligibility flag to apply:

    - ``"instrument"`` uses ``is_enabled_for_instrument`` and is used by the
      instrument reference flow.
    - ``"eod_price"`` uses ``is_enabled_for_eod_price`` and is used by the
      bulk EOD price flow.
    - ``"ingestion"`` uses the aggregate ``is_enabled_for_ingestion`` flag and
      is kept as a backward-compatible default for generic callers.

    Fundamental ingestion does not call this helper today. It either receives
    explicit exchange-qualified tickers, or derives stock-like tickers from the
    latest ``bronze.instrument`` snapshot. This universe still affects
    fundamentals indirectly: if a provider namespace is not enabled for
    instrument ingestion, its instruments will not be available for later
    fundamentals selection from Bronze.

    When the Silver table is missing, empty, or still on an older contract that
    lacks a purpose-specific flag, the function falls back conservatively:
    missing/empty tables return the caller-provided ``fallback`` codes, and old
    contracts use ``is_enabled_for_ingestion``.
    """
    lake = get_lake_client()
    if not lake.table_exists(INGESTION_UNIVERSE_SCHEMA, INGESTION_UNIVERSE_TABLE):
        fallback_codes = list(fallback)
        log.warning(
            "exchange.provider_codes_fallback",
            reason="ingestion_universe_missing",
            data_provider=data_provider,
            fallback_count=len(fallback_codes),
        )
        return fallback_codes

    qualified = lake.qualified_name(INGESTION_UNIVERSE_SCHEMA, INGESTION_UNIVERSE_TABLE)
    filter_column = _purpose_filter_column(lake, purpose)
    rows = lake.query(
        f"""
        SELECT provider_exchange_code
        FROM {qualified}
        WHERE data_provider = ?
          AND {filter_column}
        ORDER BY provider_exchange_code
        """,
        [data_provider],
    )
    codes = [str(row["provider_exchange_code"]) for row in rows]
    if not codes:
        fallback_codes = list(fallback)
        log.warning(
            "exchange.provider_codes_fallback",
            reason="no_enabled_provider_codes",
            data_provider=data_provider,
            fallback_count=len(fallback_codes),
        )
        return fallback_codes

    log.info("exchange.provider_codes_loaded", data_provider=data_provider, count=len(codes))
    return codes


def _purpose_filter_column(lake: Any, purpose: ProviderCodePurpose) -> str:
    """Return the Silver flag column for a provider-code purpose."""
    column = _PURPOSE_FILTER_COLUMNS[purpose]
    if column == _PURPOSE_FILTER_COLUMNS["ingestion"]:
        return column

    row = lake.query_one(
        """
        SELECT 1
        FROM information_schema.columns
        WHERE table_schema = ?
          AND table_name = ?
          AND column_name = ?
        """,
        [INGESTION_UNIVERSE_SCHEMA, INGESTION_UNIVERSE_TABLE, column],
    )
    if row is not None:
        return column

    log.warning(
        "exchange.provider_codes_purpose_column_missing",
        requested_purpose=purpose,
        missing_column=column,
        fallback_column=_PURPOSE_FILTER_COLUMNS["ingestion"],
    )
    return _PURPOSE_FILTER_COLUMNS["ingestion"]
