"""Queries for dbt-built exchange provider ingestion contracts."""

from collections.abc import Sequence

import structlog

from core.clients.lake import get_lake_client

log = structlog.get_logger(__name__)

INGESTION_UNIVERSE_SCHEMA = "silver"
INGESTION_UNIVERSE_TABLE = "int_exchange_provider_ingestion_universe"


def load_provider_exchange_codes(data_provider: str, fallback: Sequence[str] = ("US", "XETRA")) -> list[str]:
    """Load provider exchange/API codes approved for downstream ingestion flows."""
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
    rows = lake.query(
        f"""
        SELECT provider_exchange_code
        FROM {qualified}
        WHERE data_provider = ?
          AND is_enabled_for_ingestion
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
