"""Queries for dbt-built provider-code ingestion contracts.

The exchange dbt models publish the operational list of EODHD request codes in
``silver.int_exchange_provider_ingestion_universe``. Despite the historical
``provider_exchange_code`` name, these values are provider endpoint/symbol
namespace codes, not always real exchanges. Examples include ``US`` and
``XETRA`` for exchange-like namespaces and ``INDX`` for the curated global
index namespace.
"""

from typing import Literal

import structlog

from core.clients.lake import get_lake_client

log = structlog.get_logger(__name__)

INGESTION_UNIVERSE_SCHEMA = "silver"
INGESTION_UNIVERSE_TABLE = "int_exchange_provider_ingestion_universe"

type ProviderCodePurpose = Literal["ingestion", "instrument", "eod_price", "fundamental"]

_PURPOSE_FILTER_COLUMNS: dict[ProviderCodePurpose, str] = {
    "ingestion": "is_enabled_for_ingestion",
    "instrument": "is_enabled_for_instrument",
    "eod_price": "is_enabled_for_eod_price",
    "fundamental": "is_enabled_for_fundamental",
}


class ProviderUniverseContractError(RuntimeError):
    """Raised when the dbt-built provider universe contract is unavailable."""


def load_provider_exchange_codes(
    data_provider: str,
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
    - ``"fundamental"`` uses ``is_enabled_for_fundamental`` and is used by the
      fundamentals flow before it auto-selects split provider instruments from
      the Silver ingestion universe.
    - ``"ingestion"`` uses the aggregate ``is_enabled_for_ingestion`` flag and
      is kept as a backward-compatible default for generic callers.

    Fundamental ingestion still works with explicit split provider instruments
    regardless of this universe. The ``fundamental`` purpose applies only to
    automatic instrument discovery from
    ``silver.int_fundamental_ingestion_universe``.
    A provider namespace must still be enabled for instrument ingestion first,
    otherwise its instruments will not exist in the Silver instrument universe
    for fundamentals to discover.

    The Silver exchange view is a required dbt contract for provider-code
    discovery. Flows that auto-select instruments also require instrument
    refresh/build and fundamental-build so the Silver fundamentals ingestion universe
    is populated.
    Pass ``provider_exchange_codes`` to the flow itself when you want to restrict
    a manual run to one or two provider namespaces.
    """
    lake = get_lake_client()
    if not lake.table_exists(INGESTION_UNIVERSE_SCHEMA, INGESTION_UNIVERSE_TABLE):
        msg = (
            f"Missing required dbt model "
            f"{INGESTION_UNIVERSE_SCHEMA}.{INGESTION_UNIVERSE_TABLE}; "
            "run exchange-build before loading provider exchange codes."
        )
        raise ProviderUniverseContractError(msg)

    qualified = lake.qualified_name(INGESTION_UNIVERSE_SCHEMA, INGESTION_UNIVERSE_TABLE)
    filter_column = _PURPOSE_FILTER_COLUMNS[purpose]
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
        msg = (
            f"No provider exchange codes enabled for data_provider={data_provider!r}, "
            f"purpose={purpose!r} in {INGESTION_UNIVERSE_SCHEMA}.{INGESTION_UNIVERSE_TABLE}."
        )
        raise ProviderUniverseContractError(msg)

    log.info("exchange.provider_codes_loaded", data_provider=data_provider, purpose=purpose, count=len(codes))
    return codes
