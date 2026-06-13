"""Queries for dbt-built provider-code ingestion contracts.

The exchange dbt models publish the operational list of EODHD request codes in
``silver.int_exchange_provider_ingestion_universe``. Despite the historical
``provider_exchange_code`` name, these values are provider endpoint/symbol
namespace codes, not always real exchanges. Examples include ``US`` and
``XETRA`` for exchange-like namespaces and ``INDX`` for the curated global
index namespace. The Silver model keeps provider catalog visibility broad, but
runtime eligibility is controlled by the dbt ``provider_namespace_policy`` seed.
"""

from collections.abc import Sequence
from typing import Literal

import structlog

from core.lake import DataLakeClient, get_lake_client

log = structlog.get_logger(__name__)

INGESTION_UNIVERSE_SCHEMA = "silver"
INGESTION_UNIVERSE_TABLE = "int_exchange_provider_ingestion_universe"
PROVIDER_COVERAGE_TABLE = "int_exchange_provider_coverage"

type ProviderCodePurpose = Literal["ingestion", "instrument", "eod_price", "eod_backfill", "fundamental"]

_PURPOSE_FILTER_COLUMNS: dict[ProviderCodePurpose, str] = {
    "ingestion": "is_enabled_for_ingestion",
    "instrument": "is_enabled_for_instrument",
    "eod_price": "is_enabled_for_eod_price",
    "eod_backfill": "is_enabled_for_eod_backfill",
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
    - ``"eod_backfill"`` uses ``is_enabled_for_eod_backfill`` and is used by
      the per-instrument historical EOD backfill flow.
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


def load_provider_schedule_exchange_codes(
    data_provider: str,
    *,
    available_schedule_codes: Sequence[str],
    purpose: ProviderCodePurpose = "eod_price",
) -> list[str]:
    """Return live provider schedule codes that overlap the operational universe.

    EODHD has two related code systems:

    - provider exchange/symbol namespace codes used by instrument and price endpoints;
    - provider schedule endpoint codes used by ``/v2/exchange-details``.

    Some markets use the same value in both places, while others differ
    (``XETRA`` for symbols but ``XETR`` for schedules). This reader keeps the
    provider's live schedule-code list as the availability source and joins it
    against our dbt-built operational universe plus MIC/operating-MIC candidates.
    """
    normalized_available_codes = _normalize_codes(available_schedule_codes)
    if not normalized_available_codes:
        msg = "Provider returned no schedule exchange codes; cannot resolve operational schedule scope."
        raise ProviderUniverseContractError(msg)

    lake = get_lake_client()
    _require_silver_contract(lake, INGESTION_UNIVERSE_TABLE)
    _require_silver_contract(lake, PROVIDER_COVERAGE_TABLE)

    ingestion_universe = lake.qualified_name(INGESTION_UNIVERSE_SCHEMA, INGESTION_UNIVERSE_TABLE)
    provider_coverage = lake.qualified_name(INGESTION_UNIVERSE_SCHEMA, PROVIDER_COVERAGE_TABLE)
    filter_column = _PURPOSE_FILTER_COLUMNS[purpose]
    value_placeholders = ", ".join(["(?)"] * len(normalized_available_codes))
    rows = lake.query(
        f"""
        WITH available_schedule_code(provider_schedule_exchange_code) AS (
            VALUES {value_placeholders}
        ),

        provider_universe AS (
            SELECT
                data_provider,
                provider_exchange_code,
                COALESCE(policy_priority, 999999) AS policy_priority
            FROM {ingestion_universe}
            WHERE data_provider = ?
              AND {filter_column}
        ),

        candidate_schedule_code AS (
            SELECT
                data_provider,
                provider_exchange_code,
                provider_exchange_code AS provider_schedule_exchange_code,
                policy_priority
            FROM provider_universe

            UNION ALL

            SELECT
                provider_universe.data_provider,
                provider_universe.provider_exchange_code,
                coverage.mic AS provider_schedule_exchange_code,
                provider_universe.policy_priority
            FROM provider_universe
            INNER JOIN {provider_coverage} AS coverage
                ON provider_universe.data_provider = coverage.data_provider
                AND provider_universe.provider_exchange_code = coverage.provider_exchange_code
            WHERE coverage.mic IS NOT NULL

            UNION ALL

            SELECT
                provider_universe.data_provider,
                provider_universe.provider_exchange_code,
                coverage.operating_mic AS provider_schedule_exchange_code,
                provider_universe.policy_priority
            FROM provider_universe
            INNER JOIN {provider_coverage} AS coverage
                ON provider_universe.data_provider = coverage.data_provider
                AND provider_universe.provider_exchange_code = coverage.provider_exchange_code
            WHERE coverage.operating_mic IS NOT NULL
        )

        SELECT
            candidate_schedule_code.provider_schedule_exchange_code,
            MIN(candidate_schedule_code.policy_priority) AS policy_priority
        FROM candidate_schedule_code
        INNER JOIN available_schedule_code
            ON candidate_schedule_code.provider_schedule_exchange_code =
                available_schedule_code.provider_schedule_exchange_code
        GROUP BY 1
        ORDER BY policy_priority, provider_schedule_exchange_code
        """,
        [*normalized_available_codes, data_provider],
    )
    codes = [str(row["provider_schedule_exchange_code"]) for row in rows]
    if not codes:
        msg = (
            f"No provider schedule exchange codes overlap data_provider={data_provider!r}, "
            f"purpose={purpose!r} and the provider's live schedule-code list."
        )
        raise ProviderUniverseContractError(msg)

    log.info(
        "exchange.provider_schedule_codes_loaded",
        data_provider=data_provider,
        purpose=purpose,
        available_count=len(normalized_available_codes),
        count=len(codes),
    )
    return codes


def _require_silver_contract(lake: DataLakeClient, table_name: str) -> None:
    """Raise a clear error when a required Silver exchange contract is missing."""
    if not lake.table_exists(INGESTION_UNIVERSE_SCHEMA, table_name):
        msg = (
            f"Missing required dbt model {INGESTION_UNIVERSE_SCHEMA}.{table_name}; "
            "run exchange-build before loading provider exchange codes."
        )
        raise ProviderUniverseContractError(msg)


def _normalize_codes(codes: Sequence[str]) -> list[str]:
    """Return stable uppercase provider codes with blanks removed."""
    return sorted({code.strip().upper() for code in codes if code and code.strip()})
