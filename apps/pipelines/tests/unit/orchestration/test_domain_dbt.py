"""Tests for app dbt/build domain mappings."""

from config.domains import Domain
from orchestration.domain_dbt import DOMAIN_DBT_REGISTRY, DOMAIN_DBT_SPECS


def test_domain_dbt_specs_cover_ingestion_domains_in_order() -> None:
    """Dbt asset and build mappings should live in orchestration."""
    assert tuple(spec.domain for spec in DOMAIN_DBT_SPECS) == (
        Domain.EXCHANGE,
        Domain.EXCHANGE_SCHEDULE,
        Domain.INSTRUMENT,
        Domain.EOD_PRICE,
        Domain.FUNDAMENTAL,
    )
    assert set(DOMAIN_DBT_REGISTRY) == {spec.domain for spec in DOMAIN_DBT_SPECS}
    assert tuple(spec.asset_group for spec in DOMAIN_DBT_SPECS) == (
        "exchange",
        "exchange_schedule",
        "instrument",
        "price",
        "fundamental",
    )
    assert DOMAIN_DBT_REGISTRY[Domain.INSTRUMENT].post_ingestion_build == "instrument-build"
