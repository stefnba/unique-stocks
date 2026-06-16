"""Tests for app post-ingestion dbt build mapping."""

from config.domains import Domain
from orchestration.post_ingestion import POST_INGESTION_BUILDS_BY_DOMAIN, post_ingestion_build_for_domain


def test_post_ingestion_builds_cover_ingestion_domains() -> None:
    """The app should expose one post-ingestion dbt build for each ingestion domain."""
    assert POST_INGESTION_BUILDS_BY_DOMAIN == {
        Domain.EXCHANGE: "exchange-build",
        Domain.EXCHANGE_SCHEDULE: "exchange-build",
        Domain.INSTRUMENT: "instrument-build",
        Domain.EOD_PRICE: "price-build",
        Domain.FUNDAMENTAL: "fundamental-build",
    }
    assert post_ingestion_build_for_domain(Domain.INSTRUMENT) == "instrument-build"
