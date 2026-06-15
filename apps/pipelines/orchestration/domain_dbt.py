"""App dbt/build mappings for pipeline domains."""

from dataclasses import dataclass
from typing import Literal

from config.domains import Domain

type DbtBuildDeployment = Literal[
    "ingestion-control-build",
    "exchange-build",
    "instrument-build",
    "price-build",
    "fundamental-build",
]


@dataclass(frozen=True, slots=True)
class DomainDbtSpec:
    """Dbt asset and post-ingestion build metadata for one domain."""

    domain: Domain
    asset_group: str
    select_needles: tuple[str, ...]
    post_ingestion_build: DbtBuildDeployment | None = None


DOMAIN_DBT_SPECS: tuple[DomainDbtSpec, ...] = (
    DomainDbtSpec(
        domain=Domain.EXCHANGE,
        asset_group="exchange",
        select_needles=("exchange", "provider_namespace"),
        post_ingestion_build="exchange-build",
    ),
    DomainDbtSpec(
        domain=Domain.EXCHANGE_SCHEDULE,
        asset_group="exchange_schedule",
        select_needles=("exchange_schedule", "schedule", "holiday"),
        post_ingestion_build="exchange-build",
    ),
    DomainDbtSpec(
        domain=Domain.INSTRUMENT,
        asset_group="instrument",
        select_needles=("instrument",),
        post_ingestion_build="instrument-build",
    ),
    DomainDbtSpec(
        domain=Domain.EOD_PRICE,
        asset_group="price",
        select_needles=("price", "eod"),
        post_ingestion_build="price-build",
    ),
    DomainDbtSpec(
        domain=Domain.FUNDAMENTAL,
        asset_group="fundamental",
        select_needles=("fundamental",),
        post_ingestion_build="fundamental-build",
    ),
)

DOMAIN_DBT_REGISTRY: dict[Domain, DomainDbtSpec] = {spec.domain: spec for spec in DOMAIN_DBT_SPECS}

__all__ = ["DOMAIN_DBT_REGISTRY", "DOMAIN_DBT_SPECS", "DbtBuildDeployment", "DomainDbtSpec"]
