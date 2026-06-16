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
class DbtBuildSpec:
    """Runtime dbt selector metadata for one build deployment."""

    deployment: DbtBuildDeployment
    select: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class DomainDbtSpec:
    """Dbt asset and post-ingestion build metadata for one domain."""

    domain: Domain
    asset_group: str
    select_needles: tuple[str, ...]
    post_ingestion_build: DbtBuildDeployment | None = None


DBT_BUILD_SPECS: dict[DbtBuildDeployment, DbtBuildSpec] = {
    "ingestion-control-build": DbtBuildSpec(
        deployment="ingestion-control-build",
        select=("+tag:ingestion_control",),
    ),
    "exchange-build": DbtBuildSpec(
        deployment="exchange-build",
        select=(
            "provider_namespace_policy",
            "eodhd_provider_namespaces",
            "path:models/staging/exchange",
            "path:models/staging/exchange_schedule",
            "+path:models/intermediate/exchange",
            "+path:models/marts/exchange",
        ),
    ),
    "instrument-build": DbtBuildSpec(
        deployment="instrument-build",
        select=(
            "path:models/staging/instrument",
            "path:models/staging/fundamental",
            "+path:models/intermediate/instrument",
            "+path:models/intermediate/fundamental",
            "+path:models/marts/instrument",
        ),
    ),
    "price-build": DbtBuildSpec(
        deployment="price-build",
        select=(
            "path:models/staging/pipeline",
            "path:models/staging/price",
            "+path:models/intermediate/price",
            "+path:models/marts/price",
        ),
    ),
    "fundamental-build": DbtBuildSpec(
        deployment="fundamental-build",
        select=(
            "path:models/staging/fundamental",
            "+path:models/intermediate/fundamental",
            "+path:models/marts/fundamental",
            "+path:models/marts/instrument",
        ),
    ),
}


DOMAIN_DBT_SPECS: tuple[DomainDbtSpec, ...] = (
    DomainDbtSpec(
        domain=Domain.EXCHANGE,
        asset_group="exchange",
        select_needles=("exchange", "provider_namespace_policy", "eodhd_provider_namespaces"),
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
        select_needles=("price", "eod", "eod_price", "stg_eod_price", "fct_daily_price"),
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

__all__ = [
    "DBT_BUILD_SPECS",
    "DOMAIN_DBT_REGISTRY",
    "DOMAIN_DBT_SPECS",
    "DbtBuildDeployment",
    "DbtBuildSpec",
    "DomainDbtSpec",
]
