"""Application dbt build deployment declarations."""

from dataclasses import dataclass
from typing import Literal

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


__all__ = ["DBT_BUILD_SPECS", "DbtBuildDeployment", "DbtBuildSpec"]
