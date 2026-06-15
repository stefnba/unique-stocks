"""Application Prefect limit definitions."""

from config.settings import get_settings
from core.orchestration.limits import define_limits
from providers.registry import PROVIDER_HTTP_CLIENT_REGISTRY

PREFECT_LIMITS = define_limits(
    http_providers=PROVIDER_HTTP_CLIENT_REGISTRY,
    lake_writer_limit=get_settings().resolved_prefect_lake_writer_limit(),
    additional_limits=[],
)


__all__ = ["PREFECT_LIMITS"]
