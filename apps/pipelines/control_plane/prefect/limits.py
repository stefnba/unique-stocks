"""Application Prefect limit definitions."""

from collections.abc import Mapping
from typing import Final

from config.settings import get_settings
from core.orchestration.limits import define_limits
from core.types import Environment
from providers.registry import PROVIDER_HTTP_CLIENT_REGISTRY

LAKE_WRITER_SLOTS_BY_ENVIRONMENT: Final[Mapping[Environment, int]] = {
    "dev": 1,
    "docker_dev": 1,
    "prod": 1,
}

PREFECT_LIMITS = define_limits(
    http_providers=PROVIDER_HTTP_CLIENT_REGISTRY,
    lake_writer_limit=LAKE_WRITER_SLOTS_BY_ENVIRONMENT[get_settings().environment],
    additional_limits=[],
)


__all__ = ["PREFECT_LIMITS"]
