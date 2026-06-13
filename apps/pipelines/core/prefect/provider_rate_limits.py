"""Provider-client discovery for Prefect rate-limit registration."""

from __future__ import annotations

import inspect
from collections.abc import Iterable, Iterator
from importlib import import_module
from typing import Any

from core.clients.http.base import HttpClientBase
from core.prefect.concurrency import ProviderRateLimitRegistration


def iter_provider_http_clients(
    providers: Iterable[Any],
    *,
    package: str = "providers",
) -> Iterator[type[HttpClientBase]]:
    """Yield HTTP client classes discovered from provider package names."""
    for provider in providers:
        provider_key = str(provider)
        module = import_module(f"{package}.{provider_key}.client")
        for value in vars(module).values():
            if not inspect.isclass(value):
                continue
            if value is HttpClientBase or not issubclass(value, HttpClientBase):
                continue
            if str(value.PROVIDER) != provider_key:
                continue
            yield value


def provider_rate_limit_registrations(
    providers: Iterable[Any],
    *,
    package: str = "providers",
) -> Iterator[ProviderRateLimitRegistration]:
    """Yield Prefect rate-limit registrations declared by provider clients."""
    for client_cls in iter_provider_http_clients(providers, package=package):
        policy = getattr(client_cls, "RATE_LIMIT_POLICY", None)
        if policy is None:
            continue
        yield policy.registration(provider=str(client_cls.PROVIDER))
