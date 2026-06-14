"""Provider implementation catalog for the pipelines app.

Provider identities live in ``config.providers`` because they are stable app
vocabulary. Concrete provider clients live in ``providers`` because they are
provider-specific implementations. This module is the app-owned bridge between
those two concepts.

Keep this registry out of ``core``. Generic HTTP and Prefect helpers may accept
provider client classes, but they must not know which providers this app uses.
Keep this module passive: validation and ordering belong in app composition.
"""

from config.providers import Provider
from core.http.registry import HttpClientRegistry
from providers.eodhd.client import EODHDClient
from providers.iso10383.client import ISO10383Client

PROVIDER_HTTP_CLIENT_REGISTRY: HttpClientRegistry[Provider] = {
    Provider.EODHD: EODHDClient,
    Provider.ISO10383: ISO10383Client,
}
"""Concrete HTTP client classes keyed by stable provider identity."""


__all__ = ["PROVIDER_HTTP_CLIENT_REGISTRY"]
