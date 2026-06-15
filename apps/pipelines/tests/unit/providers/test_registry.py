"""Tests for the app provider registry."""

from config.providers import Provider
from providers.eodhd.client import EODHDClient
from providers.iso10383.client import ISO10383Client
from providers.registry import PROVIDER_HTTP_CLIENT_REGISTRY


def test_provider_http_client_catalog_declares_known_provider_clients() -> None:
    """App provider registry should expose the concrete HTTP clients."""
    assert {
        Provider.EODHD: EODHDClient,
        Provider.ISO10383: ISO10383Client,
    } == PROVIDER_HTTP_CLIENT_REGISTRY
