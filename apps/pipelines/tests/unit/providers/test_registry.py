"""Tests for the app provider registry."""

from config.providers import Provider
from providers.eodhd.client import EODHDClient
from providers.iso10383.client import ISO10383Client
from providers.registry import provider_http_client_registry, provider_http_clients


def test_provider_http_client_registry_returns_known_provider_clients() -> None:
    """App provider registry should expose the concrete HTTP clients."""
    assert provider_http_client_registry() == {
        Provider.EODHD: EODHDClient,
        Provider.ISO10383: ISO10383Client,
    }


def test_provider_http_clients_follow_provider_enum_order() -> None:
    """Provider client tuple should be deterministic and complete."""
    assert provider_http_clients() == (EODHDClient, ISO10383Client)
