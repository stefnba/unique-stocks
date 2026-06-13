"""Tests for provider-client rate-limit discovery."""

from core.prefect.provider_rate_limits import iter_provider_http_clients, provider_rate_limit_registrations
from providers.eodhd.client import EODHDClient
from providers.iso10383.client import ISO10383Client
from providers.registry import Provider


def test_iter_provider_http_clients_discovers_known_provider_clients() -> None:
    """Provider discovery should find HTTP clients from Provider enum packages."""
    clients = set(iter_provider_http_clients(Provider))

    assert EODHDClient in clients
    assert ISO10383Client in clients


def test_provider_rate_limit_registrations_include_only_declared_policies() -> None:
    """Only clients declaring RATE_LIMIT_POLICY should produce Prefect registrations."""
    registrations = list(provider_rate_limit_registrations(Provider))

    assert [registration.name for registration in registrations] == ["unique-stocks.provider.eodhd"]
    assert registrations[0].burst_capacity == 100
    assert registrations[0].slot_decay_per_second == 10.0
