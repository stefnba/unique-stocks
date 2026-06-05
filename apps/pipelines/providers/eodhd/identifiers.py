"""Identifier helpers for EODHD provider API handles."""

from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class EODHDInstrumentRef:
    """Split provider identity for one EODHD instrument."""

    provider_exchange_code: str
    provider_instrument_code: str

    @property
    def api_symbol(self) -> str:
        """Return the EODHD exchange-qualified API symbol."""
        return eodhd_api_symbol(
            provider_exchange_code=self.provider_exchange_code,
            provider_instrument_code=self.provider_instrument_code,
        )


def eodhd_api_symbol(*, provider_exchange_code: str, provider_instrument_code: str) -> str:
    """Build the EODHD exchange-qualified API symbol."""
    return f"{provider_instrument_code}.{provider_exchange_code}"


def eodhd_instrument_key(*, provider_exchange_code: str, provider_instrument_code: str) -> str:
    """Build a stable compact key for logs and dictionaries."""
    return f"{provider_exchange_code}:{provider_instrument_code}"
