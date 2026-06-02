"""Helpers for EOD price ticker symbols."""


def qualified_ticker(code: str, provider_exchange_code: str) -> str:
    """Return the provider exchange-qualified ticker symbol."""
    return f"{code}.{provider_exchange_code}"


def exchange_from_qualified_ticker(ticker: str) -> str:
    """Return the exchange suffix from a provider-qualified ticker."""
    if "." not in ticker:
        raise ValueError(f"Expected exchange-qualified ticker, got {ticker!r}")
    return ticker.rsplit(".", maxsplit=1)[1]


def ticker_without_exchange(ticker: str, provider_exchange_code: str) -> str:
    """Remove the expected exchange suffix from a qualified ticker."""
    return ticker.removesuffix(f".{provider_exchange_code}")
