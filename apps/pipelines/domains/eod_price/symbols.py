"""Helpers for EOD price ticker symbols."""


def qualified_ticker(code: str, exchange_code: str) -> str:
    """Return the EODHD exchange-qualified ticker symbol."""
    return f"{code}.{exchange_code}"


def exchange_from_qualified_ticker(ticker: str) -> str:
    """Return the exchange suffix from an EODHD-qualified ticker."""
    if "." not in ticker:
        raise ValueError(f"Expected exchange-qualified ticker, got {ticker!r}")
    return ticker.rsplit(".", maxsplit=1)[1]


def ticker_without_exchange(ticker: str, exchange_code: str) -> str:
    """Remove the expected exchange suffix from a qualified ticker."""
    return ticker.removesuffix(f".{exchange_code}")
