"""Stable domain identity keys for the pipelines app.

Executable domain metadata and cross-domain wiring belong near the behavior
that consumes them.
"""

from enum import StrEnum


class Domain(StrEnum):
    """Known pipeline domains for this app."""

    EXCHANGE = "exchange"
    EXCHANGE_SCHEDULE = "exchange_schedule"
    EOD_PRICE = "eod_price"
    FUNDAMENTAL = "fundamental"
    INSTRUMENT = "instrument"
    DBT = "dbt"


__all__ = ["Domain"]
