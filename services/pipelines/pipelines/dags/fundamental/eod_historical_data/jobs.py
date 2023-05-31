import polars as pl
from shared.clients.api.eod.client import EodHistoricalDataApiClient

ApiClient = EodHistoricalDataApiClient
ASSET_SOURCE = ApiClient.client_key

from typing import TypedDict


class SecurityExtractDict(TypedDict):
    security_code: str
    exchange_code: str
    figi: str
    exchange_mic: str
    exchange_id: int
    security_listing_id: int


def extract() -> list[SecurityExtractDict]:
    return [
        {
            "exchange_code": "NASDAQ",
            "figi": "asdf",
            "security_code": "AAPL",
            "exchange_mic": "ADSf",
            "exchange_id": 12,
            "security_listing_id": 34662,
        }
    ]


def ingest(security: SecurityExtractDict):
    return ApiClient.get_fundamentals(security_code=security["security_code"], exchange_code=security["exchange_code"])


def transform(data: pl.DataFrame, security: SecurityExtractDict):
    data = data.with_columns(
        [
            pl.lit(security["figi"]).alias("figi"),
            pl.lit(security["exchange_mic"]).alias("exchange_mic"),
            pl.lit(security["exchange_id"]).alias("exchange_id"),
            pl.lit(security["security_listing_id"]).alias("security_listing_id"),
            pl.lit(1).alias("interval_id"),
        ]
    )

    data = data.rename({"date": "timestamp"})

    return data


def load_entity(data: pl.DataFrame):
    from shared.clients.db.postgres.repositories import DbQueryRepositories

    return DbQueryRepositories.security_quote.add(data)


def load_fundamental(data: pl.DataFrame):
    from shared.clients.db.postgres.repositories import DbQueryRepositories

    return DbQueryRepositories.security_quote.add(data)
