import polars as pl
from shared.clients.api.eod.client import EodHistoricalDataApiClient
from shared.utils.dataset.validate import ValidateDataset
from typing import TypedDict
from shared.clients.duck.client import duck


ApiClient = EodHistoricalDataApiClient
ASSET_SOURCE = ApiClient.client_key


class SecurityExtractDict(TypedDict):
    ticker: str
    id: int
    mic: str
    exchange_code: str


def extract():
    from shared.clients.db.postgres.repositories import DbQueryRepositories

    security = DbQueryRepositories.security_listing.find_all_with_quote_source(source="EodHistoricalData")

    exchange_code_mapping = DbQueryRepositories.mappings.get_mappings(
        source="EodHistoricalData", product="exchange", field="exchange_code"
    )

    data = (
        security.join(exchange_code_mapping[["source_value", "uid"]], how="left", left_on="mic", right_on="uid")
        .rename({"source_value": "exchange_code"})
        .drop("mic")
    )

    data = ValidateDataset(dataset=data).is_not_null("exchange_code").is_not_null("ticker").return_dataset()

    return data.head(200).to_dicts()


def ingest(ticker: str, exchange_code: str):
    return ApiClient.get_historical_eod_quotes(ticker, exchange_code)


def transform(data: pl.DataFrame, security_listing_id: int):
    data = duck.query("sql/transform.sql", data=data, security_listing_id=security_listing_id, interval_id=1).pl()

    return ValidateDataset(dataset=data).is_not_null("security_listing_id").return_dataset()


def load(data: pl.DataFrame):
    from shared.clients.db.postgres.repositories import DbQueryRepositories

    return DbQueryRepositories.security_quote.add(data)
