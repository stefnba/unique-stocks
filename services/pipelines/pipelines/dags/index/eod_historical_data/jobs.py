import polars as pl
from shared.clients.api.eod.client import EodHistoricalDataApiClient

ApiClient = EodHistoricalDataApiClient
ASSET_SOURCE = ApiClient.client_key
INDEX_EXCHANGE_CODE = ApiClient.index_exhange_code


def ingest():
    return ApiClient.get_securities_listed_at_exchange(INDEX_EXCHANGE_CODE)


def transform(data: pl.DataFrame):
    from shared.clients.duck.client import duck

    data = data.with_columns(
        [
            pl.when(pl.col(pl.Utf8) == "Unknown").then(None).otherwise(pl.col(pl.Utf8)).keep_name(),
            pl.lit(ASSET_SOURCE).alias("data_source"),
        ]
    )

    return duck.query("./sql/transform.sql", index=data).pl()
