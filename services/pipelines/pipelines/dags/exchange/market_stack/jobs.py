import polars as pl
from shared.clients.api.market_stack.client import MarketStackApiClient

ApiClient = MarketStackApiClient
ASSET_SOURCE = ApiClient.client_key


def ingest():
    return ApiClient.get_exchanges()


def transform(data: pl.DataFrame):
    from shared.clients.db.postgres.repositories import DbQueryRepositories
    from shared.clients.duck.client import duck

    data = data.with_columns(
        [
            pl.lit(ASSET_SOURCE).alias("data_source"),
            pl.when(pl.col(pl.Utf8).str.lengths() == 0).then(None).otherwise(pl.col(pl.Utf8)).keep_name(),
        ]
    )

    return duck.query(
        "./sql/transform.sql",
        exchange=data,
        virtual_exchange_mapping=DbQueryRepositories.mappings.get_mappings(
            source=ASSET_SOURCE, field="is_virtual", product="exchange"
        ),
        exchange_code_mapping=DbQueryRepositories.mappings.get_mappings(
            source=ASSET_SOURCE, field="exchange_code", product="exchange"
        ),
    ).pl()
