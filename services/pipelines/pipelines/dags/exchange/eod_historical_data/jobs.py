import polars as pl
from shared.clients.api.eod.client import EodHistoricalDataApiClient

ApiClient = EodHistoricalDataApiClient
ASSET_SOURCE = ApiClient.client_key


def ingest():
    """
    Retrieves list of exchange from eodhistoricaldata.com and uploads
    into the Data Lake.


    """
    from shared.clients.api.eod.client import EodHistoricalDataApiClient

    return EodHistoricalDataApiClient.get_exchanges()


def transform(data: pl.DataFrame):
    """
    Steps:
    -
    """

    from shared.clients.db.postgres.repositories import DbQueryRepositories
    from shared.clients.duck.client import duck

    exchange_code_mapping = DbQueryRepositories.mappings.get_mappings(
        product="exchange", source="EodHistoricalData", field="exchange_code"
    )
    virtual_exchange_mapping = DbQueryRepositories.mappings.get_mappings(
        product="exchange", source="EodHistoricalData", field="is_virtual"
    )

    data = data.with_columns(
        [
            pl.when(pl.col(pl.Utf8) == "Unknown").then(None).otherwise(pl.col(pl.Utf8)).keep_name(),
        ]
    )
    data = data.with_columns(
        [
            pl.when(pl.col(pl.Utf8).str.lengths() == 0).then(None).otherwise(pl.col(pl.Utf8)).keep_name(),
            pl.lit(ASSET_SOURCE).alias("data_source"),
        ]
    )

    return duck.query(
        "./sql/transform.sql",
        exchange=data,
        exchange_code_mapping=exchange_code_mapping,
        virtual_exchange_mapping=virtual_exchange_mapping,
        source=ASSET_SOURCE,
    ).pl()
