import polars as pl
from dags.exchange.path import ExchangePath
from shared.clients.api.iso.client import IsoExchangesApiClient
from shared.clients.data_lake.azure.azure_data_lake import dl_client
from shared.clients.duck.client import duck

ApiClient = IsoExchangesApiClient
ASSET_SOURCE = ApiClient.client_key


def ingest():
    """
    Retrieves and uploads into the Data Lake raw ISO list with information
    on exchange and their MIC codes.
    """
    from shared.clients.api.iso.client import IsoExchangesApiClient

    return IsoExchangesApiClient.get_exchanges()


def transform(data: pl.DataFrame):
    """_summary_"""
    data = data.with_columns(
        [
            pl.when(pl.col(pl.Utf8).str.lengths() == 0).then(None).otherwise(pl.col(pl.Utf8)).keep_name(),
        ]
    )

    transformed = duck.query("./sql/transform.sql", exchanges=data, source=ASSET_SOURCE).df()

    transformed["city"] = transformed["city"].str.title()
    transformed["legal_entity_name"] = transformed["legal_entity_name"].str.title()
    transformed["name"] = transformed["name"].str.title()
    transformed["comment"] = transformed["comment"].str.title()
    transformed["website"] = transformed["website"].str.lower()

    return pl.from_pandas(transformed)
