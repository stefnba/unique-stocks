from typing import TypedDict

from shared.clients.datalake.azure.azure_datalake import datalake_client
from shared.clients.duck.client import duck
from shared.utils.path.datalake.builder import DatalakePathBuilder


class ExchangeSources(TypedDict):
    eod_exchange_path: str
    iso_exchange_path: str
    msk_exchange_path: str


def merge_exchanges(file_paths: ExchangeSources):
    """
    Merge exchanges from eod, iso and msk together into one file.
    """

    merged = duck.query(
        "./sql/merge_exchanges.sql",
        data_iso=duck.get_data(file_paths["iso_exchange_path"], handler="azure_abfs"),
        data_eod=duck.get_data(file_paths["eod_exchange_path"], handler="azure_abfs"),
        data_msk=duck.get_data(file_paths["msk_exchange_path"], handler="azure_abfs"),
    )

    return datalake_client.upload_file(
        destination_file_path="curated/product=exchanges/asset=exchanges/exchanges.parquet",
        file=merged.df().to_parquet(),
    ).file.full_path
