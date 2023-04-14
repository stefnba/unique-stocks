from typing import TypedDict

from dags.exchange.exchange.jobs.utils import ExchangePath
from shared.clients.data_lake.azure.azure_data_lake import dl_client
from shared.clients.duck.client import duck


class ExchangeSources(TypedDict):
    eod_exchange_path: str
    iso_exchange_path: str
    msk_exchange_path: str


def merge_exchanges(file_paths: ExchangeSources):
    """
    Merge exchange from eod, iso and msk together into one file.
    """

    merged = duck.query(
        "./sql/merge_exchanges.sql",
        data_iso=duck.get_data(file_paths["iso_exchange_path"], handler="azure_abfs"),
        data_eod=duck.get_data(file_paths["eod_exchange_path"], handler="azure_abfs"),
        data_msk=duck.get_data(file_paths["msk_exchange_path"], handler="azure_abfs"),
    )

    return dl_client.upload_file(
        destination_file_path=ExchangePath.curated(),
        file=merged.df().to_parquet(),
    ).file.full_path
