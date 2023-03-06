import json

import polars as pl
from services.config import config
from services.clients.api.eod.client import EodHistoricalDataApiClient
from services.clients.data_lake.azure_data_lake import datalake_client
from services.utils import formats

from .remote_locations import ExchangeDetailLocation, ExchangeListLocation

ApiClient = EodHistoricalDataApiClient
ASSET_SOURCE = ApiClient.client_key


def download_details_for_exchanges(exchange_list: list[str]):
    """
    Takes a list of exchanges, iterates over them to call exchange details
    """
    exchange_detail_paths = []
    for exchange in exchange_list:
        exchange_detail_paths.append(download_exchange_details(exchange))

    return exchange_detail_paths


def download_exchange_details(exchange: str):
    """
    Retrieves details for a given exchange
    """

    # api data
    exhange_details = ApiClient.get_exchange_details(exhange_code=exchange)

    # upload to datalake
    uploaded_file = datalake_client.upload_file(
        remote_file=ExchangeDetailLocation.raw(
            asset_source=ASSET_SOURCE, exchange=exchange
        ),
        file_system=config.azure.file_system,
        local_file=formats.convert_json_to_bytes(exhange_details),
    )
    return uploaded_file.file_path


def download_exchanges():
    """
    Retrieves list of exchange from eodhistoricaldata.com and uploads
    into the Data Lake.
    """

    # api data
    exchanges_json = ApiClient.list_exhanges()

    # upload to datalake
    uploaded_file = datalake_client.upload_file(
        remote_file=ExchangeListLocation.raw(asset_source=ASSET_SOURCE),
        file_system=config.azure.file_system,
        local_file=formats.convert_json_to_bytes(exchanges_json),
    )

    return uploaded_file.file_path


def transform_exchanges(file_path: str):
    """
    File content is in JSON format.

    Args:
        file_path (str): _description_
    """

    # donwload file
    file_content = datalake_client.download_file_into_memory(
        file_system=config.azure.file_system, remote_file=file_path
    )

    df = pl.DataFrame(json.loads(file_content))
    df = df[["Name", "Code", "OperatingMIC", "CountryISO2", "Currency"]]
    df = df.rename(
        {
            "CountryISO2": "country",
            "Name": "name",
            "Currency": "currency",
            "OperatingMIC": "mic_operating",
            "Code": "code",
        }
    )

    # upload to datalake
    uploaded_file = datalake_client.upload_file(
        remote_file=ExchangeListLocation.processed(asset_source=ASSET_SOURCE),
        file_system=config.azure.file_system,
        local_file=df.to_pandas().to_parquet(),
    )

    return uploaded_file.file_path
