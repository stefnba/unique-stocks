import json

from config import config
from services.api import EodHistoricalDataApi
from services.azure import datalake_client
from utils import formats

from .utils import (
    build_remote_location_exchange_details,
    build_remote_location_exchange_list,
)

ApiClient = EodHistoricalDataApi


def download_details_for_exchanges():
    """
    Takes a list of exchanges, iterates over them to get exchange details
    """
    exchange_list = ["US", "LSE", "XNAS"]

    for exchange in exchange_list:
        download_exchange_details(exchange)


def download_exchange_details(exchange: str):
    """
    Retrieves details for a given exchange
    """
    # config
    asset_source = ApiClient.client_key

    # api data
    exhange_details = ApiClient.get_exchange_details(exhange_code=exchange)

    # upload to datalake
    datalake_client.upload_file(
        remote_file=build_remote_location_exchange_details(
            asset_source=asset_source,
            file_extension="json",
            exchange=exchange,
        ),
        file_system=config.azure.file_system,
        local_file=formats.convert_json_to_bytes(exhange_details),
    )


def download_exchanges():
    """
    Retrieves list of exchange from eodhistoricaldata.com and uploads
    into the Data Lake.
    """
    # config
    asset_source = ApiClient.client_key

    # api data
    exchanges_json = ApiClient.list_exhanges()

    # upload to datalake
    uploaded_file = datalake_client.upload_file(
        remote_file=build_remote_location_exchange_list(
            asset_source=asset_source, file_extension="json"
        ),
        file_system=config.azure.file_system,
        local_file=formats.convert_json_to_bytes(exchanges_json),
    )

    return uploaded_file


def transform_exchanges(file_path: str):
    """
    File content is in JSON format.

    Args:
        file_path (str): _description_
    """
    file_content = datalake_client.download_file_into_memory(
        file_system=config.azure.file_system, remote_file=file_path
    )
    print(json.loads(file_content))
