from config import config
from services.api import IsoExchangesApi
from services.azure import datalake_client

from .utils import build_remote_location_exchange_list

ApiClient = IsoExchangesApi


def donwload_iso_exchange_list():
    """
    Retrieves and upploads into the Data Lake raw ISO list with information
    on exchanges and their MIC codes.
    """
    # config

    asset_source = ApiClient.client_key

    # api data
    echange_file = ApiClient.download_exhange_list()

    # datalake destination
    uploaded_file = datalake_client.upload_file(
        remote_file=build_remote_location_exchange_list(asset_source=asset_source),
        file_system=config.azure.file_system,
        local_file=echange_file.content,
    )

    return uploaded_file
