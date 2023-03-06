from services.config import config
from services.jobs.exchanges.remote_locations import ExchangeListLocation
from services.clients.api.iso.iso_exhanges import IsoExchangesApiClient
from services.clients.data_lake.azure_data_lake import datalake_client

ApiClient = IsoExchangesApiClient


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
        remote_file=ExchangeListLocation.raw(asset_source=asset_source),
        file_system=config.azure.file_system,
        local_file=echange_file.content,
    )

    return uploaded_file.file_path
