from config import config
from services.api import MarketStackApi
from services.azure import datalake_client
from utils import formats

from .utils import build_remote_location_exchange_list

ApiClient = MarketStackApi


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
        remote_file=build_remote_location_exchange_list(asset_source=asset_source),
        file_system=config.azure.file_system,
        local_file=formats.convert_json_to_bytes(exchanges_json),
    )

    return uploaded_file
