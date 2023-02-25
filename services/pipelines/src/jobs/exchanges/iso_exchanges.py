from config import config
from services.api import IsoExchangesApi
from services.azure.datalake_client import AzureDataLakeClient
from services.utils import build_file_path, path_with_dateime

client = AzureDataLakeClient(config.azure.storage_account_url)


def donwload_iso_exchange_list():
    """
    Retrieves and upploads into the Data Lake raw ISO list with information
    on exchanges and their MIC codes.
    """
    file = IsoExchangesApi.download_exhange_list()
    source = IsoExchangesApi.client_key
    remote_location = build_file_path(
        directory=["raw", "exchanges", path_with_dateime()],
        filename=f'{path_with_dateime("%Y%m%d-%H%M%S")}_{source}',
        extension=file.extension,
    )
    uploaded_file = client.upload_file(
        remote_file=remote_location, file_system="dev", local_file=file.content
    )
    print(uploaded_file)
