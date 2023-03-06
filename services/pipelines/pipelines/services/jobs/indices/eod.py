import json

from services.hooks.api import EodHistoricalDataApi
from services.services.data_lake import datalake_client
from services.utils import build_file_path, formats, path_with_dateime


def download_indices():
    """
    Retrieves list of exchange from eodhistoricaldata.com and uploads
    into the Data Lake.
    """
    exchanges_json = EodHistoricalDataApi.list_exhanges()
    exchanges_bytes = formats.convert_json_to_bytes(exchanges_json)

    source = EodHistoricalDataApi.client_key
    remote_location = build_file_path(
        directory=["raw", "exchanges", source, path_with_dateime()],
        filename=f'{path_with_dateime("%Y%m%d-%H%M%S")}_{source}',
        extension="json",
    )
    uploaded_file = datalake_client.upload_file(
        remote_file=remote_location, file_system="dev", local_file=exchanges_bytes
    )
    print(uploaded_file)

    return uploaded_file


def transform_exchanges(file_path: str):
    """
    File content is in JSON format.

    Args:
        file_path (str): _description_
    """
    file_content = datalake_client.download_file_into_memory(
        file_system="dev", remote_file=file_path
    )
    print(json.loads(file_content))
