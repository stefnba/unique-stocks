import pandas as pd
from include.config import config
from include.jobs.exchanges.remote_locations import ExchangeListLocation
from include.services.api import MarketStackApi
from include.services.azure import datalake_client
from include.utils import formats

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
        remote_file=ExchangeListLocation.raw(asset_source=asset_source),
        file_system=config.azure.file_system,
        local_file=formats.convert_json_to_bytes(exchanges_json),
    )

    return uploaded_file.file_path


def transform_exchanges():
    input = ""
    df = pd.read_json(input)

    df = df[
        [
            "name",
            "acronym",
            "mic",
            # "country",
            "country_code",
            "city",
            "website",
            "timezone",
            "currency",
        ]
    ]

    # remove index row
    df = df.query('name != "INDEX"')

    # normalize JSON
    df["timezone"] = df["timezone"].apply(lambda x: x.get("timezone", None))
    df["currency"] = df["currency"].apply(lambda x: x.get("code", None))

    # rename columns
    df = df.rename(
        {
            "country_code": "country",
        }
    )
