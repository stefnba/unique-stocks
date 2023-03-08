import io

import pandas as pd
import polars as pl
from services.clients.api.iso.iso_exhanges import IsoExchangesApiClient
from services.clients.data_lake.azure_data_lake import datalake_client
from services.config import config
from services.jobs.exchanges.remote_locations import ExchangeListLocation

API_CLIENT = IsoExchangesApiClient
ASSET_SOURCE = API_CLIENT.client_key


class IsoExchangeJobs:
    @staticmethod
    def download_exchange_list():
        """
        Retrieves and upploads into the Data Lake raw ISO list with information
        on exchanges and their MIC codes.
        """

        # api data
        exchange_file = API_CLIENT.download_exhange_list()

        # datalake destination
        uploaded_file = datalake_client.upload_file(
            remote_file=ExchangeListLocation.raw(asset_source=ASSET_SOURCE),
            file_system=config.azure.file_system,
            local_file=exchange_file.content,
        )

        return uploaded_file.file_path

    @staticmethod
    def process_raw_exchange_list(file_path: str):
        """_summary_

        Args:
            file_path (str): File location in data lake

        Raises:
            Exception: _description_
        """
        file_content = datalake_client.download_file_into_memory(
            file_system=config.azure.file_system, remote_file=file_path
        )
        df = pl.read_csv(file=io.BytesIO(file_content), encoding="ISO8859-1")

        # replace empty string with null
        df = df.with_columns(
            [
                pl.when(pl.col(pl.Utf8).str.lengths() == 0)
                .then(None)
                .otherwise(pl.col(pl.Utf8))
                .keep_name()
            ]
        )

        # filter for only OPRT
        # OPRT (Operating) or SGMT (Segment1): indicates whether the MIC is
        # an operating MIC or a market segment MIC.
        df = df.filter((pl.col("OPRT/SGMT") == "OPRT") & (pl.col("STATUS") == "ACTIVE"))
        df = df.rename(
            {
                "MIC": "mic",
                "OPERATING MIC": "operating_mic",
                "ISO COUNTRY CODE (ISO 3166)": "country",
                "CITY": "city",
                "WEBSITE": "website",
                "LEGAL ENTITY NAME": "legal_name",
                "ACRONYM": "acronym",
                "CREATION DATE": "created_at",
                "LAST UPDATE DATE": "updated_at",
                "COMMENTS": "comments",
                "LEI": "legal_entity_identifier",
                "LAST VALIDATION DATE": "validated_at",
                "EXPIRY DATE": "expires_at",
                "MARKET CATEGORY CODE": "market_category_code",
                "MARKET NAME-INSTITUTION DESCRIPTION": "market_name_institution",
            }
        )

        df = df[
            [
                "mic",
                "operating_mic",
                "market_name_institution",
                "legal_name",
                "legal_entity_identifier",
                "market_category_code",
                "acronym",
                "country",
                "city",
                "website",
                "created_at",
                "updated_at",
                "validated_at",
                "expires_at",
                "comments",
            ]
        ]

        # cast to date
        df = df.with_columns(
            [
                pl.col("created_at")
                .cast(pl.Utf8)
                .str.strptime(pl.Date, fmt="%Y%m%d")
                .cast(pl.Date),
                pl.col("updated_at")
                .cast(pl.Utf8)
                .str.strptime(pl.Date, fmt="%Y%m%d")
                .cast(pl.Date),
                pl.col("validated_at")
                .cast(pl.Utf8)
                .str.strptime(pl.Date, fmt="%Y%m%d")
                .cast(pl.Date),
                pl.col("expires_at")
                .cast(pl.Utf8)
                .str.strptime(pl.Date, fmt="%Y%m%d")
                .cast(pl.Date),
            ]
        )

        df_pandas: pd.DataFrame
        df_pandas = df.to_pandas()

        #
        df_pandas["city"] = df_pandas["city"].str.title()
        df_pandas["market_name_institution"] = df_pandas[
            "market_name_institution"
        ].str.title()
        df_pandas["legal_name"] = df_pandas["legal_name"].str.title()
        df_pandas["comments"] = df_pandas["comments"].str.title()
        df_pandas["website"] = df_pandas["website"].str.lower()

        if not df_pandas["mic"].is_unique:
            raise Exception("Index not unique.")

        # datalake destination
        uploaded_file = datalake_client.upload_file(
            remote_file=ExchangeListLocation.processed(asset_source=ASSET_SOURCE),
            file_system=config.azure.file_system,
            local_file=df_pandas.to_parquet(),
        )

        return uploaded_file.file_path
