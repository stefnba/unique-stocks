from dags.exchanges.exchanges.jobs.config import ExchangesPath
from shared.clients.api.iso.client import IsoExchangesApiClient
from shared.clients.datalake.azure.azure_datalake import datalake_client

ApiClient = IsoExchangesApiClient
ASSET_SOURCE = ApiClient.client_key


class IsoExchangeJobs:
    @staticmethod
    def download_exchanges():
        """
        Retrieves and upploads into the Data Lake raw ISO list with information
        on exchanges and their MIC codes.
        """

        # api data
        exchange_file = ApiClient.get_exchanges()

        # datalake destination
        uploaded_file = datalake_client.upload_file(
            destination_file_path=ExchangesPath(asset_source=ASSET_SOURCE, zone="raw", file_type="csv"),
            file=exchange_file.content,
        )

        return uploaded_file.file.full_path

    @staticmethod
    def process_exchanges(file_path: str):
        import io

        import duckdb
        import polars as pl

        print("asdflksfalsdfjalksdfjlkasjdfkl", file_path)

        file_content = datalake_client.download_file_into_memory(file_path=file_path)
        df1 = pl.read_csv(io.BytesIO(file_content), encoding="ISO8859-1")

        print(df1.columns)
        print(df1.head())

        df1 = df1.with_columns(
            [
                pl.when(pl.col(pl.Utf8).str.lengths() == 0).then(None).otherwise(pl.col(pl.Utf8)).keep_name(),
            ]
        )
        print(df1.head())
        # df = df.with_columns(
        #     [
        #         pl.col("CREATION DATE").cast(pl.Utf8).str.strptime(pl.Date, fmt="%Y%m%d").cast(pl.Date),
        #         pl.col("LAST UPDATE DATE").cast(pl.Utf8).str.strptime(pl.Date, fmt="%Y%m%d").cast(pl.Date),
        #         pl.col("LAST VALIDATION DATE").cast(pl.Utf8).str.strptime(pl.Date, fmt="%Y%m%d").cast(pl.Date),
        #         pl.col("EXPIRY DATE").cast(pl.Utf8).str.strptime(pl.Date, fmt="%Y%m%d").cast(pl.Date),
        #     ]
        # )

        df = duckdb.sql(
            """
        --sql
        SELECT
            "MIC" AS "mic",
            "OPERATING MIC" AS "operating_mic",
            "ISO COUNTRY CODE (ISO 3166)" AS "country",
            "CITY" AS "city",
            "WEBSITE" AS "website",
            "LEGAL ENTITY NAME" AS "legal_name",
            "ACRONYM" AS "acronym",
            "CREATION DATE" AS "created_at",
            "LAST UPDATE DATE" AS "updated_at",
            "COMMENTS" AS "comments",
            "LEI" AS "legal_entity_identifier",
            strptime("LAST VALIDATION DATE", '%Y%m%d') AS "validated_at",
            "EXPIRY DATE" AS "expires_at",
            "MARKET CATEGORY CODE" AS "market_category_code",
            "MARKET NAME-INSTITUTION DESCRIPTION" AS "market_name_institution"
        FROM
            df1
        WHERE
            "OPRT/SGMT" = 'OPRT' AND "STATUS" = 'ACTIVE';
        """
        ).df()

        df["city"] = df["city"].str.title()
        df["market_name_institution"] = df["market_name_institution"].str.title()
        df["legal_name"] = df["legal_name"].str.title()
        df["comments"] = df["comments"].str.title()
        df["website"] = df["website"].str.lower()

        # datalake destination
        uploaded_file = datalake_client.upload_file(
            destination_file_path=ExchangesPath(asset_source=ASSET_SOURCE, zone="processed"),
            file=df.to_parquet(),
        )

        return uploaded_file.file.full_path
