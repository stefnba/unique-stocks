from dags.reference.currencies.clients.currency_client import CurrencyApiClient
from services.clients.datalake.azure.azure_datalake import datalake_client
from services.config import config


class CurrencyJobs:
    @staticmethod
    def download_currencies():
        currencies = CurrencyApiClient.get_currencies()

        # upload to datalake
        uploaded_file = datalake_client.upload_file(
            remote_file="references/raw/currencies/currencies_reference.csv",
            file_system=config.azure.file_system,
            local_file=currencies,
        )
        return uploaded_file.file_path

    @staticmethod
    def process_currencies(file_path: str):
        import polars as pl
        import io

        #  download file
        file_content = datalake_client.download_file_into_memory(
            file_system=config.azure.file_system, remote_file=file_path
        )

        print(file_content)

        df = pl.read_csv(io.BytesIO(file_content), null_values=["Nil"])

        # upload to datalake
        uploaded_file = datalake_client.upload_file(
            remote_file="references/processed/currencies/currencies_reference.parquet",
            file_system=config.azure.file_system,
            local_file=df.to_pandas().to_parquet(),
        )
        return uploaded_file.file_path
