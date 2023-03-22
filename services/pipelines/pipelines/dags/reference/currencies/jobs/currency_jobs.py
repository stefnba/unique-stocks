from dags.reference.currencies.clients.currency_client import CurrencyApiClient
from shared.clients.datalake.azure.azure_datalake import datalake_client
from shared.config import config


class CurrencyJobs:
    @staticmethod
    def download_currencies():
        currencies = CurrencyApiClient.get_currencies()

        # upload to datalake
        uploaded_file = datalake_client.upload_file(
            destination_file_path="raw/references/currencies/currencies_reference.csv",
            file=currencies,
        )
        return uploaded_file.file.full_path

    @staticmethod
    def process_currencies(file_path: str):
        import io

        import polars as pl

        #  download file
        file_content = datalake_client.download_file_into_memory(file_path=file_path)

        print(file_content)

        df = pl.read_csv(io.BytesIO(file_content), null_values=["Nil"])

        # upload to datalake
        uploaded_file = datalake_client.upload_file(
            destination_file_path="processed/references/currencies/currencies_reference.parquet",
            file=df.to_pandas().to_parquet(),
        )
        return uploaded_file.file.full_path
