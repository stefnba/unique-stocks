from dags.reference.timezones.clients.timezones import TimezoneApiClient
from services.clients.datalake.azure.azure_datalake import datalake_client
from services.config import config
import polars as pl
import io


class TimezoneJobs:
    @staticmethod
    def download():
        currencies = TimezoneApiClient.get_timezones()

        # upload to datalake
        uploaded_file = datalake_client.upload_file(
            remote_file="references/raw/timezones/timezones_reference.csv",
            file_system=config.azure.file_system,
            local_file=currencies,
        )
        return uploaded_file.file_path

    @staticmethod
    def process(file_path: str):
        #  download file
        file_content = datalake_client.download_file_into_memory(
            file_system=config.azure.file_system, remote_file=file_path
        )

        df = pl.read_csv(io.BytesIO(file_content), null_values=["Nil"])

        # upload to datalake
        uploaded_file = datalake_client.upload_file(
            remote_file="references/processed/timezones/timezones_reference.parquet",
            file_system=config.azure.file_system,
            local_file=df.to_pandas().to_parquet(),
        )
        return uploaded_file.file_path