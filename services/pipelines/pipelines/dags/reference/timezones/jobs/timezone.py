import io

import polars as pl
from dags.reference.timezones.clients.timezone import TimezoneApiClient
from dags.reference.timezones.jobs.config import TimezonesPath
from shared.clients.datalake.azure.azure_datalake import datalake_client


class TimezoneJobs:
    @staticmethod
    def download():
        currencies = TimezoneApiClient.get_timezones()

        # upload to datalake
        uploaded_file = datalake_client.upload_file(
            destination_file_path=TimezonesPath(zone="raw", file_type="csv"),
            file=currencies,
        )
        return uploaded_file.file.full_path

    @staticmethod
    def process(file_path: str):
        #  download file
        file_content = datalake_client.download_file_into_memory(file_path=file_path)

        df = pl.read_csv(io.BytesIO(file_content), null_values=["Nil"])

        # upload to datalake
        uploaded_file = datalake_client.upload_file(
            destination_file_path=TimezonesPath(zone="processed"),
            file=df.to_pandas().to_parquet(),
        )
        return uploaded_file.file.full_path
