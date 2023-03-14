from dags.reference.countries.clients.country_client import CountryApiClient
from services.clients.datalake.azure.azure_datalake import datalake_client
from services.config import config


class CountryJobs:
    @staticmethod
    def download_countries():
        countries = CountryApiClient.get_countries()

        # upload to datalake
        uploaded_file = datalake_client.upload_file(
            remote_file="references/raw/countries/countries_reference.csv",
            file_system=config.azure.file_system,
            local_file=countries,
        )
        return uploaded_file.file_path

    @staticmethod
    def process_countries(file_path: str):
        import polars as pl
        import io

        #  download file
        file_content = datalake_client.download_file_into_memory(
            file_system=config.azure.file_system, remote_file=file_path
        )

        df = pl.read_csv(io.BytesIO(file_content))

        # upload to datalake
        uploaded_file = datalake_client.upload_file(
            remote_file="references/processed/countries/countries_reference.parquet",
            file_system=config.azure.file_system,
            local_file=df.to_pandas().to_parquet(),
        )
        return uploaded_file.file_path
