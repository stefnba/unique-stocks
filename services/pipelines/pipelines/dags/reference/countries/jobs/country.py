from dags.reference.countries.clients.country import CountryApiClient
from dags.reference.countries.jobs.config import CountriesPath
from shared.clients.datalake.azure.azure_datalake import datalake_client


class CountryJobs:
    @staticmethod
    def download_countries():
        countries = CountryApiClient.get_countries()

        # upload to datalake
        uploaded_file = datalake_client.upload_file(
            destination_file_path=CountriesPath(zone="raw", file_type="csv"),
            file=countries,
        )
        return uploaded_file.file.full_path

    @staticmethod
    def process_countries(file_path: str):
        import io

        import polars as pl

        #  download file
        file_content = datalake_client.download_file_into_memory(file_path=file_path)

        df = pl.read_csv(io.BytesIO(file_content))

        # upload to datalake
        uploaded_file = datalake_client.upload_file(
            destination_file_path=CountriesPath(zone="processed"),
            file=df.to_pandas().to_parquet(),
        )
        return uploaded_file.file.full_path
