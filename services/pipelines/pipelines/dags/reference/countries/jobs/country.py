import duckdb
import polars as pl
from dags.reference.countries.clients.country import CountryApiClient
from dags.reference.countries.jobs.config import CountriesFinalPath, CountriesPath
from shared.clients.datalake.azure.azure_datalake import datalake_client


class CountryJobs:
    @staticmethod
    def download():
        countries = CountryApiClient.get_countries()

        # upload to datalake
        uploaded_file = datalake_client.upload_file(
            destination_file_path=CountriesPath(zone="raw", file_type="csv"),
            file=countries,
        )
        return uploaded_file.file.full_path

    @staticmethod
    def process(file_path: str):
        #  download file
        file_content = datalake_client.download_file_into_memory(file_path=file_path)

        countries_data = pl.read_csv(file_content)

        processed_data = duckdb.sql(
            """
        --sql
        SELECT
            upper(alpha2) AS id,
            upper(alpha2) AS alpha2_code,
            upper(alpha3) AS alpha3_code,
            name,
        FROM
            countries_data;
        """
        ).pl()

        # upload to datalake
        uploaded_file = datalake_client.upload_file(
            destination_file_path=CountriesPath(zone="processed"),
            file=processed_data.to_pandas().to_parquet(),
        )
        return uploaded_file.file.full_path

    @staticmethod
    def curate(file_path: str):
        print(111111, file_path)
        #  download file
        file_content = datalake_client.download_file_into_memory(file_path=file_path)

        # history
        datalake_client.upload_file(
            destination_file_path=CountriesPath(zone="curated"),
            file=file_content,
        )
        # final reference
        datalake_client.upload_file(
            destination_file_path=CountriesFinalPath(),
            file=file_content,
        )
