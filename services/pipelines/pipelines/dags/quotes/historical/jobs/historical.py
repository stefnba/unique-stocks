from dags.quotes.historical.jobs.config import HistoricalQuotesPath
from shared.clients.api.eod.client import EodHistoricalDataApiClient
from shared.clients.datalake.azure.azure_datalake import datalake_client
from shared.clients.duck.client import duck
from shared.utils.conversion import converter

ApiClient = EodHistoricalDataApiClient
ASSET_SOURCE = ApiClient.client_key


class HistoricalQuotesJobs:
    @staticmethod
    def download(security_code, exchange_code):
        quotes = ApiClient.get_historical_eod_quotes(security_code, exchange_code)

        return datalake_client.upload_file(
            destination_file_path=HistoricalQuotesPath(
                zone="raw", asset_source=ASSET_SOURCE, file_type="json", security=security_code, exchange=exchange_code
            ),
            file=converter.json_to_bytes(quotes),
        ).file.full_path

    @staticmethod
    def transform(file_path: str, security_code: str, exchange_code: str):
        import io

        import polars as pl

        data = datalake_client.download_file_into_memory(file_path)
        quotes = pl.read_json(io.BytesIO(data))

        # quotes = duck.get_data(file_path, handler="azure_abfs", format="json")

        quotes = quotes.with_columns(
            [
                pl.lit(security_code).alias("security_source_code"),
                pl.lit(exchange_code).alias("exchange_source_code"),
            ]
        )

        return datalake_client.upload_file(
            destination_file_path=HistoricalQuotesPath(
                zone="processed", asset_source=ASSET_SOURCE, security=security_code, exchange=exchange_code
            ),
            file=quotes.to_pandas().to_parquet(),
        ).file.full_path

    @staticmethod
    def curate(file_path: str):
        quotes = duck.get_data(file_path, handler="azure_abfs", format="json")

        print(quotes)
