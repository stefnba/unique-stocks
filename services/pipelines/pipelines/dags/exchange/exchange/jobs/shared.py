from typing import TypedDict

import polars as pl
from dags.exchange.exchange.jobs.utils import ExchangePath
from shared.clients.data_lake.azure.azure_data_lake import dl_client
from shared.clients.db.postgres.repositories import DbQueryRepositories
from shared.clients.duck.client import duck
from shared.jobs.surrogate_keys.jobs import map_surrogate_keys


class ExchangeSources(TypedDict):
    eod_exchange_path: str
    iso_exchange_path: str
    msk_exchange_path: str


class SharedExchangeJobs:
    @staticmethod
    def curate(file_path: str):
        # add to mic
        exchange_id = map_surrogate_keys(data=file_path, product="exchange")
        # add to operating mic
        operating_exchange_id = map_surrogate_keys(
            data=exchange_id, product="exchange", uid_col_name="operating_mic", id_col_name="operating_exchange_id"
        )
        operating_exchange_id = operating_exchange_id.with_columns(
            pl.when(pl.col("id") == pl.col("operating_exchange_id"))
            .then(None)
            .otherwise(pl.col("operating_exchange_id"))
            .keep_name()
        )

        data = operating_exchange_id.to_pandas().to_parquet()

        dl_client.upload_file(file=data, destination_file_path=ExchangePath.curated(version="history"))
        return dl_client.upload_file(file=data, destination_file_path=ExchangePath.curated()).file.full_path

    @staticmethod
    def merge(file_paths: ExchangeSources):
        """
        Merge exchange from eod, iso and msk together into one file.
        """

        return file_paths["iso_exchange_path"]

        merged = duck.query(
            "./sql/merge_exchanges.sql",
            data_iso=duck.get_data(file_paths["iso_exchange_path"], handler="azure_abfs"),
            data_eod=duck.get_data(file_paths["eod_exchange_path"], handler="azure_abfs"),
            data_msk=duck.get_data(file_paths["msk_exchange_path"], handler="azure_abfs"),
        )

        return dl_client.upload_file(
            destination_file_path=ExchangePath.curated(),
            file=merged.df().to_parquet(),
        ).file.full_path

    @staticmethod
    def load(file_path: str):
        """
        Load into database.
        """
        data = duck.get_data(file_path, handler="azure_abfs").pl()
        DbQueryRepositories.exchange.add(data=data.to_dicts())
