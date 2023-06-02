# %%
import sys
import os

sys.path.append("..")
os.chdir("..")

# %%
from shared.hooks.data_lake import data_lake_hooks
from typing import TypedDict


class MergePaths(TypedDict):
    eod_historical_data: str
    market_stack: str
    iso_mic: str


def eod_historical_data():
    from dags.exchange.eod_historical_data import jobs
    from dags.exchange.path import ExchangePath

    raw = data_lake_hooks.checkout(
        func=jobs.ingest,
        commit_path=ExchangePath.raw(source=jobs.ASSET_SOURCE, file_type="json"),
    )
    transformed = data_lake_hooks.checkout(
        checkout_path=raw,
        func=jobs.transform,
        commit_path=ExchangePath.processed(source=jobs.ASSET_SOURCE),
    )
    return transformed


def market_stack():
    from dags.exchange.market_stack import jobs
    from dags.exchange.path import ExchangePath

    raw = data_lake_hooks.checkout(
        func=jobs.ingest,
        commit_path=ExchangePath.raw(source=jobs.ASSET_SOURCE, file_type="json"),
    )
    transformed = data_lake_hooks.checkout(
        checkout_path=raw,
        func=jobs.transform,
        commit_path=ExchangePath.processed(source=jobs.ASSET_SOURCE),
    )
    return transformed


def iso_mic():
    from dags.exchange.iso_mic import jobs
    from dags.exchange.path import ExchangePath

    raw = data_lake_hooks.checkout(
        func=jobs.ingest,
        commit_path=ExchangePath.raw(source=jobs.ASSET_SOURCE, file_type="csv"),
    )
    transformed = data_lake_hooks.checkout(
        checkout_path=raw,
        func=jobs.transform,
        commit_path=ExchangePath.processed(source=jobs.ASSET_SOURCE),
    )
    return transformed


def merge(file_paths: MergePaths):
    from dags.exchange.path import ExchangePath
    from dags.exchange.shared.jobs import merge
    from shared.hooks.data_lake import data_lake_hooks

    merged = merge(
        {
            "eod_historical_data": data_lake_hooks.download(file_paths["eod_historical_data"]).to_polars_df(),
            "iso_mic": data_lake_hooks.download(file_paths["iso_mic"]).to_polars_df(),
            "market_stack": data_lake_hooks.download(file_paths["market_stack"]).to_polars_df(),
        }
    )

    return data_lake_hooks.upload(path=ExchangePath.temp(), data=merged)


def add_surr_keys(file_path: str):
    from dags.exchange.path import ExchangePath
    from dags.exchange.shared.jobs import add_surr_keys
    from shared.hooks.data_lake import data_lake_hooks

    return data_lake_hooks.checkout(
        checkout_path=file_path, func=lambda data: add_surr_keys(data), commit_path=ExchangePath.temp()
    )


def load_into_db(file_path: str):
    from dags.exchange.shared.jobs import load_into_db
    from shared.hooks.data_lake import data_lake_hooks

    return data_lake_hooks.checkout(
        checkout_path=file_path, func=lambda data: load_into_db(data), commit_path="test.parquet"
    )


tmp = merge({"eod_historical_data": eod_historical_data(), "iso_mic": iso_mic(), "market_stack": market_stack()})

tmp = add_surr_keys(tmp)

data_lake_hooks.download(tmp).to_polars_df()
