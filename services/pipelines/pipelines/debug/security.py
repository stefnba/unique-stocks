# %%
import sys
import os

sys.path.append("..")
os.chdir("..")

# %%
from dags.exchange.eod_historical_data import jobs
from typing import TypedDict


class ExchangeDataset(TypedDict):
    exchange_code: str
    file_path: str


def extract_exchange_codes():
    from dags.security.eod_historical_data.jobs import extract

    return extract()


def ingest(exchange: str) -> ExchangeDataset:
    from dags.security.eod_historical_data.jobs import ASSET_SOURCE, ingest
    from dags.security.path import SecurityPath
    from shared.hooks.data_lake import data_lake_hooks

    file_path = data_lake_hooks.checkout(
        func=lambda: ingest(exchange),
        commit_path=SecurityPath.raw(source=ASSET_SOURCE, bin=exchange, file_type="json"),
    )

    return {
        "exchange_code": exchange,
        "file_path": file_path,
    }


def transform(dataset: ExchangeDataset):
    from dags.security.eod_historical_data.jobs import ASSET_SOURCE, transform
    from dags.security.path import SecurityPath
    from shared.hooks.data_lake import data_lake_hooks

    return data_lake_hooks.checkout(
        checkout_path=dataset["file_path"],
        func=lambda data: transform(data),
        commit_path=SecurityPath.processed(source=ASSET_SOURCE, bin=dataset["exchange_code"]),
    )


def map_figi(file_path: str):
    from dags.security.path import SecurityPath
    from shared.hooks.data_lake import data_lake_hooks

    from dags.security.eod_historical_data.jobs import map_figi

    return data_lake_hooks.checkout(
        checkout_path=file_path,
        func=lambda data: map_figi(data),
        commit_path=SecurityPath.temp(),
    )


def extract_security(file_path: str):
    from dags.security.eod_historical_data.jobs import extract_security
    from shared.hooks.data_lake import data_lake_hooks
    from dags.security.path import SecurityPath

    return data_lake_hooks.checkout(
        checkout_path=file_path, func=lambda data: extract_security(data), commit_path=SecurityPath.temp()
    )


def load_security_into_database(file_path: str):
    from dags.security.eod_historical_data.jobs import load_ecurity_into_database
    from shared.hooks.data_lake import data_lake_hooks

    data_lake_hooks.checkout(checkout_path=file_path, func=lambda data: load_ecurity_into_database(data))


def extract_security_ticker(file_path: str):
    from dags.security.eod_historical_data.jobs import extract_security_ticker
    from shared.hooks.data_lake import data_lake_hooks

    data_lake_hooks.checkout(
        checkout_path=file_path,
        func=lambda data: extract_security_ticker(data),
    )


def extract_security_listing(file_path: str):
    from dags.security.eod_historical_data.jobs import extract_security_listing
    from shared.hooks.data_lake import data_lake_hooks

    data_lake_hooks.checkout(
        checkout_path=file_path,
        func=lambda data: extract_security_listing(data),
    )


# codes = ["XETRA", "NYSE"]
# codes = ["XETRA", "NASDAQ", "LSE", "TO", "MU", "HA", "MC"]
codes = ["BE", "NEO", "F", "DU", "PA", "BR", "AS"]
for code in codes:
    print(code)
    exchange = ingest(code)

    print(exchange)

    path = transform(exchange)
    path_data = map_figi(path)

    path_security = extract_security(path_data)
    path_security_ticker = extract_security_ticker(path_data)

    load_security_into_database(path_security)

print("Done")
# %%


# %%

ingest("NYSE")


# %%

from shared.hooks.data_lake import data_lake_hooks

url = "/zone=raw/product=security/exchange=BE/source=EodHistoricalData/year=2023/month=07/day=04/20230704_104404__EodHistoricalData__security__BE__raw.json"


df = data_lake_hooks.download(url).to_polars_df()

df

# %%
df.filter(pl.col("Code") == "PRH")
# %%

df["Type"].unique()
