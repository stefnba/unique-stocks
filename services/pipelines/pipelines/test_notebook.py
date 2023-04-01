# %%
import io
import json

import polars as pl
from dags.exchanges.exchange_details.jobs.exchange_details import ExchangeDetailsJobs
from dags.exchanges.exchange_securities.jobs.eod import EodExchangeSecurityJobs
from dags.exchanges.exchanges.jobs.eod import EodExchangeJobs
from dags.exchanges.exchanges.jobs.iso import IsoExchangeJobs
from dags.exchanges.exchanges.jobs.msk import MarketStackExchangeJobs
from dags.indices.index_members.jobs.index_members import IndexMembersJobs
from dags.quotes.historical.jobs.historical import HistoricalQuotesJobs
from shared.clients.api.eod.client import EodHistoricalDataApiClient
from shared.clients.api.open_figi.client import OpenFigiApiClient
from shared.clients.datalake.azure.azure_datalake import datalake_client
from shared.clients.db.postgres.repositories import DbQueryRepositories
from shared.clients.duck.client import duck

# %%


# get data from datalake with duck.get_data()
quotes = duck.get_data(
    "processed/product=quotes/security=AAPL/exchange=US/asset_source=EodHistoricalData/year=2023/month=04/20230401_processed_EodHistoricalData_AAPL_US.parquet",
    handler="azure_abfs",
    format="parquet",
).df()

securities = duck.get_data(
    "processed/product=exchanges/asset=exchange_securities/exchange=US/source=EodHistoricalData/year=2023/month=04/20230401_EodHistoricalData_US_exchange_securities_processed.parquet",
    handler="azure_abfs",
).df()

# execute query on duckDB with duck.query()
duck.query(
    "SELECT * FROM $quotes quotes LEFT JOIN $securities securities ON quotes.security_source_code = securities.source_code",
    # supply variables, needs to be $variable in query statement
    quotes=quotes,
    securities=securities,
).df()

# %%
# get list of parquet files from datalake, currenly not possible with .get_data()
files = ["", "", ""]
# build abfs path from pure datalake path with duck.helpers.build_abfs_path() for each path supplied
duck.db.read_parquet([duck.helpers.build_abfs_path(file) for file in files])


# %%

path = EodExchangeJobs.download_exchange_list()
df = duck.get_data(path, handler="azure_abfs", format="json").pl()


# %%

df = duck.query("SELECT * FROM $test", test=df).df()

df.head()


# %%


def get_quote_for_members_of_indices():
    """
    Uses same jobs that airflow dags use. Jobs usually return file_path from datalake that are then supplied are args to next job.
    """
    # indicies = ["GDAXI", "FTSE"]
    indicies = ["DJI", "SSMI", "NDX"]
    # indicies = ["GSPC"]

    transformed_paths = []

    for index in indicies:
        raw_path = IndexMembersJobs.download(index)
        transformed_path = IndexMembersJobs.transform(raw_path)

        if transformed_path:
            transformed_paths.append(duck.helpers.build_abfs_path(transformed_path))

    data = duck.db.read_parquet(transformed_paths)
    for member in data.df()[["code", "exchange_source_code"]].to_dict("records"):
        raw_path = HistoricalQuotesJobs.download(member["code"], member["exchange_source_code"])

        HistoricalQuotesJobs.transform(raw_path, member["code"], member["exchange_source_code"])


get_quote_for_members_of_indices()
