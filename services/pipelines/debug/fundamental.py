# %%

import duckdb
import pyarrow.dataset as ds

from adlfs import AzureBlobFileSystem

abfs = AzureBlobFileSystem("uniquestocks", anon=False)

duck = duckdb.connect()
# duck.register_filesystem(abfs)

# duck.read_json("abfs://data-lake/test*.json")

import polars as pl

# pl.read_json(file)

ds = ds.dataset("data-lake/testAAPL.json", filesystem=abfs, format="json")

duck.from_arrow(ds)

# AzureBlobFileSystem("uniquestocks", anon=False).glob("data-lake/test*.json")

# %%

from azure.storage.blob import ContainerClient, BlobClient
from azure.identity import DefaultAzureCredential
import json

data = ContainerClient(
    "https://uniquestocks.dfs.core.windows.net/",
    container_name="data-lake",
    credential=DefaultAzureCredential(),
).download_blob(blob="testAAPL.json")
fundamental_data = json.loads(data.readall())
fundamental_data.keys()
# %%

# general
general = fundamental_data.get("General")
general

# %%
if general:
    description = general.get("Description")
    print(description)


#
financials_raw = fundamental_data.get("Financials")
earnings = fundamental_data.get("Earnings")
financials_raw.keys()

# %%
financials_transformed: list = []
financials_currency = financials_raw.get("currency_symbol")
income_statement = financials_raw.get("Income_Statement")

currency = income_statement.get("currency_symbol")
currency


# %%
import polars as pl


id_vars = ["date", "filing_date", "currency_symbol", "code", "period_type", "parent"]

df = pl.DataFrame(list(income_statement.get("yearly").values()))
df.columns
value_vars = [col for col in df.columns if col not in id_vars]

df.with_columns(
    [
        pl.lit("AAPL").alias("code"),
        pl.lit("yearly").alias("period"),
    ]
).melt(id_vars=id_vars, value_vars=value_vars, variable_name="fundamental_key")


# %%
df = pl.DataFrame(list(financials_raw.get("Balance_Sheet").get("yearly").values()))
value_vars = [col for col in df.columns if col not in id_vars]

df.with_columns(
    [
        pl.lit("AAPL").alias("code"),
        pl.lit("yearly").alias("period"),
    ]
).melt(id_vars=id_vars, value_vars=value_vars, variable_name="fundamental_key")
# %%
df = pl.DataFrame(list(financials_raw.get("Cash_Flow").get("yearly").values()))
value_vars = [col for col in df.columns if col not in id_vars]

df.with_columns(
    [
        pl.lit("AAPL").alias("code"),
        pl.lit("yearly").alias("period_type"),
        pl.lit("cash_flow").alias("parent"),
    ]
).melt(id_vars=id_vars, value_vars=value_vars, variable_name="fundamental_key").with_columns(
    [
        pl.col("date").str.to_date().dt.strftime("%Y").alias("period"),
        pl.col("date").str.to_date().dt.strftime("%Y-01-01").str.to_date().alias("test"),
        pl.col("filing_date").str.to_date(),
        # pl.col("value").str.to_decimal(),
    ]
)

# %%


# %%


# df.unnest(columns=df.columns)

# %%

import json
from functools import reduce
import polars as pl
from typing import Literal, Optional, Dict

Period = Literal["yearly", "quarterly"]


def deep_get(data: dict, keys: str | list[str]):
    """Helper to get dict within nested dict."""
    if isinstance(keys, str):
        keys = keys.split(".")

    return reduce(lambda d, key: d.get(key, None) if isinstance(d, dict) else None, keys, data)


def nested_fundamental(data: dict, keys: str | list[str], return_values_as_list=False):
    """Helper that return nested dict or values of that dict as a list."""
    obj = deep_get(data=data, keys=keys)

    if obj and return_values_as_list:
        return list(obj.values())

    return obj


class EodFundamentalParser:
    data: dict
    entity_id: str

    def __init__(self, fundamental_data: str, entity_id: str) -> None:
        self.data = json.loads(fundamental_data)
        self.entity_id = entity_id

    def parse_general(self):
        KEY = "General"

        general = self.data.get(KEY, {})
        self.officers = general.pop("Officers")
        self.listings = general.pop("Listings")

        return pl.DataFrame([general]).unnest("AddressData").melt(variable_name="metric")

    def parse_income_statement(self, period: Period):
        KEY = ["Financials", "Income_Statement"]
        financials = nested_fundamental(self.data, [*KEY, period], return_values_as_list=True)
        return self._unpivot_value_list(financials, period=period)

    def parse_cash_flow(self, period: Period):
        KEY = ["Financials", "Cash_Flow"]
        financials = nested_fundamental(self.data, [*KEY, period], return_values_as_list=True)
        return self._unpivot_value_list(financials, period=period)

    def parse_balance_sheet(self, period: Period):
        KEY = ["Financials", "Balance_Sheet"]
        financials = nested_fundamental(self.data, [*KEY, period], return_values_as_list=True)
        return self._unpivot_value_list(financials, period=period)

    def parse_earnings(self, type: Literal["History", "Trend", "Annual"]):
        KEY = ["Earnings"]
        period_mapping: Dict[Literal["History", "Trend", "Annual"], Period | None] = {
            "Annual": "yearly",
            "Trend": None,
        }
        earnings = nested_fundamental(self.data, [*KEY, type], return_values_as_list=True)
        return self._unpivot_value_list(earnings, period=period_mapping.get(type, None))

    def _unpivot_value_list(self, data, period: Optional[Period]):
        df = pl.DataFrame(data)

        id_vars = [
            "date",
            "filing_date",
            "currency_symbol",
            "entity_id",
            "period_id",
            "period_name"
            # "fundamental_parent",
        ]

        df = df.with_columns(
            [
                pl.lit(self.entity_id).alias("entity_id"),
                pl.lit(period).cast(pl.Utf8).alias("period_name"),
                None,
            ]
        )
        value_vars = [col for col in df.columns if col not in id_vars]
        id_vars = [col for col in id_vars if col in df.columns]

        date_truncate_key = {"quarterly": "1q", "yearly": "1y"}

        df = (
            df.melt(id_vars=id_vars, value_vars=value_vars, variable_name="metric")
            .filter(pl.col("value").is_not_null())
            .with_columns(
                [
                    pl.col("date").str.to_date().dt.year().alias("year"),
                    pl.col("value").cast(pl.Utf8).keep_name(),
                    # pl.col("date").str.to_date().dt.quarter().alias("quarter"),
                    # pl.col("date").str.to_date().dt.month().alias("month"),
                    # pl.col("date").str.to_date().dt.week().alias("week"),
                    # pl.col("date").str.to_date().dt.day().alias("day"),
                    # pl.col("date").str.to_date().dt.strftime("%q").alias("quarter"),
                    # pl.col("date").str.to_date().dt.strftime("%Y-01-01").str.to_date().alias("period"),
                    pl.col("date")
                    .str.to_date()
                    .dt.truncate(date_truncate_key.get((period or "1y"), "1y"))
                    .alias("period"),
                ]
            )
        )

        df = df.with_columns(
            [
                (
                    pl.col("filing_date").str.to_date() if "filing_date" in df.columns else pl.lit(None).cast(pl.Date)
                ).alias("published_at"),
                (pl.col("currency_symbol") if "currency_symbol" in df.columns else pl.lit(None).cast(pl.Utf8)).alias(
                    "currency"
                ),
            ]
        )

        return df[
            [
                "currency",
                "entity_id",
                "metric",
                "value",
                # "period_id",
                "period",
                "period_name",
                "published_at",
                "year",
            ]
        ]


with open("testAAPL.json") as f:
    fundamental = EodFundamentalParser(f.read(), entity_id="2460889")


fundamental_bucket = [
    fundamental.parse_balance_sheet(period="quarterly"),
    fundamental.parse_balance_sheet(period="yearly"),
    fundamental.parse_cash_flow(period="yearly"),
    fundamental.parse_cash_flow(period="quarterly"),
    fundamental.parse_income_statement(period="quarterly"),
    fundamental.parse_income_statement(period="yearly"),
    fundamental.parse_earnings(type="Annual"),
    fundamental.parse_earnings(type="History"),
]

pl.concat(items=fundamental_bucket)


# %%

from custom.hooks.db.postgres.utils.build_query import AddQueryBuilder
import psycopg

columns = ["period", "metric", "value", "currency", "period_name"]
query = AddQueryBuilder(
    data=fundamental.parse_balance_sheet(period="quarterly")[columns].to_dicts(),
    table="data.fundamental",
    columns=columns,
).build()

with psycopg.connect("postgresql://admin:password@localhost:5871/stocks") as conn:
    conn.execute(query)

# fundamental.parse_financials(period="yearly").write_database(
#     table_name='"data"."fundamental"',
#     connection="postgresql://admin:password@localhost:5871/stocks",
#     if_exists="append",
# )
