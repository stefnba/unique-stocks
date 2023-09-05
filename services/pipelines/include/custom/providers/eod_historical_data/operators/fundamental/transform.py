from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from typing import TypeAlias, Literal, Optional, Dict
from custom.providers.azure.hooks.data_lake_storage import AzureDataLakeStorageHook
from custom.providers.azure.hooks.dataset import AzureDatasetHook
from utils.dag.xcom import XComGetter
import json
import polars as pl
from functools import reduce

SourcePath: TypeAlias = str | XComGetter
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


class EodFundamentalTransformOperator(BaseOperator):
    template_fields = ("source_path", "entity_id", "destination_path")

    context: Context
    source_path: SourcePath
    adls_conn_id: str
    entity_id: str

    def __init__(
        self, task_id: str, adls_conn_id: str, entity_id: str, source_path: SourcePath, destination_path: str, **kwargs
    ):
        super().__init__(
            task_id=task_id,
            **kwargs,
        )

        self.entity_id = entity_id
        self.source_path = source_path
        self.conn_id = adls_conn_id
        self.destination_path = destination_path

    def execute(self, context: Context):
        self.context = context

        source_path = self.source_path

        if isinstance(source_path, XComGetter):
            source_path = source_path.parse(context=context)

            if not isinstance(source_path, str):
                raise ValueError("dataset_path must be a string")

        hook = AzureDataLakeStorageHook(conn_id=self.conn_id)
        data = hook.read_blob(blob_path=source_path, container=hook.container)

        fundamental = EodFundamentalParser(fundamental_data=data, entity_id=self.entity_id)

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

        df = pl.concat(items=fundamental_bucket)

        AzureDatasetHook(conn_id=self.conn_id).write(dataset=df.lazy(), destination_path=self.destination_path)

        return self.destination_path


class EodFundamentalParser:
    data: dict
    entity_id: str

    def __init__(self, fundamental_data: str | bytes, entity_id: str) -> None:
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
                    pl.col("filing_date").cast(pl.Utf8).str.to_date()
                    if "filing_date" in df.columns
                    else pl.lit(None, dtype=pl.Utf8).cast(pl.Date)
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
