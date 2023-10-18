from typing import Literal, Optional
import json
import polars as pl
from custom.providers.eod_historical_data.transformers.utils import deep_get

Period = Literal["yearly", "quarterly"]


class Schema:
    frame = [
        ("category", pl.Utf8),
        ("metric", pl.Utf8),
        ("value", pl.Utf8),
        ("currency", pl.Utf8),
        ("period", pl.Utf8),
        ("period_type", pl.Utf8),
        ("published_at", pl.Utf8),
    ]


class EoDFundamentalTransformer:
    data: dict
    security: str
    exchange: Optional[str]
    type: str

    frames: list[str]

    def __init__(self, data: str | bytes | dict) -> None:
        if isinstance(data, (str, bytes)):
            self.data = json.loads(data)
        if isinstance(data, dict):
            self.data = data

        security = deep_get(self.data, "General.Code")
        self.exchange = deep_get(self.data, "General.Exchange")
        if security:
            self.security = security

        if security is None:
            raise ValueError("Security must be specified.")

    def transform(self) -> pl.DataFrame | None:
        """Entry method to start transform. Chain fundamental frames into one DataFrame and return it."""
        frames = [
            getattr(self, f"transform_{f}")() for f in self.frames if f is not None and hasattr(self, f"transform_{f}")
        ]

        frames = [f for f in frames if f is not None]

        if len(frames) == 0:
            return None

        df = pl.concat(items=frames)

        if isinstance(df, pl.DataFrame):
            return df.with_columns(
                [
                    pl.col("period").str.to_date().keep_name(),
                    pl.col("published_at").str.to_date().keep_name(),
                    pl.lit(self.exchange).cast(pl.Utf8).alias("exchange_code"),
                    pl.lit(self.security).cast(pl.Utf8).alias("security_code"),
                    pl.lit(self.type).cast(pl.Utf8).alias("security_type"),
                ]
            )

        return None

    def _transform_from_dict(
        self,
        category: str,
        data_key: str | list[str],
        data_schema,
        unnest_columns: Optional[list[str]] = None,
    ):
        """Transform all key-value pairs of a dict into fundamental frame."""

        data = deep_get(self.data, data_key)

        if not data:
            return

        df = pl.DataFrame([data], schema=data_schema)

        if unnest_columns:
            df = df.unnest(columns=unnest_columns)

        return (
            df.melt(variable_name="metric")
            .with_columns(
                [
                    pl.lit(category).alias("category"),
                    pl.lit(None).alias("currency").cast(pl.Utf8),
                    pl.lit(None).alias("period").cast(pl.Utf8),
                    pl.lit(None).alias("period_type").cast(pl.Utf8),
                    pl.lit(None).alias("published_at").cast(pl.Utf8),
                ]
            )
            .select(
                [
                    "category",
                    "metric",
                    pl.col("value").cast(pl.Utf8),
                    "currency",
                    "period",
                    "period_type",
                    "published_at",
                ]
            )
        )

    def _transform_to_json(self, metric: str, category: str, data_key: str | list[str], data_schema):
        """
        Transform a nested JSON element into a JSON for value field of given metric, e.g. used for listings and officers.
        """

        data = deep_get(self.data, data_key)

        if not data:
            return

        data = json.dumps(pl.DataFrame(list(data.values()), schema=data_schema).to_dicts())

        return pl.DataFrame(
            {
                "category": category,
                "metric": metric,
                "value": data,
                "currency": None,
                "period": None,
                "period_type": None,
                "published_at": None,
            },
            schema=Schema.frame,
        )

    def _transform_from_records(
        self,
        category: str,
        data_keys: list[str],
        period: Period,
        id_columns=[
            "currency_symbol",
        ],
        date_column: str = "date",
        published_column: str = "filing_date",
        schema=None,
    ):
        data = deep_get(self.data, data_keys)

        if not data:
            return

        df = pl.DataFrame(
            list(data.values()),
            schema=schema,
            infer_schema_length=1_000,
        )

        if "currency_symbol" not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias("currency_symbol"))
        if published_column not in df.columns:
            df = df.with_columns(pl.lit(None).cast(pl.Utf8).alias(published_column))

        df = (
            df.melt(
                id_vars=[*[date_column, published_column], *id_columns],
                variable_name="metric",
            )
            .with_columns(
                [
                    pl.lit(category).alias("category"),
                ]
            )
            .select(
                [
                    "category",
                    "metric",
                    pl.col("value").cast(pl.Utf8),
                    pl.col("currency_symbol").cast(pl.Utf8).alias("currency"),
                    pl.col(date_column).alias("period"),
                    pl.lit(period).alias("period_type"),
                    pl.col(published_column).alias("published_at").cast(pl.Utf8),
                ]
            )
        )

        return self.extract_period(df=df, period=period)

    @staticmethod
    def extract_period(df: pl.DataFrame, column: str = "period", *, period: Period):
        period_dict = {
            "yearly": "1y",
            "quarterly": "1q",
        }

        return df.with_columns(
            [
                pl.col(column).str.to_date().dt.truncate(period_dict.get(period, "1y")).cast(pl.Utf8).alias("period"),
            ]
        )
