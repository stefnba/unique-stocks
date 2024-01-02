import datetime
from dataclasses import dataclass

import polars as pl
from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from custom.providers.azure.hooks.dataset import AzureDatasetHook, DatasetConverters, DatasetHandlers
from utils.filesystem.path import AdlsPath, LocalPath, PathInput


@dataclass
class PerformancePeriod:
    reference_date: datetime.date
    period: str


PERFORMANCE_PERIODS: list[PerformancePeriod] = [
    PerformancePeriod(
        period="YEAR_TO_DATE",
        reference_date=datetime.date(datetime.datetime.now().year, 1, 1),
    ),
    PerformancePeriod(
        period="MONTH_TO_DATE",
        reference_date=datetime.date(datetime.datetime.now().year, datetime.datetime.now().month, 1),
    ),
    PerformancePeriod(
        period="LAST_12_MONTH",
        reference_date=datetime.date(
            datetime.datetime.now().year - 1, datetime.datetime.now().month, datetime.datetime.now().day
        ),
    ),
    PerformancePeriod(
        period="LAST_5_DAY", reference_date=(datetime.datetime.now() - datetime.timedelta(days=5)).date()
    ),
    PerformancePeriod(
        period="LAST_30_DAY", reference_date=(datetime.datetime.now() - datetime.timedelta(days=30)).date()
    ),
    PerformancePeriod(
        period="LAST_14_DAY", reference_date=(datetime.datetime.now() - datetime.timedelta(days=14)).date()
    ),
]


class EoDQuotePerformanceTransformOperator(BaseOperator):
    template_fields = ("dataset_path",)

    conn_id: str
    dataset_path: PathInput
    hook: AzureDatasetHook
    data: pl.LazyFrame

    def __init__(self, task_id: str, adls_conn_id: str, dataset_path: PathInput, **kwargs):
        super().__init__(
            task_id=task_id,
            **kwargs,
        )

        self.conn_id = adls_conn_id
        self.dataset_path = dataset_path

    def execute(self, context: Context):
        """Transform quotes into performance table and save"""

        self.hook = AzureDatasetHook(conn_id=self.conn_id)

        # download dataset to local filesystem
        self.download()
        self.last_quote()

        # transform
        sink_dir = LocalPath.create_temp_dir_path()
        for p in PERFORMANCE_PERIODS:
            self.transform_period(date=p.reference_date, period=p.period, sink_dir=sink_dir)

        # upload
        destination_path = self.upload(dir_path=sink_dir.uri)

        return destination_path.to_dict()

    def download(self):
        """Download quote dataset from ADLS to local filesystem."""

        dataset = self.hook.read(
            source_path=self.dataset_path,
            handler=DatasetHandlers.Read.Azure.DeltaDowonload,
            dataset_converter=DatasetConverters.LazyFrame,
        )
        if isinstance(dataset, pl.LazyFrame):
            self.data = dataset.select(
                [
                    "date",
                    "security_code",
                    "exchange_code",
                    "adjusted_close",
                ]
            )
            return

    def upload(self, dir_path: str):
        """Upload performance dataset to ADLS."""

        destination_path = AdlsPath.create_temp_file_path()
        dataset = pl.scan_parquet(source=f"{dir_path}/*.parquet")
        self.hook.write(dataset=dataset, destination_path=destination_path)

        return destination_path

    def last_quote(self):
        """Get last available quote per security_code and exchange_code."""

        last_quote = (
            self.data.group_by("security_code", "exchange_code")
            .agg(pl.all().sort_by("date", descending=False).last())
            .rename(
                {
                    "date": "base_date",
                    "adjusted_close": "base_close",
                }
            )
        )

        self.last_quote_data = last_quote.collect().lazy()

    def reference_quote(self, date: datetime.date):
        """Get reference quote per security_code and exchange_code."""

        return (
            self.data.filter(pl.col("date") <= date)
            .group_by("security_code", "exchange_code")
            .agg(
                [
                    pl.all().sort_by("date", descending=False).last(),
                ]
            )
            .rename(
                {
                    "date": "reference_date",
                    "adjusted_close": "reference_close",
                }
            )
        )

    def transform_period(self, date: datetime.date, period: str, sink_dir: PathInput) -> None:
        reference_quote_data = self.reference_quote(date=date)

        df = self.last_quote_data.join(
            reference_quote_data,
            how="inner",
            on=["security_code", "exchange_code"],
        ).with_columns(
            [
                (pl.col("base_close") / pl.col("reference_close") - 1).alias("performance"),
                pl.lit(period).alias("period"),
            ]
        )

        df.collect(streaming=True).write_parquet(file=LocalPath.create(sink_dir).to_pathlib() / f"{period}.parquet")
