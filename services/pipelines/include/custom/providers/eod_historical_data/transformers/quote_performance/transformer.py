from dataclasses import dataclass
import datetime
from utils.filesystem.path import AdlsPath, LocalPath
import polars as pl
from custom.providers.azure.hooks.dataset import AzureDatasetHook


@dataclass
class PerformancePeriod:
    reference_date: datetime.date
    period: str


class PerformanceTransformer:
    periods: list[PerformancePeriod] = [
        PerformancePeriod(period="YEAR_TO_DATE", reference_date=datetime.date(2023, 1, 1)),
        PerformancePeriod(period="LAST_12_MONTH", reference_date=datetime.date(2022, 10, 1)),
    ]
    sink_dir: LocalPath

    def __init__(self) -> None:
        self.data = self.download_data()
        self.sink_dir = LocalPath.create_temp_dir_path()

    def download_data(self):
        # pl.scan_delta("azure://curated/security_quote", storage_options=storage_options).select(
        #     ["date", "security_code", "exchange_code", "adjusted_close"]
        # ).collect().write_parquet("test.parquet")

        return pl.scan_parquet("test.parquet")

    def upload(self):
        """Upload performance dataset to tmp zone."""
        ...

    @classmethod
    def transform(cls):
        """Transform quotes into performance table and save"""
        t = cls()
        [t.join(date=p.reference_date, period=p.period) for p in t.periods]

        t.upload()

        return t.sink_dir
