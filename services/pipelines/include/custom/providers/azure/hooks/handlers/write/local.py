import polars as pl
from custom.providers.azure.hooks.handlers.base import DatasetHandler
from custom.providers.azure.hooks.types import Dataset
from pyarrow import dataset as ds
import pyarrow as pa


class LocalDatasetWriteHandler(DatasetHandler):
    """
    Sink a `pl.LazyFrame` dataset to local filesystem.
    """

    def write(self, dataset: Dataset, **kwargs):
        if isinstance(dataset, pl.LazyFrame):
            dataset.sink_parquet(path=self.path.uri)

        return self.path


class LocalDatasetWriteArrowHandler(DatasetHandler):
    """Write a dataset to local filesystem using `pyarrow.Dataset.write_dataset()`."""

    def write(
        self,
        dataset: Dataset,
        existing_data_behavior="error",
        basename_template=None,
        partitioning_flavor="hive",
        format="parquet",
        **kwargs,
    ):
        ds.write_dataset(
            data=dataset,
            base_dir=self.path.uri,
            format=format or self.format,
            existing_data_behavior=existing_data_behavior,
            partitioning_flavor=partitioning_flavor,
            basename_template=basename_template,
            schema=pa.schema(
                [
                    pa.field("date", pa.string()),
                    pa.field("open", pa.float64()),
                    pa.field("high", pa.float64()),
                    pa.field("low", pa.float64()),
                    pa.field("close", pa.float64()),
                    pa.field("adjusted_close", pa.float64()),
                    pa.field("volume", pa.int64()),
                    pa.field("exchange_code", pa.string()),
                    pa.field("security_code", pa.string()),
                ]
            ),
            **kwargs,
        )

        return self.path
