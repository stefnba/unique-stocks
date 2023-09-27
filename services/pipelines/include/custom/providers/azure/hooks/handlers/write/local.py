import polars as pl
from custom.providers.azure.hooks.handlers.base import DatasetHandler
from custom.providers.azure.hooks.types import Dataset


class LocalDatasetWriteHandler(DatasetHandler):
    """
    Sink a `pl.LazyFrame` dataset to local filesystem.
    """

    def write(self, dataset: Dataset, **kwargs):
        if isinstance(dataset, pl.LazyFrame):
            dataset.sink_parquet(path=self.path.uri)

        return self.path
