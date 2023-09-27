import polars as pl
import pyarrow.dataset as ds
from custom.providers.azure.hooks.handlers.base import DatasetHandler


class LocalDatasetArrowHandler(DatasetHandler):
    """Reads a dataset from local filesystem with `pyarrow.dataset`."""

    def read(self, schema=None, **kwargs) -> ds.FileSystemDataset:
        return ds.dataset(source=self.path.uri, format=self.format, schema=schema, **kwargs)


class LocalDatasetReadHandler(DatasetHandler):
    """
    Lazy scan of a local dataset with `polars`.
    Works only for `parquet` and `csv` files.
    """

    def read(self, **kwargs) -> pl.LazyFrame:
        if self.format == "parquet":
            return pl.scan_parquet(self.path.uri)
        if self.format == "csv":
            return pl.scan_csv(self.path.uri)

        raise ValueError("File format not supported.")
