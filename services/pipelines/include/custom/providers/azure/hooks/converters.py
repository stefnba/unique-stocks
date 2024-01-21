import duckdb
import polars as pl
import pyarrow.dataset as ds
from custom.providers.azure.hooks.types import DatasetTypeInput


class WrongDatasetInputTypeException(Exception):
    """Wrong"""


def PyArrowDataset(dataset: DatasetTypeInput) -> ds.FileSystemDataset:
    if isinstance(dataset, duckdb.DuckDBPyRelation):
        return dataset.arrow()
    if isinstance(dataset, ds.FileSystemDataset):
        return dataset

    raise WrongDatasetInputTypeException()


def LazyFrame(dataset: DatasetTypeInput) -> pl.LazyFrame:
    if isinstance(dataset, pl.LazyFrame):
        return dataset
    if isinstance(dataset, pl.DataFrame):
        return dataset.lazy()
    if isinstance(dataset, ds.FileSystemDataset):
        return pl.scan_pyarrow_dataset(dataset)
    if isinstance(dataset, duckdb.DuckDBPyRelation):
        return pl.scan_ipc(dataset.arrow())

    raise WrongDatasetInputTypeException()


def DataFrame(dataset: DatasetTypeInput) -> pl.DataFrame:
    if isinstance(dataset, pl.LazyFrame):
        return dataset.collect()
    if isinstance(dataset, pl.DataFrame):
        return dataset

    raise WrongDatasetInputTypeException()


def DuckDbRelation(dataset: DatasetTypeInput) -> duckdb.DuckDBPyRelation:
    if isinstance(dataset, pl.LazyFrame):
        return duckdb.sql("SELECT * FROM dataset")
    if isinstance(dataset, pl.DataFrame):
        return duckdb.sql("SELECT * FROM dataset")

    raise WrongDatasetInputTypeException()
