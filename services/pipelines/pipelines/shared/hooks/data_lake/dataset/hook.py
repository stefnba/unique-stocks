import json as jsonDecoder
from typing import Optional

import duckdb
import pandas as pd
import polars as pl
import pyarrow.dataset as ds
from shared.clients.data_lake.azure.azure_data_lake import dl_client
from shared.clients.data_lake.azure.file_system import abfs_client
from shared.hooks.data_lake.dataset import types
from shared.hooks.data_lake.dataset.utils import get_format
from shared.loggers import logger
from shared.utils.path.builder import FilePathBuilder
from shared.utils.path.data_lake.file_path import DataLakeFilePath


def upload(data: types.DataInput, path: types.Path, format: Optional[types.DataFormat] = None) -> str:
    """
    Upload a dataset of common formats like pl.DataFrame or pd.DataFrame to Data Dake with AzureDatalakeHook.

    Formats of data can include:
    - duckdb.DuckDBPyRelation
    - pl.DataFrame
    - pd.DataFrame
    - JSON or other strings

    Args:
        data (types.Data): Data to be uploaded.
        path (str): File path for uploaded file.
        format (types.FileFormat, optional): File format in which data should be saved. Defaults to "parquet".

    Returns:
        str: File path of uploaded file.
    """
    if isinstance(data, pl.DataFrame):
        _data = data.to_pandas()
    elif isinstance(data, pd.DataFrame):
        _data = data
    elif isinstance(data, str) or isinstance(data, dict) or isinstance(data, list):
        _data = pd.DataFrame(data)
    elif isinstance(data, duckdb.DuckDBPyRelation):
        _data = data.df()

    _path = FilePathBuilder.convert_to_file_path(path)
    format = get_format(_path, format)

    if format == "parquet":
        file = _data.to_parquet(index=False)
    elif format == "csv":
        file = (_data.to_csv(index=False)).encode()
    elif format == "json":
        file = str(
            _data.to_json(
                orient="records",
            )
        ).encode()

    uploaded_file = dl_client.upload_file(file=file, destination_file_path=_path)
    return uploaded_file.file.full_path


class Dataset:
    _dataset: pl.DataFrame

    def __init__(self, dataset: pl.DataFrame) -> None:
        self._dataset = dataset

    def to_polars_df(self) -> pl.DataFrame:
        """
        Convert dataset to Polars DataFrame.

        Returns:
            pl.DataFrame: Dataset.
        """
        return self._dataset

    def to_duck(self):
        return duckdb.from_df(self._dataset.to_pandas())

    def to_arrow(self):
        """
        Convert dataset to Arrow Table.

        Returns:
            pd.DataFrame: Dataset.
        """
        return self._dataset.to_arrow()

    def to_pandas_df(self) -> pd.DataFrame:
        """
        Convert dataset to Pandas DataFrame.

        Returns:
            pd.DataFrame: Dataset.
        """
        return self._dataset.to_pandas()

    def to_json(self) -> str:
        """
        Convert dataset to JSON.

        Returns:
            str: Dataset.
        """
        return jsonDecoder.dumps(self._dataset.to_dicts())

    def to_csv(self) -> str:
        """
        Convert dataset to JSON.

        Returns:
            str: Dataset.
        """
        return str(self._dataset.to_pandas().to_csv())

    @property
    def data(self) -> pl.DataFrame:
        """
        Returns downloaded dataset as Polars DataFrame.

        Returns:
            pl.DataFrame: Dataset
        """
        return self._dataset


def _download_json(path: str):
    """
    Download data in json format from Data Lake.
    """
    import json

    handler = None

    duck = duckdb.connect()
    duck.register_filesystem(abfs_client)

    try:
        handler = "duckdb"
        data = duck.read_json(DataLakeFilePath.build_abfs_path(path)).pl()

    except Exception as error:
        logger.datalake.info(
            str(error),
            event=logger.datalake.events.DOWNLOAD_ERROR,
            extra={"path": path, "format": "json", "handler": handler},
        )

        handler = "dl_client"
        data = pl.DataFrame(json.loads(dl_client.download_file_into_memory(path)))

    logger.datalake.info(
        "", event=logger.datalake.events.DOWNLOAD_SUCCESS, extra={"path": path, "format": "json", "handler": handler}
    )
    return data


def _download_parquet(path: str, format: types.DataFormat) -> pl.DataFrame:
    """
    Download json files from Data Lake.
    """
    try:
        duck = duckdb
        duck.register_filesystem(abfs_client)
        data = duck.read_parquet(DataLakeFilePath.build_abfs_path(path))
        return data.pl()
    except Exception:
        data = _download_with_arrow(path, format)
    return pl.DataFrame(data)


def _download_csv(path: str, format: types.DataFormat) -> pl.DataFrame:
    """
    Download data in csv format from Data Lake.
    """
    try:
        duck = duckdb
        duck.register_filesystem(abfs_client)
        data = duck.read_csv(DataLakeFilePath.build_abfs_path(path))
        return data.pl()
    except Exception:
        data = _download_with_arrow(path, format)
    return pl.DataFrame(data)


def _download_with_arrow(path: str, format: types.DataFormat):
    """_summary_

    Returns:
        _type_: _description_
    """
    table = ds.dataset(DataLakeFilePath.build_abfs_path(path), filesystem=abfs_client, format=format).to_table()
    return table


def download(path: types.Path, format: Optional[types.DataFormat] = None) -> Dataset:
    """
    Downloads a dataset located in the Data Lake.

    Args:
        path (types.Path): _description_
        format (Optional[types.FileFormat], optional): _description_. Defaults to None.

    Raises:
        Exception: _description_

    Returns:
        _type_: _description_
    """
    _path = FilePathBuilder.convert_to_file_path(path)
    _format = get_format(_path, format)

    logger.datalake.info(event=logger.datalake.events.DOWNLOAD_INIT, extra={"path": _path, "format": _format})

    data = pl.DataFrame()
    if _format == "csv":
        data = _download_csv(_path, _format)

    elif _format == "json":
        data = _download_json(_path)

    elif _format == "parquet":
        data = _download_parquet(_path, _format)

    return Dataset(data)
