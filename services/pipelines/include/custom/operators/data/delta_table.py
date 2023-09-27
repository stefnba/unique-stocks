from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from custom.providers.azure.hooks.dataset import AzureDatasetHook
from utils.filesystem.path import Path, PathInput

from custom.operators.data.types import DeltaTableOptionsDict, PyarrowOptionsDict
from custom.providers.azure.hooks.handlers.read.azure import AzureDatasetArrowHandler
from custom.providers.delta_table.hooks.delta_table import DeltaTableHook
from custom.providers.azure.hooks.handlers.base import DatasetHandler
from custom.providers.azure.hooks import converters
from shared.types import DataLakeDatasetFileTypes
from pyarrow import dataset as ds
from typing import Optional


class WriteDeltaTableFromDatasetOperator(BaseOperator):
    """
    Writes a Delta Table using `AzureDatasetHook`. The dataset is read using the `AzureDatasetArrowHandler` handler,
    returned as a `pyarrow.Dataset` and then written via the `AzureDatasetWriteDeltaTableHandler` handler.
    """

    template_fields = ("destination_path", "dataset_path", "delta_table_options", "pyarrow_options")

    dataset_path: PathInput
    dataset_handler: type[DatasetHandler]
    destination_path: PathInput
    dataset_format: DataLakeDatasetFileTypes
    conn_id: str
    delta_table_options: Optional[DeltaTableOptionsDict]
    pyarrow_options: Optional[PyarrowOptionsDict]

    def __init__(
        self,
        task_id: str,
        adls_conn_id: str,
        dataset_path: PathInput,
        destination_path: PathInput,
        dataset_handler: type[DatasetHandler] = AzureDatasetArrowHandler,
        dataset_format: DataLakeDatasetFileTypes = "parquet",
        delta_table_options: Optional[DeltaTableOptionsDict] = None,
        pyarrow_options: Optional[PyarrowOptionsDict] = None,
        **kwargs,
    ):
        super().__init__(
            task_id=task_id,
            **kwargs,
        )

        self.dataset_handler = dataset_handler
        self.dataset_path = dataset_path
        self.dataset_format = dataset_format
        self.destination_path = destination_path
        self.conn_id = adls_conn_id
        self.delta_table_options = delta_table_options
        self.pyarrow_options = pyarrow_options

    def execute(self, context: Context) -> str:
        self.context = context

        dataset = self.read()

        return self.write(dataset=dataset)

    def read(self) -> ds.FileSystemDataset:
        """Read a dataset using `AzureDatasetArrowHandler`."""

        return AzureDatasetHook(conn_id=self.conn_id).read(
            source_path=self.dataset_path,
            dataset_converter=converters.PyArrowDataset,
            source_format=self.dataset_format,
            handler=self.dataset_handler,
            **(self.pyarrow_options or {}),
        )

    def write(self, dataset: ds.FileSystemDataset) -> str:
        """Write dataset using `DeltaTableHook`."""

        DeltaTableHook(conn_id=self.conn_id).write(
            data=dataset,
            path=self.destination_path,
            **(self.delta_table_options or {}),
        )
        return Path.create(self.destination_path).to_json()
