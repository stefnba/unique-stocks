from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from custom.providers.azure.hooks.dataset import AzureDatasetHook
from utils.filesystem.data_lake.base import DataLakePathBase

from custom.operators.data.utils import extract_dataset_path
from custom.operators.data.types import DatasetPath, DeltaTableOptionsDict, PyarrowOptionsDict
from custom.providers.azure.hooks.handlers.write import AzureDatasetWriteDeltaTableHandler
from custom.providers.azure.hooks.handlers.read import AzureDatasetArrowHandler

from typing import Optional


class WriteDeltaTableFromDatasetOperator(BaseOperator):
    """
    Writes a Delta Table using `AzureDatasetHook`. The dataset is read using the `AzureDatasetArrowHandler` handler,
    returned as a `pyarrow.Dataset` and then written via the `AzureDatasetWriteDeltaTableHandler` handler.
    """

    template_fields = ("destination_path", "dataset_path", "delta_table_options", "pyarrow_options")

    dataset_path: DatasetPath
    destination_path: DatasetPath
    conn_id: str
    delta_table_options: Optional[DeltaTableOptionsDict]
    pyarrow_options: Optional[PyarrowOptionsDict]

    def __init__(
        self,
        task_id: str,
        adls_conn_id: str,
        dataset_path: DatasetPath,
        destination_path: str | DataLakePathBase,
        delta_table_options: Optional[DeltaTableOptionsDict] = None,
        pyarrow_options: Optional[PyarrowOptionsDict] = None,
        **kwargs,
    ):
        super().__init__(
            task_id=task_id,
            **kwargs,
        )

        self.dataset_path = dataset_path
        self.destination_path = destination_path
        self.conn_id = adls_conn_id
        self.delta_table_options = delta_table_options
        self.pyarrow_options = pyarrow_options

    def execute(self, context: Context):
        self.context = context
        self.hook = AzureDatasetHook(conn_id=self.conn_id)

        dataset = self.read()
        return self.write(dataset=dataset)

    def read(self):
        """Read a dataset using `AzureDatasetArrowHandler`."""
        source_path = extract_dataset_path(path=self.dataset_path, context=self.context)

        return self.hook.read(
            source_path=source_path["path"],
            source_container=source_path["container"],
            dataset_type="ArrowDataset",
            handler=AzureDatasetArrowHandler,
            **(self.pyarrow_options or {}),
        )

    def write(self, dataset):
        """Write dataset with `AzureDatasetWriteDeltaTableHandler`."""
        path = extract_dataset_path(path=self.destination_path, context=self.context)

        return self.hook.write(
            dataset=dataset,
            destination_container=path["container"],
            destination_path=path["path"],
            handler=AzureDatasetWriteDeltaTableHandler,
            **(self.delta_table_options or {}),
        )
