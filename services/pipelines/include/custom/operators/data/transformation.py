from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
import duckdb
import polars as pl
from typing import Dict, Optional, TypeAlias, Any, Callable

from string import Template
from shared.types import DataLakeDataFileTypes
from utils.dag.xcom import XComGetter
from custom.providers.azure.hooks.dataset import AzureDatasetHook
from custom.operators.data.utils import extract_dataset_path
from custom.operators.data.types import DatasetPath
from custom.providers.azure.hooks.handlers.base import DatasetReadBaseHandler, DatasetWriteBaseHandler
from custom.providers.azure.hooks.handlers.read import AzureDatasetReadHandler
from custom.providers.azure.hooks.handlers.write import AzureDatasetWriteUploadHandler
from dataclasses import dataclass
from utils.file.type import get_dataset_format


@dataclass
class DataBindingCustomHandler:
    """Specify custom handler per data binding."""

    path: DatasetPath
    container: Optional[str] = None
    handler: Optional[type[DatasetReadBaseHandler]] = AzureDatasetReadHandler
    format: Optional[DataLakeDataFileTypes] = None

    template_fields: tuple[str, ...] = ("path", "container")


DataBindingArgs: TypeAlias = Dict[str, DatasetPath | DataBindingCustomHandler]
QueryArgs: TypeAlias = Dict[str, str | None | int | bool | list | XComGetter]


class DuckDbTransformationOperator(BaseOperator):
    destination_path: DatasetPath
    destination_container: Optional[str]
    adls_conn_id: str
    query: str
    data_bindings: Optional[DataBindingArgs]
    query_args: Optional[QueryArgs]
    duck: duckdb.DuckDBPyConnection
    context: Context

    template_fields = ("destination_path", "destination_container", "query_args", "data_bindings", "query")
    template_ext = (".sql",)

    def __init__(
        self,
        task_id: str = "transform",
        *,
        query: str,
        adls_conn_id: str,
        destination_path: DatasetPath,
        destination_container: Optional[str] = None,
        data: Optional[DataBindingArgs] = None,
        query_args: Optional[QueryArgs] = None,
        **kwargs,
    ):
        super().__init__(task_id=task_id, **kwargs)

        self.adls_conn_id = adls_conn_id
        self.query = query
        self.query_args = query_args
        self.data_bindings = data

        self.destination_path = destination_path
        self.destination_container = destination_container

    def execute(self, context: Context):
        """Main method executed by Airflow."""

        self.context = context

        self._init_db()

        self.hook = AzureDatasetHook(conn_id=self.adls_conn_id)

        data_bindings = self._register_data_bindings()

        if data_bindings is not None and len(data_bindings) == 0:
            return

        query = self._build_query(
            self.query,
            bindings={
                **(data_bindings or {}),
                **{key: self._parse_one_param_binding(value) for key, value in (self.query_args or {}).items()},
            },
        )

        # run duckdb query
        transformed_data = self.duck.sql(query)

        # save and return file path
        return self.write_data(data=transformed_data)

    def _register_data_bindings(self):
        """Register all data bindings as duckdb view and return dict with references to provided binding keys."""

        if self.data_bindings:
            bindings = {
                key: self._register_one_data_binding(data_item)
                for key, data_item in self.data_bindings.items()
                if data_item is not None
            }
            return bindings

    def _register_one_data_binding(self, data_binding: DatasetPath | DataBindingCustomHandler):
        """Register one data binding as duckdb view and return binding key with reference."""

        # turn dataset into polars.LazyFrame
        lf = self._collect_dataset(data_binding)

        # reference LazyFrame based on its identity
        reference = f"df_{id(lf)}"

        # register view
        self.duck.register(reference, lf)

        return reference

    def _collect_dataset(self, data_item: DatasetPath | DataBindingCustomHandler) -> pl.LazyFrame:
        """Convert various DataFrames and remote files into polars.LazyFrame."""

        handler: type[DatasetReadBaseHandler] = AzureDatasetReadHandler
        container = None

        if isinstance(data_item, DataBindingCustomHandler):
            if data_item.handler:
                handler = data_item.handler
            if data_item.container:
                container = data_item.container
            data_item = data_item.path

        path = extract_dataset_path(path=data_item, context=self.context)
        file_format = get_dataset_format(path["path"])

        return self.hook.read(
            source_path=path["path"],
            source_format=file_format,
            source_container=container or path["container"],
            dataset_type="PolarsLazyFrame",
            handler=handler,
        )

    def _build_query(self, query: str, bindings: Dict[str, Any]) -> str:
        """"""

        return Template(query).safe_substitute(bindings)

    def _parse_one_param_binding(self, value):
        if isinstance(value, str):
            return f"'{value}'"

        if isinstance(value, XComGetter):
            xcom_value = value.parse(self.context)
            return self._parse_one_param_binding(xcom_value)

        return value

    def _init_db(self):
        self.duck = duckdb.connect(":memory:")

    def write_data(self, data: duckdb.DuckDBPyRelation):
        path = extract_dataset_path(path=self.destination_path, context=self.context)
        container = self.destination_container or path["container"]

        self.hook.write(
            dataset=data,
            destination_path=path["path"],
            destination_container=container,
        )

        return {
            "path": path["path"],
            "container": container,
        }


class LazyFrameTransformationOperator(BaseOperator):
    dataset_path: DatasetPath
    dataset_format: DataLakeDataFileTypes
    destination_path: DatasetPath
    transformation: Callable[[pl.LazyFrame], pl.LazyFrame]
    conn_id: str
    context: Context
    dataset_handler: Optional[type[DatasetReadBaseHandler]]

    template_fields = ("dataset_path", "destination_path")

    def __init__(
        self,
        task_id: str,
        adls_conn_id: str,
        dataset_path: DatasetPath,
        dataset_format: DataLakeDataFileTypes = "parquet",
        dataset_handler: Optional[type[DatasetReadBaseHandler]] = None,
        write_handler: Optional[type[DatasetWriteBaseHandler]] = None,
        *,
        destination_path: DatasetPath,
        transformation: Callable[[pl.LazyFrame], pl.LazyFrame],
        **kwargs,
    ):
        super().__init__(
            task_id=task_id,
            **kwargs,
        )

        self.dataset_handler = dataset_handler
        self.write_handler = write_handler
        self.dataset_path = dataset_path
        self.destination_path = destination_path
        self.transformation = transformation
        self.dataset_format = dataset_format
        self.conn_id = adls_conn_id

    def execute(self, context: Context) -> Any:
        self.context = context

        self.hook = AzureDatasetHook(conn_id=self.conn_id)

        path = extract_dataset_path(path=self.dataset_path, context=self.context)

        # get dataset
        df = self.hook.read(
            source_path=path["path"],
            source_container=path["container"],
            source_format=self.dataset_format,
            dataset_type="PolarsLazyFrame",
            handler=self.dataset_handler or AzureDatasetReadHandler,
        )

        # transform
        transformed_data = self.transformation(df)

        # upload dataset
        return self.write(dataset=transformed_data)

    def write(self, dataset: pl.LazyFrame):
        path = extract_dataset_path(path=self.destination_path, context=self.context)

        self.hook.write(
            dataset=dataset,
            destination_path=path["path"],
            destination_container=path["container"],
            handler=self.write_handler or AzureDatasetWriteUploadHandler,
        )

        return path
