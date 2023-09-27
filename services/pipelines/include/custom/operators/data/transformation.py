from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
import duckdb
import polars as pl
from typing import Dict, Optional, TypeAlias, Any, Callable

from string import Template
from shared.types import DataLakeDatasetFileTypes
from utils.dag.xcom import XComGetter
from custom.providers.azure.hooks.dataset import AzureDatasetHook
from utils.filesystem.path import PathInput
from custom.providers.azure.hooks.handlers.base import DatasetHandler
from custom.providers.azure.hooks.handlers.read.azure import AzureDatasetReadHandler
from custom.providers.azure.hooks.handlers.write.azure import AzureDatasetWriteUploadHandler
from dataclasses import dataclass
from custom.providers.azure.hooks.types import DatasetConverter
from custom.providers.azure.hooks import converters
from utils.filesystem.path import Path


@dataclass
class DataBindingCustomHandler:
    """Specify custom handler per data binding."""

    path: PathInput
    handler: type[DatasetHandler] = AzureDatasetReadHandler
    format: DataLakeDatasetFileTypes = "parquet"
    dataset_converter: Optional[DatasetConverter] = None

    template_fields: tuple[str, ...] = ("path",)


DataBindingArgs: TypeAlias = Dict[str, PathInput | DataBindingCustomHandler]
QueryArgs: TypeAlias = Dict[str, str | None | int | bool | list | XComGetter]


class DuckDbTransformationOperator(BaseOperator):
    destination_path: PathInput
    adls_conn_id: str
    query: str
    data_bindings: Optional[DataBindingArgs]
    query_args: Optional[QueryArgs]
    duck: duckdb.DuckDBPyConnection
    context: Context

    template_fields = ("destination_path", "query_args", "data_bindings", "query")
    template_ext = (".sql",)

    def __init__(
        self,
        task_id: str = "transform",
        *,
        query: str,
        adls_conn_id: str,
        destination_path: PathInput,
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

    def _register_one_data_binding(self, data_binding: PathInput | DataBindingCustomHandler):
        """Register one data binding as duckdb view and return binding key with reference."""

        # turn dataset into polars.LazyFrame
        lf = self._collect_dataset(data_binding)

        # reference LazyFrame based on its identity
        reference = f"df_{id(lf)}"

        # register view
        self.duck.register(reference, lf)

        return reference

    def _collect_dataset(self, data_item: PathInput | DataBindingCustomHandler):
        """Convert various DataFrames and remote files into polars.LazyFrame."""

        if isinstance(data_item, DataBindingCustomHandler):
            return self.hook.read(
                source_path=data_item.path,
                source_format=data_item.format,
                dataset_converter=data_item.dataset_converter,
                handler=data_item.handler,
            )

        path = Path.create(data_item)

        return self.hook.read(
            source_path=path.uri, source_format=path.dataset_format, dataset_converter=converters.LazyFrame
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
        self.hook.write(
            dataset=data,
            destination_path=self.destination_path,
        )

        return Path.create(self.destination_path).to_json()


class LazyFrameTransformationOperator(BaseOperator):
    dataset_path: PathInput
    dataset_format: DataLakeDatasetFileTypes
    destination_path: PathInput
    transformation: Callable[[pl.LazyFrame], pl.LazyFrame]
    conn_id: str
    context: Context
    dataset_handler: Optional[type[DatasetHandler]]

    template_fields = ("dataset_path", "destination_path")

    def __init__(
        self,
        task_id: str,
        adls_conn_id: str,
        dataset_path: PathInput,
        dataset_format: DataLakeDatasetFileTypes = "parquet",
        dataset_handler: Optional[type[DatasetHandler]] = None,
        write_handler: Optional[type[DatasetHandler]] = None,
        *,
        destination_path: PathInput,
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

        # get dataset
        df = self.hook.read(
            source_path=self.dataset_path,
            source_format=self.dataset_format,
            dataset_converter=converters.DuckDbRelation,
        )

        # transform
        transformed_data = self.transformation(df)

        # upload dataset
        return self.write(dataset=transformed_data)

    def write(self, dataset: pl.LazyFrame):
        self.hook.write(
            dataset=dataset,
            destination_path=self.destination_path,
            handler=self.write_handler or AzureDatasetWriteUploadHandler,
        )

        return self.destination_path
