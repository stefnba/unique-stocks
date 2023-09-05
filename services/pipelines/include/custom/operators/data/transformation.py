from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
import duckdb
import polars as pl
from typing import Dict, Optional, TypeAlias, Any, TypedDict, Callable
from string import Template
from shared.types import DataLakeDataFileTypes
from utils.dag.xcom import XComGetter
from custom.providers.azure.hooks.dataset import AzureDatasetHook
from utils.filesystem.data_lake.base import DataLakePathBase


class DataBindingDict(TypedDict):
    container: str
    path: str


DatasetPath: TypeAlias = str | XComGetter | DataLakePathBase | DataBindingDict
DataBindingItem: TypeAlias = str | DataBindingDict
DataBindingArgs: TypeAlias = Dict[str, DataBindingItem]
QueryArgs: TypeAlias = Dict[str, str | None | int | bool | list | XComGetter]


class DuckDbTransformationOperator(BaseOperator):
    destination_path: str | DataLakePathBase
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
        destination_path: str | DataLakePathBase,
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

    def _collect_dataset(self, data_item: DataBindingItem) -> pl.LazyFrame:
        """Convert various DataFrames and remote files into polars.LazyFrame."""

        def file_ext(file_name: str) -> DataLakeDataFileTypes:
            if file_name.endswith(".parquet"):
                return "parquet"
            if file_name.endswith(".csv"):
                return "csv"
            if file_name.endswith(".json"):
                return "json"

            raise Exception("File Type not supported.")

        if isinstance(data_item, dict):
            container = data_item["container"]
            path = data_item["path"]
            file_format = file_ext(path)

            return self.hook.read(
                source_path=path,
                source_format=file_format,
                source_container=container,
                dataset_type="PolarsLazyFrame",
            )

        if isinstance(data_item, str):
            file_format = file_ext(data_item)

            return self.hook.read(source_path=data_item, source_format=file_format, dataset_type="PolarsLazyFrame")
        # if isinstance(data_item, LazyXComAccess):
        #     data_item = data_item[0]
        #     # data_item[self.context]
        #     # count = data_item._len
        #     # if count and count > 1:
        #     #     raise Exception("only one item supported")
        #     # data_item = data_item[0]

        # return self.hook.read(
        #     source_path=data_item, source_format=file_format, dataset_type="PolarsLazyFrame", container="temp"
        # )

    def _register_one_data_binding(self, data_binding: DataBindingItem):
        """Register one data binding as duckdb view and return binding key with reference."""

        # turn dataset into polars.LazyFrame
        lf = self._collect_dataset(data_binding)

        # reference LazyFrame based on its identity
        reference = f"df_{id(lf)}"

        # register view
        self.duck.register(reference, lf)

        return reference

    def _register_data_bindings(self):
        """Register all data bindings as duckdb view and return dict with references to provided binding keys."""

        if self.data_bindings:
            bindings = {
                key: self._register_one_data_binding(data_item) for key, data_item in self.data_bindings.items()
            }
            return bindings

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
            destination_container=self.destination_container,
        )

        return self.destination_container


class LazyFrameTransformationOperator(BaseOperator):
    dataset_path: DatasetPath
    dataset_format: DataLakeDataFileTypes
    destination_path: DatasetPath
    transformation: Callable[[pl.LazyFrame], pl.LazyFrame]
    conn_id: str
    context: Context

    template_fields = ("dataset_path", "destination_path")

    def __init__(
        self,
        task_id: str,
        adls_conn_id: str,
        dataset_path: DatasetPath,
        dataset_format: DataLakeDataFileTypes = "parquet",
        *,
        destination_path: str,
        transformation: Callable[[pl.LazyFrame], pl.LazyFrame],
        **kwargs,
    ):
        super().__init__(
            task_id=task_id,
            **kwargs,
        )

        self.dataset_path = dataset_path
        self.destination_path = destination_path
        self.transformation = transformation
        self.dataset_format = dataset_format
        self.conn_id = adls_conn_id

    def execute(self, context: Context) -> Any:
        self.context = context

        self.hook = AzureDatasetHook(conn_id=self.conn_id)

        dataset_path = self.dataset_path

        container = None
        path = None

        if isinstance(dataset_path, XComGetter):
            dataset_path = dataset_path.parse(context=context)

            if isinstance(dataset_path, dict) and "path" in dataset_path and "container" in dataset_path:
                container = dataset_path["container"]
                path = dataset_path["path"]

            if isinstance(dataset_path, str):
                raise ValueError("Container is missing")

        if isinstance(dataset_path, DataLakePathBase):
            container = dataset_path.container
            path = dataset_path.path

        if not path:
            raise ValueError("Path must be specified.")

        # get dataset
        df = self.hook.read(
            source_path=path,
            source_container=container,
            source_format=self.dataset_format,
            dataset_type="PolarsLazyFrame",
        )

        # transform
        transformed_data = self.transformation(df)

        # upload dataset
        return self.write(dataset=transformed_data)

    def write(self, dataset: pl.LazyFrame):
        destination_path = self.destination_path

        if isinstance(destination_path, XComGetter):
            destination_path = destination_path.parse(context=self.context)

        if isinstance(destination_path, dict) and "path" in destination_path and "container" in destination_path:
            self.hook.write(
                dataset=dataset,
                destination_path=destination_path["path"],
                destination_container=destination_path["container"],
            )
            return destination_path

        if isinstance(destination_path, DataLakePathBase):
            return self.hook.write(
                dataset=dataset,
                destination_path=destination_path.path,
                destination_container=destination_path.container,
            )

        if isinstance(destination_path, str):
            self.hook.write(dataset=dataset, destination_path=destination_path)

        if not destination_path:
            raise ValueError("`destination_path` must be specified and of the correct type.")
