from airflow.models.baseoperator import BaseOperator
from airflow.utils.context import Context
from typing import TypeAlias, Literal
from custom.providers.azure.hooks.data_lake_storage import AzureDataLakeStorageHook
from custom.providers.azure.hooks.dataset import AzureDatasetHook
from utils.dag.xcom import XComGetter
import json
import polars as pl
from functools import reduce
from utils.filesystem.path import Path

SourcePath: TypeAlias = str | XComGetter
Period = Literal["yearly", "quarterly"]


def deep_get(data: dict, keys: str | list[str]):
    """Helper to get dict within nested dict."""
    if isinstance(keys, str):
        keys = keys.split(".")

    return reduce(lambda d, key: d.get(key, None) if isinstance(d, dict) else None, keys, data)


def nested_fundamental(data: dict, keys: str | list[str], return_values_as_list=False):
    """Helper that return nested dict or values of that dict as a list."""
    obj = deep_get(data=data, keys=keys)

    if obj and return_values_as_list:
        return list(obj.values())

    return obj


class EodIndexMemberTransformOperator(BaseOperator):
    template_fields = ("source_path", "destination_path")

    context: Context
    source_path: Path
    destination_path: Path
    adls_conn_id: str
    entity_id: str

    def __init__(
        self,
        task_id: str,
        adls_conn_id: str,
        source_path: Path,
        destination_path: Path,
        **kwargs,
    ):
        super().__init__(
            task_id=task_id,
            **kwargs,
        )

        self.source_path = source_path
        self.conn_id = adls_conn_id
        self.destination_path = destination_path

    def execute(self, context: Context):
        self.context = context

        hook = AzureDataLakeStorageHook(conn_id=self.conn_id)
        data = hook.read_blob(**self.source_path.afls_path)

        index_data = json.loads(data)

        index_code = nested_fundamental(index_data, ["General", "Code"])
        index_name = nested_fundamental(index_data, ["General", "Name"])
        members = nested_fundamental(index_data, ["Components"], return_values_as_list=True)

        if members is not None and len(members) == 0:
            return

        df = (
            pl.DataFrame(members)
            .with_columns(
                [
                    pl.lit(index_code).alias("index_code"),
                    pl.lit(index_name).alias("index_name"),
                ]
            )
            .select(
                [
                    pl.col("Code").alias("code"),
                    pl.col("Exchange").alias("exchange_code"),
                    pl.col("Name").alias("name"),
                    "index_code",
                    "index_name",
                ]
            )
        )

        AzureDatasetHook(conn_id=self.conn_id).write(dataset=df.lazy(), destination_path=self.destination_path)

        return self.destination_path
