from custom.operators.data.types import DatasetPath, DatasetPathDict
from utils.dag.xcom import XComGetter
from utils.filesystem.data_lake.base import DataLakePathBase
from airflow.utils.context import Context
from typing import Optional


def extract_dataset_path(path: DatasetPath, context: Optional[Context] = None) -> DatasetPathDict:
    """"""

    if isinstance(path, DataLakePathBase):
        return {
            "container": path.container,
            "path": path.path,
        }

    if isinstance(path, XComGetter):
        if not context:
            raise Exception("Airflow `conext` mus be provied.")

        xcom_value = path.parse(context=context)

        if xcom_value:
            if isinstance(xcom_value, str):
                path = xcom_value

            if isinstance(xcom_value, dict) and "path" in xcom_value:
                return {
                    "container": xcom_value.get("container", None),
                    "path": xcom_value["path"],
                }

    if isinstance(path, dict) and "path" in path:
        return {
            "container": path.get("container", None),
            "path": path["path"],
        }

    if isinstance(path, str):
        return {
            "container": None,
            "path": path,
        }

    raise Exception(f"No valie path could be extracted from: '{path}'")
