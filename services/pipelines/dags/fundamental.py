# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow.decorators import task, dag, task_group


from typing import TypedDict

from utils.dag.xcom import XComGetter, set_xcom_value, get_xcom_template
from custom.providers.eod_historical_data.operators.fundamental.transform import EodFundamentalTransformOperator
from shared.data_lake_path import FundamentalPath, TempDirectory


class FundamentalSecurity(TypedDict):
    exchange_code: str
    security_code: str


@task
def extract_security() -> list[FundamentalSecurity]:
    return [
        {"exchange_code": "US", "security_code": "AAPL"},
        {"exchange_code": "US", "security_code": "MFST"},
    ]


def transform_fundamental(data):
    return data


@task_group
def one_fundamental(one_fundamental: FundamentalSecurity):
    @task
    def ingest(security: FundamentalSecurity):
        from custom.providers.eod_historical_data.hooks.api import EodHistoricalDataApiHook
        from airflow.providers.microsoft.azure.hooks.wasb import WasbHook
        from shared.data_lake_path import FundamentalPath
        import json

        fundamental_data = EodHistoricalDataApiHook().fundamental(**security)

        destination = FundamentalPath.raw(source="EodHistoricalData", format="json").add_element(
            entity=security["security_code"]
        )

        WasbHook(wasb_conn_id="azure_blob").upload(
            data=json.dumps(fundamental_data),
            container_name=destination.container,
            blob_name=destination.path,
        )

        set_xcom_value(key="entity_id", value=security["security_code"])

        return destination.serialized

    transform = EodFundamentalTransformOperator(
        task_id="transform",
        adls_conn_id="azure_data_lake",
        source_path=XComGetter(task_id="one_fundamental.ingest"),
        destination_path=TempDirectory(),
        entity_id=get_xcom_template(task_id="one_fundamental.ingest", key="entity_id"),
    )

    ingest(one_fundamental) >> transform


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["fundamental"],
)
def fundamental():
    one_fundamental.expand(one_fundamental=extract_security())


dag_object = fundamental()

if __name__ == "__main__":
    connections = "testing/connections/connections.yaml"

    dag_object.test(conn_file_path=connections)
