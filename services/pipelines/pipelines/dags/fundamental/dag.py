# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow import DAG
from airflow.decorators import task, task_group


@task
def extract_codes():
    from dags.fundamental.eod_historical_data.jobs import extract

    return extract()


@task_group
def manage_one(code: str):
    """
    Manages downloading and transforming securities of one exchange.
    """

    @task
    def ingest(exchange: str) -> str:
        from dags.security.eod_historical_data.jobs import ASSET_SOURCE, ingest
        from dags.security.path import SecurityPath
        from shared.hooks.data_lake import data_lake_hooks

        from shared.utils.dag.xcom import set_xcom_value

        set_xcom_value(key="exchange", value=exchange)

        return data_lake_hooks.checkout(
            func=lambda: ingest(exchange),
            commit_path=SecurityPath.raw(source=ASSET_SOURCE, bin=exchange, file_type="json"),
        )


with DAG(
    dag_id="fundamental",
    schedule="@daily",
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["fundamental"],
    render_template_as_native_obj=True,
) as dag:
    extract_codes_task = extract_codes()
    manage_one.expand(exchange_code=extract_codes_task)
