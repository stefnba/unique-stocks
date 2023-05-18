# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models import TaskInstance


@task
def extract_exchange_codes(**context: TaskInstance):
    from dags.exchange.exchange_security.jobs.eod import EodExchangeSecurityJobs

    # processed exchange
    file_path: str = context["ti"].xcom_pull(dag_id="exchange", task_ids="process_eod", include_prior_dates=True)

    return EodExchangeSecurityJobs.extract_exchange_codes(file_path)


@task_group
def manage_one_exchange(exchange_code: str):
    """
    Manages downloading and processing securities of one exchange.
    """
    from shared.types.types import CodeFilePath

    @task
    def download_securities_of_exchange(exchange: str):
        import logging

        from dags.exchange.exchange_security.jobs.eod import EodExchangeSecurityJobs

        try:
            return EodExchangeSecurityJobs.download(exchange)
        except Exception as e:
            logging.warning(exchange, "Watch out!", e)
            return None

    @task
    def transform_securities_of_exchange(file_path: str):
        from dags.exchange.exchange_security.jobs.eod import EodExchangeSecurityJobs

        if file_path:
            return EodExchangeSecurityJobs.transform(file_path)

    @task
    def curate_securities_of_exchange(input: CodeFilePath):
        from dags.exchange.exchange_security.jobs.eod import EodExchangeSecurityJobs

        if input:
            return EodExchangeSecurityJobs.curate(exchange_uid=input["code"], file_path=input["file_path"])

    download_securities_of_exchange_task = download_securities_of_exchange(exchange_code)
    transform_securities_of_exchange_task = transform_securities_of_exchange(download_securities_of_exchange_task)
    curate_securities_of_exchange(transform_securities_of_exchange_task)


with DAG(
    dag_id="exchange_security",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["exchange", "securities"],
) as dag:
    extract_exchange_codes_task = extract_exchange_codes()

    manage_one_exchange.expand(exchange_code=extract_exchange_codes_task)