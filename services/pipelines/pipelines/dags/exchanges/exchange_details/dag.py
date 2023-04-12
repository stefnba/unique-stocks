# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow import DAG
from airflow.decorators import task, task_group
from airflow.models import TaskInstance


@task
def extract_exchange_codes(**context: TaskInstance):
    from dags.exchanges.exchange_details.jobs.exchange_details import ExchangeDetailsJobs

    file_path = context["ti"].xcom_pull(dag_id="exchanges", task_ids="process_eod", include_prior_dates=True)

    return ExchangeDetailsJobs.extract_exchange_codes(file_path)


@task_group
def one_exchange(exchange_code: str):
    """
    Manages downloading and processing of one exchange.
    """

    @task
    def download_details_of_exchange(index: str):
        import logging

        from dags.exchanges.exchange_details.jobs.exchange_details import ExchangeDetailsJobs

        logging.warning("Watch out!")

        try:
            return ExchangeDetailsJobs.download_details(index)
        except Exception as e:
            logging.warning(index, "Watch out!", e)
            return None

    @task
    def transform_details_of_exchange(raw_details):
        from dags.exchanges.exchange_details.jobs.exchange_details import ExchangeDetailsJobs

        if raw_details:
            return ExchangeDetailsJobs.transform_details(raw_details)

    @task
    def transform_holidays_of_exchange(raw_details):
        from dags.exchanges.exchange_details.jobs.exchange_details import ExchangeDetailsJobs

        if raw_details:
            return ExchangeDetailsJobs.transform_holidays(raw_details)

    download_details_of_exchange_task = download_details_of_exchange(exchange_code)

    transform_details_of_exchange(download_details_of_exchange_task)
    transform_holidays_of_exchange(download_details_of_exchange_task)


@task
def merge_details(**context: TaskInstance):
    from dags.exchanges.exchange_details.jobs.exchange_details import ExchangeDetailsJobs

    exchange_details_path: list[str | list[str]] = context["ti"].xcom_pull(
        task_ids="one_exchange.transform_details_of_exchange"
    )

    return ExchangeDetailsJobs.merge([path for path in exchange_details_path])


@task
def merge_holidays(**context: TaskInstance):
    from dags.exchanges.exchange_details.jobs.exchange_details import ExchangeDetailsJobs

    exchange_holidays_paths: list[str | list[str]] = context["ti"].xcom_pull(
        task_ids="one_exchange.transform_holidays_of_exchange"
    )

    return ExchangeDetailsJobs.merge([path for path in exchange_holidays_paths])


@task
def curate_merged_details(**context: TaskInstance):
    from dags.exchanges.exchange_details.jobs.exchange_details import ExchangeDetailsJobs

    file_path: str = context["ti"].xcom_pull(task_ids="merge_details")

    return ExchangeDetailsJobs.curate_merged_details(file_path)


@task
def curate_merged_holidays(**context: TaskInstance):
    from dags.exchanges.exchange_details.jobs.exchange_details import ExchangeDetailsJobs

    file_path: str = context["ti"].xcom_pull(task_ids="merge_holidays")

    return ExchangeDetailsJobs.curate_merged_holidays(file_path)


with DAG(
    dag_id="exchange_details",
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=["exchanges"],
) as dag:
    merge_details_task = merge_details()
    merge_holidays_task = merge_holidays()

    one_exchange.expand(exchange_code=extract_exchange_codes()) >> [merge_details_task, merge_holidays_task]

    merge_details_task >> curate_merged_details()
    merge_holidays_task >> curate_merged_holidays()
