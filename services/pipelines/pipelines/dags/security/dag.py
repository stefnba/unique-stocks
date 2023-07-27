# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow import DAG
from airflow.decorators import task, task_group


@task
def extract_exchange_codes():
    from dags.security.eod_historical_data.jobs import extract

    return extract()


@task_group
def manage_one_exchange(exchange_code: str):
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

    @task
    def transform_pre_figi(file_path: str):
        from dags.security.eod_historical_data.jobs import transform_pre_figi
        from dags.security.path import SecurityPath
        from shared.hooks.data_lake import data_lake_hooks

        return data_lake_hooks.checkout(
            checkout_path=file_path,
            func=lambda data: transform_pre_figi(data),
            commit_path=SecurityPath.temp(),
        )

    @task
    def map_figi(file_path: str):
        from dags.security.eod_historical_data.jobs import map_figi
        from dags.security.path import SecurityPath
        from shared.hooks.data_lake import data_lake_hooks

        return data_lake_hooks.checkout(
            checkout_path=file_path, func=lambda data: map_figi(data), commit_path=SecurityPath.temp()
        )

    @task
    def transform_post_figi(file_path: str):
        from dags.security.eod_historical_data.jobs import ASSET_SOURCE, transform_post_figi
        from dags.security.path import SecurityPath
        from shared.hooks.data_lake import data_lake_hooks

        from shared.utils.dag.xcom import get_xcom_value

        exchange_code = get_xcom_value(task_id="manage_one_exchange.ingest", key="exchange", return_type=str)

        return data_lake_hooks.checkout(
            checkout_path=file_path,
            func=lambda data: transform_post_figi(data),
            commit_path=SecurityPath.processed(source=ASSET_SOURCE, bin=exchange_code),
        )

    @task
    def extract_security():
        from dags.security.eod_historical_data.jobs import extract_security
        from shared.hooks.data_lake import data_lake_hooks
        from dags.security.path import SecurityPath
        from shared.utils.dag.xcom import get_xcom_value

        file_path = get_xcom_value(task_id="manage_one_exchange.transform_post_figi", return_type=str)

        return data_lake_hooks.checkout(
            checkout_path=file_path, func=lambda data: extract_security(data), commit_path=SecurityPath.temp()
        )

    @task
    def load_security(file_path: str):
        from shared.clients.db.postgres.repositories import DbQueryRepositories
        from shared.hooks.data_lake import data_lake_hooks

        data = data_lake_hooks.download(path=file_path).to_polars_df()
        DbQueryRepositories.security.add(data)

    @task
    def extract_security_ticker():
        from dags.security.eod_historical_data.jobs import extract_security_ticker
        from shared.hooks.data_lake import data_lake_hooks
        from dags.security.path import SecurityPath

        from shared.utils.dag.xcom import get_xcom_value

        file_path = get_xcom_value(task_id="manage_one_exchange.transform_post_figi", return_type=str)

        return data_lake_hooks.checkout(
            checkout_path=file_path, func=lambda data: extract_security_ticker(data), commit_path=SecurityPath.temp()
        )

    @task
    def load_security_ticker(file_path: str):
        from dags.security.eod_historical_data.jobs import load_security_ticker_into_database
        from shared.hooks.data_lake import data_lake_hooks
        from shared.utils.dag.xcom import get_xcom_value

        file_path = get_xcom_value(task_id="manage_one_exchange.extract_security_ticker", return_type=str)

        data_lake_hooks.checkout(
            checkout_path=file_path,
            func=lambda data: load_security_ticker_into_database(data),
        )

    @task
    def extract_security_listing():
        from dags.security.eod_historical_data.jobs import extract_security_listing
        from shared.hooks.data_lake import data_lake_hooks
        from dags.security.path import SecurityPath
        from shared.utils.dag.xcom import get_xcom_value

        file_path = get_xcom_value(task_id="manage_one_exchange.transform_post_figi", return_type=str)

        return data_lake_hooks.checkout(
            checkout_path=file_path, func=lambda data: extract_security_listing(data), commit_path=SecurityPath.temp()
        )

    @task
    def load_security_listing(file_path: str):
        from dags.security.eod_historical_data.jobs import load_security_listing_into_database
        from shared.hooks.data_lake import data_lake_hooks

        # from shared.utils.dag.xcom import get_xcom_value

        # file_path = get_xcom_value(task_id="manage_one_exchange.extract_security_listing", return_type=str)

        data_lake_hooks.checkout(
            checkout_path=file_path,
            func=lambda data: load_security_listing_into_database(data),
        )

    ingest_task = ingest(exchange_code)
    transform_pre_figi_task = transform_pre_figi(ingest_task)
    map_figi_task = map_figi(transform_pre_figi_task)
    transform_post_figi_task = transform_post_figi(map_figi_task)

    extract_security_task = extract_security()
    extract_security_ticker_task = extract_security_ticker()
    extract_security_listing_task = extract_security_listing()

    load_security_task = load_security(extract_security_task)
    load_security_ticker_task = load_security_ticker(extract_security_ticker_task)
    load_security_listing(extract_security_listing_task)

    transform_post_figi_task >> extract_security_task
    load_security_task >> extract_security_ticker_task
    load_security_ticker_task >> extract_security_listing_task


with DAG(
    dag_id="security", schedule=None, start_date=datetime(2023, 1, 1), catchup=False, tags=["security"], concurrency=1
) as dag:
    extract_exchange_codes_task = extract_exchange_codes()
    manage_one_exchange.expand(exchange_code=extract_exchange_codes_task)
