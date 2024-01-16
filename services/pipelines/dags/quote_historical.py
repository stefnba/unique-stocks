import logging
from datetime import datetime, timedelta

from airflow.decorators import dag, task, task_group
from airflow.models.param import Param
from airflow.utils.dates import days_ago
from conf.spark import config as spark_config
from conf.spark import packages as spark_packages
from custom.providers.spark.operators.submit import SparkSubmitSHHOperator
from shared.path import ADLSRawZonePath
from utils.dag.xcom import XComGetter

AWS_DATA_LAKE_CONN_ID = "aws"
AZURE_DATA_LAKE_CONN_ID = "azure_data_lake"


DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": days_ago(2),
    "email": ["airflow@example.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}


@task
def extract_security():
    """
    Get all securities and map their exchange composite_code.
    """
    import polars as pl
    from custom.providers.iceberg.hooks.pyiceberg import IcebergHook, filter_expressions
    from utils.dag.conf import get_dag_conf

    conf = get_dag_conf()

    exchanges = conf.get("exchanges", [])
    security_types = conf.get("security_types", [])

    # exchanges = ["XETRA", "NASDAQ", "INDX"]
    # security_types = ["common_stock", "index", "preferred_stock"]

    logging.info(
        f"""Getting security quotes for exchanges: '{", ".join(exchanges)}' """
        f"""and security types: '{", ".join(security_types)}'."""
    )

    mapping = IcebergHook(
        conn_id="aws", catalog_name="uniquestocks_dev", table_name="uniquestocks_dev.mapping"
    ).to_polars(
        selected_fields=("source_value", "mapping_value"),
        row_filter="field = 'composite_code' AND product = 'exchange' AND source = 'EodHistoricalData'",
    )
    security = IcebergHook(
        conn_id="aws", catalog_name="uniquestocks_dev", table_name="uniquestocks_dev.security"
    ).to_polars(
        selected_fields=("exchange_code", "code", "type"),
        row_filter=filter_expressions.In("exchange_code", exchanges),
    )

    security = security.join(mapping, left_on="exchange_code", right_on="source_value", how="left").with_columns(
        pl.coalesce(["mapping_value", "exchange_code"]).alias("api_exchange_code"),
    )

    securities_as_dict = []

    # create a list of securities for each exchange and security type
    for e in exchanges:
        for t in security_types:
            s = security.filter((pl.col("exchange_code") == e) & (pl.col("type") == t)).to_dicts()

            if len(s) > 0:
                securities_as_dict.append(s)

    return securities_as_dict


@task
def set_sink_path():
    sink_path = ADLSRawZonePath(product="security_quote", source="EodHistoricalData", type="csv")

    return f"{sink_path.path.scheme}://{sink_path.path.container}/{sink_path.path.dirs}"


@task_group
def ingest_security_group(security_groups: list[dict]):
    """
    Ingest quotes for a list of securities from a specific exchange and with a specific type.
    """

    @task
    def map_url_sink_path(securities: list[dict]):
        """
        Take list of securities and add endpoint and blob path to each record.
        """

        from airflow.exceptions import AirflowException
        from airflow.models.taskinstance import TaskInstance
        from airflow.operators.python import get_current_context
        from shared.path import ADLSRawZonePath

        context = get_current_context()

        ti = context.get("task_instance")

        if not isinstance(ti, TaskInstance):
            raise AirflowException("No task instance found.")

        path = ADLSRawZonePath.parse(path=ti.xcom_pull(task_ids="set_sink_path"), extension="csv")

        return list(
            map(
                lambda exchange: {
                    "endpoint": f"{exchange['code']}.{exchange['api_exchange_code']}",
                    "blob": path.add_partition(
                        {"exchange": exchange["exchange_code"], "security": exchange["code"]}
                    ).blob,
                },
                securities,
            )
        )

    @task(max_active_tis_per_dag=1)
    def ingest(securities):
        """
        Ingest quotes for a list of securities from a specific exchange and with a specific type.
        Calls EodHistoricalData API and uploads the data to Azure Data Lake Storage using async upload.
        """

        import logging

        from custom.providers.azure.hooks.data_lake_storage import AzureDataLakeStorageBulkHook, UrlUploadRecord
        from custom.providers.eod_historical_data.hooks.api import EodHistoricalDataApiHook

        logging.info(
            f"Ingestion of {len(securities):,} securities of type '{securities[0].get('type', '')}'"
            f"""for exchange '{securities[0].get("exchange_code", "")}'."""
        )

        hook = AzureDataLakeStorageBulkHook(conn_id="azure_data_lake")
        api_hook = EodHistoricalDataApiHook()

        url_endpoints = [UrlUploadRecord(**sec) for sec in securities]

        uploaded_blobs = hook.upload_from_url(
            container="raw",
            base_url="https://eodhistoricaldata.com/api/eod",
            base_params=api_hook._base_params,
            url_endpoints=url_endpoints,
        )

        return uploaded_blobs

    map_task = map_url_sink_path(security_groups)
    ingest(map_task)


sink = SparkSubmitSHHOperator(
    task_id="sink_to_iceberg",
    app_file_name="sink_security_quote.py",
    ssh_conn_id="ssh_test",
    spark_conf={
        **spark_config.adls,
        **spark_config.iceberg,
    },
    spark_packages=[*spark_packages.adls, *spark_packages.iceberg],
    connections=[AWS_DATA_LAKE_CONN_ID, AZURE_DATA_LAKE_CONN_ID],
    dataset=XComGetter.pull_with_template(task_id="set_sink_path"),
    conn_env_mapping={
        "AWS_ACCESS_KEY_ID": "AWS__LOGIN",
        "AWS_SECRET_ACCESS_KEY": "AWS__PASSWORD",
        "AWS_REGION": "AWS__EXTRA__REGION_NAME",
        "ADLS_STORAGE_ACCOUNT_NAME": "AZURE_DATA_LAKE__HOST",
        "ADLS_CLIENT_ID": "AZURE_DATA_LAKE__LOGIN",
        "ADLS_CLIENT_SECRET": "AZURE_DATA_LAKE__PASSWORD",
        "ADLS_TENANT_ID": "AZURE_DATA_LAKE__EXTRA__TENANT_ID",
    },
)


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["quote", "security"],
    # default_args=default_args,
    params={
        "exchanges": Param(
            type="array",
            default=[
                "XETRA",
                "NASDAQ",
                "NYSE",
                "CC",
                "FOREX",
                "MONEY",
                "LSE",
                "INDX",
                "F",
                "VI",
                "PA",
                "SW",
                "AS",
                "IL",
                "STU",
                "MU",
            ],
            examples=[
                "NASDAQ",
                "NYSE",
                "LSE",
                "NEO",
                "V",
                "TO",
                "BE",
                "HM",
                "XETRA",
                "DU",
                "MU",
                "HA",
                "F",
                "STU",
                "VI",
                "LU",
                "PA",
                "BR",
                "MC",
                "SW",
                "LS",
                "AS",
                "ST",
                "IR",
                "CO",
                "OL",
                "IC",
                "HE",
                "MSE",
                "EGX",
                "XBOT",
                "GSE",
                "BRVM",
                "PR",
                "XNAI",
                "BC",
                "SEM",
                "XNSA",
                "RSE",
                "DSE",
                "USE",
                "LUSE",
                "TA",
                "XZIM",
                "VFEX",
                "KQ",
                "KO",
                "BUD",
                "WAR",
                "PSE",
                "SN",
                "JSE",
                "JK",
                "BK",
                "SHG",
                "NSE",
                "AT",
                "SR",
                "SHE",
                "KAR",
                "AU",
                "CM",
                "VN",
                "KLSE",
                "BA",
                "RO",
                "SA",
                "MX",
                "IL",
                "ZSE",
                "TWO",
                "MCX",
                "TW",
                "LIM",
                "MONEY",
                "EUFUND",
                "IS",
                "FOREX",
                "CC",
            ],
            values_display={
                "NASDAQ": "NASDAQ",
                "NYSE": "New York Stock Exchange",
                "LSE": "London Exchange",
                "NEO": "NEO Exchange",
                "V": "TSX Venture Exchange",
                "TO": "Toronto Exchange",
                "BE": "Berlin Exchange",
                "HM": "Hamburg Exchange",
                "XETRA": "XETRA Exchange",
                "DU": "Dusseldorf Exchange",
                "MU": "Munich Exchange",
                "HA": "Hanover Exchange",
                "F": "Frankfurt Exchange",
                "STU": "Stuttgart Exchange",
                "VI": "Vienna Exchange",
                "LU": "Luxembourg Stock Exchange",
                "PA": "Euronext Paris",
                "BR": "Euronext Brussels",
                "MC": "Madrid Exchange",
                "SW": "SIX Swiss Exchange",
                "LS": "Euronext Lisbon",
                "AS": "Euronext Amsterdam",
                "ST": "Stockholm Exchange",
                "IR": "Irish Exchange",
                "CO": "Copenhagen Exchange",
                "OL": "Oslo Stock Exchange",
                "IC": "Iceland Exchange",
                "HE": "Helsinki Exchange",
                "MSE": "Malawi Stock Exchange",
                "EGX": "Egyptian Exchange",
                "XBOT": "Botswana Stock Exchange ",
                "GSE": "Ghana Stock Exchange",
                "BRVM": "Regional Securities Exchange",
                "PR": "Prague Stock Exchange ",
                "XNAI": "Nairobi Securities Exchange",
                "BC": "Casablanca Stock Exchange",
                "SEM": "Stock Exchange of Mauritius",
                "XNSA": "Nigerian Stock Exchange",
                "RSE": "Rwanda Stock Exchange",
                "DSE": "Dar es Salaam Stock Exchange",
                "USE": "Uganda Securities Exchange",
                "LUSE": "Lusaka Stock Exchange",
                "TA": "Tel Aviv Exchange",
                "XZIM": "Zimbabwe Stock Exchange",
                "VFEX": "Victoria Falls Stock Exchange",
                "KQ": "KOSDAQ",
                "KO": "Korea Stock Exchange",
                "BUD": "Budapest Stock Exchange",
                "WAR": "Warsaw Stock Exchange",
                "PSE": "Philippine Stock Exchange",
                "SN": "Chilean Stock Exchange",
                "JSE": "Johannesburg Exchange",
                "JK": "Jakarta Exchange",
                "BK": "Thailand Exchange",
                "SHG": "Shanghai Exchange",
                "NSE": "NSE (India)",
                "AT": "Athens Exchange",
                "SR": "Saudi Arabia Exchange",
                "SHE": "Shenzhen Exchange",
                "KAR": "Karachi Stock Exchange",
                "AU": "Australia Exchange",
                "CM": "Colombo Stock Exchange",
                "VN": "Vietnam Stocks",
                "KLSE": "Kuala Lumpur Exchange",
                "BA": "Buenos Aires Exchange",
                "RO": "Bucharest Stock Exchange",
                "SA": "Sao Paolo Exchange",
                "MX": "Mexican Exchange",
                "IL": "London IL",
                "ZSE": "Zagreb Stock Exchange",
                "TWO": "Taiwan OTC Exchange",
                "MCX": "MICEX Moscow Russia",
                "TW": "Taiwan Exchange",
                "LIM": "Bolsa de Valores de Lima",
                "MONEY": "Money Market Virtual Exchange",
                "EUFUND": "Europe Fund Virtual Exchange",
                "IS": "Istanbul Stock Exchange",
                "FOREX": "FOREX",
                "CC": "Cryptocurrencies",
                "INDX": "Index",
            },
        ),
        "security_types": Param(
            type="array",
            default=[
                "rate",
                "common_stock",
                "capital_notes",
                "unit",
                "mutual_fund",
                "preferred_stock",
                "etc",
                "money",
                "etf",
                "note",
                "currency",
                "index",
                "fund",
            ],
            examples=[
                "rate",
                "common_stock",
                "capital_notes",
                "unit",
                "mutual_fund",
                "preferred_stock",
                "etc",
                "money",
                "etf",
                "note",
                "currency",
                "index",
                "fund",
            ],
        ),
    },
)
def quote_historical():
    extract_task = extract_security()
    set_sink_task = set_sink_path()

    ingest_task = ingest_security_group.expand(security_groups=extract_task)
    extract_task >> set_sink_task >> ingest_task >> sink  # type: ignore


dag_object = quote_historical()

if __name__ == "__main__":
    connections = "testing/connections/connections.yaml"

    dag_object.test(
        conn_file_path=connections,
        run_conf={
            "delta_table_mode": "overwrite",
            "exchanges": ["XETRA", "NASDAQ", "INDX"],
            "security_types": ["common_stock", "index", "preferred_stock"],
        },
    )
