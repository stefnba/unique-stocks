# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pylint: disable=W0106:expression-not-assigned, C0415:import-outside-toplevel
# pyright: reportUnusedExpression=false
from datetime import datetime

from airflow.decorators import task, dag
from string import Template

from typing import TypedDict

from utils.dag.xcom import XComGetter
from custom.providers.eod_historical_data.transformers.fundamental.common_stock import (
    EoDCommonStockFundamentalTransformer,
)
from shared.path import FundamentalPath, SecurityPath, AdlsPath, LocalPath
from custom.hooks.api.shared import BulkApiHook
import pyarrow as pa
from custom.operators.data.delta_table import WriteDeltaTableFromDatasetOperator


class FundamentalSecurity(TypedDict):
    exchange_code: str
    security_code: str


@task
def extract_security():
    import polars as pl
    from custom.providers.delta_table.hooks.delta_table import DeltaTableHook
    from custom.hooks.data.mapping import MappingDatasetHook
    import duckdb

    mapping = MappingDatasetHook().mapping(product="exchange", source="EodHistoricalData", field="composite_code")

    dt = DeltaTableHook(conn_id="azure_data_lake").read(path=SecurityPath.curated())

    securities = pl.from_arrow(
        dt.to_pyarrow_dataset(
            partitions=[
                (
                    "exchange_code",
                    "in",
                    [
                        "XETRA",
                        "NASDAQ",
                        "NYSE",
                    ],
                ),
                ("type", "=", "common_stock"),
            ]
        ).to_batches()
    )

    data = (
        duckdb.sql(
            """
        --sql
        SELECT
            s.exchange_code,
            s.code AS entity_code,
            COALESCE(m.mapping_value, s.exchange_code) AS api_exchange_code
        FROM securities s
        LEFT JOIN mapping m on s.exchange_code = m.source_value
        ;
        """
        ).pl()
        # .sample(300)
        .to_dicts()
    )

    return data

    if isinstance(securities, pl.DataFrame):
        return securities.select(
            [
                # pl.lit("US").alias("composite_exchange_code"),
                pl.col("exchange_code"),
                pl.col("code").alias("entity_code"),
            ]
        ).to_dicts()


def transform_on_ingest(data: bytes, record: dict[str, str], local_download_dir: LocalPath):
    import json
    from custom.providers.eod_historical_data.transformers.utils import deep_get

    data_dict = json.loads(data)
    sec_type = deep_get(data_dict, ["General", "Type"])

    # Common stocks
    if sec_type == "Common Stock":
        # Only primary ticker
        if deep_get(data_dict, ["General", "PrimaryTicker"]) == Template(
            "${entity_code}.${api_exchange_code}"
        ).safe_substitute(record):
            transformer = EoDCommonStockFundamentalTransformer(data=data)
            transformed_data = transformer.transform()

            if transformed_data is not None:
                transformed_data.write_parquet(local_download_dir.to_pathlib() / f"{transformer.security}.parquet")

        return


@task
def ingest(securities: list[dict[str, str]]):
    api = BulkApiHook(
        conn_id="eod_historical_data",
        response_format="json",
        transform=transform_on_ingest,
        adls_upload=BulkApiHook.AdlsUploadConfig(
            path=FundamentalPath,
            conn_id="azure_data_lake",
            record_mapping={
                "exchange": "exchange_code",
                "entity": "entity_code",
            },
        ),
    )
    api.run(endpoint="fundamentals/${entity_code}.${api_exchange_code}", items=securities)

    # upload as arrow ds
    destination_path = api.upload_dataset(conn_id="azure_data_lake", dataset_format="parquet")

    return destination_path.to_dict()


sink = WriteDeltaTableFromDatasetOperator(
    task_id="sink",
    adls_conn_id="azure_data_lake",
    dataset_path=XComGetter.pull_with_template(task_id="ingest"),
    # destination_path=FundamentalPath.curated(),
    destination_path="curated/test_fina",
    pyarrow_options={
        "schema": pa.schema(
            [
                pa.field("exchange_code", pa.string()),
                pa.field("security_code", pa.string()),
                pa.field("category", pa.string()),
                pa.field("metric", pa.string()),
                pa.field("value", pa.string()),
                pa.field("currency", pa.string()),
                pa.field("period", pa.date32()),
                pa.field("period_type", pa.string()),
                pa.field("published_at", pa.date32()),
            ]
        )
    },
    delta_table_options={
        "mode": "overwrite",
        "partition_by": [
            "exchange_code",
            # "security_code",
        ],
    },
)


@dag(
    schedule=None,
    start_date=datetime(2023, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=["fundamental"],
)
def fundamental():
    extract_security_task = extract_security()

    ingest(extract_security_task) >> sink


dag_object = fundamental()

if __name__ == "__main__":
    connections = "testing/connections/connections.yaml"

    dag_object.test(conn_file_path=connections)
