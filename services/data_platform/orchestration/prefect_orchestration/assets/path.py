import uuid
from datetime import datetime
from pathlib import Path
from typing import Literal

BASE_INGEST_PATH = Path("ingest")

FILE_TYPE = Literal["parquet", "json", "csv"]


def exchange_ingest_path():
    return BASE_INGEST_PATH / "exchange" / ingest_ts() / filename()


def exchange_security_ingest_path(exchange_code: str, ingest_ts: str):
    return BASE_INGEST_PATH / "exchange_security" / ingest_ts / f"exchange_code={exchange_code}" / filename()


def exchange_index_member_ingest_path(ingest_ts: str):
    return BASE_INGEST_PATH / "index_member" / ingest_ts / filename()


def security_quote_ingest_path(exchange_code: str, security_code: str, ingest_ts: str):
    return (
        BASE_INGEST_PATH
        / "security_quote"
        / ingest_ts
        / f"exchange_code={exchange_code}"
        / f"security_code={security_code}"
        / filename()
    )


def ingest_ts():
    """Helper function to generate a timestamp for the ingest path that can be used for timestamp partitionging
    with Trino and Hive."""
    return f"ingested_at={datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}"


def filename(file_type: FILE_TYPE = "parquet"):
    """Helper function to generate a unique filename for the ingest path."""
    return f"{uuid.uuid4()}.{file_type}"
