import uuid
from datetime import datetime
from pathlib import Path
from typing import Literal

BASE_INGEST_PATH = Path("ingest")

FILE_TYPE = Literal["parquet", "json", "csv"]


def exchange_ingest_path():
    return BASE_INGEST_PATH / "exchanges" / ingest_ts() / filename()


def ingest_ts():
    """Helper function to generate a timestamp for the ingest path that can be used for timestamp partitionging
    with Trino and Hive."""
    return f"ingested_at={datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')}"


def filename(file_type: FILE_TYPE = "parquet"):
    """Helper function to generate a unique filename for the ingest path."""
    return f"{uuid.uuid4()}.{file_type}"
