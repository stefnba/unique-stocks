""""
Get security data from ADLS and insert into Iceberg table.
"""

import os

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Insert into Iceberg table").getOrCreate()

ADLS_STORAGE_ACCOUNT_NAME = os.getenv("ADLS_STORAGE_ACCOUNT_NAME")
CONTAINER = "temp"
PATH = spark.conf.get("spark.datasetPath")


if not ADLS_STORAGE_ACCOUNT_NAME:
    raise Exception("Missing environment variables for ADLS")

if not PATH:
    raise Exception("Missing spark.datasetPath configuration")

PATH = PATH.replace(f"abfs://{CONTAINER}", "")


data = spark.read.parquet(
    f"abfs://{CONTAINER}@{ADLS_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/{PATH}"
).createOrReplaceTempView("data")

spark.sql(
    """
    INSERT OVERWRITE
        curated.security (code, name, isin, country, currency, exchange_code, type, created_at, updated_at) 
    SELECT code, name, isin, country, currency, exchange_code, type, current_timestamp(), NULL
    FROM data;
    """
)


spark.stop()
