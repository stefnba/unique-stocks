import os

from pyspark.sql import SparkSession
from pyspark.sql import types as t

spark = SparkSession.builder.appName("Sink entity_isin").getOrCreate()

ADLS_STORAGE_ACCOUNT_NAME = os.getenv("ADLS_STORAGE_ACCOUNT_NAME")
CONTAINER = "temp"
PATH = spark.conf.get("spark.datasetPath")


if not ADLS_STORAGE_ACCOUNT_NAME:
    raise Exception("Missing environment variables for ADLS")

if not PATH:
    raise Exception("Missing spark.datasetPath configuration")

PATH = PATH.replace(f"abfs://{CONTAINER}", "")


schema = t.StructType(
    [
        t.StructField("LEI", t.StringType(), True),
        t.StructField("ISIN", t.StringType(), True),
    ]
)


data = (
    spark.read.schema(schema)
    .option("header", True)
    .csv(f"abfs://{CONTAINER}@{ADLS_STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/{PATH}")
).createOrReplaceTempView("data")

spark.sql(
    """
SELECT
    LEI as lei,
    ISIN as isin
FROM
    data
"""
).createOrReplaceTempView("transformed")


spark.sql(
    """
INSERT OVERWRITE 
    curated.entity_isin
    (lei, isin, created_at, updated_at)
SELECT
    lei, isin, current_timestamp(), NULL
FROM transformed;
"""
)


spark.stop()
