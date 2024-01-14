import os

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Insert into Iceberg table").getOrCreate()

ADLS_STORAGE_ACCOUNT_NAME = os.getenv("ADLS_STORAGE_ACCOUNT_NAME")
CONTAINER = "temp"
PATH = spark.conf.get("spark.datasetPath")


if not PATH:
    raise Exception("Path is not set.")


PATH = PATH.replace("s3://", "s3a://")

print(PATH)


data = spark.read.parquet(PATH).createOrReplaceTempView("data")

spark.sql(
    """
    INSERT OVERWRITE exchange SELECT * FROM data;
    """
)
