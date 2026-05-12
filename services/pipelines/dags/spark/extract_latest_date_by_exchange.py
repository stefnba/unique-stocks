from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("Extract_latest_date_by_exchange").getOrCreate()


PATH = spark.conf.get("spark.datasetPath")


if not PATH:
    raise Exception("Missing spark.datasetPath configuration")

PATH = PATH.replace("s3://", "s3a://")


spark.sql(
    """
SELECT
    exchange_code,
    MAX(date) as latest_date
FROM
    curated.security_quote
GROUP BY
    exchange_code
"""
).write.parquet(PATH)


spark.stop()
